"""One explicit, bounded graph. Only identifiers and small typed payloads are checkpointed."""
import asyncio
from datetime import datetime, timezone
from uuid import uuid4

from fastapi import HTTPException
from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt
from pydantic import Field, ValidationError

from ..contracts import Contract
from ..metrics.compiler import InvalidMetricRequest
from ..metrics.contracts import EntityRequest, MetricRequest, MetricResult
from .contracts import Clarification, Interpretation, PlanDraft, Presentation, ResultReference
from .evidence import candidates, ground
from .provider import ProviderFailure


class GraphState(Contract):
    question: str
    previous_plan: dict | None = None
    plan: dict | None = None
    interpretation: dict | None = None
    catalogue_ids: list[str] = Field(default_factory=list)
    replies: list[str] = Field(default_factory=list)
    clarification: dict | None = None
    result: dict | None = None
    presentation: dict | None = None


def metric_summary(metric):
    return {key: getattr(metric,key) for key in
            ('id','version','family','label','aliases','population','periods','unit','dimensions','kind','examples')}


class ConversationGraph:
    def __init__(self, service, actor, run_id, token, checkpointer):
        self.service, self.actor, self.run_id, self.token = service, actor, run_id, token
        self.jobs, self.metrics = service.jobs, service.metrics
        builder = StateGraph(GraphState)
        for name in ('load_context','interpret','retrieve_catalogue','make_plan','clarify','resolve_entities',
                     'query_metric','validate_result','select_presentation','persist_answer'):
            builder.add_node(name, getattr(self,name))
        builder.add_edge(START,'load_context')
        builder.add_edge('load_context','interpret')
        builder.add_edge('interpret','retrieve_catalogue')
        builder.add_edge('retrieve_catalogue','make_plan')
        builder.add_conditional_edges('make_plan', lambda s: 'clarify' if s.clarification else 'resolve_entities')
        builder.add_conditional_edges('clarify', lambda s: 'resolve_entities' if s.plan and not s.clarification else 'make_plan')
        builder.add_conditional_edges('resolve_entities', lambda s: 'clarify' if s.clarification else 'query_metric')
        builder.add_edge('query_metric','validate_result')
        builder.add_edge('validate_result','select_presentation')
        builder.add_edge('select_presentation','persist_answer')
        builder.add_edge('persist_answer',END)
        self.graph = builder.compile(checkpointer=checkpointer)

    async def stage(self, name):
        # progress/budget validate the principal and execution token under lock.
        await self.jobs.progress(self.actor,self.run_id,self.token,name)

    def permitted(self):
        entries = []
        for metric in self.metrics.compiler.catalogue.metrics:
            try:
                self.metrics.require_access(metric.id,metric.version,metric.population)
                entries.append(metric)
            except HTTPException:
                continue
        return entries

    async def model(self, stage, payload, schema):
        for attempt in range(2):
            await self.jobs.budget(self.actor,self.run_id,self.token,'model')
            try:
                result, usage = await self.service.provider.generate(stage,payload,schema)
                result = schema.model_validate(result)
                await self.jobs.usage(self.actor,self.run_id,self.token,usage)
                return result
            except ProviderFailure as error:
                if not error.retryable or attempt == 1:
                    raise
                await asyncio.sleep(0.1)

    async def load_context(self, state):
        await self.stage('authorizing')
        if not self.permitted():
            raise HTTPException(503,'metric_not_certified')
        return {}

    async def interpret(self, state):
        await self.stage('interpreting')
        output = await self.model('interpret', {'question':state.question,'previous_plan':state.previous_plan,
            'catalogue':[metric_summary(m) for m in self.permitted()],
            'instructions':'Classify metric families only. Include a family even for an ambiguous or unsupported period; the next planning step checks details.'}, Interpretation)
        if not output.families:
            raise HTTPException(422,'unsupported_question')
        return {'interpretation':output.model_dump(mode='json')}

    async def retrieve_catalogue(self, state):
        await self.stage('retrieving_definitions')
        families = state.interpretation['families']
        return {'catalogue_ids':[m.id for m in self.permitted() if m.family in families]}

    async def make_plan(self, state):
        await self.stage('planning')
        permitted = {m.id:m for m in self.permitted() if m.id in state.catalogue_ids}
        entries = [{**metric_summary(m), 'date_basis':m.date_basis, 'status_filters':m.status_filters,
                    'limitations':self.metrics.compiler.catalogue.runtime_limitations(m)} for m in permitted.values()]
        draft = await self.model('plan', {'question':state.question,'replies':state.replies,
            'previous_plan':state.previous_plan,'catalogue':entries,
            'instructions':'Return a patch. Null inherits; [] clears lists. Select an explicit period for a new plan. Unsupported time windows require unsupported, never a substitute period.'},PlanDraft)
        if draft.action == 'unsupported':
            raise HTTPException(422,'unsupported_question')
        if draft.action == 'clarify':
            ids = draft.candidate_metrics or list(permitted)
            if not set(ids) <= permitted.keys():
                raise ProviderFailure('model_invalid_plan')
            reason = draft.reason if draft.reason != 'unsupported' else 'scope'
            return {'clarification':Clarification(id=uuid4(),reason=reason,
                question={'metric':'Which metric definition should I use?', 'period':'Which supported reporting period should I use?',
                          'scope':'Please specify the metric, reporting period and breakdown you need.'}[reason],
                options=[f'{permitted[i].label} ({i})' for i in ids] if reason == 'metric' else
                        sorted({period for m in permitted.values() for period in m.periods}) if reason == 'period' else []).model_dump(mode='json')}
        data = dict(state.previous_plan or {})
        changes = draft.patch.model_dump(mode='json',exclude_none=True,exclude={'clear_comparison'})
        data.update(changes)
        if draft.patch.clear_comparison:
            data['comparison'] = None
        metric = permitted.get(data.get('metric_id'))
        if metric is None or not data.get('period'):
            return {'clarification':Clarification(id=uuid4(),reason='scope',
                question='Please specify a supported metric definition and reporting period.').model_dump(mode='json')}
        # Version, population, completeness and permissions are server-owned.
        data.update(metric_version=metric.version,population=metric.population,include_total=True)
        try:
            plan = MetricRequest.model_validate(data)
            self.metrics.compiler.validate(plan)
            if plan.limit > self.service.settings.metric_max_rows:
                raise InvalidMetricRequest('limit')
        except (ValidationError, InvalidMetricRequest):
            raise ProviderFailure('model_invalid_plan') from None
        return {'plan':plan.model_dump(mode='json'),'clarification':None}

    async def clarify(self, state):
        # No side effects before interrupt: LangGraph replays this node on resume.
        if len(state.replies) >= 3:
            raise HTTPException(422,'clarification_limit')
        pending = Clarification.model_validate(state.clarification)
        reply = interrupt(pending.model_dump(mode='json'))
        if not isinstance(reply,str) or not reply.strip() or len(reply)>2000:
            raise HTTPException(422,'invalid_clarification_reply')
        await self.jobs.guard(self.actor,self.run_id,self.token)
        plan = state.plan
        if pending.reason == 'entity':
            choices = [item for item in pending.options if item.casefold() == reply.strip().casefold()]
            if len(choices) == 1:
                plan = MetricRequest.model_validate(plan).model_dump(mode='json')
                filters = plan['comparison']['filters'] if pending.comparison else plan['filters']
                for item in filters:
                    if item['dimension'] == pending.dimension:
                        item['values'] = [choices[0] if v == pending.value else v for v in item['values']]
                return {'plan':plan,'clarification':None,'replies':[*state.replies,reply]}
        # Replan from the last resolved scope, retaining every authenticated reply.
        return {'replies':[*state.replies,reply],'clarification':None,'plan':None}

    async def resolve_entities(self, state):
        await self.stage('resolving_entities')
        plan = MetricRequest.model_validate(state.plan)
        groups = [(False,plan.filters)]
        if plan.comparison and plan.comparison.filters is not None:
            groups.append((True,plan.comparison.filters))
        for comparison, filters in groups:
            for item in filters:
                for i,value in enumerate(item.values):
                    await self.jobs.budget(self.actor,self.run_id,self.token,'tool')
                    resolution = await self.metrics.resolve_entity(self.actor,EntityRequest(
                        metric_id=plan.metric_id,metric_version=plan.metric_version,population=plan.population,
                        dimension=item.dimension,query=value))
                    if resolution.status != 'resolved':
                        return {'plan':plan.model_dump(mode='json'),'clarification':Clarification(id=uuid4(),reason='entity',
                            question=f'Choose an exact {item.dimension} value for this filter.',options=resolution.candidates,
                            dimension=item.dimension,value=value,comparison=comparison).model_dump(mode='json')}
                    item.values[i] = resolution.candidates[0]
        return {'plan':plan.model_dump(mode='json'),'clarification':None}

    async def query_metric(self, state):
        await self.stage('querying')
        plan = MetricRequest.model_validate(state.plan)
        self.metrics.compiler.validate(plan)
        self.metrics.require_access(plan.metric_id,plan.metric_version,plan.population)
        await self.jobs.budget(self.actor,self.run_id,self.token,'tool')
        async with self.metrics.admitted(), asyncio.timeout(self.service.settings.metric_timeout_seconds):
            result = await self.metrics.query(self.actor,self.run_id,plan)
        await self.jobs.save_result(self.actor,self.run_id,self.token,result)
        return {'result':ResultReference(result_id=result.id,query_reference=result.provenance.query_reference,
            catalogue_sha256=result.provenance.catalogue_sha256).model_dump(mode='json')}

    async def result(self, state):
        reference = ResultReference.model_validate(state.result)
        result = MetricResult.model_validate(await self.service.store.result(self.actor,reference.result_id))
        if (result.run_id != self.run_id or result.provenance.query_reference != reference.query_reference
                or result.provenance.catalogue_sha256 != self.metrics.compiler.catalogue_hash
                or result.provenance.catalogue_sha256 != reference.catalogue_sha256
                or result.provenance.request.model_dump(mode='json') != state.plan):
            raise HTTPException(409,'result_evidence_changed')
        if (datetime.now(timezone.utc)-result.created_at).total_seconds() > self.service.settings.workflow_timeout_seconds:
            raise HTTPException(409,'result_expired')
        return result

    async def validate_result(self, state):
        await self.stage('validating_evidence')
        result = await self.result(state)
        if not candidates(result):
            raise HTTPException(422,'no_evidence')
        return {}

    async def select_presentation(self, state):
        await self.stage('selecting_presentation')
        result = await self.result(state)
        selection = await self.model('presentation', {
            'evidence':[claim.model_dump(mode='json') for claim in candidates(result)],
            'columns':[c.model_dump(mode='json') for c in result.provenance.columns],
            'row_count':len(result.rows),'truncated':result.provenance.truncated,
            'instructions':'Use table for nulls, multiple dimensions, limited data, or over 100 rows. Only month supports line. Select total and relevant comparison/contributor IDs.'},Presentation)
        # Fail closed before anything answer-like reaches the public stream.
        ground(result,selection,{},asks_for_cause=state.interpretation['asks_for_cause'])
        return {'presentation':selection.model_dump(mode='json')}

    async def persist_answer(self, state):
        await self.stage('grounding_answer')
        result = await self.result(state)
        previous = state.previous_plan or {}
        changes = {key:{'before':previous.get(key),'after':value} for key,value in state.plan.items()
                   if previous and previous.get(key) != value}
        answer = ground(result,Presentation.model_validate(state.presentation),changes,
                        asks_for_cause=state.interpretation['asks_for_cause'])
        if state.previous_plan:
            answer.notices.append('This follow-up was queried again using current permissions, source data and reporting boundaries.')
        await self.jobs.finish(self.actor,self.run_id,self.token,'completed',plan=state.plan,
                               answer=answer.model_dump(mode='json'))
        return {}
