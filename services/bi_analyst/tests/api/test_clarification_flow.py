"""Clarified scope must survive replanning and reach query generation."""
import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from bi_analyst.metrics.compiler import Compiler
from bi_analyst.metrics.contracts import MetricRequest
from bi_analyst.workflow.contracts import Clarification, ClarificationResponse, PlanDraft
from bi_analyst.workflow.graph import ConversationGraph, GraphState
from bi_analyst.workflow.provider import ProviderFailure
from test_settings import config


def workflow(*drafts):
    compiler = Compiler()
    instance = object.__new__(ConversationGraph)
    instance.actor = instance.run_id = instance.token = None
    instance.jobs = SimpleNamespace(progress=AsyncMock(), budget=AsyncMock(), usage=AsyncMock(), guard=AsyncMock())
    instance.metrics = SimpleNamespace(compiler=compiler, require_access=compiler.metric)
    provider = SimpleNamespace(generate=AsyncMock(side_effect=[(PlanDraft.model_validate(d), {}) for d in drafts]))
    instance.service = SimpleNamespace(provider=provider, settings=config())
    return instance


def answered(reason, option, *, options=None):
    return ClarificationResponse(clarification=Clarification(id=uuid4(), reason=reason,
        question='Resolve ' + reason, options=options or [option]), answer=option)


def test_period_choices_are_supported_by_the_resolved_metric_and_partial_scope_is_retained():
    patch = {'metric_id':'invoice_monthly_actual', 'grain':'month', 'dimensions':['category'],
             'filters':[{'dimension':'category', 'values':['A']}], 'clear_comparison':True}
    graph = workflow({'action':'clarify', 'reason':'period', 'patch':patch, 'asks_for_cause':True})
    previous = {'metric_id':'invoice_monthly_actual', 'period':'last_month',
                'comparison':{'period':'previous_month'}}
    result = asyncio.run(graph.make_plan(GraphState(question='Why did revenue change?', previous_plan=previous)))
    assert set(result['clarification']['options']) == set(graph.metrics.compiler.metrics['invoice_monthly_actual'].periods)
    assert 'two_year_cohort' not in result['clarification']['options']
    assert result['planning_scope']['comparison'] is None
    assert result['planning_scope']['grain'] == 'month'
    assert result['planning_scope']['filters'][0]['values'] == ['A']
    assert result['asks_for_cause'] is True
    assert previous['comparison'] == {'period':'previous_month'}


@pytest.mark.parametrize('reason', ['metric', 'period'])
@pytest.mark.parametrize('repeats', [False, True])
def test_answered_questions_receive_one_bounded_repair_instead_of_another_interrupt(reason, repeats):
    compiler = Compiler()
    option = f'{compiler.metrics["conversion_two_year"].label} (conversion_two_year)'
    response = answered('metric', option) if reason == 'metric' else answered('period', 'last_month')
    metric = 'conversion_two_year' if reason == 'metric' else 'invoice_monthly_actual'
    repeat = {'action':'clarify', 'reason':reason}
    graph = workflow(repeat, repeat if repeats else {'action':'plan'})
    state = GraphState(question='Show the selected measure by type.', clarification_responses=[response],
                       planning_scope={'metric_id':metric, 'dimensions':['type']})
    if repeats:
        with pytest.raises(ProviderFailure, match='model_invalid_plan'):
            asyncio.run(graph.make_plan(state))
    else:
        result = asyncio.run(graph.make_plan(state))
        assert result['clarification'] is None
        assert result['plan']['metric_id'] == metric and result['plan']['dimensions'] == ['type']
        assert result['plan']['period'] == ('two_year_cohort' if reason == 'metric' else 'last_month')
    calls = graph.service.provider.generate.await_args_list
    assert len(calls) == 2 and 'planning_feedback' in calls[-1].args[1]


def test_exact_period_choice_controls_the_compiled_query_even_if_the_model_returns_another_period():
    graph = workflow({'action':'plan', 'patch':{'period':'previous_month'}})
    state = GraphState(question='Show revenue.', planning_scope={'metric_id':'invoice_monthly_actual'},
        clarification_responses=[answered('period', 'last_month')])
    result = asyncio.run(graph.make_plan(state))
    plan = MetricRequest.model_validate(result['plan'])
    compiled = graph.metrics.compiler.compile(plan, now=datetime(2026, 10, 26, tzinfo=timezone.utc),
                                            business_timezone='Europe/London')
    assert plan.period == 'last_month'
    assert compiled.period.start_date.isoformat() == '2026-09-01'
    assert compiled.period.end_date_exclusive.isoformat() == '2026-10-01'


def test_a_later_free_text_correction_can_change_an_earlier_exact_metric_selection():
    compiler = Compiler()
    option = f'{compiler.metrics["conversion_two_year"].label} (conversion_two_year)'
    correction = answered('scope', 'Actually use five-year closed-only conversion, keeping the type breakdown.', options=['Other'])
    graph = workflow({'action':'plan', 'patch':{'metric_id':'conversion_closed_five_year', 'period':'five_year_cohort'}})
    state = GraphState(question='Show conversion by type.',
        planning_scope={'metric_id':'conversion_two_year', 'period':'two_year_cohort', 'dimensions':['type']},
        clarification_responses=[answered('metric', option), correction])
    result = asyncio.run(graph.make_plan(state))
    assert result['plan']['metric_id'] == 'conversion_closed_five_year'
    assert result['plan']['dimensions'] == ['type']
    payload = graph.service.provider.generate.await_args.args[1]
    assert payload['selected_metric_id'] is None and len(payload['catalogue']) == len(compiler.metrics)


def test_period_selected_before_metric_is_not_overwritten_by_replanning():
    compiler = Compiler()
    option = f'{compiler.metrics["invoice_monthly_actual"].label} (invoice_monthly_actual)'
    graph = workflow({'action':'plan', 'patch':{'period':'previous_month'}})
    state = GraphState(question='Show revenue.', planning_scope={'period':'last_month'},
        clarification_responses=[answered('period','last_month'), answered('metric',option)])
    result = asyncio.run(graph.make_plan(state))
    assert result['plan']['metric_id'] == 'invoice_monthly_actual'
    assert result['plan']['period'] == 'last_month'


def test_free_text_entity_reply_replans_from_the_current_scope_including_the_comparison(monkeypatch):
    graph = workflow({'action':'plan', 'patch':{'filters':[{'dimension':'category', 'values':['Roofing Supplies']}]}})
    metric = graph.metrics.compiler.metrics['order_parent_value']
    plan = MetricRequest(metric_id=metric.id, metric_version=metric.version, population=metric.population,
        period='all_stored', dimensions=['category'], filters=[{'dimension':'category', 'values':['roof']}],
        comparison={'period':'all_stored', 'filters':[{'dimension':'category', 'values':['A']}]}).model_dump(mode='json')
    pending = Clarification(id=uuid4(), reason='entity', question='Which category?',
        dimension='category', value='roof', options=['Roofing', 'Roofing Supplies'])
    monkeypatch.setattr('bi_analyst.workflow.graph.interrupt', lambda _: 'Use Roofing Supplies please')
    state = GraphState(question='Order value by category versus A', plan=plan, clarification=pending.model_dump(mode='json'))
    resumed = GraphState.model_validate({**state.model_dump(), **asyncio.run(graph.clarify(state))})
    result = asyncio.run(graph.make_plan(resumed))
    assert result['plan']['metric_id'] == metric.id and result['plan']['dimensions'] == ['category']
    assert result['plan']['comparison'] == plan['comparison']
    assert result['plan']['filters'][0]['values'] == ['Roofing Supplies']
    assert state.plan['filters'][0]['values'] == ['roof']


def test_literal_entity_choice_distinguishes_case_only_labels_and_changes_only_the_baseline(monkeypatch):
    graph = workflow()
    metric = graph.metrics.compiler.metrics['order_parent_value']
    plan = MetricRequest(metric_id=metric.id, metric_version=metric.version, population=metric.population,
        period='all_stored', filters=[{'dimension':'category', 'values':['A']}],
        comparison={'period':'all_stored', 'filters':[{'dimension':'category', 'values':['roo']}]}).model_dump(mode='json')
    pending = Clarification(id=uuid4(), reason='entity', question='Which category?', dimension='category',
                            value='roo', comparison=True, options=['Roofing', 'roofing'])
    monkeypatch.setattr('bi_analyst.workflow.graph.interrupt', lambda _: 'roofing')
    result = asyncio.run(graph.clarify(GraphState(question='Compare order values', plan=plan,
        clarification=pending.model_dump(mode='json'))))
    assert result['clarification'] is None and result['plan']['filters'] == plan['filters']
    assert result['plan']['comparison']['filters'][0]['values'] == ['roofing']
    assert graph.service.provider.generate.await_count == 0
