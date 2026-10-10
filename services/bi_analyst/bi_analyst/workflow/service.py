"""Application-owned scheduling. Checkpoints are persistence, never a work queue."""
import asyncio
from contextlib import suppress
from importlib.metadata import version

from fastapi import HTTPException
from langgraph.types import Command
from langsmith import tracing_context

from .checkpoints import ScopedPostgresSaver
from .evidence import EvidenceError
from .graph import ConversationGraph
from .provider import GENERATION, MODEL, PROMPT_VERSION, ProviderFailure
from .store import WorkflowStore


class WorkflowService:
    def __init__(self, store, metrics, provider):
        self.store, self.metrics, self.provider = store, metrics, provider
        self.settings, self.jobs = store.db.settings, WorkflowStore(store)
        self.tasks = {}
        self.cancellations = set()
        self.admission = asyncio.Lock()
        self.versions = {'graph':'1.1.0','prompt':PROMPT_VERSION,'model':MODEL,'generation':GENERATION,
                         'catalogue':metrics.compiler.catalogue.version,'catalogue_sha256':metrics.compiler.catalogue_hash,
                         'langgraph':version('langgraph'),'checkpointer':version('langgraph-checkpoint-postgres')}

    def require_enabled(self):
        if not self.settings.workflow_enabled or not self.settings.analyst_enabled:
            raise HTTPException(503,'workflow_disabled')

    async def submit(self, actor, conversation_id, body):
        self.require_enabled()
        async with self.admission:
            if len(self.tasks) >= self.settings.workflow_concurrency:
                # Allow completed/active idempotent replays even at capacity.
                await self.store.conversation(actor,conversation_id)
                async with self.store.scoped(actor) as conn:
                    exists = await (await conn.execute("SELECT 1 FROM analyst_state.workflow_jobs WHERE conversation_id=%s AND idempotency_key=%s",
                                                       (conversation_id,body.idempotency_key))).fetchone()
                if not exists:
                    raise HTTPException(429,'workflow_capacity_reached')
            row, created = await self.jobs.submit(actor,conversation_id,body,self.versions)
            if created:
                token = await self.jobs.claim(actor,row['run_id'],self.versions)
                self._schedule(actor,row['run_id'],token)
        return self.jobs.public(await self.jobs.get(actor,row['run_id']))

    async def resume(self, actor, run_id, reply):
        self.require_enabled()
        await self.jobs.expire(actor)
        await self.jobs.get(actor,run_id)
        async with self.admission:
            if run_id not in self.tasks and len(self.tasks) >= self.settings.workflow_concurrency:
                raise HTTPException(429,'workflow_capacity_reached')
            token = await self.jobs.claim(actor,run_id,self.versions,reply)
            if token:
                self._schedule(actor,run_id,token,reply.answer)
        return self.jobs.public(await self.jobs.get(actor,run_id))

    def _schedule(self, actor, run_id, token, reply=None):
        task = asyncio.create_task(self._drive(actor,run_id,token,reply))
        self.tasks[run_id] = task
        def done(completed):
            if self.tasks.get(run_id) is completed:
                self.tasks.pop(run_id,None)
            # Retrieve exceptions without logging prompts, keys or query data.
            if not completed.cancelled():
                completed.exception()
        task.add_done_callback(done)

    async def _drive(self, actor, run_id, token, reply):
        with self.store.db.telemetry.operation('workflow','run_workflow',run_id=run_id,attempt=1) as operation:
            await self._execute(actor,run_id,token,reply,operation)

    async def _execute(self, actor, run_id, token, reply, operation):
        outcome, error_code = 'failed', None
        work = None
        try:
            row = await self.jobs.guard(actor,run_id,token)
            run = await self.store.run(actor,run_id)
            graph = ConversationGraph(self,actor,run_id,token,ScopedPostgresSaver(self.store,actor,run_id)).graph
            config = {'configurable':{'thread_id':str(run_id)},'recursion_limit':40,'callbacks':[]}
            data = Command(resume=reply) if reply is not None else {
                'question':run['question'],'previous_plan':row['plan'] if row['follow_up_to'] else None}
            # Explicitly suppress inherited LangSmith tracing: raw graph state is private.
            with tracing_context(enabled=False):
                async with asyncio.timeout(row['remaining_seconds']):
                    work = asyncio.create_task(graph.ainvoke(data,config,durability='sync'))
                    while not work.done():
                        await asyncio.wait({work},timeout=0.2)
                        if not work.done():
                            await self.jobs.guard(actor,run_id,token)
                    output = await work
            if output.get('__interrupt__'):
                pending = output['__interrupt__'][0].value
                await self.jobs.finish(actor,run_id,token,'awaiting_clarification',
                                       clarification=pending,plan=output.get('plan'))
                outcome = 'clarification'
            else:
                outcome = 'success'
        except asyncio.CancelledError:
            outcome = 'cancelled' if run_id in self.cancellations else 'interrupted'
            error_code = 'execution_cancelled' if outcome == 'cancelled' else 'execution_interrupted'
            await self._fail(actor,run_id,token,'interrupted','execution_interrupted')
            raise
        except TimeoutError:
            outcome = 'timeout'
            error_code = 'run_timeout'
            await self._fail(actor,run_id,token,'failed','run_timeout')
        except ProviderFailure as error:
            outcome = 'timeout' if error.code == 'model_timeout' else 'failed'
            error_code = error.code
            await self._fail(actor,run_id,token,'failed',error.code)
        except EvidenceError:
            error_code = 'invalid_evidence'
            await self._fail(actor,run_id,token,'failed','invalid_evidence')
        except HTTPException as error:
            # Only fixed internal codes; never serialize arbitrary exception details.
            code = error.detail if type(error.detail) is str and error.detail in {'unsupported_question','model_invalid_plan','clarification_limit',
                'run_budget_exceeded','permissions_changed','access_denied','metric_not_certified','result_expired',
                'result_evidence_changed','workflow_version_changed'} else 'run_unavailable'
            error_code = code
            await self._fail(actor,run_id,token,'failed',code)
        except Exception:
            error_code = 'workflow_failed'
            await self._fail(actor,run_id,token,'failed','workflow_failed')
        finally:
            if work is not None:
                if not work.done():
                    work.cancel()
                await asyncio.gather(work,return_exceptions=True)
            operation.set_outcome(outcome,error_code)
            self.cancellations.discard(run_id)

    async def _fail(self, actor, run_id, token, status, code):
        # Revoked principals cannot write state; expiration handles any abandoned lease
        # after re-authorisation. An owner cancellation/terminal outcome always wins.
        with suppress(Exception):
            await self.jobs.finish(actor,run_id,token,status,error=code)

    async def cancel(self, actor, run_id):
        cancelled = await self.jobs.cancel(actor,run_id)
        if cancelled and run_id in self.tasks:
            self.cancellations.add(run_id)
            self.tasks[run_id].cancel()
        return cancelled

    async def close(self):
        tasks = list(self.tasks.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks,return_exceptions=True)
