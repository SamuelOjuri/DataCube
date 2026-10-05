"""Durable post-webhook jobs. Enqueue succeeds only after database persistence."""
from __future__ import annotations
import asyncio
from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4
from .durable_worker import DurableWorker
from . import worker_store as store


@dataclass
class QueueTask:
    name: str
    project_id: str
    metadata: dict[str, Any] = field(default_factory=dict)
    job_id: str = field(default_factory=lambda: str(uuid4()))


class TaskQueue(DurableWorker):
    def __init__(self):
        super().__init__('general', self.handle)

    def _enqueue(self, name, project_id, metadata):
        origin = store.current_job.get()
        key = f'{origin}:{name}:{project_id}' if origin else None
        with store.connect() as connection:
            return store.enqueue(connection, name, project_id, metadata, key=key)

    def enqueue_rehydrate(self, project_id, *, source=None):
        return self._enqueue('rehydrate_and_analyze', project_id, {'source': source})

    def enqueue_push_to_monday(self, project_id, *, reason=None):
        return self._enqueue('push_to_monday', project_id, {'reason': reason})

    def handle(self, job):
        task = QueueTask(job['job_type'], job['project_id'], job['payload'] or {}, str(job['id']))
        if task.name == 'rehydrate_and_analyze':
            return asyncio.run(self._handle_rehydrate(task))
        if task.name == 'push_to_monday':
            return self._handle_push(task)
        raise ValueError('Unknown durable job type')

    async def _handle_rehydrate(self, task):
        from .analysis_service import AnalysisService
        from ..tasks.pipeline import rehydrate_projects_by_ids
        await rehydrate_projects_by_ids([task.project_id])
        result = AnalysisService().analyze_and_store(task.project_id)
        if not result.get('success'):
            raise RuntimeError('Analysis failed')
        # Completion and the follow-up enqueue commit in one transaction.
        return [dict(name='push_to_monday', project_id=task.project_id,
                     payload={'reason': 'analysis_update'}, key=task.job_id + ':push')]

    def _handle_push(self, task):
        from .analysis_service import AnalysisService
        from .monday_update_service import MondayUpdateService
        payload = AnalysisService().db.get_latest_analysis_result(task.project_id)
        if not payload:
            raise ValueError('Analysis payload missing')
        # Assigning columns is replayable. Creating timeline messages has no
        # server-side idempotency key and can duplicate after a lost response.
        result = MondayUpdateService().sync_project(task.project_id, analysis=payload, include_update=False)
        if not result.get('success'):
            raise RuntimeError('Monday column sync failed')
        return []

    async def join(self):
        def remaining():
            with store.connect() as connection:
                return connection.execute("SELECT count(*) AS n FROM public.job_queue WHERE queue_name='general' "
                                          "AND status IN ('queued','retry','running')").fetchone()['n']
        while await asyncio.to_thread(remaining):
            await asyncio.sleep(1)


_global_queue = None


def get_task_queue():
    global _global_queue
    if _global_queue is None:
        _global_queue = TaskQueue()
    return _global_queue
