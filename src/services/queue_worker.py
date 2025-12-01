"""Asynchronous job queue for post-webhook processing."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from uuid import uuid4
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from ..database.supabase_client import SupabaseClient
from ..services.analysis_service import AnalysisService
from ..services.ml_analysis_service import MLAnalysisService
from ..services.monday_update_service import MondayUpdateService
from ..tasks.pipeline import rehydrate_projects_by_ids


logger = logging.getLogger(__name__)


@dataclass
class QueueTask:
    name: str
    project_id: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    job_id: str = field(default_factory=lambda: str(uuid4()))


class TaskQueue:
    """Minimal in-process task queue backed by asyncio."""

    def __init__(self) -> None:
        self._queue: asyncio.Queue[Optional[QueueTask]] = asyncio.Queue()
        self._worker_task: Optional[asyncio.Task[None]] = None
        self._running = False
        self.logger = logging.getLogger(f"{__name__}.worker")
        self._supabase = SupabaseClient()

    def start(self) -> None:
        if self._worker_task and not self._worker_task.done():
            return
        self._running = True
        self._worker_task = asyncio.create_task(self._run(), name="datacube-task-worker")
        self.logger.info("Task queue worker started")

    async def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        await self._queue.put(None)
        if self._worker_task:
            await self._worker_task
        self.logger.info("Task queue worker stopped")

    async def join(self) -> None:
        await self._queue.join()

    def enqueue_rehydrate(self, project_id: str, *, source: Optional[str] = None) -> None:
        self.start()
        task = QueueTask(
            name="rehydrate_and_analyze",
            project_id=project_id,
            metadata={"source": source},
        )
        self._queue.put_nowait(task)
        self._record_status(task, "queued")

    def enqueue_push_to_monday(self, project_id: str, *, reason: Optional[str] = None) -> None:
        self.start()
        task = QueueTask(name="push_to_monday", project_id=project_id, metadata={"reason": reason})
        self._queue.put_nowait(task)
        self._record_status(task, "queued")

    async def _run(self) -> None:
        while True:
            task = await self._queue.get()
            if task is None:
                self._queue.task_done()
                break

            try:
                task.metadata["attempts"] = int(task.metadata.get("attempts", 0)) + 1
                self._record_status(task, "running")
                if task.name == "rehydrate_and_analyze":
                    await self._handle_rehydrate(task)
                elif task.name == "push_to_monday":
                    await self._handle_push(task)
                else:
                    self.logger.warning("Unknown task type %s", task.name)
                    self._record_status(task, "failed", detail="unknown task type")
                    continue
                self._record_status(task, "completed")
            except Exception:  # noqa: BLE001
                self.logger.exception("Task %s for project %s failed", task.name, task.project_id)
                self._record_status(task, "failed")
            finally:
                self._queue.task_done()

    async def _handle_rehydrate(self, task: QueueTask) -> None:
        project_id = task.project_id
        self.logger.info("Rehydrate+analyse job started | project=%s", project_id)

        await rehydrate_projects_by_ids([project_id], logger=self.logger)

        analysis = AnalysisService()
        result = analysis.analyze_and_store(project_id)
        if not result.get("success"):
            raise RuntimeError(f"Analysis failed for project {project_id}: {result.get('error')}")

        self.logger.info("Analysis stored | project=%s", project_id)

        # Shadow Mode: Run ML Analysis
        try:
            ml_service = MLAnalysisService()
            ml_service.analyze_and_store(project_id)
        except Exception as e:
            self.logger.warning("Shadow ML analysis failed for %s: %s", project_id, e)

        self.enqueue_push_to_monday(project_id, reason="analysis_update")

    async def _handle_push(self, task: QueueTask) -> None:
        project_id = task.project_id
        self.logger.info("Monday push job started | project=%s", project_id)

        service = MondayUpdateService()
        analysis_service = AnalysisService()
        payload = analysis_service.db.get_latest_analysis_result(project_id)
        if not payload:
            self.logger.warning("No analysis payload to push for project %s", project_id)
            return

        result = service.sync_project(project_id=project_id, analysis=payload, include_update=True)
        if not result.get("success"):
            raise RuntimeError(f"Monday sync failed for project {project_id}: {result.get('error')}")

        self.logger.info("Monday sync complete | project=%s", project_id)

    def _record_status(self, task: QueueTask, status: str, detail: Optional[str] = None) -> None:
        payload: Dict[str, Any] = {
            "id": task.job_id,
            "job_type": task.name,
            "project_id": task.project_id,
            "status": status,
            "attempts": int(task.metadata.get("attempts", 0)),
            "payload": task.metadata,
            "updated_at": datetime.utcnow().isoformat(),
        }
        if status == "queued":
            payload["created_at"] = datetime.utcnow().isoformat()
        if detail:
            payload["detail"] = detail

        try:
            self._supabase.client.table("job_queue").upsert(payload, on_conflict="id").execute()
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("Failed to persist job status for %s: %s", task.job_id, exc)


_global_queue: Optional[TaskQueue] = None


def get_task_queue() -> TaskQueue:
    global _global_queue
    if _global_queue is None:
        _global_queue = TaskQueue()
    return _global_queue


__all__ = [
    "QueueTask",
    "TaskQueue",
    "get_task_queue",
]

