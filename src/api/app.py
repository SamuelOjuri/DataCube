import asyncio
import logging
import time
import os
from datetime import datetime, timedelta

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI

from .routes.analysis import router as analysis_router
from ..config import PARENT_BOARD_ID
from ..database.supabase_client import SupabaseClient
from ..services.queue_worker import get_task_queue
from ..tasks.pipeline import backfill_llm, rehydrate_delta, rehydrate_recent, sync_projects_to_monday
from ..tasks.postgres_maintenance import refresh_conversion_views

app = FastAPI()
app.include_router(analysis_router)  # exposes /analysis/{monday_id}/run

_scheduler = AsyncIOScheduler()


async def _scheduled_delta_rehydrate() -> None:
    log = logging.getLogger("scheduler.delta_rehydrate")
    try:
        await rehydrate_delta(days_back=1, chunk_size=200, logger=log)
    except Exception:  # noqa: BLE001
        log.exception("Delta rehydrate job failed")


async def _scheduled_recent_rehydrate() -> None:
    log = logging.getLogger("scheduler.recent_rehydrate")
    try:
        await rehydrate_recent(days_back=2, chunk_size=200, logger=log)
    except Exception:
        log.exception("Recent rehydrate job failed")


async def _scheduled_llm_backfill() -> None:
    log = logging.getLogger("scheduler.llm_backfill")
    loop = asyncio.get_running_loop()
    cutoff = (datetime.utcnow() - timedelta(days=7)).date().isoformat()
    try:
        await loop.run_in_executor(
            None,
            lambda: backfill_llm(
                cutoff=cutoff,
                batch_size=200,
                throttle=0.35,
                lookback_days=None,
                skip_existing_after=None,
                force=True,
                logger=log,
            ),
        )
    except Exception:  # noqa: BLE001
        log.exception("LLM backfill job failed")


async def _scheduled_monday_sync() -> None:
    log = logging.getLogger("scheduler.monday_sync")
    loop = asyncio.get_running_loop()
    supabase = SupabaseClient()
    dry_run_env = os.getenv("MONDAY_SYNC_DRY_RUN", "").lower() in {"1", "true", "yes"}
    default_cutoff = (datetime.utcnow() - timedelta(days=2)).isoformat()
    try:
        last_completed = (
            supabase.client.table("sync_log")
            .select("metadata, completed_at")
            .eq("board_id", PARENT_BOARD_ID)
            .eq("board_name", "Scheduled Monday Sync")
            .eq("status", "completed")
            .order("completed_at", desc=True)
            .limit(1)
            .execute()
        )
        last_meta = (last_completed.data[0].get("metadata") if last_completed.data else {}) or {}
        updated_after = last_meta.get("last_analysis_timestamp") or default_cutoff
    except Exception as exc:  # noqa: BLE001
        log.warning("Could not read previous Monday sync metadata: %s", exc)
        updated_after = default_cutoff

    seen: set[str] = set()
    project_rows: list[tuple[str, str]] = []
    cursor = None
    batch_size = 200
    try:
        while True:
            query = (
                supabase.client.table("analysis_results")
                .select("project_id, analysis_timestamp")
                .order("analysis_timestamp", desc=False)
                .limit(batch_size)
            )
            if cursor:
                query = query.gt("analysis_timestamp", cursor)
            else:
                query = query.gte("analysis_timestamp", updated_after)
            res = query.execute()
            rows = res.data or []
            if not rows:
                break
            cursor = rows[-1].get("analysis_timestamp")
            for row in rows:
                pid = str(row.get("project_id") or "").strip()
                ts = row.get("analysis_timestamp")
                if not pid or pid in seen or not ts:
                    continue
                seen.add(pid)
                project_rows.append((pid, ts))
                if len(project_rows) >= 1000:
                    log.warning(
                        "Truncating Monday sync batch to first %s projects updated after %s",
                        len(project_rows),
                        updated_after,
                    )
                    break
            if len(project_rows) >= 1000:
                break
    except Exception:
        log.exception("Failed to load projects with updated analysis results")
        return

    if not project_rows:
        log.info("No analysis updates since %s; skipping Monday sync", updated_after)
        return

    project_ids = [pid for pid, _ in project_rows]
    last_processed_ts = max((ts for _, ts in project_rows if ts), default=updated_after)
    log_id = supabase.log_sync_operation("scheduler", PARENT_BOARD_ID, "Scheduled Monday Sync")

    try:
        stats = await loop.run_in_executor(
            None,
            lambda: sync_projects_to_monday(
                project_ids=project_ids,
                batch_size=150,
                sleep_interval=0.2,
                dry_run=dry_run_env,
                include_updates=True,
                throttle=True,
                logger=log,
            ),
        )
        supabase.update_sync_log(
            log_id,
            "completed",
            stats={
                "projects_considered": len(project_ids),
                "last_analysis_timestamp": last_processed_ts,
                "dry_run": dry_run_env,
                "stats": stats,
            },
        )
    except Exception as exc:  # noqa: BLE001
        supabase.update_sync_log(
            log_id,
            "failed",
            stats={"projects_considered": len(project_ids)},
            error=str(exc),
        )
        log.exception("Monday sync job failed")

async def _scheduled_refresh_conversion_views() -> None:
    log = logging.getLogger("scheduler.refresh_conversion_views")
    loop = asyncio.get_running_loop()
    started = time.perf_counter()
    try:
        await loop.run_in_executor(
            None,
            lambda: refresh_conversion_views(logger=log, concurrently=True),
        )
        log.info("Materialized view refresh job finished (elapsed=%.2fs)", time.perf_counter() - started)
    except Exception:
        log.exception("Materialized view refresh job failed")


@app.on_event("startup")
async def _startup() -> None:
    logging.getLogger("apscheduler").setLevel(logging.INFO)
    get_task_queue().start()

    if not _scheduler.running:

        _scheduler.add_job(_scheduled_delta_rehydrate, "interval", hours=1, id="delta_rehydrate")
        _scheduler.add_job(_scheduled_llm_backfill, "cron", hour=2, minute=15, id="llm_backfill")
        _scheduler.add_job(_scheduled_monday_sync, "interval", minutes=10, id="monday_sync")
        _scheduler.add_job(_scheduled_recent_rehydrate, "interval", hours=6, id="recent_rehydrate")
        _scheduler.add_job(
            _scheduled_refresh_conversion_views,
            "interval",
            minutes=30,
            id="refresh_conversion_views",
            misfire_grace_time=180,
            max_instances=1,
        )

        _scheduler.start()


@app.on_event("shutdown")
async def _shutdown() -> None:
    if _scheduler.running:
        _scheduler.shutdown(wait=False)
    await get_task_queue().stop()



