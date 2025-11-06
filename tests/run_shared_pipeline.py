# tests/run_shared_pipeline.py
import argparse
import asyncio
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.database.supabase_client import SupabaseClient  # noqa: E402
from src.tasks.pipeline import (  # noqa: E402
    backfill_llm,
    rehydrate_delta,
    rehydrate_recent,
    sync_projects_to_monday,
)

DEFAULT_PROJECT_IDS = [
    "5072605477",
    "5071760777",
    "5071584303",
    "5071568603",
    "5070355738",
    "5069732320",
    "5069390242",
    "5069272487",
    "5069268170",
    "5069137419",
    "5068931481",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("tests.pipeline")


def _count_projects_since(client: SupabaseClient, since_iso: str) -> int:
    resp = (
        client.client.table("projects")
        .select("monday_id", count="exact", head=True)
        .gte("date_created", since_iso)
        .execute()
    )
    return resp.count or 0


def _count_subitems_for_parents(client: SupabaseClient, parent_ids: Iterable[str]) -> int:
    resp = (
        client.client.table("subitems")
        .select("monday_id", count="exact", head=True)
        .in_("parent_monday_id", list(parent_ids))
        .execute()
    )
    return resp.count or 0


def _get_analysis_timestamp(client: SupabaseClient, project_id: str) -> Optional[str]:
    row = client.get_latest_analysis_result(project_id)
    if not row:
        return None
    return row.get("analysis_timestamp")


def _get_earliest_project_date(client: SupabaseClient, project_ids: Iterable[str]) -> str:
    resp = (
        client.client.table("projects")
        .select("monday_id,date_created")
        .in_("monday_id", list(project_ids))
        .execute()
    )
    rows = resp.data or []
    dates: List[datetime] = []
    for row in rows:
        raw = row.get("date_created")
        if not raw:
            continue
        try:
            dates.append(datetime.fromisoformat(str(raw)))
        except ValueError:
            pass
    if not dates:
        # fallback: go far into the past so everything is included
        return "2000-01-01"
    return min(dates).date().isoformat()


async def _run_rehydrate_delta(client: SupabaseClient) -> None:
    since_iso = (datetime.utcnow() - timedelta(days=1)).date().isoformat()
    before = await asyncio.to_thread(_count_projects_since, client, since_iso)
    await rehydrate_delta(days_back=1, chunk_size=200, logger=logger)
    after = await asyncio.to_thread(_count_projects_since, client, since_iso)
    logger.info("Delta rehydrate (1d window): before=%s after=%s delta=%s", before, after, after - before)

    await rehydrate_delta(days_back=1, chunk_size=200, logger=logger)
    after_repeat = await asyncio.to_thread(_count_projects_since, client, since_iso)
    logger.info("Delta rehydrate repeat (idempotency): previous=%s after=%s delta=%s", after, after_repeat, after_repeat - after)


async def _run_rehydrate_recent(client: SupabaseClient, project_ids: List[str]) -> None:
    since_iso = (datetime.utcnow() - timedelta(days=7)).date().isoformat()
    before_projects = await asyncio.to_thread(_count_projects_since, client, since_iso)
    before_subitems = await asyncio.to_thread(_count_subitems_for_parents, client, project_ids)

    await rehydrate_recent(days_back=7, chunk_size=200, logger=logger)

    after_projects = await asyncio.to_thread(_count_projects_since, client, since_iso)
    after_subitems = await asyncio.to_thread(_count_subitems_for_parents, client, project_ids)

    logger.info(
        "Recent rehydrate (7d window): projects before=%s after=%s delta=%s | subitems for test parents before=%s after=%s delta=%s",
        before_projects,
        after_projects,
        after_projects - before_projects,
        before_subitems,
        after_subitems,
        after_subitems - before_subitems,
    )

    await rehydrate_recent(days_back=7, chunk_size=200, logger=logger)
    subitems_repeat = await asyncio.to_thread(_count_subitems_for_parents, client, project_ids)
    logger.info("Recent rehydrate repeat (idempotency): subitems previous=%s after=%s delta=%s", after_subitems, subitems_repeat, subitems_repeat - after_subitems)


async def _run_backfill_llm(client: SupabaseClient, project_id: str) -> None:
    before_ts = await asyncio.to_thread(_get_analysis_timestamp, client, project_id)
    resume_after = str(int(project_id) - 1) if project_id.isdigit() else None

    stats = await asyncio.to_thread(
        backfill_llm,
        cutoff="2000-01-01",
        batch_size=10,
        limit=1,
        resume_after=resume_after,
        throttle=0.1,
        force=True,
        logger=logger,
    )
    after_ts = await asyncio.to_thread(_get_analysis_timestamp, client, project_id)

    logger.info("LLM backfill (forced) stats: %s", stats)
    logger.info("Analysis timestamp for %s: before=%s after=%s", project_id, before_ts, after_ts)

    stats_repeat = await asyncio.to_thread(
        backfill_llm,
        cutoff="2000-01-01",
        batch_size=10,
        limit=1,
        resume_after=resume_after,
        throttle=0.1,
        force=False,  # expect skip if analysis is still fresh
        logger=logger,
    )
    logger.info("LLM backfill repeat (skip-existing) stats: %s", stats_repeat)


async def _run_monday_sync(client: SupabaseClient, project_ids: List[str], dry_run: bool) -> None:
    since_iso = await asyncio.to_thread(_get_earliest_project_date, client, project_ids)
    stats = await asyncio.to_thread(
        sync_projects_to_monday,
        since=since_iso,
        batch_size=len(project_ids),
        limit=len(project_ids),
        sleep_interval=0.2,
        dry_run=dry_run,
        include_updates=True,
        throttle=True,
        logger=logger,
    )
    logger.info("Monday sync stats (dry_run=%s): %s", dry_run, stats)

    # Run again in dry-run mode to confirm idempotency / skip behaviour
    stats_repeat = await asyncio.to_thread(
        sync_projects_to_monday,
        since=since_iso,
        batch_size=len(project_ids),
        limit=len(project_ids),
        sleep_interval=0.2,
        dry_run=True,
        include_updates=True,
        throttle=True,
        logger=logger,
    )
    logger.info("Monday sync repeat (forced dry_run) stats: %s", stats_repeat)


async def run_shared_pipeline_tasks(project_ids: List[str], dry_run_sync: bool) -> None:
    logger.info("=== Shared pipeline helpers smoke run ===")
    client = SupabaseClient()
    logger.info("Supabase client initialised; targeting %s project(s)", len(project_ids))

    await _run_rehydrate_delta(client)
    await _run_rehydrate_recent(client, project_ids)
    await _run_backfill_llm(client, project_ids[0])
    await _run_monday_sync(client, project_ids, dry_run=dry_run_sync)

    logger.info("=== Shared pipeline helpers smoke run complete ===")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Exercise shared automation helpers and log before/after state.")
    parser.add_argument(
        "--project-id",
        action="append",
        dest="project_ids",
        help="Monday project ID to focus on (may be passed multiple times). Defaults to the test set.",
    )
    parser.add_argument(
        "--push",
        action="store_true",
        help="Run the Monday sync helper with dry_run=False (perform actual updates). Defaults to dry-run mode.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    ids = args.project_ids or DEFAULT_PROJECT_IDS
    asyncio.run(run_shared_pipeline_tasks(ids, dry_run_sync=not args.push))