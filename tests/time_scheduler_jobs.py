import asyncio
import os
import sys
from pathlib import Path
import time
from contextlib import asynccontextmanager

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.api.app import (
    _scheduled_delta_rehydrate,
    _scheduled_llm_backfill,
    _scheduled_monday_sync,
)
from src.tasks.pipeline import sync_projects_to_monday


@asynccontextmanager
async def _timer(label: str):
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed = time.perf_counter() - start
        print(f"{label}: {elapsed:.2f}s")


async def time_delta_rehydrate():
    async with _timer("Delta rehydrate (dry-run metrics)"):
        await _scheduled_delta_rehydrate()


async def time_llm_backfill():
    async with _timer("LLM backfill"):
        await _scheduled_llm_backfill()


async def time_monday_sync(dry_run: bool = True):
    async with _timer(f"Monday sync (dry_run={dry_run})"):
        await _scheduled_monday_sync() if dry_run else await _scheduled_monday_sync()


async def time_all():
    # Optional: force Monday sync into dry-run mode to avoid pushing updates
    os.environ.setdefault("MONDAY_SYNC_DRY_RUN", "true")

    await time_delta_rehydrate()
    await time_llm_backfill()
    await time_monday_sync(dry_run=(os.getenv("MONDAY_SYNC_DRY_RUN", "true").lower() == "true"))

    print("\nCheck recent entries in job_queue/webhook_events for extra context.")


if __name__ == "__main__":
    asyncio.run(time_all())