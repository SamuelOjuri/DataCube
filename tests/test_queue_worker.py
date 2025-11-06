import asyncio
import logging
import sys
from pathlib import Path
from typing import Optional

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.services.queue_worker import get_task_queue
from src.database.supabase_client import SupabaseClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
LOG = logging.getLogger("tests.queue-worker")

VALID_PROJECT_ID = "5072605477"
INVALID_PROJECT_ID = "9999999999"


async def _wait_for_job(job_id: str, *, timeout: float = 30.0) -> Optional[dict]:
    """Poll Supabase job_queue until the job reaches a terminal state."""
    client = SupabaseClient().client
    deadline = asyncio.get_running_loop().time() + timeout
    terminal = {"completed", "failed"}
    while asyncio.get_running_loop().time() < deadline:
        res = (
            client.table("job_queue")
            .select("*")
            .eq("id", job_id)
            .order("updated_at", desc=True)
            .limit(1)
            .execute()
        )
        row = (res.data or [None])[0]
        if row and row.get("status") in terminal:
            return row
        await asyncio.sleep(0.5)
    return None


async def _run_success_flow() -> None:
    queue = get_task_queue()
    queue.start()

    queue.enqueue_rehydrate(VALID_PROJECT_ID, source="manual-test-success")
    LOG.info("Enqueued rehydrate job for %s", VALID_PROJECT_ID)

    # Wait for worker to drain the queue
    await asyncio.sleep(0.5)
    await queue.join()

    # Fetch latest job record
    client = SupabaseClient().client
    res = (
        client.table("job_queue")
        .select("*")
        .eq("project_id", VALID_PROJECT_ID)
        .order("updated_at", desc=True)
        .limit(1)
        .execute()
    )
    row = (res.data or [None])[0]
    if not row:
        LOG.error("No job_queue row recorded for %s", VALID_PROJECT_ID)
        return

    LOG.info(
        "Job %s status=%s attempts=%s payload=%s",
        row["id"],
        row["status"],
        row.get("attempts"),
        row.get("payload"),
    )

    if row["status"] != "completed":
        LOG.warning("Expected completed status but saw %s", row["status"])
    else:
        LOG.info("Queue worker completed rehydrate+analysis flow successfully.")


async def _run_failure_flow() -> None:
    queue = get_task_queue()
    queue.start()

    queue.enqueue_rehydrate(INVALID_PROJECT_ID, source="manual-test-failure")
    LOG.info("Enqueued rehydrate job for bogus ID %s", INVALID_PROJECT_ID)

    await asyncio.sleep(0.5)
    await queue.join()

    # Locate the job in job_queue and confirm it failed
    client = SupabaseClient().client
    res = (
        client.table("job_queue")
        .select("*")
        .eq("project_id", INVALID_PROJECT_ID)
        .order("updated_at", desc=True)
        .limit(1)
        .execute()
    )
    row = (res.data or [None])[0]
    if not row:
        LOG.error("No job_queue row recorded for bogus ID %s", INVALID_PROJECT_ID)
        return

    LOG.info(
        "Job %s status=%s detail=%s",
        row["id"],
        row["status"],
        row.get("detail"),
    )
    if row["status"] != "failed":
        LOG.warning("Expected failed status but saw %s", row["status"])
    else:
        LOG.info("Queue worker marked bogus job as failed as expected.")


async def main() -> None:
    await _run_success_flow()
    await _run_failure_flow()


if __name__ == "__main__":
    asyncio.run(main())