"""
Webhook integration smoke tests.

Prereqs:
- FastAPI app running locally (uvicorn src.api.app:app --reload)
- WEBHOOK_SECRET set to the same value the server uses
- Supabase + Monday credentials in .env (as for other tests)

This script:
  * Fires a parent-board column change and waits for the push job to finish
  * Replays the same payload to ensure duplicate detection works
  * Fires a subitem column change and waits for rehydrate + push jobs
  * Polls Supabase webhook_events and job_queue tables for confirmation
"""

import asyncio
import hashlib
import hmac
import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional
from uuid import uuid4

import httpx

from src.config import (
    PARENT_BOARD_ID,
    SUBITEM_BOARD_ID,
    PARENT_COLUMNS,
    SUBITEM_COLUMNS,
    WEBHOOK_SECRET,
)
from src.database.supabase_client import SupabaseClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOG = logging.getLogger("tests.webhooks")

BASE_URL = os.getenv("WEBHOOK_BASE_URL", "http://127.0.0.1:8000")
WEBHOOK_ENDPOINT = f"{BASE_URL.rstrip('/')}/webhooks/monday"

# Test fixtures (adjust to match real data in your Monday/Supabase environment)
PARENT_PROJECT_ID = "5072605477"
SUBITEM_ID = "5073729217"
SUBITEM_PARENT_ID = "5072605477"  # parent of the subitem above

PARENT_COLUMN_ID = PARENT_COLUMNS["pipeline_stage"]
SUBITEM_COLUMN_ID = SUBITEM_COLUMNS["new_enquiry_value"]


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sign_payload(body: bytes) -> str:
    if not WEBHOOK_SECRET:
        raise ValueError("WEBHOOK_SECRET is not configured; cannot sign webhook payloads.")
    digest = hmac.new(WEBHOOK_SECRET.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


async def _post_webhook(client: httpx.AsyncClient, payload: dict) -> httpx.Response:
    body = json.dumps(payload).encode("utf-8")
    signature = _sign_payload(body)
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {signature}",
    }
    return await client.post(WEBHOOK_ENDPOINT, content=body, headers=headers)


async def _fetch_job(
    supabase: SupabaseClient,
    *,
    project_id: str,
    since_iso: str,
    job_type: Optional[str] = None,
) -> Optional[dict]:
    def _query():
        query = (
            supabase.client.table("job_queue")
            .select("*")
            .eq("project_id", project_id)
            .gte("created_at", since_iso)
            .order("created_at", desc=True)
            .limit(1)
        )
        if job_type:
            query = query.eq("job_type", job_type)
        return (query.execute().data or [None])[0]

    return await asyncio.to_thread(_query)


async def _wait_for_job(
    supabase: SupabaseClient,
    *,
    project_id: str,
    since_iso: str,
    job_type: Optional[str] = None,
    timeout: float = 90.0,
) -> Optional[dict]:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        row = await _fetch_job(supabase, project_id=project_id, since_iso=since_iso, job_type=job_type)
        if row and row.get("status") in {"completed", "failed"}:
            return row
        await asyncio.sleep(1.0)
    return None


async def _fetch_webhook_event(supabase: SupabaseClient, *, event_id: str) -> Optional[dict]:
    def _query():
        res = (
            supabase.client.table("webhook_events")
            .select("*")
            .eq("event_id", event_id)
            .order("received_at", desc=True)
            .limit(1)
            .execute()
        )
        return (res.data or [None])[0]

    return await asyncio.to_thread(_query)


async def test_parent_update() -> None:
    LOG.info("=== Parent-board column update test ===")
    supabase = SupabaseClient()
    event_id = str(uuid4())
    trigger_uuid = str(uuid4())
    changed_at = _now_utc_iso()

    payload = {
        "event": {
            "id": event_id,
            "type": "change_column_value",
            "boardId": int(PARENT_BOARD_ID),
            "pulseId": int(PARENT_PROJECT_ID),
            "columnId": PARENT_COLUMN_ID,
            "value": {"index": 1},
            "userId": 123456,  # dummy
            "changedAt": changed_at,
            "triggerUuid": trigger_uuid,
        }
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        sent_at = _now_utc_iso()
        r = await _post_webhook(client, payload)
        LOG.info("Parent update response %s %s", r.status_code, r.text)
        r.raise_for_status()

        job = await _wait_for_job(
            supabase,
            project_id=PARENT_PROJECT_ID,
            since_iso=sent_at,
            job_type="push_to_monday",
        )
        if not job:
            LOG.warning("No push_to_monday job observed for parent update.")
        else:
            LOG.info("Job completed: %s status=%s", job["id"], job["status"])

        event_row = await _fetch_webhook_event(supabase, event_id=event_id)
        if not event_row:
            LOG.warning("No webhook_events row found for event %s", event_id)
        else:
            LOG.info(
                "Webhook event recorded status=%s retry_count=%s processing_time_ms=%s",
                event_row["status"],
                event_row.get("retry_count"),
                event_row.get("processing_time_ms"),
            )

        # Duplicate event check
        dup = await _post_webhook(client, payload)
        LOG.info("Duplicate response %s %s", dup.status_code, dup.text)
        assert dup.status_code == 200
        assert dup.json().get("status") == "duplicate"


async def test_subitem_update() -> None:
    LOG.info("=== Subitem column update test ===")
    supabase = SupabaseClient()
    event_id = str(uuid4())
    trigger_uuid = str(uuid4())
    changed_at = _now_utc_iso()

    payload = {
        "event": {
            "id": event_id,
            "type": "change_column_value",
            "boardId": int(SUBITEM_BOARD_ID),
            "pulseId": int(SUBITEM_ID),
            "columnId": SUBITEM_COLUMN_ID,
            "value": {"value": "123.45"},
            "userId": 123456,
            "changedAt": changed_at,
            "triggerUuid": trigger_uuid,
        }
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        sent_at = _now_utc_iso()
        r = await _post_webhook(client, payload)
        LOG.info("Subitem update response %s %s", r.status_code, r.text)
        r.raise_for_status()

        # Expect rehydrate job followed by push job for the parent project
        rehydrate_job = await _wait_for_job(
            supabase,
            project_id=SUBITEM_PARENT_ID,
            since_iso=sent_at,
            job_type="rehydrate_and_analyze",
        )
        push_job = await _wait_for_job(
            supabase,
            project_id=SUBITEM_PARENT_ID,
            since_iso=sent_at,
            job_type="push_to_monday",
        )

        if not rehydrate_job:
            LOG.warning("No rehydrate job observed for parent %s", SUBITEM_PARENT_ID)
        else:
            LOG.info("Rehydrate job %s status=%s", rehydrate_job["id"], rehydrate_job["status"])

        if not push_job:
            LOG.warning("No push job observed after subitem change.")
        else:
            LOG.info("Push job %s status=%s", push_job["id"], push_job["status"])

        event_row = await _fetch_webhook_event(supabase, event_id=event_id)
        if not event_row:
            LOG.warning("No webhook_events row found for subitem event %s", event_id)
        else:
            LOG.info(
                "Subitem webhook recorded status=%s retry_count=%s",
                event_row["status"],
                event_row.get("retry_count"),
            )


async def main() -> None:
    await test_parent_update()
    await test_subitem_update()


if __name__ == "__main__":
    asyncio.run(main())