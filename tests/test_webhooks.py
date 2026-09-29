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
import importlib
import sys
from unittest.mock import Mock, patch
from datetime import datetime, timezone
from typing import Optional
from uuid import uuid4

import httpx
import pytest

from src.config import (
    PARENT_BOARD_ID,
    SUBITEM_BOARD_ID,
    HIDDEN_ITEMS_BOARD_ID,
    PARENT_COLUMNS,
    SUBITEM_COLUMNS,
    HIDDEN_ITEMS_COLUMNS,
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


@pytest.fixture
def order_webhook_server(monkeypatch):
    module_name = "src.webhooks.webhook_server"
    was_imported = module_name in sys.modules
    with patch("src.database.supabase_client.SupabaseClient"), patch("src.database.sync_service.DataSyncService"):
        server = importlib.import_module(module_name)
    monkeypatch.setattr(server, "supabase_client", Mock())
    monkeypatch.setattr(server, "sync_service", Mock())
    monkeypatch.setattr(server, "_queue_rehydrate_job", Mock())
    monkeypatch.setattr(server, "_queue_hidden_rehydrate_jobs", Mock())
    monkeypatch.setattr(server, "_lookup_parent_project_id", Mock(return_value="p1"))
    yield server
    if not was_imported:
        sys.modules.pop(module_name, None)
        package = sys.modules.get("src.webhooks")
        if package is not None and getattr(package, "webhook_server", None) is server:
            delattr(package, "webhook_server")


def test_order_parent_mirror_queues_recompute_without_overwriting_total(order_webhook_server):
    server = order_webhook_server
    asyncio.run(server.handle_column_changed_minimal(PARENT_BOARD_ID, "p1", {
        "event": {"columnId": PARENT_COLUMNS["total_order_value"], "value": {"value": "100"}}
    }))
    server._queue_rehydrate_job.assert_called_once_with("p1", "parent_order_mirror_change")
    server.supabase_client.client.table.assert_not_called()
    assert server.get_enhanced_column_field_mapping(PARENT_BOARD_ID, PARENT_COLUMNS["total_order_value"]) is None


@pytest.mark.parametrize("field", ["cust_order_value_material", "cust_additional_charges"])
@pytest.mark.parametrize("value", [{"value": "25.50"}, None, {"value": "invalid"}])
def test_order_hidden_changes_refetch_both_components(order_webhook_server, field, value):
    server = order_webhook_server
    column_id = HIDDEN_ITEMS_COLUMNS[field]
    asyncio.run(server.handle_column_changed_minimal(HIDDEN_ITEMS_BOARD_ID, "h1", {
        "event": {"columnId": column_id, "value": value}
    }))
    server._queue_hidden_rehydrate_jobs.assert_called_once_with("h1", f"hidden_order_change:{column_id}")
    server.supabase_client.client.table.assert_not_called()
    assert server.get_enhanced_column_field_mapping(HIDDEN_ITEMS_BOARD_ID, column_id)["field"] == field


@pytest.mark.parametrize("field", ["cust_order_value_material", "hidden_item_id"])
def test_order_subitem_changes_refetch_authoritative_hidden_values(order_webhook_server, field):
    server = order_webhook_server
    asyncio.run(server.handle_column_changed_minimal(SUBITEM_BOARD_ID, "s1", {
        "event": {"columnId": SUBITEM_COLUMNS[field], "value": {"value": "999"}}
    }))
    server._queue_rehydrate_job.assert_called_once_with("p1", "subitem_order_change")
    server.supabase_client.client.table.assert_not_called()


if __name__ == "__main__":
    asyncio.run(main())