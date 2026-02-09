
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware
import hmac
import hashlib
import json
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Set
import time
from collections import defaultdict

from ..database.supabase_client import SupabaseClient
from ..database.sync_service import DataSyncService
from ..services.queue_worker import get_task_queue
from ..config import (
    WEBHOOK_SECRET,
    PARENT_BOARD_ID,
    SUBITEM_BOARD_ID,
    HIDDEN_ITEMS_BOARD_ID,
    PARENT_COLUMNS,
    SUBITEM_COLUMNS,
    WEBHOOK_RATE_LIMIT_MAX_REQUESTS,
    WEBHOOK_RATE_LIMIT_WINDOW_SECONDS,
    WEBHOOK_DUPLICATE_TTL_SECONDS,
    PIPELINE_STAGE_LABELS,
    TYPE_LABELS,
    CATEGORY_LABELS,
)

app = FastAPI(
    title="DataCube Webhook Server",
    description="High-performance webhook processing for Monday.com events",
    version="2.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

logger = logging.getLogger(__name__)

# Global instances
supabase_client = SupabaseClient()
sync_service = DataSyncService()


def _queue_rehydrate_job(project_id: str, reason: str) -> None:
    try:
        get_task_queue().enqueue_rehydrate(project_id, source=reason)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to enqueue rehydrate job for %s: %s", project_id, exc)


def _queue_push_job(project_id: str, reason: str) -> None:
    try:
        get_task_queue().enqueue_push_to_monday(project_id, reason=reason)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to enqueue Monday sync for %s: %s", project_id, exc)


def _lookup_parents_for_hidden(hidden_item_id: str) -> Set[str]:
    try:
        rows = (
            supabase_client.client.table('subitems')
            .select('parent_monday_id')
            .eq('hidden_item_id', hidden_item_id)
            .execute()
            .data
            or []
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to map hidden item %s to parents: %s", hidden_item_id, exc)
        return set()

    return {
        str(row.get('parent_monday_id'))
        for row in rows
        if row.get('parent_monday_id')
    }


def _lookup_parent_for_subitem(subitem_id: str) -> Optional[str]:
    try:
        rows = (
            supabase_client.client.table('subitems')
            .select('parent_monday_id')
            .eq('monday_id', subitem_id)
            .limit(1)
            .execute()
            .data
            or []
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to map subitem %s to parent: %s", subitem_id, exc)
        return None

    if not rows:
        return None

    parent_id = rows[0].get('parent_monday_id')
    return str(parent_id) if parent_id else None

# Rate limiting and monitoring
request_counts = defaultdict(list)
processing_metrics = {
    'total_webhooks': 0,
    'successful_webhooks': 0,
    'failed_webhooks': 0,
    'duplicate_webhooks': 0,
    'rate_limited_requests': 0,
    'challenge_requests': 0,
    'processing_times': [],
    'last_reset': datetime.now()
}

# Webhook event cache for deduplication
recent_events = {}
EVENT_CACHE_TTL = WEBHOOK_DUPLICATE_TTL_SECONDS

ANALYSIS_TRIGGER_COLUMNS = {
    PARENT_BOARD_ID: {
        PARENT_COLUMNS['pipeline_stage'],
        PARENT_COLUMNS['type'],
        PARENT_COLUMNS['category'],
        PARENT_COLUMNS['account_mirror'],
        PARENT_COLUMNS['product_mirror'],
        PARENT_COLUMNS['new_enq_value_mirror'],
        PARENT_COLUMNS['gestation_period'],
        PARENT_COLUMNS['project_value'],
        PARENT_COLUMNS['overall_project_value'],
        PARENT_COLUMNS['total_order_value'],
        PARENT_COLUMNS['probability_percent'],
        PARENT_COLUMNS['weighted_pipeline'],
    },
    SUBITEM_BOARD_ID: {
        SUBITEM_COLUMNS['account'],
        SUBITEM_COLUMNS['product_type'],
        SUBITEM_COLUMNS['new_enquiry_value'],
        SUBITEM_COLUMNS['cust_order_value_material'],
    },
}

class WebhookRateLimiter:
    """Simple rate limiter for webhook requests"""
    
    def __init__(self, max_requests: int = 100, window_seconds: int = 60):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.requests = defaultdict(list)
    
    def is_allowed(self, client_ip: str) -> bool:
        now = time.time()
        # Clean old requests
        self.requests[client_ip] = [
            req_time for req_time in self.requests[client_ip]
            if now - req_time < self.window_seconds
        ]
        
        if len(self.requests[client_ip]) >= self.max_requests:
            return False
        
        self.requests[client_ip].append(now)
        return True

rate_limiter = WebhookRateLimiter(
    max_requests=WEBHOOK_RATE_LIMIT_MAX_REQUESTS,
    window_seconds=WEBHOOK_RATE_LIMIT_WINDOW_SECONDS,
)

def _extract_client_ip(request: Request) -> str:
    """Prefer X-Forwarded-For (Render) but fall back to socket IP."""
    forwarded_for = request.headers.get("x-forwarded-for")
    if forwarded_for:
        return forwarded_for.split(",")[0].strip()
    return request.client.host

def _lookup_parent_project_id(subitem_id: str) -> Optional[str]:
    """Fetch parent project ID for a subitem from Supabase."""
    try:
        result = (
            supabase_client.client.table('subitems')
            .select('parent_monday_id')
            .eq('monday_id', subitem_id)
            .limit(1)
            .execute()
        )
        if result.data:
            parent = result.data[0].get('parent_monday_id')
            return str(parent) if parent is not None else None
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Failed to lookup parent project for subitem %s: %s", subitem_id, exc
        )
    return None

def _mark_analysis_warning(
    webhook_log_id: Optional[str],
    message: str,
    *,
    status: str = 'processed_with_warnings',
    extra_fields: Optional[Dict[str, Any]] = None,
) -> None:
    """Persist analysis warnings/errors to webhook_events."""
    if not webhook_log_id:
        return
    try:
        payload = {
            'status': status,
            'error_message': message[:500],
            'processed_at': datetime.now().isoformat(),
        }
        if extra_fields:
            payload.update(extra_fields)
        supabase_client.client.table('webhook_events')\
            .update(payload)\
            .eq('id', webhook_log_id)\
            .execute()
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Failed to record analysis warning for webhook %s: %s",
            webhook_log_id,
            exc,
        )

def verify_webhook_signature(payload: bytes, signature: str) -> bool:
    """Enhanced HMAC verification with proper error handling"""
    if not WEBHOOK_SECRET:
        logger.warning("No webhook secret configured - skipping verification")
        return True  # Development mode
    
    try:
        # Create expected signature using SHA256 HMAC
        expected = hmac.new(
            WEBHOOK_SECRET.encode('utf-8'),
            payload,
            hashlib.sha256
        ).hexdigest()
        
        # Use constant-time comparison to prevent timing attacks
        return hmac.compare_digest(f"sha256={expected}", signature)
    except Exception as e:
        logger.error(f"Signature verification failed: {e}")
        return False

def is_duplicate_event(dedupe_key: Optional[str], event_type: str, item_id: str) -> bool:
    """Check if this webhook event is a duplicate"""
    if not dedupe_key:
        return False

    # Clean expired events
    now = datetime.now()
    expired_keys = [
        key for key, timestamp in recent_events.items()
        if (now - timestamp).total_seconds() > EVENT_CACHE_TTL
    ]
    for key in expired_keys:
        del recent_events[key]

    event_key = f"{dedupe_key}:{event_type}:{item_id}"

    if event_key in recent_events:
        logger.info(f"Duplicate event detected: {event_key}")
        processing_metrics['duplicate_webhooks'] += 1
        return True

    recent_events[event_key] = now
    return False

@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    """Add processing time to response headers"""
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)

    # Update metrics
    if request.url.path.startswith("/webhooks"):
        processing_metrics['processing_times'].append(process_time)
        # Keep only last 1000 measurements
        if len(processing_metrics['processing_times']) > 1000:
            processing_metrics['processing_times'] = processing_metrics['processing_times'][-1000:]

    return response

@app.post("/webhooks/monday")
async def handle_monday_webhook(
    request: Request,
    background_tasks: BackgroundTasks
):
    """Enhanced webhook handler with rate limiting, deduplication, and monitoring"""

    start_time = time.time()
    client_ip = _extract_client_ip(request)
    logger.info("Incoming Monday webhook from %s", client_ip)

    if not rate_limiter.is_allowed(client_ip):
        logger.warning("Rate limit exceeded for IP: %s", client_ip)
        processing_metrics['rate_limited_requests'] += 1
        raise HTTPException(status_code=429, detail="Rate limit exceeded")

    processing_metrics['total_webhooks'] += 1

    try:
        body = await request.body()
        logger.debug("Raw webhook payload: %s", body.decode("utf-8", errors="ignore"))

        try:
            data = json.loads(body) if body else {}
        except json.JSONDecodeError as exc:
            logger.error("Invalid JSON payload: %s", exc)
            processing_metrics['failed_webhooks'] += 1
            raise HTTPException(status_code=400, detail="Invalid JSON")

        challenge_value = data.get('challenge')
        if challenge_value:
            processing_metrics['challenge_requests'] += 1
            processing_metrics['successful_webhooks'] += 1
            logger.info(
                "Responding to Monday challenge for IP %s with value %s",
                client_ip,
                challenge_value
            )
            return JSONResponse({'challenge': challenge_value}, status_code=200)

        signature = request.headers.get('Authorization', '').replace('Bearer ', '')
        if not verify_webhook_signature(body, signature):
            processing_metrics['failed_webhooks'] += 1
            raise HTTPException(status_code=401, detail="Invalid signature")

        event_data = data.get('event', {}) or {}

        event_id_raw = event_data.get('id')
        event_type_raw = event_data.get('type')
        EVENT_ALIASES = {
            "update_column_value": "change_column_value",
            "create_pulse": "create_item",
            "delete_pulse": "delete_item",
            "change_name": "update_name",
        }
        event_type = EVENT_ALIASES.get(event_type_raw, event_type_raw)
        board_id_raw = event_data.get('boardId')
        board_id = str(board_id_raw) if board_id_raw is not None else None
        item_id_raw = event_data.get('pulseId') or event_data.get('itemId')
        item_id = str(item_id_raw) if item_id_raw is not None else None
        event_id = str(event_id_raw) if event_id_raw is not None else None

        logger.info(
            "Webhook event=%s board=%s pulse=%s event_id=%s",
            event_type,
            board_id,
            item_id,
            event_id,
        )

        if not all([event_type, board_id, item_id]):
            logger.warning("Missing required fields in webhook: %s", data)
            processing_metrics['failed_webhooks'] += 1
            raise HTTPException(status_code=400, detail="Missing required fields")

        # New dedupe/uniqueness logic
        dedupe_token = event_id or event_data.get('triggerUuid') or event_data.get('originalTriggerUuid')
        if not dedupe_token and event_data:
            changed_at = event_data.get('changedAt')
            column_id = event_data.get('columnId')
            if changed_at:
                dedupe_token = f"{event_type}:{changed_at}:{column_id or ''}"
        if dedupe_token is not None:
            dedupe_token = str(dedupe_token)

        if is_duplicate_event(dedupe_token, event_type, item_id):
            logger.info("Ignoring duplicate event: %s", dedupe_token)
            return JSONResponse({"status": "duplicate", "ignored": True}, status_code=200)

        webhook_log_id: Optional[str] = None
        try:
            result = supabase_client.client.table('webhook_events').insert({
                'event_id': event_id,
                'board_id': board_id,
                'item_id': item_id,
                'event_type': event_type,
                'webhook_payload': data,
                'received_at': datetime.now().isoformat(),
                'status': 'pending',
                'client_ip': client_ip,
                'processing_time_ms': None
            }).execute()
            if result.data:
                webhook_log_id = result.data[0]['id']
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to log webhook event: %s", exc)

        background_tasks.add_task(
            process_webhook_event_with_retry,
            event_type,
            board_id,
            item_id,
            data,
            webhook_log_id,
            start_time
        )

        processing_metrics['successful_webhooks'] += 1

        return JSONResponse({
            "status": "accepted",
            "event_id": event_id,
            "processing_time_ms": round((time.time() - start_time) * 1000, 2)
        }, status_code=202)

    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        logger.error("Unexpected error in webhook handler: %s", exc)
        processing_metrics['failed_webhooks'] += 1
        raise HTTPException(status_code=500, detail="Internal server error")

async def process_webhook_event_with_retry(
    event_type: str,
    board_id: str,
    item_id: str,
    payload: Dict[str, Any],
    webhook_log_id: Optional[str] = None,
    start_time: float = None,
    max_retries: int = 3
):
    """Process webhook event with retry logic and comprehensive error handling"""

    retry_count = 0
    last_error = None

    while retry_count <= max_retries:
        try:
            logger.debug(
                "Processing %s for item %s on board %s (attempt %s/%s)",
                event_type,
                item_id,
                board_id,
                retry_count + 1,
                max_retries + 1,
            )
            await process_webhook_event_optimized(
                event_type, board_id, item_id, payload, webhook_log_id=webhook_log_id
            )

            # Mark as processed successfully
            if webhook_log_id:
                processing_time = round((time.time() - start_time) * 1000, 2) if start_time else None
                supabase_client.client.table('webhook_events')\
                    .update({
                        'status': 'processed',
                        'processed_at': datetime.now().isoformat(),
                        'processing_time_ms': processing_time,
                        'retry_count': retry_count
                    })\
                    .eq('id', webhook_log_id)\
                    .execute()

            logger.info(f"Successfully processed {event_type} for item {item_id} (attempts: {retry_count + 1})")
            return

        except Exception as e:
            last_error = e
            retry_count += 1

            if retry_count <= max_retries:
                wait_time = min(2 ** retry_count, 30)  # Exponential backoff, max 30s
                logger.warning(f"Webhook processing failed (attempt {retry_count}), retrying in {wait_time}s: {e}")
                await asyncio.sleep(wait_time)
            else:
                logger.error(f"Webhook processing failed after {max_retries} attempts: {e}")

    # Mark as failed after all retries
    if webhook_log_id:
        processing_time = round((time.time() - start_time) * 1000, 2) if start_time else None
        _mark_analysis_warning(
            webhook_log_id,
            f"{event_type} processing failed after {retry_count} attempts: {last_error}",
            status='failed',
            extra_fields={
                'processing_time_ms': processing_time,
                'retry_count': retry_count,
            },
        )

async def process_webhook_event_optimized(
    event_type: str,
    board_id: str,
    item_id: str,
    payload: Dict[str, Any],
    *,
    webhook_log_id: Optional[str] = None
):
    """Optimized webhook processing with board-specific handling"""

    # Coerce board_id to string
    board_id = str(board_id) if board_id is not None else None
    valid_boards = {PARENT_BOARD_ID, SUBITEM_BOARD_ID, HIDDEN_ITEMS_BOARD_ID}
    if board_id not in valid_boards:
        logger.warning("Received webhook for unknown board: %s", board_id)
        return

    try:
        if event_type == "change_column_value":
            await handle_column_changed_minimal(board_id, item_id, payload, webhook_log_id=webhook_log_id)

        elif event_type == "create_item":
            await handle_item_created(board_id, item_id, payload, webhook_log_id=webhook_log_id)

        elif event_type == "delete_item":
            await handle_item_deleted(board_id, item_id)

        elif event_type == "update_name":
            await handle_item_name_updated(board_id, item_id, payload)

        else:
            logger.info(f"Unhandled event type: {event_type}")

    except Exception as e:
        logger.error(f"Error processing {event_type} for item {item_id}: {e}")
        raise

async def handle_item_created(
    board_id: str,
    item_id: str,
    payload: Dict,
    *,
    webhook_log_id: Optional[str] = None
):
    """Handle new item creation with enhanced error handling"""
    logger.info(f"Processing new item: {item_id} in board: {board_id}")

    try:
        from ..core.monday_client import MondayClient
        monday = MondayClient()

        query = f"""
        query {{
            items(ids: [{item_id}]) {{
                id
                name
                parent_item {{
                    id
                }}
                column_values {{
                    id
                    text
                    value
                    type
                }}
            }}
        }}
        """

        result = monday.execute_query(query)
        if not result.get('data', {}).get('items'):
            logger.warning(f"No item data returned for {item_id}")
            return

        item_data = result['data']['items'][0]

        if board_id == PARENT_BOARD_ID:
            # Normalize the item before transformation
            normalized_item = sync_service._normalize_monday_item(item_data)
            transformed = sync_service._transform_for_projects_table([normalized_item])
            if transformed:
                supabase_client.upsert_projects(transformed)
                logger.info("Upserted project: %s", transformed[0].get("monday_id"))
            from ..services.analysis_service import AnalysisService

            try:
                logger.info(
                    "Running analysis for new project %s on board %s",
                    item_id,
                    board_id,
                )
                analysis_started = time.time()
                AnalysisService().analyze_and_store(item_id)
                logger.info(
                    "Analysis complete for project %s (%.2f ms)",
                    item_id,
                    (time.time() - analysis_started) * 1000,
                )
                _queue_push_job(item_id, "parent_create")
            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    "Analysis failed for newly created item %s on board %s",
                    item_id,
                    board_id,
                )
                _mark_analysis_warning(webhook_log_id, f"analysis failed: {exc}")

        elif board_id == SUBITEM_BOARD_ID:
            transformed = sync_service._transform_for_subitems_table([item_data])
            if transformed:
                supabase_client.upsert_subitems(transformed)
                logger.info("Upserted subitem: %s", transformed[0].get("monday_id"))
            parent_ref = item_data.get("parent_item") or {}
            parent_id = str(parent_ref.get("id") or "")
            if parent_id:
                _queue_rehydrate_job(parent_id, "subitem_create")

        elif board_id == HIDDEN_ITEMS_BOARD_ID:
            transformed = sync_service._transform_for_hidden_table([item_data])
            if transformed:
                supabase_client.upsert_hidden_items(transformed)
                logger.info("Upserted hidden item: %s", transformed[0].get("monday_id"))
            else:
                logger.debug("Hidden item %s produced no rows after transform", item_id)
            for parent in _lookup_parents_for_hidden(str(item_id)):
                _queue_rehydrate_job(parent, "hidden_create")
            return  # skip AnalysisService for hidden items

    except Exception as e:
        logger.error(f"Failed to handle item creation for {item_id}: {e}")
        raise

async def handle_column_changed_minimal(
    board_id: str,
    item_id: str,
    payload: Dict,
    *,
    webhook_log_id: Optional[str] = None
):
    """Minimal column update with enhanced error handling and validation"""

    event_data = payload.get('event', {})
    column_id = event_data.get('columnId')
    new_value = event_data.get('value')

    if not column_id:
        logger.warning(f"No column ID in payload for item {item_id}")
        return

    field_mapping = get_enhanced_column_field_mapping(board_id, column_id)

    if not field_mapping:
        logger.debug(f"No mapping for column {column_id} in board {board_id}")
        return

    table_name = field_mapping['table']
    field_name = field_mapping['field']
    transform_func = field_mapping.get('transform')

    if transform_func:
        try:
            new_value = transform_func(new_value)
        except Exception as e:
            logger.warning(f"Value transformation failed for {column_id}: {e}")
            return

    analysis_targets: Set[str] = set()
    trigger_columns = ANALYSIS_TRIGGER_COLUMNS.get(board_id, set())

    if column_id in trigger_columns:
        if board_id == PARENT_BOARD_ID:
            analysis_targets.add(str(item_id))
        elif board_id == SUBITEM_BOARD_ID:
            parent_id = _lookup_parent_project_id(item_id)
            if parent_id:
                analysis_targets.add(parent_id)

    try:
        result = supabase_client.client.table(table_name)\
            .update({
                field_name: new_value,
                'last_synced_at': datetime.now().isoformat()
            })\
            .eq('monday_id', item_id)\
            .execute()

        if result.data:
            logger.debug(f"Updated {field_name} for item {item_id}: {new_value}")
        else:
            logger.warning(f"No rows updated for item {item_id} in table {table_name}")

        if analysis_targets and board_id == PARENT_BOARD_ID:
            from ..services.analysis_service import AnalysisService

            svc = AnalysisService()
            for target_id in analysis_targets:
                try:
                    logger.info(
                        "Running analysis for project %s due to %s update (%s)",
                        target_id,
                        column_id,
                        board_id,
                    )
                    analysis_started = time.time()
                    svc.analyze_and_store(target_id)
                    logger.info(
                        "Analysis complete for project %s after %s change (%.2f ms)",
                        target_id,
                        column_id,
                        (time.time() - analysis_started) * 1000,
                    )
                    _queue_push_job(target_id, "parent_change")
                except Exception as exc:  # noqa: BLE001
                    logger.exception(
                        "Analysis failed for project %s after %s change", target_id, column_id
                    )
                    _mark_analysis_warning(
                        webhook_log_id,
                        f"analysis failed for {target_id} after {column_id} update: {exc}",
                    )

        elif analysis_targets and board_id == SUBITEM_BOARD_ID:
            for target_id in analysis_targets:
                _queue_rehydrate_job(target_id, "subitem_change")

        elif board_id == HIDDEN_ITEMS_BOARD_ID:
            for parent in _lookup_parents_for_hidden(item_id):
                _queue_rehydrate_job(parent, "hidden_change")

    except Exception as e:
        logger.error(f"Failed to update {field_name} for item {item_id}: {e}")
        raise

async def handle_item_deleted(board_id: str, item_id: str):
    """Handle item deletion with proper table mapping"""
    logger.info(f"Processing deleted item: {item_id} in board: {board_id}")

    parent_to_refresh: Set[str] = set()
    if board_id == SUBITEM_BOARD_ID:
        parent = _lookup_parent_for_subitem(str(item_id))
        if parent:
            parent_to_refresh.add(parent)
    elif board_id == HIDDEN_ITEMS_BOARD_ID:
        parent_to_refresh |= _lookup_parents_for_hidden(str(item_id))

    try:
        # Delete from appropriate table based on board_id
        if board_id == PARENT_BOARD_ID:  # Parent board
            result = supabase_client.client.table('projects').delete().eq('monday_id', item_id).execute()
            logger.info(f"Deleted project {item_id}: {len(result.data)} rows affected")

        elif board_id == SUBITEM_BOARD_ID:  # Subitems board
            result = supabase_client.client.table('subitems').delete().eq('monday_id', item_id).execute()
            logger.info(f"Deleted subitem {item_id}: {len(result.data)} rows affected")

        elif board_id == HIDDEN_ITEMS_BOARD_ID:  # Hidden items board
            result = supabase_client.client.table('hidden_items').delete().eq('monday_id', item_id).execute()
            logger.info(f"Deleted hidden item {item_id}: {len(result.data)} rows affected")

    except Exception as e:
        logger.error(f"Failed to delete item {item_id} from board {board_id}: {e}")
        raise
    finally:
        for parent in parent_to_refresh:
            _queue_rehydrate_job(parent, "child_delete")

async def handle_item_name_updated(board_id: str, item_id: str, payload: Dict) -> None:
    """Handle item name updates"""
    event = payload.get("event") or {}
    raw_value = event.get("value")
    new_name: Optional[str] = None

    if isinstance(raw_value, str):
        new_name = raw_value.strip()
    elif isinstance(raw_value, dict):
        new_name = (
            raw_value.get("name")
            or raw_value.get("text")
            or raw_value.get("value")
        )
        if isinstance(new_name, str):
            new_name = new_name.strip()

    if not new_name:
        logger.warning(f"No new name provided for item {item_id} (value={raw_value!r})")
        return

    try:
        table_map = {
            PARENT_BOARD_ID: "projects",
            SUBITEM_BOARD_ID: "subitems",
            HIDDEN_ITEMS_BOARD_ID: "hidden_items",
        }

        table_name = table_map.get(board_id)
        if not table_name:
            logger.warning(f"Unknown board ID for name update: {board_id}")
            return

        result = (
            supabase_client.client.table(table_name)
            .update(
                {
                    "item_name": new_name,
                    "last_synced_at": datetime.now().isoformat(),
                }
            )
            .eq("monday_id", item_id)
            .execute()
        )

        logger.info(f"Updated name for {item_id} in {table_name}: {new_name}")

    except Exception as exc:  # noqa: BLE001
        logger.error(f"Failed to update name for item {item_id}: {exc}")
        raise

def get_enhanced_column_field_mapping(board_id: str, column_id: str) -> Optional[Dict]:
    """Enhanced column mapping with transformation functions and validation."""

    def _coerce_monday_value(raw_value):
        if isinstance(raw_value, dict):
            return dict(raw_value)
        data = {"raw": raw_value}
        if isinstance(raw_value, str):
            value = raw_value.strip()
            if value.startswith("{") and value.endswith("}"):
                try:
                    parsed = json.loads(value)
                except json.JSONDecodeError:
                    data["text"] = raw_value
                else:
                    if isinstance(parsed, dict):
                        return parsed
                    data["text"] = raw_value
            else:
                data["text"] = raw_value
        elif raw_value is not None:
            data["text"] = str(raw_value)
        return data

    def parse_text_value(raw_value):
        data = _coerce_monday_value(raw_value)
        text = data.get("text") or data.get("value") or data.get("name") or data.get("label")
        if isinstance(text, dict):
            text = text.get("label") or text.get("text") or text.get("value")
        if text is not None:
            return str(text).strip()
        raw = data.get("raw")
        if raw is not None:
            return str(raw).strip()
        return None

    def parse_status_value(raw_value, label_map):
        data = _coerce_monday_value(raw_value)
        index = data.get("index")
        label_entry = data.get("label")
        label = None
        if isinstance(label_entry, dict):
            if index is None:
                index = label_entry.get("index")
            label = label_entry.get("label") or label_entry.get("text")
        elif label_entry is not None:
            label = str(label_entry)
        if index is not None:
            mapped = label_map.get(str(index))
            if mapped:
                return mapped
        if label:
            mapped = label_map.get(str(label))
            return mapped or label.strip()
        text = data.get("text") or data.get("value")
        if text:
            text_str = str(text).strip()
            mapped = label_map.get(text_str)
            return mapped or text_str
        raw = data.get("raw")
        if raw:
            raw_str = str(raw).strip()
            mapped = label_map.get(raw_str)
            return mapped or raw_str
        return None

    def parse_dropdown_value(raw_value, label_map=None):
        data = _coerce_monday_value(raw_value)
        chosen = data.get("chosenValues") or data.get("labels")
        candidate_id = None
        candidate_label = None
        if isinstance(chosen, list) and chosen:
            first = chosen[0]
            if isinstance(first, dict):
                candidate_id = first.get("id") or first.get("value")
                candidate_label = first.get("name") or first.get("label") or first.get("text")
            else:
                candidate_id = str(first)
        ids = data.get("ids")
        if candidate_id is None and isinstance(ids, list) and ids:
            candidate_id = str(ids[0])
        if candidate_label is None:
            value = data.get("text") or data.get("value") or data.get("label")
            if isinstance(value, dict):
                candidate_label = value.get("label") or value.get("text")
            elif value is not None:
                candidate_label = str(value)
        if label_map:
            if candidate_id is not None:
                mapped = label_map.get(str(candidate_id))
                if mapped:
                    return mapped
            if candidate_label:
                mapped = label_map.get(candidate_label)
                if mapped:
                    return mapped
        if candidate_label:
            return candidate_label.strip()
        if candidate_id is not None:
            return str(candidate_id).strip()
        raw = data.get("raw")
        if raw is not None:
            return str(raw).strip()
        return None

    def parse_numeric_value(raw_value):
        if raw_value is None or raw_value == "":
            return None
        if isinstance(raw_value, (int, float)):
            return float(raw_value)
        data = _coerce_monday_value(raw_value)
        candidate = data.get("value") or data.get("text")
        if isinstance(candidate, dict):
            candidate = candidate.get("value") or candidate.get("amount") or candidate.get("text")
        if candidate is None:
            candidate = data.get("raw")
        if candidate is None:
            return None
        try:
            clean_value = str(candidate).replace("£", "").replace(",", "").strip()
            return float(clean_value) if clean_value else None
        except (ValueError, TypeError):
            return None

    def parse_date_value(raw_value):
        data = _coerce_monday_value(raw_value)
        date_text = data.get("date") or data.get("text") or data.get("value")
        if isinstance(date_text, dict):
            date_text = date_text.get("date") or date_text.get("text")
        if not date_text:
            date_text = data.get("raw")
        if not date_text:
            return None
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
            try:
                return datetime.strptime(str(date_text), fmt).date().isoformat()
            except ValueError:
                continue
        return str(date_text)

    mappings = {
        PARENT_BOARD_ID: {
            PARENT_COLUMNS["project_name"]: {
                "table": "projects",
                "field": "project_name",
                "transform": parse_text_value,
            },
            PARENT_COLUMNS["pipeline_stage"]: {
                "table": "projects",
                "field": "pipeline_stage",
                "transform": lambda raw: parse_status_value(raw, PIPELINE_STAGE_LABELS),
            },
            PARENT_COLUMNS["type"]: {
                "table": "projects",
                "field": "type",
                "transform": lambda raw: parse_dropdown_value(raw, TYPE_LABELS),
            },
            PARENT_COLUMNS["category"]: {
                "table": "projects",
                "field": "category",
                "transform": lambda raw: parse_dropdown_value(raw, CATEGORY_LABELS),
            },
            PARENT_COLUMNS["gestation_period"]: {
                "table": "projects",
                "field": "gestation_period",
                "transform": parse_numeric_value,
            },
            PARENT_COLUMNS["date_created"]: {
                "table": "projects",
                "field": "date_created",
                "transform": parse_date_value,
            },
            PARENT_COLUMNS["follow_up_date"]: {
                "table": "projects",
                "field": "follow_up_date",
                "transform": parse_date_value,
            },
            PARENT_COLUMNS["total_order_value"]: {
                "table": "projects",
                "field": "total_order_value",
                "transform": parse_numeric_value,
            },
        },
        SUBITEM_BOARD_ID: {
            SUBITEM_COLUMNS["account"]: {
                "table": "subitems",
                "field": "account",
                "transform": parse_text_value,
            },
            SUBITEM_COLUMNS["product_type"]: {
                "table": "subitems",
                "field": "product_type",
                "transform": parse_text_value,
            },
            SUBITEM_COLUMNS["new_enquiry_value"]: {
                "table": "subitems",
                "field": "new_enquiry_value",
                "transform": parse_numeric_value,
            },
            SUBITEM_COLUMNS["date_order_received"]: {
                "table": "subitems",
                "field": "date_order_received",
                "transform": parse_date_value,
            },
            SUBITEM_COLUMNS["cust_order_value_material"]: {
                "table": "subitems",
                "field": "cust_order_value_material",
                "transform": parse_numeric_value,
            },
        },
        HIDDEN_ITEMS_BOARD_ID: {
            # These constant names must exist in your config:
            # e.g. HIDDEN_ITEMS_COLUMNS["quote_amount"]
            "formula63__1": {
                "table": "hidden_items",
                "field": "quote_amount",
                "transform": parse_numeric_value,
            },
            "date__1": {
                "table": "hidden_items",
                "field": "date_design_completed",
                "transform": parse_date_value,
            },
            "date42__1": {
                "table": "hidden_items",
                "field": "invoice_date",
                "transform": parse_date_value,
            },
        },
    }

    return mappings.get(board_id, {}).get(column_id)

@app.get("/health")
async def health_check():
    """Enhanced health check with system status"""
    try:
        # Test Supabase connection
        test_result = supabase_client.client.table('webhook_events').select('count').limit(1).execute()
        supabase_healthy = test_result is not None
    except Exception:
        supabase_healthy = False

    return {
        "status": "healthy" if supabase_healthy else "degraded",
        "timestamp": datetime.now().isoformat(),
        "supabase_connection": "ok" if supabase_healthy else "error",
        "metrics": {
            "total_webhooks": processing_metrics['total_webhooks'],
            "success_rate": round(
                processing_metrics['successful_webhooks'] / max(processing_metrics['total_webhooks'], 1) * 100, 2
            ),
            "avg_processing_time_ms": round(
                sum(processing_metrics['processing_times'][-100:]) /
                max(len(processing_metrics['processing_times'][-100:]), 1) * 1000, 2
            ) if processing_metrics['processing_times'] else 0
        }
    }

@app.get("/metrics")
async def get_metrics():
    """Detailed metrics endpoint"""
    return {
        "webhook_metrics": processing_metrics,
        "cache_size": len(recent_events),
        "active_rate_limits": len(rate_limiter.requests),
        "timestamp": datetime.now().isoformat()
    }

@app.post("/admin/clear-cache")
async def clear_event_cache():
    """Admin endpoint to clear event cache"""
    global recent_events
    cache_size = len(recent_events)
    recent_events.clear()
    return {
        "status": "cleared",
        "events_cleared": cache_size,
        "timestamp": datetime.now().isoformat()
    }
