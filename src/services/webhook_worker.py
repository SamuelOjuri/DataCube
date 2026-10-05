"""Durable ordinary webhook execution, isolated from the HTTP event loop."""
import asyncio
from .durable_worker import DurableWorker


def handle(job):
    # Import lazily to avoid constructing webhook API clients in the main API.
    from ..webhooks.webhook_server import process_webhook_event_optimized
    payload = job['payload']
    asyncio.run(process_webhook_event_optimized(payload['event_type'], payload['board_id'],
        payload['item_id'], payload['data'], webhook_log_id=payload['webhook_log_id']))
    return []


def record_outcome(outcome):
    from ..webhooks.webhook_server import processing_metrics
    key = 'completed_webhooks' if outcome == 'completed' else 'processing_failures'
    processing_metrics[key] += 1


worker = DurableWorker('webhook', handle, record_outcome)
