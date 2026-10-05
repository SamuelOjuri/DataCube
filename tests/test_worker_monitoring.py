"""Offline health, ingress and worker supervision contracts."""
import asyncio
import importlib
import json
import threading
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock, patch

from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi.testclient import TestClient
import pytest
import requests

from src.services.worker_monitor import WorkerMonitor
from src.services.durable_worker import DurableWorker
from src.services import worker_store as store
from src.api.routes import health
from src.services.scheduled_workers import ScheduledWorkers
from test_monday_lifecycle import server, request


@pytest.fixture(autouse=True)
def no_external(monkeypatch):
    monkeypatch.setattr(requests.sessions.Session, 'request', lambda *a, **k: pytest.fail('No external HTTP'))
    monkeypatch.setattr(store, 'connect', lambda: pytest.fail('No live DB in unit tests'))


def ready_monitor():
    clock = [100.0]
    monitor = WorkerMonitor(lambda: clock[0])
    monitor.register('general', budget=1800)
    monitor.update('general', 'idle')
    monitor.last_persisted = clock[0]
    return monitor, clock


def test_idle_and_disabled_workers_are_healthy():
    monitor, _ = ready_monitor()
    monitor.register('lifecycle', enabled=False)
    assert monitor.snapshot()['ready']


def test_database_outage_changes_readiness_not_liveness():
    monitor, _ = ready_monitor()
    monitor.database_error = 'ConnectionError'
    state = monitor.snapshot()
    assert state['live'] and not state['ready']


def test_busy_job_has_separate_budget_and_heartbeat_cannot_hide_stall():
    monitor, clock = ready_monitor()
    monitor.update('general', 'busy', job='one')
    clock[0] += 100
    assert monitor.snapshot()['live']
    clock[0] += 1800
    monitor.last_persisted = clock[0]
    monitor.update('general', 'busy', job='one')
    assert monitor.snapshot()['workers']['general']['failure'] == 'work_overdue'
    assert not monitor.snapshot()['live']


def test_dead_task_and_stale_poll_fail_liveness():
    monitor, clock = ready_monitor()
    monitor.bind('general', SimpleNamespace(done=lambda: True))
    assert monitor.snapshot()['workers']['general']['failure'] == 'task_stopped'
    monitor.bind('general', None)
    clock[0] += 76
    assert monitor.snapshot()['workers']['general']['failure'] == 'poll_stale'


def test_health_status_codes_authentication_and_no_io(monkeypatch):
    monitor, _ = ready_monitor()
    monkeypatch.setattr(health, 'monitor', monitor)
    monkeypatch.setenv('WORKER_MONITOR_TOKEN', 'local-test-token')
    app = FastAPI()
    app.include_router(health.router)
    client = TestClient(app)
    assert client.get('/health/live').status_code == 200
    assert client.get('/health').status_code == 200
    assert client.get('/health/workers').status_code == 403
    assert client.get('/health/workers', headers={'Authorization': 'Bearer local-test-token'}).status_code == 200
    monitor.database_error = 'ConnectionError:secret-never-expose'
    response = client.get('/health/ready')
    assert response.status_code == 503 and 'secret' not in response.text
    assert client.get('/health/live').status_code == 200
    monitor.bind('general', SimpleNamespace(done=lambda: True))
    assert client.get('/health/live').status_code == 503


def test_both_applications_register_only_readonly_health(server):
    api = importlib.import_module('src.api.app')
    for app in (server.app, api.app):
        paths = [r.path for r in app.routes]
        assert all(p in paths for p in ['/health', '/health/live', '/health/ready', '/health/workers'])
        assert paths.count('/health') == 1
        assert '/monitoring/cleanup' not in paths


def test_ordinary_webhook_ack_requires_durable_insert_and_ignores_memory_dedupe(server, monkeypatch):
    persist = Mock(return_value='durable-job')
    monkeypatch.setattr(server, 'accept_webhook', persist)
    payload = {'event': {'type': 'change_column_value', 'boardId': server.PARENT_BOARD_ID, 'pulseId': '101', 'id': 'one'}}
    tasks = BackgroundTasks()
    result = asyncio.run(server.handle_monday_webhook(request(payload), tasks))
    assert result.status_code == 202 and json.loads(result.body)['durable']
    assert not tasks.tasks
    persist.side_effect = ConnectionError('secret')
    with pytest.raises(HTTPException) as exc:
        asyncio.run(server.handle_monday_webhook(request(payload), tasks))
    assert exc.value.status_code == 503 and 'secret' not in exc.value.detail


def test_enqueue_failure_is_propagated_to_webhook_retry(server, monkeypatch):
    queue = Mock()
    queue.enqueue_rehydrate.side_effect = ConnectionError('db down')
    monkeypatch.setattr(server, 'get_task_queue', lambda: queue)
    with pytest.raises(ConnectionError):
        server._queue_rehydrate_job('101', 'change')


def test_cancelled_worker_cannot_restart_while_thread_is_running(monkeypatch):
    import src.services.durable_worker as module
    monitor, _ = ready_monitor()
    monkeypatch.setattr(module, 'monitor', monitor)
    entered, release = threading.Event(), threading.Event()
    worker = DurableWorker('general', lambda job: [])
    def blocked():
        entered.set()
        assert release.wait(5)
        return False
    monkeypatch.setattr(worker, 'run_once', blocked)
    async def check():
        worker.start()
        assert await asyncio.to_thread(entered.wait, 2)
        worker.task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await worker.task
        with pytest.raises(RuntimeError, match='still running'):
            worker.start()
        release.set()
        await worker.inflight
    try:
        asyncio.run(check())
    finally:
        release.set()


def test_schedule_occurrences_are_utc_and_replica_stable():
    workers = ScheduledWorkers(Mock())
    workers.specs['sync'] = dict(seconds=1500, hour=None, minute=None)
    workers.specs['daily'] = dict(seconds=None, hour=2, minute=15)
    now = datetime(2026, 10, 5, 2, 16, tzinfo=timezone.utc)
    assert workers.occurrence('daily', now) == now.replace(minute=15)
    assert workers.occurrence('sync', now).timestamp() % 1500 == 0


def test_all_seven_schedules_registered_and_shutdown_consistent(monkeypatch):
    api = importlib.import_module('src.api.app')
    from unittest.mock import AsyncMock
    scheduled = Mock(start=AsyncMock(), stop=AsyncMock())
    queue = Mock(stop=AsyncMock())
    life = Mock(stop=AsyncMock())
    mon = Mock(stop=AsyncMock())
    monkeypatch.setattr(api, '_scheduled_workers', scheduled)
    monkeypatch.setattr(api, '_scheduler', SimpleNamespace(running=False))
    monkeypatch.setattr(api, 'get_task_queue', lambda: queue)
    monkeypatch.setattr(api, 'lifecycle_worker', life)
    monkeypatch.setattr(api, 'monitor', mon)
    monkeypatch.setenv('SCHEDULER_ENABLED', 'true')
    asyncio.run(api._startup())
    assert {c.args[0] for c in scheduled.add.call_args_list} == {
        'delta_rehydrate','recent_rehydrate','llm_backfill','monday_sync',
        'refresh_conversion_views','forecast_snapshot_maintenance','webhook_cleanup'}
    asyncio.run(api._shutdown())
    scheduled.stop.assert_awaited_once()
    queue.stop.assert_awaited_once()
    life.stop.assert_awaited_once()


def test_scheduler_wrapper_propagates_partial_result_and_failure(monkeypatch):
    api = importlib.import_module('src.api.app')
    async def partial(**kwargs):
        return {'errors': 2, 'succeeded': 4}
    monkeypatch.setattr(api, 'rehydrate_recent', partial)
    assert asyncio.run(api._scheduled_recent_rehydrate())['errors'] == 2
    async def boom(**kwargs):
        raise ConnectionError('failed')
    monkeypatch.setattr(api, 'rehydrate_recent', boom)
    with pytest.raises(ConnectionError):
        asyncio.run(api._scheduled_recent_rehydrate())


def test_watchdog_is_readonly_unless_record_requested(monkeypatch):
    from scripts import health_check
    connection = Mock()
    monkeypatch.setattr(store, 'connect', lambda: __import__('contextlib').nullcontext(connection))
    monkeypatch.setattr(health_check, 'operational_status', Mock(return_value={'healthy': True, 'alerts': []}))
    record = Mock()
    monkeypatch.setattr(health_check, 'record_alerts', record)
    assert health_check.check(['api'])['healthy']
    record.assert_not_called()
    health_check.check(['api'], record=True)
    record.assert_called_once()


def test_rehydrate_followup_and_push_use_replayable_operations(monkeypatch):
    from src.services.queue_worker import TaskQueue, QueueTask
    from src.services import analysis_service, monday_update_service
    from src.tasks import pipeline
    from unittest.mock import AsyncMock
    analysis = Mock()
    analysis.analyze_and_store.return_value = {'success': True}
    analysis.db.get_latest_analysis_result.return_value = {'rating_score': 80}
    monday = Mock()
    monday.sync_project.return_value = {'success': True}
    monkeypatch.setattr(analysis_service, 'AnalysisService', lambda: analysis)
    monkeypatch.setattr(monday_update_service, 'MondayUpdateService', lambda: monday)
    monkeypatch.setattr(pipeline, 'rehydrate_projects_by_ids', AsyncMock())
    queue = TaskQueue()
    task = QueueTask('rehydrate_and_analyze', '101', job_id='source')
    followup = asyncio.run(queue._handle_rehydrate(task))
    assert followup[0]['key'] == 'source:push'
    assert followup[0]['project_id'] == '101'
    queue._handle_push(task)
    assert monday.sync_project.call_args.kwargs['include_update'] is False
    analysis.analyze_and_store.return_value = {'success': False}
    with pytest.raises(RuntimeError, match='Analysis failed'):
        asyncio.run(queue._handle_rehydrate(task))


def test_failed_batch_results_reach_scheduler(monkeypatch):
    from src.tasks import pipeline
    async def rehydrate(ids, **kwargs):
        if '102' in ids:
            raise ConnectionError('no partial success')
    monkeypatch.setattr(pipeline, 'rehydrate_projects_by_ids', rehydrate)
    candidates = [pipeline.ProjectCandidate('101','101 name','101'), pipeline.ProjectCandidate('102','102 name','102')]
    result = asyncio.run(pipeline._rehydrate_candidates_batched(candidates, batch_prefix_limit=1, chunk_size=1))
    assert result == {'total': 2, 'succeeded': 1, 'errors': 1}


def test_missing_maintenance_function_is_a_failure(monkeypatch):
    from src.tasks import postgres_maintenance as maintenance
    from test_postgres_maintenance_phase5 import FakeConnection
    connection = FakeConnection()
    monkeypatch.setenv('SUPABASE_DB_URL', 'unused-offline-dsn')
    monkeypatch.setattr(maintenance.psycopg, 'connect', lambda *a, **kw: connection)
    monkeypatch.setattr(maintenance, '_function_exists', lambda *a: False)
    with pytest.raises(maintenance.MaintenanceUnavailable):
        maintenance.create_pipeline_forecast_snapshot()


def test_webhook_lifecycle_starts_and_stops_all_expected_consumers(server, monkeypatch):
    from unittest.mock import AsyncMock
    general = Mock(stop=AsyncMock())
    webhook = Mock(stop=AsyncMock())
    lifecycle = Mock(stop=AsyncMock())
    mon = Mock(stop=AsyncMock())
    monkeypatch.setattr(server, 'get_task_queue', lambda: general)
    monkeypatch.setattr(server, 'webhook_worker', webhook)
    monkeypatch.setattr(server.monday_lifecycle, 'worker', lifecycle)
    monkeypatch.setattr(server, 'monitor', mon)
    asyncio.run(server.start_lifecycle_worker())
    asyncio.run(server.stop_lifecycle_worker())
    for worker in [general, webhook, lifecycle]:
        worker.start.assert_called_once()
        worker.stop.assert_awaited_once()
    mon.start.assert_called_once_with('webhook')


def test_notification_is_bounded_and_excludes_payloads(monkeypatch):
    from src.services import notification_service as notifications
    monkeypatch.setenv('WORKER_ALERT_WEBHOOK_URL', 'https://alerts.example.test/private-token')
    post = Mock(return_value=SimpleNamespace(status_code=200))
    monkeypatch.setattr(notifications.requests, 'post', post)
    notifications.send_worker_alerts([{'key': 'queue:general:failed', 'issue': 'queue_needs_attention', 'payload': 'sensitive'}])
    kwargs = post.call_args.kwargs
    assert kwargs['timeout'] == 5 and kwargs['allow_redirects'] is False
    assert 'sensitive' not in str(kwargs['json'])
    post.return_value.status_code = 500
    with pytest.raises(RuntimeError):
        notifications.send_worker_alerts([{'key': 'x', 'issue': 'failed'}])


@pytest.mark.parametrize('entity', ['projects', 'subitems', 'hidden_items'])
def test_worker_batch_write_failures_are_not_silent(entity):
    from src.database.sync_service import DataSyncService
    service = DataSyncService.__new__(DataSyncService)
    service.strict_writes = True
    service.supabase_client = Mock()
    getattr(service.supabase_client, 'upsert_' + entity).return_value = {'success': False, 'error': 'write rejected'}
    with pytest.raises(RuntimeError, match='batch write failed'):
        asyncio.run(getattr(service, '_batch_upsert_' + entity)([{'monday_id': '101'}]))


def test_scheduled_timeline_failure_is_not_reported_as_success():
    from src.services.monday_update_service import MondayUpdateService
    monday = Mock()
    monday.get_item_updates.return_value = []
    monday.create_item_update.side_effect = ConnectionError('unavailable')
    service = MondayUpdateService(db_client=Mock(), monday_client=monday)
    service.db.is_project_reporting_excluded.return_value = False
    assert service.sync_project('101', {'rating_score': 80})['success'] is False
    monday.get_item_updates.side_effect = ConnectionError('lookup unavailable')
    monday.create_item_update.reset_mock()
    with pytest.raises(ConnectionError):
        service.sync_project('101', {'rating_score': 80})
    monday.create_item_update.assert_not_called()


def test_monitor_creates_identity_at_startup_not_module_import(monkeypatch):
    monitor = WorkerMonitor()
    inherited_id = monitor.instance_id
    async def parked():
        await asyncio.sleep(3600)
    monkeypatch.setattr(monitor, 'run', parked)
    async def check():
        monitor.start('api')
        assert monitor.instance_id != inherited_id
        active_id = monitor.instance_id
        monitor.start('api')
        assert monitor.instance_id == active_id
        monitor.task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await monitor.task
    asyncio.run(check())
