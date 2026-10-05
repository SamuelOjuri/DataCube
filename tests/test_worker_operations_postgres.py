"""Real concurrent claims on an explicitly supplied disposable loopback database."""
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path
import threading
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
import pytest

from src.services import worker_store as store
from src.services.worker_monitor import operational_status, record_alerts
from test_order_value_scopes_postgres import database

SCHEMA = Path(__file__).resolve().parents[1] / 'src/database/schema/worker_operations.sql'


@pytest.fixture
def db(database, monkeypatch):
    c, dsn, _, _ = database
    c.row_factory = dict_row
    c.execute('''CREATE TABLE webhook_events (id uuid PRIMARY KEY,event_id text UNIQUE,
        board_id text,item_id text,event_type text,webhook_payload jsonb,status text,client_ip text,
        received_at timestamptz DEFAULT now(),processed_at timestamptz,retry_count int,error_message text,processing_time_ms numeric)''')
    c.execute(SCHEMA.read_text())
    monkeypatch.setattr(store, 'connect', lambda: psycopg.connect(dsn, autocommit=True, row_factory=dict_row))
    yield c, dsn


def test_migration_rerun_preserves_legacy_jobs_and_no_silent_replay(db):
    c, _ = db
    c.execute("INSERT INTO job_queue(id,job_type,status) VALUES (%s,'push_to_monday','running')", (uuid4(),))
    c.execute(SCHEMA.read_text())
    assert store.claim(c, 'general', 'new') is None
    report = operational_status(c)
    assert any(a.get('queue') == 'legacy' for a in report['alerts'])


def test_queue_rls_blocks_unprivileged_clients_and_preserves_backend_processing(db):
    c, _ = db
    store.enqueue(c, 'push_to_monday', '101', key='private-job')
    c.execute(SCHEMA.read_text())
    assert c.execute("SELECT relrowsecurity FROM pg_class WHERE oid='public.job_queue'::regclass").fetchone()['relrowsecurity']
    # Role, grants and attempted writes exist only inside this rolled-back
    # transaction on the disposable loopback fixture, never in production.
    role = sql.Identifier('worker_rls_test_' + uuid4().hex)
    with c.transaction(force_rollback=True):
        c.execute(sql.SQL('CREATE ROLE {} NOLOGIN NOSUPERUSER NOBYPASSRLS').format(role))
        c.execute(sql.SQL('GRANT USAGE ON SCHEMA public TO {}').format(role))
        # Simulate a client with table grants, so RLS itself must deny access.
        c.execute(sql.SQL('GRANT SELECT,INSERT,UPDATE,DELETE ON public.job_queue TO {}').format(role))
        c.execute(sql.SQL('SET LOCAL ROLE {}').format(role))
        assert not c.execute('SELECT * FROM public.job_queue').fetchall()
        assert not c.execute("UPDATE public.job_queue SET status='completed' RETURNING id").fetchall()
        assert not c.execute('DELETE FROM public.job_queue RETURNING id').fetchall()
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            with c.transaction():
                c.execute("INSERT INTO public.job_queue(id,job_type,status) VALUES (%s,'push_to_monday','queued')", (uuid4(),))
    job = store.claim(c, 'general', 'backend')
    assert job is not None
    store.finish(c, job)
    assert c.execute('SELECT status FROM public.job_queue').fetchone()['status'] == 'completed'


def test_dedupe_never_resets_a_completed_job(db):
    c, _ = db
    job_id = store.enqueue(c, 'push_to_monday', '101', key='source')
    job = store.claim(c, 'general', 'one')
    store.finish(c, job)
    assert store.enqueue(c, 'push_to_monday', '101', key='source') == job_id
    row = c.execute('SELECT * FROM job_queue').fetchone()
    assert row['status'] == 'completed' and row['attempts'] == 1


def test_lease_expiry_cannot_duplicate_an_active_thread(db):
    c, _ = db
    store.enqueue(c, 'push_to_monday', '101')
    with store.connect() as owner, store.connect() as other:
        job = store.claim(owner, 'general', 'one')
        c.execute("UPDATE job_queue SET lease_until=now()-interval '1 minute'")
        assert store.claim(other, 'general', 'two') is None
        store.finish(owner, job)


def test_process_loss_recovers_job_and_fences_stale_ack(db):
    c, _ = db
    store.enqueue(c, 'push_to_monday', '101')
    with store.connect() as owner:
        old = store.claim(owner, 'general', 'one')
    c.execute("UPDATE job_queue SET lease_until=now()-interval '1 minute'")
    recovered = store.claim(c, 'general', 'two')
    assert recovered['attempts'] == 2 and recovered['lease_token'] != old['lease_token']
    with pytest.raises(RuntimeError, match='stale'):
        store.finish(c, old)
    store.finish(c, recovered)


def test_completion_and_followup_are_atomic(db):
    c, _ = db
    store.enqueue(c, 'rehydrate_and_analyze', '101')
    job = store.claim(c, 'general', 'one')
    with pytest.raises(psycopg.errors.NotNullViolation):
        store.finish(c, job, followups=[dict(name=None, project_id='101')])
    assert c.execute('SELECT status FROM job_queue').fetchone()['status'] == 'running'
    followup = dict(name='push_to_monday', project_id='101', key=str(job['id']) + ':push')
    store.finish(c, job, followups=[followup])
    rows = c.execute('SELECT status FROM job_queue ORDER BY created_at').fetchall()
    assert [r['status'] for r in rows] == ['completed', 'queued']


def test_retry_backoff_and_attempt_limit(db):
    c, _ = db
    store.enqueue(c, 'push_to_monday', '101')
    for attempt in range(1, store.MAX_ATTEMPTS + 1):
        with store.connect() as owner:
            job = store.claim(owner, 'general', 'one')
            assert job['attempts'] == attempt
            status = store.finish(owner, job, error=ConnectionError('secret DSN'))
        assert store.claim(c, 'general', 'other') is None
        c.execute('UPDATE job_queue SET available_at=now()')
    assert status == 'failed'
    row = c.execute('SELECT * FROM job_queue').fetchone()
    assert row['detail'] == 'ConnectionError'
    assert store.claim(c, 'general', 'other') is None


def test_webhook_receipt_and_job_are_deduplicated_and_acknowledged_together(db):
    c, _ = db
    payload = {'event': {'triggerUuid': 'delivery', 'value': {'label': 'Quoted'}}}
    first = store.accept_webhook('change_column_value', 'board', '101', payload)
    assert store.accept_webhook('change_column_value', 'board', '101', payload) == first
    assert c.execute('SELECT count(*) AS n FROM webhook_events').fetchone()['n'] == 1
    assert c.execute('SELECT count(*) AS n FROM job_queue').fetchone()['n'] == 1
    job = store.claim(c, 'webhook', 'receiver')
    store.finish(c, job)
    receipt = c.execute('SELECT status,processing_time_ms FROM webhook_events').fetchone()
    assert receipt['status'] == 'processed' and receipt['processing_time_ms'] >= 0


def test_webhook_enqueue_failure_rolls_back_receipt(db, monkeypatch):
    c, _ = db
    def fail(*a, **kw):
        raise ConnectionError('unavailable')
    monkeypatch.setattr(store, 'enqueue', fail)
    with pytest.raises(ConnectionError):
        store.accept_webhook('create_item', 'board', '101', {'event': {'id': 'one'}})
    assert c.execute('SELECT count(*) AS n FROM webhook_events').fetchone()['n'] == 0


def test_schedule_one_owner_across_replicas_and_no_replay(db):
    c, _ = db
    entered, release = threading.Event(), threading.Event()
    due = store.utcnow()
    def handler():
        entered.set()
        assert release.wait(5)
        return {'updated': 2}
    with ThreadPoolExecutor(2) as pool:
        first = pool.submit(store.run_scheduled, 'sync', due, 'one', handler)
        try:
            assert entered.wait(5)
            assert store.run_scheduled('sync', due, 'two', lambda: pytest.fail('duplicate'))['outcome'] == 'owned_elsewhere'
        finally:
            release.set()
        assert first.result()['outcome'] == 'succeeded'
    assert store.run_scheduled('sync', due, 'two', lambda: pytest.fail('replayed'))['outcome'] == 'already_recorded'
    assert c.execute('SELECT count(*) AS n FROM worker_job_runs').fetchone()['n'] == 1


def test_schedule_partial_failure_and_exception_are_durable(db):
    c, _ = db
    assert store.run_scheduled('sync', store.utcnow(), 'one', lambda: {'errors': 2, 'synced': 3})['outcome'] == 'partial'
    def boom():
        raise ConnectionError('credentials must not appear')
    with pytest.raises(ConnectionError):
        store.run_scheduled('broken', store.utcnow(), 'one', boom)
    row = c.execute("SELECT * FROM worker_job_runs WHERE job_id='broken'").fetchone()
    assert row['outcome'] == 'failed' and row['error_type'] == 'ConnectionError'


def test_unresolved_schedule_blocks_later_occurrences_even_after_session_loss(db):
    c, _ = db
    c.execute("INSERT INTO worker_job_runs(id,job_id,scheduled_for,instance_id,outcome) "
              "VALUES (%s,'sync',now()-interval '1 day','dead','running')", (uuid4(),))
    result = store.run_scheduled('sync', store.utcnow(), 'new', lambda: pytest.fail('overlapping execution'))
    assert result['outcome'] == 'previous_run_unresolved'


def test_schedule_claim_advances_expectation_before_long_running_work(db):
    c, _ = db
    due = store.utcnow()
    c.execute('INSERT INTO worker_schedules(job_id,next_due_at) VALUES (%s,%s)', ('slow', due - timedelta(hours=1)))
    def handler():
        row = c.execute("SELECT next_due_at FROM worker_schedules WHERE job_id='slow'").fetchone()
        assert row['next_due_at'] == due + timedelta(hours=6)
        assert not any(a['issue'] == 'schedule_needs_attention' for a in operational_status(c)['alerts'])
        return {}
    store.run_scheduled('slow', due, 'one', handler, next_due=due + timedelta(hours=6))


def test_same_target_waits_for_older_retry_but_other_targets_can_run(db):
    c, _ = db
    store.enqueue(c, 'push_to_monday', '101')
    with store.connect() as owner:
        first = store.claim(owner, 'general', 'one')
        store.finish(owner, first, error=ConnectionError())
    store.enqueue(c, 'rehydrate_and_analyze', '101')
    assert store.claim(c, 'general', 'other') is None
    other_id = store.enqueue(c, 'push_to_monday', '102')
    assert str(store.claim(c, 'general', 'other')['id']) == other_id


def test_watchdog_detects_missing_roles_stale_instances_queues_and_schedules(db):
    c, _ = db
    c.execute("INSERT INTO worker_heartbeats(instance_id,service,started_at,heartbeat_at,status) "
              "VALUES ('dead','api',now(),now()-interval '5 minutes','{}')")
    store.enqueue(c, 'push_to_monday', '101')
    c.execute("UPDATE job_queue SET available_at=now()-interval '1 hour',created_at=now()-interval '1 hour'")
    c.execute("INSERT INTO worker_schedules VALUES ('sync',now()-interval '1 hour',now())")
    report = operational_status(c, expected_services=['api', 'webhook'])
    issues = {a['issue'] for a in report['alerts']}
    assert {'heartbeat_missing', 'service_missing', 'queue_needs_attention', 'schedule_needs_attention'} <= issues
    record_alerts(c, report)
    record_alerts(c, {'alerts': []})
    assert not c.execute('SELECT * FROM worker_alerts WHERE resolved_at IS NULL').fetchall()
