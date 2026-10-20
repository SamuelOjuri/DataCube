"""Lock-timeout retries for archive-gated writes. Offline: database and Monday are fakes."""
from contextlib import contextmanager
import time

import psycopg
import pytest

from scripts import order_value_monday_compare as compare
from src.services import monday_archive as archive
from src.services import monday_lifecycle as life

IDLE = psycopg.pq.TransactionStatus.IDLE


def lock_timeout():
    return psycopg.errors.LockNotAvailable('canceling statement due to lock timeout')


class FakeConnection:
    """Autocommit connection whose LOCK TABLE times out a set number of times."""

    def __init__(self, lock_failures=0):
        self.lock_failures = lock_failures
        self.closed = False
        self.statements, self.commits, self.rollbacks = [], 0, 0
        self.info = type('Info', (), {'transaction_status': IDLE})()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    @contextmanager
    def transaction(self):
        self.info.transaction_status = psycopg.pq.TransactionStatus.INTRANS
        try:
            yield
        except BaseException:
            self.rollbacks += 1
            raise
        else:
            self.commits += 1
        finally:
            self.info.transaction_status = IDLE

    def execute(self, statement, params=None):
        text = str(statement)
        self.statements.append(text)
        if text.startswith('LOCK TABLE') and self.lock_failures:
            self.lock_failures -= 1
            raise lock_timeout()
        return type('Cursor', (), {'fetchone': lambda s: None, 'fetchall': lambda s: []})()


@pytest.fixture
def no_sleep(monkeypatch):
    sleeps = []
    monkeypatch.setattr(life.time, 'sleep', sleeps.append)
    return sleeps


def failing_then(result, failures):
    calls = []

    def write():
        calls.append(1)
        if len(calls) <= failures:
            raise lock_timeout()
        return result
    return write, calls


def test_lock_timeouts_rerun_the_whole_transaction(no_sleep):
    write, calls = failing_then('written', 2)
    assert life.retry_lock_conflicts(FakeConnection(), write) == 'written'
    assert len(calls) == 3 and len(no_sleep) == 2


def test_lock_timeouts_fail_after_the_retry_budget(no_sleep):
    write, calls = failing_then('never', 99)
    with pytest.raises(psycopg.errors.LockNotAvailable):
        life.retry_lock_conflicts(FakeConnection(), write)
    assert len(calls) == len(life.LOCK_RETRY_DELAYS) + 1
    assert all(0.5 * d <= s <= 1.5 * d for d, s in zip(life.LOCK_RETRY_DELAYS, no_sleep, strict=True))


def test_evidence_failures_are_never_retried(no_sleep):
    calls = []

    def write():
        calls.append(1)
        raise life.ReviewRequired('Monday changed')
    with pytest.raises(life.ReviewRequired):
        life.retry_lock_conflicts(FakeConnection(), write)
    assert len(calls) == 1 and not no_sleep


def test_lock_timeouts_inside_an_outer_transaction_are_not_retried(no_sleep):
    connection = FakeConnection()
    connection.info.transaction_status = psycopg.pq.TransactionStatus.INERROR
    write, calls = failing_then('never', 99)
    with pytest.raises(psycopg.errors.LockNotAvailable):
        life.retry_lock_conflicts(connection, write)
    assert len(calls) == 1 and not no_sleep


def test_sync_gate_retries_its_locked_transaction_after_lock_timeouts(monkeypatch, no_sleep):
    connection = FakeConnection(lock_failures=2)
    source = {'101': {'state': 'active', 'board': {'id': next(iter(life.BOARDS))}, 'parent_item': None}}
    observed = []
    monkeypatch.setattr(archive, 'enabled', lambda: True)
    monkeypatch.setattr(archive, 'require_runtime', lambda c: None)
    monkeypatch.setattr(archive, 'observe', lambda *args: observed.append(args[3]))
    monkeypatch.setattr(life, 'connect', lambda: connection)
    monkeypatch.setattr(life, 'read_items', lambda monday, ids, **kw: {})
    monkeypatch.setattr(life, 'read_rows', lambda *args: [])
    monkeypatch.setattr(compare, 'ComparisonMondayClient', lambda: object())
    monkeypatch.setattr(compare, 'fetch_items', lambda *args, **kw: source)

    rows = archive.prepare_sync_rows('projects', [{'monday_id': '101', 'item_name': 'x', 'total_order_value': 5}])

    assert rows == [{'monday_id': '101', 'item_name': 'x', **{f: None for f in archive.FINANCIAL_FIELDS}}]
    assert sum(s.startswith('LOCK TABLE') for s in connection.statements) == 3
    assert (connection.rollbacks, connection.commits) == (2, 1)
    assert observed == ['101']


@pytest.fixture
def refresh_one(monkeypatch):
    connection = FakeConnection()
    job = {'event_key': 'recovery:k:current-values', 'lease_token': 't', 'attempts': 1}
    finished = []
    monkeypatch.setattr(archive, 'require_runtime', lambda c: None)
    monkeypatch.setattr(archive, 'enqueue_refresh', lambda c, parent, key: key)
    monkeypatch.setattr(life, 'connect', lambda: connection)
    monkeypatch.setattr(life, 'claim', lambda c, **kw: job)
    monkeypatch.setattr(life, 'check_lease', lambda c, j: None)
    monkeypatch.setattr(life, 'finish', lambda c, j, status, result, error=None: finished.append((status, error)))
    monkeypatch.setattr(compare, 'ComparisonMondayClient', lambda: object())
    return connection, finished


def test_parent_refresh_returns_transient_failures_to_the_queue(refresh_one, monkeypatch):
    connection, finished = refresh_one

    def timed_out(*args):
        raise lock_timeout()
    monkeypatch.setattr(archive, 'refresh_current_values', timed_out)
    with pytest.raises(psycopg.errors.LockNotAvailable):
        archive.refresh_parents(['101'])
    assert finished == [('retry', 'LockNotAvailable')]
    assert not any(s.startswith('LOCK TABLE') for s in connection.statements)


def test_parent_refresh_review_survives_lock_timeouts(refresh_one, monkeypatch, no_sleep):
    connection, finished = refresh_one
    connection.lock_failures = 1

    def changed(*args):
        raise life.ReviewRequired('Current-value baseline changed')
    monkeypatch.setattr(archive, 'refresh_current_values', changed)
    with pytest.raises(life.ReviewRequired):
        archive.refresh_parents(['101'])
    assert finished == [('review', 'Current-value baseline changed')]
    assert len(no_sleep) == 1


@pytest.fixture
def finishes(monkeypatch):
    fresh, calls = FakeConnection(), []

    def finish(c, job, status, result, error=None):
        calls.append((c, status, error))
        if getattr(c, 'dies_on_finish', False):
            c.closed = True
            raise psycopg.OperationalError('server closed the connection unexpectedly')
    monkeypatch.setattr(life, 'finish', finish)
    monkeypatch.setattr(life, 'connect', lambda: fresh)
    return fresh, calls


def test_terminated_session_releases_its_claim_on_a_fresh_connection(finishes):
    fresh, calls = finishes
    dead = FakeConnection()
    dead.closed = True
    life.release(dead, {'event_key': 'k'}, 'retry', {}, error='TransactionTimeout')
    assert calls == [(fresh, 'retry', 'TransactionTimeout')]


def test_session_dying_during_release_falls_back_to_a_fresh_connection(finishes):
    fresh, calls = finishes
    dying = FakeConnection()
    dying.dies_on_finish = True
    life.release(dying, {'event_key': 'k'}, 'retry', {})
    assert [c for c, _, _ in calls] == [dying, fresh]


def test_release_errors_on_a_live_connection_are_not_hidden(finishes, monkeypatch):
    def broken(*args, **kwargs):
        raise psycopg.OperationalError('statement failed')
    monkeypatch.setattr(life, 'finish', broken)
    with pytest.raises(psycopg.OperationalError):
        life.release(FakeConnection(), {'event_key': 'k'}, 'retry', {})


def test_lifecycle_worker_releases_claims_after_a_transaction_timeout(finishes, monkeypatch):
    fresh, calls = finishes
    dead = FakeConnection()
    job = {'event_key': 'k', 'item_id': '101', 'attempts': 1, 'lease_token': 't'}
    monkeypatch.setattr(life, 'claim', lambda c, **kw: job)

    def timed_out(connection, monday, job):
        dead.closed = True
        raise psycopg.errors.TransactionTimeout('terminating connection due to transaction timeout')
    monkeypatch.setattr(life, 'process_job', timed_out)
    assert life.run_once(dead, object()) is True
    assert calls == [(fresh, 'retry', 'TransactionTimeout')]


def test_parent_refresh_releases_claims_after_a_transaction_timeout(refresh_one, finishes, monkeypatch):
    connection, _ = refresh_one
    fresh, calls = finishes
    connections = iter([connection, fresh])
    monkeypatch.setattr(life, 'connect', lambda: next(connections))

    def timed_out(*args):
        connection.closed = True
        raise psycopg.errors.TransactionTimeout('terminating connection due to transaction timeout')
    monkeypatch.setattr(archive, 'refresh_current_values', timed_out)
    with pytest.raises(psycopg.errors.TransactionTimeout):
        archive.refresh_parents(['101'])
    assert calls == [(fresh, 'retry', 'TransactionTimeout')]


class FakeRequest:
    def __init__(self, errors):
        self.errors, self.calls = list(errors), 0

    def execute(self):
        self.calls += 1
        if self.errors:
            raise self.errors.pop(0)
        return type('Response', (), {'data': [{'monday_id': '201'}]})()


def rest_error(code):
    from postgrest.exceptions import APIError
    return APIError({'code': code, 'message': 'canceling statement', 'hint': None, 'details': None})


@pytest.fixture
def rest(monkeypatch):
    from src.database import supabase_client as sc
    sleeps = []
    monkeypatch.setattr(time, 'sleep', sleeps.append)
    monkeypatch.delenv('MONDAY_ARCHIVE_ENABLED', raising=False)
    client = sc.SupabaseClient.__new__(sc.SupabaseClient)
    return sc, client, sleeps


@pytest.mark.parametrize('code', ['55P03', '57014'])
def test_rest_upserts_retry_postgrest_lock_waits(rest, code):
    sc, client, sleeps = rest
    request = FakeRequest([rest_error(code), rest_error(code)])
    client.client = type('Client', (), {'table': lambda s, name: type('T', (), {'upsert': lambda s, rows, on_conflict: request})()})()
    assert client.upsert_subitems([{'monday_id': '201'}]) == {'success': True, 'count': 1}
    assert request.calls == 3 and len(sleeps) == 2


def test_rest_upserts_report_other_errors_without_retrying(rest):
    sc, client, sleeps = rest
    request = FakeRequest([rest_error('23505')])
    client.client = type('Client', (), {'table': lambda s, name: type('T', (), {'upsert': lambda s, rows, on_conflict: request})()})()
    assert client.upsert_hidden_items([{'monday_id': '301'}])['success'] is False
    assert request.calls == 1 and not sleeps


def test_rest_upserts_give_up_after_bounded_lock_waits(rest):
    from postgrest.exceptions import APIError
    sc, client, sleeps = rest
    request = FakeRequest([rest_error('57014')] * 10)
    with pytest.raises(APIError):
        sc.execute_with_lock_retry(request)
    assert request.calls == len(sc.LOCK_WAIT_RETRY_DELAYS) + 1


class RecordingConnection(FakeConnection):
    def __init__(self):
        super().__init__()
        self.batches = []

    def execute(self, statement, params=None):
        self.statements.append(str(statement))
        return type('Cursor', (), {'fetchall': lambda s: [], 'fetchone': lambda s: None})()

    @contextmanager
    def cursor(self):
        batches = self.batches

        class Cursor:
            def executemany(self, statement, rows):
                batches.append((str(statement), list(rows)))
        yield Cursor()


def test_source_observations_use_one_read_and_two_batched_writes():
    from datetime import datetime, timezone
    boards = {table: board for board, table in life.BOARDS.items()}

    def evidence(table, state='active', parent=None):
        return {'state': state, 'board': {'id': boards[table]}, 'parent_item': {'id': parent} if parent else None}
    source = {'projects': {'101': evidence('projects')},
              'subitems': {str(200 + i): evidence('subitems', parent='101') for i in range(40)},
              'hidden_items': {'301': evidence('hidden_items'), '302': evidence('hidden_items', 'archived')}}
    connection = RecordingConnection()
    archive.observe_source(connection, {'event_key': 'k'}, source, datetime.now(timezone.utc))
    assert len(connection.statements) == 1
    (observation_sql, observations), (audit_sql, audits) = connection.batches
    assert 'monday_item_lifecycle' in observation_sql and 'monday_lifecycle_audit' in audit_sql
    assert len(observations) == len(audits) == 42
    assert '302' not in {row[1] for row in observations}
