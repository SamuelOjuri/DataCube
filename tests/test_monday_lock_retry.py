"""Lock-timeout retries for archive-gated writes. Offline: database and Monday are fakes."""
from contextlib import contextmanager

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
