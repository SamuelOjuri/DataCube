"""Missing-key insertion protocol on explicitly configured loopback PostgreSQL.

Reuses the existing fixture that creates/drops a randomly named test database.
Only Monday reads are mocked; constraints, locks, SQL writes and journals are real.
"""
from copy import deepcopy
import threading

import psycopg
from psycopg import sql
import pytest
import requests

from scripts import backfill_order_values as backfill
from scripts import order_value_rehydrate as hydrate
from scripts import order_value_scopes as scopes
from scripts import reconcile_order_values as reconcile
from test_order_value_rehydrate import example
from test_order_value_scopes_postgres import database


@pytest.fixture(autouse=True)
def no_http(monkeypatch):
    monkeypatch.setattr(requests.sessions.Session, 'request',
                        lambda *a, **kw: pytest.fail('PostgreSQL tests must not contact Monday or Supabase HTTP'))


@pytest.fixture
def reviewed(database, tmp_path):
    connection, dsn, _, scope = database
    _, source, raw, schema = example()
    current = reconcile.read_contract(connection)
    for table in scopes.TABLES:
        for field, column in schema[table].items():
            if field in current[table]:
                continue
            kind = 'numeric(12,2)' if column['type'] == 'numeric' else 'text'
            if 'date' in field: kind = 'date'
            connection.execute(sql.SQL('ALTER TABLE {} ADD COLUMN {} {}').format(
                sql.Identifier(table), sql.Identifier(field), sql.SQL(kind)))
    for table in ('hidden_items', 'subitems'):
        connection.execute(sql.SQL('ALTER TABLE {} ADD COLUMN id uuid NOT NULL DEFAULT gen_random_uuid(), '
                                   'ADD COLUMN created_at timestamptz NOT NULL DEFAULT now()').format(sql.Identifier(table)))
    connection.execute('''ALTER TABLE projects ADD COLUMN invoicing_spread_days integer GENERATED ALWAYS AS
        (CASE WHEN first_date_invoiced IS NOT NULL AND last_date_invoiced IS NOT NULL
        THEN greatest(last_date_invoiced-first_date_invoiced,0) END) STORED''')
    connection.execute("DELETE FROM subitems WHERE monday_id='201'")
    connection.execute("DELETE FROM hidden_items WHERE monday_id='301'")
    contract = reconcile.read_contract(connection)
    before = scopes.read_boundary(connection, scope, full=True)
    record = hydrate.make_record(before, source, raw, contract, ['101'])
    staged = {'mode': 'repair', 'contract': contract, 'insert_contract': hydrate.insert_contract(connection),
              'selected_project_ids': ['101'], 'scopes': [record], 'deferred': []}
    path = tmp_path / 'run'
    manifest = hydrate.save_run(path, staged, backfill.target_fingerprint(connection), scopes.schema_safety(connection))
    return connection, dsn, source, path, manifest, staged, record


def test_atomic_inserts_rollups_defaults_journal_and_resume(reviewed):
    connection, _, _, _, manifest, staged, record = reviewed
    result = hydrate.commit_scope(connection, manifest, staged, record)
    assert result['inserted_rows'] == {'projects': 0, 'hidden_items': 1, 'subitems': 1}
    actual = scopes.read_boundary(connection, record['boundary'], full=True)
    hydrate.validate_actual(actual, record)
    assert actual['hidden_items'][0]['id'] and actual['subitems'][0]['created_at']
    assert actual['projects'][0]['total_order_value'] == '105.00'
    journal = connection.execute('SELECT after_sha256 FROM order_value_scope_commits').fetchone()[0]
    assert journal == backfill.fingerprint(actual)
    connection.execute("UPDATE projects SET total_order_value=999 WHERE monday_id='101'")
    assert hydrate.commit_scope(connection, manifest, staged, record)['status'] == 'already_committed'
    assert connection.execute("SELECT total_order_value FROM projects WHERE monday_id='101'").fetchone()[0] == 999


@pytest.mark.parametrize('statement', [
    "INSERT INTO hidden_items(monday_id) VALUES ('301')",
    "INSERT INTO subitems(monday_id,parent_monday_id) VALUES ('201','101')",
    "INSERT INTO subitems(monday_id,parent_monday_id) VALUES ('new-child','101')",
    "UPDATE projects SET item_name='writer change' WHERE monday_id='101'",
])
def test_changes_after_staging_refuse_writes(reviewed, statement):
    connection, _, _, _, manifest, staged, record = reviewed
    connection.execute(statement)
    before_attempt = scopes.read_boundary(connection, record['boundary'], full=True)
    with pytest.raises(scopes.ScopeConflict, match='changed since staging'):
        hydrate.commit_scope(connection, manifest, staged, record)
    assert scopes.read_boundary(connection, record['boundary'], full=True) == before_attempt
    assert not scopes.committed_scopes(connection, manifest)


@pytest.mark.parametrize('statement', [
    "INSERT INTO hidden_items(monday_id) VALUES ('301')",
    "INSERT INTO subitems(monday_id,parent_monday_id) VALUES ('new-child','101')",
    "UPDATE subitems SET hidden_item_id='301' WHERE monday_id='299'",
    "UPDATE projects SET item_name='unrelated' WHERE monday_id='999'",
])
def test_brief_write_locks_protect_missing_keys_but_allow_reads(reviewed, monkeypatch, statement):
    connection, dsn, _, _, manifest, staged, record = reviewed
    original = hydrate.write_inserts
    def inspect_locked(conn, inserts):
        with psycopg.connect(dsn, autocommit=True) as writer:
            writer.execute("SET lock_timeout='150ms'")
            assert writer.execute('SELECT count(*) FROM projects').fetchone()[0] == 2
            with pytest.raises(psycopg.errors.LockNotAvailable):
                writer.execute(statement)
        return original(conn, inserts)
    monkeypatch.setattr(hydrate, 'write_inserts', inspect_locked)
    hydrate.commit_scope(connection, manifest, staged, record)


def test_trigger_corruption_rolls_back_hidden_child_parent_and_journal(reviewed):
    connection, _, _, _, manifest, staged, record = reviewed
    connection.execute('''CREATE FUNCTION corrupt_insert() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN NEW.cust_additional_charges=999; RETURN NEW; END $$;
        CREATE TRIGGER corrupt BEFORE INSERT ON subitems FOR EACH ROW EXECUTE FUNCTION corrupt_insert()''')
    with pytest.raises(scopes.ScopeConflict, match='Post-write'):
        hydrate.commit_scope(connection, manifest, staged, record)
    assert scopes.read_boundary(connection, record['boundary'], full=True) == record['before']
    assert not scopes.committed_scopes(connection, manifest)


def test_changed_default_refuses_insert(reviewed):
    connection, _, _, _, manifest, staged, record = reviewed
    connection.execute("ALTER TABLE hidden_items ALTER COLUMN created_at SET DEFAULT '2000-01-01'::timestamptz")
    with pytest.raises(scopes.ScopeConflict, match='schema/defaults'):
        hydrate.commit_scope(connection, manifest, staged, record)


def test_two_applicators_commit_only_once(reviewed):
    _, dsn, _, _, manifest, staged, record = reviewed
    barrier, statuses, errors = threading.Barrier(2), [], []
    def apply():
        try:
            with psycopg.connect(dsn, autocommit=True) as worker:
                barrier.wait(timeout=5)
                statuses.append(hydrate.commit_scope(worker, manifest, staged, record)['status'])
        except Exception as exc:
            errors.append(exc)
    workers = [threading.Thread(target=apply) for _ in range(2)]
    for worker in workers: worker.start()
    for worker in workers: worker.join(timeout=15)
    assert not errors
    assert sorted(statuses) == ['already_committed', 'committed_pending_source_verification']


def source_checks(monkeypatch, source, record):
    scans = []
    def capture(*a):
        scans.append('scan')
        return {'items': source['subitems']}
    monkeypatch.setattr(hydrate.reads, 'capture_ownership', capture)
    # Keep check_scope's real independent owner comparison.
    monkeypatch.setattr(scopes, 'targeted_orders', lambda *a: deepcopy(record['source']))
    monkeypatch.setattr(reconcile, 'capture_targeted', lambda *a: deepcopy(record['raw']))
    return scans


def test_apply_verify_and_defaults_drift_detection(reviewed, monkeypatch):
    connection, _, source, path, manifest, _, record = reviewed
    scans = source_checks(monkeypatch, source, record)
    result = hydrate.execute_run(connection, None, path, apply=True,
        confirm_run_id=manifest['run_id'], allow_rehydration=True)
    assert result['remaining_uncommitted'] == 0
    assert hydrate.execute_run(connection, None, path)['complete']
    assert scans == ['scan', 'scan']
    connection.execute("UPDATE subitems SET created_at='2000-01-01' WHERE monday_id='201'")
    result = hydrate.execute_run(connection, None, path)
    assert not result['complete'] and result['counts'] == {'changed_requires_reassessment': 1}


def test_new_monday_owner_prevents_insert(reviewed, monkeypatch):
    connection, _, source, path, manifest, _, record = reviewed
    source_checks(monkeypatch, source, record)
    source['subitems'].append({**source['subitems'][0], 'monday_id': '299', 'parent_monday_id': '999'})
    result = hydrate.execute_run(connection, None, path, apply=True,
        confirm_run_id=manifest['run_id'], allow_rehydration=True)
    assert result['counts'] == {'deferred': 1}
    assert scopes.read_boundary(connection, record['boundary'], full=True) == record['before']


def test_lost_receipt_resumes_via_journal_without_reapplying(reviewed, monkeypatch):
    connection, _, source, path, manifest, _, record = reviewed
    source_checks(monkeypatch, source, record)
    original = backfill.write_json
    def fail_receipt(path, data):
        if path.name.startswith('scope-'): raise OSError('disk error')
        return original(path, data)
    monkeypatch.setattr(backfill, 'write_json', fail_receipt)
    with pytest.raises(OSError):
        hydrate.execute_run(connection, None, path, apply=True,
            confirm_run_id=manifest['run_id'], allow_rehydration=True)
    assert scopes.committed_scopes(connection, manifest) == {record['scope_id']}
    monkeypatch.setattr(backfill, 'write_json', original)
    result = hydrate.execute_run(connection, None, path, apply=True,
        confirm_run_id=manifest['run_id'], allow_rehydration=True)
    assert result['results'] == [] and result['remaining_uncommitted'] == 0
    assert hydrate.execute_run(connection, None, path)['complete']


def test_read_only_stage_rechecks_current_absence(reviewed, monkeypatch, tmp_path):
    connection, _, source, _, _, _, record = reviewed
    monkeypatch.setattr(hydrate.refresh, 'capture_selected', lambda *a: (source, {'started_at': 'test'}))
    monkeypatch.setattr(reconcile, 'capture_targeted', lambda *a: deepcopy(record['raw']))
    monkeypatch.setattr(scopes, 'targeted_orders', lambda *a: deepcopy(record['source']))
    manifest = hydrate.stage_run(connection, None, tmp_path / 'fresh-stage', ['101'])
    assert manifest['insert_rows']['subitems'] == 1
    assert scopes.read_boundary(connection, record['boundary'], full=True) == record['before']
    assert connection.execute('SELECT count(*) FROM order_value_scope_commits').fetchone()[0] == 0
