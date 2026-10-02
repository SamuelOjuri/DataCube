"""Real PostgreSQL concurrency tests; ONLY an explicitly supplied loopback server.

ORDER_SCOPE_TEST_DSN must name database postgres on localhost. Each test creates
and drops its own randomly named order_scope_test_* database; no production DSN
is ever read. Requires PostgreSQL 17+ and permission to create test databases.
"""
from copy import deepcopy
import os
from pathlib import Path
import threading
import time
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
import pytest

from scripts import backfill_order_values as backfill
from scripts import order_value_scopes as scopes
from scripts import reconcile_order_values as reconcile
from test_order_value_scopes import sample


@pytest.fixture
def database():
    dsn = os.getenv('ORDER_SCOPE_TEST_DSN')
    if not dsn:
        pytest.skip('Set ORDER_SCOPE_TEST_DSN to an isolated local PostgreSQL 17+ server')
    info = conninfo_to_dict(dsn)
    if info.get('host') not in {'127.0.0.1', '::1', 'localhost'} or info.get('dbname') != 'postgres':
        pytest.fail('Integration tests require an explicit loopback postgres admin database')
    name = 'order_scope_test_' + uuid4().hex
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
        local_dsn = make_conninfo(dsn, dbname=name)
        try:
            with psycopg.connect(local_dsn, autocommit=True) as connection:
                connection.execute('''
                    CREATE TABLE projects (monday_id text PRIMARY KEY, item_name text,
                        total_order_value numeric(12,2), new_enquiry_value numeric(12,2),
                        total_amount_invoiced numeric(12,2), date_order_received date);
                    CREATE TABLE hidden_items (monday_id text PRIMARY KEY,
                        cust_order_value_material numeric(12,2), cust_additional_charges numeric(12,2),
                        amount_invoiced numeric(12,2), invoice_date date, date_order_received date);
                    CREATE TABLE subitems (monday_id text PRIMARY KEY,
                        parent_monday_id text REFERENCES projects(monday_id) ON DELETE CASCADE,
                        hidden_item_id text REFERENCES hidden_items(monday_id) ON DELETE SET NULL,
                        cust_order_value_material numeric(12,2), cust_additional_charges numeric(12,2),
                        amount_invoiced numeric(12,2), invoice_date date, date_order_received date)
                ''')
                connection.execute(scopes.SCHEMA_PATH.read_text())
                baseline, source, scope = sample()
                for table in ('projects', 'hidden_items', 'subitems'):
                    for row in baseline[table]:
                        connection.execute(sql.SQL('INSERT INTO public.{} ({}) VALUES ({})').format(
                            sql.Identifier(table), sql.SQL(',').join(map(sql.Identifier, row)),
                            sql.SQL(',').join(sql.Placeholder() for _ in row)), list(row.values()))
                connection.execute("INSERT INTO projects(monday_id,total_order_value) VALUES ('999',50)")
                connection.execute("INSERT INTO hidden_items(monday_id) VALUES ('399')")
                connection.execute("INSERT INTO subitems(monday_id,parent_monday_id,hidden_item_id) VALUES ('299','999','399')")
                yield connection, local_dsn, source, scope
        finally:
            # The random database is created above and never comes from user data.
            admin.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(name)))


def reviewed(database):
    connection, _, source, scope = database
    before = scopes.read_boundary(connection, scope)
    evidence = scopes.source_evidence(source, scope)
    updates = scopes.order_updates(before, evidence, scope)
    record = {'scope_id': 'example', 'scope': scope, 'boundary': scope, 'before': before,
              'after': scopes.expected_state(before, updates), 'source': evidence, 'updates': updates, 'raw': None}
    staged = {'mode': 'orders', 'contract': reconcile.read_contract(connection), 'scopes': [record]}
    manifest = {'run_id': str(uuid4()), 'sha256': backfill.fingerprint(staged)}
    return manifest, staged, record


def test_real_set_based_commit_journal_and_resume_after_later_writer(database):
    connection, _, _, _ = database
    manifest, staged, record = reviewed(database)
    assert scopes.schema_safety(connection)['foreign_keys_verified']
    assert scopes.commit_scope(connection, manifest, staged, record)['status'] == 'committed_pending_source_verification'
    assert scopes.read_boundary(connection, record['boundary']) == record['after']
    assert scopes.committed_scopes(connection, manifest) == {'example'}
    connection.execute("UPDATE projects SET total_order_value=999 WHERE monday_id='101'")
    assert scopes.commit_scope(connection, manifest, staged, record)['status'] == 'already_committed'
    assert connection.execute("SELECT total_order_value FROM projects WHERE monday_id='101'").fetchone()[0] == 999


@pytest.mark.parametrize('statement', [
    "INSERT INTO subitems(monday_id,parent_monday_id) VALUES ('new-child','101')",
    "INSERT INTO subitems(monday_id,parent_monday_id,hidden_item_id) VALUES ('new-owner','999','301')",
    "UPDATE subitems SET parent_monday_id='101' WHERE monday_id='299'",
    "UPDATE subitems SET hidden_item_id='301' WHERE monday_id='299'",
    "DELETE FROM subitems WHERE monday_id='201'",
])
def test_existing_fk_protocol_blocks_phantoms_relinks_and_deletes(database, statement):
    connection, dsn, _, scope = database
    with connection.transaction():
        scopes.read_boundary(connection, scope, lock=True)
        with psycopg.connect(dsn, autocommit=True) as writer:
            writer.execute("SET lock_timeout='150ms'")
            with pytest.raises(psycopg.errors.LockNotAvailable):
                writer.execute(statement)


def test_unrelated_project_remains_writable_while_scope_is_locked(database):
    connection, dsn, _, scope = database
    with connection.transaction():
        scopes.read_boundary(connection, scope, lock=True)
        with psycopg.connect(dsn, autocommit=True) as writer:
            writer.execute("SET statement_timeout='500ms'")
            writer.execute("UPDATE projects SET total_order_value=75 WHERE monday_id='999'")
            writer.execute("INSERT INTO subitems(monday_id,parent_monday_id,hidden_item_id) VALUES ('unrelated','999','399')")


@pytest.mark.parametrize('statement', [
    "INSERT INTO subitems(monday_id,parent_monday_id) VALUES ('new-child','101')",
    "UPDATE subitems SET hidden_item_id='301' WHERE monday_id='299'",
    "UPDATE projects SET total_order_value=999 WHERE monday_id='101'",
])
def test_preexisting_drift_defers_without_committing_any_correction(database, statement):
    connection, _, _, _ = database
    manifest, staged, record = reviewed(database)
    connection.execute(statement)
    with pytest.raises(scopes.ScopeConflict, match='changed'):
        scopes.commit_scope(connection, manifest, staged, record)
    assert not scopes.committed_scopes(connection, manifest)
    assert connection.execute("SELECT cust_order_value_material FROM hidden_items WHERE monday_id='301'").fetchone()[0] == 40


def test_failure_in_last_table_rolls_back_values_and_journal(database):
    connection, _, _, _ = database
    manifest, staged, record = reviewed(database)
    connection.execute("ALTER TABLE projects ADD CONSTRAINT fail_test CHECK(total_order_value < 100)")
    with pytest.raises(psycopg.errors.CheckViolation):
        scopes.commit_scope(connection, manifest, staged, record)
    assert scopes.read_boundary(connection, record['boundary']) == record['before']
    assert not scopes.committed_scopes(connection, manifest)


def test_bad_post_write_reconciliation_rolls_back_trigger_effects(database):
    connection, _, _, _ = database
    manifest, staged, record = reviewed(database)
    connection.execute('''CREATE FUNCTION sabotage_order_test() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN NEW.total_order_value = NEW.total_order_value + 1; RETURN NEW; END $$;
        CREATE TRIGGER sabotage BEFORE UPDATE ON projects FOR EACH ROW EXECUTE FUNCTION sabotage_order_test()''')
    with pytest.raises(scopes.ScopeConflict, match='Post-write'):
        scopes.commit_scope(connection, manifest, staged, record)
    assert scopes.read_boundary(connection, record['boundary']) == record['before']
    assert not scopes.committed_scopes(connection, manifest)


def test_missing_or_deferrable_fk_refuses_online_mode(database):
    connection, _, _, _ = database
    connection.execute('ALTER TABLE subitems ALTER CONSTRAINT subitems_hidden_item_id_fkey DEFERRABLE')
    with pytest.raises(ValueError, match='foreign keys'):
        scopes.schema_safety(connection)


def test_two_applicators_commit_each_scope_only_once(database):
    connection, dsn, _, _ = database
    manifest, staged, record = reviewed(database)
    barrier = threading.Barrier(2)
    statuses, errors = [], []

    def run():
        try:
            with psycopg.connect(dsn, autocommit=True) as worker:
                barrier.wait(timeout=5)
                statuses.append(scopes.commit_scope(worker, manifest, staged, record)['status'])
        except Exception as exc:
            errors.append(exc)

    workers = [threading.Thread(target=run) for _ in range(2)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=10)
    assert not errors
    assert sorted(statuses) == ['already_committed', 'committed_pending_source_verification']
    assert connection.execute('SELECT count(*) FROM order_value_scope_commits').fetchone()[0] == 1


def test_real_staging_and_artifact_reload(database, tmp_path, monkeypatch):
    connection, _, source, _ = database
    baseline = backfill.read_baseline(connection)
    source_without_exclusions = {k: source[k] for k in ('project_ids', 'subitems', 'hidden_items')}
    plan = backfill.build_plan(baseline, source_without_exclusions)
    origin = {'run_id': 'original', 'target': backfill.target_fingerprint(connection),
              'approve_reviewed_parentless_duplicates': True, 'approved_empty': [], 'summary': {'blocked_projects': 1}}
    monkeypatch.setattr(backfill, 'load_run', lambda path: (origin, baseline, source, plan))
    run_dir = tmp_path / 'review'
    manifest = scopes.stage_run(connection, None, tmp_path / 'capture', run_dir, mode='orders', project_ids={'101'})
    _, staged = scopes.load_run(run_dir)
    assert manifest['scopes'] == 1
    assert scopes.commit_scope(connection, manifest, staged, staged['scopes'][0])['status'].startswith('committed')


def test_lost_local_receipt_resumes_without_reapplying(database, tmp_path, monkeypatch):
    connection, _, source, _ = database
    baseline = backfill.read_baseline(connection)
    plan = backfill.build_plan(baseline, {k: source[k] for k in ('project_ids', 'subitems', 'hidden_items')})
    origin = {'run_id': 'original', 'target': backfill.target_fingerprint(connection),
              'approve_reviewed_parentless_duplicates': True, 'approved_empty': [], 'summary': {'blocked_projects': 1}}
    monkeypatch.setattr(backfill, 'load_run', lambda path: (origin, baseline, source, plan))
    run_dir = tmp_path / 'review'
    manifest = scopes.stage_run(connection, None, tmp_path / 'capture', run_dir, mode='orders', project_ids={'101'})
    _, staged = scopes.load_run(run_dir)
    record = staged['scopes'][0]
    monkeypatch.setattr(scopes, 'fresh_capture', lambda *a: source)
    monkeypatch.setattr(scopes, 'targeted_orders', lambda *a: record['source'])
    original_write = backfill.write_json
    monkeypatch.setattr(backfill, 'write_json', lambda *a: (_ for _ in ()).throw(OSError('simulated local disk failure')))
    with pytest.raises(OSError):
        scopes.apply_run(connection, None, run_dir, confirm_run_id=manifest['run_id'], allow_partial=True)
    assert len(scopes.committed_scopes(connection, manifest)) == 1
    monkeypatch.setattr(backfill, 'write_json', original_write)
    resumed = scopes.apply_run(connection, None, run_dir, confirm_run_id=manifest['run_id'], allow_partial=True)
    assert resumed['previously_committed'] == 1 and resumed['results'] == []
    assert scopes.verify_run(connection, None, run_dir)['complete']


def test_transaction_deadline_rolls_back_and_journal_resolves_connection_loss(database):
    connection, dsn, _, _ = database
    manifest, staged, record = reviewed(database)
    connection.execute('''CREATE FUNCTION stall_order_test() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN PERFORM pg_sleep(20); RETURN NEW; END $$;
        CREATE TRIGGER stall BEFORE UPDATE ON projects FOR EACH ROW EXECUTE FUNCTION stall_order_test()''')
    started = time.monotonic()
    # The four-second statement budget usually expires before the total ten-second
    # transaction budget. Either server cancellation must leave no commit behind.
    with pytest.raises((psycopg.errors.QueryCanceled, psycopg.OperationalError)):
        scopes.commit_scope(connection, manifest, staged, record)
    assert time.monotonic() - started < 12
    with psycopg.connect(dsn, autocommit=True) as verifier:
        assert scopes.read_boundary(verifier, record['boundary']) == record['before']
        assert not scopes.committed_scopes(verifier, manifest)


def test_real_repair_stages_broad_fields_and_preserves_unrelated_rows(database, tmp_path, monkeypatch):
    from test_reconcile_order_values import raw_rows

    connection, _, source, scope = database
    hidden, children = raw_rows()
    for item in hidden + children:
        item['state'] = 'active'
    children[0]['parent_item'].update({'state': 'active', 'board': {'id': backfill.PARENT_BOARD_ID}})
    for field, value in zip(backfill.ORDER_FIELDS, ('100', '5')):
        next(c for c in hidden[0]['column_values'] if c['id'] == backfill.HIDDEN_ITEMS_COLUMNS[field])['value'] = value
    hidden[0]['column_values'] = [c for c in hidden[0]['column_values'] if c['id'] != backfill.TOTAL_COLUMN]
    hidden[0]['column_values'].append({'id': backfill.TOTAL_COLUMN, 'value': None, 'display_value': '105'})
    raw = {'hidden_items': hidden, 'subitems': children}
    transformed = reconcile.transform_exact_rows(hidden, children, {'101'})
    contract = reconcile.read_contract(connection)
    for table, rows in transformed.items():
        for field, value in rows[0].items():
            if field not in contract[table]:
                datatype = 'date' if 'date' in field else 'numeric(12,2)' if isinstance(value, float) else 'text'
                connection.execute(sql.SQL('ALTER TABLE {} ADD COLUMN {} {}').format(
                    sql.Identifier(table), sql.Identifier(field), sql.SQL(datatype)))
    connection.execute('ALTER TABLE projects ADD COLUMN invoicing_spread_days integer GENERATED ALWAYS AS '
        '(CASE WHEN first_date_invoiced IS NOT NULL AND last_date_invoiced IS NOT NULL '
        'THEN greatest(last_date_invoiced-first_date_invoiced,0) END) STORED')
    connection.execute("UPDATE subitems SET hidden_item_id='399' WHERE monday_id='201'")
    baseline = backfill.read_baseline(connection)
    plan = backfill.build_plan(baseline, {k: source[k] for k in ('project_ids', 'subitems', 'hidden_items')})
    origin = {'run_id': 'original', 'target': backfill.target_fingerprint(connection),
              'approve_reviewed_parentless_duplicates': True, 'approved_empty': [], 'summary': {'blocked_projects': 2}}
    monkeypatch.setattr(backfill, 'load_run', lambda path: (origin, baseline, source, plan))
    monkeypatch.setattr(reconcile, 'capture_targeted', lambda *a: deepcopy(raw))
    run_dir = tmp_path / 'repair'
    manifest = scopes.stage_run(connection, object(), tmp_path / 'capture', run_dir, mode='repair', project_ids={'101'})
    assert manifest['scopes'] == 1
    loaded, staged = scopes.load_run(run_dir)
    with pytest.raises(ValueError, match='allow-repair-fields'):
        scopes.apply_run(connection, None, run_dir, confirm_run_id=manifest['run_id'], allow_partial=True)
    scopes.commit_scope(connection, loaded, staged, staged['scopes'][0])
    assert connection.execute("SELECT hidden_item_id FROM subitems WHERE monday_id='201'").fetchone()[0] == '301'
    assert connection.execute("SELECT total_order_value FROM projects WHERE monday_id='101'").fetchone()[0] == 105
    assert connection.execute("SELECT total_order_value FROM projects WHERE monday_id='999'").fetchone()[0] == 50


def test_schema_and_role_checks_fail_closed(database):
    connection, _, _, _ = database
    connection.execute('ALTER TABLE subitems DISABLE TRIGGER ALL')
    with pytest.raises(ValueError, match='foreign keys'):
        scopes.schema_safety(connection)
    connection.execute('ALTER TABLE subitems ENABLE TRIGGER ALL')
    connection.execute("SET session_replication_role='replica'")
    with pytest.raises(ValueError, match='enforcement'):
        scopes.schema_safety(connection)
    connection.execute("SET session_replication_role='origin'")


def test_total_transaction_deadline_covers_multiple_short_statements(database):
    connection, dsn, _, _ = database
    manifest, staged, record = reviewed(database)
    connection.execute('''CREATE FUNCTION slow_order_test() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN PERFORM pg_sleep(3.6); RETURN NEW; END $$''')
    for table in ('hidden_items', 'subitems', 'projects'):
        connection.execute(sql.SQL('CREATE TRIGGER slow BEFORE UPDATE ON {} FOR EACH ROW '
                                   'EXECUTE FUNCTION slow_order_test()').format(sql.Identifier(table)))
    started = time.monotonic()
    with pytest.raises(psycopg.Error) as caught:
        scopes.commit_scope(connection, manifest, staged, record)
    assert 'transaction timeout' in str(caught.value).lower()
    assert 9 < time.monotonic() - started < 13
    with psycopg.connect(dsn, autocommit=True) as verifier:
        assert scopes.read_boundary(verifier, record['boundary']) == record['before']
        assert not scopes.committed_scopes(verifier, manifest)
