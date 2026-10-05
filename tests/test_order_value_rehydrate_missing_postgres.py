"""Real, isolated PG17 insertion/rollback/verification; no production or HTTP."""
from copy import deepcopy
from pathlib import Path
import re

import psycopg
from psycopg import sql
import pytest
import requests

from scripts import backfill_order_values as backfill
from scripts import order_value_rehydrate_missing as missing
from scripts import order_value_scopes as scopes
from scripts import reconcile_order_values as reconcile
from test_order_value_rehydrate_missing import example
from test_order_value_scopes_postgres import database


@pytest.fixture(autouse=True)
def no_http(monkeypatch):
    monkeypatch.setattr(requests.sessions.Session, 'request',
                        lambda *a, **k: pytest.fail('No production or Monday HTTP in database tests'))


def two_targets():
    targets, source, _, contract, _ = example()
    targets.append({'project_id': '102', 'subitem_id': '202', 'hidden_item_id': '302'})
    for table, old, new in [('projects', '101', '102'), ('subitems', '201', '202'), ('hidden_items', '301', '302')]:
        item = deepcopy(source[table][old])
        item['id'] = new
        if table == 'projects': item['subitems'] = [{'id': '202', 'parent_item': {'id': '102'}}]
        if table == 'subitems':
            item['parent_item'] = {'id': '102'}
            for column in item['column_values']:
                if 'linked_item_ids' in column: column['linked_item_ids'] = ['302']
                for link in column.get('mirrored_items', []): link['linked_item']['id'] = '302'
        source[table][new] = item
    return targets, source, contract


@pytest.fixture
def reviewed(database, monkeypatch, tmp_path):
    connection, dsn, _, _ = database
    targets, source, schema = two_targets()
    current = reconcile.read_contract(connection)
    for field, column in schema['subitems'].items():
        if field in current['subitems'] or field == 'id': continue
        kind = f'numeric(12,{column["scale"]})' if column['type'] == 'numeric' else column['type']
        connection.execute(sql.SQL('ALTER TABLE subitems ADD COLUMN {} {}').format(sql.Identifier(field), sql.SQL(kind)))
    connection.execute('ALTER TABLE subitems ADD COLUMN id uuid NOT NULL DEFAULT gen_random_uuid(), '
                       'ADD COLUMN created_at timestamptz NOT NULL DEFAULT now()')
    definition = (Path(__file__).resolve().parents[1] / 'src/database/schema/schema.sql').read_text()
    generated = re.search(r'status_category TEXT GENERATED ALWAYS AS \([\s\S]*?\) STORED', definition).group()
    connection.execute('ALTER TABLE projects ADD COLUMN pipeline_stage text')
    connection.execute('ALTER TABLE projects ADD COLUMN ' + generated)
    connection.execute("UPDATE projects SET total_order_value=86605.48, pipeline_stage='Won - Closed (Invoiced)' WHERE monday_id='101'")
    connection.execute("INSERT INTO projects(monday_id,item_name,total_order_value,pipeline_stage) VALUES ('102','Other',NULL,'Lost')")
    connection.execute("INSERT INTO hidden_items(monday_id,cust_order_value_material) VALUES ('302',0)")
    connection.execute("DELETE FROM subitems WHERE monday_id='201'")
    connection.execute("INSERT INTO subitems(monday_id,parent_monday_id,hidden_item_id) VALUES ('209','101','301')")
    monkeypatch.setattr(missing, 'capture', lambda *a, **k: deepcopy(source))
    path = tmp_path / 'missing'
    before = scopes.read_boundary(connection, missing.boundary_for(targets), full=True)
    manifest = missing.stage_run(connection, None, path, targets)
    assert scopes.read_boundary(connection, missing.boundary_for(targets), full=True) == before
    assert not scopes.committed_scopes(connection, manifest)
    _, staged = missing.load_run(path)
    return connection, dsn, source, path, manifest, staged, staged['scopes'][0]


def test_two_inserts_preserve_every_existing_row_and_generated_categories(reviewed):
    connection, _, _, path, manifest, staged, record = reviewed
    assert manifest['insert_rows'] == {'projects': 0, 'hidden_items': 0, 'subitems': 2}
    result = missing.execute_run(connection, None, path, apply=True, confirm_run_id=manifest['run_id'], allow_rehydration=True)
    assert result['staged_changes_successful'] and result['remaining_uncommitted'] == 0
    actual = scopes.read_boundary(connection, record['boundary'], full=True)
    assert actual['projects'] == record['before']['projects']
    assert actual['hidden_items'] == record['before']['hidden_items']
    old_child = next(r for r in actual['subitems'] if r['monday_id'] == '209')
    assert old_child == record['before']['subitems'][0]
    assert {r['status_category'] for r in actual['projects']} == {'Won', 'Lost'}
    for cid in ('201', '202'):
        row = next(r for r in actual['subitems'] if r['monday_id'] == cid)
        assert row['id'] and row['created_at'] and row['cust_order_value_material'] == '100.00'
    assert missing.execute_run(connection, None, path)['counts'] == {'verified': 1}
    assert connection.execute('SELECT count(*) FROM order_value_scope_commits').fetchone()[0] == 1


@pytest.mark.parametrize('statement', [
    "UPDATE projects SET total_order_value=12 WHERE monday_id='101'",
    "UPDATE hidden_items SET cust_order_value_material=12 WHERE monday_id='301'",
    "INSERT INTO subitems(monday_id,parent_monday_id,hidden_item_id) VALUES ('201','101','301')",
    "INSERT INTO subitems(monday_id,parent_monday_id) VALUES ('210','101')",
])
def test_source_rows_or_absence_changed_after_stage_refuses_inserts(reviewed, statement):
    connection, _, _, _, manifest, staged, record = reviewed
    connection.execute(statement)
    before_attempt = scopes.read_boundary(connection, record['boundary'], full=True)
    with pytest.raises(ValueError, match='changed since staging'):
        missing.commit_scope(connection, manifest, staged, record)
    assert scopes.read_boundary(connection, record['boundary'], full=True) == before_attempt
    assert not scopes.committed_scopes(connection, manifest)


def test_second_insert_constraint_failure_rolls_back_both_rows(reviewed):
    connection, _, _, _, manifest, staged, record = reviewed
    connection.execute("ALTER TABLE subitems ADD CONSTRAINT refuse_second CHECK (monday_id <> '202')")
    with pytest.raises(psycopg.IntegrityError): missing.commit_scope(connection, manifest, staged, record)
    assert scopes.read_boundary(connection, record['boundary'], full=True) == record['before']
    assert not scopes.committed_scopes(connection, manifest)


def test_parent_rollup_trigger_is_detected_and_whole_transaction_rolls_back(reviewed):
    connection, _, _, _, manifest, staged, record = reviewed
    connection.execute('''CREATE FUNCTION unexpected_rollup() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN UPDATE projects SET total_order_value=105 WHERE monday_id=NEW.parent_monday_id;
        RETURN NEW; END $$;
        CREATE TRIGGER wrong_total AFTER INSERT ON subitems FOR EACH ROW EXECUTE FUNCTION unexpected_rollup()''')
    with pytest.raises(ValueError, match='Post-insert values differ'):
        missing.commit_scope(connection, manifest, staged, record)
    assert scopes.read_boundary(connection, record['boundary'], full=True) == record['before']
    assert not scopes.committed_scopes(connection, manifest)


def test_table_lock_protects_absence_and_allows_reads(reviewed, monkeypatch):
    connection, dsn, _, _, manifest, staged, record = reviewed
    original = missing.legacy.write_inserts
    def check_lock(conn, inserts):
        with psycopg.connect(dsn, autocommit=True) as writer:
            writer.execute("SET lock_timeout='150ms'")
            assert writer.execute('SELECT count(*) FROM projects').fetchone()[0] > 0
            with pytest.raises(psycopg.errors.LockNotAvailable):
                writer.execute("INSERT INTO subitems(monday_id,parent_monday_id) VALUES ('201','101')")
        return original(conn, inserts)
    monkeypatch.setattr(missing.legacy, 'write_inserts', check_lock)
    missing.commit_scope(connection, manifest, staged, record)


def test_monday_change_before_insert_defers_without_journal(reviewed):
    connection, _, source, path, manifest, _, record = reviewed
    source['subitems']['201']['updated_at'] = 'Finance edit after stage'
    result = missing.execute_run(connection, None, path, apply=True, confirm_run_id=manifest['run_id'], allow_rehydration=True)
    assert result['counts'] == {'requires_reassessment': 1} and result['remaining_uncommitted'] == 1
    assert scopes.read_boundary(connection, record['boundary'], full=True) == record['before']


def test_source_change_after_commit_is_reported_without_claiming_success(reviewed, monkeypatch):
    connection, _, source, path, manifest, _, _ = reviewed
    calls = []
    def changing(*a, **k):
        calls.append(1)
        evidence = deepcopy(source)
        if len(calls) > 1: evidence['subitems']['201']['updated_at'] = 'Later Finance edit'
        return evidence
    monkeypatch.setattr(missing, 'capture', changing)
    result = missing.execute_run(connection, None, path, apply=True, confirm_run_id=manifest['run_id'], allow_rehydration=True)
    assert not result['staged_changes_successful'] and result['remaining_uncommitted'] == 0
    assert scopes.committed_scopes(connection, manifest)


def test_resume_after_lost_receipt_skips_inserts_but_verify_detects_later_writer(reviewed, monkeypatch):
    connection, _, _, path, manifest, _, record = reviewed
    with monkeypatch.context() as patch:
        patch.setattr(backfill, 'write_json', lambda *a: (_ for _ in ()).throw(OSError('receipt disk failure')))
        with pytest.raises(OSError):
            missing.execute_run(connection, None, path, apply=True, confirm_run_id=manifest['run_id'], allow_rehydration=True)
    assert scopes.committed_scopes(connection, manifest)
    result = missing.execute_run(connection, None, path, apply=True, confirm_run_id=manifest['run_id'], allow_rehydration=True)
    assert result['counts'] == {'already_committed': 1}
    assert missing.execute_run(connection, None, path)['counts'] == {'verified': 1}
    connection.execute("UPDATE subitems SET customer_po='later sync change' WHERE monday_id='201'")
    result = missing.execute_run(connection, None, path)
    assert not result['staged_changes_successful'] and result['counts'] == {'requires_reassessment': 1}


def test_new_stage_after_rows_exist_is_verified_noop_without_journal(reviewed):
    connection, _, _, path, manifest, staged, record = reviewed
    missing.commit_scope(connection, manifest, staged, record)
    run = path.parent / 'noop'
    next_manifest = missing.stage_run(connection, None, run, record['targets'])
    assert next_manifest['already_present'] == 2 and next_manifest['insert_rows']['subitems'] == 0
    assert missing.execute_run(connection, None, run, apply=True, confirm_run_id=next_manifest['run_id'],
                               allow_rehydration=True)['counts'] == {'verified_no_changes': 1}
    assert missing.execute_run(connection, None, run)['counts'] == {'verified_no_changes': 1}
    assert not scopes.committed_scopes(connection, next_manifest)


def test_verify_before_apply_and_missing_ack_do_not_write(reviewed):
    connection, _, _, path, manifest, _, record = reviewed
    assert missing.execute_run(connection, None, path)['counts'] == {'not_committed': 1}
    with pytest.raises(ValueError, match='allow-rehydration'):
        missing.execute_run(connection, None, path, apply=True, confirm_run_id=manifest['run_id'])
    assert scopes.read_boundary(connection, record['boundary'], full=True) == record['before']
