"""Bounded comparison transactions on an explicitly configured loopback PG17."""
from copy import deepcopy
from pathlib import Path
import re
from uuid import uuid4

import psycopg
import pytest

from scripts import backfill_order_values as backfill
from scripts import order_value_monday_compare as compare
from scripts import order_value_scopes as scopes
from scripts import reconcile_order_values as reconcile
from test_order_value_scopes_postgres import database
from test_order_value_monday_compare import fixture_data


def prepared(database):
    connection, _, _, _ = database
    evidence, _, _, boundary = fixture_data()
    # The intentionally small legacy fixture lacks metadata columns. Those
    # fields are reported as unresolved; financial corrections remain testable.
    contract = reconcile.read_contract(connection)
    before = scopes.read_boundary(connection, boundary, full=True)
    record = compare.build_record(['101'], evidence, before, contract, boundary)
    staged = {'contract': contract, 'scopes': [record], 'deferred': []}
    manifest = {'run_id': str(uuid4()), 'sha256': backfill.fingerprint(staged)}
    return manifest, staged, record


def test_commit_changes_only_and_journal_resume(database):
    connection, _, _, _ = database
    manifest, staged, record = prepared(database)
    assert compare.commit_scope(connection, manifest, staged, record) == 'committed_pending_verification'
    assert scopes.read_boundary(connection, record['boundary'], full=True) == record['after']
    connection.execute("UPDATE projects SET total_order_value=777 WHERE monday_id='101'")
    assert compare.commit_scope(connection, manifest, staged, record) == 'already_committed'
    assert connection.execute("SELECT total_order_value FROM projects WHERE monday_id='101'").fetchone()[0] == 777


@pytest.mark.parametrize('statement', [
    "UPDATE projects SET total_order_value=999 WHERE monday_id='101'",
    "INSERT INTO subitems(monday_id,parent_monday_id) VALUES ('298','101')",
    "UPDATE subitems SET hidden_item_id='301' WHERE monday_id='299'",
])
def test_database_drift_rolls_back_without_journal(database, statement):
    connection, _, _, _ = database
    manifest, staged, record = prepared(database)
    connection.execute(statement)
    with pytest.raises(ValueError, match='changed'):
        compare.commit_scope(connection, manifest, staged, record)
    assert not scopes.committed_scopes(connection, manifest)
    assert connection.execute("SELECT cust_order_value_material FROM hidden_items WHERE monday_id='301'").fetchone()[0] == 40


def test_trigger_side_effect_rolls_back_every_table(database):
    connection, _, _, _ = database
    manifest, staged, record = prepared(database)
    connection.execute('''CREATE FUNCTION sabotage_compare() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN NEW.total_order_value = NEW.total_order_value + 1; RETURN NEW; END $$;
        CREATE TRIGGER sabotage BEFORE UPDATE ON projects FOR EACH ROW EXECUTE FUNCTION sabotage_compare()''')
    with pytest.raises(ValueError, match='Post-write'):
        compare.commit_scope(connection, manifest, staged, record)
    assert scopes.read_boundary(connection, record['boundary'], full=True) == record['before']
    assert not scopes.committed_scopes(connection, manifest)


def test_constraint_failure_rolls_back_hidden_and_child_changes(database):
    connection, _, _, _ = database
    manifest, staged, record = prepared(database)
    connection.execute('ALTER TABLE projects ADD CONSTRAINT reject_compare CHECK(total_order_value<100)')
    with pytest.raises(psycopg.IntegrityError):
        compare.commit_scope(connection, manifest, staged, record)
    assert scopes.read_boundary(connection, record['boundary'], full=True) == record['before']
    assert not scopes.committed_scopes(connection, manifest)


def test_table_lock_conflict_leaves_no_partial_commit(database):
    connection, dsn, _, _ = database
    manifest, staged, record = prepared(database)
    with psycopg.connect(dsn, autocommit=True) as writer:
        with writer.transaction():
            writer.execute("UPDATE projects SET total_order_value=51 WHERE monday_id='999'")
            with pytest.raises(psycopg.errors.LockNotAvailable):
                compare.commit_scope(connection, manifest, staged, record)
    assert scopes.read_boundary(connection, record['boundary'], full=True) == record['before']
    assert not scopes.committed_scopes(connection, manifest)


def test_actual_readonly_stage_and_verify_then_finance_drift(database, monkeypatch, tmp_path):
    connection, _, _, _ = database
    evidence, _, _, boundary = fixture_data()
    monkeypatch.setattr(compare, 'capture', lambda *a: deepcopy(evidence))
    original = scopes.read_boundary(connection, boundary, full=True)
    run_dir = tmp_path / 'review'
    manifest = compare.stage_run(connection, None, run_dir, ['101'])
    assert scopes.read_boundary(connection, boundary, full=True) == original
    assert not scopes.committed_scopes(connection, manifest)
    result = compare.execute_run(connection, None, run_dir, apply=True, confirm_run_id=manifest['run_id'])
    assert result['staged_changes_successful']
    result = compare.execute_run(connection, None, run_dir)
    assert result['staged_changes_successful'] and result['counts'] == {'verified': 1}
    evidence['projects']['101']['updated_at'] = 'Finance edited after commit'
    result = compare.execute_run(connection, None, run_dir)
    assert not result['staged_changes_successful']
    assert result['counts'] == {'requires_reassessment': 1}


def test_postcommit_source_change_is_not_success(database, monkeypatch, tmp_path):
    connection, _, _, _ = database
    evidence, *_ = fixture_data()
    monkeypatch.setattr(compare, 'capture', lambda *a: deepcopy(evidence))
    run_dir = tmp_path / 'review'
    manifest = compare.stage_run(connection, None, run_dir, ['101'])
    calls = []
    def changing(*args):
        calls.append(1)
        result = deepcopy(evidence)
        if len(calls) > 1:
            result['projects']['101']['updated_at'] = 'new Finance edit'
        return result
    monkeypatch.setattr(compare, 'capture', changing)
    result = compare.execute_run(connection, None, run_dir, apply=True, confirm_run_id=manifest['run_id'])
    assert result['remaining_uncommitted'] == 0
    assert not result['staged_changes_successful']
    assert result['counts'] == {'requires_reassessment': 1}
    assert scopes.committed_scopes(connection, manifest)


def test_noop_scope_gets_no_journal_but_is_verified(database, monkeypatch, tmp_path):
    connection, _, _, _ = database
    evidence, *_ = fixture_data()
    monkeypatch.setattr(compare, 'capture', lambda *a: deepcopy(evidence))
    manifest, staged, record = prepared(database)
    compare.commit_scope(connection, manifest, staged, record)
    run_dir = tmp_path / 'no_changes'
    manifest = compare.stage_run(connection, None, run_dir, ['101'])
    assert manifest['changes'] == 0
    result = compare.execute_run(connection, None, run_dir, apply=True, confirm_run_id=manifest['run_id'])
    assert result['counts'] == {'no_changes_staged': 1}
    assert not scopes.committed_scopes(connection, manifest)
    assert compare.execute_run(connection, None, run_dir)['counts'] == {'verified_no_changes': 1}


@pytest.mark.parametrize('old_stage, new_stage, category', [
    ('Open Enquiry', 'Won - Closed (Invoiced)', 'Won'),
    ('Won - Open (Order Received)', 'Won - Closed (Invoiced)', 'Won'),
    ('Won - Closed (Invoiced)', 'Lost', 'Lost'),
    ('Won - Closed (Invoiced)', 'Won - Open (Order Received)', 'Open'),
    ('Lost', 'Won Closed', 'Open'),
    ('Lost', 'lost', 'Open'),
    ('Lost', 'Archived', 'Open'),
    ('Lost', ' Won - Closed (Invoiced) ', 'Open'),
    ('Lost', '', 'Open'),
    ('Lost', None, 'Open'),
])
def test_generated_category_stage_apply_verify_matches_defined_schema(
        database, monkeypatch, tmp_path, old_stage, new_stage, category):
    connection, _, _, _ = database
    # Use the actual repository definition, rather than recreating the Python
    # expectation in SQL. PostgreSQL must compute this value on every update.
    schema = (Path(__file__).resolve().parents[1] / 'src/database/schema/schema.sql').read_text()
    column = re.search(r'status_category TEXT GENERATED ALWAYS AS \([\s\S]*?\) STORED', schema)
    assert column is not None
    connection.execute('ALTER TABLE projects ADD COLUMN pipeline_stage text')
    connection.execute('ALTER TABLE projects ADD COLUMN ' + column.group())
    connection.execute("UPDATE projects SET pipeline_stage=%s WHERE monday_id='101'", (old_stage,))
    evidence, _, _, boundary = fixture_data()
    compare.col(evidence['projects']['101'], compare.PARENT_COLUMNS['pipeline_stage'])['label'] = new_stage
    monkeypatch.setattr(compare, 'capture', lambda *a, **k: deepcopy(evidence))
    before = scopes.read_boundary(connection, boundary, full=True)
    run_dir = tmp_path / 'generated_category'
    manifest = compare.stage_run(connection, None, run_dir, ['101'])
    assert scopes.read_boundary(connection, boundary, full=True) == before
    _, staged = compare.load_run(run_dir)
    record = staged['scopes'][0]
    assert record['after']['projects'][0]['status_category'] == category
    assert all('status_category' not in update for update in record['updates']['projects'])
    result = compare.execute_run(connection, None, run_dir, apply=True, confirm_run_id=manifest['run_id'])
    assert result['staged_changes_successful']
    assert connection.execute(
        "SELECT pipeline_stage, status_category FROM projects WHERE monday_id='101'"
    ).fetchone() == (new_stage or None, category)
    assert compare.execute_run(connection, None, run_dir)['counts'] == {'verified': 1}
