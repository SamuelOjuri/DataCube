from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
import hashlib
import json

import pytest

from scripts import backfill_order_values as backfill
from scripts import order_value_scopes as scopes
from scripts import reconcile_order_values as reconcile


def sample():
    baseline = {t: [] for t in scopes.TABLES}
    baseline['projects'] = [{**dict.fromkeys(backfill.BASELINE_COLUMNS['projects']),
        'monday_id': '101', 'item_name': 'Example', 'total_order_value': '40.00', 'new_enquiry_value': '90.00'}]
    baseline['hidden_items'] = [{**dict.fromkeys(backfill.BASELINE_COLUMNS['hidden_items']),
        'monday_id': '301', 'cust_order_value_material': '40.00', 'cust_additional_charges': '0.00'}]
    baseline['subitems'] = [{**dict.fromkeys(backfill.BASELINE_COLUMNS['subitems']),
        'monday_id': '201', 'parent_monday_id': '101', 'hidden_item_id': '301',
        'cust_order_value_material': '40.00', 'cust_additional_charges': '0.00'}]
    parent = {'id': '101', 'state': 'active', 'board': {'id': backfill.PARENT_BOARD_ID}, 'parent_item': None}
    child = {'id': '201', 'state': 'active', 'board': {'id': backfill.SUBITEM_BOARD_ID},
             'parent_item': {k: v for k, v in parent.items() if k != 'parent_item'}}
    parent['subitems'] = [child]
    source = {'project_ids': ['101'], 'hidden_items': [{'monday_id': '301', 'cust_order_value_material': '100.00',
        'cust_additional_charges': '5.00', 'monday_total': '105.00', 'issues': []}], 'subitems': [{
        'monday_id': '201', 'parent_monday_id': '101', 'hidden_ids': ['301'], 'link_error': False,
        'state': 'active', 'board_id': backfill.SUBITEM_BOARD_ID, 'parent_state': 'active', 'parent_board_id': backfill.PARENT_BOARD_ID}],
        'exclusion_evidence': {'parent_details': {'items': [parent]}}}
    scope = {'projects': ['101'], 'subitems': ['201'], 'hidden_items': ['301']}
    return baseline, source, scope


class ReadConnection:
    info = SimpleNamespace(host='localhost', port=5432, dbname='test', user='test', server_version=170004)

    def __init__(self):
        self.statements = []

    @contextmanager
    def transaction(self):
        yield

    def execute(self, statement, *args):
        self.statements.append(statement)


@pytest.fixture
def staged(tmp_path, monkeypatch):
    connection = ReadConnection()
    baseline, source, scope = sample()
    plan = backfill.build_plan(baseline, {k: source[k] for k in ('project_ids', 'subitems', 'hidden_items')})
    origin = {'run_id': 'original', 'target': backfill.target_fingerprint(connection),
              'approve_reviewed_parentless_duplicates': True, 'approved_empty': [], 'summary': {'blocked_projects': 598}}
    monkeypatch.setattr(backfill, 'load_run', lambda path: (origin, deepcopy(baseline), deepcopy(source), deepcopy(plan)))
    monkeypatch.setattr(backfill, 'read_baseline', lambda conn: deepcopy(baseline))
    monkeypatch.setattr(scopes, 'schema_safety', lambda *a, **k: {'journal_installed': False})
    monkeypatch.setattr(reconcile, 'read_contract', lambda conn: {})
    run_dir = tmp_path / 'run'
    manifest = scopes.stage_run(connection, None, tmp_path / 'capture', run_dir, mode='orders', project_ids={'101'})
    return connection, run_dir, manifest, baseline, source, scope


def test_order_staging_is_read_only_and_rebuilds_its_review(staged):
    connection, run_dir, manifest, baseline, _, _ = staged
    loaded, plan = scopes.load_run(run_dir)
    assert loaded == manifest
    assert manifest['scopes'] == 1 and manifest['changes'] == 5
    assert all('READ ONLY' in q for q in connection.statements)
    record = plan['scopes'][0]
    assert record['after']['projects'][0]['total_order_value'] == '105.00'
    assert record['after']['projects'][0]['new_enquiry_value'] == '90.00'
    assert baseline['projects'][0]['total_order_value'] == '40.00'
    assert (run_dir / 'changes.csv').is_file()


def test_unrelated_source_changes_are_ignored_but_new_shared_owners_are_not():
    _, source, scope = sample()
    expected = scopes.source_evidence(source, scope)
    source['hidden_items'].append({'monday_id': 'other', 'issues': ['monday_formula_mismatch']})
    source['project_ids'].append('999')
    assert scopes.source_evidence(source, scope) == expected
    source['subitems'].append({**source['subitems'][0], 'monday_id': '202', 'parent_monday_id': '999'})
    assert scopes.source_evidence(source, scope) != expected


def test_new_child_membership_is_not_ignored():
    _, source, scope = sample()
    source['subitems'].append({**source['subitems'][0], 'monday_id': '202'})
    with pytest.raises(scopes.ScopeConflict, match='membership'):
        scopes.source_evidence(source, scope)


def test_scope_reads_include_external_owners_and_old_sources():
    baseline, _, scope = sample()
    baseline['subitems'][0]['hidden_item_id'] = 'old'
    baseline['subitems'].append({**baseline['subitems'][0], 'monday_id': '202', 'parent_monday_id': '999', 'hidden_item_id': '301'})
    boundary = scopes.boundary_from_baseline(baseline, scope)
    assert boundary['hidden_items'] == ['301', 'old']
    selected = scopes.select_boundary(scopes.index_baseline(baseline), boundary)
    assert {r['monday_id'] for r in selected['subitems']} == {'201', '202'}
    with pytest.raises(scopes.ScopeConflict, match='shared'):
        scopes.check_ownership(selected, scope)


@pytest.mark.parametrize('table', scopes.TABLES)
def test_missing_rows_cannot_be_inserted_by_online_workflow(table):
    baseline, _, scope = sample()
    baseline[table] = []
    with pytest.raises(scopes.ScopeConflict, match='Missing scoped rows'):
        scopes.require_existing_scope(baseline, scope)


def test_large_scope_refuses_before_writes():
    baseline, _, scope = sample()
    baseline['subitems'].extend({'monday_id': 'extra-' + str(i)} for i in range(scopes.MAX_SCOPE_ROWS))
    with pytest.raises(scopes.ScopeConflict, match='transaction limit'):
        scopes.require_existing_scope(baseline, scope)


@pytest.mark.parametrize('artifact', ['changes.csv', 'scopes.json', 'manifest.json'])
def test_tampered_review_is_refused(staged, artifact):
    _, run_dir, _, _, _, _ = staged
    path = run_dir / artifact
    if artifact == 'manifest.json':
        data = json.loads(path.read_text())
        data['code'] = 'changed'
        path.write_text(json.dumps(data))
    else:
        path.write_text(path.read_text() + ' ')
        if artifact == 'scopes.json':
            data = json.loads(path.read_text())
            data['mode'] = 'repair'
            path.write_text(json.dumps(data))
    with pytest.raises(ValueError):
        scopes.load_run(run_dir)


def test_rehashed_wrong_arithmetic_still_refused(staged):
    _, run_dir, _, _, _, _ = staged
    path = run_dir / 'scopes.json'
    staged_plan = json.loads(path.read_text())
    record = staged_plan['scopes'][0]
    record['updates']['projects'][0]['total_order_value'] = '999.00'
    record['after']['projects'][0]['total_order_value'] = '999.00'
    path.write_text(json.dumps(staged_plan))
    manifest = json.loads((run_dir / 'manifest.json').read_text())
    manifest['sha256'] = backfill.fingerprint(staged_plan)
    (run_dir / 'manifest.json').write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match='source evidence'):
        scopes.load_run(run_dir)


def test_apply_requires_explicit_partial_acknowledgement(staged):
    connection, run_dir, manifest, *_ = staged
    with pytest.raises(ValueError, match='allow-partial'):
        scopes.apply_run(connection, None, run_dir, confirm_run_id=manifest['run_id'])


def test_repeat_uses_database_journal_before_fetching_sources(staged, monkeypatch):
    connection, run_dir, manifest, *_ = staged
    _, plan = scopes.load_run(run_dir)
    monkeypatch.setattr(scopes, 'committed_scopes', lambda *a: {plan['scopes'][0]['scope_id']})
    monkeypatch.setattr(scopes, 'fresh_capture', lambda *a: pytest.fail('Committed runs must not require unchanged source to resume'))
    result = scopes.apply_run(connection, None, run_dir, confirm_run_id=manifest['run_id'], allow_partial=True)
    assert result['previously_committed'] == 1 and result['results'] == []
    assert result['requires_fresh_source_verification']


def test_apply_defers_source_conflict_without_attempting_writes(staged, monkeypatch):
    connection, run_dir, manifest, _, source, _ = staged
    source['hidden_items'][0]['cust_order_value_material'] = '111.00'
    monkeypatch.setattr(scopes, 'committed_scopes', lambda *a: set())
    monkeypatch.setattr(scopes, 'fresh_capture', lambda *a: source)
    monkeypatch.setattr(scopes, 'commit_scope', lambda *a: pytest.fail('Must not write changed source'))
    result = scopes.apply_run(connection, None, run_dir, confirm_run_id=manifest['run_id'], allow_partial=True)
    assert result['results'][0]['status'] == 'deferred'


def test_targeted_missing_source_defers_scope(staged, monkeypatch):
    connection, run_dir, manifest, _, source, _ = staged
    monkeypatch.setattr(scopes, 'committed_scopes', lambda *a: set())
    monkeypatch.setattr(scopes, 'fresh_capture', lambda *a: source)
    monkeypatch.setattr(scopes, 'targeted_orders', lambda *a: (_ for _ in ()).throw(ValueError('Targeted item is inactive')))
    monkeypatch.setattr(scopes, 'commit_scope', lambda *a: pytest.fail('Missing evidence must not write'))
    result = scopes.apply_run(connection, None, run_dir, confirm_run_id=manifest['run_id'], allow_partial=True)
    assert result['results'][0]['status'] == 'deferred'


def test_repair_parent_move_is_deferred_even_inside_selected_group():
    before, source, scope = sample()
    evidence = scopes.source_evidence(source, scope)
    before['subitems'][0]['parent_monday_id'] = 'other'
    with pytest.raises(scopes.ScopeConflict, match='parent changed'):
        scopes.check_parent_membership(before, scope, evidence)


def test_order_scopes_are_bounded_by_projects_and_rows():
    plan = {'projects': [{'project_id': str(i), 'status': 'verified'} for i in range(60)]}
    source = {'subitems': [{'parent_monday_id': str(i)} for i in range(60) for _ in range(20)]}
    groups = scopes.choose_groups({}, source, plan, 'orders', set(), all_verified=True)
    assert sum(map(len, groups)) == 60
    assert all(len(group) * 41 <= scopes.MAX_SCOPE_ROWS for group in groups)


def test_verify_detects_postcommit_source_drift(staged, monkeypatch):
    connection, run_dir, _, _, source, _ = staged
    _, plan = scopes.load_run(run_dir)
    record = plan['scopes'][0]
    source['hidden_items'][0]['cust_additional_charges'] = '50.00'
    monkeypatch.setattr(scopes, 'committed_scopes', lambda *a: {record['scope_id']})
    monkeypatch.setattr(scopes, 'fresh_capture', lambda *a: source)
    monkeypatch.setattr(scopes, 'read_boundary', lambda *a, **k: record['after'])
    result = scopes.verify_run(connection, None, run_dir)
    assert result['counts'] == {'changed_requires_reassessment': 1}
    assert not result['complete']


def test_reassessment_offline_cli_never_connects(tmp_path, monkeypatch):
    monkeypatch.setattr(scopes, 'reassess', lambda *a: {'transitions': {'blocked -> verified': 1}})
    monkeypatch.setattr(scopes.psycopg, 'connect', lambda *a, **k: pytest.fail('Offline reassessment must not connect'))
    assert scopes.main(['reassess', '--previous-run', 'old', '--capture-dir', 'new', '--output-dir', str(tmp_path)]) == 0


def test_dependency_selection_cannot_split_a_repair_group(monkeypatch):
    monkeypatch.setattr(reconcile, 'build_report', lambda *a: {'repair_groups': [
        {'project_ids': ['101', '102'], 'action': 'prepare_candidate'}]})
    with pytest.raises(ValueError, match='complete repair dependency'):
        scopes.choose_groups({}, {}, {}, 'repair', {'101'})
    assert scopes.choose_groups({}, {}, {}, 'repair', {'101', '102'}) == [['101', '102']]


@pytest.mark.parametrize('missing', [False, True])
def test_online_readiness_separates_missing_rows_from_existing_repairs(missing):
    baseline, source, _ = sample()
    baseline['subitems'][0]['hidden_item_id'] = 'old'
    if missing:
        baseline['hidden_items'] = []
    plan = backfill.build_plan(baseline, {k: source[k] for k in ('project_ids', 'subitems', 'hidden_items')})
    result = scopes.online_readiness(baseline, source, plan)
    assert len(result) == 1
    assert result[0]['readiness'] == ('separate_rehydration_or_review' if missing else 'stage_candidate')
