"""Recovery coverage with actual parsers/loaders; all live connections forbidden."""
from copy import deepcopy
import json
from pathlib import Path
import socket
from types import SimpleNamespace

import pytest
import requests

from scripts import backfill_order_values as backfill
from scripts import order_value_scope_reads as reads
from scripts import order_value_scopes as scopes
from scripts import order_value_scopes_refresh as refresh
from scripts import order_value_scopes_targeted as targeted
from scripts import reconcile_order_values as reconcile
from test_order_value_scopes import ReadConnection, sample


@pytest.fixture(autouse=True)
def no_live_io(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail('Recovery tests must not connect to Monday or a database')
    monkeypatch.setattr(requests.sessions.Session, 'request', forbidden)
    monkeypatch.setattr(socket.socket, 'connect', forbidden)
    monkeypatch.setattr(refresh.psycopg, 'connect', forbidden)
    monkeypatch.setattr(backfill, 'read_baseline', forbidden)
    monkeypatch.setattr(backfill, 'capture_source_with_reviewed_duplicates', forbidden)
    monkeypatch.setattr(reads, 'capture_ownership', forbidden)


class SelectedMonday:
    def __init__(self, source):
        self.parents = deepcopy(source['exclusion_evidence']['parent_details']['items'])
        child = deepcopy(self.parents[0]['subitems'][0])
        child.update(name='Child', column_values=[{
            'id': backfill.SUBITEM_COLUMNS['hidden_item_id'], 'type': 'board_relation',
            'value': None, 'linked_item_ids': ['301']}])
        self.children = [child]
        self.hidden = [{'id': '301', 'name': 'Source', 'state': 'active',
                        'board': {'id': backfill.HIDDEN_ITEMS_BOARD_ID}, 'parent_item': None,
                        'column_values': [
                            {'id': backfill.HIDDEN_ITEMS_COLUMNS[backfill.ORDER_FIELDS[0]],
                             'type': 'numbers', 'value': '"200"'},
                            {'id': backfill.HIDDEN_ITEMS_COLUMNS[backfill.ORDER_FIELDS[1]],
                             'type': 'numbers', 'value': '"7"'},
                            {'id': backfill.TOTAL_COLUMN, 'type': 'formula', 'value': None,
                             'display_value': '207'}]}]
        self.calls = []

    def execute_query(self, query, variables):
        assert 'mutation' not in query and 'items_page' not in query
        self.calls.append((query, deepcopy(variables)))
        if 'InventoryDetails' in query:
            assert 'subitems {' in query
            rows = self.parents
        elif variables['columns'] == [backfill.SUBITEM_COLUMNS['hidden_item_id']]:
            rows = self.children
        else:
            assert set(variables['columns']) == {backfill.TOTAL_COLUMN,
                *(backfill.HIDDEN_ITEMS_COLUMNS[f] for f in backfill.ORDER_FIELDS)}
            rows = self.hidden
        return {'data': {'items': deepcopy([r for r in rows if r['id'] in variables['ids']])}}


@pytest.fixture
def recovery(tmp_path, monkeypatch):
    connection = ReadConnection()
    baseline, source, scope = sample()
    captured = deepcopy(baseline)
    plan = backfill.build_plan(captured, {k: source[k] for k in ('project_ids', 'subitems', 'hidden_items')})
    origin = {'run_id': 'capture', 'target': backfill.target_fingerprint(connection),
              'approve_reviewed_parentless_duplicates': True, 'approved_empty': [], 'summary': {'blocked_projects': 598}}
    monkeypatch.setattr(backfill, 'load_run', lambda path: (origin, captured, source, plan))
    for module in (scopes, targeted):
        monkeypatch.setattr(module, 'schema_safety', lambda *a, **k: {'journal_installed': True})
    monkeypatch.setattr(reconcile, 'read_contract', lambda conn: {})
    boundaries = []
    def read_boundary(conn, boundary, **kwargs):
        boundaries.append(deepcopy(boundary))
        return scopes.select_boundary(scopes.index_baseline(baseline), boundary)
    monkeypatch.setattr(scopes, 'read_boundary', read_boundary)
    monkeypatch.setattr(targeted, 'read_boundary', read_boundary)
    previous_dir = tmp_path / 'previous'
    targeted.stage_run(connection, None, tmp_path / 'capture', previous_dir, mode='orders', project_ids={'101'})
    boundaries.clear()
    return SimpleNamespace(connection=connection, baseline=baseline, source=source, scope=scope,
                           monday=SelectedMonday(source), previous=previous_dir, output=tmp_path / 'recovery',
                           boundaries=boundaries)


def stage(env):
    return refresh.stage_run(env.connection, env.monday, env.previous, env.output, ['101'])


def test_refresh_uses_live_amounts_and_current_database_without_changing_original_run(recovery):
    env = recovery
    previous = {p.name: p.read_bytes() for p in env.previous.iterdir()}
    # Another writer changed SQL after the original stage. Fresh before-values are required.
    env.baseline['projects'][0]['total_order_value'] = '180.00'
    manifest = stage(env)
    assert manifest['projects'] == manifest['selected_projects'] == 1
    assert manifest['monday_source_refreshed'] and manifest['changes'] == 5
    loaded, staged = targeted.load_run(env.output)
    assert loaded == manifest and staged['deferred'] == []
    record = staged['scopes'][0]
    assert record['before']['projects'][0]['total_order_value'] == '180.00'
    assert record['after']['projects'][0]['total_order_value'] == '207.00'
    assert record['after']['projects'][0]['new_enquiry_value'] == '90.00'
    assert record['after']['hidden_items'][0]['cust_order_value_material'] == '200.00'
    assert env.boundaries == [env.scope]
    assert all('READ ONLY' in q for q in env.connection.statements)
    assert {p.name: p.read_bytes() for p in env.previous.iterdir()} == previous
    assert [v['ids'] for _, v in env.monday.calls] == [['101'], ['201'], ['301']]
    provenance = staged['refresh']
    assert provenance['source_of_truth'] == 'Monday CRM'
    assert not provenance['global_ownership_validated_at_stage']
    assert provenance['ownership_check_required_at_apply']
    raw = json.loads((env.output / 'monday-refresh.json').read_text())
    assert backfill.fingerprint(raw) == provenance['raw_evidence_sha256']
    assert targeted.commit_scope is scopes.commit_scope


def add_child(env):
    child = deepcopy(env.monday.children[0])
    child['id'] = '202'
    child['column_values'][0]['linked_item_ids'] = ['302']
    env.monday.children.append(child)
    metadata = deepcopy(env.monday.parents[0]['subitems'][0])
    metadata['id'] = '202'
    env.monday.parents[0]['subitems'].append(metadata)
    hidden = deepcopy(env.monday.hidden[0])
    hidden['id'] = '302'
    env.monday.hidden.append(hidden)
    env.baseline['subitems'].append({**env.baseline['subitems'][0], 'monday_id': '202', 'hidden_item_id': '302'})
    env.baseline['hidden_items'].append({**env.baseline['hidden_items'][0], 'monday_id': '302'})


def test_new_monday_membership_is_refreshed_if_database_already_has_matching_relationships(recovery):
    add_child(recovery)
    manifest = stage(recovery)
    _, staged = targeted.load_run(recovery.output)
    record = staged['scopes'][0]
    assert not manifest['deferred_scopes'] and record['scope']['subitems'] == ['201', '202']
    assert record['after']['projects'][0]['total_order_value'] == '414.00'


@pytest.mark.parametrize('problem', ['missing_child', 'missing_source', 'missing_parent', 'new_child_not_synced',
                                     'stored_link', 'swapped_links', 'extra_stored_child', 'foreign_owner'])
def test_database_relationship_problems_defer_without_repairs(recovery, problem):
    env = recovery
    if problem.startswith('missing_'):
        table = {'missing_child': 'subitems', 'missing_source': 'hidden_items', 'missing_parent': 'projects'}[problem]
        env.baseline[table] = []
    elif problem == 'new_child_not_synced':
        add_child(env)
        env.baseline['subitems'].pop()
    elif problem == 'stored_link':
        env.baseline['subitems'][0]['hidden_item_id'] = '999'
    elif problem == 'swapped_links':
        add_child(env)
        env.baseline['subitems'][0]['hidden_item_id'] = '302'
        env.baseline['subitems'][1]['hidden_item_id'] = '301'
    else:
        extra = {**env.baseline['subitems'][0], 'monday_id': '299'}
        if problem == 'foreign_owner':
            extra['parent_monday_id'] = '199'
        env.baseline['subitems'].append(extra)
    manifest = stage(env)
    assert manifest['deferred_scopes'] == 1 and manifest['scopes'] == manifest['changes'] == 0
    _, staged = targeted.load_run(env.output)
    assert staged['deferred'][0]['project_ids'] == ['101']


@pytest.mark.parametrize('problem', ['missing_parent', 'inactive_parent', 'wrong_parent_board', 'nested_parent',
                                     'no_children', 'duplicate_child', 'moved_child', 'inactive_child',
                                     'missing_child', 'multiple_links', 'empty_link', 'shared_source',
                                     'missing_source', 'wrong_source_board', 'formula', 'invalid_amount',
                                     'missing_amount_column'])
def test_invalid_monday_evidence_cannot_create_an_apply_run(recovery, problem):
    env = recovery
    if problem == 'missing_parent': env.monday.parents = []
    elif problem == 'inactive_parent': env.monday.parents[0]['state'] = 'archived'
    elif problem == 'wrong_parent_board': env.monday.parents[0]['board']['id'] = '999'
    elif problem == 'nested_parent': env.monday.parents[0]['parent_item'] = {'id': '999'}
    elif problem == 'no_children': env.monday.parents[0]['subitems'] = []
    elif problem == 'duplicate_child': env.monday.parents[0]['subitems'] *= 2
    elif problem == 'moved_child': env.monday.children[0]['parent_item']['id'] = '999'
    elif problem == 'inactive_child': env.monday.children[0]['state'] = 'archived'
    elif problem == 'missing_child': env.monday.children = []
    elif problem == 'multiple_links': env.monday.children[0]['column_values'][0]['linked_item_ids'] = ['301', '302']
    elif problem == 'empty_link': env.monday.children[0]['column_values'][0]['linked_item_ids'] = []
    elif problem == 'shared_source':
        add_child(env)
        env.monday.children[1]['column_values'][0]['linked_item_ids'] = ['301']
    elif problem == 'missing_source': env.monday.hidden = []
    elif problem == 'wrong_source_board': env.monday.hidden[0]['board']['id'] = '999'
    elif problem == 'formula': env.monday.hidden[0]['column_values'][2]['display_value'] = '999'
    elif problem == 'invalid_amount': env.monday.hidden[0]['column_values'][0]['value'] = '"unreadable"'
    elif problem == 'missing_amount_column': env.monday.hidden[0]['column_values'].pop(0)
    with pytest.raises(ValueError):
        stage(env)
    assert not env.output.exists()


@pytest.mark.parametrize('ids', [[], ['999'], ['101', '101'], ['bad'], [101]])
def test_selection_must_be_explicitly_within_previous_reviewed_projects(recovery, ids):
    with pytest.raises(ValueError, match='project IDs'):
        refresh.stage_run(recovery.connection, recovery.monday, recovery.previous, recovery.output, ids)
    assert recovery.monday.calls == []


def test_target_mismatch_and_existing_directory_refuse_before_monday(recovery):
    recovery.output.mkdir()
    with pytest.raises(ValueError, match='new recovery'):
        stage(recovery)
    recovery.connection.info = SimpleNamespace(host='other', port=5432, dbname='test', user='test')
    with pytest.raises(ValueError, match='target differs'):
        stage(recovery)
    assert recovery.monday.calls == []


def test_already_correct_project_remains_verifiable_with_zero_updates(recovery):
    for table in ('subitems', 'hidden_items'):
        recovery.baseline[table][0].update(cust_order_value_material='200.00', cust_additional_charges='7.00')
    recovery.baseline['projects'][0]['total_order_value'] = '207.00'
    manifest = stage(recovery)
    assert manifest['changes'] == 0 and manifest['scopes'] == 1 and manifest['projects'] == 1


@pytest.mark.parametrize('drift', ['none', 'new_owner', 'amount_before_apply', 'amount_after_apply'])
def test_refreshed_runs_use_original_apply_verify_and_fresh_ownership_guards(recovery, monkeypatch, drift):
    env = recovery
    manifest = stage(env)
    _, plan = targeted.load_run(env.output)
    record = plan['scopes'][0]
    owners = deepcopy(record['source']['owners'])
    if drift == 'new_owner':
        owners.append({**owners[0], 'monday_id': '299', 'parent_monday_id': '199'})
    scans, commits, committed = [], [], set()
    monkeypatch.setattr(reads, 'capture_ownership', lambda *a: scans.append(True) or {'items': owners})
    monkeypatch.setattr(targeted, 'committed_scopes', lambda *a: set(committed))
    def commit(conn, current_manifest, staged, current_record):
        commits.append(current_record['scope_id'])
        committed.add(current_record['scope_id'])
        env.baseline.clear()
        env.baseline.update(deepcopy(current_record['after']))
        return {'scope_id': current_record['scope_id'], 'status': 'committed_pending_source_verification'}
    monkeypatch.setattr(targeted, 'commit_scope', commit)
    def change_amount():
        env.monday.hidden[0]['column_values'][0]['value'] = '"300"'
        env.monday.hidden[0]['column_values'][2]['display_value'] = '307'
    if drift == 'amount_before_apply': change_amount()
    result = targeted.apply_run(env.connection, env.monday, env.output,
                               confirm_run_id=manifest['run_id'], allow_partial=True, all_pending=True)
    if drift in ('new_owner', 'amount_before_apply'):
        assert result['counts'] == {'deferred': 1} and not commits and len(scans) == 1
    else:
        if drift == 'amount_after_apply': change_amount()
        result = targeted.verify_run(env.connection, env.monday, env.output)
        assert result['complete'] is (drift == 'none') and len(scans) == 2 and len(commits) == 1


def test_grouping_retains_project_and_row_limits():
    source = {'project_ids': [str(i) for i in range(30)],
              'subitems': [{'parent_monday_id': str(i)} for i in range(30)]}
    assert [len(group) for group in refresh.group_projects(source)] == [25, 5]
    source['project_ids'] = ['1', '2']
    source['subitems'] = [{'parent_monday_id': p} for p in ['1', '2'] for _ in range(125)]
    assert refresh.group_projects(source) == [['1'], ['2']]


@pytest.mark.parametrize('contents', ['', '101\n101\n', 'bad\n', '１２３\n'])
def test_invalid_selection_file(tmp_path, contents):
    path = tmp_path / 'selection.txt'
    path.write_text(contents, encoding='utf-8')
    with pytest.raises(ValueError):
        refresh.project_ids_from_file(path)


def test_recovery_selection_has_48_unique_projects_and_excludes_already_matching_parents():
    path = Path(__file__).resolve().parents[1] / 'docs/order-value-recovery-48-projects.txt'
    selected = refresh.project_ids_from_file(path)
    assert len(selected) == 48 and '3222674942' in selected
    assert not {'2122665676', '2132052486', '5033000087'} & set(selected)
