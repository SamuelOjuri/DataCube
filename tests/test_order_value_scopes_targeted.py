"""Offline regression coverage; live HTTP and database connections are forbidden."""
from copy import deepcopy
import json
import socket

import pytest
import requests

from scripts import backfill_order_values as backfill
from scripts import order_value_scopes as legacy
from scripts import order_value_scopes_targeted as targeted
from scripts import order_value_scope_reads as reads
from scripts import reconcile_order_values as reconcile
from test_order_value_scopes import ReadConnection, sample


@pytest.fixture(autouse=True)
def no_live_io(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail('This test must never connect to Monday or a database')
    monkeypatch.setattr(requests.sessions.Session, 'request', forbidden)
    monkeypatch.setattr(socket.socket, 'connect', forbidden)
    monkeypatch.setattr(targeted.psycopg, 'connect', forbidden)
    monkeypatch.setattr(backfill, 'read_baseline', forbidden)
    monkeypatch.setattr(backfill, 'capture_source_with_reviewed_duplicates', forbidden)


@pytest.fixture
def staged(tmp_path, monkeypatch):
    connection = ReadConnection()
    baseline, source, scope = sample()
    plan = backfill.build_plan(baseline, {k: source[k] for k in ('project_ids', 'subitems', 'hidden_items')})
    origin = {'run_id': 'original', 'target': backfill.target_fingerprint(connection),
              'approve_reviewed_parentless_duplicates': True, 'approved_empty': [], 'summary': {'blocked_projects': 598}}
    monkeypatch.setattr(backfill, 'load_run', lambda path: (origin, deepcopy(baseline), deepcopy(source), deepcopy(plan)))
    monkeypatch.setattr(targeted, 'schema_safety', lambda *a, **k: {'journal_installed': True})
    monkeypatch.setattr(reconcile, 'read_contract', lambda conn: {})
    boundaries = []
    def read_boundary(conn, boundary, **kwargs):
        boundaries.append(deepcopy(boundary))
        return legacy.select_boundary(legacy.index_baseline(baseline), boundary)
    monkeypatch.setattr(targeted, 'read_boundary', read_boundary)
    run_dir = tmp_path / 'run'
    manifest = targeted.stage_run(connection, None, tmp_path / 'capture', run_dir, mode='orders', project_ids={'101'})
    return connection, run_dir, manifest, baseline, source, scope, boundaries


def test_staging_reads_selected_boundary_once_and_preserves_other_fields(staged):
    connection, path, manifest, baseline, _, scope, boundaries = staged
    assert boundaries == [scope]
    assert all('READ ONLY' in q for q in connection.statements)
    loaded, plan = targeted.load_run(path)
    assert loaded == manifest and manifest['changes'] == 5
    record = plan['scopes'][0]
    assert record['after']['projects'][0]['total_order_value'] == '105.00'
    assert record['after']['projects'][0]['new_enquiry_value'] == '90.00'
    assert baseline['projects'][0]['total_order_value'] == '40.00'
    assert targeted.commit_scope is legacy.commit_scope  # Shared, unchanged lock protocol.


def test_new_and_legacy_run_formats_cannot_be_mixed(staged):
    _, path, *_ = staged
    with pytest.raises(ValueError, match='Code'):
        legacy.load_run(path)
    manifest = json.loads((path / 'manifest.json').read_text())
    manifest.pop('workflow')
    (path / 'manifest.json').write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match='original entry point'):
        targeted.load_run(path)


@pytest.mark.parametrize('artifact', ['changes.csv', 'scopes.json', 'manifest.json'])
def test_review_tampering_refuses(staged, artifact):
    _, path, *_ = staged
    file = path / artifact
    if artifact == 'changes.csv':
        file.write_text(file.read_text() + '\nchanged')
    else:
        data = json.loads(file.read_text())
        data['mode' if artifact == 'scopes.json' else 'code'] = 'changed'
        file.write_text(json.dumps(data))
    with pytest.raises(ValueError):
        targeted.load_run(path)


def test_rehashed_wrong_arithmetic_is_rebuilt_and_refused(staged):
    _, path, *_ = staged
    manifest, plan = targeted.load_run(path)
    record = plan['scopes'][0]
    record['updates']['projects'][0]['total_order_value'] = '999.00'
    record['after']['projects'][0]['total_order_value'] = '999.00'
    manifest['sha256'] = backfill.fingerprint(plan)
    (path / 'manifest.json').write_text(json.dumps(manifest))
    (path / 'scopes.json').write_text(json.dumps(plan))
    with pytest.raises(ValueError, match='source evidence'):
        targeted.load_run(path)


def test_staging_includes_stored_external_owners_and_defers(staged, tmp_path):
    connection, _, _, baseline, *_ = staged
    baseline['subitems'].append({**baseline['subitems'][0], 'monday_id': 'foreign', 'parent_monday_id': '999'})
    manifest = targeted.stage_run(connection, None, tmp_path / 'capture', tmp_path / 'shared',
                                 mode='orders', project_ids={'101'})
    assert manifest['scopes'] == 0 and manifest['deferred_scopes'] == 1


def test_changed_stored_source_outside_lock_boundary_is_deferred(staged, tmp_path):
    connection, _, _, baseline, *_ = staged
    baseline['subitems'][0]['hidden_item_id'] = 'new-source'
    # The fixture's capture is regenerated from baseline, so freeze the original
    # capture before making the current SQL state diverge from it.
    origin, captured, source, plan = backfill.load_run(tmp_path)
    captured['subitems'][0]['hidden_item_id'] = '301'
    from unittest.mock import patch
    with patch.object(backfill, 'load_run', return_value=(origin, captured, source, plan)):
        manifest = targeted.stage_run(connection, None, tmp_path / 'capture', tmp_path / 'relinked',
                                     mode='orders', project_ids={'101'})
    assert manifest['scopes'] == 0 and manifest['deferred_scopes'] == 1


def setup_apply(staged, monkeypatch):
    connection, path, manifest, _, source, *_ = staged
    _, plan = targeted.load_run(path)
    monkeypatch.setattr(targeted, 'committed_scopes', lambda *a: set())
    monkeypatch.setattr(reads, 'capture_ownership', lambda *a: {'items': source['subitems']})
    return connection, path, manifest, plan


def test_all_pending_over_100_scopes_shares_one_scan_and_keeps_independent_commits(staged, monkeypatch):
    connection, path, manifest, plan = setup_apply(staged, monkeypatch)
    template = plan['scopes'][0]
    plan['scopes'] = [{**template, 'scope_id': f'scope-{i}'} for i in range(121)]
    monkeypatch.setattr(targeted, 'load_run', lambda *a: (manifest, plan))
    scans, commits, checks = [], [], []
    monkeypatch.setattr(reads, 'capture_ownership', lambda *a: scans.append(True) or {'items': []})
    def check(monday, record, owners, **kwargs):
        checks.append(record['scope_id'])
        if record['scope_id'] == 'scope-2':
            raise legacy.ScopeConflict('changed Monday value')
    monkeypatch.setattr(reads, 'check_scope', check)
    monkeypatch.setattr(targeted, 'commit_scope', lambda c, m, p, r: commits.append(r['scope_id']) or
                        {'scope_id': r['scope_id'], 'status': 'committed_pending_source_verification'})
    result = targeted.apply_run(connection, None, path, confirm_run_id=manifest['run_id'],
                                allow_partial=True, all_pending=True)
    assert len(scans) == 1 and len(checks) == 121 and len(commits) == 120
    assert result['remaining_uncommitted'] == 1 and result['remaining_unattempted'] == 0
    assert result['counts']['deferred'] == 1 and 'scope-120' in commits
    assert len(list(path.glob('ownership-apply-*.json'))) == 1


def test_retry_skips_committed_scope_without_source_reads(staged, monkeypatch):
    connection, path, manifest, plan = setup_apply(staged, monkeypatch)
    monkeypatch.setattr(targeted, 'committed_scopes', lambda *a: {plan['scopes'][0]['scope_id']})
    monkeypatch.setattr(reads, 'capture_ownership', lambda *a: pytest.fail('No pending scopes'))
    result = targeted.apply_run(connection, None, path, confirm_run_id=manifest['run_id'],
                                allow_partial=True, all_pending=True)
    assert result['previously_committed'] == 1 and result['results'] == []


@pytest.mark.parametrize('options', [{'limit': 101}, {'limit': 0}, {'limit': 1, 'all_pending': True}])
def test_invalid_batch_arguments_refuse_before_reads(staged, monkeypatch, options):
    connection, path, manifest, *_ = staged
    monkeypatch.setattr(reads, 'capture_ownership', lambda *a: pytest.fail('Must reject arguments first'))
    with pytest.raises(ValueError):
        targeted.apply_run(connection, None, path, confirm_run_id=manifest['run_id'], allow_partial=True, **options)


def test_failed_owner_inventory_never_attempts_commit(staged, monkeypatch):
    connection, path, manifest, _ = setup_apply(staged, monkeypatch)
    def fail(*a):
        raise ValueError('Incomplete owner scan')
    monkeypatch.setattr(reads, 'capture_ownership', fail)
    monkeypatch.setattr(targeted, 'commit_scope', lambda *a: pytest.fail('Incomplete ownership'))
    with pytest.raises(ValueError, match='Incomplete owner'):
        targeted.apply_run(connection, None, path, confirm_run_id=manifest['run_id'], allow_partial=True)


def test_new_foreign_monday_owner_defers_before_targeted_reads_or_writes(staged, monkeypatch):
    connection, path, manifest, plan = setup_apply(staged, monkeypatch)
    owner = plan['scopes'][0]['source']['owners'][0]
    monkeypatch.setattr(reads, 'capture_ownership', lambda *a: {'items': [owner, {**owner, 'monday_id': '202', 'parent_monday_id': '999'}]})
    monkeypatch.setattr(legacy, 'targeted_orders', lambda *a: pytest.fail('New shared owner must defer first'))
    monkeypatch.setattr(targeted, 'commit_scope', lambda *a: pytest.fail('New shared owner must not write'))
    result = targeted.apply_run(connection, None, path, confirm_run_id=manifest['run_id'], allow_partial=True)
    assert result['counts'] == {'deferred': 1} and result['remaining_uncommitted'] == 1


def test_connection_failure_stops_batch_for_journal_recovery(staged, monkeypatch):
    connection, path, manifest, _ = setup_apply(staged, monkeypatch)
    monkeypatch.setattr(reads, 'check_scope', lambda *a, **k: None)
    def fail(*a):
        raise targeted.psycopg.OperationalError('lost connection')
    monkeypatch.setattr(targeted, 'commit_scope', fail)
    with pytest.raises(targeted.psycopg.OperationalError):
        targeted.apply_run(connection, None, path, confirm_run_id=manifest['run_id'], allow_partial=True)
    assert not list(path.glob('apply-*.json'))


@pytest.mark.parametrize('changed', ['none', 'source', 'database', 'ownership'])
def test_verification_detects_drift_with_one_scan(staged, monkeypatch, changed):
    connection, path, _, plan = setup_apply(staged, monkeypatch)
    record = plan['scopes'][0]
    monkeypatch.setattr(targeted, 'committed_scopes', lambda *a: {record['scope_id']})
    actual = deepcopy(record['source'])
    if changed == 'source':
        actual['hidden_items'][0]['monday_total'] = '999.00'
    monkeypatch.setattr(legacy, 'targeted_orders', lambda *a: actual)
    current = deepcopy(record['after'])
    if changed == 'database':
        current['projects'][0]['total_order_value'] = '12.00'
    monkeypatch.setattr(targeted, 'read_boundary', lambda *a, **k: current)
    scans = []
    owners = [] if changed == 'ownership' else record['source']['owners']
    monkeypatch.setattr(reads, 'capture_ownership', lambda *a: scans.append(True) or {'items': owners})
    result = targeted.verify_run(connection, None, path)
    assert result['complete'] is (changed == 'none')
    assert result['certifies_entire_dataset'] is False and len(scans) == 1


def test_verification_with_no_commits_skips_monday(staged, monkeypatch):
    connection, path, _, _ = setup_apply(staged, monkeypatch)
    monkeypatch.setattr(reads, 'capture_ownership', lambda *a: pytest.fail('No committed scopes'))
    result = targeted.verify_run(connection, None, path)
    assert result['counts'] == {'not_committed': 1} and not result['complete']


@pytest.mark.parametrize('drift', ['none', 'formula', 'membership'])
def test_actual_financial_and_parent_queries_use_exact_scope_ids(staged, monkeypatch, drift):
    _, path, _, _, source, scope, _ = staged
    _, plan = targeted.load_run(path)
    record = plan['scopes'][0]
    parents = deepcopy(source['exclusion_evidence']['parent_details']['items'])
    if drift == 'membership':
        parents[0]['subitems'].append({**parents[0]['subitems'][0], 'id': '202'})
    seen = []
    def parent_details(monday, ids, **kwargs):
        assert ids == scope['projects'] and kwargs['include_subitems']
        seen.append('parents')
        return {'items': parents, 'not_returned_ids': []}
    def columns(monday, ids, selected_columns, board_id):
        if board_id == backfill.SUBITEM_BOARD_ID:
            assert ids == scope['subitems']
            assert selected_columns == [backfill.SUBITEM_COLUMNS['hidden_item_id']]
            seen.append('children')
            return [raw_link()]
        assert board_id == backfill.HIDDEN_ITEMS_BOARD_ID and ids == scope['hidden_items']
        assert set(selected_columns) == {backfill.TOTAL_COLUMN, *(backfill.HIDDEN_ITEMS_COLUMNS[f] for f in backfill.ORDER_FIELDS)}
        seen.append('hidden')
        values = [{'id': backfill.HIDDEN_ITEMS_COLUMNS[f], 'type': 'numbers', 'value': json.dumps(amount)}
                  for f, amount in zip(backfill.ORDER_FIELDS, ('100', '5'))]
        values.append({'id': backfill.TOTAL_COLUMN, 'type': 'formula', 'value': None,
                       'display_value': '999' if drift == 'formula' else '105'})
        return [{'id': '301', 'column_values': values}]
    monkeypatch.setattr(backfill, 'fetch_inventory_details', parent_details)
    monkeypatch.setattr(reconcile, 'fetch_columns', columns)
    owners = reads.owner_index({'items': record['source']['owners']})
    if drift == 'none':
        reads.check_scope(None, record, owners, mode='orders')
    else:
        with pytest.raises(ValueError):
            reads.check_scope(None, record, owners, mode='orders')
    assert seen == ['parents', 'children', 'hidden']


def test_repair_additional_fields_are_rechecked(staged, monkeypatch):
    _, path, *_ = staged
    _, plan = targeted.load_run(path)
    record = plan['scopes'][0]
    record['raw'] = {'hidden_items': [{'amount_invoiced': '0'}], 'subitems': []}
    monkeypatch.setattr(legacy, 'targeted_orders', lambda *a: record['source'])
    monkeypatch.setattr(reconcile, 'capture_targeted', lambda *a: {'hidden_items': [{'amount_invoiced': '50'}], 'subitems': []})
    with pytest.raises(ValueError, match='repair inputs changed'):
        reads.check_scope(None, record, reads.owner_index({'items': record['source']['owners']}), mode='repair')


def raw_link(item_id='201', parent_id='101', hidden_id='301'):
    return {'id': item_id, 'state': 'active', 'board': {'id': backfill.SUBITEM_BOARD_ID},
            'parent_item': {'id': parent_id, 'state': 'active', 'board': {'id': backfill.PARENT_BOARD_ID}} if parent_id else None,
            'column_values': [{'id': backfill.SUBITEM_COLUMNS['hidden_item_id'], 'type': 'board_relation',
                               'value': None, 'linked_item_ids': [hidden_id]}]}


class MondayPages:
    headers = {'API-Version': '2025-07'}

    def __init__(self, pages, counts=(0, 0)):
        self.pages = iter(pages)
        self.counts = iter(counts)
        self.calls = []

    def execute_query(self, query, variables):
        self.calls.append((query, deepcopy(variables)))
        page = next(self.pages)
        if 'cursor' in variables:
            return {'data': {'next_items_page': page}}
        return {'data': {'boards': [{'id': backfill.SUBITEM_BOARD_ID, 'items_page': page}]}}

    def get_board_info(self, board_id):
        assert board_id == backfill.SUBITEM_BOARD_ID
        return {'items_count': next(self.counts)}


def test_link_pagination_is_complete_and_requests_only_relationship_columns():
    monday = MondayPages([{'items': [raw_link()], 'cursor': 'next'},
                          {'items': [raw_link('202')], 'cursor': None}])
    scan = reads.link_pages(monday)
    assert [r['monday_id'] for r in scan['items']] == ['201', '202']
    assert len(monday.calls) == 2
    assert all(v['columns'] == [backfill.SUBITEM_COLUMNS['hidden_item_id']] for _, v in monday.calls)
    assert all('mutation' not in q and 'subitems {' not in q for q, _ in monday.calls)


@pytest.mark.parametrize('pages', [
    [{'items': [raw_link()]}],
    [{'items': [raw_link()], 'cursor': ''}],
    [{'items': [raw_link()], 'cursor': 'same'}, {'items': [raw_link('202')], 'cursor': 'same'}],
    [{'items': [raw_link()], 'cursor': 'next'}, {'items': [raw_link()], 'cursor': None}],
    [{'items': [], 'cursor': 'next'}],
    [{'items': [raw_link(), raw_link()], 'cursor': None}],
])
def test_incomplete_or_repeating_pages_fail_closed(pages):
    with pytest.raises(ValueError):
        reads.link_pages(MondayPages(pages))


@pytest.mark.parametrize('change', ['state', 'board', 'parent', 'parent_id', 'column', 'invalid_link'])
def test_incomplete_or_invalid_link_evidence_is_rejected(change):
    row = raw_link()
    if change == 'state':
        row['state'] = 'archived'
    elif change == 'board':
        row['board'] = {'id': 'wrong'}
    elif change == 'parent':
        row.pop('parent_item')
    elif change == 'parent_id':
        row['parent_item'] = {}
    elif change == 'column':
        row['column_values'] = []
    else:
        row['column_values'][0]['linked_item_ids'] = None
    with pytest.raises(ValueError):
        reads.link_pages(MondayPages([{'items': [row], 'cursor': None}]))


def exception_links():
    rows = []
    for excluded, expected in backfill.REVIEWED_PARENTLESS_DUPLICATES.items():
        rows.extend([raw_link(excluded, None, expected['hidden_id']),
                     raw_link(expected['subitem_id'], expected['parent_id'], expected['hidden_id'])])
    return rows


@pytest.mark.parametrize('counts, omit', [((4, 4), False), ((5, 5), False), ((4, 5), False), ((4, 4), True)])
def test_exact_four_duplicate_count_contract_is_preserved(counts, omit):
    rows = exception_links()
    if omit:
        rows.pop(0)
    monday = MondayPages([{'items': rows, 'cursor': None}], counts)
    if counts == (4, 4) and not omit:
        assert len(reads.scan_links(monday)['items']) == 8
    else:
        with pytest.raises(ValueError, match='counts'):
            reads.scan_links(monday)


@pytest.fixture
def exceptions(monkeypatch):
    connection = ReadConnection()
    links = exception_links()
    scan = {'items': [reads.normalized_link(row) for row in links]}
    parents, hidden = [], []
    state = {t: [] for t in legacy.TABLES}
    for expected in backfill.REVIEWED_PARENTLESS_DUPLICATES.values():
        child = raw_link(expected['subitem_id'], expected['parent_id'], expected['hidden_id'])
        child.pop('column_values')
        parents.append({'id': expected['parent_id'], 'state': 'active', 'board': {'id': backfill.PARENT_BOARD_ID},
                        'parent_item': None, 'subitems': [child]})
        columns = [{'id': backfill.HIDDEN_ITEMS_COLUMNS[f], 'type': 'numbers', 'value': None, 'text': ''}
                   for f in (*backfill.ORDER_FIELDS, 'amount_invoiced', 'date_order_received', 'invoice_date')]
        columns.append({'id': backfill.TOTAL_COLUMN, 'type': 'formula', 'value': None, 'display_value': '0.00'})
        hidden.append({'id': expected['hidden_id'], 'name': 'Example', 'state': 'active',
                       'board': {'id': backfill.HIDDEN_ITEMS_BOARD_ID}, 'column_values': columns})
        state['projects'].append({'monday_id': expected['parent_id']})
        state['hidden_items'].append({'monday_id': expected['hidden_id']})
        state['subitems'].append({'monday_id': expected['subitem_id'], 'parent_monday_id': expected['parent_id'],
                                  'hidden_item_id': expected['hidden_id']})
    monkeypatch.setattr(backfill, 'fetch_inventory_details', lambda *a, **k: {'items': parents, 'not_returned_ids': []})
    monkeypatch.setattr(reconcile, 'fetch_columns', lambda *a, **k: hidden)
    boundaries = []
    def boundary(conn, value):
        boundaries.append(value)
        return state
    monkeypatch.setattr(legacy, 'read_boundary', boundary)
    return connection, scan, parents, hidden, state, boundaries


def test_exception_validation_uses_only_selected_ids_and_reverse_sql_owners(exceptions):
    connection, scan, _, _, _, boundaries = exceptions
    reads.validate_exceptions(connection, None, scan)
    assert len(boundaries) == 1
    excluded = set(backfill.REVIEWED_PARENTLESS_DUPLICATES)
    assert all(excluded <= set(boundaries[0][t]) for t in legacy.TABLES)
    assert all('READ ONLY' in q for q in connection.statements)


@pytest.mark.parametrize('drift', ['order', 'invoice', 'date', 'stored_amount', 'stored_date', 'stored_owner',
                                  'monday_owner', 'excluded_in_db', 'missing_parent', 'missing_child', 'relinked_duplicate'])
def test_each_duplicate_exclusion_still_requires_financial_and_relationship_proof(exceptions, drift):
    connection, scan, parents, hidden, state, _ = exceptions
    expected = next(iter(backfill.REVIEWED_PARENTLESS_DUPLICATES.values()))
    if drift in ('order', 'invoice', 'date'):
        field = {'order': backfill.ORDER_FIELDS[0], 'invoice': 'amount_invoiced', 'date': 'invoice_date'}[drift]
        col = next(c for c in hidden[0]['column_values'] if c['id'] == backfill.HIDDEN_ITEMS_COLUMNS[field])
        col['value'] = json.dumps('1' if drift != 'date' else {'date': '2026-10-02'})
    elif drift == 'stored_amount':
        state['subitems'][0]['cust_additional_charges'] = '1.00'
    elif drift == 'stored_date':
        state['hidden_items'][0]['invoice_date'] = '2026-10-02'
    elif drift == 'stored_owner':
        state['subitems'].append({'monday_id': 'external', 'hidden_item_id': expected['hidden_id']})
    elif drift == 'monday_owner':
        scan['items'].append(reads.normalized_link(raw_link('999', '888', expected['hidden_id'])))
    elif drift == 'excluded_in_db':
        state['projects'].append({'monday_id': next(iter(backfill.REVIEWED_PARENTLESS_DUPLICATES))})
    elif drift == 'missing_parent':
        state['projects'].pop(0)
    elif drift == 'missing_child':
        parents[0]['subitems'] = []
    else:
        scan['items'][0]['hidden_ids'] = ['wrong']
    with pytest.raises(ValueError, match='Reviewed duplicate'):
        reads.validate_exceptions(connection, None, scan)


def test_filtered_query_is_numeric_paginated_and_diagnostic_only():
    rows = exception_links() + [raw_link()]
    monday = MondayPages([{'items': rows[:4], 'cursor': 'more'}, {'items': rows[4:], 'cursor': None}])
    scan = {'items': [reads.normalized_link(row) for row in rows]}
    result = reads.compare_owner_filter(monday, scan, {'301'})
    assert result['matches'] and result['diagnostic_only'] and not result['enables_filtered_apply']
    rule = monday.calls[0][1]['filter']['rules'][0]
    assert rule['operator'] == 'any_of' and 301 in rule['compare_value']
    assert all(isinstance(i, int) for i in rule['compare_value'])


def test_filter_omitting_parentless_duplicates_is_reported_as_incomplete():
    rows = exception_links()
    monday = MondayPages([{'items': rows[1::2], 'cursor': None}])
    result = reads.compare_owner_filter(monday, {'items': [reads.normalized_link(r) for r in rows]}, set())
    assert not result['matches']
    assert result['missing_owner_ids'] == sorted(backfill.REVIEWED_PARENTLESS_DUPLICATES)
    assert not result['enables_filtered_apply']


def test_cli_all_pending_and_limit_are_mutually_exclusive():
    with pytest.raises(SystemExit):
        targeted.argument_parser().parse_args(['apply', '--run-dir', 'x', '--confirm-run-id', 'id',
                                               '--all-pending', '--limit', '1'])
