"""Offline blocked-project classification and compatibility with guarded apply."""
from copy import deepcopy
import json
import socket

import pytest
import requests

from scripts import backfill_order_values as backfill
from scripts import order_value_blocked_capture as capture
from scripts import order_value_blocked_review as review
from scripts import order_value_scope_reads as reads
from scripts import order_value_scopes as scopes
from scripts import order_value_scopes_targeted as targeted
from scripts import reconcile_order_values as reconcile
from test_order_value_scopes import ReadConnection, sample
from test_reconcile_order_values import raw_rows


@pytest.fixture(autouse=True)
def no_live(monkeypatch):
    def forbidden(*a, **kw):
        pytest.fail('No live I/O in blocked-project tests')
    monkeypatch.setattr(requests.sessions.Session, 'request', forbidden)
    monkeypatch.setattr(socket.socket, 'connect', forbidden)
    monkeypatch.setattr(capture.psycopg, 'connect', forbidden)


def inputs():
    baseline, source, _ = sample()
    hidden, children = raw_rows()
    parent = source['exclusion_evidence']['parent_details']['items'][0]
    children[0].update(state='active', parent_item=deepcopy(parent['subitems'][0]['parent_item']))
    hidden[0].update(state='active', parent_item=None)
    for column in children[0]['column_values']:
        column['type'] = 'board_relation' if column['id'] == backfill.SUBITEM_COLUMNS['hidden_item_id'] else 'mirror'
    for column in hidden[0]['column_values']:
        column['type'] = 'numbers'
        if column['id'] == backfill.HIDDEN_ITEMS_COLUMNS[backfill.ORDER_FIELDS[0]]:
            column['value'] = '"100.00"'
        elif column['id'] == backfill.HIDDEN_ITEMS_COLUMNS[backfill.ORDER_FIELDS[1]]:
            column['value'] = '"5.00"'
    hidden[0]['column_values'].append({'id': backfill.TOTAL_COLUMN, 'type': 'formula',
                                      'value': None, 'text': None, 'display_value': '105.00'})
    updates = reconcile.transform_exact_rows(hidden, children, {'101'})
    contract = {table: {key: {'type': 'numeric' if isinstance(value, (float, int)) or 'value' in key
                                          or key in ('quote_amount', 'total_amount_invoiced') else 'text',
                              'scale': 2, 'generated': 'NEVER'}
                        for key, value in rows[0].items()} for table, rows in updates.items()}
    for table in scopes.TABLES:
        for key in backfill.BASELINE_COLUMNS[table]:
            contract[table].setdefault(key, {'type': 'text', 'scale': None, 'generated': 'NEVER'})
    source_data = {'parents': {'items': [parent], 'not_returned_ids': []},
                   'children': {'items': children, 'not_returned_ids': [],
                                'requested_columns': sorted(c['id'] for c in children[0]['column_values'])},
                   'hidden': {'items': hidden, 'not_returned_ids': [],
                              'requested_columns': sorted(c['id'] for c in hidden[0]['column_values'])}}
    context = {'selected_project_ids': ['101'], 'complete': True, 'target': 'test-target',
               'boundary': {'projects': ['101'], 'subitems': ['201'], 'hidden_items': ['301']},
               'previous_capture_id': 'original', 'original_summary': {'blocked_projects': 599},
               'finished_at': '2026-10-02T18:00:00+00:00', 'safety': {'journal_installed': True}}
    docs = {'source': source_data, 'baseline': baseline, 'contract': contract,
            'ownership': {'items': source['subitems']}, 'exceptions': {'valid': True, 'reviewed_ids': []}}
    return context, docs


def save_capture(path, context, docs):
    path.mkdir()
    for key, value in docs.items():
        context[key + '_sha256'] = backfill.fingerprint(value)
        if key == 'source':
            for label, data in value.items():
                backfill.write_json(path / (label + '.json'), data)
        else:
            backfill.write_json(path / (key + '.json'), value)
    backfill.write_json(path / 'context.json', context)


def run_review(tmp_path, context, docs):
    save_capture(tmp_path / 'capture', context, docs)
    return review.review(tmp_path / 'capture', tmp_path / 'review')


def test_current_evidence_can_release_previously_blocked_project_as_orders(tmp_path):
    context, docs = inputs()
    result = run_review(tmp_path, context, docs)
    assert result['counts'] == {'orders_ready': 1}
    assert result['runs']['orders']['changes'] == 5
    _, staged = targeted.load_run(tmp_path / 'review/orders-run')
    assert staged['scopes'][0]['after']['projects'][0]['total_order_value'] == '105.00'


def test_explicit_monday_links_drive_repair_and_runtime_loader_rebuilds_full_updates(tmp_path):
    context, docs = inputs()
    docs['baseline']['subitems'][0]['hidden_item_id'] = None
    result = run_review(tmp_path, context, docs)
    assert result['counts'] == {'repair_ready': 1}
    _, staged = targeted.load_run(tmp_path / 'review/repair-run')
    record = staged['scopes'][0]
    assert record['after']['subitems'][0]['hidden_item_id'] == '301'
    assert record['after']['projects'][0]['total_order_value'] == '105.00'
    assert record['source']['owners'] == record['source']['subitems']


def test_already_correct_project_is_not_staged_again(tmp_path):
    context, docs = inputs()
    for table in ('subitems', 'hidden_items'):
        docs['baseline'][table][0].update(cust_order_value_material='100.00', cust_additional_charges='5.00')
    docs['baseline']['projects'][0]['total_order_value'] = '105.00'
    result = run_review(tmp_path, context, docs)
    assert result['counts'] == {'already_matches': 1} and not result['runs']


@pytest.mark.parametrize('problem', ['inactive_parent', 'missing_parent', 'missing_database_child',
                                     'missing_database_source', 'empty', 'bad_formula', 'multiple_links',
                                     'outside_monday_owner', 'outside_database_owner', 'source_drift',
                                     'incomplete_columns', 'exception_changed', 'stale_database_child'])
def test_blockers_are_explicit_and_never_staged(tmp_path, problem):
    context, docs = inputs()
    if problem == 'inactive_parent': docs['source']['parents']['items'][0]['state'] = 'archived'
    elif problem == 'missing_parent': docs['source']['parents']['items'] = []
    elif problem == 'missing_database_child': docs['baseline']['subitems'] = []
    elif problem == 'missing_database_source': docs['baseline']['hidden_items'] = []
    elif problem == 'empty':
        docs['ownership']['items'] = []
        docs['source']['parents']['items'][0]['subitems'] = []
        docs['source']['children']['items'] = []
        docs['baseline']['subitems'] = []
    elif problem == 'bad_formula': docs['source']['hidden']['items'][0]['column_values'][-1]['display_value'] = '999'
    elif problem in ('multiple_links', 'source_drift'):
        col = next(c for c in docs['source']['children']['items'][0]['column_values']
                   if c['id'] == backfill.SUBITEM_COLUMNS['hidden_item_id'])
        col['linked_item_ids'] = ['301', '302'] if problem == 'multiple_links' else ['302']
    elif problem == 'outside_monday_owner':
        docs['ownership']['items'].append({**docs['ownership']['items'][0], 'monday_id': '299', 'parent_monday_id': '199'})
    elif problem == 'outside_database_owner':
        docs['baseline']['subitems'].append({**docs['baseline']['subitems'][0], 'monday_id': '299', 'parent_monday_id': '199'})
    elif problem == 'incomplete_columns': docs['source']['children']['items'][0]['column_values'].pop()
    elif problem == 'exception_changed': docs['exceptions']['valid'] = False
    elif problem == 'stale_database_child':
        docs['baseline']['subitems'].append({**docs['baseline']['subitems'][0], 'monday_id': '299'})
    result = run_review(tmp_path, context, docs)
    assert not result['runs']
    assert set(result['counts']) <= {'manual_review', 'needs_missing_rows'}
    if problem in ('missing_database_child', 'missing_database_source'):
        assert result['counts'] == {'needs_missing_rows': 1}


def test_capture_tampering_or_partial_capture_is_rejected(tmp_path):
    context, docs = inputs()
    save_capture(tmp_path / 'capture', context, docs)
    path = tmp_path / 'capture/baseline.json'
    changed = json.loads(path.read_text())
    changed['projects'][0]['total_order_value'] = '999'
    path.write_text(json.dumps(changed), encoding='utf-8')
    with pytest.raises(ValueError, match='hashes'):
        review.load_capture(tmp_path / 'capture')
    context['complete'] = False
    (tmp_path / 'capture/context.json').write_text(json.dumps(context), encoding='utf-8')
    with pytest.raises(ValueError, match='incomplete'):
        review.load_capture(tmp_path / 'capture')


def test_old_and_new_source_dependencies_are_kept_together():
    baseline = {'subitems': [{'monday_id': '201', 'parent_monday_id': '101', 'hidden_item_id': '301'},
                            {'monday_id': '202', 'parent_monday_id': '102', 'hidden_item_id': '301'}]}
    source = {'subitems': [{'monday_id': '201', 'parent_monday_id': '101', 'hidden_ids': ['302']},
                          {'monday_id': '202', 'parent_monday_id': '102', 'hidden_ids': ['303']}]}
    assert review.dependency_groups(['101', '102'], baseline, source) == [['101', '102']]


def test_capture_detail_errors_are_not_interpreted_as_absence():
    class Monday:
        def execute_query(self, *args):
            return {'data': {'items': []}, 'errors': [{'message': 'partial result'}]}
    with pytest.raises(ValueError, match='incomplete'):
        capture.details(Monday(), {'101'}, ['a'])


def test_capture_is_read_only_completes_receipt_and_can_finalize_without_rescanning(tmp_path, monkeypatch):
    context, docs = inputs()
    connection = ReadConnection()
    baseline, old_source, _ = sample()
    origin = {'target': backfill.target_fingerprint(connection), 'run_id': 'original',
              'hashes': {'plan': 'old-plan-hash'}, 'summary': {'blocked_projects': 1}}
    old_plan = {'projects': [{'project_id': '101', 'status': 'blocked'}], 'diagnostics': []}
    monkeypatch.setattr(backfill, 'load_run', lambda path: (origin, baseline, old_source, old_plan))
    monkeypatch.setattr(scopes, 'schema_safety', lambda *a, **k: {'journal_installed': True})
    monkeypatch.setattr(scopes, 'read_boundary', lambda *a, **k: deepcopy(baseline))
    monkeypatch.setattr(reconcile, 'read_contract', lambda *a: docs['contract'])
    scans = []
    monkeypatch.setattr(reads, 'scan_links', lambda *a: scans.append(True) or docs['ownership'])
    monkeypatch.setattr(reads, 'validate_exceptions', lambda *a: None)
    def fetch(monday, ids, columns=None, *, parents=False):
        label = 'parents' if parents else ('children' if backfill.SUBITEM_COLUMNS['hidden_item_id'] in columns else 'hidden')
        result = deepcopy(docs['source'][label])
        result['not_returned_ids'] = sorted(set(ids) - {r['id'] for r in result['items']})
        return result
    monkeypatch.setattr(capture, 'details', fetch)
    directory = tmp_path / 'capture'
    result = capture.capture(connection, None, tmp_path / 'old', directory)
    assert result['complete'] and len(scans) == 1
    loaded, documents = review.load_capture(directory)
    assert loaded['complete'] and documents['baseline'] == baseline
    assert all('READ ONLY' in statement for statement in connection.statements)
    with pytest.raises(ValueError, match='directory exists'):
        capture.capture(connection, None, tmp_path / 'old', directory)
    # Simulate interruption after data files, before the completion receipt.
    (directory / 'completed-context.json').unlink()
    monkeypatch.setattr(capture, 'details', lambda *a, **k: pytest.fail('Do not reread Monday when finalizing a saved capture'))
    result = capture.capture(connection, None, tmp_path / 'old', directory)
    assert result['finalized_saved_capture'] and len(scans) == 1
    assert review.load_capture(directory)[0]['complete']
