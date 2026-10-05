"""Offline source-of-truth projection and artifact safeguards."""
from copy import deepcopy
import json
import socket

import pytest
import requests

from scripts import order_value_monday_compare as compare
from scripts import order_value_scopes as scopes
from scripts import reconcile_order_values as reconcile
from test_order_value_scopes import ReadConnection


@pytest.fixture(autouse=True)
def no_live(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail('Offline tests must not contact services')
    monkeypatch.setattr(requests.sessions.Session, 'request', forbidden)
    monkeypatch.setattr(socket.socket, 'connect', forbidden)


def number(amount):
    return {'__typename': 'NumbersValue', 'number': amount}


def mirror(entries, function='sum'):
    settings = {'function': function} if function is not None else {}
    return {'__typename': 'MirrorValue', 'display_value': '',
            'column': {'settings_str': json.dumps(settings)},
            'mirrored_items': [{'linked_item': {'id': i}, 'linked_board_id': '123', 'mirrored_value': v}
                               for i, v in entries]}


def item(table, item_id, values, *, parent=None, name='Example'):
    return {'id': item_id, 'name': name, 'state': 'active', 'updated_at': '2026-10-03T00:00:00Z',
            'board': {'id': compare.BOARDS[table]}, 'parent_item': {'id': parent} if parent else None,
            'column_values': [{'id': key, **value} for key, value in values.items()]}


def fixture_data():
    h = compare.HIDDEN_ITEMS_COLUMNS
    s = compare.SUBITEM_COLUMNS
    p = compare.PARENT_COLUMNS
    hidden = item('hidden_items', '301', {
        **{h[f]: number(v) for f, v in zip(compare.MONEY_FIELDS, ['100', '5', '90', '105'])},
        **{h[f]: {'__typename': 'DateValue', 'date': None} for f in compare.DATE_FIELDS},
        h['status']: {'__typename': 'StatusValue', 'label': 'Archived'}})
    child = item('subitems', '201', {
        s['new_enquiry_value']: {'__typename': 'FormulaValue', 'display_value': '90'},
        s['hidden_item_id']: {'type': 'board_relation', 'linked_item_ids': ['301']},
        s['quote_amount']: mirror([('301', number('90'))]),
        s['amount_invoiced']: mirror([('301', number('105'))]),
        **{s[f]: mirror([('301', {'__typename': 'DateValue', 'date': None})]) for f in compare.DATE_FIELDS},
        s['order_status']: mirror([('301', {'__typename': 'StatusValue', 'label': 'Won Closed'})]),
    }, parent='101')
    parent = item('projects', '101', {
        p['project_name']: {'__typename': 'TextValue', 'text': 'Project'},
        p['pipeline_stage']: {'__typename': 'StatusValue', 'label': 'Won Closed'},
        p['total_order_value']: mirror([('201', mirror([('301', number('105'))]))]),
        p['new_enq_value_mirror']: mirror([('201', number('90'))]),
    }, name='nonnumeric project')
    parent['subitems'] = [{'id': '201', 'parent_item': {'id': '101'}}]
    evidence = {'projects': {'101': parent}, 'subitems': {'201': child}, 'hidden_items': {'301': hidden},
                'project_ids': ['101'], 'extra_children': ['201']}
    before = {'projects': [{'monday_id': '101', 'item_name': 'nonnumeric project', 'project_name': 'Project',
                'pipeline_stage': 'Won Closed', 'total_order_value': '40.00', 'new_enquiry_value': '90.00',
                'total_amount_invoiced': None}],
        'hidden_items': [{'monday_id': '301', 'item_name': 'Example', 'status': 'Archived',
            'cust_order_value_material': '40.00', 'cust_additional_charges': '0.00', 'quote_amount': '90.00',
            'amount_invoiced': '105.00', 'invoice_date': None, 'date_order_received': None}],
        'subitems': [{'monday_id': '201', 'parent_monday_id': '101', 'hidden_item_id': '301', 'item_name': 'Example',
            'cust_order_value_material': '40.00', 'cust_additional_charges': '0.00', 'quote_amount': '90.00',
            'amount_invoiced': '105.00', 'invoice_date': None, 'date_order_received': None, 'order_status': 'Won Closed'}]}
    financial = set(compare.MONEY_FIELDS) | {'total_order_value', 'total_amount_invoiced', 'new_enquiry_value'}
    contract = {t: {f: {'type': 'numeric' if f in financial else 'date' if f in compare.DATE_FIELDS else 'text',
                       'scale': 2 if f in financial else None, 'generated': 'NEVER'}
                    for f in rows[0]} for t, rows in before.items()}
    boundary = {'projects': ['101'], 'subitems': ['201'], 'hidden_items': ['301']}
    return evidence, before, contract, boundary


def record_for(evidence=None, before=None, contract=None, boundary=None):
    defaults = fixture_data()
    return compare.build_record(['101'], evidence or defaults[0], before or defaults[1],
                                contract or defaults[2], boundary or defaults[3])


def test_only_actual_fields_are_staged_and_archived_business_values_count():
    record = record_for()
    assert not record['issues']
    assert record['updates']['projects'] == [{'monday_id': '101', 'total_order_value': '105.00', 'total_amount_invoiced': '105.00'}]
    assert record['updates']['subitems'] == [{'monday_id': '201', 'cust_order_value_material': '100.00', 'cust_additional_charges': '5.00'}]
    assert not any('status' in r for r in record['updates']['hidden_items'])
    assert not any(record_for(before=record['after'])['updates'].values())


@pytest.mark.parametrize('stage, category', [
    ('Won - Closed (Invoiced)', 'Won'),
    ('Lost', 'Lost'),
    ('Won - Open (Order Received)', 'Open'),
    ('Won Closed', 'Open'),
    ('lost', 'Open'),
    ('Archived', 'Open'),
    (' Won - Closed (Invoiced) ', 'Open'),
    ('', 'Open'),
    (None, 'Open'),
])
def test_generated_category_uses_exact_schema_case_without_staging_a_write(stage, category):
    evidence, before, contract, boundary = fixture_data()
    before['projects'][0].update(pipeline_stage='Open Enquiry', status_category='Open')
    contract['projects']['status_category'] = {'type': 'text', 'scale': None, 'generated': 'ALWAYS'}
    compare.col(evidence['projects']['101'], compare.PARENT_COLUMNS['pipeline_stage'])['label'] = stage
    original = deepcopy(before)
    record = record_for(evidence, before, contract, boundary)
    assert not record['issues']
    # The existing source mapper represents an empty Monday label as SQL NULL.
    assert record['after']['projects'][0]['pipeline_stage'] == (stage or None)
    assert record['after']['projects'][0]['status_category'] == category
    assert all('status_category' not in update for update in record['updates']['projects'])
    assert before == original
    assert not any(record_for(evidence, record['after'], contract, boundary)['updates'].values())


def test_16312_total_is_7570_64_not_won_only():
    evidence, before, contract, boundary = fixture_data()
    parent = evidence['projects']['101']
    value = compare.col(parent, compare.PARENT_COLUMNS['total_order_value'])
    value.update(mirror([('201', mirror([('301', number('2104.88'))])),
                         ('202', mirror([('302', number('5465.76'))]))]))
    record = record_for(evidence=evidence)
    assert record['after']['projects'][0]['total_order_value'] == '7570.64'


def test_shared_source_preserves_both_children_and_monday_total():
    evidence, before, contract, boundary = fixture_data()
    child = deepcopy(evidence['subitems']['201'])
    child['id'] = '202'
    evidence['subitems']['202'] = child
    parent = evidence['projects']['101']
    parent['subitems'].append({'id': '202', 'parent_item': {'id': '101'}})
    compare.col(parent, compare.PARENT_COLUMNS['total_order_value']).update(
        mirror([('201', number('105')), ('202', number('105'))]))
    before['subitems'].append({**before['subitems'][0], 'monday_id': '202'})
    boundary['subitems'].append('202')
    record = record_for(evidence, before, contract, boundary)
    assert not record['issues']
    assert len(record['updates']['hidden_items']) == 1
    assert len(record['updates']['subitems']) == 2
    assert record['after']['projects'][0]['total_order_value'] == '210.00'
    assert record['after']['projects'][0]['total_amount_invoiced'] == '210.00'


def test_formula_authority_is_not_replaced_with_our_material_plus_charges():
    evidence, *_ = fixture_data()
    compare.col(evidence['projects']['101'], compare.PARENT_COLUMNS['total_order_value']).update(
        mirror([('201', {'__typename': 'FormulaValue', 'display_value': '999'})]))
    record = record_for(evidence=evidence)
    assert record['after']['projects'][0]['total_order_value'] == '999.00'
    assert record['after']['subitems'][0]['cust_order_value_material'] == '100.00'


@pytest.mark.parametrize('value', ['2,104.88, 5,465.76', '1,000', 'NaN', 'Infinity', '#ERROR!', True])
def test_ambiguous_numeric_values_are_never_silently_changed(value):
    with pytest.raises(ValueError):
        compare.decimal_value(value)


def test_unknown_aggregation_and_unreadable_mirror_are_deferred():
    with pytest.raises(ValueError, match='SUM'):
        compare.numeric(mirror([('1', number(1)), ('2', number(2))], function=None))
    with pytest.raises(ValueError, match='aggregation'):
        compare.numeric(mirror([('1', number(1))], function='average'))
    with pytest.raises(ValueError, match='unreadable'):
        compare.numeric(mirror([('1', None)]))
    contradictory = mirror([('1', number(None))])
    contradictory['display_value'] = '1200'
    with pytest.raises(ValueError, match='blank'):
        compare.numeric(contradictory)


def test_cleared_source_stays_null_and_empty_parent_is_compared():
    evidence, before, contract, boundary = fixture_data()
    compare.col(evidence['hidden_items']['301'], compare.HIDDEN_ITEMS_COLUMNS['cust_order_value_material'])['number'] = None
    assert record_for(evidence=evidence)['after']['subitems'][0]['cust_order_value_material'] is None
    evidence['projects']['101']['subitems'] = []
    for f in ('total_order_value', 'new_enq_value_mirror'):
        compare.col(evidence['projects']['101'], compare.PARENT_COLUMNS[f]).update(mirror([]))
    before['subitems'] = []
    record = record_for(evidence, before, contract, boundary)
    assert not record['issues']
    assert record['after']['projects'][0]['item_name'] == 'nonnumeric project'
    assert record['after']['projects'][0]['total_order_value'] is None
    assert record['after']['projects'][0]['new_enquiry_value'] == '0.00'


def test_stale_child_kept_and_not_used_in_invoice_total():
    evidence, before, contract, boundary = fixture_data()
    stale = {**before['subitems'][0], 'monday_id': '299', 'amount_invoiced': '5000.00'}
    before['subitems'].append(stale)
    record = record_for(evidence, before, contract, boundary)
    assert record['after']['subitems'][1] == stale
    assert record['after']['projects'][0]['total_amount_invoiced'] == '105.00'
    assert any('absent from current' in i['reason'] for i in record['issues'])


@pytest.mark.parametrize('state', ['archived', 'deleted', 'missing'])
def test_actual_lifecycle_and_unavailable_parent_are_not_rewritten(state):
    evidence, *_ = fixture_data()
    if state == 'missing':
        evidence['projects'] = {}
    else:
        evidence['projects']['101']['state'] = state
    record = record_for(evidence=evidence)
    assert not any(record['updates'].values())
    assert record['issues']


@pytest.mark.parametrize('ids', [[], ['301', '302']])
def test_empty_and_multiple_links_are_reported_without_scalar_guess(ids):
    evidence, *_ = fixture_data()
    compare.col(evidence['subitems']['201'], compare.SUBITEM_COLUMNS['hidden_item_id'])['linked_item_ids'] = ids
    record = record_for(evidence=evidence)
    assert not record['updates']['subitems']
    assert any(i['field'] == 'hidden_item_id' for i in record['issues'])
    assert record['updates']['projects']


def test_missing_source_requires_rehydration_without_fk_write():
    evidence, before, contract, boundary = fixture_data()
    before['hidden_items'] = []
    before['subitems'][0]['hidden_item_id'] = None
    record = record_for(evidence, before, contract, boundary)
    assert not record['updates']['hidden_items']
    assert 'hidden_item_id' not in record['updates']['subitems'][0]
    assert any('rehydration' in i['reason'] for i in record['issues'])


def test_report_selection_uses_status_not_item_name(tmp_path):
    report = tmp_path / 'projects.csv'
    report.write_text('project_id,item_name,status\n101,New project,manual_review\n102,12345,already_matches\n')
    assert compare.project_ids_from_report(report) == ['101']
    report.write_text('project_id,action\n101,manual_review\n101,manual_review\n')
    with pytest.raises(ValueError, match='unique'):
        compare.project_ids_from_report(report)


@pytest.fixture
def staged(tmp_path, monkeypatch):
    evidence, before, contract, _ = fixture_data()
    connection = ReadConnection()
    monkeypatch.setattr(compare, 'capture', lambda *a, **k: deepcopy(evidence))
    monkeypatch.setattr(scopes, 'schema_safety', lambda *a, **k: {})
    monkeypatch.setattr(scopes, 'read_boundary', lambda *a, **k: deepcopy(before))
    monkeypatch.setattr(reconcile, 'read_contract', lambda *a: contract)
    run_dir = tmp_path / 'run'
    manifest = compare.stage_run(connection, None, run_dir, ['101'])
    return connection, run_dir, manifest


def test_stage_read_only_and_review_tampering_rejected(staged):
    connection, run_dir, manifest = staged
    assert all('READ ONLY' in str(s) for s in connection.statements)
    assert manifest['changes'] == 6
    assert manifest['unresolved_fields'] == 0
    assert compare.load_run(run_dir)[0] == manifest
    with (run_dir / 'changes.csv').open('a') as stream:
        stream.write('tampered')
    with pytest.raises(ValueError, match='evidence changed'):
        compare.load_run(run_dir)


def test_changed_source_rejected(staged, monkeypatch):
    _, run_dir, _ = staged
    record = compare.load_run(run_dir)[1]['scopes'][0]
    evidence = deepcopy(record['source'])
    evidence['projects']['101']['name'] = 'Finance changed Monday'
    monkeypatch.setattr(compare, 'capture', lambda *a: evidence)
    with pytest.raises(ValueError, match='Monday changed'):
        compare.check_source(None, record)


def test_partial_graphql_and_duplicate_items_rejected():
    class FakeMonday:
        def execute_query(self, query, variables):
            assert 'limit: 100' in query and 'exclude_nonactive: false' in query
            assert 'mirrored_items' in query and 'mutation' not in query
            return {'data': {'items': []}, 'errors': [{'message': 'unavailable'}]}
    with pytest.raises(ValueError, match='Incomplete GraphQL'):
        compare.fetch_items(FakeMonday(), ['101'], ['x'])


def test_mirror_contribution_order_is_not_financial_drift():
    a = mirror([('1', number(1)), ('2', number(2))])
    b = deepcopy(a)
    b['mirrored_items'].reverse()
    assert compare.canonical_source(a) == compare.canonical_source(b)


def test_current_child_reparenting_is_withheld_as_one_row():
    evidence, before, contract, boundary = fixture_data()
    before['subitems'][0]['parent_monday_id'] = '999'
    record = record_for(evidence, before, contract, boundary)
    assert not record['updates']['subitems']
    assert any('old parent 999' in i['reason'] for i in record['issues'])


def test_exact_reads_return_absence_without_guessing_and_are_batched():
    calls = []
    class EmptyMonday:
        def execute_query(self, query, variables):
            calls.append(variables['ids'])
            return {'data': {'items': []}}
    ids = [str(i) for i in range(200)]
    assert compare.fetch_items(EmptyMonday(), ids, ['x']) == {}
    assert len(calls) == 20 and all(len(batch) == 10 for batch in calls)
    assert sorted(i for batch in calls for i in batch) == sorted(ids)


def test_multiple_parents_pack_into_one_nonoverlapping_scope(tmp_path, monkeypatch):
    evidence, before, contract, _ = fixture_data()
    # A second parent, no children, remains eligible regardless of name.
    other = deepcopy(evidence['projects']['101'])
    other.update(id='102', name='', subitems=[])
    for f in ('total_order_value', 'new_enq_value_mirror'):
        compare.col(other, compare.PARENT_COLUMNS[f]).update(mirror([]))
    evidence['projects']['102'] = other
    evidence['project_ids'].append('102')
    before['projects'].append({**before['projects'][0], 'monday_id': '102', 'item_name': ''})
    connection = ReadConnection()
    monkeypatch.setattr(compare, 'capture', lambda *a, **k: deepcopy(evidence))
    monkeypatch.setattr(scopes, 'schema_safety', lambda *a, **k: {})
    monkeypatch.setattr(scopes, 'read_boundary', lambda *a, **k: deepcopy(before))
    monkeypatch.setattr(reconcile, 'read_contract', lambda *a: contract)
    run_dir = tmp_path / 'packed'
    manifest = compare.stage_run(connection, None, run_dir, ['101', '102'])
    _, plan = compare.load_run(run_dir)
    assert manifest['selected_projects'] == 2
    assert len(plan['scopes']) == 1
    assert plan['scopes'][0]['project_ids'] == ['101', '102']
    assert not plan['scopes'][0]['issues']


def test_source_change_before_apply_prevents_commit(staged, monkeypatch):
    connection, run_dir, manifest = staged
    monkeypatch.setattr(scopes, 'committed_scopes', lambda *a: set())
    evidence, *_ = fixture_data()
    evidence['projects']['101']['updated_at'] = 'New Finance edit'
    monkeypatch.setattr(compare, 'capture', lambda *a: evidence)
    def unexpected(*args):
        pytest.fail('Changed Monday evidence must never reach commit')
    monkeypatch.setattr(compare, 'commit_scope', unexpected)
    result = compare.execute_run(connection, None, run_dir, apply=True, confirm_run_id=manifest['run_id'])
    assert result['counts'] == {'requires_reassessment': 1}
    assert result['remaining_uncommitted'] == 1


def test_apply_requires_reviewed_run_id_before_any_query(staged, monkeypatch):
    connection, run_dir, _ = staged
    def unexpected(*args):
        pytest.fail('Invalid acknowledgement must never read/write live services')
    monkeypatch.setattr(scopes, 'schema_safety', unexpected)
    with pytest.raises(ValueError, match='confirm-run-id'):
        compare.execute_run(connection, None, run_dir, apply=True, confirm_run_id='wrong')
