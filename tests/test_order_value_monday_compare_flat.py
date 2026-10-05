"""Resolve mirrors from independently read exact IDs, never nested API values."""
from copy import deepcopy
import json

import pytest

from scripts import order_value_monday_compare as compare
from test_order_value_monday_compare import fixture_data, mirror, number, no_live
from test_order_value_scopes import ReadConnection


def reference(item_id, board, column, function='sum'):
    value = mirror([(item_id, None)], function=function)
    value['mirrored_items'][0].pop('mirrored_value')
    value['mirrored_items'][0]['linked_board_id'] = board
    settings = json.loads(value['column']['settings_str'])
    settings['displayed_linked_columns'] = {board: [column]}
    value['column']['settings_str'] = json.dumps(settings)
    return value


def set_column(item, column, value):
    try:
        current = compare.col(item, column)
    except ValueError:
        item['column_values'].append({'id': column, **value})
    else:
        current.clear()
        current.update(id=column, **value)


def flat_data():
    evidence, before, contract, boundary = fixture_data()
    parent = evidence['projects']['101']
    child = evidence['subitems']['201']
    source = evidence['hidden_items']['301']
    p, s, h = compare.PARENT_COLUMNS, compare.SUBITEM_COLUMNS, compare.HIDDEN_ITEMS_COLUMNS
    set_column(parent, p['total_order_value'], reference('201', compare.BOARDS['subitems'], s['cust_order_value_material']))
    set_column(parent, p['new_enq_value_mirror'], reference('201', compare.BOARDS['subitems'], s['new_enquiry_value']))
    set_column(child, s['new_enquiry_value'], {'__typename': 'FormulaValue', 'display_value': '90'})
    set_column(child, s['cust_order_value_material'], reference('301', compare.BOARDS['hidden_items'], h['cust_order_value_material']))
    for field in ('quote_amount', 'amount_invoiced', *compare.DATE_FIELDS):
        set_column(child, s[field], reference('301', compare.BOARDS['hidden_items'], h[field]))
    set_column(child, s['order_status'], reference('301', compare.BOARDS['hidden_items'], 'status_17__1'))
    set_column(source, 'status_17__1', {'__typename': 'StatusValue', 'label': 'Lost'})
    set_column(source, compare.backfill.TOTAL_COLUMN, {'__typename': 'FormulaValue', 'display_value': '105'})
    return evidence, before, contract, boundary


def test_parent_follows_its_actual_mirror_column_not_an_assumed_formula():
    evidence, before, contract, boundary = flat_data()
    original = deepcopy(evidence)
    record = compare.build_record(['101'], evidence, before, contract, boundary)
    assert not record['issues']
    # Config points to material 100, even though the source's order formula is 105.
    assert record['after']['projects'][0]['total_order_value'] == '100.00'
    assert record['after']['subitems'][0]['cust_additional_charges'] == '5.00'
    assert record['after']['hidden_items'][0]['status'] == 'Archived'
    assert record['after']['subitems'][0]['order_status'] == 'Lost'
    assert evidence == original  # Raw observations remain raw audit evidence.


def test_capture_discovers_exact_source_columns_and_never_queries_nested_values():
    evidence, _, _, _ = flat_data()
    calls = []
    class Monday:
        def execute_query(self, query, variables):
            assert 'mirrored_value' not in query and 'items_page' not in query
            calls.append(variables)
            index = {i: row for table in compare.scopes.TABLES for i, row in evidence[table].items()}
            rows = []
            for i in variables['ids']:
                item = deepcopy(index[i])
                item['column_values'] = [v for v in item['column_values'] if v['id'] in variables['columns']]
                rows.append(item)
            return {'data': {'items': rows}}
    captured = compare.capture(Monday(), ['101'])
    assert [c['ids'] for c in calls] == [['101'], ['201'], ['301']]
    assert compare.SUBITEM_COLUMNS['cust_order_value_material'] in calls[1]['columns']
    assert compare.SUBITEM_COLUMNS['new_enquiry_value'] in calls[1]['columns']
    assert 'status_17__1' in calls[2]['columns']
    total = compare.numeric(compare.resolved_col(captured, captured['projects']['101'], compare.PARENT_COLUMNS['total_order_value']))
    assert total == 100


@pytest.mark.parametrize('fault', ['missing_source', 'wrong_board', 'missing_column', 'ambiguous_columns', 'cycle'])
def test_bad_reference_withholds_only_unproven_fields(fault):
    evidence, before, contract, boundary = flat_data()
    child = evidence['subitems']['201']
    column = compare.SUBITEM_COLUMNS['cust_order_value_material']
    if fault == 'missing_source':
        evidence['hidden_items'] = {}
    elif fault == 'wrong_board':
        evidence['hidden_items']['301']['board']['id'] = 'unknown-board'
    elif fault == 'missing_column':
        evidence['hidden_items']['301']['column_values'] = [v for v in evidence['hidden_items']['301']['column_values']
            if v['id'] != compare.HIDDEN_ITEMS_COLUMNS['cust_order_value_material']]
    elif fault == 'ambiguous_columns':
        value = compare.col(child, column)
        value['column']['settings_str'] = json.dumps({'displayed_linked_columns': {
            compare.BOARDS['hidden_items']: ['one', 'two']}})
    elif fault == 'cycle':
        set_column(child, column, reference('101', compare.BOARDS['projects'], compare.PARENT_COLUMNS['total_order_value']))
    record = compare.build_record(['101'], evidence, before, contract, boundary)
    assert record['after']['projects'][0]['total_order_value'] == before['projects'][0]['total_order_value']
    assert any(i['field'] == 'total_order_value' for i in record['issues'])


def test_explicit_mirror_dependencies_are_part_of_the_guarded_boundary():
    evidence, before, _, _ = flat_data()
    # Mirror source differs from the scalar source relation. Both are evidence.
    extra = deepcopy(evidence['hidden_items']['301'])
    extra['id'] = '302'
    evidence['hidden_items']['302'] = extra
    set_column(evidence['subitems']['201'], compare.SUBITEM_COLUMNS['cust_order_value_material'],
               reference('302', compare.BOARDS['hidden_items'], compare.HIDDEN_ITEMS_COLUMNS['cust_order_value_material']))
    assert compare.boundary_for(['101'], evidence, before)['hidden_items'] == ['301', '302']


def test_list_and_legacy_settings_formats():
    value = {'column': {'settings_str': json.dumps({'displayed_linked_columns': [
        {'board_id': '123', 'column_ids': ['numbers']}]})}}
    assert compare.mirror_columns(value) == {'123': 'numbers'}
    value['column']['settings_str'] = json.dumps({'displayed_column': {'123': 'numbers'}})
    assert compare.mirror_columns(value) == {'123': 'numbers'}


def test_unknown_aggregate_is_not_silently_assumed():
    evidence, before, contract, boundary = flat_data()
    child = deepcopy(evidence['subitems']['201'])
    child['id'] = '202'
    evidence['subitems']['202'] = child
    parent = evidence['projects']['101']
    value = reference('201', compare.BOARDS['subitems'], compare.SUBITEM_COLUMNS['new_enquiry_value'], function=None)
    value['mirrored_items'].append({**value['mirrored_items'][0], 'linked_item': {'id': '202'}})
    set_column(parent, compare.PARENT_COLUMNS['new_enq_value_mirror'], value)
    record = compare.build_record(['101'], evidence, before, contract, boundary)
    assert any('confirmed SUM' in i['reason'] for i in record['issues'])
    assert record['after']['projects'][0]['new_enquiry_value'] == before['projects'][0]['new_enquiry_value']


def test_verify_retains_the_original_union_of_requested_columns(monkeypatch):
    source, *_ = flat_data()
    source['read_columns'] = {'projects': [], 'subitems': ['extra_from_another_scope'], 'hidden_items': ['extra_hidden']}
    calls = []
    def capture(*args, **kwargs):
        calls.append(kwargs)
        return deepcopy(source)
    monkeypatch.setattr(compare, 'capture', capture)
    compare.check_source(None, {'project_ids': ['101'], 'source': source})
    assert calls == [{'read_columns': source['read_columns']}]


def test_flat_evidence_can_stage_reload_and_pass_fresh_source_check(tmp_path, monkeypatch):
    source, before, contract, _ = flat_data()
    class Monday:
        def execute_query(self, query, variables):
            index = {i: r for t in compare.scopes.TABLES for i, r in source[t].items()}
            rows = []
            for i in variables['ids']:
                if i in index:
                    row = deepcopy(index[i])
                    row['column_values'] = [v for v in row['column_values'] if v['id'] in variables['columns']]
                    rows.append(row)
            return {'data': {'items': rows}}
    monkeypatch.setattr(compare.scopes, 'schema_safety', lambda *a: {})
    monkeypatch.setattr(compare.scopes, 'read_boundary', lambda *a, **k: deepcopy(before))
    monkeypatch.setattr(compare.reconcile, 'read_contract', lambda *a: contract)
    run_dir = tmp_path / 'flat_stage'
    manifest = compare.stage_run(ReadConnection(), Monday(), run_dir, ['101'])
    assert manifest['unresolved_fields'] == 0 and manifest['scopes_with_changes'] == 1
    _, staged = compare.load_run(run_dir)
    record = staged['scopes'][0]
    assert 'read_columns' in record['source']
    assert record['after']['projects'][0]['total_order_value'] == '100.00'
    compare.check_source(Monday(), record)
    compare.col(source['hidden_items']['301'], compare.HIDDEN_ITEMS_COLUMNS['cust_order_value_material'])['number'] = '999'
    with pytest.raises(ValueError, match='Monday changed'):
        compare.check_source(Monday(), record)
