"""Offline insertion-only source validation, transformations and artifact guards."""
from copy import deepcopy
import json
from pathlib import Path
import re
import socket

import pytest
import requests

from scripts import backfill_order_values as backfill
from scripts import order_value_monday_compare as compare
from scripts import order_value_rehydrate_missing as missing
from scripts import order_value_scopes as scopes
from test_reconcile_order_values import raw_rows


@pytest.fixture(autouse=True)
def no_live(monkeypatch):
    def forbidden(*a, **k):
        pytest.fail('Missing-row unit tests must not contact external services')
    monkeypatch.setattr(requests.sessions.Session, 'request', forbidden)
    monkeypatch.setattr(socket.socket, 'connect', forbidden)
    monkeypatch.setattr(missing.psycopg, 'connect', forbidden)


def example():
    targets = [{'project_id': '101', 'subitem_id': '201', 'hidden_item_id': '301'}]
    hidden, children = raw_rows()
    source = {'hidden_items': {'301': hidden[0]}, 'subitems': {'201': children[0]},
              'projects': {}, 'read_columns': missing.required_columns()}
    parent = {'id': '101', 'name': 'Example', 'parent_item': None,
              'subitems': [{'id': '201', 'parent_item': {'id': '101'}}], 'column_values': [
                  {'id': c, 'type': 'text', 'text': '', 'value': None}
                  for c in source['read_columns']['projects']]}
    source['projects']['101'] = parent
    for table in scopes.TABLES:
        for item in source[table].values():
            item.update(state='active', updated_at='2026-10-05T00:00:00Z', board={'id': compare.BOARDS[table]})
            item.setdefault('parent_item', None)
            for column in item['column_values']:
                column['type'] = 'text'
    h, s = compare.HIDDEN_ITEMS_COLUMNS, compare.SUBITEM_COLUMNS
    for field, value in [('cust_order_value_material', '100'), ('cust_additional_charges', '5'),
                         ('quote_amount', '90'), ('amount_invoiced', '105')]:
        compare.col(hidden[0], h[field]).update(__typename='NumbersValue', type='numbers',
                                              number=value, value=json.dumps(value), text=value)
    for field in compare.DATE_FIELDS:
        compare.col(hidden[0], h[field]).update(__typename='DateValue', type='date', date=None)
    compare.col(hidden[0], h['status']).update(__typename='StatusValue', type='status', label='Archived', text='Archived')
    for field in ('quote_amount', 'amount_invoiced', *compare.DATE_FIELDS, 'order_status'):
        source_column = h['status'] if field == 'order_status' else h[field]
        compare.col(children[0], s[field]).update(
            __typename='MirrorValue', type='mirror', display_value='',
            column={'settings_str': json.dumps({'displayed_linked_columns': {compare.BOARDS['hidden_items']: [source_column]}})},
            mirrored_items=[{'linked_item': {'id': '301'}, 'linked_board_id': compare.BOARDS['hidden_items']}])
    compare.col(children[0], s['new_enquiry_value']).update(
        __typename='FormulaValue', type='formula', display_value='90')
    compare.col(children[0], s['hidden_item_id'])['type'] = 'board_relation'
    before = {'projects': [{'monday_id': '101', 'item_name': 'Example', 'total_order_value': '999.99',
                           'pipeline_stage': 'Won - Closed (Invoiced)', 'status_category': 'Won'}],
              'hidden_items': [{'monday_id': '301', 'cust_order_value_material': '40.00'}], 'subitems': []}
    schema = (Path(__file__).resolve().parents[1] / 'src/database/schema/schema.sql').read_text()
    body = re.search(r'CREATE TABLE subitems \(([\s\S]*?)\n\);', schema).group(1)
    contract = {t: {} for t in scopes.TABLES}
    for name, kind, scale in re.findall(r'^\s+(\w+) (TEXT|DATE|UUID|NUMERIC\(\d+,\s*(\d+)\))', body, re.M):
        contract['subitems'][name] = {'type': 'numeric' if kind.startswith('NUMERIC') else kind.lower(),
                                     'scale': int(scale) if scale else None, 'generated': 'NEVER'}
    contract['subitems']['cust_additional_charges'] = {'type': 'numeric', 'scale': 2, 'generated': 'NEVER'}
    return targets, source, before, contract, []


def record_for():
    return missing.build_record(*example())


def test_production_rows_insert_only_and_do_not_call_rollups(monkeypatch):
    def forbidden(*a, **k):
        pytest.fail('Insertion must not calculate or write parent rollups')
    for name in ('_rollup_order_values_from_subitems', '_rollup_invoice_totals_from_subitems',
                 '_rollup_new_enquiry_from_subitems', '_rollup_invoice_date_ranges_from_subitems'):
        monkeypatch.setattr(backfill.DataSyncService, name, forbidden)
    targets, source, before, contract, defaults = example()
    original = deepcopy((source, before))
    record = missing.build_record(targets, source, before, contract, defaults)
    row = record['inserts']['subitems'][0]
    assert row['hidden_item_id'] == '301' and row['parent_monday_id'] == '101'
    assert row['cust_order_value_material'] == '100.00' and row['cust_additional_charges'] == '5.00'
    assert row['order_status'] == 'Archived' and row['new_enquiry_value'] == '90.00'
    assert record['inserts']['projects'] == record['inserts']['hidden_items'] == []
    assert record['after']['projects'] == before['projects']
    assert record['after']['hidden_items'] == before['hidden_items']
    assert (source, before) == original


def test_authoritative_blank_zero_formula_and_decimal_values():
    targets, source, before, contract, defaults = example()
    hidden, child = source['hidden_items']['301'], source['subitems']['201']
    compare.col(hidden, compare.HIDDEN_ITEMS_COLUMNS['cust_order_value_material']).update(number=None, text='', value=None)
    compare.col(hidden, compare.HIDDEN_ITEMS_COLUMNS['cust_additional_charges'])['number'] = '0'
    compare.col(hidden, compare.HIDDEN_ITEMS_COLUMNS['quote_amount'])['number'] = '123.456'
    compare.col(child, compare.SUBITEM_COLUMNS['new_enquiry_value'])['display_value'] = ''
    row = missing.build_record(targets, source, before, contract, defaults)['inserts']['subitems'][0]
    assert row['cust_order_value_material'] is None
    assert row['cust_additional_charges'] == '0.00'
    assert row['quote_amount'] == '123.46'
    assert row['new_enquiry_value'] is None


@pytest.mark.parametrize('problem', ['archived', 'missing_parent', 'missing_source', 'link', 'membership',
                                     'moved', 'wrong_board', 'missing_column', 'unreadable_money'])
def test_incomplete_or_changed_source_cannot_stage(problem):
    args = list(example())
    source = args[1]
    child = source['subitems']['201']
    if problem == 'archived': child['state'] = 'archived'
    elif problem == 'missing_parent': source['projects'].clear()
    elif problem == 'missing_source': source['hidden_items'].clear()
    elif problem == 'link': compare.col(child, compare.SUBITEM_COLUMNS['hidden_item_id'])['linked_item_ids'] = ['999']
    elif problem == 'membership': source['projects']['101']['subitems'] = []
    elif problem == 'moved': child['parent_item']['id'] = '999'
    elif problem == 'wrong_board': child['board']['id'] = '999'
    elif problem == 'missing_column': child['column_values'].pop()
    else: compare.col(source['hidden_items']['301'], compare.HIDDEN_ITEMS_COLUMNS['quote_amount'])['number'] = '#ERROR!'
    with pytest.raises(ValueError): missing.build_record(*args)


@pytest.mark.parametrize('table', ['projects', 'hidden_items'])
def test_missing_prerequisites_are_not_inserted(table):
    args = list(example())
    args[2][table] = []
    with pytest.raises(ValueError, match='only inserts subitems'): missing.build_record(*args)


def test_existing_target_is_never_overwritten_and_matching_row_is_noop():
    args = list(example())
    record = missing.build_record(*args)
    args[2] = record['after']
    noop = missing.build_record(*args)
    assert noop['inserts']['subitems'] == [] and noop['already_present'] == ['201']
    args[2]['subitems'][0]['quote_amount'] = '999.00'
    with pytest.raises(ValueError, match='existing target differs'): missing.build_record(*args)


def test_shared_and_stale_stored_children_are_preserved_without_global_deduplication():
    args = list(example())
    args[2]['subitems'] = [{'monday_id': '299', 'parent_monday_id': '101', 'hidden_item_id': '301', 'quote_amount': '8.00'}]
    record = missing.build_record(*args)
    assert next(r for r in record['after']['subitems'] if r['monday_id'] == '299') == args[2]['subitems'][0]
    missing.validate_actual(record['after'], record)


def test_only_insert_defaults_may_differ_from_review():
    record = record_for()
    actual = deepcopy(record['after'])
    actual['subitems'][0]['id'] = 'database-default'
    missing.validate_actual(actual, record)
    actual['projects'][0]['total_order_value'] = '105.00'
    with pytest.raises(ValueError, match='values differ'): missing.validate_actual(actual, record)


def test_schema_generated_columns_cannot_be_in_insert_payload():
    args = list(example())
    args[3]['subitems']['quote_amount']['generated'] = 'ALWAYS'
    with pytest.raises(ValueError, match='generated'): missing.build_record(*args)


def test_size_and_target_validation(monkeypatch):
    with pytest.raises(ValueError): missing.validate_targets([{'project_id': 'Example'}])
    args = example()
    with pytest.raises(ValueError): missing.validate_targets(args[0] * 2)
    monkeypatch.setattr(scopes, 'MAX_SCOPE_ROWS', 2)
    with pytest.raises(ValueError, match='500-row'): missing.build_record(*args)


def saved_run(path):
    targets, source, before, contract, defaults = example()
    record = missing.build_record(targets, source, before, contract, defaults)
    staged = {'targets': targets, 'contract': contract, 'insert_contract': defaults, 'scopes': [record]}
    manifest = missing.save_run(path, staged, 'offline', {})
    return manifest, staged


@pytest.mark.parametrize('tamper', ['csv', 'existing_update', 'insert_value', 'coverage', 'summary'])
def test_review_integrity_and_resealed_plan_reconstruction(tmp_path, tamper):
    path = tmp_path / 'run'
    manifest, staged = saved_run(path)
    assert manifest['update_rows'] == 0
    assert missing.load_run(path) == (manifest, staged)
    record = staged['scopes'][0]
    if tamper == 'csv':
        with (path/'changes.csv').open('a') as stream: stream.write('tampered')
    else:
        if tamper == 'existing_update': record['after']['projects'][0]['total_order_value'] = '0.00'
        elif tamper == 'insert_value': record['inserts']['subitems'][0]['quote_amount'] = '999.00'
        elif tamper == 'summary': manifest['insert_rows']['subitems'] = 0
        else: staged['scopes'] = []
        manifest['sha256'] = backfill.fingerprint(staged)
        (path/'manifest.json').write_text(json.dumps(manifest))
        (path/'inserts.json').write_text(json.dumps(staged))
    with pytest.raises(ValueError): missing.load_run(path)


def test_capture_reads_only_target_ids_and_no_parent_financial_mirrors(monkeypatch):
    targets, source, *_ = example()
    calls = []
    def fetch(monday, ids, columns, **kwargs):
        table = 'projects' if kwargs.get('parents') else 'hidden_items' if kwargs.get('mirror_depth') == 0 else 'subitems'
        calls.append((table, ids, columns))
        return deepcopy(source[table])
    monkeypatch.setattr(compare, 'fetch_items', fetch)
    assert missing.capture(None, targets) == source
    assert [(table, ids) for table, ids, _ in calls] == [('projects', ['101']), ('subitems', ['201']), ('hidden_items', ['301'])]
    assert compare.PARENT_COLUMNS['total_order_value'] not in calls[0][2]


def test_default_targets_are_exactly_the_two_reviewed_children():
    targets = missing.validate_targets(json.loads(missing.DEFAULT_TARGETS.read_text()))
    assert targets == [
        {'project_id': '1772110252', 'subitem_id': '2828149014', 'hidden_item_id': '2727538504'},
        {'project_id': '2964337986', 'subitem_id': '3123480558', 'hidden_item_id': '3121757431'},
    ]
