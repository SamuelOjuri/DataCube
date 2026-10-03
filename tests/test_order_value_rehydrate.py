"""Offline rehydration boundaries and source-derived artifact validation."""
from copy import deepcopy
import json
import socket

import pytest
import requests

from scripts import backfill_order_values as backfill
from scripts import order_value_blocked_review as review
from scripts import order_value_rehydrate as hydrate
from scripts import order_value_scopes as scopes
from test_order_value_blocked_review import inputs


@pytest.fixture(autouse=True)
def no_live(monkeypatch):
    def forbidden(*a, **kw):
        pytest.fail('Offline tests must not contact services')
    monkeypatch.setattr(requests.sessions.Session, 'request', forbidden)
    monkeypatch.setattr(socket.socket, 'connect', forbidden)
    monkeypatch.setattr(hydrate.psycopg, 'connect', forbidden)


def example():
    context, docs = inputs()
    source, raw, issues = review.source_from_capture(context, docs)
    assert not issues
    docs['baseline']['subitems'] = []
    docs['baseline']['hidden_items'] = []
    return docs['baseline'], source, raw, docs['contract']


def test_rehydrates_sources_and_children_with_production_rollups():
    baseline, source, raw, contract = example()
    record = hydrate.make_record(baseline, source, raw, contract, ['101'])
    inserts, updates = hydrate.split_writes(record['before'], record['updates'])
    assert {t: len(r) for t, r in inserts.items()} == {'projects': 0, 'hidden_items': 1, 'subitems': 1}
    assert inserts['subitems'][0]['hidden_item_id'] == '301'
    assert inserts['subitems'][0]['parent_monday_id'] == '101'
    assert updates['projects'][0]['total_order_value'] == '105.00'
    assert updates['projects'][0]['new_enquiry_value'] == '0.00'
    assert baseline['subitems'] == []
    with pytest.raises(scopes.ScopeConflict, match='separate rehydration'):
        scopes.require_existing_scope(record['before'], record['scope'])


@pytest.mark.parametrize('problem', ['parent_missing', 'stale_child', 'moved_child', 'outside_sql_owner',
                                    'outside_monday_owner', 'formula_changed', 'link_changed', 'row_limit'])
def test_unsafe_insert_groups_remain_blocked(problem, monkeypatch):
    baseline, source, raw, contract = example()
    if problem == 'parent_missing': baseline['projects'] = []
    elif problem == 'stale_child':
        baseline['subitems'] = [{'monday_id': '299', 'parent_monday_id': '101', 'hidden_item_id': None}]
    elif problem == 'moved_child':
        baseline['subitems'] = [{'monday_id': '201', 'parent_monday_id': '999', 'hidden_item_id': None}]
    elif problem == 'outside_sql_owner':
        baseline['subitems'] = [{'monday_id': '299', 'parent_monday_id': '999', 'hidden_item_id': '301'}]
    elif problem == 'outside_monday_owner':
        source['subitems'].append({**source['subitems'][0], 'monday_id': '299', 'parent_monday_id': '999'})
    elif problem == 'formula_changed':
        next(c for c in raw['hidden_items']['301']['column_values'] if c['id'] == backfill.TOTAL_COLUMN)['display_value'] = '999'
    elif problem == 'link_changed':
        next(c for c in raw['subitems']['201']['column_values'] if c['id'] == backfill.SUBITEM_COLUMNS['hidden_item_id'])['linked_item_ids'] = ['999']
    elif problem == 'row_limit': monkeypatch.setattr(scopes, 'MAX_SCOPE_ROWS', 2)
    with pytest.raises(ValueError):
        hydrate.make_record(baseline, source, raw, contract, ['101'])


def test_defaults_allowed_but_business_fields_and_existing_rows_exact():
    record = hydrate.make_record(*example(), ['101'])
    actual = deepcopy(record['after'])
    actual['hidden_items'][0].update(id='database-generated-id', created_at='2026-10-02')
    hydrate.validate_actual(actual, record)
    actual['hidden_items'][0]['cust_additional_charges'] = '999.00'
    with pytest.raises(scopes.ScopeConflict, match='values'):
        hydrate.validate_actual(actual, record)
    actual = deepcopy(record['after'])
    actual['projects'][0]['item_name'] = 'unreviewed trigger effect'
    with pytest.raises(scopes.ScopeConflict, match='values'):
        hydrate.validate_actual(actual, record)


def test_insert_requires_mandatory_fields_unless_database_has_default():
    inserts, _ = hydrate.split_writes(
        {t: [] for t in scopes.TABLES}, {t: [{'monday_id': '301'}] if t == 'hidden_items' else [] for t in scopes.TABLES})
    contract = [{'table_name': 'hidden_items', 'column_name': 'id', 'is_nullable': 'NO',
                 'column_default': None, 'is_identity': 'NO', 'is_generated': 'NEVER'}]
    with pytest.raises(scopes.ScopeConflict, match='required field'):
        hydrate.validate_insert_fields(inserts, contract)
    contract[0]['column_default'] = 'gen_random_uuid()'
    hydrate.validate_insert_fields(inserts, contract)


def test_review_reload_and_tamper_detection(tmp_path):
    baseline, source, raw, contract = example()
    record = hydrate.make_record(baseline, source, raw, contract, ['101'])
    staged = {'mode': 'repair', 'contract': contract, 'insert_contract': [],
              'selected_project_ids': ['101'], 'scopes': [record], 'deferred': []}
    run = tmp_path / 'run'
    manifest = hydrate.save_run(run, staged, 'offline-target', {})
    assert manifest['insert_rows']['subitems'] == 1
    assert hydrate.load_run(run)[1] == staged
    with (run / 'changes.csv').open('a') as file:
        file.write('tamper')
    with pytest.raises(ValueError, match='artifacts changed'):
        hydrate.load_run(run)


def test_resealed_plan_cannot_inject_non_source_updates(tmp_path):
    baseline, source, raw, contract = example()
    record = hydrate.make_record(baseline, source, raw, contract, ['101'])
    staged = {'mode': 'repair', 'contract': contract, 'insert_contract': [],
              'selected_project_ids': ['101'], 'scopes': [record], 'deferred': []}
    run = tmp_path / 'run'
    manifest = hydrate.save_run(run, staged, 'offline-target', {})
    staged['scopes'][0]['updates']['projects'][0]['total_order_value'] = '999.00'
    manifest['sha256'] = backfill.fingerprint(staged)
    (run / 'scopes.json').write_text(json.dumps(staged))
    (run / 'manifest.json').write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match='differ'):
        hydrate.load_run(run)


@pytest.mark.parametrize('change', ['link', 'child_value', 'hidden_value', 'total'])
def test_production_transform_must_still_match_monday_evidence(monkeypatch, change):
    baseline, source, raw, contract = example()
    original = hydrate.reconcile.transform_exact_rows
    def incorrect_transform(*args):
        result = original(*args)
        if change == 'link': result['subitems'][0]['hidden_item_id'] = '999'
        elif change == 'child_value': result['subitems'][0]['cust_additional_charges'] = 999
        elif change == 'hidden_value': result['hidden_items'][0]['cust_additional_charges'] = 999
        else: result['projects'][0]['total_order_value'] = '999.00'
        return result
    monkeypatch.setattr(hydrate.reconcile, 'transform_exact_rows', incorrect_transform)
    with pytest.raises(scopes.ScopeConflict):
        hydrate.make_record(baseline, source, raw, contract, ['101'])
