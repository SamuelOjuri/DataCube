"""Offline checks for derived project dates; database behaviour is in test_project_rollups_postgres."""
from src.services import monday_archive as archive
from src.services import monday_lifecycle as life
from src.services import project_rollups as rollups
from scripts import order_value_monday_compare as compare
from test_monday_lock_retry import FakeConnection


def test_sql_writes_exactly_the_derived_fields_with_one_parameter():
    for field in rollups.DERIVED_FIELDS:
        assert f'{field} = d.{field}' in rollups.UPDATE_SQL
        assert f'before_{field}' in rollups.PREVIEW_SQL and f'after_{field}' in rollups.PREVIEW_SQL
    assert rollups.UPDATE_SQL.count('%(ids)s') == 1
    assert 'total_order_value' not in rollups.UPDATE_SQL


def test_batches_are_unique_sorted_and_bounded():
    assert list(rollups.batches(['3', '1', '3', None, ' ', '2'], size=2)) == [['1', '2'], ['3']]


def test_archive_gate_leaves_derived_fields_to_the_rollup_step(monkeypatch):
    stored = {'102': [{'monday_id': '102'}]}
    board = next(iter(life.BOARDS))
    source = {pid: {'state': 'active', 'board': {'id': board}, 'parent_item': None} for pid in ('101', '102')}
    monkeypatch.setattr(archive, 'enabled', lambda: True)
    monkeypatch.setattr(archive, 'require_runtime', lambda c: None)
    monkeypatch.setattr(life, 'connect', lambda: FakeConnection())
    monkeypatch.setattr(life, 'read_items', lambda monday, ids, **kw: {})
    monkeypatch.setattr(life, 'read_rows', lambda connection, table, field, ids: [r for i in ids for r in stored.get(i, [])])
    monkeypatch.setattr(compare, 'ComparisonMondayClient', lambda: object())
    monkeypatch.setattr(compare, 'fetch_items', lambda *args, **kw: source)
    blank_dates = {field: None for field in rollups.DERIVED_FIELDS}

    rows = archive.prepare_sync_rows('projects', [
        {'monday_id': '101', 'pipeline_stage': 'Open Enquiry', **blank_dates},
        {'monday_id': '102', 'pipeline_stage': 'Lost', **blank_dates}])

    assert [r['monday_id'] for r in rows] == ['101', '102']
    assert not any(field in row for row in rows for field in rollups.DERIVED_FIELDS)
    assert rows[1] == {'monday_id': '102', 'pipeline_stage': 'Lost'}
