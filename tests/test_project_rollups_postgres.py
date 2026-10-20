"""Derived project dates on a real PostgreSQL 17 database with the archive guards installed."""
from datetime import date
import threading

import psycopg
import pytest

from scripts import backfill_project_derived_dates as backfill
from src.services import project_rollups as rollups
from test_monday_archive_postgres import archive_db, observe  # noqa: F401
from test_monday_lifecycle_postgres import db, no_http  # noqa: F401
from test_order_value_scopes_postgres import database  # noqa: F401

FIELDS = ', '.join(rollups.DERIVED_FIELDS)


@pytest.fixture
def rollup_db(archive_db):  # noqa: F811
    c, dsn = archive_db
    c.execute('ALTER TABLE projects ADD COLUMN first_date_designed date, ADD COLUMN last_date_designed date, '
              'ADD COLUMN first_date_invoiced date, ADD COLUMN last_date_invoiced date, '
              'ADD COLUMN gestation_period integer')
    c.execute('ALTER TABLE subitems ADD COLUMN date_design_completed date')
    return c, dsn


def project(c, pid, **stored):
    columns = ['monday_id', 'item_name', *stored]
    c.execute(f"INSERT INTO projects ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})",
              [pid, pid, *stored.values()])


def child(c, cid, pid, design=None, invoice=None, order=None):
    c.execute('INSERT INTO subitems (monday_id, parent_monday_id, date_design_completed, invoice_date, '
              'date_order_received) VALUES (%s, %s, %s, %s, %s)', (cid, pid, design, invoice, order))


def stored(c, pid):
    return c.execute(f'SELECT {FIELDS} FROM projects WHERE monday_id=%s', (pid,)).fetchone()


def test_dates_come_from_live_children_and_ignore_archived_or_deleted_ones(rollup_db):
    c, _ = rollup_db
    project(c, '7001')
    child(c, '7101', '7001', date(2026, 1, 10), date(2026, 3, 1), date(2026, 2, 1))
    child(c, '7102', '7001', date(2026, 1, 5), None, date(2026, 2, 20))
    child(c, '7103', '7001', date(2025, 12, 1), date(2025, 12, 2), date(2025, 12, 3))
    child(c, '7104', '7001', date(2025, 11, 1), date(2025, 11, 2), date(2025, 11, 3))
    observe(c, 'subitems', '7103', state='archived')
    c.execute("UPDATE monday_item_lifecycle SET blocked=true WHERE table_name='subitems' AND monday_id='7104'")

    assert rollups.apply(c, ['7001']) == ['7001']
    assert stored(c, '7001') == {
        'first_date_designed': date(2026, 1, 5), 'last_date_designed': date(2026, 1, 10),
        'first_date_invoiced': date(2026, 3, 1), 'last_date_invoiced': date(2026, 3, 1),
        'date_order_received': date(2026, 2, 1), 'gestation_period': 55}
    assert rollups.apply(c, ['7001']) == []


def test_missing_child_dates_never_erase_stored_values(rollup_db):
    c, _ = rollup_db
    kept = dict(first_date_designed=date(2025, 6, 1), last_date_designed=date(2025, 6, 9),
                first_date_invoiced=date(2025, 7, 1), last_date_invoiced=date(2025, 8, 1),
                date_order_received=date(2025, 6, 20), gestation_period=30)
    project(c, '7002', **kept)
    child(c, '7201', '7002')
    assert rollups.apply(c, ['7002']) == []
    assert stored(c, '7002') == kept


def test_gestation_is_zero_until_invoiced_and_untouched_without_a_design_date(rollup_db):
    c, _ = rollup_db
    project(c, '7003', gestation_period=106752)
    child(c, '7301', '7003', design=date(2026, 10, 5))
    project(c, '7004', gestation_period=0)
    child(c, '7401', '7004', order=date(2026, 9, 1))
    assert sorted(rollups.apply(c, ['7003', '7004'])) == ['7003', '7004']
    assert stored(c, '7003')['gestation_period'] == 0
    assert stored(c, '7004') == {**{f: None for f in rollups.DERIVED_FIELDS},
                                 'date_order_received': date(2026, 9, 1), 'gestation_period': 0}


def test_projects_without_children_or_archived_are_left_unchanged(rollup_db):
    c, _ = rollup_db
    project(c, '7005', first_date_designed=date(2024, 1, 1))
    project(c, '7006')
    child(c, '7601', '7006', design=date(2026, 1, 1))
    observe(c, 'projects', '7006', state='archived')
    assert rollups.apply(c, ['7005', '7006']) == []
    assert stored(c, '7005')['first_date_designed'] == date(2024, 1, 1)
    assert stored(c, '7006')['first_date_designed'] is None


def test_preview_matches_what_apply_writes(rollup_db):
    c, _ = rollup_db
    project(c, '7007', first_date_designed=date(2026, 2, 1))
    child(c, '7701', '7007', design=date(2026, 1, 1), invoice=date(2026, 1, 11))
    [row] = rollups.preview(c, ['7007'])
    assert (row['before_first_date_designed'], row['after_first_date_designed']) == (date(2026, 2, 1), date(2026, 1, 1))
    assert row['after_gestation_period'] == 10
    rollups.apply(c, ['7007'])
    assert stored(c, '7007') == {f: row[f'after_{f}'] for f in rollups.DERIVED_FIELDS}
    assert rollups.preview(c, ['7007']) == []


def test_update_waits_out_a_concurrent_gate_lock(rollup_db):
    c, dsn = rollup_db
    project(c, '7008')
    child(c, '7801', '7008', design=date(2026, 3, 1))
    with psycopg.connect(dsn) as holder:
        holder.execute('LOCK TABLE public.projects,public.hidden_items,public.subitems IN SHARE ROW EXCLUSIVE MODE')
        release = threading.Timer(2.5, holder.rollback)
        release.start()
        try:
            assert rollups.apply(c, ['7008']) == ['7008']
        finally:
            release.join()


def test_backfill_script_previews_reports_and_applies(rollup_db, monkeypatch, tmp_path, capsys):
    c, dsn = rollup_db
    monkeypatch.setenv('SUPABASE_DB_URL', dsn)
    project(c, '7009')
    child(c, '7901', '7009', design=date(2026, 4, 1), invoice=date(2026, 4, 15))
    report = tmp_path / 'changes.csv'

    assert backfill.main(['--report', str(report)]) == 0
    assert stored(c, '7009')['first_date_designed'] is None
    assert 'Read-only preview' in capsys.readouterr().out
    assert '7009,gestation_period,,14' in report.read_text()

    assert backfill.main(['--apply', '--yes', '--project-id', '7009']) == 0
    assert 'Still differing after the run: 0' in capsys.readouterr().out
    assert stored(c, '7009')['gestation_period'] == 14
    assert backfill.main(['--project-id', '7009']) == 0
    assert 'Projects that would change: 0' in capsys.readouterr().out
