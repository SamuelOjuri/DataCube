"""Bulk archive backfill on real PostgreSQL; Monday is an in-memory fake, no HTTP."""
from copy import deepcopy
import threading
from types import SimpleNamespace

import psycopg
import pytest

from scripts import monday_archive_bulk_backfill as bulk
from scripts import monday_lifecycle as cli
from src.services import monday_archive as archive
from src.services import monday_lifecycle as life
from src.services import monday_lifecycle_refresh as refresh
from test_monday_archive_postgres import archive_db, observe
from test_monday_lifecycle_postgres import db, Monday, item, full_source, no_http
from test_order_value_scopes_postgres import database

TABLES = ['projects', 'subitems', 'hidden_items', 'monday_item_lifecycle',
          'monday_lifecycle_events', 'monday_lifecycle_audit']


class ColumnMonday(Monday):
    """Returns only the requested columns, as the API does; counts requests."""

    def execute_query(self, query, variables):
        assert 'items_page' not in query and 'mirrored_value' not in query
        self.calls.append(list(variables['ids']))
        columns = set(variables.get('columns') or [])
        items = []
        for ident in variables['ids']:
            if ident in self.rows:
                row = deepcopy(self.rows[ident])
                if columns:
                    row['column_values'] = [c for c in row['column_values'] if c['id'] in columns]
                items.append(row)
        return {'data': {'items': items}}


def snapshot(c):
    return {t: c.execute(f'SELECT to_jsonb(t) AS row FROM {t} t ORDER BY to_jsonb(t)::text').fetchall() for t in TABLES}


@pytest.fixture
def setup(archive_db, tmp_path):
    c, dsn = archive_db
    source = full_source()
    rows = {i: r for table in life.BOARDS.values() for i, r in source[table].items()}
    return SimpleNamespace(c=c, dsn=dsn, monday=ColumnMonday(rows), out=tmp_path / 'run',
                           approved={'101': {'201': '301'}})


def go(s, **kwargs):
    return bulk.run(s.c, s.monday, s.out, approved=s.approved, **{'dry_run': False, **kwargs})


def ready(c, pid):
    return not [r for r in c.execute(bulk.PENDING_SQL, ([pid],)).fetchall()]


def test_bulk_capture_matches_the_full_refresh_capture_for_each_project(setup):
    sources, problems = bulk.capture_chunk(setup.monday, ['101'], 100)
    assert not problems
    assert sources['101'] == refresh.fetch_projects(setup.monday, ['101'])


def test_dry_run_reads_and_plans_without_writing(setup):
    before = snapshot(setup.c)
    result = go(setup, dry_run=True)
    assert snapshot(setup.c) == before
    assert result['phase'] == 'complete' and result['outcomes'] == {'would_verify': 1}
    assert result['pending_at_start'] == 1 and (setup.out / 'outcomes.csv').is_file()
    # Two reads of one project: parent, children and sources, once each.
    assert len(setup.monday.calls) == 6


def test_apply_verifies_the_project_like_the_worker_refresh(setup):
    result = go(setup)
    c = setup.c
    assert result['outcomes'] == {'verified': 1} and ready(c, '101')
    project = archive.state_row(c, 'projects', '101')
    assert project['monday_state'] == 'active' and project['state_evidence']['transaction_values_verified'] is True
    assert isinstance(project['state_evidence']['item']['subitems'], list)
    assert archive.state_row(c, 'subitems', '201')['state_evidence']['parent_monday_id'] == '101'
    assert archive.state_row(c, 'hidden_items', '301')['monday_state'] == 'active'
    assert life.read_rows(c, 'projects', 'monday_id', ['101'])[0]['total_order_value'] == 100
    actions = {r['action'] for r in c.execute("SELECT action FROM monday_lifecycle_audit WHERE event_key LIKE 'archive-bulk:%'")}
    assert {'observe_monday_state', 'refresh_field_changes', 'verify_current_parent_values'} <= actions
    again = go(setup)
    assert again['pending_at_start'] == 0 and again['processed'] == 0


def receipt(c, key, scope):
    c.execute("INSERT INTO monday_lifecycle_events (event_key,board_id,item_id,kind,payload,status,result,last_error) "
              "VALUES (%s,%s,'101','refresh',%s,'review',%s,'Fresh verification required; resume the backfill CLI')",
              (key, life.PARENT_BOARD_ID, psycopg.types.json.Jsonb(dict(
                  operator_policy='archive_coverage_backfill_v1', archive_policy=archive.POLICY, run_id='r',
                  campaign_sha256='c', scope_sha256=scope)),
               psycopg.types.json.Jsonb({'phase': 'applied_pending_verification'})))


def test_apply_closes_the_unfinished_campaign_receipt_for_exactly_this_project(setup):
    receipt(setup.c, 'archive-backfill:r:101', cli.digest({'101': {'201': '301'}}))
    receipt(setup.c, 'archive-backfill:other:101', cli.digest({'101': {}, '102': {}}))
    result = go(setup)
    rows = {r['event_key']: r for r in setup.c.execute(
        "SELECT event_key,status,result FROM monday_lifecycle_events WHERE event_key LIKE 'archive-backfill:%'")}
    assert rows['archive-backfill:r:101']['status'] == 'processed'
    assert rows['archive-backfill:r:101']['result']['phase'] == 'verified'
    assert rows['archive-backfill:other:101']['status'] == 'review'
    assert setup.c.execute("SELECT count(*) AS n FROM monday_lifecycle_audit WHERE action='close_backfill_receipt'"
                           ).fetchone()['n'] == 1
    assert (setup.out / 'outcomes.csv').read_text().count('archive-backfill:r:101') == 1
    assert result['outcomes'] == {'verified': 1}


def test_monday_change_between_the_two_reads_writes_nothing(setup, monkeypatch):
    original, reads = bulk.capture, []

    def capture(monday, pids, batch_size):
        if reads:
            setup.monday.rows['201']['updated_at'] = '2026-10-21T00:00:00Z'
        reads.append(pids)
        return original(monday, pids, batch_size)
    monkeypatch.setattr(bulk, 'capture', capture)
    before = snapshot(setup.c)
    result = go(setup)
    assert result['outcomes'] == {'retry': 1} and snapshot(setup.c) == before


def test_one_unreadable_project_does_not_stop_the_others(setup):
    setup.approved['999'] = {'299': '399'}
    result = go(setup)
    assert result['outcomes'] == {'verified': 1, 'review': 1}
    assert 'absence is not deletion evidence' in result['top_review_reasons'][0][0]
    assert ready(setup.c, '101') and not ready(setup.c, '999')


@pytest.mark.parametrize('change,reason', [
    ("UPDATE subitems SET hidden_item_id='301' WHERE monday_id='299'", 'Shares subitems or hidden sources'),
    ("UPDATE projects SET item_name='New project' WHERE monday_id='101'", 'excluded'),
    ("INSERT INTO monday_item_lifecycle (table_name,monday_id,blocked) VALUES ('hidden_items','301',true)",
     'restoration requires separate review'),
])
def test_cases_needing_judgment_are_reported_not_written(setup, change, reason):
    setup.c.execute(change)
    before = snapshot(setup.c)
    result = go(setup)
    assert result['outcomes'] == {'review': 1} and reason in result['top_review_reasons'][0][0]
    assert snapshot(setup.c) == before


def test_projects_with_a_recorded_archive_or_deletion_are_held(setup):
    observe(setup.c, 'projects', '101', state='archived')
    result = go(setup)
    assert result['held_lifecycle_projects'] == 1 and result['selected_this_run'] == 0


def test_projects_outside_the_approval_are_refused(setup):
    with pytest.raises(ValueError, match='Not in the reviewed approval'):
        go(setup, only_ids=['999'])


def test_lock_waits_are_retried_not_failed(setup):
    with psycopg.connect(setup.dsn) as holder:
        holder.execute('LOCK TABLE public.projects,public.hidden_items,public.subitems IN SHARE ROW EXCLUSIVE MODE')
        # The old runner failed a project after one 750 ms wait.
        release = threading.Timer(2.5, holder.rollback)
        release.start()
        try:
            result = go(setup)
        finally:
            release.join()
    assert result['outcomes'] == {'verified': 1}


def test_call_budget_stops_cleanly_and_a_rerun_resumes(setup):
    class Budget(ColumnMonday):
        def execute_query(self, query, variables):
            if len(self.calls) >= 4:
                raise bulk.CallBudgetReached('Monday call budget of 4 reached; rerun to continue')
            return super().execute_query(query, variables)
    rows, out = setup.monday.rows, setup.out
    setup.monday = Budget(rows)
    before = snapshot(setup.c)
    result = go(setup)
    assert result['phase'] == 'stopped' and 'budget' in result['stop_reason'] and result['processed'] == 0
    assert snapshot(setup.c) == before and (out / 'result.json').is_file()
    setup.monday = ColumnMonday(rows)
    assert go(setup)['outcomes'] == {'verified': 1}
