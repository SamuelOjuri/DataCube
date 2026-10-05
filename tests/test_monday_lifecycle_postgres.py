"""Real PostgreSQL tests on randomly created loopback-only databases; no HTTP."""
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
import threading
import time

import psycopg
from psycopg.rows import dict_row
import pytest
import requests

from src.services import monday_lifecycle as life
from src.services import monday_lifecycle_refresh as refresh
from scripts import order_value_monday_compare as compare
from scripts import monday_lifecycle as cli
from test_order_value_scopes_postgres import database
from test_order_value_monday_compare_flat import flat_data, set_column


@pytest.fixture(autouse=True)
def no_http(monkeypatch):
    monkeypatch.setattr(requests.sessions.Session, 'request', lambda *a, **k: pytest.fail('No live HTTP in PostgreSQL tests'))


@pytest.fixture
def db(database):
    connection, dsn, _, _ = database
    connection.row_factory = dict_row
    connection.execute('ALTER TABLE projects ADD COLUMN project_name text, ADD COLUMN pipeline_stage text, '
        "ADD COLUMN status_category text GENERATED ALWAYS AS (CASE WHEN pipeline_stage='Won - Closed (Invoiced)' THEN 'Won' WHEN pipeline_stage='Lost' THEN 'Lost' ELSE 'Open' END) STORED")
    connection.execute('ALTER TABLE hidden_items ADD COLUMN item_name text, ADD COLUMN quote_amount numeric(12,2), ADD COLUMN status text')
    connection.execute('ALTER TABLE subitems ADD COLUMN item_name text, ADD COLUMN quote_amount numeric(12,2), '
                       'ADD COLUMN order_status text, ADD COLUMN new_enquiry_value numeric(12,2)')
    connection.execute(Path('src/database/schema/monday_lifecycle.sql').read_text())
    connection.execute(Path('src/database/schema/monday_lifecycle_scoped_cleanup.sql').read_text())
    return connection, dsn


def item(item_id, table='subitems', state='deleted'):
    return dict(id=item_id, name='Same name', state=state, parent_item=None,
                board={'id': next(b for b, t in life.BOARDS.items() if t == table)},
                updated_at='2026-10-05T00:00:00Z', column_values=[])


class Monday:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def execute_query(self, query, variables):
        assert 'items_page' not in query and 'mirrored_value' not in query
        self.calls.append(variables['ids'])
        return {'data': {'items': [deepcopy(self.rows[i]) for i in variables['ids'] if i in self.rows]}}


def job(connection, board=None, item_id='201', kind='delete'):
    key = life.enqueue(connection, kind, board or life.SUBITEM_BOARD_ID, item_id)
    claimed = life.claim(connection)
    assert claimed['event_key'] == key
    return claimed


def test_subitem_delete_is_atomic_audited_and_keeps_same_name_active_row(db):
    c, _ = db
    c.execute("INSERT INTO subitems(monday_id,parent_monday_id,item_name) VALUES ('202','101','Same name')")
    j = job(c)
    life.process_job(c, Monday({'201': item('201')}), j)
    assert not life.read_rows(c, 'subitems', 'monday_id', ['201'])
    assert life.read_rows(c, 'subitems', 'monday_id', ['202'])
    assert life.read_rows(c, 'projects', 'monday_id', ['101'])
    assert life.read_rows(c, 'hidden_items', 'monday_id', ['301'])
    assert c.execute("SELECT count(*) AS n FROM monday_lifecycle_audit WHERE action='delete'").fetchone()['n'] == 1
    assert c.execute("SELECT count(*) AS n FROM monday_lifecycle_events WHERE kind='refresh'").fetchone()['n'] == 1
    assert c.execute("SELECT status FROM monday_lifecycle_events WHERE event_key=%s", (j['event_key'],)).fetchone()['status'] == 'processed'
    # A stale importer and its valid neighbour share a batch: only deleted ID is skipped.
    c.execute("INSERT INTO subitems(monday_id,parent_monday_id) VALUES ('201','101'),('203','101')")
    assert not life.read_rows(c, 'subitems', 'monday_id', ['201'])
    assert life.read_rows(c, 'subitems', 'monday_id', ['203'])


@pytest.mark.parametrize('state', ['active','archived','missing'])
def test_current_state_protects_against_stale_or_unproven_delete(db, state):
    c, _ = db
    j = job(c)
    monday = Monday({} if state == 'missing' else {'201': item('201', state=state)})
    if state == 'active':
        life.process_job(c, monday, j)
    else:
        with pytest.raises(life.ReviewRequired):
            life.process_job(c, monday, j)
    assert life.read_rows(c, 'subitems', 'monday_id', ['201'])
    assert c.execute('SELECT count(*) AS n FROM monday_lifecycle_audit').fetchone()['n'] == 0


def test_parent_delete_cascades_only_confirmed_deleted_children(db):
    c, _ = db
    j = job(c, life.PARENT_BOARD_ID, '101')
    life.process_job(c, Monday({'101': item('101', 'projects'), '201': item('201')}), j)
    assert not life.read_rows(c, 'projects', 'monday_id', ['101'])
    assert not life.read_rows(c, 'subitems', 'monday_id', ['201'])
    assert life.read_rows(c, 'hidden_items', 'monday_id', ['301'])
    c.execute("INSERT INTO projects(monday_id) VALUES ('101')")
    assert not life.read_rows(c, 'projects', 'monday_id', ['101'])
    c.execute("INSERT INTO subitems(monday_id,parent_monday_id) VALUES ('205','101')")
    assert not life.read_rows(c, 'subitems', 'monday_id', ['205'])


def test_parent_delete_refuses_moved_or_unavailable_child(db):
    c, _ = db
    j = job(c, life.PARENT_BOARD_ID, '101')
    with pytest.raises(life.ReviewRequired):
        life.process_job(c, Monday({'101': item('101', 'projects'), '201': item('201', state='active')}), j)
    assert life.read_rows(c, 'projects', 'monday_id', ['101'])


def test_hidden_deletion_unlinks_all_exact_owners_without_deleting_children(db):
    c, _ = db
    c.execute("UPDATE subitems SET hidden_item_id='301' WHERE monday_id='299'")
    j = job(c, life.HIDDEN_ITEMS_BOARD_ID, '301')
    life.process_job(c, Monday({'301': item('301', 'hidden_items')}), j)
    assert not life.read_rows(c, 'hidden_items', 'monday_id', ['301'])
    assert all(r['hidden_item_id'] is None for r in life.read_rows(c, 'subitems', 'monday_id', ['201','299']))
    queued = c.execute("SELECT item_id FROM monday_lifecycle_events WHERE kind='refresh'").fetchall()
    assert {r['item_id'] for r in queued} == {'101','999'}
    c.execute("UPDATE subitems SET hidden_item_id='301' WHERE monday_id='201'")
    assert life.read_rows(c, 'subitems', 'monday_id', ['201'])[0]['hidden_item_id'] is None


def test_failure_rolls_back_delete_marker_audit_and_followup_jobs(db):
    c, _ = db
    c.execute("CREATE FUNCTION fail_delete() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test'; END $$")
    c.execute('CREATE TRIGGER fail_delete BEFORE DELETE ON subitems FOR EACH ROW EXECUTE FUNCTION fail_delete()')
    j = job(c)
    with pytest.raises(psycopg.Error):
        life.process_job(c, Monday({'201': item('201')}), j)
    assert life.read_rows(c, 'subitems', 'monday_id', ['201'])
    assert c.execute('SELECT count(*) AS n FROM monday_item_lifecycle WHERE blocked').fetchone()['n'] == 0
    assert c.execute('SELECT count(*) AS n FROM monday_lifecycle_audit').fetchone()['n'] == 0
    assert c.execute('SELECT count(*) AS n FROM monday_lifecycle_events').fetchone()['n'] == 1


def test_expired_lease_is_reclaimed_and_fences_old_worker(db):
    c, _ = db
    old = job(c)
    c.execute("UPDATE monday_lifecycle_events SET lease_until=now()-interval '1 second'")
    new = life.claim(c)
    assert new['event_key'] == old['event_key'] and new['lease_token'] != old['lease_token']
    with pytest.raises(RuntimeError, match='lease'):
        life.process_job(c, Monday({'201': item('201')}), old)
    life.process_job(c, Monday({'201': item('201')}), new)
    assert not life.read_rows(c, 'subitems', 'monday_id', ['201'])


def test_worker_records_failure_without_losing_job(db):
    c, _ = db
    life.enqueue(c, 'delete', life.SUBITEM_BOARD_ID, '201')
    assert life.run_once(c, Monday({}))
    row = c.execute('SELECT status,last_error FROM monday_lifecycle_events').fetchone()
    assert row['status'] == 'review' and 'absence' in row['last_error']
    assert life.read_rows(c, 'subitems', 'monday_id', ['201'])


def test_concurrent_stale_insert_waits_then_respects_tombstone(db):
    c, dsn = db
    j = job(c)
    errors = []
    def writer():
        try:
            with psycopg.connect(dsn, autocommit=True) as other:
                other.execute("INSERT INTO subitems(monday_id,parent_monday_id) VALUES ('201','101') ON CONFLICT(monday_id) DO UPDATE SET parent_monday_id=EXCLUDED.parent_monday_id")
        except Exception as exc:
            errors.append(exc)
    with life.write_transaction(c, j):
        life.marker(c, 'subitems', '201', True, j)
        c.execute("DELETE FROM subitems WHERE monday_id='201'")
        thread = threading.Thread(target=writer)
        thread.start()
        time.sleep(.15)
        assert thread.is_alive()
    thread.join(4)
    assert not thread.is_alive() and not errors
    assert not life.read_rows(c, 'subitems', 'monday_id', ['201'])


def full_source():
    source, _, _, _ = flat_data()
    parent = source['projects']['101']
    present = {c['id'] for c in parent['column_values']}
    for cid in set(compare.PARENT_COLUMNS.values()) - present - {'name'}:
        parent['column_values'].append({'id': cid, 'type': 'text', 'text': '', 'value': None, '__typename': 'TextValue'})
    return source


def test_refresh_uses_current_monday_parent_mirror_and_preserves_generated_semantics(db, monkeypatch):
    c, _ = db
    source = full_source()
    monkeypatch.setattr(refresh, 'fetch_project', lambda *a: deepcopy(source))
    j = job(c, life.PARENT_BOARD_ID, '101', 'refresh')
    refresh.refresh_project(c, None, j)
    parent = life.read_rows(c, 'projects', 'monday_id', ['101'])[0]
    # Parent explicitly mirrors material 100, not material+additional (105).
    assert parent['total_order_value'] == 100
    assert parent['status_category'] == ('Won' if parent['pipeline_stage']=='Won - Closed (Invoiced)' else 'Open')
    assert life.read_rows(c, 'subitems', 'monday_id', ['201'])[0]['cust_additional_charges'] == 5


def test_confirmed_restore_rehydrates_and_clears_marker_atomically(db, monkeypatch):
    c, _ = db
    j = job(c)
    life.process_job(c, Monday({'201': item('201')}), j)
    # Claim only the newly requested restore; other durable follow-ups remain saved.
    c.execute("UPDATE monday_lifecycle_events SET next_attempt_at=now()+interval '1 day' WHERE status='pending'")
    source = full_source()
    monkeypatch.setattr(refresh, 'fetch_project', lambda *a: deepcopy(source))
    restore = job(c, life.SUBITEM_BOARD_ID, '201', 'restore')
    active = item('201', state='active')
    active['parent_item'] = {'id': '101'}
    life.process_job(c, Monday({'201': active}), restore)
    assert life.read_rows(c, 'subitems', 'monday_id', ['201'])
    assert not c.execute("SELECT blocked FROM monday_item_lifecycle WHERE table_name='subitems' AND monday_id='201'").fetchone()['blocked']


def test_source_drift_prevents_refresh_write(db, monkeypatch):
    c, _ = db
    source = full_source()
    changed = deepcopy(source)
    changed['projects']['101']['name'] = 'Finance edit'
    sources = iter([source, changed])
    monkeypatch.setattr(refresh, 'fetch_project', lambda *a: next(sources))
    j = job(c, life.PARENT_BOARD_ID, '101', 'refresh')
    before = life.read_rows(c, 'projects', 'monday_id', ['101'])
    with pytest.raises(ValueError, match='Monday changed'):
        refresh.refresh_project(c, None, j)
    assert life.read_rows(c, 'projects', 'monday_id', ['101']) == before


def test_unchanged_refresh_verifies_without_an_endless_write_chain(db, monkeypatch):
    c, _ = db
    source = full_source()
    monkeypatch.setattr(refresh, 'fetch_project', lambda *a: deepcopy(source))
    j = job(c, life.PARENT_BOARD_ID, '101', 'refresh')
    refresh.refresh_project(c, None, j)
    verification = life.claim(c)
    refresh.refresh_project(c, None, verification)
    result = c.execute('SELECT result FROM monday_lifecycle_events WHERE event_key=%s',
                       (verification['event_key'],)).fetchone()['result']
    assert result['rows_written'] == 0 and result['verification_queued'] is False
    assert life.claim(c) is None


def test_parent_restoration_rehydrates_current_children_without_restoring_old_values(db, monkeypatch):
    c, _ = db
    deletion = job(c, life.PARENT_BOARD_ID, '101')
    life.process_job(c, Monday({'101': item('101','projects'), '201': item('201')}), deletion)
    c.execute("UPDATE monday_lifecycle_events SET next_attempt_at=now()+interval '1 day' WHERE status='pending'")
    source = full_source()
    monkeypatch.setattr(refresh, 'fetch_project', lambda *a: deepcopy(source))
    restore = job(c, life.PARENT_BOARD_ID, '101', 'restore')
    life.process_job(c, Monday({'101': item('101', 'projects', 'active')}), restore)
    assert life.read_rows(c, 'projects', 'monday_id', ['101'])[0]['total_order_value'] == 100
    assert life.read_rows(c, 'subitems', 'monday_id', ['201'])[0]['cust_additional_charges'] == 5
    assert c.execute('SELECT count(*) AS n FROM monday_item_lifecycle WHERE blocked').fetchone()['n'] == 0


def test_repeatable_read_writer_cannot_miss_a_new_tombstone(db):
    c, dsn = db
    with psycopg.connect(dsn) as old:
        old.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
        old.execute('SELECT count(*) FROM monday_item_lifecycle').fetchone()
        j = job(c)
        life.process_job(c, Monday({'201': item('201')}), j)
        with pytest.raises(psycopg.errors.SerializationFailure):
            old.execute("INSERT INTO subitems(monday_id,parent_monday_id) VALUES ('201','101')")
        old.rollback()
    assert not life.read_rows(c, 'subitems', 'monday_id', ['201'])


def test_known_tombstone_rechecks_are_bounded_and_scheduled_only_once_per_day(db):
    c, _ = db
    j = job(c)
    life.process_job(c, Monday({'201': item('201')}), j)
    assert life.schedule_rechecks(c) == 0
    c.execute("UPDATE monday_item_lifecycle SET recheck_after=now()-interval '1 day' WHERE blocked")
    assert life.schedule_rechecks(c) == 1
    assert life.schedule_rechecks(c) == 0


def test_duplicate_enqueue_keeps_processed_status(db):
    c, _ = db
    j = job(c)
    life.process_job(c, Monday({'201': item('201')}), j)
    life.enqueue(c, 'delete', life.SUBITEM_BOARD_ID, '201', key=j['event_key'])
    assert c.execute('SELECT status FROM monday_lifecycle_events WHERE event_key=%s',
                     (j['event_key'],)).fetchone()['status'] == 'processed'


def test_historical_stage_is_read_only_and_queue_rejects_tampered_evidence(db, tmp_path):
    c, _ = db
    args = SimpleNamespace(run_dir=tmp_path/'run', review_csv=None,
                           board=life.SUBITEM_BOARD_ID, item_id=['201'])
    before = life.deletion_snapshot(c, 'subitems', '201')
    manifest = cli.stage(c, Monday({'201': item('201')}), args)
    assert manifest['read_only'] and manifest['selected'] == 1
    assert life.deletion_snapshot(c, 'subitems', '201') == before
    assert c.execute('SELECT count(*) AS n FROM monday_lifecycle_events').fetchone()['n'] == 0
    args.confirm_run_id = manifest['run_id']
    assert cli.queue_run(c, args)['queued'] == 1
    assert life.deletion_snapshot(c, 'subitems', '201') == before  # queueing itself only records a job
    with (args.run_dir/'review.csv').open('a') as stream:
        stream.write('tampered')
    with pytest.raises(ValueError, match='artifacts'):
        cli.queue_run(c, args)


def test_blank_current_link_clears_old_financial_values(db, monkeypatch):
    c, _ = db
    source = full_source()
    child = source['subitems']['201']
    link = compare.col(child, compare.SUBITEM_COLUMNS['hidden_item_id'])
    link['linked_item_ids'] = []
    for column in child['column_values']:
        if column.get('__typename') == 'MirrorValue':
            column['mirrored_items'] = []
            column['display_value'] = ''
    source['hidden_items'] = {}
    monkeypatch.setattr(refresh, 'fetch_project', lambda *a: deepcopy(source))
    j = job(c, life.PARENT_BOARD_ID, '101', 'refresh')
    refresh.refresh_project(c, None, j)
    row = life.read_rows(c, 'subitems', 'monday_id', ['201'])[0]
    assert row['hidden_item_id'] is None
    assert row['cust_order_value_material'] is None and row['cust_additional_charges'] is None
    assert row['quote_amount'] is None and row['amount_invoiced'] is None


def test_shared_source_changes_queue_every_other_exact_owner(db, monkeypatch):
    c, _ = db
    c.execute("UPDATE subitems SET hidden_item_id='301' WHERE monday_id='299'")
    source = full_source()
    monkeypatch.setattr(refresh, 'fetch_project', lambda *a: deepcopy(source))
    j = job(c, life.PARENT_BOARD_ID, '101', 'refresh')
    refresh.refresh_project(c, None, j)
    assert c.execute("SELECT count(*) AS n FROM monday_lifecycle_events WHERE kind='refresh' AND item_id='999'").fetchone()['n'] == 1
    assert life.read_rows(c, 'subitems', 'monday_id', ['299'])[0]['parent_monday_id'] == '999'


def test_full_capture_uses_only_parent_children_and_explicit_source_ids():
    source = full_source()
    monday = Monday({i: row for table in source.values() if isinstance(table, dict)
                     for i, row in table.items() if isinstance(row, dict) and 'id' in row})
    captured = refresh.fetch_project(monday, '101')
    assert monday.calls == [['101'], ['201'], ['301']]
    assert captured['projects']['101']['state'] == 'active'


def test_baseline_drift_prevents_deletion_after_source_read(db):
    c, _ = db
    j = job(c)
    before = life.deletion_snapshot(c, 'subitems', '201')
    c.execute("UPDATE subitems SET parent_monday_id='999' WHERE monday_id='201'")
    with pytest.raises(RuntimeError, match='dependencies changed'):
        life.apply_deletion(c, j, before, {'201': item('201')})
    assert life.read_rows(c, 'subitems', 'monday_id', ['201'])[0]['parent_monday_id'] == '999'


def test_hidden_restoration_restores_current_values_and_refreshes_former_owner(db):
    c, _ = db
    deletion = job(c, life.HIDDEN_ITEMS_BOARD_ID, '301')
    life.process_job(c, Monday({'301': item('301', 'hidden_items')}), deletion)
    c.execute("UPDATE monday_lifecycle_events SET next_attempt_at=now()+interval '1 day' WHERE status='pending'")
    source = full_source()
    restore = job(c, life.HIDDEN_ITEMS_BOARD_ID, '301', 'restore')
    life.process_job(c, Monday(source['hidden_items']), restore)
    restored = life.read_rows(c, 'hidden_items', 'monday_id', ['301'])[0]
    assert restored['cust_order_value_material'] == 100 and restored['cust_additional_charges'] == 5
    assert not c.execute("SELECT blocked FROM monday_item_lifecycle WHERE table_name='hidden_items' AND monday_id='301'").fetchone()['blocked']
    assert life.read_rows(c, 'subitems', 'monday_id', ['201'])  # Owner remains, independently of source restoration.
    refresh_jobs = c.execute("SELECT item_id FROM monday_lifecycle_events WHERE event_key LIKE %s AND kind='refresh'",
                             (restore['event_key'] + ':%',)).fetchall()
    assert {r['item_id'] for r in refresh_jobs} == {'101'}
