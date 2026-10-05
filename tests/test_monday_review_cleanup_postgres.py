"""31-item cleanup, guarded queueing and scoped execution in disposable PostgreSQL."""
from copy import deepcopy
from datetime import datetime, timezone
import json

import pytest

from scripts import monday_review_cleanup as cleanup
from src.services import monday_lifecycle as life
from src.services import monday_lifecycle_activity as activity
from src.services import monday_lifecycle_refresh as refresh
from test_order_value_scopes_postgres import database
from test_monday_lifecycle_postgres import db, full_source, no_http


class Monday:
    def __init__(self, rows):
        self.sources, self.events = {}, {}
        self.extra_events = {}
        for index, pid in enumerate(sorted({r['parent_id'] for r in rows})):
            child, hidden = str(9000000000 + index), str(9100000000 + index)
            source = json.loads(json.dumps(full_source()).replace('"101"', '"' + pid + '"')
                .replace('"201"', '"' + child + '"').replace('"301"', '"' + hidden + '"'))
            source['projects'][pid]['subitems'] = [dict(id=child, state='active',
                board={'id': life.SUBITEM_BOARD_ID}, parent_item={'id': pid})]
            cleanup.compare.col(source['subitems'][child], cleanup.compare.SUBITEM_COLUMNS['new_enquiry_value'])[
                'display_value'] = str(100 + index) + '.25'
            self.sources[pid] = source
        for row in rows:
            timestamp = activity.utc_date(row['deleted_at_utc'])
            self.events[row['item_id']] = dict(id=row['log_id'], event='delete_pulse', entity='pulse',
                user_id='1', created_at=str(int(timestamp.timestamp() * 10_000_000)),
                data=json.dumps(dict(pulse_id=int(row['item_id']), parent_item_id=int(row['parent_id']),
                                     board_id=int(life.SUBITEM_BOARD_ID), parent_board_id=int(life.PARENT_BOARD_ID))))

    def execute_query(self, query, variables):
        if query == activity.ACTIVITY_QUERY:
            events = []
            if variables['board'] == life.SUBITEM_BOARD_ID:
                events = [e for i, e in self.events.items() if i in variables['items']]
                events += [e for i, e in self.extra_events.items() if i in variables['items']]
            events.sort(key=lambda e: int(e['created_at']), reverse=True)
            offset = (variables['page'] - 1) * variables['limit']
            return {'data': {'boards': [dict(id=variables['board'], state='active',
                activity_logs=deepcopy(events[offset:offset + variables['limit']]))]}}
        assert query.lstrip().startswith('query CompareMonday(')
        index = {i: r for source in self.sources.values() for table in cleanup.compare.scopes.TABLES
                 for i, r in source[table].items()}
        result = []
        for i in variables['ids']:
            if i in index:
                row = deepcopy(index[i])
                row['column_values'] = [v for v in row['column_values'] if v['id'] in variables['columns']]
                result.append(row)
        return {'data': {'items': result}}


@pytest.fixture
def reviewed(db, tmp_path):
    connection, _ = db
    rows = cleanup.targets()
    monday = Monday(rows)
    for pid, source in monday.sources.items():
        child = next(iter(source['subitems']))
        hidden = next(iter(source['hidden_items']))
        connection.execute('INSERT INTO projects(monday_id,new_enquiry_value) VALUES (%s,500)', (pid,))
        connection.execute('INSERT INTO hidden_items(monday_id) VALUES (%s)', (hidden,))
        connection.execute('INSERT INTO subitems(monday_id,parent_monday_id,hidden_item_id) VALUES (%s,%s,%s)',
                           (child, pid, hidden))
    for row in rows:
        connection.execute('INSERT INTO subitems(monday_id,parent_monday_id,item_name) VALUES (%s,%s,%s)',
                           (row['item_id'], row['parent_id'], row['subitem_name']))
    run_dir = tmp_path / 'review31'
    manifest = cleanup.stage(connection, monday, run_dir)
    assert manifest['ready'] and manifest['selected'] == 31 and manifest['enquiry_ready'] == 27
    assert not connection.execute('SELECT * FROM monday_lifecycle_events').fetchall()
    return connection, monday, run_dir, manifest


def test_stage_queue_and_scoped_worker_complete_exact_deletions_and_sum_refresh(reviewed, monkeypatch):
    c, monday, run_dir, manifest = reviewed
    unchanged = {pid: {
        'parent': life.read_rows(c, 'projects', 'monday_id', [pid])[0],
        'children': life.read_rows(c, 'subitems', 'monday_id', list(source['subitems'])),
        'hidden': life.read_rows(c, 'hidden_items', 'monday_id', list(source['hidden_items']))}
        for pid, source in monday.sources.items()}
    unrelated = life.enqueue(c, 'delete', life.SUBITEM_BOARD_ID, '299', key='unrelated-work')
    with pytest.raises(ValueError, match='confirm-run-id'):
        cleanup.queue(c, monday, run_dir, 'wrong')
    assert cleanup.queue(c, monday, run_dir, manifest['run_id'])['queued'] == 31
    assert cleanup.queue(c, monday, run_dir, manifest['run_id'])['already_queued'] == 31
    assert not cleanup.status(c, run_dir)['complete']
    monkeypatch.setattr(refresh, 'fetch_project', lambda client, pid: deepcopy(monday.sources[pid]))
    _, plan = cleanup.load_run(run_dir)
    for _ in range(300):
        if not life.run_once(c, monday, event_prefixes=cleanup.prefixes(plan)):
            break
    result = cleanup.status(c, run_dir)
    assert result['complete'], [(e['kind'],e['item_id'],e['status'],e['last_error']) for e in result['events'] if e['status'] != 'processed']
    assert result['deletions_processed'] == result['deletions_verified'] == 31
    assert result['parent_refreshes_completed'] == 27
    assert c.execute('SELECT status FROM monday_lifecycle_events WHERE event_key=%s', (unrelated,)).fetchone()['status'] == 'pending'
    assert life.read_rows(c, 'subitems', 'monday_id', ['299'])
    for pid, source in monday.sources.items():
        actual = life.read_rows(c, 'projects', 'monday_id', [pid])[0]['new_enquiry_value']
        assert actual == cleanup.compare.project_new_enquiry_total(source, source['projects'][pid])
        parent = life.read_rows(c, 'projects', 'monday_id', [pid])[0]
        assert {k:v for k,v in parent.items() if k != 'new_enquiry_value'} == {
            k:v for k,v in unchanged[pid]['parent'].items() if k != 'new_enquiry_value'}
        assert life.read_rows(c, 'subitems', 'monday_id', list(source['subitems'])) == unchanged[pid]['children']
        assert life.read_rows(c, 'hidden_items', 'monday_id', list(source['hidden_items'])) == unchanged[pid]['hidden']
    assert c.execute("SELECT count(*) AS n FROM monday_lifecycle_audit WHERE action='delete'").fetchone()['n'] == 31
    assert c.execute('SELECT count(*) AS n FROM monday_item_lifecycle WHERE blocked').fetchone()['n'] == 31


def test_changed_final_subitem_rolls_back_entire_queue(reviewed):
    c, monday, run_dir, manifest = reviewed
    row = cleanup.targets()[-1]
    c.execute('UPDATE subitems SET item_name=%s WHERE monday_id=%s', ('Changed since review', row['item_id']))
    with pytest.raises(ValueError, match='changed since staging'):
        cleanup.queue(c, monday, run_dir, manifest['run_id'])
    assert not c.execute('SELECT * FROM monday_lifecycle_events').fetchall()


def test_changed_monday_formula_prevents_any_queue_writes(reviewed):
    c, monday, run_dir, manifest = reviewed
    source = next(iter(monday.sources.values()))
    child = next(iter(source['subitems'].values()))
    cleanup.compare.col(child, cleanup.compare.SUBITEM_COLUMNS['new_enquiry_value'])['display_value'] = '999'
    with pytest.raises(ValueError, match='Enquiry source'):
        cleanup.queue(c, monday, run_dir, manifest['run_id'])
    assert not c.execute('SELECT * FROM monday_lifecycle_events').fetchall()


def test_changed_review_artifact_cannot_be_queued(reviewed):
    c, monday, run_dir, manifest = reviewed
    with (run_dir / 'review.csv').open('a', encoding='utf-8') as stream:
        stream.write('changed\n')
    with pytest.raises(ValueError, match='artifacts changed'):
        cleanup.queue(c, monday, run_dir, manifest['run_id'])
    assert not c.execute('SELECT * FROM monday_lifecycle_events').fetchall()


@pytest.mark.parametrize('stage', ['Won - Closed (Invoiced)', 'Lost'])
def test_nonopen_parent_preview_and_worker_preserve_existing_enquiry_value(reviewed, stage):
    c, monday, _, _ = reviewed
    pid, source = next(iter(monday.sources.items()))
    cleanup.compare.col(source['projects'][pid], cleanup.compare.PARENT_COLUMNS['pipeline_stage'])['label'] = stage
    c.execute('UPDATE projects SET pipeline_stage=%s WHERE monday_id=%s', (stage, pid))
    before = life.read_rows(c, 'projects', 'monday_id', [pid])[0]
    preview = cleanup.enquiry_preview(c, monday, pid)
    assert not preview['eligible'] and not preview['changed']
    assert preview['before'] == preview['after'] == 500
    assert preview['eligible_subitems'] == 0
    key = life.enqueue(c, 'refresh', life.PARENT_BOARD_ID, pid,
                       payload={'refresh_mode': 'new_enquiry_sum'})
    job = life.claim(c)
    assert job['event_key'] == key
    refresh.refresh_project(c, monday, job)
    assert life.read_rows(c, 'projects', 'monday_id', [pid])[0] == before
    result = c.execute('SELECT result FROM monday_lifecycle_events WHERE event_key=%s', (key,)).fetchone()['result']
    assert result['rows_written'] == 0 and not result['eligible']
    assert result['skipped_reason'] and not result['verification_queued']


def test_parent_category_change_prevents_queue_even_if_formula_is_unchanged(reviewed):
    c, monday, run_dir, manifest = reviewed
    pid, source = next(iter(monday.sources.items()))
    cleanup.compare.col(source['projects'][pid], cleanup.compare.PARENT_COLUMNS['pipeline_stage'])['label'] = 'Lost'
    c.execute("UPDATE projects SET pipeline_stage='Lost' WHERE monday_id=%s", (pid,))
    with pytest.raises(ValueError, match='Enquiry source'):
        cleanup.queue(c, monday, run_dir, manifest['run_id'])
    assert not c.execute('SELECT * FROM monday_lifecycle_events').fetchall()


def test_stale_stored_parent_category_defers_preview(reviewed):
    c, monday, _, _ = reviewed
    pid, source = next(iter(monday.sources.items()))
    cleanup.compare.col(source['projects'][pid], cleanup.compare.PARENT_COLUMNS['pipeline_stage'])['label'] = 'Lost'
    with pytest.raises(ValueError, match='status_category differs'):
        cleanup.enquiry_preview(c, monday, pid)


def test_parent_category_change_during_worker_read_prevents_write(reviewed, monkeypatch):
    c, monday, _, _ = reviewed
    pid = next(iter(monday.sources))
    source = cleanup.compare.capture_new_enquiry(monday, pid)
    changed = deepcopy(source)
    cleanup.compare.col(changed['projects'][pid], cleanup.compare.PARENT_COLUMNS['pipeline_stage'])['label'] = 'Lost'
    samples = iter([source, changed])
    monkeypatch.setattr(cleanup.compare, 'capture_new_enquiry', lambda *a: next(samples))
    before = life.read_rows(c, 'projects', 'monday_id', [pid])[0]
    life.enqueue(c, 'refresh', life.PARENT_BOARD_ID, pid, payload={'refresh_mode': 'new_enquiry_sum'})
    job = life.claim(c)
    with pytest.raises(ValueError, match='Monday changed'):
        refresh.refresh_project(c, monday, job)
    assert life.read_rows(c, 'projects', 'monday_id', [pid])[0] == before
