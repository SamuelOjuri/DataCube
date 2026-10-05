"""Worker compatibility, historical audit and two-ID recovery in disposable SQL."""
import json
from uuid import uuid4

import psycopg
import pytest

from scripts import monday_review_cleanup as cleanup
from src.services import monday_lifecycle as life
from src.services import monday_lifecycle_refresh as refresh
from test_order_value_scopes_postgres import database
from test_monday_lifecycle_postgres import db, no_http
from test_monday_review_cleanup_postgres import Monday, reviewed


def scoped_root(c):
    key = f'recovery:{uuid4()}:{life.SUBITEM_BOARD_ID}:201'
    life.enqueue(c, 'delete', life.SUBITEM_BOARD_ID, '201', parent='101', key=key,
                 payload={'cleanup_policy': life.CLEANUP_POLICY, 'refresh_mode': 'new_enquiry_sum'})
    return key


def test_database_blocks_old_claim_but_updated_claim_works(db):
    c, _ = db
    key = scoped_root(c)
    with pytest.raises(psycopg.errors.RaiseException, match='updated lifecycle worker'):
        c.execute("UPDATE monday_lifecycle_events SET status='processing',lease_token=gen_random_uuid() WHERE event_key=%s", (key,))
    assert life.claim(c)['event_key'] == key
    assert c.execute("SELECT current_setting('datacube.lifecycle_worker_protocol',true) AS protocol").fetchone()['protocol'] in ('', None)


def test_descendants_inherit_scope_even_if_enqueue_omits_mode(db):
    c, _ = db
    root = scoped_root(c)
    key = life.enqueue(c, 'refresh', life.PARENT_BOARD_ID, '101', key=root + ':refresh:101')
    payload = c.execute('SELECT payload FROM monday_lifecycle_events WHERE event_key=%s', (key,)).fetchone()['payload']
    assert payload == {'cleanup_policy': life.CLEANUP_POLICY, 'refresh_mode': 'new_enquiry_sum'}
    verify = life.enqueue(c, 'refresh', life.PARENT_BOARD_ID, '101', key=key + ':verify_refresh')
    assert c.execute('SELECT payload FROM monday_lifecycle_events WHERE event_key=%s', (verify,)).fetchone()['payload'] == payload
    with pytest.raises(psycopg.errors.RaiseException, match='cannot be removed'):
        c.execute("UPDATE monday_lifecycle_events SET payload='{}' WHERE event_key=%s", (key,))


@pytest.mark.parametrize('fault', ['different_parent', 'restore', 'full_refresh', 'different_child'])
def test_descendant_cannot_escape_reviewed_scope(db, fault):
    c, _ = db
    root = scoped_root(c)
    kind, board, item, parent, payload = 'refresh', life.PARENT_BOARD_ID, '101', None, {}
    if fault == 'different_parent':
        item = '102'
    elif fault == 'restore':
        kind = 'restore'
    elif fault == 'full_refresh':
        payload['refresh_mode'] = 'full'
    else:
        kind, board, item, parent = 'reconcile', life.SUBITEM_BOARD_ID, '202', '101'
        payload['verification_only'] = True
    with pytest.raises(psycopg.errors.RaiseException):
        life.enqueue(c, kind, board, item, parent=parent, payload=payload, key=root + ':bad')


def test_queue_requires_database_guard_and_does_not_partially_enqueue(reviewed):
    c, monday, run_dir, manifest = reviewed
    c.execute('DROP TRIGGER guard_monday_cleanup_scope ON monday_lifecycle_events')
    with pytest.raises(ValueError, match='Install monday_lifecycle_scoped_cleanup'):
        cleanup.queue(c, monday, run_dir, manifest['run_id'])
    assert not c.execute('SELECT * FROM monday_lifecycle_events').fetchall()


def test_two_linked_deletions_preserve_current_ids_and_only_change_enquiry(db, tmp_path, monkeypatch):
    c, _ = db
    selected = cleanup.targets('linked2')
    monday = Monday(selected)
    before_parents, survivors = {}, {}
    for row in selected:
        pid, survivor = row['parent_id'], row['preserve_item_id']
        c.execute('INSERT INTO projects(monday_id,new_enquiry_value,project_name) VALUES (%s,999,%s)', (pid, row['project_name']))
        for item in (row['item_id'], survivor):
            c.execute('INSERT INTO subitems(monday_id,parent_monday_id,item_name) VALUES (%s,%s,%s)', (item, pid, row['subitem_name']))
        before_parents[pid] = life.read_rows(c, 'projects', 'monday_id', [pid])[0]
        survivors[survivor] = life.read_rows(c, 'subitems', 'monday_id', [survivor])[0]
    directory = tmp_path / 'linked2'
    manifest = cleanup.stage(c, monday, directory, 'linked2')
    assert manifest['ready'] and manifest['selected'] == manifest['projects'] == 2
    assert cleanup.queue(c, monday, directory, manifest['run_id'])['queued'] == 2
    monkeypatch.setattr(refresh, 'fetch_project', lambda *a: pytest.fail('Full refresh forbidden'))
    _, plan = cleanup.load_run(directory)
    for _ in range(20):
        if not life.run_once(c, monday, event_prefixes=cleanup.prefixes(plan)):
            break
    result = cleanup.status(c, directory)
    assert result['complete'], result
    assert result['deletions_verified'] == 2
    for row in selected:
        pid, survivor = row['parent_id'], row['preserve_item_id']
        assert not life.read_rows(c, 'subitems', 'monday_id', [row['item_id']])
        assert life.read_rows(c, 'subitems', 'monday_id', [survivor])[0] == survivors[survivor]
        after = life.read_rows(c, 'projects', 'monday_id', [pid])[0]
        assert after.pop('new_enquiry_value') != before_parents[pid].pop('new_enquiry_value')
        assert after == before_parents[pid]
    writes = c.execute("SELECT evidence FROM monday_lifecycle_audit WHERE action='refresh_field_changes'").fetchall()
    assert len(writes) == 2
    assert all(set(w['evidence']['changes']) == {'new_enquiry_value'} for w in writes)


def test_completed_jobs_with_broad_refresh_are_reported_for_review(reviewed, tmp_path):
    c, monday, directory, manifest = reviewed
    cleanup.queue(c, monday, directory, manifest['run_id'])
    _, plan = cleanup.load_run(directory)
    for _ in range(300):
        if not life.run_once(c, monday, event_prefixes=cleanup.prefixes(plan)):
            break
    assert cleanup.status(c, directory)['complete']
    event = c.execute("SELECT * FROM monday_lifecycle_events WHERE kind='refresh' LIMIT 1").fetchone()
    # Represent a historical successful result that did not prove the narrow mode.
    c.execute("UPDATE monday_lifecycle_events SET result=result-'refresh_mode'-'eligible' WHERE event_key=%s", (event['event_key'],))
    baseline = life.read_rows(c, 'projects', 'monday_id', [event['item_id']])[0]
    life.audit(c, event, 'refresh_or_restore', 'projects', event['item_id'], baseline, {})
    c.execute("UPDATE projects SET project_name='Later human edit' WHERE monday_id=%s", (event['item_id'],))
    result = cleanup.status(c, directory)
    assert result['jobs_complete'] and not result['complete'] and not result['scope_compliant']
    assert len(result['scope_issues']) == 2
    before = life.read_rows(c, 'projects', 'monday_id', [event['item_id']])
    report = cleanup.audit_run(c, directory, tmp_path / 'audit')
    assert report['read_only'] and report['scope_issue_count'] == 2
    details = json.loads((tmp_path / 'audit' / 'audit.json').read_text())
    historical = next(a for a in details['refresh_audits'] if a['action'] == 'refresh_or_restore')
    assert historical['recorded_write_changes'] is None
    assert historical['current_differences']['project_name']['current'] == 'Later human edit'
    assert life.read_rows(c, 'projects', 'monday_id', [event['item_id']]) == before


def test_scoped_verification_does_not_restore_a_returned_old_id(db):
    from test_monday_lifecycle_postgres import Monday as ItemsMonday, item
    c, _ = db
    root = scoped_root(c)
    c.execute("UPDATE monday_lifecycle_events SET status='processed' WHERE event_key=%s", (root,))
    key = life.enqueue(c, 'reconcile', life.SUBITEM_BOARD_ID, '201', parent='101',
        key=root + ':verify', payload={'verification_only': True})
    job = life.claim(c)
    assert job['event_key'] == key
    with pytest.raises(life.ReviewRequired, match='cannot restore'):
        life.process_job(c, ItemsMonday({'201': item('201', state='active')}), job)
    assert not c.execute('SELECT * FROM monday_lifecycle_audit').fetchall()
