"""Activity recovery transactions in disposable loopback PostgreSQL databases."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from types import SimpleNamespace

import psycopg
import pytest

from scripts import monday_lifecycle as cli
from src.services import monday_lifecycle as life
from src.services import monday_lifecycle_activity as activity
from src.services import monday_lifecycle_refresh as refresh
from test_order_value_scopes_postgres import database
from test_monday_lifecycle_postgres import db, full_source, item
from test_monday_lifecycle_activity import Monday, event, no_http, request


def staged(connection, tmp_path, monday=None):
    selection = request()
    args = SimpleNamespace(run_dir=tmp_path/'run', review_csv=None,
        board=life.SUBITEM_BOARD_ID, item_id=['201'], parent_id='101',
        activity_log_id=selection['log_id'], activity_log_from=selection['from'])
    monday = monday or Monday()
    manifest = cli.stage(connection, monday, args)
    args.confirm_run_id = manifest['run_id']
    return args, manifest, monday


def queued(connection, tmp_path):
    args, manifest, monday = staged(connection, tmp_path)
    assert manifest['selected'] == 1 and manifest['deferred'] == 0
    cli.queue_run(connection, args)
    job = life.claim(connection)
    return job, monday


def assert_not_deleted(connection):
    assert life.read_rows(connection, 'subitems', 'monday_id', ['201'])
    assert not connection.execute('SELECT * FROM monday_item_lifecycle WHERE blocked').fetchall()
    assert not connection.execute('SELECT * FROM monday_lifecycle_audit').fetchall()


def test_staging_is_read_only_and_deletion_is_single_row_audited_guarded_and_verified(db, tmp_path, monkeypatch):
    c, _ = db
    c.execute("INSERT INTO subitems(monday_id,parent_monday_id,item_name,hidden_item_id) VALUES ('202','101','Same name','301')")
    surviving = {table: life.read_rows(c, table, 'monday_id', ids) for table, ids in
                 [('projects', ['101']), ('hidden_items', ['301']), ('subitems', ['202', '299'])]}
    args, manifest, monday = staged(c, tmp_path)
    assert manifest['read_only'] and manifest['selected'] == 1 and manifest['deferred'] == 0
    assert_not_deleted(c)
    assert not c.execute('SELECT * FROM monday_lifecycle_events').fetchall()
    plan = json.loads((args.run_dir/'plan.json').read_text())
    assert plan['selected'][0]['activity_evidence']['deletion_event']['id'] == 'deletion-1'
    assert 'activity_log' in (args.run_dir/'review.csv').read_text(encoding='utf-8-sig')
    cli.queue_run(c, args)
    deletion = life.claim(c)
    life.process_job(c, monday, deletion)
    assert not life.read_rows(c, 'subitems', 'monday_id', ['201'])
    for table, ids in [('projects', ['101']), ('hidden_items', ['301']), ('subitems', ['202', '299'])]:
        assert life.read_rows(c, table, 'monday_id', ids) == surviving[table]
    audit = c.execute("SELECT * FROM monday_lifecycle_audit WHERE action='delete'").fetchone()
    assert audit['before_row']['monday_id'] == '201'
    assert audit['evidence']['basis'] == 'activity_log'
    assert audit['evidence']['activity']['deletion_event']['id'] == 'deletion-1'
    assert '201' not in audit['evidence']['activity']['after_items']
    marker = c.execute("SELECT * FROM monday_item_lifecycle WHERE monday_id='201'").fetchone()
    assert marker['blocked'] and marker['former_parent_id'] == '101'
    c.execute("INSERT INTO subitems(monday_id,parent_monday_id) VALUES ('201','101'),('203','101')")
    assert not life.read_rows(c, 'subitems', 'monday_id', ['201'])
    assert life.read_rows(c, 'subitems', 'monday_id', ['203'])

    # Exercise the durable parent refresh using only the surviving Monday child.
    source = json.loads(json.dumps(full_source()).replace('"201"', '"202"'))
    monkeypatch.setattr(refresh, 'fetch_project', lambda *a: deepcopy(source))
    refresh_job = life.claim(c)
    assert refresh_job['kind'] == 'refresh' and refresh_job['item_id'] == '101'
    life.process_job(c, None, refresh_job)
    assert life.read_rows(c, 'projects', 'monday_id', ['101'])[0]['total_order_value'] == 100
    verification = life.claim(c)
    assert verification['event_key'] == deletion['event_key'] + ':verify'
    life.process_job(c, monday, verification)
    result = c.execute('SELECT status,result FROM monday_lifecycle_events WHERE event_key=%s',
                       (verification['event_key'],)).fetchone()
    assert result['status'] == 'processed' and result['result']['deletion_verified'] == '201'
    assert c.execute("SELECT count(*) AS n FROM monday_lifecycle_audit WHERE action='verify_activity_deletion'").fetchone()['n'] == 1


@pytest.mark.parametrize('fault', ['missing_log', 'wrong_parent', 'current_active', 'later_restore'])
def test_invalid_recovery_is_deferred_without_database_changes(db, tmp_path, fault):
    c, _ = db
    monday = Monday()
    if fault == 'missing_log':
        monday.histories[life.SUBITEM_BOARD_ID] = []
    elif fault == 'wrong_parent':
        c.execute("UPDATE subitems SET parent_monday_id='999' WHERE monday_id='201'")
    elif fault == 'current_active':
        monday.rows['201'] = {**item('201', state='active'), 'subitems': []}
    else:
        monday.histories[life.SUBITEM_BOARD_ID].insert(0, event('restore', 'restore_pulse', seconds=1))
    args, manifest, _ = staged(c, tmp_path, monday)
    assert manifest['selected'] == 0 and manifest['deferred'] == 1
    assert_not_deleted(c)
    with pytest.raises(ValueError, match='No confirmed deletions'):
        cli.queue_run(c, args)


def test_normal_missing_item_never_automatically_uses_activity_history(db, tmp_path):
    c, _ = db
    args = SimpleNamespace(run_dir=tmp_path/'normal', review_csv=None,
                           board=life.SUBITEM_BOARD_ID, item_id=['201'])
    monday = Monday()
    manifest = cli.stage(c, monday, args)
    assert manifest['selected'] == 0 and manifest['deferred'] == 1
    assert not any(q == activity.ACTIVITY_QUERY for q, _ in monday.calls)
    assert_not_deleted(c)


def test_queue_rejects_tampered_activity_plan_and_stored_row_drift(db, tmp_path):
    c, _ = db
    args, _, _ = staged(c, tmp_path)
    original = (args.run_dir/'plan.json').read_text()
    plan = json.loads(original)
    plan['selected'][0]['activity_recovery']['parent_id'] = '999'
    (args.run_dir/'plan.json').write_text(json.dumps(plan))
    with pytest.raises(ValueError, match='artifacts differ'):
        cli.queue_run(c, args)
    (args.run_dir/'plan.json').write_text(original)
    c.execute("UPDATE subitems SET item_name='Changed' WHERE monday_id='201'")
    with pytest.raises(ValueError, match='changed since staging'):
        cli.queue_run(c, args)
    assert_not_deleted(c)


@pytest.mark.parametrize('fault', ['row_drift', 'later_restore', 'current_active', 'proof_changed'])
def test_worker_rechecks_source_and_reviewed_row_after_queueing(db, tmp_path, fault):
    c, _ = db
    deletion, monday = queued(c, tmp_path)
    if fault == 'row_drift':
        c.execute("UPDATE subitems SET item_name='Changed after queue' WHERE monday_id='201'")
    elif fault == 'later_restore':
        monday.histories[life.SUBITEM_BOARD_ID].insert(0, event('restore', 'restore_pulse', seconds=1))
    elif fault == 'current_active':
        monday.rows['201'] = {**item('201', state='active'), 'subitems': []}
    else:
        monday.histories[life.SUBITEM_BOARD_ID][0]['user_id'] = 'other'
    with pytest.raises(life.ReviewRequired):
        life.process_job(c, monday, deletion)
    assert_not_deleted(c)


def test_activity_recovery_rolls_back_markers_audit_and_followups_on_sql_failure(db, tmp_path):
    c, _ = db
    deletion, monday = queued(c, tmp_path)
    c.execute("CREATE FUNCTION fail_activity_delete() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test'; END $$")
    c.execute('CREATE TRIGGER fail_activity_delete BEFORE DELETE ON subitems FOR EACH ROW EXECUTE FUNCTION fail_activity_delete()')
    with pytest.raises(psycopg.Error):
        life.process_job(c, monday, deletion)
    assert_not_deleted(c)
    assert c.execute('SELECT count(*) AS n FROM monday_lifecycle_events').fetchone()['n'] == 1


def test_row_drift_after_network_read_is_caught_under_write_lock(db, tmp_path, monkeypatch):
    c, _ = db
    deletion, monday = queued(c, tmp_path)
    capture = activity.capture
    def drift(*args):
        proof = capture(*args)
        c.execute("UPDATE subitems SET parent_monday_id='999' WHERE monday_id='201'")
        return proof
    monkeypatch.setattr(activity, 'capture', drift)
    with pytest.raises(RuntimeError, match='dependencies changed'):
        life.process_job(c, monday, deletion)
    assert_not_deleted(c)


def test_stale_proof_cannot_be_applied(db, tmp_path, monkeypatch):
    c, _ = db
    deletion, monday = queued(c, tmp_path)
    capture = activity.capture
    def stale(*args):
        proof = capture(*args)
        proof['history_to'] = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
        return proof
    monkeypatch.setattr(activity, 'capture', stale)
    with pytest.raises(life.ReviewRequired, match='proof is stale'):
        life.process_job(c, monday, deletion)
    assert_not_deleted(c)


def test_periodic_recheck_retains_activity_evidence_and_can_restore_current_item(db, tmp_path, monkeypatch):
    c, _ = db
    deletion, monday = queued(c, tmp_path)
    life.process_job(c, monday, deletion)
    c.execute("UPDATE monday_lifecycle_events SET next_attempt_at=now()+interval '1 day' WHERE status='pending'")
    c.execute("UPDATE monday_item_lifecycle SET recheck_after=now()-interval '1 day' WHERE blocked")
    assert life.schedule_rechecks(c) == 1
    check = life.claim(c)
    assert check['payload']['activity_recovery'] == deletion['payload']['activity_recovery']
    life.process_job(c, monday, check)
    assert life.schedule_rechecks(c) == 0
    assert not life.read_rows(c, 'subitems', 'monday_id', ['201'])

    # A later successful exact-ID read takes the normal restoration path.
    c.execute("UPDATE monday_item_lifecycle SET recheck_after=now()-interval '1 day' WHERE blocked")
    assert life.schedule_rechecks(c) == 1
    restored = life.claim(c)
    source = full_source()
    monkeypatch.setattr(refresh, 'fetch_project', lambda *a: deepcopy(source))
    monday.rows['201'] = {**item('201', state='active'), 'parent_item': {'id': '101'}}
    life.process_job(c, monday, restored)
    assert life.read_rows(c, 'subitems', 'monday_id', ['201'])
    assert not c.execute("SELECT blocked FROM monday_item_lifecycle WHERE monday_id='201'").fetchone()['blocked']


def test_worker_persists_review_status_when_fresh_activity_is_unavailable(db, tmp_path):
    c, _ = db
    args, _, monday = staged(c, tmp_path)
    cli.queue_run(c, args)
    monday.histories[life.SUBITEM_BOARD_ID] = []
    assert life.run_once(c, monday)
    status = c.execute('SELECT status,last_error FROM monday_lifecycle_events').fetchone()
    assert status['status'] == 'review' and 'Exact deletion event' in status['last_error']
    assert_not_deleted(c)
