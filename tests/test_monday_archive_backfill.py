from copy import deepcopy
from datetime import timedelta
import json
from types import SimpleNamespace
from uuid import uuid4

import psycopg
from psycopg.types.json import Jsonb
import pytest

from scripts import monday_archive_backfill as batch
from scripts import monday_archive_pilot as pilot
from src.services import monday_archive as archive
from src.services import monday_lifecycle as life
from src.services import monday_lifecycle_refresh as refresh
from test_monday_archive_pilot import setup_pilot, rows
from test_monday_archive_postgres import archive_db
from test_monday_lifecycle_postgres import db, Monday, no_http
from test_order_value_scopes_postgres import database
from test_order_value_monday_compare_flat import set_column, number


def save_approval(p, monkeypatch):
    data = dict(version=1, approval_manifest_sha256=batch.SOURCE_MANIFEST_SHA256, projects=p.projects)
    p.approval.write_text(json.dumps(data))
    monkeypatch.setattr(batch, 'APPROVAL_SHA256', batch.cli.digest(data))


@pytest.fixture
def backfill(setup_pilot, monkeypatch, tmp_path):
    old = setup_pilot
    pilot.TARGETS.write_text(json.dumps(dict(version=1, projects=[
        dict(monday_id='999', subitems={'299': '399'})])))
    pilot_id = str(uuid4())
    old.c.execute('INSERT INTO monday_lifecycle_events(event_key,board_id,item_id,kind,payload,status,result) '
        "VALUES (%s,%s,'999','refresh',%s,'processed',%s)",
        (f'archive-pilot:{pilot_id}:999', life.PARENT_BOARD_ID,
         Jsonb(dict(operator_policy=pilot.POLICY, archive_policy=archive.POLICY, run_id=pilot_id, plan_sha256='test')),
         Jsonb({'phase': 'verified'})))
    approval = tmp_path / 'approved.json'
    monkeypatch.setattr(batch, 'TARGETS', approval)
    p = SimpleNamespace(c=old.c, dsn=old.dsn, source=old.source, pilot_id=pilot_id, approval=approval,
                        projects={'999': {'299': '399'}, '101': {'201': '301'}}, directory=tmp_path / 'campaign')
    save_approval(p, monkeypatch)
    return p


def reader(p):
    class Reader(Monday):
        def execute_query(self, query, variables):
            assert p.c.info.transaction_status == psycopg.pq.TransactionStatus.IDLE
            assert query.lstrip().startswith('query CompareMonday(')
            return super().execute_query(query, variables)
    return Reader({ident: row for table in life.BOARDS.values() for ident, row in p.source[table].items()})


def prepare(p):
    return batch.prepare(p.c, p.directory, p.pilot_id)


def run(p, manifest, **kwargs):
    return batch.execute(p.c, reader(p), p.directory, confirmation=manifest['run_id'], **kwargs)


def add_child(p, cid, hid, *, pid='101', new_source=True):
    child = json.loads(json.dumps(p.source['subitems']['201']).replace('"201"', f'"{cid}"')
                       .replace('"301"', f'"{hid}"').replace('"101"', f'"{pid}"'))
    p.source['subitems'][cid] = child
    parent = p.source['projects'][pid]
    if not any(m['id'] == cid for m in parent['subitems']):
        member = deepcopy(parent['subitems'][0])
        member.update(id=cid, parent_item={'id': pid})
        parent['subitems'].append(member)
    if new_source:
        p.source['hidden_items'][hid] = json.loads(json.dumps(p.source['hidden_items']['301'])
                                                  .replace('"301"', f'"{hid}"'))
        p.c.execute('INSERT INTO hidden_items(monday_id) VALUES (%s)', (hid,))
    p.c.execute('INSERT INTO subitems(monday_id,parent_monday_id,hidden_item_id) VALUES (%s,%s,%s)', (cid, pid, hid))
    p.projects.setdefault(pid, {})[cid] = hid


def add_project(p, monkeypatch, *, shared=False):
    parent = json.loads(json.dumps(p.source['projects']['101']).replace('"101"', '"102"').replace('"201"', '"202"'))
    p.source['projects']['102'] = parent
    p.c.execute("INSERT INTO projects(monday_id) VALUES ('102')")
    add_child(p, '202', '301' if shared else '302', pid='102', new_source=not shared)
    save_approval(p, monkeypatch)


def test_prepare_preview_and_resume_preserve_scope(backfill):
    p = backfill
    tables = [*life.BOARDS.values(), 'monday_item_lifecycle', 'monday_lifecycle_events', 'monday_lifecycle_audit']
    baseline = {t: rows(p.c, t) for t in tables}
    p.c.execute('SET default_transaction_read_only=on')
    manifest = prepare(p)
    assert manifest['expected_projects'] == 1 and manifest['excluded_pilot_projects'] == ['999']
    preview = batch.execute(p.c, reader(p), p.directory, preview=True)
    assert preview['read_only'] is True and preview['phase'] == 'previewed'
    assert {t: rows(p.c, t) for t in tables} == baseline
    p.c.execute('SET default_transaction_read_only=off')
    assert run(p, manifest)['complete']
    assert life.read_rows(p.c, 'projects', 'monday_id', ['999'])[0]['total_order_value'] == 50
    assert not archive.state_row(p.c, 'projects', '999')
    audits = rows(p.c, 'monday_lifecycle_audit')
    assert run(p, manifest)['batches_this_invocation'] == 0
    assert rows(p.c, 'monday_lifecycle_audit') == audits
    assert life.claim(p.c) is None


def test_deterministic_campaign_recovers_after_local_files_lost(backfill, tmp_path):
    p = backfill
    manifest = prepare(p)
    assert run(p, manifest)['complete']
    p.directory = tmp_path / 'recovered'
    assert prepare(p) == manifest
    assert run(p, manifest)['verified_projects'] == 1


@pytest.mark.parametrize('phase', ['review', 'missing', 'unverified', 'wrong_plan', 'wrong_identity'])
def test_completed_pilot_required(backfill, phase):
    p = backfill
    if phase == 'missing':
        p.c.execute('DELETE FROM monday_lifecycle_events')
    elif phase == 'unverified':
        p.c.execute("UPDATE monday_lifecycle_events SET result='{}'")
    elif phase == 'wrong_plan':
        p.c.execute("UPDATE monday_lifecycle_events SET payload=payload-'plan_sha256'")
    elif phase == 'wrong_identity':
        p.c.execute("UPDATE monday_lifecycle_events SET item_id='123'")
    else:
        p.c.execute("UPDATE monday_lifecycle_events SET status='review'")
    with pytest.raises(ValueError):
        prepare(p)


@pytest.mark.parametrize('field', ['code', 'target', 'expected_projects', 'run_id', 'approval_sha256'])
def test_tampered_campaign_fails_before_writes(backfill, field):
    p = backfill
    manifest = prepare(p)
    changed = {**manifest, field: 'changed'}
    pilot.write_json(p.directory / 'manifest.json', changed)
    with pytest.raises(ValueError):
        run(p, manifest)
    assert not rows(p.c, 'monday_item_lifecycle')


def test_wrong_or_missing_confirmation_is_not_authority(backfill):
    p = backfill
    prepare(p)
    for confirmation in (None, 'wrong'):
        with pytest.raises(ValueError):
            batch.execute(p.c, reader(p), p.directory, confirmation=confirmation)
    assert not rows(p.c, 'monday_item_lifecycle')


@pytest.mark.parametrize('table,ident', [('projects', '101'), ('subitems', '201'), ('hidden_items', '301')])
@pytest.mark.parametrize('state', ['archived', 'deleted', None, 'missing'])
def test_inactive_missing_sources_stop_without_guessing(backfill, table, ident, state):
    p = backfill
    manifest = prepare(p)
    if state == 'missing':
        del p.source[table][ident]
    else:
        p.source[table][ident]['state'] = state
    with pytest.raises(ValueError):
        run(p, manifest)
    assert not rows(p.c, 'monday_item_lifecycle')


@pytest.mark.parametrize('fault', ['outside_owner', 'link_change', 'new_member', 'free',
                                  'new_project', 'blocked', 'extra_source'])
def test_fresh_scope_drift_stops_before_mutation(backfill, fault):
    p = backfill
    manifest = prepare(p)
    if fault == 'outside_owner':
        p.c.execute("INSERT INTO subitems(monday_id,parent_monday_id,hidden_item_id) VALUES ('202','999','301')")
    elif fault == 'link_change':
        p.c.execute("UPDATE subitems SET hidden_item_id=NULL WHERE monday_id='201'")
    elif fault == 'new_member':
        p.source['projects']['101']['subitems'].append({'id': '202', 'parent_item': {'id': '101'}})
    elif fault in {'free', 'new_project'}:
        p.source['projects']['101']['name'] = 'free to use' if fault == 'free' else 'New project'
    elif fault == 'blocked':
        p.c.execute("INSERT INTO monday_item_lifecycle(table_name,monday_id,blocked) VALUES ('projects','101',true)")
    else:
        hidden_col = batch.compare.col(p.source['subitems']['201'], batch.compare.SUBITEM_COLUMNS['cust_order_value_material'])
        hidden_col['mirrored_items'][0]['linked_item']['id'] = '399'
    before = rows(p.c, 'monday_item_lifecycle')
    with pytest.raises(ValueError):
        run(p, manifest)
    assert rows(p.c, 'monday_item_lifecycle') == before


@pytest.mark.parametrize('label,enquiry', [('Open', 90), ('Archive', 90), ('Lost', 777), ('Won - Closed (Invoiced)', 777)])
@pytest.mark.parametrize('invoice', [None, 0, 25])
def test_bulk_financial_rules(backfill, label, enquiry, invoice):
    p = backfill
    p.c.execute("UPDATE projects SET new_enquiry_value=777,pipeline_stage=%s WHERE monday_id='101'", (label,))
    set_column(p.source['projects']['101'], batch.compare.PARENT_COLUMNS['pipeline_stage'],
               {'__typename': 'StatusValue', 'label': label})
    set_column(p.source['hidden_items']['301'], batch.compare.HIDDEN_ITEMS_COLUMNS['amount_invoiced'], number(invoice))
    assert run(p, prepare(p))['complete']
    actual = life.read_rows(p.c, 'projects', 'monday_id', ['101'])[0]
    assert actual['new_enquiry_value'] == enquiry
    assert actual['total_order_value'] == 100
    assert actual['total_amount_invoiced'] == invoice
    assert archive.state_row(p.c, 'hidden_items', '301')['monday_state'] == 'active'


def test_shared_source_contributions_are_not_deduplicated(backfill, monkeypatch):
    p = backfill
    add_child(p, '202', '301', new_source=False)
    save_approval(p, monkeypatch)
    assert run(p, prepare(p))['complete']
    actual = life.read_rows(p.c, 'projects', 'monday_id', ['101'])[0]
    assert actual['new_enquiry_value'] == 180
    job = p.c.execute("SELECT result FROM monday_lifecycle_events WHERE payload->>'operator_policy'=%s", (batch.POLICY,)).fetchone()
    assert job['result']['rows_written']['hidden_items'] == 1
    assert job['result']['rows_written']['subitems'] == 2


def test_shared_parent_group_stays_atomic(backfill, monkeypatch):
    p = backfill
    add_project(p, monkeypatch, shared=True)
    manifest = prepare(p)
    assert manifest['expected_groups'] == 1
    assert run(p, manifest, batch_size=1)['verified_projects'] == 2
    assert p.c.execute("SELECT count(*) AS n FROM monday_lifecycle_events WHERE payload->>'operator_policy'=%s",
                       (batch.POLICY,)).fetchone()['n'] == 1


def test_failure_rolls_back_and_resumes_prepared_receipt(backfill, monkeypatch):
    p = backfill
    manifest = prepare(p)
    before = {t: rows(p.c, t) for t in [*life.BOARDS.values(), 'monday_item_lifecycle']}
    original = batch.write_values
    def fail(connection, *args):
        original(connection, *args)
        connection.execute('SELECT 1/0')
    monkeypatch.setattr(batch, 'write_values', fail)
    with pytest.raises(psycopg.errors.DivisionByZero):
        run(p, manifest)
    assert {t: rows(p.c, t) for t in before} == before
    assert life.claim(p.c) is None
    assert not rows(p.c, 'monday_lifecycle_audit')
    monkeypatch.setattr(batch, 'write_values', original)
    assert run(p, manifest)['complete']


def test_interrupt_after_apply_resumes_verification_only(backfill, monkeypatch):
    p = backfill
    manifest = prepare(p)
    original = batch.verify_case
    def stop(*args):
        raise KeyboardInterrupt
    monkeypatch.setattr(batch, 'verify_case', stop)
    with pytest.raises(KeyboardInterrupt):
        run(p, manifest)
    assert archive.coverage(p.c)['unresolved_archive_jobs'] == 1
    assert archive.coverage(p.c)['unverified_current_values'] == 1
    monkeypatch.setattr(batch, 'verify_case', original)
    monkeypatch.setattr(batch, 'apply_case', lambda *args: pytest.fail('Must not repeat business writes'))
    assert run(p, manifest)['complete']


def test_batch_limit_resumes_without_replaying_completed_projects(backfill, monkeypatch):
    p = backfill
    add_project(p, monkeypatch)
    manifest = prepare(p)
    first = run(p, manifest, batch_size=1, max_batches=1)
    assert first['verified_projects'] == 1 and first['complete'] is False
    original = batch.apply_case
    def remaining(connection, manifest, case):
        assert set(case['group']) == {'102'}
        return original(connection, manifest, case)
    monkeypatch.setattr(batch, 'apply_case', remaining)
    assert run(p, manifest)['verified_projects'] == 2


def test_full_batch_preflight_precedes_any_write(backfill, monkeypatch):
    p = backfill
    add_project(p, monkeypatch)
    manifest = prepare(p)
    p.source['projects']['102']['state'] = 'archived'
    before = rows(p.c, 'monday_item_lifecycle')
    with pytest.raises(ValueError):
        run(p, manifest)
    assert rows(p.c, 'monday_item_lifecycle') == before
    assert not rows(p.c, 'monday_lifecycle_audit')


def test_post_apply_source_drift_leaves_review_not_verified(backfill, monkeypatch):
    p = backfill
    manifest = prepare(p)
    original = batch.apply_case
    def drift(*args):
        result = original(*args)
        p.source['projects']['101']['name'] = 'Changed source'
        return result
    monkeypatch.setattr(batch, 'apply_case', drift)
    with pytest.raises(ValueError, match='Post-write source'):
        run(p, manifest)
    assert archive.coverage(p.c)['unverified_current_values'] == 1
    assert archive.coverage(p.c)['unresolved_archive_jobs'] == 1
    assert json.loads((p.directory / 'result.json').read_text())['phase'] == 'stopped'


@pytest.mark.parametrize('fault', ['sql', 'state', 'schema', 'freshness'])
def test_last_moment_guards(backfill, fault):
    p = backfill
    manifest = prepare(p)
    group = {'101': p.projects['101']}
    case = batch.capture_cases(p.c, reader(p), [group])[0]
    if fault == 'sql':
        p.c.execute("UPDATE projects SET total_order_value=123 WHERE monday_id='101'")
    elif fault == 'state':
        p.c.execute("INSERT INTO monday_item_lifecycle(table_name,monday_id) VALUES ('projects','101')")
    elif fault == 'schema':
        p.c.execute('ALTER TABLE subitems DISABLE TRIGGER ALL')
    else:
        case['captured_at'] = (pilot.now() - timedelta(minutes=6)).isoformat()
    before = {t: rows(p.c, t) for t in [*life.BOARDS.values(), 'monday_item_lifecycle']}
    with pytest.raises(ValueError):
        batch.apply_case(p.c, manifest, case)
    assert {t: rows(p.c, t) for t in before} == before


def test_parallel_campaign_is_rejected(backfill):
    p = backfill
    manifest = prepare(p)
    with psycopg.connect(p.dsn, autocommit=True) as other:
        other.execute('SELECT pg_advisory_lock(hashtextextended(%s,0))', (batch.POLICY,))
        with pytest.raises(ValueError, match='Another archive backfill'):
            run(p, manifest)
    assert run(p, manifest)['complete']


def test_largest_approved_shape_keeps_existing_ten_second_budget(backfill, monkeypatch):
    p = backfill
    for index in range(138):
        add_child(p, str(10000 + index), str(20000 + index))
    save_approval(p, monkeypatch)
    assert sum(map(len, batch.boundary({'101': p.projects['101']}).values())) == 279
    original = batch.write_values
    def bounded(connection, *args):
        assert connection.execute("SHOW transaction_timeout").fetchone()['transaction_timeout'] == '10s'
        return original(connection, *args)
    monkeypatch.setattr(batch, 'write_values', bounded)
    assert run(p, prepare(p), batch_size=25)['complete']
    parent = life.read_rows(p.c, 'projects', 'monday_id', ['101'])[0]
    assert parent['new_enquiry_value'] == 139 * 90
    assert len(life.read_rows(p.c, 'subitems', 'parent_monday_id', ['101'])) == 139


@pytest.mark.parametrize('label,expected', [('Open', 0), ('Lost', 777)])
def test_approved_projects_without_children_are_processed(backfill, monkeypatch, label, expected):
    p = backfill
    p.c.execute("DELETE FROM subitems WHERE monday_id='201'")
    p.c.execute("UPDATE projects SET new_enquiry_value=777,pipeline_stage=%s WHERE monday_id='101'", (label,))
    parent = p.source['projects']['101']
    parent['subitems'] = []
    set_column(parent, batch.compare.PARENT_COLUMNS['pipeline_stage'], {'__typename': 'StatusValue', 'label': label})
    for field in ('total_order_value', 'new_enq_value_mirror'):
        column = batch.compare.col(parent, batch.compare.PARENT_COLUMNS[field])
        column['mirrored_items'] = []
        column['display_value'] = ''
    p.source['subitems'] = {}
    p.source['hidden_items'] = {}
    p.projects['101'] = {}
    save_approval(p, monkeypatch)
    hidden = life.read_rows(p.c, 'hidden_items', 'monday_id', ['301'])
    assert run(p, prepare(p))['complete']
    assert life.read_rows(p.c, 'projects', 'monday_id', ['101'])[0]['new_enquiry_value'] == expected
    assert life.read_rows(p.c, 'hidden_items', 'monday_id', ['301']) == hidden


def test_no_value_changes_still_establish_verified_coverage(backfill):
    p = backfill
    group = {'101': p.projects['101']}
    case = batch.capture_cases(p.c, reader(p), [group])[0]
    with life.locked_write_transaction(p.c):
        job = {'event_key': 'previous-normal-refresh'}
        archive.observe_source(p.c, job, case['source'], pilot.now())
        refresh.write_values(p.c, case['values'], case['before'], case['contract'])
    before = {t: rows(p.c, t) for t in life.BOARDS.values()}
    assert run(p, prepare(p))['complete']
    assert {t: rows(p.c, t) for t in before} == before
    job = p.c.execute("SELECT result FROM monday_lifecycle_events WHERE payload->>'operator_policy'=%s",
                      (batch.POLICY,)).fetchone()
    assert job['result']['rows_written'] == dict(projects=0, subitems=0, hidden_items=0)
    assert archive.state_row(p.c, 'projects', '101')['state_evidence']['transaction_values_verified'] is True


def test_checkpoint_ignores_columns_requested_only_by_batch_neighbours(backfill):
    p = backfill
    group = {'101': p.projects['101']}
    original = batch.narrow_source(p.source, group)
    p.source['hidden_items']['301']['column_values'].append(
        {'id': 'unrelated_column', '__typename': 'NumbersValue', 'number': 456})
    assert batch.narrow_source(p.source, group) == original


def test_pinned_full_partition_and_batches():
    projects = batch.load_approval()
    excluded = {r['monday_id'] for r in pilot.load_targets()['projects']}
    groups = batch.components({p: v for p, v in projects.items() if p not in excluded})
    packed = list(batch.batches(groups, 25))
    assert len(projects) == 15153 and len(groups) == 15143
    assert all(len(g) == 1 for g in groups)
    assert sum(len(g) for pack in packed for g in pack) == 15143
    assert all(sum(map(len, batch.boundary({p: c for g in pack for p, c in g.items()}).values())) <= 500
               for pack in packed)
    assert not any(excluded.intersection(g) for pack in packed for g in pack)
    assert not any(p in projects for p in ['1776390707', '3002770448', '5045321800',
                                         '2573553261', '2979954977', '3002837604',
                                         '3011456213', '3025866403', '3136993702', '3261026516'])
