from copy import deepcopy
from datetime import timedelta
import json
from types import SimpleNamespace

import psycopg
import pytest

from scripts import monday_archive_pilot as pilot
from scripts import monday_lifecycle as cli
from src.services import monday_archive as archive
from src.services import monday_lifecycle as life
from src.services import monday_lifecycle_refresh as refresh
from test_monday_archive_postgres import archive_db
from test_monday_lifecycle_postgres import db, Monday, full_source, no_http
from test_order_value_scopes_postgres import database
from test_order_value_monday_compare_flat import set_column, number


@pytest.fixture
def setup_pilot(archive_db, monkeypatch, tmp_path):
    c, dsn = archive_db
    source = full_source()
    fetch = refresh.fetch_project
    monkeypatch.setattr(refresh, 'fetch_project', lambda *a: deepcopy(source))
    targets = tmp_path / 'targets.json'
    targets.write_text(json.dumps(dict(version=1, approval_manifest_sha256='test',
        projects=[dict(monday_id='101', subitems={'201': '301'})])))
    monkeypatch.setattr(pilot, 'TARGETS', targets)
    return SimpleNamespace(c=c, dsn=dsn, source=source, directory=tmp_path / 'run', fetch=fetch)


def stage(p):
    return pilot.stage(p.c, None, p.directory)


def apply(p, manifest):
    return pilot.run(p.c, None, p.directory, manifest['run_id'], applying=True)


def rows(c, table):
    return c.execute(f'SELECT to_jsonb(t) AS row FROM {table} t ORDER BY to_jsonb(t)::text').fetchall()


def test_stage_is_read_only_and_exact(setup_pilot):
    p = setup_pilot
    tables = ['projects', 'subitems', 'hidden_items', 'monday_item_lifecycle',
              'monday_lifecycle_events', 'monday_lifecycle_audit']
    before = {t: rows(p.c, t) for t in tables}
    p.c.execute('SET default_transaction_read_only=on')
    manifest = stage(p)
    assert manifest['read_only'] is True
    assert (manifest['projects'], manifest['subitems'], manifest['hidden_sources']) == (1, 1, 1)
    assert {t: rows(p.c, t) for t in tables} == before
    assert (p.directory / 'review.csv').is_file()


@pytest.mark.parametrize('label,enquiry', [('Open', 90), ('Archive', 90),
                                          ('Lost', 777), ('Won - Closed (Invoiced)', 777)])
@pytest.mark.parametrize('invoice', [None, 0, 25])
def test_apply_and_rerun_verify_exact_financial_rules(setup_pilot, label, enquiry, invoice):
    p = setup_pilot
    p.c.execute("UPDATE projects SET pipeline_stage=%s,new_enquiry_value=777 WHERE monday_id='101'", (label,))
    set_column(p.source['projects']['101'], pilot.compare.PARENT_COLUMNS['pipeline_stage'],
               {'__typename': 'StatusValue', 'label': label})
    set_column(p.source['hidden_items']['301'], pilot.compare.HIDDEN_ITEMS_COLUMNS['amount_invoiced'], number(invoice))
    outside = life.read_rows(p.c, 'projects', 'monday_id', ['999'])
    manifest = stage(p)
    result = apply(p, manifest)
    assert result['complete'] and result['verified_projects'] == ['101']
    parent = life.read_rows(p.c, 'projects', 'monday_id', ['101'])[0]
    assert parent['new_enquiry_value'] == enquiry
    assert parent['total_order_value'] == 100
    assert parent['total_amount_invoiced'] == invoice
    assert life.read_rows(p.c, 'projects', 'monday_id', ['999']) == outside
    assert archive.coverage(p.c) == dict(unverified_projects=1, unverified_subitems=0,
        unverified_sources=0, unverified_current_values=0, unresolved_archive_jobs=0)
    assert len(rows(p.c, 'monday_item_lifecycle')) == 3
    assert archive.state_row(p.c, 'hidden_items', '301')['monday_state'] == 'active'
    assert life.read_rows(p.c, 'hidden_items', 'monday_id', ['301'])[0]['status'] == 'Archived'
    audits = rows(p.c, 'monday_lifecycle_audit')
    assert apply(p, manifest)['complete']
    assert pilot.run(p.c, None, p.directory, manifest['run_id'], applying=False)['complete']
    assert rows(p.c, 'monday_lifecycle_audit') == audits
    assert life.claim(p.c) is None


@pytest.mark.parametrize('table,ident', [('projects', '101'), ('subitems', '201'), ('hidden_items', '301')])
@pytest.mark.parametrize('state', ['archived', 'deleted', None, 'missing'])
def test_nonactive_or_missing_exact_source_aborts(setup_pilot, table, ident, state):
    p = setup_pilot
    if state == 'missing':
        del p.source[table][ident]
    else:
        p.source[table][ident]['state'] = state
    with pytest.raises(life.ReviewRequired):
        stage(p)
    assert not rows(p.c, 'monday_item_lifecycle')
    assert not rows(p.c, 'monday_lifecycle_events')


@pytest.mark.parametrize('fault', ['extra_child', 'moved_child', 'source_link', 'extra_source',
                                   'wrong_board', 'extra_member', 'stored_owner', 'stored_link',
                                   'missing_stored', 'restoration', 'reporting_exclusion'])
def test_scope_drift_aborts_without_writes(setup_pilot, fault):
    p = setup_pilot
    if fault == 'extra_child':
        p.source['subitems']['202'] = deepcopy(p.source['subitems']['201'])
    elif fault == 'moved_child':
        p.source['subitems']['201']['parent_item'] = {'id': '999'}
    elif fault == 'source_link':
        set_column(p.source['subitems']['201'], pilot.compare.SUBITEM_COLUMNS['hidden_item_id'],
                   {'__typename': 'BoardRelationValue', 'type': 'board_relation', 'linked_item_ids': ['302']})
    elif fault == 'extra_source':
        p.source['hidden_items']['302'] = deepcopy(p.source['hidden_items']['301'])
    elif fault == 'wrong_board':
        p.source['hidden_items']['301']['board']['id'] = life.SUBITEM_BOARD_ID
    elif fault == 'extra_member':
        p.source['projects']['101']['subitems'].append({'id': '202'})
    elif fault == 'stored_owner':
        p.c.execute("INSERT INTO subitems(monday_id,parent_monday_id,hidden_item_id) VALUES ('202','999','301')")
    elif fault == 'stored_link':
        p.c.execute("UPDATE subitems SET hidden_item_id=NULL WHERE monday_id='201'")
    elif fault == 'missing_stored':
        p.c.execute("DELETE FROM subitems WHERE monday_id='201'")
    elif fault == 'restoration':
        p.c.execute("INSERT INTO monday_item_lifecycle(table_name,monday_id,blocked) VALUES ('projects','101',true)")
    else:
        p.c.execute("CREATE OR REPLACE VIEW reportable_projects AS SELECT * FROM projects WHERE monday_id<>'101'")
    before = rows(p.c, 'monday_item_lifecycle')
    with pytest.raises((life.ReviewRequired, ValueError)):
        stage(p)
    assert rows(p.c, 'monday_item_lifecycle') == before
    assert not rows(p.c, 'monday_lifecycle_events')


@pytest.mark.parametrize('name', ['New project', 'FREE', 'free number', 'freeee', 'free to use'])
@pytest.mark.parametrize('location', ['monday', 'sql'])
def test_placeholder_free_holds(setup_pilot, name, location):
    p = setup_pilot
    if location == 'monday':
        p.source['projects']['101']['name'] = name
    else:
        p.c.execute("UPDATE projects SET project_name=%s WHERE monday_id='101'", (name,))
    with pytest.raises(life.ReviewRequired, match='excluded'):
        stage(p)


def test_genuine_free_name_is_allowed(setup_pilot):
    setup_pilot.source['projects']['101']['name'] = 'Royal Free Hospital'
    assert stage(setup_pilot)['projects'] == 1


@pytest.mark.parametrize('fault', ['source', 'sql', 'lifecycle', 'schema', 'code', 'plan',
                                   'review', 'confirmation', 'expiry', 'target', 'approval'])
def test_stale_or_unapproved_apply_rejected(setup_pilot, monkeypatch, fault):
    p = setup_pilot
    manifest = stage(p)
    if fault == 'source':
        p.source['projects']['101']['name'] = 'Changed'
    elif fault == 'sql':
        p.c.execute("UPDATE projects SET total_order_value=123 WHERE monday_id='101'")
    elif fault == 'lifecycle':
        p.c.execute("INSERT INTO monday_item_lifecycle(table_name,monday_id) VALUES ('projects','101')")
    elif fault == 'schema':
        p.c.execute('ALTER TABLE projects ADD COLUMN pilot_new_column text')
    elif fault == 'code':
        monkeypatch.setattr(pilot, 'code_digest', lambda: 'changed')
    elif fault == 'plan':
        plan = json.loads((p.directory / 'plan.json').read_text())
        plan['cases'][0]['values']['projects'][0]['total_order_value'] = '0'
        pilot.write_json(p.directory / 'plan.json', plan)
    elif fault == 'review':
        (p.directory / 'review.csv').write_text('changed')
    elif fault == 'confirmation':
        manifest['run_id'] = 'not-confirmed'
    elif fault == 'expiry':
        current = pilot.now()
        monkeypatch.setattr(pilot, 'now', lambda: current + timedelta(days=2))
    elif fault == 'target':
        monkeypatch.setattr(cli, 'target_digest', lambda c: 'wrong-db')
    else:
        data = pilot.load_targets()
        data['projects'][0]['subitems']['201'] = '302'
        pilot.TARGETS.write_text(json.dumps(data))
    with pytest.raises((ValueError, life.ReviewRequired)):
        apply(p, manifest)
    assert not rows(p.c, 'monday_lifecycle_events')


def test_mid_capture_drift_is_explicit(setup_pilot, monkeypatch):
    p = setup_pilot
    changed = deepcopy(p.source)
    changed['projects']['101']['name'] = 'Changed'
    source = iter([p.source, changed])
    monkeypatch.setattr(refresh, 'fetch_project', lambda *a: next(source))
    with pytest.raises(life.ReviewRequired, match='Monday changed'):
        stage(p)


def test_failure_rolls_back_and_cannot_be_claimed_by_old_workers(setup_pilot, monkeypatch):
    p = setup_pilot
    manifest = stage(p)
    baseline = {t: rows(p.c, t) for t in life.BOARDS.values()}
    original = refresh.write_values
    def fail(connection, *args, **kwargs):
        original(connection, *args, **kwargs)
        connection.execute('SELECT 1/0')
    monkeypatch.setattr(refresh, 'write_values', fail)
    with pytest.raises(psycopg.errors.DivisionByZero):
        apply(p, manifest)
    assert {t: rows(p.c, t) for t in life.BOARDS.values()} == baseline
    assert not rows(p.c, 'monday_item_lifecycle')
    assert not rows(p.c, 'monday_lifecycle_audit')
    assert rows(p.c, 'monday_lifecycle_events')[0]['row']['result']['phase'] == 'prepared'
    monkeypatch.setenv('MONDAY_ARCHIVE_ENABLED', 'false')
    assert life.claim(p.c) is None
    monkeypatch.setenv('MONDAY_ARCHIVE_ENABLED', 'true')
    monkeypatch.setattr(refresh, 'write_values', original)
    assert apply(p, manifest)['complete']


def test_interruption_after_commit_resumes_verification_without_replaying(setup_pilot, monkeypatch):
    p = setup_pilot
    manifest = stage(p)
    original = pilot.verify_case
    def stop(*args):
        raise RuntimeError('simulated interruption after apply commit')
    monkeypatch.setattr(pilot, 'verify_case', stop)
    with pytest.raises(RuntimeError, match='interruption'):
        apply(p, manifest)
    assert archive.coverage(p.c)['unverified_current_values'] == 1
    assert archive.coverage(p.c)['unresolved_archive_jobs'] == 1
    job = p.c.execute('SELECT * FROM monday_lifecycle_events').fetchone()
    assert job['status'] == 'review' and job['result']['phase'] == 'applied_pending_verification'
    assert life.claim(p.c) is None
    with pytest.raises(life.ReviewRequired, match='Operator-only'):
        life.process_job(p.c, None, job)
    monkeypatch.setenv('SUPABASE_DB_URL', p.dsn)
    with pytest.raises(ValueError, match='non-operator'):
        cli.main(['requeue', '--event-key', job['event_key']])
    monkeypatch.setattr(pilot, 'verify_case', original)
    monkeypatch.setattr(refresh, 'write_values', lambda *a, **k: pytest.fail('Must not replay business writes'))
    assert pilot.run(p.c, None, p.directory, manifest['run_id'], applying=False)['complete']


def test_post_write_drift_cannot_certify_values(setup_pilot, monkeypatch):
    p = setup_pilot
    manifest = stage(p)
    original = pilot.apply_case
    def changed(*args):
        original(*args)
        p.source['projects']['101']['name'] = 'Changed after write'
    monkeypatch.setattr(pilot, 'apply_case', changed)
    with pytest.raises(life.ReviewRequired, match='Post-write Monday'):
        apply(p, manifest)
    assert archive.coverage(p.c)['unverified_current_values'] == 1
    assert archive.coverage(p.c)['unresolved_archive_jobs'] == 1
    assert json.loads((p.directory / 'result.json').read_text())['complete'] is False


def test_locked_boundary_rejects_late_owner(setup_pilot):
    p = setup_pilot
    manifest = stage(p)
    plan = json.loads((p.directory / 'plan.json').read_text())
    fresh = pilot.read_case(p.c, None, plan['cases'][0]['target'])
    p.c.execute("INSERT INTO subitems(monday_id,parent_monday_id,hidden_item_id) VALUES ('202','999','301')")
    states = rows(p.c, 'monday_item_lifecycle')
    with pytest.raises(life.ReviewRequired, match='out-of-scope owners'):
        pilot.apply_case(p.c, manifest, plan['cases'][0], fresh)
    assert rows(p.c, 'monday_item_lifecycle') == states


def test_production_capture_is_query_only_and_bounded(setup_pilot, monkeypatch):
    p = setup_pilot
    monkeypatch.setattr(refresh, 'fetch_project', p.fetch)
    monday = Monday({ident: row for table in life.BOARDS.values() for ident, row in p.source[table].items()})
    captured = refresh.fetch_project(monday, '101')
    pilot.require_source({'monday_id': '101', 'subitems': {'201': '301'}}, captured)
    assert monday.calls == [['101'], ['201'], ['301']]


def test_real_reader_never_uses_http_inside_transactions(setup_pilot, monkeypatch):
    p = setup_pilot
    monkeypatch.setattr(refresh, 'fetch_project', p.fetch)
    class Reader(Monday):
        def execute_query(self, query, variables):
            assert p.c.info.transaction_status == psycopg.pq.TransactionStatus.IDLE
            assert query.lstrip().startswith('query CompareMonday(')
            return super().execute_query(query, variables)
    monday = Reader({ident: row for table in life.BOARDS.values() for ident, row in p.source[table].items()})
    manifest = pilot.stage(p.c, monday, p.directory)
    assert pilot.run(p.c, monday, p.directory, manifest['run_id'], applying=True)['complete']
    assert {i for batch in monday.calls for i in batch} == {'101', '201', '301'}


@pytest.mark.parametrize('fault', ['ingestion_off', 'reporting_on', 'foreign_keys_disabled'])
def test_required_runtime_environment(setup_pilot, monkeypatch, fault):
    p = setup_pilot
    if fault == 'ingestion_off':
        monkeypatch.setenv('MONDAY_ARCHIVE_ENABLED', 'false')
    elif fault == 'reporting_on':
        monkeypatch.setenv('MONDAY_ARCHIVE_REPORTING_ENABLED', 'true')
    else:
        p.c.execute('ALTER TABLE subitems DISABLE TRIGGER ALL')
    with pytest.raises(ValueError):
        stage(p)
    assert not rows(p.c, 'monday_lifecycle_events')


@pytest.mark.parametrize('fault', ['expired_capture', 'lifecycle', 'schema', 'foreign_keys'])
def test_last_moment_guard_before_any_business_write(setup_pilot, fault):
    p = setup_pilot
    manifest = stage(p)
    staged = json.loads((p.directory / 'plan.json').read_text())['cases'][0]
    fresh = pilot.read_case(p.c, None, staged['target'])
    if fault == 'expired_capture':
        fresh['captured_at'] = (pilot.now() - timedelta(minutes=6)).isoformat()
    elif fault == 'lifecycle':
        p.c.execute("INSERT INTO monday_item_lifecycle(table_name,monday_id) VALUES ('projects','101')")
    elif fault == 'schema':
        p.c.execute('ALTER TABLE projects ADD COLUMN new_column text')
    else:
        p.c.execute('ALTER TABLE subitems DISABLE TRIGGER ALL')
    before = {t: rows(p.c, t) for t in [*life.BOARDS.values(), 'monday_item_lifecycle']}
    with pytest.raises(ValueError):
        pilot.apply_case(p.c, manifest, staged, fresh)
    assert {t: rows(p.c, t) for t in before} == before


def two_projects(p, monkeypatch):
    second = json.loads(json.dumps(p.source).replace('"101"', '"102"').replace('"201"', '"202"')
                        .replace('"301"', '"302"'))
    p.c.execute("INSERT INTO projects(monday_id) VALUES ('102')")
    p.c.execute("INSERT INTO hidden_items(monday_id) VALUES ('302')")
    p.c.execute("INSERT INTO subitems(monday_id,parent_monday_id,hidden_item_id) VALUES ('202','102','302')")
    targets = pilot.load_targets()
    targets['projects'].append(dict(monday_id='102', subitems={'202': '302'}))
    pilot.TARGETS.write_text(json.dumps(targets))
    sources = {'101': p.source, '102': second}
    monkeypatch.setattr(refresh, 'fetch_project', lambda monday, pid: deepcopy(sources[pid]))
    return second


def test_all_projects_preflight_before_first_write(setup_pilot, monkeypatch):
    p = setup_pilot
    second = two_projects(p, monkeypatch)
    manifest = stage(p)
    second['projects']['102']['name'] = 'Changed second project'
    with pytest.raises(life.ReviewRequired):
        apply(p, manifest)
    assert not rows(p.c, 'monday_lifecycle_events')


def test_partial_multi_project_run_resumes_without_rewriting_verified_project(setup_pilot, monkeypatch):
    p = setup_pilot
    two_projects(p, monkeypatch)
    manifest = stage(p)
    original = refresh.write_values
    def fail_second(connection, values, before, contract, *, job=None):
        if before['projects'][0]['monday_id'] == '102':
            connection.execute('SELECT 1/0')
        original(connection, values, before, contract, job=job)
    monkeypatch.setattr(refresh, 'write_values', fail_second)
    with pytest.raises(psycopg.errors.DivisionByZero):
        apply(p, manifest)
    result = json.loads((p.directory / 'result.json').read_text())
    assert result['verified_projects'] == ['101'] and result['complete'] is False
    def only_second(connection, values, before, contract, *, job=None):
        assert before['projects'][0]['monday_id'] == '102'
        original(connection, values, before, contract, job=job)
    monkeypatch.setattr(refresh, 'write_values', only_second)
    assert apply(p, manifest)['verified_projects'] == ['101', '102']


@pytest.mark.parametrize('first,second,invoice', [(None, None, None), (None, 0, 0), (25, 25, 50)])
def test_open_enquiry_is_sum_of_current_children(setup_pilot, first, second, invoice):
    p = setup_pilot
    extra = json.loads(json.dumps(dict(child=p.source['subitems']['201'],
                                      hidden=p.source['hidden_items']['301']))
                       .replace('"201"', '"202"').replace('"301"', '"302"'))
    p.source['subitems']['202'] = extra['child']
    p.source['hidden_items']['302'] = extra['hidden']
    member = deepcopy(p.source['projects']['101']['subitems'][0])
    member['id'] = '202'
    p.source['projects']['101']['subitems'].append(member)
    set_column(p.source['projects']['101'], pilot.compare.PARENT_COLUMNS['pipeline_stage'],
               {'__typename': 'StatusValue', 'label': 'Open'})
    set_column(extra['child'], pilot.compare.SUBITEM_COLUMNS['new_enquiry_value'],
               {'__typename': 'FormulaValue', 'display_value': '30'})
    for ident, value in [('301', first), ('302', second)]:
        set_column(p.source['hidden_items'][ident], pilot.compare.HIDDEN_ITEMS_COLUMNS['amount_invoiced'], number(value))
    p.c.execute("INSERT INTO hidden_items(monday_id) VALUES ('302')")
    p.c.execute("INSERT INTO subitems(monday_id,parent_monday_id,hidden_item_id) VALUES ('202','101','302')")
    targets = pilot.load_targets()
    targets['projects'][0]['subitems']['202'] = '302'
    pilot.TARGETS.write_text(json.dumps(targets))
    assert apply(p, stage(p))['complete']
    parent = life.read_rows(p.c, 'projects', 'monday_id', ['101'])[0]
    assert parent['new_enquiry_value'] == 120
    assert parent['total_amount_invoiced'] == invoice
    assert parent['total_order_value'] == 100


def test_verification_locks_out_concurrent_lifecycle_observations(setup_pilot, monkeypatch):
    p = setup_pilot
    original = archive.verify_parent_values
    def verify(connection, job, pid):
        with psycopg.connect(p.dsn, autocommit=True) as other:
            other.execute("SET lock_timeout='200ms'")
            with pytest.raises(psycopg.errors.LockNotAvailable):
                other.execute("UPDATE monday_item_lifecycle SET state_evidence='{}' "
                              "WHERE table_name='projects' AND monday_id='101'")
        original(connection, job, pid)
    monkeypatch.setattr(archive, 'verify_parent_values', verify)
    assert apply(p, stage(p))['complete']


def test_pinned_production_scope_matches_reviewed_ten():
    targets = pilot.load_targets()
    assert len(targets['projects']) == 10
    assert sum(len(t['subitems']) for t in targets['projects']) == 12
    assert targets['projects'][0]['monday_id'] == '3262467071'
    assert targets['projects'][-1]['monday_id'] == '2910684811'
