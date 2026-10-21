from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
import threading

import psycopg
import pytest

from src.services import monday_archive as archive
from src.services import monday_lifecycle as life
from src.services import monday_lifecycle_refresh as refresh
from scripts import order_value_monday_compare as compare
from test_monday_lifecycle_postgres import db, Monday, item, job, full_source, no_http
from test_order_value_scopes_postgres import database
from test_order_value_monday_compare_flat import set_column, number


@pytest.fixture
def archive_db(db, monkeypatch):
    c, dsn = db
    c.execute('CREATE VIEW reportable_projects AS SELECT * FROM projects')
    c.execute(Path(r'src\database\schema\monday_lifecycle_archive_state.sql').read_text())
    c.execute(Path(r'src\database\schema\monday_lifecycle_archive_runtime.sql').read_text())
    monkeypatch.setenv('MONDAY_ARCHIVE_ENABLED', 'true')
    monkeypatch.delenv('MONDAY_ARCHIVE_REPORTING_ENABLED', raising=False)
    return c, dsn


def observe(c, table, ident, *, state='active', parent=None, verified_at=None):
    source = item(ident, table, state)
    source['parent_item'] = {'id': parent} if parent else None
    with c.transaction():
        archive.observe(c, {'event_key': 'test-observation'}, table, ident, {ident: source},
                        verified_at or datetime.now(timezone.utc))


def test_archive_retains_history_links_and_financial_values(archive_db):
    c, _ = archive_db
    before = life.deletion_snapshot(c, 'subitems', '201')
    j = job(c, kind='reconcile')
    life.process_job(c, Monday({'201': item('201', state='archived')}), j)
    assert life.deletion_snapshot(c, 'subitems', '201') == before
    state = archive.state_row(c, 'subitems', '201')
    assert state['monday_state'] == 'archived' and not state['blocked']
    assert state['state_evidence']['item']['state'] == 'archived'
    assert c.execute("SELECT count(*) AS n FROM monday_lifecycle_events WHERE kind='refresh'").fetchone()['n'] == 1
    with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
        c.execute("UPDATE subitems SET quote_amount=0 WHERE monday_id='201'")
    with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState, match='verified current-value'):
        c.execute("UPDATE projects SET total_order_value=0 WHERE monday_id='101'")


def test_archived_parent_does_not_relabel_children_or_zero_values(archive_db):
    c, _ = archive_db
    observe(c, 'subitems', '201', parent='101')
    before = life.deletion_snapshot(c, 'projects', '101')
    j = job(c, life.PARENT_BOARD_ID, '101', 'reconcile')
    life.process_job(c, Monday({'101': item('101', 'projects', 'archived')}), j)
    assert life.deletion_snapshot(c, 'projects', '101') == before
    assert archive.state_row(c, 'subitems', '201')['monday_state'] == 'active'
    assert c.execute('SELECT count(*) AS n FROM current_subitems').fetchone()['n'] == 0
    with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
        c.execute("UPDATE subitems SET quote_amount=0 WHERE monday_id='201'")


def test_unknown_is_not_active_and_administrative_archive_label_is_not_lifecycle(archive_db):
    c, _ = archive_db
    c.execute("UPDATE projects SET pipeline_stage='Archive' WHERE monday_id='101'")
    assert c.execute('SELECT count(*) AS n FROM current_projects').fetchone()['n'] == 0
    observe(c, 'projects', '101')
    observe(c, 'subitems', '201', parent='101')
    assert [r['monday_id'] for r in c.execute('SELECT monday_id FROM current_projects')] == ['101']
    assert [r['monday_id'] for r in c.execute('SELECT monday_id FROM current_subitems')] == ['201']
    observe(c, 'subitems', '201', parent='999')
    assert c.execute('SELECT count(*) AS n FROM current_subitems').fetchone()['n'] == 0
    assert c.execute('SELECT count(*) AS n FROM reportable_projects').fetchone()['n'] == 2


def test_missing_source_never_becomes_archived(archive_db):
    c, _ = archive_db
    j = job(c, kind='reconcile')
    with pytest.raises(life.ReviewRequired, match='absence'):
        life.process_job(c, Monday({}), j)
    assert archive.state_row(c, 'subitems', '201') is None


def test_stale_observation_and_ordinary_reactivation_are_rejected(archive_db):
    c, _ = archive_db
    now = datetime.now(timezone.utc)
    observe(c, 'projects', '101', state='archived', verified_at=now)
    with pytest.raises(life.ReviewRequired, match='newer'):
        observe(c, 'projects', '101', state='archived', verified_at=now-timedelta(seconds=1))
    with pytest.raises(life.ReviewRequired, match='restoration'):
        observe(c, 'projects', '101')
    assert archive.state_row(c, 'projects', '101')['monday_state'] == 'archived'


def test_source_observation_batch_validates_every_item_before_writing(archive_db):
    c, _ = archive_db
    now = datetime.now(timezone.utc)
    observe(c, 'projects', '101', verified_at=now)
    child = item('201', 'subitems', 'active')
    child['parent_item'] = {'id': '101'}
    source = {'projects': {'101': item('101', 'projects', 'active')}, 'subitems': {'201': child},
              'hidden_items': {'301': item('301', 'hidden_items', 'archived')}}
    with pytest.raises(life.ReviewRequired, match='newer'):
        with c.transaction():
            archive.observe_source(c, {'event_key': 'batch'}, source, now - timedelta(seconds=1))
    assert archive.state_row(c, 'subitems', '201') is None
    with c.transaction():
        archive.observe_source(c, {'event_key': 'batch'}, source, now + timedelta(seconds=1))
    assert archive.state_row(c, 'projects', '101')['state_verified_at'] == now + timedelta(seconds=1)
    assert archive.state_row(c, 'subitems', '201')['monday_state'] == 'active'
    assert archive.state_row(c, 'hidden_items', '301') is None
    assert c.execute("SELECT count(*) AS n FROM monday_lifecycle_audit WHERE event_key='batch'").fetchone()['n'] == 2


def test_archive_rechecks_are_bounded_and_old_workers_cannot_claim(archive_db):
    c, _ = archive_db
    observe(c, 'projects', '101', state='archived')
    c.execute("UPDATE monday_item_lifecycle SET recheck_after=now()-interval '1 second'")
    assert life.schedule_rechecks(c) == 1
    assert life.schedule_rechecks(c) == 0
    with pytest.raises(psycopg.errors.RaiseException, match='archive-capable'):
        c.execute("UPDATE monday_lifecycle_events SET status='processing'")
    assert life.claim(c)['payload']['archive_policy'] == archive.POLICY


def test_archive_restore_refreshes_unchanged_members_too(archive_db, monkeypatch):
    c, _ = archive_db
    observe(c, 'projects', '101', state='archived')
    source = full_source()
    monkeypatch.setattr(refresh, 'fetch_project', lambda *a: deepcopy(source))
    j = job(c, life.PARENT_BOARD_ID, '101', 'reconcile')
    life.process_job(c, Monday({'101': item('101', 'projects', 'active')}), j)
    assert archive.state_row(c, 'projects', '101')['monday_state'] == 'active'
    assert archive.state_row(c, 'subitems', '201')['monday_state'] == 'active'
    assert archive.state_row(c, 'hidden_items', '301')['monday_state'] == 'active'
    assert life.read_rows(c, 'projects', 'monday_id', ['101'])[0]['total_order_value'] == 100


def test_runtime_migration_is_repeatable_and_preserves_base_rows(archive_db):
    c, _ = archive_db
    before = life.deletion_snapshot(c, 'projects', '101')
    c.execute(Path(r'src\database\schema\monday_lifecycle_archive_runtime.sql').read_text())
    assert life.deletion_snapshot(c, 'projects', '101') == before


def test_repeatable_read_writer_cannot_miss_an_archived_child(archive_db):
    c, dsn = archive_db
    with psycopg.connect(dsn) as old:
        old.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
        old.execute('SELECT count(*) FROM monday_item_lifecycle')
        observe(c, 'subitems', '201', state='archived')
        with pytest.raises(psycopg.errors.SerializationFailure):
            old.execute("UPDATE projects SET total_order_value=0 WHERE monday_id='101'")
        old.rollback()


def test_sync_gate_retains_archived_rows_and_queues_restore(archive_db, monkeypatch):
    c, dsn = archive_db
    monkeypatch.setenv('SUPABASE_DB_URL', dsn)
    monday = Monday({'201': item('201', state='archived')})
    monkeypatch.setattr(compare, 'ComparisonMondayClient', lambda: monday)
    before = life.deletion_snapshot(c, 'subitems', '201')
    assert archive.prepare_sync_rows('subitems', [{'monday_id': '201', 'parent_monday_id': '101'}]) == []
    assert life.deletion_snapshot(c, 'subitems', '201') == before
    active = item('201', state='active')
    active['parent_item'] = {'id': '101'}
    monday.rows = {'201': active, '101': item('101', 'projects', 'active')}
    assert archive.prepare_sync_rows('subitems', [{'monday_id': '201', 'parent_monday_id': '101'}]) == []
    assert archive.state_row(c, 'subitems', '201')['monday_state'] == 'archived'
    assert c.execute("SELECT count(*) AS n FROM monday_lifecycle_events WHERE kind='restore'").fetchone()['n'] == 1


@pytest.mark.parametrize('stage,enquiry', [('Open', 90), ('Lost', 777), ('Won - Closed (Invoiced)', 777)])
@pytest.mark.parametrize('invoice', [None, 0, 25])
def test_current_financial_refresh_preserves_distinct_rules_and_history(archive_db, monkeypatch, stage, enquiry, invoice):
    c, _ = archive_db
    c.execute("UPDATE projects SET pipeline_stage=%s,new_enquiry_value=777 WHERE monday_id='101'", (stage,))
    c.execute("INSERT INTO subitems(monday_id,parent_monday_id,new_enquiry_value) VALUES ('202','101',9999)")
    observe(c, 'subitems', '202', state='archived')
    source = full_source()
    source['subitems']['202'] = item('202', state='archived')
    set_column(source['projects']['101'], compare.PARENT_COLUMNS['pipeline_stage'],
               {'__typename': 'StatusValue', 'label': stage})
    set_column(source['hidden_items']['301'], compare.HIDDEN_ITEMS_COLUMNS['amount_invoiced'], number(invoice))
    monkeypatch.setattr(compare, 'capture', lambda *a: deepcopy(source))
    before = life.read_rows(c, 'subitems', 'monday_id', ['202'])
    j = job(c, life.PARENT_BOARD_ID, '101', 'refresh')
    archive.refresh_current_values(c, None, j)
    actual = life.read_rows(c, 'projects', 'monday_id', ['101'])[0]
    assert actual['new_enquiry_value'] == enquiry
    assert actual['total_order_value'] == 100  # Actual parent mirror, not material + charges.
    assert actual['total_amount_invoiced'] == invoice
    assert life.read_rows(c, 'subitems', 'monday_id', ['202']) == before


def test_hidden_deletion_can_unlink_archived_history_without_rewriting_it(archive_db):
    c, _ = archive_db
    observe(c, 'subitems', '201', state='archived')
    before = life.read_rows(c, 'subitems', 'monday_id', ['201'])[0]
    j = job(c, life.HIDDEN_ITEMS_BOARD_ID, '301', 'delete')
    life.process_job(c, Monday({'301': item('301', 'hidden_items')}), j)
    after = life.read_rows(c, 'subitems', 'monday_id', ['201'])[0]
    assert after['hidden_item_id'] is None
    assert all(after[k] == v for k, v in before.items() if k not in {'hidden_item_id', 'updated_at', 'last_synced_at'})
    assert archive.state_row(c, 'subitems', '201')['monday_state'] == 'archived'
    assert archive.state_row(c, 'hidden_items', '301')['monday_state'] == 'deleted'


def test_current_reporting_requires_coverage_and_keeps_history(archive_db, monkeypatch):
    c, dsn = archive_db
    monkeypatch.setenv('SUPABASE_DB_URL', dsn)
    assert archive.current_relation('reportable_projects') == 'reportable_projects'
    monkeypatch.setenv('MONDAY_ARCHIVE_REPORTING_ENABLED', 'true')
    with pytest.raises(life.ReviewRequired, match='coverage'):
        archive.current_relation('reportable_projects')
    observe(c, 'projects', '101', state='archived')
    observe(c, 'projects', '999', state='archived')
    assert archive.current_relation('reportable_projects') == 'current_projects'
    assert c.execute('SELECT count(*) AS n FROM reportable_projects').fetchone()['n'] == 2


def test_reporting_sql_copies_formulas_and_never_overwrites_snapshots(archive_db):
    c, _ = archive_db
    c.execute('CREATE VIEW vw_pipeline_forecast_project_v1 AS SELECT monday_id AS project_id,total_order_value FROM reportable_projects')
    c.execute('CREATE VIEW vw_pipeline_smoothing_score_v1 AS SELECT * FROM vw_pipeline_forecast_project_v1')
    c.execute('CREATE MATERIALIZED VIEW mv_pipeline_forecast_monthly_12m_v1 AS SELECT count(*) AS n FROM vw_pipeline_forecast_project_v1')
    c.execute('CREATE MATERIALIZED VIEW mv_pipeline_smoothed_revenue_monthly_12m_v1 AS SELECT count(*) AS n FROM vw_pipeline_smoothing_score_v1')
    for name, source in [('pipeline_forecast', 'vw_pipeline_forecast_project_v1'),
                         ('pipeline_smoothing_forecast', 'vw_pipeline_smoothing_score_v1')]:
        c.execute(f'CREATE TABLE {name}_snapshot(snapshot_date date,project_id text)')
        c.execute(f'''CREATE FUNCTION create_{name}_snapshot(target_snapshot_date date DEFAULT CURRENT_DATE)
            RETURNS integer LANGUAGE plpgsql AS $$ BEGIN
            INSERT INTO {name}_snapshot SELECT target_snapshot_date,project_id FROM {source};
            RETURN 1; END $$''')
    c.execute(Path(r'src\database\schema\monday_archive_reporting.sql').read_text())
    c.execute(Path(r'src\database\schema\monday_archive_reporting.sql').read_text())
    observe(c, 'projects', '101')
    observe(c, 'projects', '999', state='archived')
    assert c.execute('SELECT n FROM current_pipeline_forecast_monthly').fetchone()['n'] == 1
    assert c.execute('SELECT n FROM current_pipeline_smoothed_monthly').fetchone()['n'] == 1
    assert c.execute('SELECT count(*) AS n FROM vw_pipeline_forecast_project_v1').fetchone()['n'] == 2
    c.execute('SELECT create_current_pipeline_forecast_snapshot(CURRENT_DATE)')
    assert c.execute('SELECT project_id FROM pipeline_forecast_snapshot').fetchone()['project_id'] == '101'
    with pytest.raises(psycopg.errors.RaiseException, match='cannot replace history'):
        c.execute('SELECT create_current_pipeline_forecast_snapshot(CURRENT_DATE)')
    with pytest.raises(psycopg.errors.RaiseException, match='cannot replace history'):
        c.execute('SELECT create_current_pipeline_smoothing_forecast_snapshot(CURRENT_DATE-1)')


def test_restore_reparent_verifies_both_parents_and_audits_old_link(archive_db, monkeypatch):
    c, _ = archive_db
    observe(c, 'subitems', '201', state='archived')
    source = full_source()
    source['projects']['999'] = source['projects'].pop('101')
    source['projects']['999']['id'] = '999'
    source['projects']['999']['subitems'][0]['parent_item'] = {'id': '999'}
    source['subitems']['201']['parent_item'] = {'id': '999'}
    source['project_ids'] = ['999']
    monkeypatch.setattr(refresh, 'fetch_project', lambda *a: deepcopy(source))
    active = item('201', state='active')
    active['parent_item'] = {'id': '999'}
    old_parent = item('101', 'projects', 'active')
    old_parent['subitems'] = []
    new_parent = item('999', 'projects', 'active')
    j = job(c, item_id='201', kind='restore')
    life.process_job(c, Monday({'201': active, '101': old_parent, '999': new_parent}), j)
    assert life.read_rows(c, 'subitems', 'monday_id', ['201'])[0]['parent_monday_id'] == '999'
    assert c.execute("SELECT count(*) AS n FROM monday_lifecycle_events WHERE item_id='101' AND kind='refresh'").fetchone()['n'] == 1
    assert c.execute("SELECT count(*) AS n FROM monday_lifecycle_audit WHERE action='refresh_field_changes' "
                     "AND table_name='subitems' AND monday_id='201' AND before_row->>'parent_monday_id'='101'").fetchone()['n'] == 1


def test_archive_stage_is_read_only_and_changed_state_cannot_authorise_deletion(archive_db, tmp_path):
    from types import SimpleNamespace
    from scripts import monday_lifecycle as cli
    c, _ = archive_db
    before = life.deletion_snapshot(c, 'projects', '101')
    args = SimpleNamespace(run_dir=tmp_path / 'archive', board=life.PARENT_BOARD_ID,
                           item_id=['101'], review_csv=None, state='archived')
    manifest = cli.stage(c, Monday({'101': item('101', 'projects', 'archived')}), args)
    assert manifest['selected'] == 1 and manifest['deferred'] == 0 and manifest['read_only']
    assert life.deletion_snapshot(c, 'projects', '101') == before
    assert archive.state_row(c, 'projects', '101') is None
    args.confirm_run_id = manifest['run_id']
    cli.queue_run(c, args)
    j = life.claim(c)
    with pytest.raises(life.ReviewRequired, match='changed since archive staging'):
        life.process_job(c, Monday({'101': item('101', 'projects'), '201': item('201')}), j)
    assert life.deletion_snapshot(c, 'projects', '101') == before


@pytest.mark.parametrize('path,relation', [
    ('/forecast/pipeline', 'current_pipeline_forecast_monthly'),
    ('/forecast/smoothing/projects', 'current_pipeline_smoothing_score'),
    ('/forecast/smoothing/monthly', 'current_pipeline_smoothed_monthly'),
])
def test_current_forecast_endpoints_identify_source_and_withhold_incomplete_totals(monkeypatch, path, relation):
    from types import SimpleNamespace
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from src.api.routes import forecast

    queried = []

    class Query:
        def table(self, name):
            queried.append(name)
            return self

        def execute(self):
            return SimpleNamespace(data=[], count=0)

        def __getattr__(self, name):
            return lambda *a, **kw: self

    monkeypatch.setattr(forecast, 'SupabaseClient', lambda: SimpleNamespace(client=Query()))
    monkeypatch.setattr(forecast, 'current_relation', lambda name: relation)
    app = FastAPI()
    app.include_router(forecast.router)
    client = TestClient(app)
    response = client.get(path)
    assert response.status_code == 200
    assert response.json()['source'] == relation and queried == [relation]

    def incomplete(name):
        raise life.ReviewRequired('Unverified test population')

    monkeypatch.setattr(forecast, 'current_relation', incomplete)
    response = client.get(path)
    assert response.status_code == 503
    assert 'totals' not in response.json()
    historical = client.get('/forecast/snapshot')
    assert historical.status_code == 200
    assert historical.json()['source'] == 'pipeline_forecast_snapshot'


def test_prediction_publication_never_writes_monday_for_inactive_or_unverified_projects(monkeypatch):
    from types import SimpleNamespace
    from src.services.monday_update_service import MondayUpdateService
    monkeypatch.setenv('MONDAY_ARCHIVE_ENABLED', 'true')
    db = SimpleNamespace(is_project_reporting_excluded=lambda _: False,
                         is_project_lifecycle_ready=lambda _: False)
    monday = SimpleNamespace(update_item_columns=lambda *a, **k: pytest.fail('No Monday writes for inactive records'))
    result = MondayUpdateService(db_client=db, monday_client=monday).sync_project('101', analysis={'rating_score': 1})
    assert result == {'success': True, 'skipped': True, 'reason': 'lifecycle_not_ready'}


def test_inactive_mirror_dependencies_withhold_current_fields_without_zeroing_history():
    from test_order_value_monday_compare_flat import flat_data
    source, before, contract, _ = flat_data()
    source['hidden_items']['301']['state'] = 'archived'
    values, issues = compare.project_projection('101', source, before, contract, lifecycle={})
    assert 'total_order_value' not in values['projects']['101']
    assert 'total_amount_invoiced' not in values['projects']['101']
    assert any('not API active' in issue['reason'] for issue in issues)


def test_active_normal_sync_ignores_archive_label_and_preserves_financial_history(archive_db, monkeypatch):
    c, dsn = archive_db
    monkeypatch.setenv('SUPABASE_DB_URL', dsn)
    source = item('101', 'projects', 'active')
    monday = Monday({'101': source})
    monkeypatch.setattr(compare, 'ComparisonMondayClient', lambda: monday)
    before = life.read_rows(c, 'projects', 'monday_id', ['101'])
    rows = archive.prepare_sync_rows('projects', [{
        'monday_id': '101', 'pipeline_stage': 'Archive',
        'total_order_value': 0, 'total_amount_invoiced': 0, 'new_enquiry_value': 0}])
    assert rows == [{'monday_id': '101', 'pipeline_stage': 'Archive'}]
    assert archive.state_row(c, 'projects', '101')['monday_state'] == 'active'
    assert life.read_rows(c, 'projects', 'monday_id', ['101']) == before


def test_sync_gate_waits_out_a_concurrent_lock_holder(archive_db, monkeypatch):
    c, dsn = archive_db
    monkeypatch.setenv('SUPABASE_DB_URL', dsn)
    monkeypatch.setattr(compare, 'ComparisonMondayClient',
                        lambda: Monday({'101': item('101', 'projects', 'active')}))
    with psycopg.connect(dsn) as holder:
        holder.execute('LOCK TABLE public.projects,public.hidden_items,public.subitems IN SHARE ROW EXCLUSIVE MODE')
        # Another gated writer keeps the lock for several 750 ms lock waits.
        release = threading.Timer(2.5, holder.rollback)
        release.start()
        try:
            rows = archive.prepare_sync_rows('projects', [{'monday_id': '101', 'pipeline_stage': 'Open'}])
        finally:
            release.join()
    assert rows == [{'monday_id': '101', 'pipeline_stage': 'Open'}]
    assert archive.state_row(c, 'projects', '101')['monday_state'] == 'active'


def test_normal_sync_missing_source_aborts_without_partial_observations(archive_db, monkeypatch):
    c, dsn = archive_db
    monkeypatch.setenv('SUPABASE_DB_URL', dsn)
    monkeypatch.setattr(compare, 'ComparisonMondayClient',
                        lambda: Monday({'101': item('101', 'projects', 'active')}))
    with pytest.raises(life.ReviewRequired, match='absence'):
        archive.prepare_sync_rows('projects', [{'monday_id': '101'}, {'monday_id': '999'}])
    assert archive.state_row(c, 'projects', '101') is None
    assert archive.state_row(c, 'projects', '999') is None


def linked_subitem(ident, hidden_id, state='active'):
    source = item(ident, 'subitems', state)
    source['parent_item'] = {'id': '101'}
    source['column_values'] = [{'id': compare.SUBITEM_COLUMNS['hidden_item_id'], 'type': 'board_relation',
                                'text': None, 'value': None, 'linked_item_ids': [hidden_id]}]
    return source


def test_batched_sync_gate_keeps_each_rows_lifecycle_decision(archive_db, monkeypatch):
    c, dsn = archive_db
    monkeypatch.setenv('SUPABASE_DB_URL', dsn)
    observe(c, 'subitems', '205', state='archived', parent='101')
    life.marker(c, 'hidden_items', '303', True, {'event_key': 'earlier-deletion'})
    links = {'201': '301', '202': '302', '203': '302', '204': '303', '205': '301', '206': '301'}
    sources = {ident: linked_subitem(ident, hid) for ident, hid in links.items()}
    sources['206']['state'] = 'archived'
    monkeypatch.setattr(compare, 'ComparisonMondayClient', lambda: Monday({
        **sources, '101': item('101', 'projects', 'active'), '301': item('301', 'hidden_items', 'active'),
        '302': item('302', 'hidden_items', 'archived'), '303': item('303', 'hidden_items', 'active')}))
    rows = [{'monday_id': ident, 'parent_monday_id': '101', 'hidden_item_id': hid} for ident, hid in links.items()]

    assert archive.prepare_sync_rows('subitems', rows) == rows[:1]

    states = {(r['table_name'], r['monday_id']): (r['monday_state'], r['blocked']) for r in c.execute(
        'SELECT table_name,monday_id,monday_state,blocked FROM monday_item_lifecycle')}
    assert {k: v for k, v in states.items() if k[0] == 'subitems'} == {
        ('subitems', i): ('active', False) for i in ('201', '202', '203', '204')} | {
        ('subitems', '205'): ('archived', False), ('subitems', '206'): ('archived', False)}
    assert states[('hidden_items', '302')] == ('archived', False)
    assert states[('hidden_items', '303')] == (None, True)
    # The shared archived source is observed once; the second owner sees that observation.
    assert [(r['action'], r['monday_id']) for r in c.execute(
        "SELECT action,monday_id FROM monday_lifecycle_audit WHERE event_key LIKE 'sync-observation:%' ORDER BY id")] == [
        ('observe_monday_state', '201'), ('observe_monday_state', '202'), ('observe_monday_state', '302'),
        ('observe_monday_state', '203'), ('observe_monday_state', '204'), ('observe_monday_state', '206'),
        ('retain_archived_record', '206')]
    assert {(r['kind'], r['board_id'], r['item_id'], r['payload']['archive_policy']) for r in c.execute(
        "SELECT kind,board_id,item_id,payload FROM monday_lifecycle_events WHERE event_key LIKE 'sync-observation:%'")} == {
        ('restore', life.HIDDEN_ITEMS_BOARD_ID, '303', archive.POLICY),
        ('restore', life.SUBITEM_BOARD_ID, '205', archive.POLICY),
        ('reconcile', life.SUBITEM_BOARD_ID, '206', archive.POLICY)}


def known_active(c):
    """Project 101, subitem 201 under it, and its hidden source 301, all observed active."""
    observe(c, 'projects', '101')
    observe(c, 'subitems', '201', parent='101')
    observe(c, 'hidden_items', '301')
    c.execute("UPDATE monday_item_lifecycle SET recheck_after=now()+interval '1 day'")


def periodic_check(c, table, ident):
    c.execute("UPDATE monday_item_lifecycle SET recheck_after=now()-interval '1 second' "
              'WHERE table_name=%s AND monday_id=%s', (table, ident))
    assert life.schedule_rechecks(c) == 1
    claimed = life.claim(c)
    assert claimed['payload']['cause'] == 'periodic_lifecycle_check'
    return claimed


def still_active(ident, table, parent=None):
    source = item(ident, table, 'active')
    source['parent_item'] = {'id': parent} if parent else None
    return source


def business_rows(c):
    return {t: c.execute(f'SELECT * FROM {t} ORDER BY monday_id').fetchall()
            for t in ('projects', 'subitems', 'hidden_items')}


@pytest.fixture
def no_project_refresh(monkeypatch):
    calls = []
    monkeypatch.setattr(refresh, 'restore_item', lambda c, monday, j, observed: calls.append(j['item_id']))
    return calls


@pytest.mark.parametrize('table,ident,parent', [('subitems', '201', '101'), ('hidden_items', '301', None)])
def test_periodic_check_of_an_active_child_records_only_the_observation(
        archive_db, no_project_refresh, table, ident, parent):
    c, _ = archive_db
    known_active(c)
    previous = archive.state_row(c, table, ident)
    rows = business_rows(c)
    j = periodic_check(c, table, ident)
    monday = Monday({ident: still_active(ident, table, parent)})

    life.process_job(c, monday, j)

    assert not no_project_refresh and monday.calls == [[ident], [ident]]
    event = c.execute('SELECT status,result FROM monday_lifecycle_events WHERE event_key=%s', (j['event_key'],)).fetchone()
    assert event == {'status': 'processed', 'result': {'observed_active': ident, 'table': table, 'rows_written': 0}}
    state = archive.state_row(c, table, ident)
    assert state['monday_state'] == 'active' and not state['blocked']
    assert state['state_event_key'] == j['event_key'] and state['state_verified_at'] > previous['state_verified_at']
    assert state['state_evidence']['parent_monday_id'] == parent
    assert c.execute("SELECT recheck_after > now()+interval '23 hours' AS later FROM monday_item_lifecycle "
                     'WHERE table_name=%s AND monday_id=%s', (table, ident)).fetchone()['later']
    assert [(r['action'], r['monday_id']) for r in c.execute(
        'SELECT action,monday_id FROM monday_lifecycle_audit WHERE event_key=%s', (j['event_key'],))] == [
        ('observe_monday_state', ident)]
    # No business writes and no parent or owner refreshes queued.
    assert business_rows(c) == rows
    assert c.execute('SELECT count(*) AS n FROM monday_lifecycle_events WHERE event_key<>%s',
                     (j['event_key'],)).fetchone()['n'] == 0


@pytest.mark.parametrize('case', ['project', 'moved_subitem', 'unstored_subitem', 'blocked_source', 'operator_reconcile'])
def test_other_active_checks_keep_the_full_refresh(archive_db, no_project_refresh, case):
    c, _ = archive_db
    known_active(c)
    table, ident, parent = 'subitems', '201', '101'
    if case == 'project':
        table, ident, parent = 'projects', '101', None
    elif case == 'moved_subitem':
        parent = '999'
    elif case == 'unstored_subitem':
        ident = '202'
        observe(c, 'subitems', '202', parent='101')
    elif case == 'blocked_source':
        table, ident, parent = 'hidden_items', '301', None
        life.marker(c, 'hidden_items', '301', True, {'event_key': 'earlier-deletion'})
    if case == 'operator_reconcile':
        life.enqueue(c, 'reconcile', life.SUBITEM_BOARD_ID, ident)
        j = life.claim(c)
    else:
        j = periodic_check(c, table, ident)
    life.process_job(c, Monday({ident: still_active(ident, table, parent)}), j)
    assert no_project_refresh == [ident]


def test_periodic_check_superseded_by_a_later_observation_writes_nothing(archive_db, no_project_refresh):
    c, _ = archive_db
    known_active(c)
    j = periodic_check(c, 'subitems', '201')
    observe(c, 'subitems', '201', parent='101', verified_at=datetime.now(timezone.utc) + timedelta(hours=1))
    later = archive.state_row(c, 'subitems', '201')

    life.process_job(c, Monday({'201': still_active('201', 'subitems', '101')}), j)

    assert archive.state_row(c, 'subitems', '201') == later
    result = c.execute('SELECT status,result FROM monday_lifecycle_events WHERE event_key=%s', (j['event_key'],)).fetchone()
    assert result['status'] == 'processed' and result['result']['superseded_by'] == 'test-observation'
    assert not c.execute('SELECT 1 FROM monday_lifecycle_audit WHERE event_key=%s', (j['event_key'],)).fetchall()


class ChangingMonday(Monday):
    """Runs `change` during the check's second Monday read, after its pre-checks."""

    def __init__(self, rows, change):
        super().__init__(rows)
        self.change = change

    def execute_query(self, query, variables):
        if self.calls:
            self.change(self.rows)
        return super().execute_query(query, variables)


@pytest.mark.parametrize('change,error', [
    (lambda c: lambda rows: rows['201'].update(updated_at='2026-10-06T00:00:00Z'), 'Monday changed'),
    (lambda c: lambda rows: life.marker(c, 'subitems', '201', True, {'event_key': 'concurrent-deletion'}),
     'lifecycle state changed'),
    (lambda c: lambda rows: c.execute("UPDATE subitems SET parent_monday_id='999' WHERE monday_id='201'"),
     'lifecycle state changed'),
])
def test_periodic_check_retries_when_evidence_changes_during_the_check(archive_db, no_project_refresh, change, error):
    c, _ = archive_db
    known_active(c)
    previous = archive.state_row(c, 'subitems', '201')
    j = periodic_check(c, 'subitems', '201')
    c.execute("UPDATE monday_lifecycle_events SET status='pending',attempts=0,lease_token=NULL,lease_until=NULL "
              'WHERE event_key=%s', (j['event_key'],))

    assert life.run_once(c, ChangingMonday({'201': still_active('201', 'subitems', '101')}, change(c)))

    event = c.execute('SELECT status,last_error FROM monday_lifecycle_events WHERE event_key=%s',
                      (j['event_key'],)).fetchone()
    assert event['status'] == 'retry' and error in event['last_error']
    assert archive.state_row(c, 'subitems', '201')['state_event_key'] == previous['state_event_key']
    assert not no_project_refresh


@pytest.mark.parametrize('newer,agrees', [({}, True), ({'parent': '999'}, False), ({'state': 'archived'}, False)])
def test_source_observation_batch_keeps_only_agreeing_newer_child_observations(archive_db, newer, agrees):
    c, _ = archive_db
    now = datetime.now(timezone.utc)
    observe(c, 'subitems', '201', **{'parent': '101', 'verified_at': now + timedelta(hours=1), **newer})
    later = archive.state_row(c, 'subitems', '201')
    source = {'projects': {'101': still_active('101', 'projects')},
              'subitems': {'201': still_active('201', 'subitems', '101')},
              'hidden_items': {'301': still_active('301', 'hidden_items')}}
    if agrees:
        with c.transaction():
            archive.observe_source(c, {'event_key': 'batch'}, source, now)
        assert archive.state_row(c, 'projects', '101')['state_event_key'] == 'batch'
        assert archive.state_row(c, 'hidden_items', '301')['state_event_key'] == 'batch'
    else:
        with pytest.raises(life.ReviewRequired, match='newer'):
            with c.transaction():
                archive.observe_source(c, {'event_key': 'batch'}, source, now)
        assert archive.state_row(c, 'projects', '101') is None
    assert archive.state_row(c, 'subitems', '201') == later


def test_project_refresh_survives_a_concurrent_check_of_one_of_its_children(archive_db, monkeypatch):
    # Production: a child's periodic check committed while its project's refresh was running.
    c, _ = archive_db
    source = full_source()
    reads = []

    def fetch(*args):
        reads.append(1)
        if len(reads) == 2:
            observe(c, 'subitems', '201', parent='101')
        return deepcopy(source)
    monkeypatch.setattr(refresh, 'fetch_project', fetch)
    j = job(c, life.PARENT_BOARD_ID, '101', 'refresh')

    refresh.refresh_project(c, Monday({'101': item('101', 'projects', 'active')}), j)

    assert c.execute('SELECT status FROM monday_lifecycle_events WHERE event_key=%s',
                     (j['event_key'],)).fetchone()['status'] == 'processed'
    assert archive.state_row(c, 'subitems', '201')['state_event_key'] == 'test-observation'
    assert archive.state_row(c, 'projects', '101')['state_event_key'] == j['event_key']
    assert archive.state_row(c, 'hidden_items', '301')['state_event_key'] == j['event_key']


def test_sync_gate_keeps_a_newer_agreeing_child_observation(archive_db, monkeypatch):
    c, dsn = archive_db
    monkeypatch.setenv('SUPABASE_DB_URL', dsn)
    observe(c, 'subitems', '201', parent='101', verified_at=datetime.now(timezone.utc) + timedelta(hours=1))
    later = archive.state_row(c, 'subitems', '201')
    monkeypatch.setattr(compare, 'ComparisonMondayClient', lambda: Monday({
        '201': linked_subitem('201', '301'), '101': item('101', 'projects', 'active'),
        '301': item('301', 'hidden_items', 'active')}))
    rows = [{'monday_id': '201', 'parent_monday_id': '101', 'hidden_item_id': '301'}]

    assert archive.prepare_sync_rows('subitems', rows) == rows
    assert archive.state_row(c, 'subitems', '201') == later


def test_incomplete_analysis_population_is_an_error_not_an_empty_dataset(monkeypatch):
    from types import SimpleNamespace
    from src.database.supabase_client import SupabaseClient
    monkeypatch.setenv('MONDAY_ARCHIVE_ENABLED', 'true')
    client = SupabaseClient.__new__(SupabaseClient)
    client.client = SimpleNamespace(table=lambda _: None)

    def incomplete(name):
        raise life.ReviewRequired('Unverified population')

    monkeypatch.setattr(archive, 'current_relation', incomplete)
    with pytest.raises(life.ReviewRequired, match='Unverified population'):
        client.get_projects_for_analysis()



def verified_project(c, pid='101', children=('201',)):
    """A project whose last full refresh recorded its subitems and verified its values."""
    source = still_active(pid, 'projects')
    source['subitems'] = [{'id': cid, 'state': 'active', 'board': {'id': life.SUBITEM_BOARD_ID},
                           'parent_item': {'id': pid}} for cid in children]
    with c.transaction():
        archive.observe(c, {'event_key': 'verified-refresh'}, 'projects', pid, {pid: source},
                        datetime.now(timezone.utc))
        archive.verify_parent_values(c, {'event_key': 'verified-refresh'}, pid)
    return source


def due(c, *keys):
    c.execute("UPDATE monday_item_lifecycle SET recheck_after=now()+interval '30 days'")
    for key in keys:
        c.execute("UPDATE monday_item_lifecycle SET recheck_after=now()-interval '1 second' "
                  'WHERE table_name=%s AND monday_id=%s', key)


def days_to_next_check(c, table, ident):
    return float(c.execute('SELECT extract(epoch FROM recheck_after-now())/86400 AS d FROM monday_item_lifecycle '
                           'WHERE table_name=%s AND monday_id=%s', (table, ident)).fetchone()['d'])


def without_schedule(row):
    return {k: v for k, v in row.items() if k != 'recheck_after'}


def queued(c):
    return [(r['kind'], r['item_id'], r['payload'].get('cause'), r['payload'].get('verification_only'))
            for r in c.execute('SELECT kind,item_id,payload FROM monday_lifecycle_events ORDER BY item_id')]


ACTIVE = (('projects', '101'), ('subitems', '201'), ('hidden_items', '301'))


def active_monday(project):
    return Monday({'101': project, '201': still_active('201', 'subitems', '101'),
                   '301': still_active('301', 'hidden_items')})


def test_periodic_check_records_unchanged_items_with_two_bulk_reads(archive_db, no_project_refresh):
    c, _ = archive_db
    known_active(c)
    monday = active_monday(verified_project(c))
    project = archive.state_row(c, 'projects', '101')
    rows = business_rows(c)
    due(c, *ACTIVE)

    assert archive.periodic_check(c, monday) == 3

    # Two reads: the project with its subitems, then children and sources together.
    assert monday.calls == [['101'], ['201', '301'], ['101'], ['201', '301']]
    for table, ident in ACTIVE[1:]:
        assert archive.state_row(c, table, ident)['state_event_key'].startswith('periodic-check:')
    # A lifecycle read cannot re-verify values: the project's verified observation is kept.
    assert without_schedule(archive.state_row(c, 'projects', '101')) == without_schedule(project)
    actions = c.execute("SELECT action,monday_id,evidence FROM monday_lifecycle_audit "
                        "WHERE event_key LIKE 'periodic-check:%' ORDER BY monday_id").fetchall()
    assert [(a['action'], a['monday_id']) for a in actions] == [
        ('recheck_monday_state', '101'), ('observe_monday_state', '201'), ('observe_monday_state', '301')]
    assert actions[0]['evidence']['kept_observation'] == 'verified-refresh'
    assert all(5.2 < days_to_next_check(c, *key) < 8.8 for key in ACTIVE)
    assert not queued(c) and not no_project_refresh and business_rows(c) == rows


@pytest.mark.parametrize('case', ['project_subitems', 'project_unverified', 'moved_subitem',
                                  'archived_subitem', 'missing_source', 'unstored_subitem'])
def test_periodic_check_queues_the_full_check_for_anything_changed(archive_db, no_project_refresh, case):
    c, _ = archive_db
    known_active(c)
    project = verified_project(c) if case != 'project_unverified' else {
        **still_active('101', 'projects'), 'subitems': []}
    monday = active_monday(project)
    target = {'project_subitems': ('projects', '101'), 'project_unverified': ('projects', '101'),
              'missing_source': ('hidden_items', '301')}.get(case, ('subitems', '201'))
    if case == 'project_subitems':
        monday.rows['101'] = {**project, 'subitems': project['subitems'] + [
            {'id': '202', 'state': 'active', 'board': {'id': life.SUBITEM_BOARD_ID}, 'parent_item': {'id': '101'}}]}
    elif case == 'moved_subitem':
        monday.rows['201']['parent_item'] = {'id': '999'}
    elif case == 'archived_subitem':
        monday.rows['201']['state'] = 'archived'
    elif case == 'missing_source':
        del monday.rows['301']
    elif case == 'unstored_subitem':
        target = ('subitems', '202')
        observe(c, 'subitems', '202', parent='101')
        monday.rows['202'] = still_active('202', 'subitems', '101')
    before = archive.state_row(c, *target)
    due(c, *ACTIVE, target)

    archive.periodic_check(c, monday)

    assert queued(c) == [('reconcile', target[1], 'periodic_lifecycle_check', True)]
    assert without_schedule(archive.state_row(c, *target)) == without_schedule(before)
    assert 5.2 < days_to_next_check(c, *target) < 8.8
    others = [key for key in ACTIVE if key != target and not (case == 'project_unverified' and key[0] == 'projects')]
    assert all(5.2 < days_to_next_check(c, *key) < 8.8 for key in others)
    if case in {'project_subitems', 'moved_subitem', 'unstored_subitem'}:
        # The queued check takes the existing full path, as daily checks did.
        life.process_job(c, monday, life.claim(c))
        assert no_project_refresh == [target[1]]


def test_periodic_check_leaves_items_that_changed_between_reads(archive_db):
    c, _ = archive_db
    known_active(c)
    before = archive.state_row(c, 'subitems', '201')
    due(c, ('subitems', '201'))
    monday = ChangingMonday({'201': still_active('201', 'subitems', '101')},
                            lambda rows: rows['201'].update(updated_at='2026-10-21T00:00:00Z'))

    assert archive.periodic_check(c, monday) == 1

    assert without_schedule(archive.state_row(c, 'subitems', '201')) == without_schedule(before)
    assert not queued(c)
    assert 0.03 < days_to_next_check(c, 'subitems', '201') < 0.05  # Leased for an hour, then retried.


def test_periodic_check_skips_items_another_writer_observed_meanwhile(archive_db):
    c, _ = archive_db
    known_active(c)
    due(c, ('subitems', '201'))
    monday = ChangingMonday({'201': still_active('201', 'subitems', '101')},
                            lambda rows: observe(c, 'subitems', '201', parent='101'))

    archive.periodic_check(c, monday)

    assert archive.state_row(c, 'subitems', '201')['state_event_key'] == 'test-observation'
    assert not c.execute("SELECT 1 FROM monday_lifecycle_audit WHERE event_key LIKE 'periodic-check:%'").fetchall()


def test_periodic_check_reads_one_hundred_items_per_call(archive_db):
    c, _ = archive_db
    ids = [str(n) for n in range(5000, 5150)]
    c.execute("INSERT INTO subitems(monday_id,parent_monday_id) SELECT g::text,'101' FROM generate_series(5000,5149) g")
    c.execute("""INSERT INTO monday_item_lifecycle
        (table_name,monday_id,monday_state,state_verified_at,state_event_key,state_evidence,recheck_after)
        SELECT 'subitems',g::text,'active',now()-interval '8 days','seed',
               '{"basis":"test","parent_monday_id":"101"}'::jsonb,now()-interval '1 second'
        FROM generate_series(5000,5149) g
        ON CONFLICT (table_name,monday_id) DO UPDATE SET monday_state=EXCLUDED.monday_state,
            state_verified_at=EXCLUDED.state_verified_at,state_event_key=EXCLUDED.state_event_key,
            state_evidence=EXCLUDED.state_evidence,recheck_after=EXCLUDED.recheck_after""")
    monday = Monday({i: still_active(i, 'subitems', '101') for i in ids})

    assert archive.periodic_check(c, monday) == 150

    # The old per-item checks needed at least two calls for each of the 150.
    assert [len(call) for call in monday.calls] == [100, 50, 100, 50]
    assert c.execute("SELECT count(*) AS n FROM monday_item_lifecycle "
                     "WHERE state_event_key LIKE 'periodic-check:%'").fetchone()['n'] == 150


def test_periodic_check_claims_due_items_once(archive_db):
    c, _ = archive_db
    known_active(c)
    due(c, ('subitems', '201'), ('hidden_items', '301'))
    monday = active_monday(still_active('101', 'projects'))
    assert archive.periodic_check(c, monday, limit=1) == 1
    assert archive.periodic_check(c, monday, limit=1) == 1
    assert archive.periodic_check(c, monday, limit=1) == 0


def test_idle_worker_checks_active_items_in_bulk_and_queues_only_archived_ones(archive_db):
    c, _ = archive_db
    known_active(c)
    observe(c, 'hidden_items', '302', state='archived')
    due(c, ('subitems', '201'), ('hidden_items', '302'))
    monday = Monday({'201': still_active('201', 'subitems', '101'), '302': item('302', 'hidden_items', 'archived')})

    assert life.run_once(c, monday) is True

    assert archive.state_row(c, 'subitems', '201')['state_event_key'].startswith('periodic-check:')
    assert [(kind, ident, cause) for kind, ident, cause, _ in queued(c)] == [
        ('reconcile', '302', 'periodic_lifecycle_check')]


def test_periodic_checks_can_be_paused(archive_db, monkeypatch):
    c, _ = archive_db
    known_active(c)
    observe(c, 'hidden_items', '302', state='archived')
    due(c, ('subitems', '201'), ('hidden_items', '302'))
    monkeypatch.setenv('MONDAY_LIFECYCLE_RECHECK_DAYS', '0')
    monday = Monday({})

    assert life.run_once(c, monday) is False

    assert monday.calls == [] and not queued(c)
    assert days_to_next_check(c, 'subitems', '201') < 0


@pytest.mark.parametrize('setting,low,high', [(None, 5.2, 8.8), ('2', 1.4, 2.6), ('not-a-number', 5.2, 8.8)])
def test_next_check_follows_the_configured_interval(archive_db, monkeypatch, setting, low, high):
    c, _ = archive_db
    if setting is not None:
        monkeypatch.setenv('MONDAY_LIFECYCLE_RECHECK_DAYS', setting)
    observe(c, 'subitems', '201', parent='101')
    observe(c, 'projects', '101', state='archived')
    c.execute("UPDATE monday_item_lifecycle SET recheck_after=now()-interval '1 second' WHERE monday_id='101'")
    assert life.schedule_rechecks(c) == 1
    assert low < days_to_next_check(c, 'subitems', '201') < high
    assert low < days_to_next_check(c, 'projects', '101') < high


def test_sync_gate_reads_one_hundred_items_per_call(archive_db, monkeypatch):
    c, dsn = archive_db
    monkeypatch.setenv('SUPABASE_DB_URL', dsn)
    links = {str(2000 + n): str(3000 + n) for n in range(40)}
    monday = Monday({**{i: linked_subitem(i, h) for i, h in links.items()},
                     **{h: item(h, 'hidden_items', 'active') for h in links.values()},
                     '101': item('101', 'projects', 'active')})
    monkeypatch.setattr(compare, 'ComparisonMondayClient', lambda: monday)
    rows = [{'monday_id': i, 'parent_monday_id': '101', 'hidden_item_id': h} for i, h in links.items()]

    assert archive.prepare_sync_rows('subitems', rows) == rows

    # Subitems, parent and sources, read twice: 6 calls, where 10 IDs per call took 18.
    assert sorted(len(call) for call in monday.calls) == [1, 1, 40, 40, 40, 40]
