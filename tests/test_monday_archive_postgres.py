from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path

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


def test_normal_sync_missing_source_aborts_without_partial_observations(archive_db, monkeypatch):
    c, dsn = archive_db
    monkeypatch.setenv('SUPABASE_DB_URL', dsn)
    monkeypatch.setattr(compare, 'ComparisonMondayClient',
                        lambda: Monday({'101': item('101', 'projects', 'active')}))
    with pytest.raises(life.ReviewRequired, match='absence'):
        archive.prepare_sync_rows('projects', [{'monday_id': '101'}, {'monday_id': '999'}])
    assert archive.state_row(c, 'projects', '101') is None
    assert archive.state_row(c, 'projects', '999') is None


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
