"""Phase 7 on disposable PostgreSQL: access switches, telemetry, retention and load."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from pydantic import SecretStr
import psycopg
from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row
import pytest

from bi_analyst.operations.maintenance import retain, pilot_report
from bi_analyst.operations.preflight import check as preflight
from test_presentation_postgres import presentation_database
from test_workflow_postgres import ROOT, workflow_database, setup, submit, settle, ScriptedProvider
from test_metric_postgres import query, new_run, payload

pytestmark = pytest.mark.postgres


@pytest.fixture(scope='module',autouse=True)
def operations_database(presentation_database):
    admin,dsn = presentation_database
    before = admin.execute("SELECT oid,relacl,relowner FROM pg_class WHERE relnamespace='public'::regnamespace ORDER BY oid").fetchall()
    with admin.transaction():
        admin.execute((ROOT/'src/database/migrations/20261009_008_analyst_operations.sql').read_text())
        admin.execute((ROOT/'src/database/migrations/20261010_009_analyst_reasoning_budget.sql').read_text())
    admin.execute('ALTER ROLE bi_analyst_maintenance LOGIN')
    assert admin.execute("SELECT oid,relacl,relowner FROM pg_class WHERE relnamespace='public'::regnamespace ORDER BY oid").fetchall() == before
    assert admin.execute('SELECT version FROM analyst_state.schema_version').fetchone() == {'version':7}
    return admin,dsn


def maintenance(dsn):
    return psycopg.connect(make_conninfo(dsn,user='bi_analyst_maintenance'),autocommit=True,row_factory=dict_row)


def test_switches_preserve_health_and_require_existing_grants(setup):
    admin, _, owner, config, start = setup
    for update, status in (({'analyst_enabled':False},503),({'pilot_only':True},403)):
        with start(config_override=config.model_copy(update=update)) as client:
            assert client.get('/health/ready').status_code == 200
            assert client.get('/v1/conversations').status_code == status
    pilot = config.model_copy(update={'pilot_only':True,'pilot_subjects':[owner]})
    with start(config_override=pilot) as client:
        assert client.get('/v1/conversations').status_code == 200
        admin.execute('UPDATE analyst_state.principals SET enabled=false WHERE subject=%s',(owner,))
        assert client.get('/v1/conversations').status_code == 403


def test_preflight_checks_real_roles_and_pilot_without_writes(setup):
    admin,_,owner,config,_ = setup
    # Configuration is synthetic; only the loopback fixture is contacted.
    deployed = config.model_copy(update={'environment':'staging','deployment_overlap':2,'connection_budget':8,
        'metric_evaluation_enabled':False,'pilot_only':True,'pilot_subjects':[owner],
        'telemetry_token':SecretStr('x'*32),'model_input_usd_per_million':1,'model_output_usd_per_million':2,
        'auth_provider':'monday','workflow_timeout_seconds':600})
    before = admin.execute('SELECT count(*) AS n FROM analyst_state.audit_events').fetchone()
    assert asyncio.run(preflight(deployed,pilot=True))['passed']
    admin.execute('ALTER TABLE analyst_state.workflow_jobs RENAME CONSTRAINT '
                  'workflow_jobs_remaining_seconds_600_check TO workflow_jobs_remaining_seconds_check')
    try:
        assert 'database_or_privilege_preflight_failed' in asyncio.run(preflight(deployed,pilot=True))['errors']
    finally:
        admin.execute('ALTER TABLE analyst_state.workflow_jobs RENAME CONSTRAINT '
                      'workflow_jobs_remaining_seconds_check TO workflow_jobs_remaining_seconds_600_check')
    assert asyncio.run(preflight(deployed,pilot=True))['passed']
    admin.execute('UPDATE analyst_state.principals SET enabled=false WHERE subject=%s',(owner,))
    assert 'pilot_principal_not_provisioned' in asyncio.run(preflight(deployed,pilot=True))['errors']
    assert admin.execute('SELECT count(*) AS n FROM analyst_state.audit_events').fetchone() == before


def test_disabled_metric_rejected_by_direct_queries_and_graph(setup):
    _, _, _, config, start = setup
    blocked = config.model_copy(update={'disabled_metrics':['order_parent_value']})
    with start(config_override=blocked) as client:
        response = client.post(f'/v1/runs/{new_run(client)}/metric',json=payload())
        assert response.status_code == 503 and response.json()['detail'] == 'metric_disabled'
        result = settle(client,submit(client))
        assert result['status'] in {'failed','awaiting_clarification'} and result['answer'] is None
        catalogue = client.get('/v1/metrics').json()
        assert not next(m for m in catalogue['metrics'] if m['id']=='order_parent_value')['enabled']


def test_telemetry_is_protected_redacted_and_reports_unknowns(setup,caplog):
    _, _, owner, config, start = setup
    token = 'test-telemetry-secret-'+'x'*32
    with start(config_override=config.model_copy(update={'telemetry_token':SecretStr(token)})) as client:
        assert client.get('/ops/metrics').status_code == 404
        assert client.get('/ops/metrics',headers={'Authorization':'Bearer wrong'}).status_code == 404
        result = query(client)
        response = client.get('/ops/metrics',headers={'Authorization':'Bearer '+token})
        assert response.status_code == 200
        assert 'bi_analyst_operations_total{stage="query",outcome="success"} 1' in response.text
        assert 'source_freshness_known{source="ingestion"} 0' in response.text
        assert 'model_cost_configured 0' in response.text
        assert 'coverage{counter="incomplete_order_inputs"} 1' in response.text
        for secret in (token,str(owner),result['id'],'password=','Release qualification'):
            assert secret not in response.text
        assert response.headers['Cache-Control'] == 'no-store'


def test_retention_preview_apply_removes_graph_and_preserves_audit_and_source(setup):
    admin, dsn, owner, _, start = setup
    with start() as client:
        run_id = submit(client)
        assert settle(client,run_id)['status'] == 'completed'
        cid = client.get(f'/v1/runs/{run_id}').json()['conversation_id']
        result = admin.execute('SELECT id FROM analyst_state.results WHERE run_id=%s',(run_id,)).fetchone()['id']
        assert client.post(f'/v1/results/{result}/feedback',json={'rating':'helpful'}).status_code == 200
    for table, key, value in (('conversations','id',cid),('runs','conversation_id',cid),('results','run_id',run_id)):
        admin.execute(f"UPDATE analyst_state.{table} SET created_at=now()-interval '40 days' WHERE {key}=%s",(value,))
    admin.execute("UPDATE analyst_state.workflow_jobs SET segment_started_at=now()-interval '40 days' WHERE run_id=%s",(run_id,))
    source = admin.execute('SELECT * FROM public.projects ORDER BY monday_id').fetchall()
    audit_count = admin.execute('SELECT count(*) AS n FROM analyst_state.audit_events').fetchone()['n']
    with maintenance(dsn) as conn:
        preview = retain(conn)
        assert preview['counts']['conversations'] == 1 and not preview['applied']
        assert preview['counts']['checkpoints'] > 0
        assert conn.execute('SELECT 1 FROM analyst_state.conversations WHERE id=%s',(cid,)).fetchone()
        report = pilot_report(conn)
        assert 'helpful' in str(report)
        applied = retain(conn,apply=True)
        assert applied['counts'] == preview['counts']
        assert not conn.execute('SELECT 1 FROM analyst_state.conversations WHERE id=%s',(cid,)).fetchone()
        assert not conn.execute('SELECT 1 FROM analyst_state.checkpoints WHERE thread_id=%s',(str(run_id),)).fetchone()
        assert retain(conn,apply=True)['counts']['conversations'] == 0
        for statement in ('SELECT * FROM public.projects', 'DELETE FROM public.projects',
                          'UPDATE analyst_state.runs SET question=question','SELECT * FROM analyst_state.external_identities',
                          'DELETE FROM analyst_state.principals'):
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                conn.execute(statement)
    assert admin.execute('SELECT * FROM public.projects ORDER BY monday_id').fetchall() == source
    assert admin.execute('SELECT count(*) AS n FROM analyst_state.audit_events').fetchone()['n'] == audit_count


def test_retention_preserves_recent_activity_and_live_lease(setup):
    admin, dsn, _, _, start = setup
    with start(ScriptedProvider(delay=0.2)) as client:
        run_id = submit(client)
        cid = client.get(f'/v1/runs/{run_id}').json()['conversation_id']
        admin.execute("UPDATE analyst_state.conversations SET created_at=now()-interval '40 days' WHERE id=%s",(cid,))
        with maintenance(dsn) as conn:
            assert retain(conn,apply=True)['counts']['conversations'] == 0
        assert settle(client,run_id)['status'] == 'completed'


def test_concurrent_queries_and_pool_failure_recover_without_data_leak(setup):
    _, _, _, _, start = setup
    with start() as client:
        runs = [new_run(client) for _ in range(4)]
        with ThreadPoolExecutor(max_workers=4) as pool:
            responses = list(pool.map(lambda rid:client.post(f'/v1/runs/{rid}/metric',json=payload()),runs))
        assert all(r.status_code in {201,429} for r in responses)
        assert any(r.status_code == 201 for r in responses)
        assert client.app_instance.state.metrics.active == 0
        assert query(client)['provenance']['total']['value'] == '350.00'
        async def broken():
            raise psycopg.OperationalError('SENSITIVE_DSN password=secret')
        client.app_instance.state.database.ready = broken
        response = client.get('/health/ready')
        assert response.status_code == 503
        assert 'SENSITIVE' not in response.text and 'password' not in response.text


def test_retention_refuses_privilege_drift_and_expires_auth_with_separate_audit_window(setup):
    admin,dsn,owner,_,_ = setup
    token = uuid4().hex * 2
    admin.execute("""INSERT INTO analyst_state.sessions(token_hash,owner_id,permissions_version,created_at,expires_at)
        VALUES(%s,%s,1,now()-interval '2 days',now()-interval '2 days'+interval '15 minutes')""",(token,owner))
    audit_id = uuid4()
    admin.execute("""INSERT INTO analyst_state.audit_events(id,request_id,owner_id,route,method,status,created_at)
        VALUES(%s,%s,%s,'/v1/results/{result_id}','GET',200,now()-interval '100 days')""",(audit_id,uuid4(),owner))
    with maintenance(dsn) as conn:
        admin.execute('GRANT EXECUTE ON FUNCTION public.forbidden_write() TO bi_analyst_maintenance')
        try:
            with pytest.raises(ValueError,match='function privilege'):
                retain(conn,apply=True)
        finally:
            admin.execute('REVOKE EXECUTE ON FUNCTION public.forbidden_write() FROM bi_analyst_maintenance')
        admin.execute('GRANT SELECT ON public.projects TO bi_analyst_maintenance')
        try:
            with pytest.raises(ValueError,match='privileges'):
                retain(conn,apply=True)
        finally:
            admin.execute('REVOKE SELECT ON public.projects FROM bi_analyst_maintenance')
        assert retain(conn,batch=1)['batch_full']
        report = retain(conn,apply=True)
        assert report['counts']['sessions'] == 1 and report['counts']['audit_events'] == 1
    assert not admin.execute('SELECT 1 FROM analyst_state.sessions WHERE token_hash=%s',(token,)).fetchone()
    assert not admin.execute('SELECT 1 FROM analyst_state.audit_events WHERE id=%s',(audit_id,)).fetchone()
