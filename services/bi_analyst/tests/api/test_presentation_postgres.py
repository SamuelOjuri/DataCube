"""Phase 6 detail, feedback and export checks under actual restricted roles."""
from decimal import Decimal
from uuid import uuid4

import psycopg
from psycopg.conninfo import make_conninfo
import pytest

from bi_analyst.metrics.compiler import Compiler
from test_workflow_postgres import ROOT, workflow_database, setup, submit, settle, ScriptedProvider
from test_metric_postgres import query

pytestmark = pytest.mark.postgres


@pytest.fixture(autouse=True, scope='module')
def presentation_database(workflow_database):
    admin, dsn = workflow_database
    before = admin.execute("SELECT oid,relacl,relowner FROM pg_class WHERE relnamespace='public'::regnamespace ORDER BY oid").fetchall()
    with admin.transaction():
        admin.execute((ROOT/'src/database/migrations/20261008_007_analyst_presentation.sql').read_text(encoding='utf-8'))
    assert admin.execute("SELECT oid,relacl,relowner FROM pg_class WHERE relnamespace='public'::regnamespace ORDER BY oid").fetchall() == before
    return admin, dsn


@pytest.mark.parametrize('metric_id', [m.id for m in Compiler().catalogue.metrics])
def test_project_detail_uses_saved_plan_and_population(setup, metric_id):
    _, _, _, _, start = setup
    with start() as client:
        result = query(client, metric_id)
        response = client.get(f"/v1/results/{result['id']}/projects", params={'limit':100})
        if result['provenance']['source_population'] == 'hidden_inventory':
            assert response.status_code == 422
            return
        assert response.status_code == 200, response.text
        page = response.json()
        assert not page['has_more']
        assert page['result_id'] == result['id']
        assert 'Live project evidence' in page['limitation']
        assert all(len(row) == len(page['columns']) for row in page['rows'])
        assert all(row[0] != 'p-excluded' for row in page['rows'])
        metric = Compiler().metrics[metric_id]
        if metric.calculation.aggregation == 'sum':
            detail_total = sum(Decimal(r[2]) for r in page['rows'] if r[2] is not None)
            assert detail_total == Decimal(result['provenance']['total']['value'])


def test_detail_filter_limit_offset_and_read_only_authorization(setup):
    admin, _, owner, _, start = setup
    with start() as client:
        result = query(client, dimensions=['category'], filters=[{'dimension':'category','values':['A']}], limit=1)
        path = f"/v1/results/{result['id']}/projects"
        page = client.get(path, params={'limit':1}).json()
        assert len(page['rows']) == 1 and page['has_more']
        assert page['rows'][0][3] == 'A'
        next_page = client.get(path,params={'limit':1,'offset':1}).json()
        assert next_page['rows'][0][0] != page['rows'][0][0]
        assert client.get(path,params={'limit':101}).status_code == 422
        assert client.get(path,params={'offset':-1}).status_code == 422
    other = uuid4()
    admin.execute('INSERT INTO analyst_state.principals(subject,enabled,company_wide) VALUES (%s,true,true)',(other,))
    with start(subject=other) as client:
        for suffix in ('', '/projects', '/export'):
            assert client.get(f"/v1/results/{result['id']}{suffix}").status_code == 404
        assert client.post(f"/v1/results/{result['id']}/feedback",json={'rating':'helpful'}).status_code == 404
    admin.execute('UPDATE analyst_state.principals SET permissions_version=permissions_version+1 WHERE subject=%s',(owner,))
    with start() as client:
        assert client.get(path).status_code == 403
        assert client.post(f"/v1/results/{result['id']}/feedback",json={'rating':'helpful'}).status_code == 403


def test_feedback_is_idempotent_and_rls_protected(setup):
    admin, dsn, owner, _, start = setup
    with start() as client:
        result = query(client)
        path = f"/v1/results/{result['id']}/feedback"
        for rating in ('helpful','helpful','not_helpful'):
            response = client.post(path,json={'rating':rating,'comment':'Needs more context'})
            assert response.status_code == 200, response.text
            assert response.json()['rating'] == rating
        assert client.post(path,json={'rating':'helpful','comment':'x'*2001}).status_code == 422
        assert client.post(path,json={'rating':'helpful','owner_id':str(uuid4())}).status_code == 422
    assert admin.execute('SELECT count(*) AS n FROM analyst_state.result_feedback WHERE result_id=%s',(result['id'],)).fetchone()['n'] == 1
    with psycopg.connect(make_conninfo(dsn,user='bi_analyst_state'),autocommit=True) as conn:
        assert conn.execute('SELECT * FROM analyst_state.result_feedback').fetchall() == []
        with conn.transaction():
            conn.execute("SELECT set_config('bi_analyst.subject',%s,true)",(str(owner),))
            assert len(conn.execute('SELECT * FROM analyst_state.result_feedback').fetchall()) == 1
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                with conn.transaction():
                    conn.execute('UPDATE analyst_state.result_feedback SET owner_id=%s',(uuid4(),))


def test_export_is_stored_rows_and_escapes_untrusted_text(setup):
    admin, _, _, _, start = setup
    with start() as client:
        result = query(client, dimensions=['category'],limit=1)
        # Admin fixture changes only synthetic persisted result data.
        admin.execute("UPDATE analyst_state.results SET rows='[[\"=HYPERLINK(1)\",\"-10.00\"]]'::jsonb WHERE id=%s",(result['id'],))
        response = client.get(f"/v1/results/{result['id']}/export")
        assert response.status_code == 200
        assert response.headers['X-Export-Scope'] == 'stored-result-rows'
        assert "'=HYPERLINK(1)" in response.text
        assert "'-10.00" in response.text


def test_workflows_and_cancel_remain_available_with_schema_seven(setup):
    _, _, _, _, start = setup
    with start() as client:
        run = submit(client)
        assert settle(client, run)['status'] == 'completed'
    with start(ScriptedProvider(delay=1)) as client:
        run = submit(client)
        assert client.post(f'/v1/runs/{run}/cancel').json()['status'] == 'cancelled'


def test_feedback_permission_drift_prevents_startup(setup):
    admin, _, _, _, start = setup
    admin.execute('GRANT UPDATE(owner_id) ON analyst_state.result_feedback TO bi_analyst_state')
    try:
        with pytest.raises(RuntimeError, match='Unsafe feedback'):
            with start():
                pass
    finally:
        admin.execute('REVOKE UPDATE(owner_id) ON analyst_state.result_feedback FROM bi_analyst_state')
