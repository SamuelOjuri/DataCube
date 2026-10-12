"""Cross-site bearer sessions with restricted roles, OAuth handoff and database time."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from importlib.resources import files
import secrets
import time
from threading import Event
from uuid import uuid4

from fastapi.testclient import TestClient
import psycopg
from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row
import pytest
from pydantic import SecretStr

from bi_analyst.api import create_app
from bi_analyst.auth_store import digest
from bi_analyst.database import Database
from bi_analyst.settings import Settings
from bi_analyst.operations.maintenance import retain
from test_auth_postgres import ROOT, BASE, ORIGIN, client, pilot, sign_in, begin, callback, exchange
from test_workflow_postgres import ScriptedProvider

pytestmark = pytest.mark.postgres


@pytest.fixture(scope='module')
def auth_database(database):
    admin, _ = database
    snapshot = admin.execute("SELECT oid,relacl,relowner FROM pg_class WHERE relnamespace='public'::regnamespace ORDER BY oid").fetchall()
    for number in (4, 5):
        with admin.transaction():
            admin.execute(next((ROOT/'src/database/migrations').glob(f'*_00{number}_*.sql')).read_text())
    owner, token = uuid4(), secrets.token_urlsafe(32)
    admin.execute('INSERT INTO analyst_state.principals VALUES (%s,true,true,1)', (owner,))
    original = admin.execute("""INSERT INTO analyst_state.sessions(token_hash,owner_id,permissions_version,expires_at)
        VALUES (%s,%s,1,statement_timestamp()+interval '15 minutes') RETURNING expires_at""", (digest(token),owner)).fetchone()
    for number in (6, 7, 8, 9, 10):
        with admin.transaction():
            admin.execute(next((ROOT/'src/database/migrations').glob(f'*_{number:03}_*.sql')).read_text())
    legacy = admin.execute('SELECT * FROM analyst_state.sessions WHERE token_hash=%s', (digest(token),)).fetchone()
    assert legacy['expires_at'] == original['expires_at']
    assert legacy['last_active_at'] == legacy['created_at']
    assert admin.execute("SELECT oid,relacl,relowner FROM pg_class WHERE relnamespace='public'::regnamespace ORDER BY oid").fetchall() == snapshot
    assert admin.execute('SELECT version FROM analyst_state.schema_version').fetchone() == {'version':8}
    return database


@pytest.fixture
def auth_settings(auth_database, settings):
    auth_database[0].execute('DELETE FROM analyst_state.auth_rate_limit')
    return Settings(**{**settings.model_dump(), 'auth_provider':'monday', 'requests_per_minute':600,
        'session_seconds':28800, 'session_idle_seconds':1800, 'cors_origins':[ORIGIN],
        'monday_client_id':'client-id', 'monday_client_secret':'client-secret-marker',
        'monday_account_id':'123', 'monday_redirect_uri':BASE+'/auth/callback',
        'auth_frontend_url':ORIGIN+'/auth/callback'})


def signed(client):
    browser = client[0]
    session, _ = sign_in(browser)
    return browser, session, {'Origin':ORIGIN, 'Authorization':'Bearer '+session['access_token']}


def test_bearer_restore_on_existing_urls_is_read_only_and_survives_restart(client, auth_database, auth_settings):
    browser, session, headers = signed(client)
    token = session['access_token']
    assert 'csrf_token' not in session and '__Host-datacube_session' not in browser.cookies
    assert 0 < session['absolute_expires_in_ms'] <= 28800000
    assert 0 < session['idle_expires_in_ms'] <= 1800000
    row = auth_database[0].execute('SELECT * FROM analyst_state.sessions WHERE token_hash=%s', (digest(token),)).fetchone()
    restore = browser.get('/auth/session', headers=headers)
    assert restore.status_code == 200 and restore.json()['subject'] == session['subject']
    assert 'access_token' not in restore.json()
    assert auth_database[0].execute('SELECT last_active_at FROM analyst_state.sessions WHERE token_hash=%s', (digest(token),)).fetchone()['last_active_at'] == row['last_active_at']
    with TestClient(create_app(auth_settings), base_url=BASE) as restarted:
        assert restarted.get('/auth/session',headers=headers).status_code == 200
    newer, _ = sign_in(browser)
    assert newer['access_token'] != token
    assert browser.get('/auth/session',headers=headers).status_code == 200


@pytest.mark.parametrize('origin', [None, 'null', 'https://retired.example.test', 'https://foreign.test'])
def test_exchange_still_requires_the_configured_frontend_origin(client, origin):
    browser = client[0]
    state, verifier = begin(browser); callback(browser,state)
    r = browser.post('/auth/exchange',json={'code':state,'verifier':verifier},headers={'Origin':origin} if origin else {})
    assert r.status_code == 403 and r.json() == {'detail':'origin_denied'}


def test_bearer_preflight_mutation_shape_and_cookie_only_rejection(client):
    browser, session, headers = signed(client)
    assert browser.post('/auth/activity',json={'last_active_at':'2099-01-01'},headers=headers).status_code == 422
    r = browser.options('/auth/activity',headers={'Origin':ORIGIN,'Access-Control-Request-Method':'POST','Access-Control-Request-Headers':'content-type,authorization'})
    assert r.status_code == 200 and r.headers['access-control-allow-origin'] == ORIGIN
    assert 'access-control-allow-credentials' not in r.headers
    token = session['access_token']
    browser.cookies.set('__Host-datacube_session', token)
    assert browser.get('/auth/session',headers={'Origin':ORIGIN}).status_code == 401
    assert browser.get('/auth/session',headers=[('Authorization','Bearer '+token),('Authorization','Bearer '+token)]).status_code == 401
    browser.cookies.clear()
    assert browser.get('/auth/session',headers={'Authorization':'Bearer '+token}).status_code == 200
    assert browser.get('/auth/logout-context').status_code == 404


def test_activity_extends_idle_only_and_uses_principal_budget(client, auth_database):
    browser, session, headers = signed(client)
    admin = auth_database[0]; token = session['access_token']
    admin.execute("UPDATE analyst_state.sessions SET created_at=now()-interval '1 hour',last_active_at=now()-interval '20 minutes',expires_at=now()+interval '7 hours' WHERE token_hash=%s", (digest(token),))
    before = admin.execute('SELECT expires_at,last_active_at FROM analyst_state.sessions WHERE token_hash=%s', (digest(token),)).fetchone()
    # Auth/session traffic must not consume the pre-auth service budget.
    admin.execute("UPDATE analyst_state.auth_rate_limit SET requests=1000,window_start=date_trunc('minute',clock_timestamp())")
    response = browser.post('/auth/activity',json={},headers=headers)
    assert response.status_code == 200, response.text
    after = admin.execute('SELECT expires_at,last_active_at FROM analyst_state.sessions WHERE token_hash=%s', (digest(token),)).fetchone()
    assert after['expires_at'] == before['expires_at'] and after['last_active_at'] > before['last_active_at']
    admin.execute("INSERT INTO analyst_state.rate_limits VALUES (%s,date_trunc('minute',clock_timestamp()),600) ON CONFLICT(owner_id) DO UPDATE SET requests=600,window_start=EXCLUDED.window_start", (session['subject'],))
    assert browser.post('/auth/activity',json={},headers=headers).status_code == 429
    assert admin.execute('SELECT last_active_at FROM analyst_state.sessions WHERE token_hash=%s', (digest(token),)).fetchone()['last_active_at'] == after['last_active_at']


@pytest.mark.parametrize('policy,reason', [
    ("created_at=now()-interval '1 hour',last_active_at=now()-interval '31 minutes',expires_at=now()+interval '7 hours'", 'session_idle_expired'),
    ("created_at=now()-interval '9 hours',last_active_at=now()-interval '9 hours',expires_at=now()-interval '1 hour'", 'session_absolute_expired'),
    ("revoked_at=now()", 'session_revoked')])
def test_expiry_cannot_be_revived(client, auth_database, policy, reason):
    browser, session, headers = signed(client); token = session['access_token']
    admin = auth_database[0]
    admin.execute('UPDATE analyst_state.sessions SET '+policy+' WHERE token_hash=%s', (digest(token),))
    before = admin.execute('SELECT last_active_at FROM analyst_state.sessions WHERE token_hash=%s', (digest(token),)).fetchone()
    r = browser.post('/auth/activity',json={},headers=headers)
    assert r.status_code == 401 and r.json() == {'detail':reason}
    assert 'set-cookie' not in r.headers
    assert admin.execute('SELECT last_active_at FROM analyst_state.sessions WHERE token_hash=%s', (digest(token),)).fetchone() == before


def test_withdrawn_access_can_still_revoke_idempotently(client, auth_database, pilot):
    browser, _, headers = signed(client)
    auth_database[0].execute('UPDATE analyst_state.principals SET enabled=false,permissions_version=2 WHERE subject=%s', (pilot[0],))
    assert browser.get('/auth/session',headers=headers).status_code == 403
    assert browser.post('/auth/logout',json={},headers=headers).status_code == 204
    assert browser.post('/auth/logout',json={},headers=headers).status_code == 204


def test_activity_logout_and_duplicate_activity_across_replicas(client, auth_database, auth_settings):
    browser, session, headers = signed(client); token = session['access_token']
    with TestClient(create_app(auth_settings),base_url=BASE) as replica:
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(b.post,'/auth/activity',json={},headers=headers) for b in (browser,replica)]
            assert [f.result().status_code for f in futures] == [200,200]
            activity = pool.submit(browser.post,'/auth/activity',json={},headers=headers)
            logout = pool.submit(replica.post,'/auth/logout',json={},headers=headers)
            assert activity.result().status_code in (200,401) and logout.result().status_code == 204
    row = auth_database[0].execute('SELECT * FROM analyst_state.sessions WHERE token_hash=%s', (digest(token),)).fetchone()
    assert row['revoked_at'] is not None
    assert browser.post('/auth/activity',json={},headers=headers).status_code == 401


def test_schema_audit_exact_permissions_and_maintenance(auth_database, auth_settings):
    admin, dsn = auth_database
    audit = files('bi_analyst').joinpath('permissions_sessions.sql').read_text()
    embedded = (ROOT/'src/database/migrations/20261012_010_analyst_session_management.sql').read_text().split('-- BEGIN SHARED AUDIT\n')[1].split('-- END SHARED AUDIT')[0].strip()
    assert audit.strip().rstrip(';') == embedded and admin.execute(audit).fetchall() == []
    with psycopg.connect(auth_settings.state_dsn.get_secret_value(),autocommit=True) as state:
        assert state.execute('SELECT * FROM analyst_state.sessions').fetchall() == []
        for name in ('expires_at','created_at','owner_id','token_hash','permissions_version','idle_timeout_seconds'):
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                state.execute(f'UPDATE analyst_state.sessions SET {name}={name}')
    async def check():
        db = Database(auth_settings)
        try: await db.open(); await db.ready()
        finally: await db.close()
    asyncio.run(check())
    admin.execute('GRANT UPDATE(idle_timeout_seconds) ON analyst_state.sessions TO bi_analyst_state')
    try:
        assert admin.execute(audit).fetchall()
        with pytest.raises(RuntimeError,match='Unsafe analyst effective privileges'): asyncio.run(check())
    finally: admin.execute('REVOKE UPDATE(idle_timeout_seconds) ON analyst_state.sessions FROM bi_analyst_state')
    admin.execute('REVOKE UPDATE(last_active_at) ON analyst_state.sessions FROM bi_analyst_state')
    try:
        assert any(v['issue']=='session_missing_update' for v in admin.execute(audit).fetchall())
    finally: admin.execute('GRANT UPDATE(last_active_at) ON analyst_state.sessions TO bi_analyst_state')
    admin.execute('ALTER ROLE bi_analyst_maintenance LOGIN')
    source = admin.execute('SELECT * FROM public.projects ORDER BY monday_id').fetchall()
    with psycopg.connect(make_conninfo(dsn,user='bi_analyst_maintenance'),autocommit=True,row_factory=dict_row) as conn:
        preview = retain(conn)
        applied = retain(conn,apply=True)
        assert applied['counts'] == preview['counts']
    assert admin.execute('SELECT * FROM public.projects ORDER BY monday_id').fetchall() == source


def test_late_activity_rechecks_after_session_lock(client, auth_database):
    browser, session, headers = signed(client); token = session['access_token']
    admin = auth_database[0]
    with ThreadPoolExecutor(max_workers=1) as pool:
        with admin.transaction():
            admin.execute('SELECT 1 FROM analyst_state.sessions WHERE token_hash=%s FOR UPDATE', (digest(token),))
            pending = pool.submit(browser.post,'/auth/activity',json={},headers=headers)
            deadline = time.monotonic()+0.8
            while True:
                admin.execute('SELECT pg_stat_clear_snapshot()')
                if admin.execute("""SELECT 1 FROM pg_stat_activity WHERE application_name='datacube-bi-analyst'
                    AND wait_event_type='Lock' AND query LIKE '%%FROM analyst_state.sessions%%'""").fetchone():
                    break
                assert time.monotonic()<deadline, 'Activity did not reach the held session lock'
                time.sleep(.01)
            admin.execute("""UPDATE analyst_state.sessions SET created_at=now()-interval '1 hour',
                last_active_at=now()-interval '31 minutes',expires_at=now()+interval '7 hours' WHERE token_hash=%s""", (digest(token),))
        response = pending.result()
    assert response.status_code == 401 and response.json()['detail'] == 'session_idle_expired'


@pytest.mark.parametrize('change,reason', [
    ("created_at=now()-interval '1 hour',last_active_at=now()-interval '31 minutes',expires_at=now()+interval '7 hours'", 'session_idle_expired'),
    ("created_at=now()-interval '9 hours',last_active_at=now()-interval '9 hours',expires_at=now()-interval '1 hour'", 'session_absolute_expired'),
    ("revoked_at=now()", 'session_revoked'),
])
def test_stream_revalidates_without_activity_or_cancelling_durable_work(client, auth_database, change, reason):
    browser, session, headers = signed(client); token=session['access_token']
    app=browser.app; app.state.settings.workflow_enabled=True
    app.state.workflow.provider=ScriptedProvider(delay=2)
    cid=browser.post('/v1/conversations',json={'title':'Streaming'},headers=headers).json()['id']
    submitted=browser.post(f'/v1/conversations/{cid}/messages',json={'question':'Show stored order value','idempotency_key':str(uuid4())},headers=headers)
    assert submitted.status_code == 202, submitted.text
    run_id=submitted.json()['run_id']
    snapshot=app.state.workflow.jobs.snapshot; checked=Event()
    async def observe(*args):
        value=await snapshot(*args); checked.set(); return value
    app.state.workflow.jobs.snapshot=observe
    admin=auth_database[0]
    before=admin.execute('SELECT last_active_at FROM analyst_state.sessions WHERE token_hash=%s',(digest(token),)).fetchone()
    with ThreadPoolExecutor(max_workers=1) as pool:
        future=pool.submit(browser.get,f'/v1/runs/{run_id}/events',headers=headers)
        assert checked.wait(5)
        assert admin.execute('SELECT last_active_at FROM analyst_state.sessions WHERE token_hash=%s',(digest(token),)).fetchone() == before
        admin.execute('UPDATE analyst_state.sessions SET '+change+' WHERE token_hash=%s',(digest(token),))
        response=future.result(timeout=5)
    assert '"status":"access_changed"' in response.text
    assert admin.execute('SELECT status FROM analyst_state.workflow_jobs WHERE run_id=%s',(run_id,)).fetchone()['status'] != 'cancelled'
    assert browser.get(f'/v1/runs/{run_id}/workflow',headers=headers).json()['detail'] == reason


def test_exchange_audit_failure_does_not_release_credential(client, monkeypatch):
    browser=client[0]; state, verifier=begin(browser); callback(browser,state)
    async def unavailable(*args): raise RuntimeError('audit down')
    monkeypatch.setattr(browser.app.state.store,'audit',unavailable)
    response=exchange(browser,state,verifier)
    assert response.status_code == 503 and response.json() == {'detail':'audit_unavailable'}
    assert 'set-cookie' not in response.headers and 'access_token' not in response.json()


def test_operational_token_retains_its_independent_contract(client):
    browser, _, _ = signed(client)
    browser.app.state.settings.telemetry_token=SecretStr('independent-telemetry-marker-'+'t'*32)
    response=browser.get('/ops/metrics',headers={'Authorization':'Bearer '+browser.app.state.settings.telemetry_token.get_secret_value()})
    assert response.status_code == 200 and 'bi_analyst_' in response.text
    assert 'independent-telemetry-marker-' not in response.text
