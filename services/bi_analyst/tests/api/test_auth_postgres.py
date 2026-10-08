"""Real restricted PostgreSQL logins plus mocked Monday HTTPS; no identity override."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from importlib.resources import files
from pathlib import Path
import secrets
import logging
from urllib.parse import parse_qs, urlsplit
from uuid import uuid4

from fastapi.testclient import TestClient
import httpx
import psycopg
import pytest

from bi_analyst.api import create_app
from bi_analyst.auth import challenge
from bi_analyst.auth_store import digest
from bi_analyst.database import Database
from bi_analyst.monday_auth import API_URL, REVOKE_URL, TOKEN_URL
from bi_analyst.settings import Settings

pytestmark = pytest.mark.postgres
ROOT = Path(__file__).resolve().parents[4]
ORIGIN = 'https://analyst.example.test'
BASE = 'https://api.example.test'


@pytest.fixture(scope='module')
def auth_database(database):
    admin, _ = database
    # Existing source ACLs/definitions are untouched by the additive migration.
    snapshot = admin.execute("SELECT oid,relacl,relowner FROM pg_class WHERE relnamespace='public'::regnamespace ORDER BY oid").fetchall()
    with admin.transaction():
        admin.execute((ROOT/'src/database/migrations/20261008_005_analyst_auth.sql').read_text(encoding='utf-8'))
    assert admin.execute("SELECT oid,relacl,relowner FROM pg_class WHERE relnamespace='public'::regnamespace ORDER BY oid").fetchall() == snapshot
    return database


@pytest.fixture
def auth_settings(auth_database, settings):
    admin, _ = auth_database
    admin.execute('DELETE FROM analyst_state.auth_rate_limit')
    return Settings(**{**settings.model_dump(), 'auth_provider': 'monday',
        'monday_client_id': 'client-id', 'monday_client_secret': 'client-secret-marker',
        'monday_account_id': '123', 'monday_redirect_uri': BASE+'/auth/callback',
        'auth_frontend_url': ORIGIN+'/auth/callback'})


@pytest.fixture
def pilot(auth_database):
    admin, _ = auth_database
    subject = uuid4()
    user_id = str(secrets.randbelow(10**15)+1)
    admin.execute('INSERT INTO analyst_state.principals(subject,enabled,company_wide) VALUES (%s,true,true)', (subject,))
    admin.execute("INSERT INTO analyst_state.external_identities VALUES ('monday','123',%s,%s)", (user_id,subject))
    return subject, user_id


@pytest.fixture
def client(auth_settings, pilot):
    app = create_app(auth_settings)
    seen = []
    profile = {'id': pilot[1], 'account': {'id': '123'}, 'enabled': True,
               'is_guest': False, 'is_pending': False, 'is_verified': True}
    behavior = {'me': profile}

    def monday(request):
        import json
        body = json.loads(request.content)
        seen.append((str(request.url), body))
        if str(request.url) == TOKEN_URL:
            assert body['client_secret'] == 'client-secret-marker'
            assert body['grant_type'] == 'authorization_code'
            assert len(body['code_verifier']) == 43
            assert body['redirect_uri'] == BASE+'/auth/callback'
            if behavior.get('token_error'):
                return httpx.Response(400, json={'secret': 'do-not-expose'})
            return httpx.Response(200, json={'access_token': 'monday-access-marker',
                'refresh_token': 'monday-refresh-marker', 'token_type': 'Bearer',
                'scope': behavior.get('scope', 'me:read')})
        if str(request.url) == API_URL:
            assert request.headers['Authorization'] == 'monday-access-marker'
            return httpx.Response(200, json=behavior.get('response', {'data': {'me': behavior['me']}}))
        if str(request.url) == REVOKE_URL:
            return httpx.Response(200, json={'success': not behavior.get('revoke_error')})
        raise AssertionError('Unexpected provider URL')

    with TestClient(app, base_url=BASE) as browser:
        transport = httpx.MockTransport(monday)
        provider_client = httpx.AsyncClient(transport=transport)
        app.state.monday.client = provider_client
        yield browser, behavior, seen
        asyncio.run(provider_client.aclose())


def begin(browser):
    verifier = secrets.token_urlsafe(32)
    response = browser.get('/auth/login', params={'client_challenge': challenge(verifier)}, follow_redirects=False)
    assert response.status_code == 303, response.text
    query = parse_qs(urlsplit(response.headers['location']).query)
    assert query['scope'] == ['me:read'] and query['code_challenge_method'] == ['S256']
    assert len(query['code_challenge'][0]) == 43
    assert 'HttpOnly' in response.headers['set-cookie'] and 'Secure' in response.headers['set-cookie']
    assert 'SameSite=lax' in response.headers['set-cookie']
    return query['state'][0], verifier


def callback(browser, state, **params):
    return browser.get('/auth/callback', params={'state': state, 'code': 'provider-code-marker', **params}, follow_redirects=False)


def exchange(browser, state, verifier):
    return browser.post('/auth/exchange', json={'code': state, 'verifier': verifier}, headers={'Origin': ORIGIN})


def sign_in(browser):
    state, verifier = begin(browser)
    response = callback(browser, state)
    assert response.headers['location'] == ORIGIN+'/auth/callback#code='+state, response.text
    response = exchange(browser, state, verifier)
    assert response.status_code == 200, response.text
    return response.json(), (state, verifier)


def test_real_login_session_logout_and_reconnect(client, pilot, auth_database, auth_settings, caplog):
    caplog.set_level(logging.INFO, logger='bi_analyst.audit')
    browser, _, seen = client
    admin, _ = auth_database
    assert browser.get('/health/ready').json()['identity'] == 'monday'
    assert browser.get('/v1/conversations').status_code == 401
    session, flow = sign_in(browser)
    assert session['subject'] == str(pilot[0])
    token = session['access_token']
    assert len(token) == 43
    headers = {'Authorization': 'Bearer '+token}
    assert [body['token_type_hint'] for url,body in seen if url == REVOKE_URL] == ['access_token','refresh_token']
    assert exchange(browser,*flow).status_code == 400
    created = browser.post('/v1/conversations',json={'title':'Private'},headers=headers)
    assert created.status_code == 201, created.text
    cid = created.json()['id']
    # Reconnect/restart uses the same durable session and owner.
    with TestClient(create_app(auth_settings), base_url=BASE) as restarted:
        assert restarted.get('/v1/conversations/'+cid,headers=headers).status_code == 200
    saved = admin.execute('SELECT token_hash,owner_id FROM analyst_state.sessions WHERE owner_id=%s',(pilot[0],)).fetchone()
    assert saved == {'token_hash': digest(token), 'owner_id': pilot[0]}
    assert browser.post('/auth/logout',headers=headers).status_code == 204
    assert browser.get('/auth/session',headers=headers).status_code == 401
    assert browser.post('/auth/logout',headers=headers).status_code == 204
    new_session, _ = sign_in(browser)
    assert new_session['subject'] == session['subject']
    assert browser.get('/v1/conversations/'+cid,headers={'Authorization':'Bearer '+new_session['access_token']}).status_code == 200
    for secret in ['client-secret-marker','provider-code-marker','monday-access-marker',token]:
        assert secret not in caplog.text
    assert admin.execute("SELECT count(*) AS n FROM analyst_state.audit_events WHERE owner_id=%s AND route='/auth/logout'", (pilot[0],)).fetchone()['n'] == 1


@pytest.mark.parametrize('change', [{'enabled':False},{'is_guest':True},{'is_pending':True},
    {'is_verified':False},{'is_guest':None},{'account':{'id':'999'}},{'id':'untrusted'}, {'id':'9999999999999999999'}])
def test_denied_monday_identity(client, auth_database, change):
    browser, behavior, seen = client
    behavior['me'].update(change)
    state, verifier = begin(browser)
    assert callback(browser,state).headers['location'].endswith('#error=sign_in_failed')
    assert exchange(browser,state,verifier).status_code == 400
    assert len([url for url,_ in seen if url == REVOKE_URL]) == 2


@pytest.mark.parametrize('sql', ['enabled=false', 'company_wide=false'])
def test_datacube_grants_required(client, pilot, auth_database, sql):
    auth_database[0].execute('UPDATE analyst_state.principals SET '+sql+' WHERE subject=%s',(pilot[0],))
    browser, _, _ = client
    state, verifier = begin(browser)
    assert '#error=' in callback(browser,state).headers['location']
    assert exchange(browser,state,verifier).status_code == 400


def test_browser_binding_replay_and_expiry(client, auth_database):
    browser, _, seen = client
    state, verifier = begin(browser)
    cookie = browser.cookies.get('__Host-datacube_oauth')
    browser.cookies.clear()
    assert '#error=' in callback(browser,state).headers['location']
    assert seen == []
    browser.cookies.set('__Host-datacube_oauth',cookie,domain='api.example.test',path='/')
    assert '#error=' in callback(browser,secrets.token_urlsafe(32)).headers['location']
    browser.cookies.set('__Host-datacube_oauth',cookie,domain='api.example.test',path='/')
    assert '#code=' in callback(browser,state).headers['location']
    browser.cookies.set('__Host-datacube_oauth',cookie,domain='api.example.test',path='/')
    assert '#error=' in callback(browser,state).headers['location']
    assert len([url for url,_ in seen if url == TOKEN_URL]) == 1
    assert exchange(browser,state,secrets.token_urlsafe(32)).status_code == 400
    assert browser.post('/auth/exchange',json={'code':state,'verifier':verifier},headers={'Origin':'https://evil.test'}).status_code == 403
    auth_database[0].execute("UPDATE analyst_state.oauth_attempts SET expires_at=now()-interval '1 second' WHERE state_hash=%s",(digest(state),))
    assert exchange(browser,state,verifier).status_code == 400
    state, _ = begin(browser)
    auth_database[0].execute("UPDATE analyst_state.oauth_attempts SET expires_at=now()-interval '1 second' WHERE state_hash=%s",(digest(state),))
    assert '#error=' in callback(browser,state).headers['location']


@pytest.mark.parametrize('behavior_change', [{'token_error':True},{'revoke_error':True},{'scope':'me:read boards:read'},
    {'response':{'data':{'me':None}}},{'response':{'data':[]}},{'response':{'data':{'me':{'account':[]}}}},
    {'response':{'errors':[{'message':'secret-marker'}]}}])
def test_provider_errors_do_not_create_sessions(client, behavior_change):
    browser, behavior, _ = client
    behavior.update(behavior_change)
    state, verifier = begin(browser)
    response = callback(browser,state)
    assert response.headers['location'].endswith('#error=sign_in_failed')
    assert 'secret-marker' not in response.text
    assert exchange(browser,state,verifier).status_code == 400


@pytest.mark.parametrize('params', [{'error':'access_denied'}, {'status':'failed'}])
def test_cancelled_consent_does_not_contact_provider(client, params):
    browser, _, seen = client
    state, _ = begin(browser)
    assert '#error=' in callback(browser,state,**params).headers['location']
    assert seen == []


def test_immediate_revocation_version_change_and_expiration(client, pilot, auth_database):
    browser, _, _ = client
    admin, _ = auth_database
    session, _ = sign_in(browser)
    headers = {'Authorization':'Bearer '+session['access_token']}
    admin.execute('UPDATE analyst_state.principals SET enabled=false WHERE subject=%s',(pilot[0],))
    assert browser.get('/v1/conversations',headers=headers).status_code == 403
    admin.execute('UPDATE analyst_state.principals SET enabled=true,permissions_version=2 WHERE subject=%s',(pilot[0],))
    assert browser.get('/v1/conversations',headers=headers).json()['detail'] == 'permissions_changed'
    session, _ = sign_in(browser)
    headers = {'Authorization':'Bearer '+session['access_token']}
    admin.execute("UPDATE analyst_state.sessions SET created_at=now()-interval '20 minutes',expires_at=now()-interval '10 minutes' WHERE token_hash=%s",(digest(session['access_token']),))
    assert browser.get('/v1/conversations',headers=headers).status_code == 401


def test_cross_user_isolation_with_real_sessions(client, auth_database):
    browser, behavior, _ = client
    first, _ = sign_in(browser)
    one = {'Authorization':'Bearer '+first['access_token']}
    cid = browser.post('/v1/conversations',json={'title':'Private'},headers=one).json()['id']
    rid = browser.post(f'/v1/conversations/{cid}/runs',json={'question':'Private question'},headers=one).json()['id']
    admin, _ = auth_database
    other, user_id = uuid4(), str(secrets.randbelow(10**15)+1)
    admin.execute('INSERT INTO analyst_state.principals VALUES (%s,true,true,1)',(other,))
    admin.execute("INSERT INTO analyst_state.external_identities VALUES ('monday','123',%s,%s)",(user_id,other))
    behavior['me']['id'] = user_id
    second, _ = sign_in(browser)
    two = {'Authorization':'Bearer '+second['access_token']}
    assert browser.get('/v1/conversations',headers=two).json() == []
    assert browser.get('/v1/conversations/'+cid,headers=two).status_code == 404
    assert browser.get('/v1/runs/'+rid,headers=two).status_code == 404
    assert browser.post('/v1/runs/'+rid+'/resume',headers=two).status_code == 404
    for headers in [{'X-User-ID':first['subject']},{'Authorization':'Bearer monday-access-marker'}]:
        assert browser.get('/v1/conversations',headers=headers).status_code == 401
    assert browser.get('/v1/conversations?access_token='+first['access_token']).status_code == 401


def test_handoff_is_atomic_across_replicas(client, auth_settings):
    browser, _, _ = client
    state, verifier = begin(browser)
    callback(browser,state)
    with TestClient(create_app(auth_settings), base_url=BASE) as second:
        with ThreadPoolExecutor(max_workers=2) as threads:
            a = threads.submit(exchange,browser,state,verifier)
            b = threads.submit(exchange,second,state,verifier)
            assert sorted([a.result().status_code,b.result().status_code]) == [200,400]


def test_auth_budget_shared_across_replicas(client, auth_database, auth_settings):
    auth_database[0].execute("INSERT INTO analyst_state.auth_rate_limit VALUES (1,date_trunc('minute',clock_timestamp()),120)")
    browser, _, _ = client
    with TestClient(create_app(auth_settings), base_url=BASE) as second:
        for app in (browser,second):
            response = app.get('/auth/login',params={'client_challenge':challenge(secrets.token_urlsafe(32))})
            assert response.status_code == 429


def test_auth_storage_permissions_and_drift(auth_settings, auth_database, pilot):
    with psycopg.connect(auth_settings.state_dsn.get_secret_value(),autocommit=True) as state:
        for table in ('external_identities','oauth_attempts','sessions'):
            assert state.execute('SELECT * FROM analyst_state.'+table).fetchall() == []
        for command in ["UPDATE analyst_state.external_identities SET subject=gen_random_uuid()",
                        "UPDATE analyst_state.sessions SET expires_at=now()+interval '1 day'",
                        "UPDATE analyst_state.sessions SET owner_id=gen_random_uuid()",
                        "DELETE FROM analyst_state.sessions",
                        "UPDATE analyst_state.oauth_attempts SET client_challenge='bad'"]:
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                state.execute(command)
    admin, _ = auth_database
    assert admin.execute(files('bi_analyst').joinpath('permissions_auth.sql').read_text()).fetchall() == []
    admin.execute('GRANT UPDATE(expires_at) ON analyst_state.sessions TO bi_analyst_state')
    async def check():
        db = Database(auth_settings)
        with pytest.raises(RuntimeError,match='Unsafe authentication'):
            await db.open()
    try:
        asyncio.run(check())
    finally:
        admin.execute('REVOKE UPDATE(expires_at) ON analyst_state.sessions FROM bi_analyst_state')
