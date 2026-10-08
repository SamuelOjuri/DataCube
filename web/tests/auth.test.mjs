import assert from 'node:assert/strict';
import {webcrypto} from 'node:crypto';
import {test} from 'node:test';
import {createAuth} from '../src/auth.mjs';

function fixture() {
  const storage = new Map();
  const seen = [];
  const changes = [];
  let timer;
  const session = {access_token:'S'.repeat(43),subject:'user-one',expires_at:new Date(Date.now()+890000).toISOString()};
  const browser = {
    crypto: webcrypto,
    sessionStorage: {setItem: (k,v) => storage.set(k,v),getItem: k => storage.get(k),removeItem: k => storage.delete(k)},
    location: {hash:'',pathname:'/auth/callback',search:'',assign: url => seen.push(['navigate',url])},
    history: {replaceState: (...args) => seen.push(['history',...args])},
    setTimeout: cb => {timer=cb; return 1;},clearTimeout: () => {},
    fetch: async (url, options) => {
      seen.push([url,options]);
      return new Response(JSON.stringify(session), {status:200,headers:{'Content-Type':'application/json'}});
    },
  };
  const auth = createAuth({apiOrigin:'https://api.example.test',browser,onChange:s => changes.push(s)});
  return {auth,browser,storage,seen,changes,session,expire:()=>timer()};
}

async function login(f) {
  await f.auth.login();
  f.browser.location.hash = '#code='+'C'.repeat(43);
  await f.auth.finish();
}

test('PKCE verifier stays in this tab; session is memory-only; URL is immediately scrubbed',async () => {
  const f = fixture();
  await login(f);
  assert.equal(f.storage.size,0);
  const nav = f.seen.find(x => x[0]==='navigate')[1];
  assert.match(nav,/\/auth\/login\?client_challenge=[A-Za-z0-9_-]{43}$/);
  assert.equal(f.seen[1][0],'history');
  const exchange = f.seen.find(x=>x[0].endsWith('/auth/exchange'));
  assert.match(JSON.parse(exchange[1].body).verifier,/^[A-Za-z0-9_-]{43}$/);
  assert.equal(f.changes.at(-1).subject,'user-one');
  assert.equal(f.changes.at(-1).access_token,undefined);
  await f.auth.request('/auth/session');
  assert.equal(f.seen.at(-1)[1].headers.get('Authorization'),'Bearer '+f.session.access_token);
});

test('unsolicited callback cannot sign a different browser into an attacker session',async () => {
  const f = fixture();
  f.browser.location.hash='#code='+'C'.repeat(43);
  await assert.rejects(f.auth.finish(),/could not be completed/);
  assert.equal(f.seen.length,1);
  assert.equal(f.seen[0][0],'history');
});

test('denied and stale login discard pending verifier',async () => {
  for (const denied of [false,true]) {
    const f = fixture();
    await f.auth.login();
    const key = [...f.storage.keys()][0];
    const pending = JSON.parse(f.storage.get(key));
    pending.created=Date.now()-600000;
    f.storage.set(key,JSON.stringify(pending));
    f.browser.location.hash=denied ? '#error=sign_in_failed' : '#code='+'C'.repeat(43);
    await assert.rejects(f.auth.finish(),/could not be completed/);
    assert.equal(f.storage.size,0);
  }
});

test('expiry and access denials clear all client-held session state',async () => {
  for (const status of [401,403]) {
    const f = fixture();
    await login(f);
    f.browser.fetch=async () => new Response('',{status});
    await assert.rejects(f.auth.request('/auth/session'),/session has ended/);
    assert.equal(f.changes.at(-1),null);
    await assert.rejects(f.auth.request('/auth/session'),/sign in/);
  }
  const f = fixture(); await login(f); f.expire();
  await assert.rejects(f.auth.request('/auth/session'),/sign in/);
});

test('logout aborts pending requests and rejects late data even if revocation is unavailable',async () => {
  const f = fixture();
  await login(f);
  let signal, finish;
  f.browser.fetch=async (url,options) => {
    if (url.endsWith('/auth/logout')) throw new Error('offline');
    signal=options.signal;
    return new Promise(resolve=> {finish=resolve;});
  };
  const pending=f.auth.request('/v1/conversations');
  await assert.rejects(f.auth.logout(),/Server sign-out could not be confirmed/);
  assert.equal(signal.aborted,true);
  finish(new Response('private data'));
  await assert.rejects(pending,/session has ended/);
  assert.equal(f.changes.at(-1),null);
});

test('a reconnect uses the active bearer and tokens cannot be sent to arbitrary destinations',async () => {
  const f = fixture(); await login(f);
  await f.auth.request('/v1/runs/123');
  await f.auth.request('/v1/runs/123');
  assert.equal(f.seen.at(-1)[1].headers.get('Authorization'),'Bearer '+f.session.access_token);
  for (const path of ['https://evil.test','//evil.test','/v1/../auth/login','/v1/runs?token=bad']) {
    await assert.rejects(f.auth.request(path),/Unsupported/);
  }
  assert.throws(()=>createAuth({apiOrigin:'https://api.example.test/path',browser:f.browser}),/explicit API/);
});
