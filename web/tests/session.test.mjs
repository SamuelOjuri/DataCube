import assert from 'node:assert/strict';
import {test} from 'node:test';
import {webcrypto} from 'node:crypto';
import {createAuth} from '../src/auth.mjs';
import {readDraft, saveDraft, clearDrafts, isolateDrafts} from '../src/drafts.mjs';

function storage() {
  const map = new Map();
  return {get length(){return map.size;}, key:i=>[...map.keys()][i], getItem:k=>map.get(k) ?? null,
    setItem:(k,v)=>map.set(k,v), removeItem:k=>map.delete(k), map};
}
function fixture(shared = storage(), tab = storage(), seed = true) {
  let elapsed = 0;
  const changes = [], calls = [], events = {}, channels = [];
  const apiOrigin = 'https://datacube-bi-analyst.onrender.com';
  if (seed) tab.setItem('datacube.session', JSON.stringify({apiOrigin, token:'S'.repeat(43)}));
  const data = {subject:'verified-user', permissions_version:1,
    absolute_expires_at:new Date(Date.now()+28800000).toISOString(), idle_expires_at:new Date(Date.now()+1800000).toISOString(),
    absolute_expires_in_ms:28800000, idle_expires_in_ms:1800000, idle_timeout_seconds:1800, warning_seconds:120};
  const browser = {crypto:webcrypto, performance:{now:()=>elapsed}, localStorage:shared, sessionStorage:tab,
    location:{hash:'',pathname:'/',search:'',assign:()=>{}}, history:{replaceState:()=>{}},
    setTimeout:()=>1, clearTimeout:()=>{}, document:{visibilityState:'visible',addEventListener:(k,v)=>events[k]=v,removeEventListener:()=>{}},
    addEventListener:(k,v)=>events[k]=v, removeEventListener:()=>{},
    BroadcastChannel:class {constructor(){channels.push(this);}postMessage(value){calls.push(['broadcast',value]);}close(){}},
    fetch:async(url,options)=>{calls.push([url,options]);return new Response(JSON.stringify(data),{headers:{'Content-Type':'application/json'}});}};
  const auth = createAuth({apiOrigin,browser,onChange:(s,r)=>changes.push([s,r])});
  return {auth,browser,data,changes,calls,events,channels,advance:ms=>elapsed+=ms};
}

test('tab restoration validates the bearer with the existing API and omits cookies', async()=>{
  const f=fixture(); await f.auth.restore();
  assert.equal(f.calls[0][1].credentials,'omit');
  assert.equal(f.changes.at(-1)[0].access_token,undefined);
  await f.auth.request('/v1/conversations',{method:'POST',headers:{'Content-Type':'application/json'},body:'{}'});
  assert.equal(f.calls.at(-1)[1].headers.get('Authorization'),'Bearer '+'S'.repeat(43));
  assert.equal(f.calls.at(-1)[1].headers.get('X-CSRF-Token'),null);
  assert.equal(f.browser.localStorage.length,0);
});
test('interaction is leading and throttled; hidden tabs and passive restore never report activity', async()=>{
  const f=fixture(); await f.auth.restore(); await f.auth.activity(); await f.auth.activity();
  assert.equal(f.calls.filter(c=>c[0].endsWith('/auth/activity')).length,1);
  f.advance(60000); f.browser.document.visibilityState='hidden'; await f.auth.activity();
  assert.equal(f.calls.filter(c=>c[0].endsWith('/auth/activity')).length,1);
  f.browser.document.visibilityState='visible'; await f.auth.restore(); await f.auth.activity();
  assert.equal(f.calls.filter(c=>c[0].endsWith('/auth/activity')).length,2);
  assert.equal(f.calls.filter(c=>c[0]==='broadcast').length,2);
});
test('only accepted activity changes deadlines; origin failures preserve session',async()=>{
  const f=fixture(); await f.auth.restore(); const deadline=f.changes.at(-1)[0].idleDeadline;
  for(const code of ['origin_denied']) {
    f.browser.fetch=async()=>new Response(JSON.stringify({detail:code}),{status:403});
    await assert.rejects(f.auth.activity({force:true}),e=>e.code===code);
    assert.equal(f.changes.at(-1)[0].idleDeadline,deadline);
    await assert.rejects(f.auth.request('/v1/conversations'),e=>e.code===code);
    assert.ok(f.changes.at(-1)[0]);
  }
});
test('server idle expiry clears protected requests and preserves stable reason',async()=>{
  const f=fixture(); await f.auth.restore();
  f.browser.fetch=async()=>new Response(JSON.stringify({detail:'session_idle_expired'}),{status:401});
  await assert.rejects(f.auth.activity({force:true}),e=>e.code==='session_idle_expired');
  assert.deepEqual(f.changes.at(-1),[null,'session_idle_expired']);
});
test('reload during pending logout retries the tab credential before restoration',async()=>{
  const shared=storage(), f=fixture(shared); await f.auth.restore();
  f.browser.fetch=async()=>{throw new Error('offline');};
  await assert.rejects(f.auth.logout(),/Server sign-out could not be confirmed/);
  assert.equal(shared.getItem('datacube.logout-pending'),'1');
  const reloaded=fixture(shared,f.browser.sessionStorage,false);
  reloaded.browser.fetch=async(url,opts)=>{reloaded.calls.push([url,opts]);return new Response('{}');};
  assert.equal(await reloaded.auth.restore(),false);
  assert.deepEqual(reloaded.calls.map(c=>c[0]),['https://datacube-bi-analyst.onrender.com/auth/logout']);
  assert.equal(reloaded.calls[0][1].headers.get('Authorization'),'Bearer '+'S'.repeat(43));
  assert.equal(f.browser.sessionStorage.length,0);
  assert.equal(shared.getItem('datacube.logout-pending'),null);
  assert.ok(shared.getItem('datacube.logout-version')); assert.equal(reloaded.changes.at(-1)[0],null);
});
test('restore outage is distinct from sign-out and can be retried',async()=>{
  const f=fixture(); const fetch=f.browser.fetch;
  f.browser.fetch=async()=>new Response('{}',{status:503});
  await assert.rejects(f.auth.restore(),/checking is unavailable/);
  assert.equal(f.changes.length,0);
  f.browser.fetch=fetch; assert.equal(await f.auth.restore(),true);
});
test('multi-tab notifications carry status only and never ping-pong',async()=>{
  const f=fixture(); await f.auth.restore(); await f.auth.activity();
  const message=f.calls.find(c=>c[0]==='broadcast')[1]; assert.deepEqual(message,{type:'activity'});
  f.channels[0].onmessage({data:message}); await new Promise(r=>setImmediate(r));
  assert.equal(f.calls.filter(c=>c[0]==='broadcast').length,1);
  f.browser.localStorage.setItem('datacube.logout-version','remote-logout');
  f.channels[0].onmessage({data:{type:'logout'}}); assert.equal(f.changes.at(-1)[0],null);
});
test('a fresh tab has no credential; malformed and other-API tokens cannot restore',async()=>{
  for(const value of [null,'invalid',JSON.stringify({apiOrigin:'https://evil.test',token:'S'.repeat(43)}),
    JSON.stringify({apiOrigin:'https://datacube-bi-analyst.onrender.com',token:'invalid'})]) {
    const f=fixture(storage(),storage(),false);
    if(value) f.browser.sessionStorage.setItem('datacube.session',value);
    assert.equal(await f.auth.restore(),false); assert.equal(f.calls.length,0);
    assert.equal(f.browser.sessionStorage.length,0);
  }
});
test('restoration requires live server confirmation and deletes an expired stored token',async()=>{
  const f=fixture();
  f.browser.fetch=async()=>new Response(JSON.stringify({detail:'session_idle_expired'}),{status:401});
  assert.equal(await f.auth.restore(),false);
  assert.deepEqual(f.changes.at(-1),[null,'session_idle_expired']);
  assert.equal(f.browser.sessionStorage.length,0);
});
test('new login revokes the prior tab token and never reuses a stored deadline',async()=>{
  const f=fixture(); await f.auth.restore();
  f.browser.fetch=async(url,options)=>{f.calls.push([url,options]);return new Response('{}');};
  await f.auth.login();
  assert.equal(f.calls.at(-1)[0],'https://datacube-bi-analyst.onrender.com/auth/logout');
  assert.equal(f.browser.sessionStorage.getItem('datacube.session'),null);
  assert.equal(f.browser.sessionStorage.getItem('datacube.logout-token'),null);
});
test('a tab that missed logout revokes its cached token instead of restoring it',async()=>{
  const shared=storage(), first=fixture(shared), missed=fixture(shared);
  await first.auth.restore(); await missed.auth.restore();
  await first.auth.logout();
  assert.equal(shared.getItem('datacube.logout-pending'),null);
  const reopened=fixture(shared,missed.browser.sessionStorage,false);
  assert.equal(await reopened.auth.restore(),false);
  assert.deepEqual(reopened.calls.map(c=>c[0]),['https://datacube-bi-analyst.onrender.com/auth/logout']);
  assert.equal(missed.browser.sessionStorage.length,0);
});
test('a delayed logout notification does not end a deliberate later sign-in',async()=>{
  const shared=storage(); shared.setItem('datacube.logout-version','completed-logout');
  const f=fixture(shared);
  // A confirmed later login stores the current logout version.
  f.browser.sessionStorage.setItem('datacube.session',JSON.stringify({apiOrigin:'https://datacube-bi-analyst.onrender.com',token:'S'.repeat(43),logoutVersion:'completed-logout'}));
  await f.auth.restore();
  f.channels[0].onmessage({data:{type:'logout'}});
  assert.equal(f.changes.at(-1)[0].subject,'verified-user');
  assert.equal(f.calls.filter(c=>c[0].endsWith('/auth/logout')).length,0);
});
test('a logout during an in-flight restore cannot stamp the old token with the new version',async()=>{
  const f=fixture();
  f.browser.fetch=async(url,options)=>{
    f.calls.push([url,options]);
    if(url.endsWith('/auth/session')) {
      f.browser.localStorage.setItem('datacube.logout-version','new-logout');
      return new Response(JSON.stringify(f.data));
    }
    return new Response('{}');
  };
  assert.equal(await f.auth.restore(),false);
  assert.equal(f.browser.sessionStorage.length,0);
  assert.equal(f.changes.at(-1)[0],null);
});
test('drafts preserve same-subject text and uncertain keys, bound lifetime and isolate users',()=>{
  const s=storage(), id='10000000-0000-4000-8000-000000000001', key='20000000-0000-4000-8000-000000000001';
  const value={question:'Company draft',reply:'Reply',pending:{status:'submitting',request:{path:`/v1/conversations/${id}/messages`,question:'Sent question',body:{question:'Sent question',idempotency_key:key}}}};
  saveDraft('one',id,value,s);
  assert.equal(readDraft('one',id,s).pending.request.body.idempotency_key,key);
  assert.equal(readDraft('one',id,s).pending.status,'unconfirmed');
  assert.equal(readDraft('two',id,s).question,'');
  isolateDrafts('two',s); assert.equal(s.length,0);
  saveDraft('one',id,value,s); const name=s.key(0); const stale=JSON.parse(s.getItem(name)); stale.savedAt=0; s.setItem(name,JSON.stringify(stale));
  assert.equal(readDraft('one',id,s).question,''); assert.equal(s.length,0);
  saveDraft('one',id,value,s); clearDrafts(s); assert.equal(s.length,0);
  saveDraft('one',id,{...value,question:'x'.repeat(8001)},s); assert.equal(s.length,0);
});
