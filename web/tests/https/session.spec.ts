import {test, expect, type Page} from '@playwright/test';
import {start, FRONT, API} from './server.mjs';
let server: Awaited<ReturnType<typeof start>>;
test.beforeAll(async()=>{server=await start();});
test.afterAll(async()=>{await server.close();});
test.beforeEach(()=>server.reset());
async function signIn(page:Page) {
  await page.goto(FRONT); await page.getByRole('button',{name:'Continue with Monday'}).click();
  await expect(page.getByRole('button',{name:'Sign out'})).toBeVisible();
}
test('cross-site HTTPS bearer sessions restore on reload and sign out all tabs without cookies',async({page,context})=>{
  await signIn(page);
  expect(await context.cookies(API)).toHaveLength(0);
  expect(await page.evaluate(()=>localStorage.getItem('datacube.session'))).toBeNull();
  expect(await page.evaluate(()=>JSON.parse(sessionStorage.getItem('datacube.session')!).apiOrigin)).toBe(API);
  expect(server.calls.find(c=>c.path==='/auth/exchange')?.authorization).toBeUndefined();
  await page.getByLabel('Your question',{exact:true}).fill('Unsent company text');
  await page.reload(); await expect(page.getByLabel('Your question',{exact:true})).toHaveValue('Unsent company text');
  const tab=await context.newPage(); await tab.goto(FRONT);
  await expect(tab.getByRole('button',{name:'Continue with Monday'})).toBeVisible();
  await signIn(tab);
  await page.getByRole('button',{name:'Sign out'}).click();
  await expect(tab.getByRole('button',{name:'Continue with Monday'})).toBeVisible();
  await expect.poll(()=>server.sessions.size).toBe(0);
  expect(await context.cookies(API)).toHaveLength(0);
  expect(server.calls.find(c=>c.path==='/auth/logout' && c.method==='POST')?.authorization).toMatch(/^Bearer [A-Za-z0-9_-]{43}$/);
  expect(server.calls.filter(c=>c.path==='/auth/session' && c.method==='GET').every(c=>c.authorization && !c.cookie)).toBeTruthy();
});
test('cross-site API calls require an explicit bearer; exchange retains its Origin boundary',async({page})=>{
  await signIn(page);
  const denied=await page.evaluate(async api=>(await fetch(api+'/auth/activity',{credentials:'omit',method:'POST',headers:{'Content-Type':'application/json'},body:'{}'})).status,API);
  expect(denied).toBe(401);
  const form=await page.evaluate(async api=>(await fetch(api+'/auth/activity',{credentials:'omit',method:'POST',body:new URLSearchParams({})})).status,API);
  expect(form).toBe(401);
  // APIRequestContext uses Node DNS rather than Chromium's host resolver rules.
  const noOrigin=await page.request.get('https://127.0.0.1:4474/auth/session'); expect(noOrigin.status()).toBe(401);
  const foreign=await page.request.post('https://127.0.0.1:4474/auth/exchange',{headers:{Origin:'https://foreign.test'},data:{verifier:'V'.repeat(43)}}); expect(foreign.status()).toBe(403);
  await expect(page.getByRole('button',{name:'Sign out'})).toBeVisible();
});
test('idle warning accepts explicit activity; absolute warning requires another login',async({page})=>{
  server.state.idle=110000; await signIn(page);
  await expect(page.getByRole('button',{name:'Stay signed in'})).toBeVisible();
  const absolute=[...server.sessions.values()][0].absolute;
  await page.getByRole('button',{name:'Stay signed in'}).click();
  await expect(page.getByRole('button',{name:'Stay signed in'})).toBeHidden();
  expect([...server.sessions.values()][0].absolute).toBe(absolute);
  server.state.absolute=110000;
  await page.getByRole('button',{name:'Sign out'}).click(); await signIn(page);
  await expect(page.getByRole('button',{name:'Sign in with Monday'})).toBeVisible();
  await page.getByLabel('Your question',{exact:true}).fill('Preserve this at the cap');
  await page.getByRole('button',{name:'Sign in with Monday'}).click();
  await expect(page.getByLabel('Your question',{exact:true})).toHaveValue('Preserve this at the cap');
});
test('offline logout survives reload and revokes privately before showing any workspace',async({page})=>{
  await signIn(page); server.state.offline=true;
  await page.getByRole('button',{name:'Sign out'}).click();
  await expect(page.getByRole('status')).toContainText('Server sign-out could not be confirmed');
  await page.reload(); await expect(page.getByRole('button',{name:'Sign out'})).toBeHidden();
  expect(await page.evaluate(()=>localStorage.getItem('datacube.logout-pending'))).toBe('1');
  server.state.offline=false;
  await page.getByRole('button',{name:'Retry session check'}).click();
  await expect.poll(()=>server.sessions.size).toBe(0);
  expect(server.calls.some(c=>c.path==='/auth/logout' && c.authorization)).toBeTruthy();
  expect(server.calls.some(c=>c.path==='/auth/logout-context')).toBeFalsy();
  expect(await page.evaluate(()=>localStorage.getItem('datacube.logout-pending'))).toBeNull();
});

test('return after sleep revalidates without extending idle and hides an expired workspace',async({page})=>{
  await signIn(page); await page.getByLabel('Your question',{exact:true}).fill('Preserve after sleep');
  await expect.poll(()=>server.calls.filter(c=>c.path==='/auth/activity' && c.method==='POST').length).toBe(1);
  const before=server.calls.filter(c=>c.path==='/auth/activity' && c.method==='POST').length;
  [...server.sessions.values()][0].idle=Date.now()-1;
  await page.evaluate(()=>window.dispatchEvent(new PageTransitionEvent('pageshow',{persisted:true})));
  await expect(page.getByRole('button',{name:'Sign out'})).toBeHidden();
  await expect(page.getByRole('status')).toContainText('inactivity');
  expect(server.calls.filter(c=>c.path==='/auth/activity' && c.method==='POST')).toHaveLength(before);
  await page.getByRole('button',{name:'Continue with Monday'}).click();
  await expect(page.getByLabel('Your question',{exact:true})).toHaveValue('Preserve after sleep');
});

test('another verified subject cannot inherit drafts or pending work',async({page})=>{
  server.state.absolute=110000; await signIn(page);
  await page.getByLabel('Your question',{exact:true}).fill('Private draft for user one');
  server.state.subject='https-user-two';
  await page.getByRole('button',{name:'Sign in with Monday'}).click();
  await expect(page.getByLabel('Your question',{exact:true})).toHaveValue('');
  expect(await page.evaluate(()=>Object.values(sessionStorage).join(' '))).not.toContain('Private draft for user one');
});
