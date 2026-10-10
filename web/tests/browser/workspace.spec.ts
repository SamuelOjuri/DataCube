import {test,expect} from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';
import {fixture,signIn,thread,runId} from './fixture';
import {readFile,readdir} from 'node:fs/promises';

test('production assets exclude backend secrets and test fixtures',async()=>{
  for(const name of await readdir('dist/assets')) {
    const contents=await readFile('dist/assets/'+name,'utf8');
    expect(contents).not.toContain('phase6-backend-secret-sentinel');
    expect(contents).not.toContain('synthetic-user');
  }
});

for (const skew of [-3000,3600000]) {
  test(`sign-in survives a browser clock offset of ${skew}ms`,async({page})=>{
    await page.clock.setFixedTime(Date.now()+skew);
    await fixture(page);
    await signIn(page);
    await expect(page.getByLabel('Your question',{exact:true})).toBeVisible();
    await page.getByRole('button',{name:'Sign out'}).click();
    await expect(page.getByRole('button',{name:'Continue with Monday'})).toBeVisible();
  });
}

test('Tapered Plus styling is consistent across sign-in, workspace, controls and charts',async({page})=>{
  const fontLicense=await readFile('dist/open-sans-license.txt','utf8');
  expect(fontLicense.replace(/\r\n/g,'\n')).toBe((await readFile('node_modules/@fontsource/open-sans/LICENSE','utf8')).replace(/\r\n/g,'\n'));
  await fixture(page,{existing:true});
  const fontRequests:string[]=[];
  page.on('request',request=>{if(/\.(woff2?|ttf)(\?|$)/.test(request.url())) fontRequests.push(request.url());});
  await page.goto(`/conversations/${thread}`);
  const login=page.getByRole('button',{name:'Continue with Monday'});
  await expect(login).toBeEnabled();
  await page.evaluate(()=>document.fonts.ready);
  await expect(page.locator('body')).toHaveCSS('color','rgb(38, 38, 38)');
  await expect(page.locator('body')).toHaveCSS('font-family',/^"Open Sans"/);
  await expect(page.locator('.brand')).toHaveCSS('color','rgb(147, 31, 31)');
  await expect(login).toHaveCSS('background-color','rgb(147, 31, 31)');
  await login.hover();
  await expect(login).toHaveCSS('background-color','rgb(105, 22, 22)');
  await login.focus();
  await expect(login).toHaveCSS('outline-color','rgb(147, 31, 31)');
  await expect(login).toHaveCSS('outline-style','solid');
  expect((await new AxeBuilder({page}).analyze()).violations).toEqual([]);
  expect(fontRequests.length).toBeGreaterThan(0);
  expect(fontRequests.every(url=>new URL(url).origin==='http://127.0.0.1:4173')).toBe(true);
  expect(await page.evaluate(()=>Array.from(document.fonts).some(font=>font.family==='Open Sans' && font.status==='loaded'))).toBe(true);
  await login.click();
  await expect(page.getByRole('heading',{name:'Order Value (parent mirror)'})).toBeVisible();
  await expect(page.locator('aside')).toHaveCSS('background-color','rgb(247, 247, 247)');
  await expect(page.locator('nav a[aria-current]')).toHaveCSS('color','rgb(147, 31, 31)');
  await expect(page.locator('.total strong')).toHaveCSS('color','rgb(147, 31, 31)');
  await expect(page.locator('.secondary').first()).toHaveCSS('border-top-color','rgb(147, 31, 31)');
  await expect(page.getByRole('button',{name:'Ask analyst'})).toHaveCSS('background-color','rgb(147, 31, 31)');
  await expect(page.locator('.chart svg')).toBeVisible();
  const bars=page.locator('.chart svg .mark-rect.role-mark path');
  await expect(bars).toHaveCount(12);
  await expect(bars.first()).toHaveAttribute('fill','#931f1f');
  await expect(page.locator('.chart svg text').first()).toHaveAttribute('font-family','Open Sans');
  await page.getByRole('button',{name:'+ New conversation',exact:true}).click();
  const suggestion=page.locator('.suggestion').first();
  await expect(suggestion).toHaveCSS('background-color','rgb(255, 255, 255)');
  await suggestion.hover();
  await expect(suggestion).toHaveCSS('background-color','rgb(248, 237, 237)');
  await expect(suggestion).toHaveCSS('border-top-color','rgb(147, 31, 31)');
});

test('sign-in, streaming replay, saved results, charts, tables, CSV, detail, feedback and accessibility',async ({page})=>{
  const state=await fixture(page,{disconnect:true});
  const errors:string[]=[]; page.on('pageerror',e=>errors.push(e.message));
  await signIn(page);
  expect((await new AxeBuilder({page}).analyze()).violations).toEqual([]);
  await page.getByLabel('Your question',{exact:true}).fill('Show stored Order Value by category.');
  await page.getByRole('button',{name:'Ask analyst'}).click();
  await expect(page.getByRole('heading',{name:'Order Value (parent mirror)'})).toBeVisible();
  await expect(page.locator('.chart svg')).toBeVisible();
  await page.getByText('Definitions and source details',{exact:true}).click();
  await expect(page.getByText('Verified parent mirror.',{exact:true})).toBeVisible();
  expect(state.events).toContain('?after=1');
  await expect(page.getByText('12 of 12 groups · Complete requested dataset · no sampling',{exact:true})).toBeVisible();
  await page.getByRole('button',{name:'Next rows'}).click();
  await expect(page.getByText('Page 2 of 2',{exact:false})).toBeVisible();
  await page.getByRole('button',{name:'value (GBP)'}).click();
  const download=page.waitForEvent('download');await page.getByRole('button',{name:'Export 12 stored rows (CSV)'}).click();
  expect((await download).suggestedFilename()).toMatch(/datacube-.*\.csv/);
  await page.getByRole('button',{name:'Explore contributing projects'}).click();
  await expect(page.getByText('project-1',{exact:true})).toBeVisible();
  await page.getByText('Give feedback',{exact:true}).click();
  await page.getByLabel('Optional comment').fill('Useful detail');
  await page.getByRole('button',{name:'Helpful',exact:true}).click();
  await expect(page.getByText('Thank you. Your feedback has been saved.')).toBeVisible();
  expect(state.feedback[0].comment).toBe('Useful detail');
  expect((await new AxeBuilder({page}).analyze()).violations).toEqual([]);
  await page.screenshot({path:'../outputs/phase6-desktop.png',fullPage:true});
  expect(errors).toEqual([]);
});

test('clarification retry retains the same idempotency key and follow-ups name the previous run',async({page})=>{
  const state=await fixture(page,{clarify:true});state.failReply=true;
  await signIn(page);
  await page.getByLabel('Your question',{exact:true}).fill('Show order value');await page.getByRole('button',{name:'Ask analyst'}).click();
  await page.getByRole('button',{name:'All stored history',exact:true}).click();await page.getByRole('button',{name:'Send clarification'}).click();
  await page.getByRole('button',{name:'Retry submission'}).click();
  await expect(page.getByRole('heading',{name:'Order Value (parent mirror)'})).toBeVisible();
  expect(state.replies).toHaveLength(2);expect(state.replies[0]).toEqual(state.replies[1]);
  await page.getByLabel('Ask a follow-up or a new question').fill('Only category A');await page.getByRole('button',{name:'Ask analyst'}).click();
  expect(state.submissions[1].follow_up_to).toBe(runId);
});

test('cancellation and retry create a new run while history links recover persisted answers',async({page})=>{
  const state=await fixture(page,{hold:true});await signIn(page);
  await page.getByLabel('Your question',{exact:true}).fill('Show orders');await page.getByRole('button',{name:'Ask analyst'}).click();
  await page.getByRole('button',{name:'Cancel run'}).click();await expect(page.getByText('This run was cancelled.')).toBeVisible();
  await page.getByRole('button',{name:'Retry question'}).click();
  expect(state.submissions).toHaveLength(2);expect(state.submissions[1].idempotency_key).not.toBe(state.submissions[0].idempotency_key);
});

test('an interrupted saved run can be retried and displays the completed answer',async({page})=>{
  const state=await fixture(page,{existing:true});
  state.runs[0].status='interrupted';
  state.workflow[runId].status='interrupted';
  state.workflow[runId].answer=null;
  await signIn(page,`/conversations/${thread}`);
  await expect(page.getByText('Execution was interrupted. Retry to start a new run with current access and data.')).toBeVisible();
  await page.getByRole('button',{name:'Retry question'}).click();
  await expect(page.getByRole('heading',{name:'Order Value (parent mirror)'})).toBeVisible();
  await expect(page.locator('.chart svg')).toBeVisible();
  expect(state.submissions).toHaveLength(1);
  expect(state.submissions[0].question).toBe('Show stored Order Value by category.');
  expect(state.submissions[0].follow_up_to).toBeUndefined();
  expect(state.runs[0].id).not.toBe(runId);
  expect(state.workflow[runId].status).toBe('interrupted');
  await expect(page.getByText('Scope changed from the previous answer')).toHaveCount(0);
  await page.screenshot({path:'../outputs/analyst-interrupted-retry.png',fullPage:true});
});

test('deep links survive reauthentication, expired sessions clear results, sign-out clears the workspace',async({page})=>{
  const state=await fixture(page,{existing:true});await signIn(page,`/conversations/${thread}`);
  await expect(page).toHaveURL(`/conversations/${thread}`);
  await expect(page.getByRole('heading',{name:'Order Value (parent mirror)'})).toBeVisible();
  state.expired=true;
  await page.getByRole('button',{name:'Explore contributing projects'}).click();
  await expect(page.getByRole('button',{name:'Continue with Monday'})).toBeVisible();
  await expect(page.getByRole('heading',{name:'Order Value (parent mirror)'})).toHaveCount(0);
  await page.getByRole('button',{name:'Continue with Monday'}).click();
  await expect(page.getByRole('heading',{name:'Order Value (parent mirror)'})).toBeVisible();
  await page.getByRole('button',{name:'Sign out'}).click();
  await expect(page.getByRole('heading',{name:'Order Value (parent mirror)'})).toHaveCount(0);
});

test('mobile layout and keyboard navigation remain accessible',async({page})=>{
  await page.setViewportSize({width:390,height:844});await fixture(page,{existing:true});await signIn(page,`/conversations/${thread}`);
  await expect(page.locator('.chart svg')).toBeVisible();
  expect(await page.evaluate(()=>document.documentElement.scrollWidth<=window.innerWidth)).toBe(true);
  expect((await new AxeBuilder({page}).analyze()).violations).toEqual([]);
  await page.keyboard.press('Control+Home');
  await page.getByRole('link',{name:'Skip to content'}).focus();
  await expect(page.getByRole('link',{name:'Skip to content'})).toBeFocused();
  await page.keyboard.press('Enter');
  await expect(page).toHaveURL(/#workspace$/);
  await page.screenshot({path:'../outputs/phase6-mobile.png',fullPage:true});
});

test('comparison shows its baseline scope and undefined percentage change',async({page})=>{
  await fixture(page,{existing:true,comparison:true});await signIn(page,`/conversations/${thread}`);
  await expect(page.getByRole('region',{name:'Comparison'})).toContainText('2026-08-01 to 2026-09-01 (end exclusive)');
  await expect(page.getByRole('region',{name:'Comparison'})).toContainText('Percentage change is undefined because the baseline is zero or unknown.');
});

test('limited results explicitly label table and export scope',async({page})=>{
  await fixture(page,{existing:true,limited:true});await signIn(page,`/conversations/${thread}`);
  await expect(page.getByText('Limited result; this display is not the full population',{exact:false})).toBeVisible();
  await expect(page.getByText('excluded groups are not exported',{exact:false})).toBeVisible();
  await expect(page.locator('.chart svg')).toHaveCount(0);
});
