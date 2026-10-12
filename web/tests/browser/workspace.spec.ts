import {test,expect} from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';
import {fixture,signIn,thread,runId} from './fixture';
import {readFile,readdir} from 'node:fs/promises';

function acknowledgementGate() {
  let release!:()=>void;
  const wait=new Promise<void>(resolve=>{release=resolve;});
  return {wait,release};
}

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
  const ask=page.getByRole('button',{name:'Ask analyst',exact:true});
  await expect(ask).toBeDisabled();
  await expect(ask).toHaveCSS('background-color','rgb(147, 31, 31)');
  await expect(ask).toHaveCSS('color','rgb(255, 255, 255)');
  await page.getByLabel('Ask a follow-up or a new question').fill('Only category A');
  await expect(ask).toBeEnabled();
  await expect(ask).toHaveCSS('background-color','rgb(147, 31, 31)');
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

test('home page cards are readable and compact, and selection only prepares an editable question',async({page})=>{
  await page.setViewportSize({width:1280,height:900});
  const state=await fixture(page,{existing:true});
  await signIn(page);
  const question=page.getByLabel('Your question',{exact:true});
  const ask=page.getByRole('button',{name:'Ask analyst',exact:true});
  await expect(ask).toBeDisabled();
  await expect(ask).toHaveCSS('box-shadow','none');
  await expect(ask).toHaveCSS('height','44px');
  await ask.hover();
  await expect(ask).toHaveCSS('background-color','rgb(147, 31, 31)');
  await expect(ask).toHaveCSS('color','rgb(255, 255, 255)');
  await expect(page.locator('nav a').first()).toHaveCSS('font-size','14px');
  await expect(page.locator('nav a time').first()).toHaveCSS('font-size','12px');
  await expect(page.locator('.conversation-heading p')).toHaveCSS('font-size','16px');
  await expect(page.locator('.suggestion-question').first()).toHaveCSS('font-size','14px');
  const examples=[
    ['Monthly revenue','Show revenue for the last completed month.'],
    ['Order value by category','Show stored order value by category.'],
    ['Five-year conversion','What is the five-year conversion rate?'],
  ];
  await expect(page.locator('.suggestion')).toHaveCount(examples.length);
  for(const [title,text] of examples) {
    const card=page.getByRole('button',{name:`${title} ${text}`,exact:true});
    expect((await card.boundingBox())!.height).toBeLessThanOrEqual(120);
    await card.click();
    await expect(question).toHaveValue(text);
    await expect(ask).toBeEnabled();
    expect(state.submissions).toHaveLength(0);
  }
  await expect(page.locator('.composer')).toHaveCSS('border-radius','12px');
  await expect(page.locator('.composer-footer')).toHaveCSS('border-top-width','0px');
  const cards=(await page.locator('.suggestions').boundingBox())!;
  const composer=(await page.locator('.composer').boundingBox())!;
  expect(composer.x).toBe(cards.x);
  expect(composer.width).toBe(cards.width);
  expect(composer.y-cards.y-cards.height).toBe(24);
  expect((await ask.boundingBox())!.height).toBe(44);
  await expect(ask).toHaveCSS('color','rgb(255, 255, 255)');
  await ask.hover();
  await expect(ask).toHaveCSS('background-color','rgb(105, 22, 22)');
  await question.focus();
  await page.keyboard.press('Tab');
  await expect(ask).toBeFocused();
  await expect(ask).toHaveCSS('outline-style','solid');
  await expect(ask).toHaveCSS('outline-width','3px');
  await page.getByRole('heading',{name:'What would you like to understand?'}).click();
  await page.mouse.move(0,0);
  expect((await new AxeBuilder({page}).analyze()).violations).toEqual([]);
  await page.screenshot({path:'../outputs/analyst-home-desktop.png',fullPage:true});
  await ask.click();
  const answer=page.getByRole('article').filter({has:page.getByRole('heading',{name:examples[2][1],exact:true})});
  await expect(answer.getByRole('heading',{name:'Order Value (parent mirror)'})).toBeVisible();
  expect(state.submissions).toHaveLength(1);
  expect(state.submissions[0].question).toBe(examples[2][1]);
});

test('home page stays usable on tablet and small mobile screens with keyboard and reduced motion',async({page})=>{
  const state=await fixture(page,{existing:true});
  await signIn(page);
  const question=page.getByLabel('Your question',{exact:true});
  const ask=page.getByRole('button',{name:'Ask analyst',exact:true});
  for(const width of [768,390,320]) {
    await page.setViewportSize({width,height:844});
    await expect(ask).toHaveCSS('height','44px');
    expect(await page.evaluate(()=>document.documentElement.scrollWidth<=window.innerWidth)).toBe(true);
    const cards=await page.locator('.suggestion').all();
    for(let i=1;i<cards.length;i++) {
      const previous=(await cards[i-1].boundingBox())!;
      expect((await cards[i].boundingBox())!.y).toBeGreaterThanOrEqual(previous.y+previous.height);
    }
    expect((await new AxeBuilder({page}).analyze()).violations).toEqual([]);
  }
  await page.setViewportSize({width:390,height:844});
  await page.emulateMedia({reducedMotion:'reduce'});
  const card=page.getByRole('button',{name:/^Monthly revenue /});
  await expect(card).toHaveCSS('transition-duration','0s');
  await expect(ask).toHaveCSS('transition-duration','0s');
  await card.focus();
  await expect(card).toHaveCSS('outline-width','3px');
  await page.keyboard.press('Enter');
  await expect(question).toHaveValue('Show revenue for the last completed month.');
  expect(state.submissions).toHaveLength(0);
  await question.fill('   ');
  await expect(ask).toBeDisabled();
  await question.fill('Show revenue for the last completed month.');
  await expect(page.locator('.composer')).toHaveCSS('border-top-color','rgb(147, 31, 31)');
  await page.screenshot({path:'../outputs/analyst-home-mobile.png',fullPage:true});
  await page.keyboard.press('Tab');
  await expect(ask).toBeFocused();
  await page.keyboard.press('Enter');
  const answer=page.getByRole('article').filter({has:page.getByRole('heading',{name:'Show revenue for the last completed month.',exact:true})});
  await expect(answer.getByRole('heading',{name:'Order Value (parent mirror)'})).toBeVisible();
  expect(state.submissions).toHaveLength(1);
  expect(state.submissions[0].question).toBe('Show revenue for the last completed month.');
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
  await page.getByRole('button',{name:'View 12 values',exact:true}).click();
  await page.getByRole('button',{name:'Next rows'}).click();
  await expect(page.getByText('Page 2 of 2',{exact:false})).toBeVisible();
  await page.getByRole('button',{name:'value (GBP)'}).click();
  const download=page.waitForEvent('download');await page.getByRole('button',{name:'Download CSV (12 rows)'}).click();
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

for(const suggested of [true,false]) {
  test(`${suggested?'suggested':'typed'} questions show progress throughout conversation creation and message acknowledgement`,async({page})=>{
    const creation=acknowledgementGate(), acknowledgement=acknowledgementGate();
    let creating=false;
    const state=await fixture(page,{hold:true,beforeConversationReply:()=>{creating=true;return creation.wait;},beforeMessageReply:()=>acknowledgement.wait});
    await signIn(page);
    if(suggested) await page.getByRole('button',{name:/^Monthly revenue /}).click();
    else await page.getByLabel('Your question',{exact:true}).fill('Show revenue for the last completed month.');
    try {
      await page.getByRole('button',{name:'Ask analyst',exact:true}).click();
      await expect.poll(()=>creating).toBe(true);
      await expect(page.getByRole('status')).toHaveText('Submitting your query…');
      await expect(page.getByLabel('Your question',{exact:true})).toHaveValue('Show revenue for the last completed month.');
      await expect(page.getByLabel('Your question',{exact:true})).toBeDisabled();
      expect(state.submissions).toHaveLength(0);
      await expect(page.getByRole('button',{name:'Ask analyst',exact:true})).toBeDisabled();
      await expect(page.getByRole('button',{name:'+ New conversation',exact:true})).toBeDisabled();
      creation.release();
      await expect.poll(()=>state.submissions.length).toBe(1);
      await expect(page.getByRole('status')).toHaveText('Submitting your query…');
      await expect(page.getByText('Submission was not confirmed.',{exact:false})).toHaveCount(0);
      await expect(page.getByRole('button',{name:'Retry submission',exact:true})).toHaveCount(0);
      expect(state.runs).toHaveLength(1);
      expect(state.workflow[runId].status).toBe('running');
      expect((await new AxeBuilder({page}).analyze()).violations).toEqual([]);
      acknowledgement.release();
      await expect(page.getByRole('region',{name:'Current run'})).toBeVisible();
      await expect(page.getByRole('heading',{name:'Show revenue for the last completed month.',exact:true})).toBeVisible();
      await expect(page.getByText('Submitting your query…',{exact:true})).toHaveCount(0);
      await expect(page.getByText('Submission was not confirmed.',{exact:false})).toHaveCount(0);
      await expect(page.getByRole('button',{name:'Retry submission',exact:true})).toHaveCount(0);
      await expect(page.getByRole('button',{name:'Cancel run',exact:true})).toBeEnabled();
      expect(state.submissions).toHaveLength(1);
    } finally {creation.release();acknowledgement.release();}
  });
}

test('the acknowledged question and progress stay visible when opening a new conversation with slow API reads',async({page})=>{
  const acknowledgement=acknowledgementGate(), conversationRead=acknowledgementGate(), workflowRead=acknowledgementGate();
  const state=await fixture(page,{beforeMessageReply:()=>acknowledgement.wait});
  let readingWorkflow=false;
  await page.route(`http://127.0.0.1:4174/v1/conversations/${thread}`,async route=>{
    await conversationRead.wait;await route.fallback();
  });
  await page.route(`http://127.0.0.1:4174/v1/runs/${runId}/workflow`,async route=>{
    readingWorkflow=true;await workflowRead.wait;await route.fallback();
  });
  await signIn(page);
  const question='What was the revenue for January 2026?';
  await page.getByLabel('Your question',{exact:true}).fill(question);
  try {
    await page.getByRole('button',{name:'Ask analyst',exact:true}).click();
    await expect(page.getByRole('status')).toHaveText('Submitting your query…');
    await expect.poll(()=>state.submissions.length).toBe(1);
    acknowledgement.release();
    await expect(page).toHaveURL(`/conversations/${thread}`);
    await expect.poll(()=>readingWorkflow).toBe(true);
    await expect(page.getByRole('heading',{name:question,exact:true})).toBeVisible();
    await expect(page.getByRole('status')).toHaveText('Connecting to your run…');
    await expect(page.getByRole('button',{name:'Cancel run',exact:true})).toBeEnabled();
    await expect(page.locator('nav a[aria-current="page"]')).toBeVisible();
    await expect(page.getByText('Submitting your query…',{exact:true})).toHaveCount(0);
    await page.screenshot({path:'../outputs/analyst-continuous-submission.png',fullPage:true});
    conversationRead.release();workflowRead.release();
    await expect(page.getByRole('heading',{name:'Order Value (parent mirror)',exact:true})).toBeVisible();
    expect(state.submissions).toHaveLength(1);
  } finally {acknowledgement.release();conversationRead.release();workflowRead.release();}
});

for(const stage of ['conversation','message']) {
  test(`a rejected initial ${stage} request keeps the question and error available for correction`,async({page})=>{
    const state=await fixture(page);
    let reject=true;
    await page.route(stage==='conversation'?'http://127.0.0.1:4174/v1/conversations':`http://127.0.0.1:4174/v1/conversations/${thread}/messages`,async route=>{
      if(!reject || route.request().method()!=='POST') {await route.fallback();return;}
      await route.fulfill({status:422,headers:{'Access-Control-Allow-Origin':'http://127.0.0.1:4173','Content-Type':'application/json'},body:JSON.stringify({detail:'synthetic_rejection'})});
    });
    await signIn(page);
    const question=page.getByLabel('Your question',{exact:true});
    await question.fill('Show revenue.');
    await page.getByRole('button',{name:'Ask analyst',exact:true}).click();
    await expect(page).toHaveURL(stage==='conversation'?'/':`/conversations/${thread}`);
    await expect(page.getByRole('button',{name:'Ask analyst',exact:true})).toBeEnabled();
    await expect(page.getByRole('alert')).toContainText('The request could not be completed. Please retry.');
    await expect(question).toHaveValue('Show revenue.');
    await expect(question).toBeEnabled();
    await expect(page.getByText('Submitting your query…',{exact:true})).toHaveCount(0);
    await expect(page.getByText('Submission was not confirmed.',{exact:false})).toHaveCount(0);
    expect(state.submissions).toHaveLength(0);
    reject=false;
    await question.fill('Show revenue for the last completed month.');
    await page.getByRole('button',{name:'Ask analyst',exact:true}).click();
    await expect(page.getByRole('heading',{name:'Order Value (parent mirror)',exact:true})).toBeVisible();
    await expect(page.getByRole('alert')).toHaveCount(0);
    expect(state.submissions).toHaveLength(1);
    expect(state.submissions[0].question).toBe('Show revenue for the last completed month.');
  });
}

test('lost message acknowledgement offers a safe retry only after failure and reuses the same run',async({page})=>{
  const first=acknowledgementGate(), retry=acknowledgementGate();
  let replies=0;
  const state=await fixture(page,{hold:true,beforeMessageReply:()=>++replies===1?first.wait:retry.wait});
  state.failSubmission=true;
  await signIn(page);
  await page.getByRole('button',{name:/^Order value by category /}).click();
  await page.getByRole('button',{name:'Ask analyst',exact:true}).click();
  await expect(page.getByRole('status')).toHaveText('Submitting your query…');
  await expect(page.getByRole('button',{name:'Retry submission',exact:true})).toHaveCount(0);
  first.release();
  await expect(page.getByText('Submission was not confirmed.',{exact:false})).toBeVisible();
  await expect(page.getByText('Submitting your query…',{exact:true})).toHaveCount(0);
  await expect(page.getByRole('region',{name:'Current run'})).toBeVisible();
  expect(state.runs).toHaveLength(1);
  await page.getByRole('button',{name:'Retry submission',exact:true}).click();
  await expect(page.getByText('Submitting your query…',{exact:true})).toBeVisible();
  await expect(page.getByText('Submission was not confirmed.',{exact:false})).toHaveCount(0);
  await expect(page.getByRole('button',{name:'Retry submission',exact:true})).toHaveCount(0);
  await expect.poll(()=>state.submissions.length).toBe(2);
  expect(state.submissions[1]).toEqual(state.submissions[0]);
  expect(state.submissions[1].idempotency_key).toBeTruthy();
  expect(state.runs).toHaveLength(1);
  retry.release();
  await expect(page.getByText('Submitting your query…',{exact:true})).toHaveCount(0);
  await expect(page.getByText('Submission was not confirmed.',{exact:false})).toHaveCount(0);
  await expect(page.getByRole('button',{name:'Retry submission',exact:true})).toHaveCount(0);
  await expect(page.getByRole('button',{name:'Cancel run',exact:true})).toBeEnabled();
  expect(state.runs).toHaveLength(1);
});

test('definite submission rejections surface errors without an uncertain-delivery warning',async({page})=>{
  const state=await fixture(page,{existing:true});
  let rejection=400;
  await page.route('http://127.0.0.1:4174/v1/conversations/*/messages',async route=>{
    if(route.request().method()!=='POST') {await route.fallback();return;}
    await route.fulfill({status:rejection,headers:{'Access-Control-Allow-Origin':'http://127.0.0.1:4173','Content-Type':'application/json'},body:JSON.stringify({detail:'synthetic_rejection'})});
  });
  await signIn(page,`/conversations/${thread}`);
  const question=page.getByLabel('Ask a follow-up or a new question');
  for(const status of [400,404,409,422]) {
    rejection=status;
    await question.fill('Only category A');
    await page.getByRole('button',{name:'Ask analyst',exact:true}).click();
    await expect(page.getByRole('alert')).toBeVisible();
    await expect(page.getByText('Submitting your query…',{exact:true})).toHaveCount(0);
    await expect(page.getByText('Submission was not confirmed.',{exact:false})).toHaveCount(0);
    await expect(page.getByRole('button',{name:'Retry submission',exact:true})).toHaveCount(0);
    await expect(page.getByRole('button',{name:'Ask analyst',exact:true})).toBeEnabled();
    await expect(question).toHaveValue('Only category A');
  }
  expect(state.submissions).toHaveLength(0);
  expect(state.runs).toHaveLength(1);
});

test('clarification retry retains the same idempotency key and follow-ups name the previous run',async({page})=>{
  const acknowledgement=acknowledgementGate();
  const state=await fixture(page,{clarify:true,beforeResumeReply:()=>acknowledgement.wait});state.failReply=true;
  await signIn(page);
  await page.getByLabel('Your question',{exact:true}).fill('Show order value');await page.getByRole('button',{name:'Ask analyst'}).click();
  await page.getByRole('button',{name:'All stored history',exact:true}).click();await page.getByRole('button',{name:'Send clarification'}).click();
  await expect(page.getByText('Submitting your clarification…',{exact:true})).toBeVisible();
  await expect(page.getByText('Submission was not confirmed.',{exact:false})).toHaveCount(0);
  await expect(page.getByRole('button',{name:'Retry submission',exact:true})).toHaveCount(0);
  acknowledgement.release();
  await expect(page.getByText('Submission was not confirmed.',{exact:false})).toBeVisible();
  await page.getByRole('button',{name:'Retry submission'}).click();
  await expect(page.getByRole('heading',{name:'Order Value (parent mirror)'})).toBeVisible();
  await expect(page.getByText('Submitting your clarification…',{exact:true})).toHaveCount(0);
  await expect(page.getByRole('button',{name:'Retry submission',exact:true})).toHaveCount(0);
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
  const chartWidth=async()=>(await page.locator('.chart svg').boundingBox())!.width;
  const mobileWidth=await chartWidth();
  await page.setViewportSize({width:1280,height:900});
  await expect.poll(chartWidth).toBeGreaterThan(mobileWidth);
  await page.setViewportSize({width:390,height:844});
  await expect.poll(chartWidth).toBeLessThanOrEqual(mobileWidth+1);
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
  await expect(page.getByText('Showing 12 of 15 groups.',{exact:false})).toBeVisible();
  await expect(page.getByText('the table and CSV include only these 12 groups.',{exact:false})).toBeVisible();
  await expect(page.getByRole('table',{name:'Order Value (parent mirror) — values'})).toBeVisible();
  await expect(page.locator('.chart svg')).toHaveCount(0);
});

test('exploratory answers are labelled uncertified and show their table, SQL and export without drill-down',async({page})=>{
  const state=await fixture(page,{existing:true,exploratory:true});
  const errors:string[]=[]; page.on('pageerror',e=>errors.push(e.message));
  await signIn(page,`/conversations/${thread}`);
  const card=page.getByRole('region',{name:'Invoiced revenue by account, 1 January 2026 to 31 January 2026'});
  await expect(card.getByRole('heading',{name:'Invoiced revenue by account, 1 January 2026 to 31 January 2026'})).toBeVisible();
  await expect(card.getByText('Exploratory analysis · not a certified metric',{exact:true})).toBeVisible();
  await expect(card.getByText('Generated query over analyst_query.invoice_reporting_facts_v1, analyst_query.projects_v1 · Europe/London')).toBeVisible();
  const table=card.getByRole('table',{name:'Invoiced revenue by account, 1 January 2026 to 31 January 2026 — values'});
  await expect(table).toContainText('=Untrusted account');
  await expect(table).toContainText('£1,200.00');
  await expect(table.getByRole('button',{name:'revenue (GBP)'})).toBeVisible();
  await expect(card.getByRole('button',{name:'Explore contributing projects'})).toHaveCount(0);
  await expect(card.getByRole('note',{name:'What this answer leaves out'})).toContainText('Invoices without a recorded account are grouped as an unknown account.');
  await card.getByText('How this answer was produced',{exact:true}).click();
  await expect(card.locator('pre.sql')).toHaveText(state.result.provenance.sql);
  await expect(card.getByText('Exploratory analysis: generated SQL over governed views, not a certified metric.',{exact:true})).toBeVisible();
  const download=page.waitForEvent('download');await card.getByRole('button',{name:'Download CSV (3 rows)'}).click();
  expect((await download).suggestedFilename()).toMatch(/datacube-.*\.csv/);
  expect(state.exports).toBe(1);
  expect(state.drilldowns).toBe(0);
  expect((await new AxeBuilder({page}).analyze()).violations).toEqual([]);
  expect(errors).toEqual([]);
});

test('explanations answer in plain language and offer editable suggestions without loading a result',async({page})=>{
  const state=await fixture(page,{existing:true,explanation:true});
  const errors:string[]=[]; page.on('pageerror',e=>errors.push(e.message));
  await signIn(page,`/conversations/${thread}`);
  const card=page.getByRole('region',{name:"Forecasts aren't available"});
  await expect(card.getByRole('heading',{name:"Forecasts aren't available"})).toBeVisible();
  await expect(card.getByText("I report recorded results only, so I can't forecast or predict future values.",{exact:true})).toBeVisible();
  await expect(card.getByText('Invoices are recorded for completed months up to September 2026.',{exact:true})).toBeVisible();
  await expect(page.getByText('could not complete this request',{exact:false})).toHaveCount(0);
  await expect(page.getByRole('button',{name:'Retry question'})).toHaveCount(0);
  // An explanation has no dataset, so it cannot be used as follow-up context.
  await expect(page.getByLabel('Use the latest answer as context')).toHaveCount(0);
  await card.getByRole('button',{name:'Show monthly invoiced revenue for the last 12 completed months.'}).click();
  const composer=page.getByLabel('Your question');
  await expect(composer).toHaveValue('Show monthly invoiced revenue for the last 12 completed months.');
  await expect(composer).toBeFocused();
  expect(state.submissions).toHaveLength(0);
  expect((await new AxeBuilder({page}).analyze()).violations).toEqual([]);
  await page.getByRole('button',{name:'Ask analyst'}).click();
  await expect.poll(()=>state.submissions.length).toBe(1);
  expect(state.submissions[0].follow_up_to).toBeUndefined();
  expect(state.resultReads).toBe(0);
  expect(errors).toEqual([]);
});

test('retries stay with their question while the completed answer leads the default view',async({page})=>{
  const state=await fixture(page,{existing:true});
  for(let i=0;i<3;i++) {
    const id=`30000000-0000-4000-8000-${String(i+20).padStart(12,'0')}`;
    state.runs.push({...state.runs[0],id,status:'failed',created_at:`2026-10-08T11:0${2-i}:00Z`});
    state.workflow[id]={...state.workflow[runId],run_id:id,status:'failed',answer:null};
  }
  await signIn(page,`/conversations/${thread}`);
  await expect(page.locator('.message')).toHaveCount(1);
  await expect(page.getByRole('heading',{name:'Show stored Order Value by category.'})).toHaveCount(1);
  await expect(page.getByRole('heading',{name:'Order Value (parent mirror)'})).toBeVisible();
  await expect(page.getByText('3 earlier unsuccessful attempts',{exact:true})).toBeVisible();
  await expect(page.getByRole('button',{name:'Retry question'})).toHaveCount(0);
  await expect(page.getByText('1 of 24 matching records have no value.',{exact:false})).toBeVisible();
  await expect(page.getByText('Source freshness unknown',{exact:true})).toBeVisible();
  await expect(page.locator('nav time')).toHaveText('8 Oct, 13:00');
  await expect(page.getByRole('table')).toHaveCount(0);
  await expect(page.locator('.answer-text')).toBeHidden();
  await expect(page.locator('.limitations')).toBeHidden();
  await page.getByRole('button',{name:'View 12 values',exact:true}).click();
  await expect(page.getByRole('table')).toBeVisible();
  await expect(page.getByRole('columnheader',{name:'source rows (count)'})).toHaveCount(0);
  await page.getByLabel('Show source record counts').check();
  await expect(page.getByRole('columnheader',{name:'source rows (count)'})).toBeVisible();
  await expect(page.getByRole('columnheader',{name:'known values (count)'})).toBeVisible();
  await page.getByRole('button',{name:'Hide values',exact:true}).click();
  await expect(page.getByRole('table')).toHaveCount(0);
  await page.screenshot({path:'../outputs/analyst-simplified-desktop.png',fullPage:true});
  await page.getByText('3 earlier unsuccessful attempts',{exact:true}).click();
  await expect(page.locator('.attempt-history li')).toHaveCount(3);
  await expect(page.locator('.attempt-history li').first()).toBeVisible();
  await page.getByText('Definitions and source details',{exact:true}).click();
  await expect(page.getByText('One source value is unknown.',{exact:true})).toBeVisible();
  await page.getByText('Answer text',{exact:true}).click();
  await expect(page.locator('.answer-text')).toBeVisible();
  expect((await new AxeBuilder({page}).analyze()).violations).toEqual([]);
});

test('a fourteen-category breakdown keeps long labels readable and every exact value accessible',async({page})=>{
  const state=await fixture(page,{existing:true});
  const categories=['Apartments','Commercial','Commodity','Consultancy','Datacentre','Education','Health','House','Industrial','Leisure','Military','Mixed Use','Student Accommodation',null];
  state.result.rows=categories.map((category,i)=>[category,`${14000000-i*900000}.00`,100,80]) as typeof state.result.rows;
  state.result.provenance.returned_rows=14;
  state.result.provenance.matched_groups=14;
  state.result.provenance.total={value:'114100000.00',source_rows:1400,known_values:1120};
  state.result.provenance.limitations=['Some source values are unknown.'];
  await signIn(page,`/conversations/${thread}`);
  await expect(page.locator('.chart svg .mark-rect.role-mark path')).toHaveCount(14);
  const labels=page.locator('.chart svg text');
  await expect(labels.filter({hasText:'Student Accommodation'})).toBeVisible();
  await expect(labels.filter({hasText:'(Unclassified)'})).toBeVisible();
  await page.screenshot({path:'../outputs/analyst-category-desktop.png',fullPage:true});
  await page.setViewportSize({width:390,height:844});
  await expect(labels.filter({hasText:'Student Accommodation'})).toBeVisible();
  expect(await page.evaluate(()=>document.documentElement.scrollWidth<=window.innerWidth)).toBe(true);
  await page.screenshot({path:'../outputs/analyst-category-mobile.png',fullPage:true});
  await page.getByRole('button',{name:'View 14 values',exact:true}).click();
  await page.getByRole('button',{name:'Next rows'}).click();
  await expect(page.getByRole('cell',{name:'Student Accommodation',exact:true})).toBeVisible();
  await expect(page.getByRole('cell',{name:'£3,200,000.00',exact:true})).toBeVisible();
});

test('a failed latest attempt remains actionable and unrelated questions and completed answers stay separate',async({page})=>{
  const state=await fixture(page,{existing:true});
  const latest='30000000-0000-4000-8000-000000000020';
  const previous='30000000-0000-4000-8000-000000000021';
  const other='30000000-0000-4000-8000-000000000022';
  const template={...state.runs[0]};
  state.runs=[{...template,id:latest,status:'failed'},{...template,id:previous,status:'failed'},template,
    {...template,id:other,question:'Show revenue.',status:'failed'}];
  for(const id of [latest,previous,other]) state.workflow[id]={...state.workflow[runId],run_id:id,status:'failed',answer:null};
  await signIn(page,`/conversations/${thread}`);
  await expect(page.locator('.message')).toHaveCount(3);
  await expect(page.getByText('1 earlier unsuccessful attempt',{exact:true})).toBeVisible();
  const turn=page.locator('.message').filter({has:page.getByText('1 earlier unsuccessful attempt',{exact:true})});
  await expect(turn.getByRole('button',{name:'Retry question'})).toBeEnabled();
  await turn.getByRole('button',{name:'Retry question'}).click();
  expect(state.submissions[0].question).toBe(template.question);
  expect(state.submissions[0].follow_up_to).toBeUndefined();
  await expect(page.getByRole('heading',{name:'Order Value (parent mirror)'})).toHaveCount(2);
  await expect(page.getByRole('heading',{name:'Show revenue.'})).toBeVisible();
});
