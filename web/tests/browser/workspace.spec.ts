import {test,expect} from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';
import {fixture,signIn,thread,runId,monthlySummary,certifiedConversionResult,certifiedConversionSummary} from './fixture';
import {readFile,readdir} from 'node:fs/promises';

function acknowledgementGate() {
  let release!:()=>void;
  const wait=new Promise<void>(resolve=>{release=resolve;});
  return {wait,release};
}

test('uncertain submission survives reload and same-subject login with its original key',async({page})=>{
  const state=await fixture(page); await signIn(page); state.failSubmission=true;
  await page.getByLabel('Your question',{exact:true}).fill('Recover an uncertain query');
  await page.getByRole('button',{name:'Ask analyst'}).click();
  await expect(page.getByRole('button',{name:'Retry submission'})).toBeVisible();
  const key=state.submissions[0].idempotency_key;
  await page.reload(); await page.getByRole('button',{name:'Continue with Monday'}).click();
  await expect(page.getByRole('button',{name:'Retry submission'})).toBeVisible();
  await expect(page.locator('.message .question h2')).toHaveText('Recover an uncertain query');
  await page.getByRole('button',{name:'Retry submission'}).click();
  await expect(page.getByRole('button',{name:'Retry submission'})).toBeHidden();
  expect(state.submissions.map(s=>s.idempotency_key)).toEqual([key,key]);
  expect(state.runs).toHaveLength(1);
});

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
  await page.getByRole('button',{name:'Cancel run'}).click();await expect(page.getByText('You cancelled this question.')).toBeVisible();
  await page.getByRole('button',{name:'Retry question'}).click();
  expect(state.submissions).toHaveLength(2);expect(state.submissions[1].idempotency_key).not.toBe(state.submissions[0].idempotency_key);
});

test('an interrupted saved run can be retried and displays the completed answer',async({page})=>{
  const state=await fixture(page,{existing:true});
  state.runs[0].status='interrupted';
  state.workflow[runId].status='interrupted';
  state.workflow[runId].answer=null;
  await signIn(page,`/conversations/${thread}`);
  await expect(page.getByText('The analyst stopped before finishing, usually because the service restarted. Retry to ask again.')).toBeVisible();
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

test('unsuccessful runs explain the cause in plain language without internal codes',async({page})=>{
  const state=await fixture(page,{existing:true});
  const failures=[
    ['model_timeout','failed','Show revenue by month.','The AI model took too long to respond. This is usually temporary, so please retry.'],
    ['run_budget_exceeded','failed','Compare every account by every product.','This question needed more steps than one answer allows. Try splitting it into smaller questions.'],
    ['metric_query_timeout','failed','Show all order history.','Reading the data took longer than the time allowed. Try a shorter period or fewer breakdowns.'],
    ['an_unrecognised_code','failed','Show bookings.','Something went wrong while answering this question. Please retry.'],
    ['execution_interrupted','interrupted','Show conversion.','The analyst stopped before finishing, usually because the service restarted. Retry to ask again.'],
  ];
  failures.forEach(([code,status,question],i)=>{
    const id=`30000000-0000-4000-8000-${String(i+40).padStart(12,'0')}`;
    state.runs.unshift({...state.runs.at(-1),id,question,status,created_at:`2026-10-08T12:1${i}:00Z`});
    state.workflow[id]={run_id:id,conversation_id:thread,status,error_code:code,clarification:null,answer:null};
  });
  await signIn(page,`/conversations/${thread}`);
  const turn=(question:string)=>page.locator('.message').filter({has:page.getByRole('heading',{name:question,exact:true})});
  for(const [,,question,message] of failures) await expect(turn(question).getByText(message,{exact:true})).toBeVisible();
  const text=await page.locator('main').innerText();
  for(const [code] of failures) expect(text).not.toContain(code);
  await turn('Compare every account by every product.').getByRole('button',{name:'Edit question'}).click();
  await expect(page.locator('#question')).toHaveValue('Compare every account by every product.');
  await expect(page.locator('#question')).toBeFocused();
  expect(state.submissions).toHaveLength(0);
  expect((await new AxeBuilder({page}).analyze()).violations).toEqual([]);
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

test('long conversation history scrolls independently and stays reachable on smaller screens',async({page})=>{
  await page.setViewportSize({width:1280,height:900});
  await fixture(page,{existing:true});
  const conversations=Array.from({length:40},(_,i)=>({
    id:i===0 ? thread : `10000000-0000-4000-8000-${String(i+1).padStart(12,'0')}`,
    title:`Saved analysis ${i+1}: what was the revenue for January 2026?`,created_at:'2026-10-08T12:00:00Z',
  }));
  await page.route('http://127.0.0.1:4174/v1/conversations?*',async route=>{
    const url=new URL(route.request().url());
    const offset=Number(url.searchParams.get('offset') || 0);
    await route.fulfill({json:conversations.slice(offset,offset+30),headers:{'Access-Control-Allow-Origin':'http://127.0.0.1:4173'}});
  });
  await signIn(page,`/conversations/${thread}`);
  const history=page.getByRole('complementary',{name:'Conversation history'});
  const list=history.locator('nav');
  const links=list.getByRole('link');
  const newConversation=history.getByRole('button',{name:'+ New conversation',exact:true});
  const older=history.getByRole('button',{name:'Older conversations',exact:true});
  await expect(links).toHaveCount(30);
  await expect(older).toBeInViewport();
  await expect.poll(()=>list.evaluate(node=>node.scrollHeight>node.clientHeight)).toBe(true);
  await expect.poll(()=>page.evaluate(()=>document.documentElement.scrollHeight<=window.innerHeight+1)).toBe(true);
  const question=page.locator('.composer');
  const questionTop=(await question.boundingBox())!.y;
  await list.hover();
  await page.mouse.wheel(0,800);
  await expect.poll(()=>list.evaluate(node=>node.scrollTop)).toBeGreaterThan(0);
  expect((await question.boundingBox())!.y).toBeCloseTo(questionTop,0);
  await expect(newConversation).toBeInViewport();
  await older.click();
  await expect(links).toHaveCount(40);
  await expect(older).toHaveCount(0);
  await links.last().focus();
  await expect(links.last()).toBeInViewport();
  await expect(newConversation).toBeInViewport();
  await expect(page.locator('.chart svg')).toHaveCount(1);
  await page.getByRole('button',{name:'View 12 values',exact:true}).click();
  const main=page.locator('.conversation');
  await expect.poll(()=>main.evaluate(node=>node.scrollHeight>node.clientHeight)).toBe(true);
  const historyScroll=await list.evaluate(node=>node.scrollTop);
  const mainScroll=await main.evaluate(node=>node.scrollTop);
  await main.hover();
  await page.mouse.wheel(0,800);
  await expect.poll(()=>main.evaluate(node=>node.scrollTop)).toBeGreaterThan(mainScroll);
  expect(await list.evaluate(node=>node.scrollTop)).toBe(historyScroll);
  await expect(newConversation).toBeInViewport();
  await page.screenshot({path:'../outputs/analyst-scrollable-history-desktop.png'});
  await page.setViewportSize({width:800,height:600});
  await expect(newConversation).toBeInViewport();
  await newConversation.focus();
  await links.last().focus();
  await expect(links.last()).toBeInViewport();
  expect(await history.evaluate(node=>node.getBoundingClientRect().bottom<=window.innerHeight+1)).toBe(true);
  await page.setViewportSize({width:390,height:844});
  await page.evaluate(()=>window.scrollTo(0,0));
  await expect(newConversation).toBeInViewport();
  expect((await history.boundingBox())!.height).toBeLessThan(250);
  await newConversation.focus();
  await links.last().focus();
  await expect(links.last()).toBeInViewport();
  expect(await list.evaluate(node=>node.scrollLeft)).toBeGreaterThan(0);
  expect(await page.evaluate(()=>document.documentElement.scrollWidth<=window.innerWidth)).toBe(true);
  expect((await new AxeBuilder({page}).analyze()).violations).toEqual([]);
  await page.screenshot({path:'../outputs/analyst-scrollable-history-mobile.png'});
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

test('comparison rounds repeating percentage changes and retains exact currency amounts',async({page})=>{
  const state=await fixture(page,{existing:true,answerOverride:{chart:{kind:'number',y:'value'}}});
  state.result.provenance.total.value='857588.52';
  state.result.provenance.comparison_period={start_date:'2026-08-01',end_date_exclusive:'2026-09-01'};
  state.result.provenance.comparison={baseline:{value:'293732.20'},absolute_change:'563856.32',
    percentage_change:'191.96271978353071266956772189089245237668869807260',percentage_point_change:null,zero_denominator:false};
  await signIn(page,`/conversations/${thread}`);
  const comparison=page.getByRole('region',{name:'Comparison'});
  await expect(comparison.locator('div').filter({has:page.locator('dt',{hasText:'Percentage change'})}).locator('dd')).toHaveText('192.0%');
  await expect(comparison).toContainText('£293,732.20');
  await expect(comparison).toContainText('£563,856.32');
  await expect(page.locator('.total strong')).toHaveText('£857,588.52');
});

test('comparison shows its baseline scope and undefined percentage change',async({page})=>{
  await fixture(page,{existing:true,comparison:true});await signIn(page,`/conversations/${thread}`);
  await expect(page.getByRole('region',{name:'Comparison'})).toContainText('August 2026');
  await expect(page.getByRole('region',{name:'Comparison'}).locator('dd')).toContainText(['Undefined']);
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
  await expect(card.getByText('Generated query over analyst_query.invoice_reporting_facts_v1, analyst_query.projects_v1 · Europe/London')).toBeHidden();
  const table=card.getByRole('table',{name:'Invoiced revenue by account, 1 January 2026 to 31 January 2026 — values'});
  await expect(table).toContainText('=Untrusted account');
  await expect(table).toContainText('£1,200.00');
  await expect(table.getByRole('button',{name:'Revenue (GBP)'})).toBeVisible();
  await expect(card.getByRole('button',{name:'Explore contributing projects'})).toHaveCount(0);
  await expect(card.getByRole('note',{name:'What this answer leaves out'})).toContainText('Invoices without a recorded account are grouped as an unknown account.');
  await card.getByText('How this answer was produced',{exact:true}).click();
  await expect(card.getByText('Generated query over analyst_query.invoice_reporting_facts_v1, analyst_query.projects_v1 · Europe/London')).toBeVisible();
  await expect(card.locator('pre.sql')).toHaveText(state.result.provenance.sql);
  await expect(card.getByText('Exploratory analysis: generated SQL over governed views, not a certified metric.',{exact:true})).toBeVisible();
  const download=page.waitForEvent('download');await card.getByRole('button',{name:'Download CSV (3 rows)'}).click();
  expect((await download).suggestedFilename()).toMatch(/datacube-.*\.csv/);
  expect(state.exports).toBe(1);
  expect(state.drilldowns).toBe(0);
  expect((await new AxeBuilder({page}).analyze()).violations).toEqual([]);
  expect(errors).toEqual([]);
});

test('monthly exploration opens with summary, maturity caveat and chart and retains all exact values',async({page},testInfo)=>{
  const state=await fixture(page,{existing:true,exploratory:true,monthly:true});
  const errors:string[]=[];page.on('pageerror',e=>errors.push(e.message));
  await signIn(page,`/conversations/${thread}`);
  const card=page.getByRole('region',{name:'Monthly conversion for the completed two-year period'});
  await expect(card.getByLabel('Summary',{exact:true})).toHaveText(monthlySummary);
  await expect(card.getByRole('note',{name:'What this answer leaves out'})).toContainText('less time to reach invoiced completion');
  await expect(card.locator('.chart svg')).toBeVisible();
  await card.getByLabel('Summary',{exact:true}).scrollIntoViewIfNeeded();
  await expect(card.getByLabel('Summary',{exact:true})).toBeInViewport();
  await page.screenshot({path:testInfo.outputPath('monthly-summary.png'),fullPage:true});
  await expect(card.getByText('Generated query over',{exact:false})).toBeHidden();
  await expect(card.getByRole('table')).toHaveCount(0);
  await card.getByRole('button',{name:'View 24 values'}).click();
  const table=card.getByRole('table');
  await expect(table.getByRole('columnheader',{name:'Inclusive conversion (%)'})).toBeVisible();
  await expect(table).toContainText('50.0%');
  await expect(table.getByRole('columnheader',{name:'Won invoiced projects'})).toBeVisible();
  await card.getByRole('button',{name:'Next rows'}).click();
  await card.getByRole('button',{name:'Next rows'}).click();
  await expect(table).toContainText('2025-12-01');
  await expect(table).toContainText('9.0%');
  await expect(card.getByRole('button',{name:'Download CSV (24 rows)'})).toBeVisible();
  await card.getByRole('button',{name:'Hide values'}).click();
  expect((await new AxeBuilder({page}).analyze()).violations).toEqual([]);
  expect(errors).toEqual([]);
  await page.reload();
  // The fixture requires a fresh login after reload; production restoration is
  // covered independently by the session tests.
  await page.getByRole('button',{name:'Continue with Monday'}).click();
  await expect(page.getByRole('button',{name:'Sign out'})).toBeVisible();
  await expect(card.getByLabel('Summary',{exact:true})).toHaveText(monthlySummary);
  expect(state.resultReads).toBeGreaterThan(1);
});

test('monthly enquiry uses conversational wording and keeps exact dates in source details',async({page},testInfo)=>{
  const summaryText='New enquiry value totalled £6,055,247.06 in September 2026.';
  const evidenceText='Monthly New Enquiry Value: GBP 6055247.06.';
  const state=await fixture(page,{existing:true,answerOverride:{summary:summaryText,summary_version:'2',text:evidenceText,
    chart:{kind:'number',x:null,y:'value'}}});
  state.result={...state.result,columns:['value'],rows:[['6055247.06']],provenance:{...state.result.provenance,
    metric_id:'enquiry_monthly_actual',metric_label:'Monthly New Enquiry Value',
    request:{period:'last_month',filters:[],dimensions:[]},resolved_period:{start_date:'2026-09-01',end_date_exclusive:'2026-10-01'},
    columns:[{name:'value',type:'decimal',unit:'source_currency'}],total:{value:'6055247.06',source_rows:1,known_values:1},
    coverage:{},limitations:[],returned_rows:1,matched_groups:1,evaluation_only:false}};
  state.runs[0].question='What was gross New Enquiry Value for the last completed month?';
  await signIn(page,`/conversations/${thread}`);
  const card=page.getByRole('region',{name:'Monthly New Enquiry Value'});
  await expect(card.getByLabel('Summary',{exact:true})).toHaveText(summaryText);
  await expect(card.locator('.result-scope')).toHaveText('September 2026 · Reportable projects, including archived projects');
  await expect(card.getByText('2026-09-01 to 2026-10-01 (end exclusive) · Europe/London',{exact:true})).toBeHidden();
  await expect(card.locator('.answer-text')).toBeHidden();
  await page.screenshot({path:testInfo.outputPath('enquiry-summary-desktop.png'),fullPage:true});
  await page.setViewportSize({width:390,height:844});
  await card.getByLabel('Summary',{exact:true}).scrollIntoViewIfNeeded();
  expect(await page.evaluate(()=>document.documentElement.scrollWidth<=window.innerWidth)).toBe(true);
  expect((await new AxeBuilder({page}).analyze()).violations).toEqual([]);
  await page.screenshot({path:testInfo.outputPath('enquiry-summary-mobile.png'),fullPage:true});
  await card.getByText('Supporting figures',{exact:true}).click();
  await expect(card.locator('.answer-text')).toHaveText(evidenceText);
  await card.getByText('Definitions and source details',{exact:true}).click();
  await expect(card.getByText('2026-09-01 to 2026-10-01 (end exclusive) · Europe/London',{exact:true})).toBeVisible();
});

test('certified conversion shows the conversational summary and exclusions above the figure on desktop and mobile',async({page},testInfo)=>{
  const state=await fixture(page,{existing:true,answerOverride:{summary:certifiedConversionSummary,
    summary_notices:['Open projects are excluded from closed-only conversion.','Recent creation cohorts have had less time to reach invoiced completion.'],
    chart:{kind:'number',x:null,y:'value'}}});
  state.result=structuredClone(certifiedConversionResult);
  await signIn(page,`/conversations/${thread}`);
  const card=page.getByRole('region',{name:'2-year Closed-only Conversion Rate'}), summary=card.getByLabel('Summary',{exact:true});
  await expect(summary).toHaveText(certifiedConversionSummary);
  await expect(summary).toBeVisible();
  expect((await summary.boundingBox())!.y).toBeLessThan((await card.locator('.total').boundingBox())!.y);
  await expect(card.getByRole('note',{name:'What this answer leaves out'})).toContainText('Open projects are excluded');
  const counts=card.getByLabel('Conversion counts',{exact:true});
  await expect(counts).toContainText('Invoiced wins405');
  await expect(counts).toContainText('Closed projects696');
  await expect(card.getByRole('columnheader',{name:'Closed projects'})).toBeVisible();
  await expect(card.getByRole('columnheader',{name:'Eligible projects'})).toHaveCount(0);
  await expect(card.locator('.answer-text')).toBeHidden();
  await summary.scrollIntoViewIfNeeded();
  await page.screenshot({path:testInfo.outputPath('certified-summary-desktop.png'),fullPage:true});
  await page.setViewportSize({width:390,height:844});
  await summary.scrollIntoViewIfNeeded();
  await expect(summary).toBeInViewport();
  expect(await page.evaluate(()=>document.documentElement.scrollWidth<=window.innerWidth)).toBe(true);
  expect((await new AxeBuilder({page}).analyze()).violations).toEqual([]);
  await page.screenshot({path:testInfo.outputPath('certified-summary-mobile.png'),fullPage:true});
  await page.reload();
  await page.getByRole('button',{name:'Continue with Monday'}).click();
  await expect(summary).toHaveText(certifiedConversionSummary);
});

test('inclusive conversion shows wins and eligible counts with accurate scope on desktop and mobile',async({page},testInfo)=>{
  const summary='The inclusive conversion rate was 19.8% for projects created on or after 24 October 2021: 1,557 invoiced wins out of 7,876 eligible projects. The denominator includes open and lost projects, as well as wins; projects with an unknown stage also remain eligible.';
  const state=await fixture(page,{existing:true,answerOverride:{summary,
    summary_notices:['This reflects recorded outcomes so far. More recent projects have had less time to reach invoiced completion.'],
    chart:{kind:'number',x:null,y:'value'}}});
  state.result=structuredClone(certifiedConversionResult);
  Object.assign(state.result.provenance,{metric_id:'conversion_five_year',metric_label:'5-year Inclusive Conversion Rate',
    request:{period:'five_year_cohort',filters:[],dimensions:[]},resolved_period:{start_date:'2021-10-24',end_date_exclusive:null},
    total:{value:'0.198',source_rows:7876,known_values:7876,numerator:'1557',denominator:'7876'},evaluation_only:false});
  state.result.columns=['value','numerator','denominator','source_rows','known_values'];
  state.result.rows=[['0.198','1557','7876',7876,7876]];
  state.result.provenance.columns.push(...['source_rows','known_values'].map(name=>({name,type:'integer',unit:'count'})));
  state.runs[0].question='What is inclusive conversion for the five-year cohort? Show wins and eligible project counts.';
  await signIn(page,`/conversations/${thread}`);
  const card=page.getByRole('region',{name:'5-year Inclusive Conversion Rate'});
  await expect(card.getByLabel('Summary',{exact:true})).toHaveText(summary);
  await expect(card.locator('.total strong')).toHaveText('19.8%');
  const counts=card.getByLabel('Conversion counts',{exact:true});
  await expect(counts).toContainText('Invoiced wins1,557');
  await expect(counts).toContainText('Eligible projects7,876');
  await expect(card.getByRole('columnheader',{name:'Inclusive conversion rate (%)'})).toBeVisible();
  await expect(card.getByRole('columnheader',{name:'Invoiced wins (count)'})).toBeVisible();
  await expect(card.getByRole('columnheader',{name:'Eligible projects'})).toBeVisible();
  await expect(card.getByRole('columnheader',{name:/numerator|denominator|source rows|known values/i})).toHaveCount(0);
  await expect(card.locator('.result-scope')).toHaveText('Projects created on or after 24 October 2021 · Reportable projects, including archived projects');
  await expect(card.getByRole('note')).not.toContainText('closed-only');
  await expect(card.getByText('Source update unconfirmed',{exact:true})).toHaveAttribute('title',/query time does not establish source freshness/);
  await page.screenshot({path:testInfo.outputPath('inclusive-conversion-desktop.png'),fullPage:true});
  await page.setViewportSize({width:390,height:844});
  await counts.scrollIntoViewIfNeeded();
  await expect(counts).toBeInViewport();
  expect(await page.evaluate(()=>document.documentElement.scrollWidth<=window.innerWidth)).toBe(true);
  expect((await new AxeBuilder({page}).analyze()).violations).toEqual([]);
  await page.screenshot({path:testInfo.outputPath('inclusive-conversion-mobile.png'),fullPage:true});
  await card.getByLabel('Show source record counts').check();
  await expect(card.getByRole('columnheader',{name:'source rows (count)'})).toBeVisible();
  await card.getByText('Definitions and source details',{exact:true}).click();
  await expect(card.getByText(/The cohort has a lower date bound only/)).toBeVisible();
  await expect(card.getByText('The latest source update could not be confirmed.',{exact:true})).toBeVisible();
});

test('certified summaries survive initial submission, clarification and follow-up answers',async({page})=>{
  const state=await fixture(page,{clarify:true,answerOverride:{summary:certifiedConversionSummary,chart:{kind:'number',x:null,y:'value'}}});
  state.result=structuredClone(certifiedConversionResult);
  await signIn(page);
  await page.getByLabel('Your question',{exact:true}).fill('What is the closed-only two-year conversion rate?');
  await page.getByRole('button',{name:'Ask analyst'}).click();
  await expect(page.getByRole('button',{name:'Send clarification'})).toBeVisible();
  await expect(page.getByLabel('Summary',{exact:true})).toHaveCount(0);
  await page.getByLabel('Which reporting period?').fill('Two-year creation cohort');
  await page.getByRole('button',{name:'Send clarification'}).click();
  await expect(page.getByLabel('Summary',{exact:true})).toHaveText(certifiedConversionSummary);
  await page.getByLabel('Ask a follow-up or a new question',{exact:true}).fill('Keep the same scope and explain this rate');
  await page.getByRole('button',{name:'Ask analyst'}).click();
  await page.getByLabel('Which reporting period?').fill('Keep the two-year creation cohort');
  await page.getByRole('button',{name:'Send clarification'}).click();
  await expect(page.getByLabel('Summary',{exact:true})).toHaveCount(2);
  expect(state.submissions[1].follow_up_to).toBe(runId);
});

for(const exploratory of [false,true]) {
  test(`older ${exploratory?'exploratory':'certified'} saved answers show their text as a visible summary fallback`,async({page})=>{
    const state=await fixture(page,{existing:true,exploratory});
    const text=state.workflow[runId].answer.text;
    await signIn(page,`/conversations/${thread}`);
    await expect(page.getByLabel('Summary',{exact:true})).toHaveText(text);
    await expect(page.getByLabel('Summary',{exact:true})).toBeVisible();
    await expect(page.locator('.answer-text')).toBeHidden();
  });
}

for(const [reason,title,text] of [
  ['data_unavailable','The data for this is not available','The data needed to answer this is not available to me.'],
  ['forecast','Forecasts are not available','I report recorded results only, so I cannot forecast future values.'],
  ['not_analytical','This is outside the business data','This question is outside the business data I can read.'],
  ['unsafe','I cannot do that','I can only read business data to answer analytical questions.'],
  ['query_failed','I could not answer this reliably','I could not build a reliable query within the time and safety limits.'],
  ['unsupported','This report is not enabled','This question needs a report that is not enabled yet.'],
]) {
  test(`${reason} explanations display their answer immediately without a dataset`,async({page})=>{
    const state=await fixture(page,{existing:true,explanation:true});
    state.workflow[runId].answer={kind:'explanation',reason,title,text,suggestions:[],notices:[]};
    await signIn(page,`/conversations/${thread}`);
    await expect(page.getByLabel('Summary',{exact:true})).toHaveText(text);
    await expect(page.getByLabel('Summary',{exact:true})).toBeVisible();
    await expect(page.getByText('Supporting figures',{exact:true})).toHaveCount(0);
    expect(state.resultReads).toBe(0);
  });
}

test('explanations answer in plain language and offer editable suggestions without loading a result',async({page})=>{
  const state=await fixture(page,{existing:true,explanation:true});
  const errors:string[]=[]; page.on('pageerror',e=>errors.push(e.message));
  await signIn(page,`/conversations/${thread}`);
  const card=page.getByRole('region',{name:"Forecasts aren't available"});
  await expect(card.getByRole('heading',{name:"Forecasts aren't available"})).toBeVisible();
  await expect(card.getByText("I report recorded results only, so I can't forecast or predict future values.",{exact:true})).toBeVisible();
  await expect(card.getByText('Invoices are recorded for completed months up to September 2026.',{exact:true})).toBeVisible();
  await expect(page.locator('.message .notice')).toHaveCount(0);
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
  await expect(page.getByText('Source update unconfirmed',{exact:true})).toBeVisible();
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
  await page.getByText('Supporting figures',{exact:true}).click();
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
