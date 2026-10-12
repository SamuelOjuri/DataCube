import type {Page} from '@playwright/test';
export const thread = '10000000-0000-4000-8000-000000000001';
export const resultId = '20000000-0000-4000-8000-000000000001';
export const runId = '30000000-0000-4000-8000-000000000001';
export const result = {id:resultId,run_id:runId,columns:['category','value','source_rows','known_values'],
  rows:Array.from({length:12},(_,i)=>[i === 0 ? '=Untrusted CRM text' : `Category ${i}`,`${1000-i*25}.00`,2,2]),
  provenance:{metric_id:'order_parent_value',metric_version:'1.0.0',metric_label:'Order Value (parent mirror)',unit:'source_currency',
    source_population:'reportable',source_relation:'analyst_query.projects_v1',source_grain:'One reportable project',catalogue_version:'1.1.0',catalogue_sha256:'synthetic-catalogue',query_reference:'test-query-reference',
    request:{period:'all_stored',filters:[],dimensions:['category']},resolved_period:null,business_timezone:'Europe/London',
    columns:[{name:'category',type:'text'},{name:'value',type:'decimal',unit:'source_currency'},{name:'source_rows',type:'integer',unit:'count'},{name:'known_values',type:'integer',unit:'count'}],
    total:{value:'10350.00',source_rows:24,known_values:23},total_scope:'complete_filtered_population',
    freshness:{status:'unknown',queried_at:'2026-10-08T12:00:00Z',limitation:'Successful ingestion and refresh evidence is unavailable.'},
    coverage:{incomplete_order_inputs:1},coverage_scope:'gateway_population_unfiltered',limitations:['One source value is unknown.'],
    truncated:false,returned_rows:12,matched_groups:12,dataset_scope:'complete',evaluation_only:true}};
export const exploratoryResult = {id:resultId,run_id:runId,columns:['account','revenue'],
  rows:[['=Untrusted account','1200.00'],['Account B','300.50'],[null,'10.00']],
  provenance:{kind:'exploratory',title:'Invoiced revenue by account, 1 January 2026 to 31 January 2026',
    sql:"SELECT account, SUM(amount_invoiced) AS revenue FROM analyst_query.invoice_reporting_facts_v1 GROUP BY account",
    relations:['invoice_reporting_facts_v1','projects_v1'],business_timezone:'Europe/London',catalogue_version:'1.1.0',
    query_reference:'exploratory-reference',columns:[{name:'account',label:'Account',role:'dimension',unit:'none',type:'text'},
      {name:'revenue',label:'Revenue',role:'measure',unit:'source_currency',type:'decimal'}],
    freshness:{status:'unknown',queried_at:'2026-10-12T09:00:00Z',limitation:'Successful ingestion and refresh evidence is unavailable.'},
    limitations:['Exploratory analysis: generated SQL over governed views, not a certified metric.'],
    caveats:['Invoices without a recorded account are grouped as an unknown account.'],
    truncated:false,returned_rows:3,dataset_scope:'complete'}};
export const explanationAnswer = {kind:'explanation',reason:'forecast',title:"Forecasts aren't available",
  text:"I report recorded results only, so I can't forecast or predict future values.\nInvoices are recorded for completed months up to September 2026.",
  suggestions:['Show monthly invoiced revenue for the last 12 completed months.'],notices:[]};

export async function fixture(page: Page, {clarify=false, hold=false, existing=false, disconnect=false, comparison=false, limited=false, exploratory=false, explanation=false,
  beforeConversationReply=async()=>{}, beforeMessageReply=async()=>{}, beforeResumeReply=async()=>{}}={}) {
  const state = {result:structuredClone(exploratory ? exploratoryResult as any : result), runs: [] as any[], workflow: {} as Record<string,any>, submissions: [] as any[], replies: [] as any[], events: [] as string[], exports:0, feedback:[] as any[], expired:false, streams:0, failReply:false, failSubmission:false, drilldowns:0, resultReads:0};
  const messageRuns = new Map<string,string>();
  const answer = (id: string) => explanation ? structuredClone(explanationAnswer) : ({text:exploratory ? 'Invoiced revenue by account.\nAccount B — Revenue: GBP 300.50.' : 'Order Value is GBP 10,350.00. This is a complete filtered total.',
    result:{result_id:resultId},chart:limited||exploratory?{kind:'table',x:null,y:'value'}:{kind:'bar',x:'category',y:'value'},notices:['Measured contributions do not establish cause.'],
    scope_changes:id !== runId && state.submissions.at(-1)?.follow_up_to ? {filters:{before:[],after:['category: A']}} : {}});
  function create(question: string) {
    const id = state.runs.length ? `30000000-0000-4000-8000-${String(state.runs.length+1).padStart(12,'0')}` : runId;
    state.runs.unshift({id,conversation_id:thread,question,status:'running',created_at:'2026-10-08T12:00:00Z'});
    state.workflow[id]={run_id:id,conversation_id:thread,status:clarify?'awaiting_clarification':'running',answer:null,
      clarification:clarify?{id:'40000000-0000-4000-8000-000000000001',question:'Which reporting period?',options:['All stored history','Last completed month']}:null};
    return state.workflow[id];
  }
  if(existing) {const run=create('Show stored Order Value by category.');run.status='completed';run.answer=answer(run.run_id);}
  await page.route('http://127.0.0.1:4174/**', async route => {
    const request=route.request(), url=new URL(request.url()), path=url.pathname;
    const headers={'Access-Control-Allow-Origin':'http://127.0.0.1:4173','Access-Control-Allow-Headers':'authorization,content-type','Access-Control-Allow-Methods':'GET,POST,OPTIONS','Access-Control-Expose-Headers':'X-Export-Scope'};
    const reply=(body:unknown,status=200,extra={})=>route.fulfill({status,headers:{...headers,'Content-Type':'application/json',...extra},body:JSON.stringify(body)});
    if(request.method()==='OPTIONS') {await reply({});return;}
    if(path==='/auth/login'){await route.fulfill({status:302,headers:{location:'http://127.0.0.1:4173/auth/callback#code='+'C'.repeat(43)}});return;}
    if(path==='/auth/exchange'){state.expired=false; await reply({access_token:'S'.repeat(43),subject:'synthetic-user',expires_at:new Date(Date.now()+900000).toISOString(),expires_in:900});return;}
    if(state.expired){await reply({detail:'session_expired'},401);return;}
    if(!request.headers().authorization?.startsWith('Bearer ')){await reply({detail:'unauthorized'},401);return;}
    if(path.startsWith('/auth/')){await reply({});return;}
    if(path==='/v1/metrics'){await reply({catalogue_sha256:'synthetic-catalogue',metrics:[{id:'order_parent_value',version:'1.0.0',source:{priority:['Verified parent mirror.']},calculation:{aggregation:'sum',nulls:'Unknown values stay unknown.',zeroes:'Zero is zero.',negatives:'Signed values retained.',empty:'No known values means unknown.'}}]});return;}
    if(path==='/v1/conversations') {const conversation={id:thread,title:'Synthetic analysis',created_at:'2026-10-08T12:00:00Z'};if(request.method()==='POST') await beforeConversationReply();await reply(request.method()==='POST'?conversation:state.runs.length?[conversation]:[]);return;}
    if(path===`/v1/conversations/${thread}`){await reply({id:thread,title:'Synthetic analysis'});return;}
    if(path.endsWith('/runs')) {await reply(state.runs);return;}
    if(path.endsWith('/messages')) {
      const body=request.postDataJSON();state.submissions.push(body);
      const previous=messageRuns.get(body.idempotency_key);
      const workflow=previous ? state.workflow[previous] : create(body.question);
      messageRuns.set(body.idempotency_key,workflow.run_id);
      await beforeMessageReply();
      if(state.failSubmission){state.failSubmission=false;await route.abort();return;}
      await reply(workflow,202);return;
    }
    const id=path.split('/')[3], w=state.workflow[id];
    if(path.endsWith('/workflow')) {await reply(w || {},w?200:404);return;}
    if(path.endsWith('/resume')) {
      state.replies.push(request.postDataJSON()); w.status='completed';w.clarification=null;w.answer=answer(id);
      await beforeResumeReply();
      if(state.failReply){state.failReply=false;await route.abort();return;}
      await reply(w);return;
    }
    if(path.endsWith('/cancel')){w.status='cancelled';await reply({...state.runs.find(r=>r.id===id),status:'cancelled'});return;}
    if(path.endsWith('/events')) {
      state.events.push(url.search);state.streams++;
      if(!hold && !(disconnect && state.streams===1)) {w.status='completed';w.answer=answer(id);}
      const event=state.streams===1?'id: 1\nevent: progress\ndata: {"stage":"querying"}\n\n':`id: 2\nevent: terminal\ndata: {"status":"${w.status}"}\n\n`;
      await route.fulfill({headers:{...headers,'Content-Type':'text/event-stream'},body:': heartbeat\n\n'+event});return;
    }
    if(path===`/v1/results/${resultId}`){state.resultReads++;const saved=state.result;await reply(limited ? {...saved,provenance:{...saved.provenance,truncated:true,dataset_scope:'limited',matched_groups:15}} : comparison ? {...saved,provenance:{...saved.provenance,
      comparison_period:{start_date:'2026-08-01',end_date_exclusive:'2026-09-01'},
      comparison:{baseline:{value:'0.00'},absolute_change:'10350.00',percentage_change:null,percentage_point_change:null,zero_denominator:true}}} : saved);return;}
    if(path.endsWith('/projects')){state.drilldowns++;await reply({columns:['project_id','source_value'],rows:[['project-1','1000.00']],has_more:false,offset:0,queried_at:'2026-10-08T12:01:00Z',limitation:'Live project evidence for the saved plan; not a historical snapshot.'});return;}
    if(path.endsWith('/export')){state.exports++;await route.fulfill({headers:{...headers,'Content-Type':'text/csv','X-Export-Scope':'stored-result-rows'},body:"category,value\n'=Untrusted CRM text,1000.00\n"});return;}
    if(path.endsWith('/feedback')){state.feedback.push(request.postDataJSON());await reply({rating:'helpful'});return;}
    await reply({detail:'not_found'},404);
  });
  return state;
}
export async function signIn(page:Page,path='/') {await page.goto(path);await page.getByRole('button',{name:'Continue with Monday'}).click();await page.getByRole('button',{name:'Sign out'}).waitFor();}
