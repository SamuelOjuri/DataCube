import assert from 'node:assert/strict';
import {test} from 'node:test';
import {chartSpec, columnHeading, compareDecimal, formatCell, periodLabel} from '../src/presentation.mjs';
import {readEvents} from '../src/sse.mjs';

const fixture = () => ({id:'owned', columns:['category','value'], rows:[['A','20.00'],['B','10.00']],
  provenance:{metric_label:'Order Value',unit:'source_currency',columns:[{name:'value',unit:'source_currency'}],dataset_scope:'complete'}});
test('readable reporting periods retain exclusive boundaries and are independent of the browser timezone', () => {
  const original = process.env.TZ;
  try {
    for (const timezone of ['Europe/London', 'America/Los_Angeles', 'Pacific/Auckland']) {
      process.env.TZ = timezone;
      for (const [start_date, end_date_exclusive, expected] of [
        ['2026-09-01', '2026-10-01', 'September 2026'],
        ['2025-10-01', '2026-10-01', 'October 2025 to September 2026'],
        ['2024-02-01', '2024-03-01', 'February 2024'],
        ['2024-02-15', '2024-03-01', '15 February 2024 to 29 February 2024'],
        ['2026-09-30', '2026-10-01', '30 September 2026'],
        ['2024-10-22', null, 'Projects created on or after 22 October 2024'],
      ]) assert.equal(periodLabel({start_date, end_date_exclusive}), expected);
      assert.equal(periodLabel(null), 'All recorded history');
    }
  } finally {
    if (original === undefined) delete process.env.TZ;
    else process.env.TZ = original;
  }
});

test('charts use only owned inline data and permitted fields; unknowns are not zero', () => {
  const r = fixture(), intent = {kind:'bar',x:'category',y:'value'};
  assert.deepEqual(chartSpec(r,intent,'owned').spec.data.values.map(r=>r.value),[20,10]);
  for (const attack of [{...intent,data:{url:'https://evil'}},{...intent,config:'https://evil'},{...intent,x:'constructor'},{...intent,y:'expression'}, {...intent,kind:'line'}]) {
    assert.equal(chartSpec(r,attack,'owned').spec,null);
  }
  assert.equal(chartSpec(r,intent,'other').spec,null);
  r.provenance.truncated=true; assert.equal(chartSpec(r,intent,'owned').spec,null); r.provenance.truncated=false;
  r.rows[1][1]=null; assert.match(chartSpec(r,intent,'owned').reason,/Unknown/);
  r.rows[1][1]='9007199254740991000'; assert.equal(chartSpec(r,intent,'owned').spec,null);
});
test('chart axis sorts months and rejects repeated coordinates, mixed units and excessive series', () => {
  const r = fixture(); r.columns=['month','value']; r.rows=[['2026-09-01','1'],['2026-08-01','2']];
  assert.equal(chartSpec(r,{kind:'line',x:'month',y:'value'},'owned').spec.data.values[0].label,'2026-08-01');
  r.rows.push(['2026-08-01','3']); assert.equal(chartSpec(r,{kind:'line',x:'month',y:'value'},'owned').spec,null);
  const s = fixture(); s.provenance.columns[0].unit='days'; assert.equal(chartSpec(s,{kind:'bar',x:'category',y:'value'},'owned').spec,null);
  s.provenance.columns[0].unit='source_currency'; s.columns.push('type'); s.rows=Array.from({length:9},(_,i)=>['A',1,String(i)]);
  assert.equal(chartSpec(s,{kind:'bar',x:'category',y:'value'},'owned').spec,null);
});
test('bar and line charts use the Tapered Plus font, primary color and categorical palette', () => {
  const r = fixture();
  const {spec} = chartSpec(r,{kind:'bar',x:'category',y:'value'},'owned');
  assert.equal(spec.config.mark.color,'#931f1f');
  assert.equal(spec.config.font,'Open Sans');
  assert.equal(spec.config.axis.labelColor,'#666666');
  assert.equal(spec.config.range.category[0],spec.config.mark.color);
  assert.equal(new Set(spec.config.range.category).size,8);
  r.columns=['month','type','value']; r.rows=[['2026-08-01','PIR','20'],['2026-08-01','EPS','10']];
  const line = chartSpec(r,{kind:'line',x:'month',y:'value'},'owned').spec;
  assert.deepEqual(line.config,spec.config);
  assert.equal(line.encoding.color.field,'series');
});
test('financial formatting and sorting retain decimal precision and signed values', () => {
  assert.equal(formatCell('9007199254740993.01','source_currency'),'£9,007,199,254,740,993.01');
  assert.equal(compareDecimal('9007199254740993.01','9007199254740993.02'),-1);
  assert.equal(compareDecimal('-0.01','0'),-1);
  assert.equal(formatCell(null),'Unknown'); assert.equal(formatCell('0','ratio'),'0.0%');
});
test('rates use exact decimal percentage formatting and headers avoid repeated units', () => {
  for (const [value, unit, expected] of [['0.1837','ratio','18.4%'], ['18.37','percent','18.4%'],
    ['-0.0005','ratio','-0.1%'], ['1','ratio','100.0%'], ['9007199254740993.01','percent','9,007,199,254,740,993.0%']]) {
    assert.equal(formatCell(value,unit),expected);
  }
  assert.equal(columnHeading('won_invoiced_count',{label:'Won invoiced projects',unit:'count'}),'Won invoiced projects');
  assert.equal(columnHeading('inclusive_conversion',{label:'Inclusive conversion',unit:'ratio'}),'Inclusive conversion (%)');
});

const exploration = () => ({id:'owned',columns:['creation_month','inclusive','closed','wins'],
  rows:[['2026-09-01','0.2',null,2],['2026-08-01','0.1','0.5',1]],
  provenance:{kind:'exploratory',title:'Conversion by creation month',columns:[
    {name:'creation_month',label:'Creation month',role:'dimension',type:'date',time_grain:'month'},
    ...['inclusive','closed'].map(name=>({name,label:name,role:'measure',type:'decimal',unit:'ratio'})),
    {name:'wins',label:'Wins',role:'measure',type:'integer',unit:'count'}],dataset_scope:'complete'}});
test('exploratory charts map owned columns, separate units, sort months and preserve nulls', () => {
  const result=exploration(), intent={kind:'line',x:'creation_month',series:['inclusive','closed']};
  const {spec}=chartSpec(result,intent,'owned');
  assert.equal(spec.data.values[0].label,'2026-08-01');
  assert.equal(spec.data.values.find(v=>v.label==='2026-09-01' && v.series==='closed').value,null);
  assert.equal(spec.encoding.y.axis.format,'.0%');
  for (const attack of [{...intent,data:{url:'https://evil'}},{...intent,series:['wins','inclusive']},
    {...intent,series:['constructor']},{...intent,x:'wins'},{...intent,series:['inclusive','inclusive']}]) {
    assert.equal(chartSpec(result,attack,'owned').spec,null);
  }
  assert.equal(chartSpec(result,intent,'other').spec,null);
  result.provenance.query_limited=true;assert.equal(chartSpec(result,intent,'owned').spec,null);
  result.provenance.query_limited=false;
  result.rows.push(result.rows[0]);assert.equal(chartSpec(result,intent,'owned').spec,null);
});
test('absent calendar months break exploratory lines rather than implying uninterrupted data', () => {
  const result=exploration();result.rows[0][0]='2026-10-01';
  const {spec}=chartSpec(result,{kind:'line',x:'creation_month',series:['inclusive']},'owned');
  assert.equal(spec.data.values.find(v=>v.label==='2026-09-01').value,null);
  assert.equal(spec.mark.invalid,'break-paths-show-domains');
});
test('SSE accepts split UTF-8, CRLF and heartbeats without leaking partial answer frames',async () => {
  const payload = new TextEncoder().encode(': heartbeat\r\n\r\nid: 2\r\nevent: progress\r\ndata: {"stage":"query","text":"£"}\r\n\r\nevent: answer\ndata: {');
  let i=0,beats=0; const events=[];
  const stream = new ReadableStream({pull(controller){if(i<payload.length) controller.enqueue(payload.slice(i,i+=1)); else controller.close();}});
  await readEvents(new Response(stream,{headers:{'Content-Type':'text/event-stream'}}),e=>events.push(e),()=>beats++);
  assert.equal(events.length,1); assert.equal(events[0].id,2); assert.equal(events[0].payload.text,'£'); assert.ok(beats>2);
});
test('SSE rejects oversized and non-stream responses',async () => {
  await assert.rejects(readEvents(new Response('not events'),()=>{}),/Invalid/);
  await assert.rejects(readEvents(new Response('data: '+'x'.repeat(270000),{headers:{'Content-Type':'text/event-stream'}}),()=>{}),/too large/);
});
