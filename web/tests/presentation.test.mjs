import assert from 'node:assert/strict';
import {test} from 'node:test';
import {chartSpec, compareDecimal, formatCell} from '../src/presentation.mjs';
import {readEvents} from '../src/sse.mjs';

const fixture = () => ({id:'owned', columns:['category','value'], rows:[['A','20.00'],['B','10.00']],
  provenance:{metric_label:'Order Value',unit:'source_currency',columns:[{name:'value',unit:'source_currency'}],dataset_scope:'complete'}});
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
test('financial formatting and sorting retain decimal precision and signed values', () => {
  assert.equal(formatCell('9007199254740993.01','source_currency'),'£9,007,199,254,740,993.01');
  assert.equal(compareDecimal('9007199254740993.01','9007199254740993.02'),-1);
  assert.equal(compareDecimal('-0.01','0'),-1);
  assert.equal(formatCell(null),'Unknown'); assert.equal(formatCell('0','ratio'),'0');
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
