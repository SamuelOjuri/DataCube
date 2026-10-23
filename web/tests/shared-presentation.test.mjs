import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import test from 'node:test';
import {compile} from 'vega-lite';
import {parse, View} from 'vega';
import {expressionInterpreter} from 'vega-interpreter';
import {chartSpec} from '../src/presentation.mjs';
import {comparisonFixture} from './comparison-fixture.mjs';

const fixtures = JSON.parse(readFileSync(new URL('./summary-fixtures.json', import.meta.url), 'utf8'));
const render = (item, intent = item.answer.exploratory_chart || item.answer.chart, id = item.result.id) => chartSpec(item.result, intent, id);

test('backend-selected chart templates compile and render using the restricted interpreter', async () => {
  for (const name of ['ranking', 'comparison', 'daily', 'quarterly', 'distribution', 'relationship']) {
    const {spec, reason} = render(fixtures[name]);
    assert.ok(spec, `${name}: ${reason}`);
    const runtime = parse(compile(spec).spec, undefined, {ast: true});
    const view = new View(runtime, {renderer: 'none', expr: expressionInterpreter});
    await view.runAsync();
    const svg = await view.toSVG();
    assert.match(svg, /<svg/);
    view.finalize();
  }
});

test('daily gaps, quarterly order, rankings and distributions represent the actual observations', () => {
  const daily = render(fixtures.daily).spec;
  assert.equal(daily.data.values.find(v => v.label === '2026-10-22').value, null);
  assert.equal(daily.data.values.find(v => v.label === '2026-10-21').value, null);
  assert.deepEqual(render(fixtures.quarterly).spec.encoding.x.sort, ['Q1 2026', 'Q2 2026', 'Q3 2026']);
  assert.equal(render(fixtures.ranking).spec.encoding.y.sort, 'x');
  assert.equal(render(fixtures.distribution).spec.data.values.length, 4);
  assert.deepEqual(render(fixtures.relationship).spec.data.values, [{x: 10, y: 1}, {x: 20, y: 2}, {x: 30, y: 3}]);
  assert.deepEqual(render(fixtures.comparison).spec.data.values.map(v => v.value), [0.6, 0.4]);
});

test('unknown, empty and unsupported interpretations retain summaries without fabricated charts', () => {
  for (const name of ['total', 'fallback', 'records', 'empty', 'unknown']) {
    assert.ok(fixtures[name].answer.summary.trim());
    assert.equal(render(fixtures[name]).spec, null);
  }
});

test('new templates reject unowned, limited, misreferenced and unsafe numerical data', () => {
  for (const name of ['ranking', 'comparison', 'daily', 'quarterly', 'distribution', 'relationship']) {
    const item = fixtures[name];
    assert.equal(render(item, undefined, 'another-result').spec, null);
    assert.equal(render(item, {...item.answer.exploratory_chart, transform: [{calculate: '1'}]}).spec, null);
    assert.equal(render(item, {...item.answer.exploratory_chart, x: 'absent'}).spec, null);
    for (const flag of ['query_limited', 'truncated']) {
      const limited = structuredClone(item); limited.result.provenance[flag] = true;
      assert.equal(render(limited).spec, null);
    }
    for (const value of ['NaN', 'Infinity', '9007199254740993', 'datum.x']) {
      const changed = structuredClone(item), column = changed.result.provenance.columns.findIndex(c => c.role === 'measure');
      changed.result.rows[0][column] = value;
      assert.equal(render(changed).spec, null);
    }
  }
});

test('nonmonthly period comparisons validate calendar and ISO week alignment', () => {
  for (const [grain, dates] of [
    ['day', [['2026-10-20', '2025-10-20'], ['2026-10-22', '2025-10-22']]],
    ['week', [['2026-01-05', '2025-01-06'], ['2026-01-19', '2025-01-20']]],
    ['quarter', [['2026-04-01', '2025-04-01'], ['2026-07-01', '2025-07-01']]],
    ['year', [['2025-01-01', '2024-01-01']]],
  ]) {
    const {result} = comparisonFixture();
    result.provenance.columns = [
      {name: 'a', label: 'Current period', role: 'dimension', unit: 'none', type: 'date', time_grain: grain},
      {name: 'b', label: 'Baseline period', role: 'dimension', unit: 'none', type: 'date', time_grain: grain},
      {name: 'current', label: 'Current revenue', role: 'measure', unit: 'source_currency', type: 'decimal'},
      {name: 'prior', label: 'Prior revenue', role: 'measure', unit: 'source_currency', type: 'decimal'}];
    result.columns = ['a', 'b', 'current', 'prior'];
    result.rows = dates.map(([a, b]) => [a, b, '120', '100']);
    const intent = {kind: 'line', x: 'a', series: ['current', 'prior'], time_grain: grain,
      period_comparison: {current_axis: 'a', baseline_axis: 'b', current_value: 'current', baseline_value: 'prior',
        time_grain: grain, kind: 'year_over_year', completed_periods_only: true}};
    const rendered = chartSpec(result, intent, result.id);
    assert.ok(rendered.spec, `${grain}: ${rendered.reason}`);
    if (['day', 'week'].includes(grain)) assert.equal(rendered.spec.data.values.filter(v => v.value === null).length, 2);
    result.rows[0][1] = '2024-01-02';
    assert.equal(chartSpec(result, intent, result.id).spec, null);
  }
});

test('certified aggregate comparisons chart the saved totals rather than summing displayed rows', async () => {
  const result = {id: 'saved', columns: ['value'], rows: [['5']], provenance: {metric_label: 'Revenue', unit: 'source_currency',
    columns: [{name: 'value', unit: 'source_currency'}], total: {value: '100'}, comparison: {baseline: {value: '80'}},
    resolved_period: {start_date: '2026-09-01', end_date_exclusive: '2026-10-01'},
    comparison_period: {start_date: '2026-08-01', end_date_exclusive: '2026-09-01'}}};
  const {spec} = chartSpec(result, {kind: 'comparison', x: null, y: 'value'}, result.id);
  assert.deepEqual(spec.data.values.map(v => v.value), [100, 80]);
  assert.match(spec.data.values[0].label, /September 2026/);
  const view = new View(parse(compile(spec).spec, undefined, {ast: true}), {renderer: 'none', expr: expressionInterpreter});
  await view.runAsync(); view.finalize();
});
