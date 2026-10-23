import assert from 'node:assert/strict';
import {test} from 'node:test';
import {chartSpec} from '../src/presentation.mjs';
import {comparisonFixture} from './comparison-fixture.mjs';

test('paired series align equivalent months with exact source dates and an incomplete-month cutoff', () => {
  const {result, intent} = comparisonFixture();
  result.rows.reverse();
  const {spec} = chartSpec(result, intent, result.id);
  assert.equal(spec.data.values.length, 22);
  assert.deepEqual(spec.data.values.slice(0, 2).map(v => v.value), [709552.98, 737328.31]);
  assert.equal(spec.data.values[1].label, '2025-11-01');
  assert.equal(spec.data.values[1].actualMonth, '2024-11-01');
  assert.equal(spec.data.values.at(-1).label, '2026-09-01');
  assert.deepEqual(spec.encoding.color.scale.domain, ['Nov 2025 – Sept 2026', 'Nov 2024 – Sept 2025']);
  assert.deepEqual(spec.encoding.strokeDash.scale.range, [[1, 0], [6, 4]]);
  assert.equal(spec.data.values.at(-1).difference, 30000);
  assert.ok(spec.encoding.tooltip.some(t => t.field === 'relative'));
  assert.equal(spec.mark.invalid, 'break-paths-show-domains');
});

test('unknown and absent month pairs leave gaps; zero baselines have no percentage change', () => {
  const {result, intent} = comparisonFixture();
  result.rows[0][5] = '0';
  result.rows[1][2] = null;
  result.rows.splice(2, 1);
  const {spec} = chartSpec(result, intent, result.id);
  assert.equal(spec.data.values[0].relative, null);
  assert.equal(spec.data.values.find(v => v.label === '2025-12-01').value, null);
  const gap = spec.data.values.filter(v => v.label === '2026-01-01');
  assert.equal(gap.length, 2);
  assert.ok(gap.every(v => v.value === null && v.difference === null));
});

test('the same chart supports rates with percentage-point changes and arbitrary aliases', () => {
  const {result, intent} = comparisonFixture();
  result.provenance.columns[2].unit = result.provenance.columns[5].unit = 'ratio';
  for (const row of result.rows) {row[2] = '0.3'; row[5] = '0.2';}
  result.columns[2] = result.provenance.columns[2].name = 'conversion_now';
  result.columns[5] = result.provenance.columns[5].name = 'conversion_before';
  intent.series = ['conversion_now', 'conversion_before'];
  intent.comparison.current_value = intent.series[0]; intent.comparison.baseline_value = intent.series[1];
  const {spec} = chartSpec(result, intent, result.id);
  assert.equal(spec.encoding.y.axis.format, '.0%');
  assert.ok(Math.abs(spec.data.values[0].difference - 10) < 1e-10);
  assert.ok(spec.encoding.tooltip.some(t => t.title === 'Difference (percentage points)'));
  assert.ok(!spec.encoding.tooltip.some(t => t.field === 'relative'));
});

test('preceding monthly periods use their validated offset rather than assuming a year', () => {
  const {result, intent} = comparisonFixture();
  result.rows = result.rows.slice(2, 5);
  result.rows.forEach((row, i) => {row[1] = `2026-0${i+4}-01`; row[4] = `2026-0${i+1}-01`; row[0] = ['Apr','May','Jun'][i];});
  intent.comparison.kind = 'previous_period';
  const {spec} = chartSpec(result, intent, result.id);
  assert.equal(spec.data.values[1].label, '2026-04-01');
  assert.equal(spec.data.values[1].actualMonth, '2026-01-01');
});

test('comparison charts reject invalid pairings, extra breakdowns, unsupported fields and limited or unowned results', () => {
  const mutations = [
    (r, i) => {i.comparison.baseline_value = 'missing';},
    (r, i) => {i.comparison.kind = 'unsupported';},
    (r, i) => {i.comparison.expression = 'evil()';},
    (r, i) => {i.data = {url: 'https://example.com'};},
    r => {r.rows.push(r.rows[0]);},
    r => {r.rows[0][4] = '2023-11-01';},
    r => {r.rows[0][1] = '2025-11-02';},
    r => {r.rows[0][0] = 'Jan';},
    r => {r.provenance.columns[0].name = r.columns[0] = 'account';},
    r => {r.provenance.columns[5].unit = 'days';},
    r => {r.rows[0][2] = '900719925474099300';},
    r => {r.provenance.query_limited = true;},
    r => {r.provenance.truncated = true;},
  ];
  for (const mutate of mutations) {
    const {result, intent} = comparisonFixture(); mutate(result, intent);
    assert.equal(chartSpec(result, intent, result.id).spec, null);
  }
  const {result, intent} = comparisonFixture();
  assert.equal(chartSpec(result, intent, 'unowned').spec, null);
});
