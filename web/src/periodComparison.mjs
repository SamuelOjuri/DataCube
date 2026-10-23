import {advance, bucket, bucketLabel, coordinate, iso, periodStart, samePriorYear} from './calendar.mjs';
// Validate column references and calendar alignment again at the rendering boundary.
// All values remain inline and owned by the result; comparison metadata contains no expressions.
export function periodComparisonData(result, intent) {
  const no = reason => ({reason});
  const legacy = !intent.period_comparison, pairing = intent.period_comparison || intent.comparison, p = result.provenance;
  const grain = legacy ? 'month' : pairing?.time_grain;
  const fields = [legacy ? 'current_month' : 'current_axis', legacy ? 'baseline_month' : 'baseline_axis', 'current_value', 'baseline_value'];
  const completed = legacy ? 'completed_months_only' : 'completed_periods_only';
  if (intent.comparison && intent.period_comparison || !pairing
      || Object.keys(pairing).some(k => ![...fields, 'kind', completed, ...(legacy ? [] : ['time_grain'])].includes(k))
      || !['year_over_year', 'previous_period'].includes(pairing.kind) || typeof pairing[completed] !== 'boolean'
      || intent.time_grain && intent.time_grain !== grain
      || intent.kind !== 'line' || intent.x !== pairing[fields[0]] || intent.series.length !== 2
      || intent.series[0] !== pairing.current_value || intent.series[1] !== pairing.baseline_value) return no('Invalid monthly comparison.');
  const names = fields.map(k => pairing[k]);
  const indices = names.map(n => result.columns.indexOf(n));
  const columns = names.map(n => p.columns.find(c => c.name === n));
  if (new Set(names).size !== 4 || indices.some(i => i < 0) || columns.some(c => !c)
      || columns.slice(0, 2).some(c => c.role !== 'dimension' || c.type !== 'date' || c.time_grain && c.time_grain !== grain)
      || !['source_currency', 'count', 'ratio', 'percent', 'days'].includes(columns[2].unit)
      || columns.slice(2).some(c => c.role !== 'measure' || !['integer', 'decimal'].includes(c.type) || c.unit !== columns[2].unit)) return no('Comparison fields or units do not match.');
  const extras = p.columns.filter(c => c.role === 'dimension' && !names.slice(0, 2).includes(c.name));
  if (extras.some(c => !['month_label', 'fiscal_month_label'].includes(c.name) || c.type !== 'text')) return no('Use the table for additional breakdowns.');
  if (!result.rows.length || result.rows.length > 200) return no('Use the table for this dataset size.');
  const date = raw => bucket(raw, grain);
  const numeric = raw => raw === null || /^-?\d+(\.\d+)?$/.test(String(raw)) && Number.isFinite(Number(raw)) && Math.abs(Number(raw)) <= Number.MAX_SAFE_INTEGER;
  let cutoff = null;
  if (pairing[completed]) {
    try {
      const parts = new Intl.DateTimeFormat('en-GB', {year: 'numeric', month: '2-digit', day: '2-digit', timeZone: p.business_timezone})
        .formatToParts(new Date(p.freshness.queried_at));
      const today = `${parts.find(p => p.type === 'year').value}-${parts.find(p => p.type === 'month').value}-${parts.find(p => p.type === 'day').value}`;
      cutoff = iso(periodStart(new Date(today + 'T00:00:00Z'), grain));
    } catch { return no('The completed-period boundary is unavailable.'); }
  }
  const currentDates = new Set(), baselineDates = new Set(), offsets = new Set(), pairs = [];
  for (const row of result.rows) {
    const [current, baseline] = indices.slice(0, 2).map(i => date(row[i]));
    if (!current || !baseline || currentDates.has(row[indices[0]]) || baselineDates.has(row[indices[1]])) return no('Comparison periods must be valid and unique.');
    if (pairing.kind === 'year_over_year' && !samePriorYear(current, baseline, grain)) return no('Comparison periods are not consistently aligned.');
    const monthLabel = current.toLocaleDateString('en-GB', {month: 'long', timeZone: 'UTC'}).toLowerCase();
    const labels = [monthLabel, monthLabel.slice(0, 3), ...(current.getUTCMonth() === 8 ? ['sept'] : [])];
    if (extras.some(c => !labels.includes(String(row[result.columns.indexOf(c.name)]).toLowerCase()))) return no('Month labels do not match the comparison dates.');
    currentDates.add(row[indices[0]]); baselineDates.add(row[indices[1]]);
    offsets.add(coordinate(current, grain) - coordinate(baseline, grain));
    const raw = indices.slice(2).map(i => row[i]);
    if (!raw.every(numeric)) return no('Use the table to retain numerical precision.');
    if (!cutoff || row[indices[0]] < cutoff) pairs.push({label: row[indices[0]], baselineMonth: row[indices[1]],
      current: raw[0] === null ? null : Number(raw[0]), baseline: raw[1] === null ? null : Number(raw[1])});
  }
  const offset = [...offsets][0];
  if (pairing.kind === 'previous_period' && offsets.size !== 1 || offset <= 0
      || [...baselineDates].sort().at(-1) >= [...currentDates].sort()[0]) return no('Comparison periods are not consistently aligned.');
  if (!pairs.length || pairs.every(p => p.current === null && p.baseline === null)) return no('No known values are available for these completed periods.');
  pairs.sort((a, b) => a.label.localeCompare(b.label));
  const monthName = value => bucketLabel(value, grain);
  const range = values => values[0] === values.at(-1) ? monthName(values[0]) : `${monthName(values[0])} – ${monthName(values.at(-1))}`;
  const series = [range(pairs.map(p => p.label)), range(pairs.map(p => p.baselineMonth))];
  const last = pairs.at(-1).label, byDate = new Map(pairs.map(p => [p.label, p]));
  // Add explicit unknown buckets so absent periods break both lines.
  for (let month = date(pairs[0].label); iso(month) <= last; month = advance(month, grain)) {
    const key = month.toISOString().slice(0, 10);
    if (!byDate.has(key)) {
      pairs.push({label: key, baselineMonth: null, current: null, baseline: null});
    }
    if (pairs.length > 200) return no('Use the table for this time range.');
  }
  pairs.sort((a, b) => a.label.localeCompare(b.label));
  const unit = columns[2].unit, rate = unit === 'ratio' || unit === 'percent';
  const values = pairs.flatMap(pair => {
    const known = pair.current !== null && pair.baseline !== null;
    const difference = known ? (pair.current - pair.baseline) * (unit === 'ratio' ? 100 : 1) : null;
    return [pair.current, pair.baseline].map((value, i) => ({...pair, value, series: series[i],
      actualMonth: i === 0 ? pair.label : pair.baselineMonth, difference,
      relative: known && !rate && pair.baseline > 0 ? (pair.current - pair.baseline) / pair.baseline : null}));
  });
  return {reason: '', values, series, unit, rate, grain,
    title: columns[2].label.replace(/^(current(?: period| year)?|this year)\s+/i, '') + ' comparison'};
}
