export const unitLabel = unit => ({source_currency: 'GBP', ratio: '%', percent: '%', days: 'days', count: 'count', none: ''}[unit] ?? unit ?? '');
// Display copy stays separate from the fingerprinted catalogue and saved evidence.
export function metricPresentation(provenance) {
  if (provenance.metric_id === 'invoice_monthly_actual' && provenance.metric_version === '1.1.0') {
    return {
      label: 'Monthly Invoiced Revenue',
      definition: 'Positive invoice amounts grouped by invoice date for reportable projects, including archived projects, regardless of project stage. Only completed months are included. Undated invoices and negative amounts are excluded; months without qualifying invoices are shown as zero.',
    };
  }
  return {label: provenance.metric_label, definition: null};
}
// These are calendar dates already resolved in the business timezone. UTC keeps
// browser timezones from shifting their day/month during presentation.
export function periodLabel(period) {
  if (!period) return 'All recorded history';
  const start = new Date(`${period.start_date}T00:00:00Z`);
  const format = (date, monthOnly = false) => date.toLocaleDateString('en-GB', {
    ...(monthOnly ? {} : {day: 'numeric'}), month: 'long', year: 'numeric', timeZone: 'UTC',
  });
  if (!period.end_date_exclusive) return `Projects created on or after ${format(start)}`;
  const end = new Date(`${period.end_date_exclusive}T00:00:00Z`);
  const last = new Date(end.getTime() - 86400000);
  if (start.getUTCDate() === 1 && end.getUTCDate() === 1) {
    const firstMonth = format(start, true), lastMonth = format(last, true);
    return firstMonth === lastMonth ? firstMonth : `${firstMonth} to ${lastMonth}`;
  }
  return start.getTime() === last.getTime() ? format(start) : `${format(start)} to ${format(last)}`;
}

export function columnHeading(name, column) {
  const label = column?.label || name.replaceAll('_', ' ');
  const unit = unitLabel(column?.unit);
  const repeated = column?.unit === 'count' && /\b(count|projects|records)\b/i.test(label)
    || column?.unit === 'days' && /\bdays\b/i.test(label);
  return label + (unit && !repeated ? ` (${unit})` : '');
}
const chartTheme = {
  background: 'transparent', view: {stroke: null}, mark: {color: '#931f1f'}, font: 'Open Sans',
  range: {category: ['#931f1f', '#262626', '#737373', '#c14c4c', '#4b5563', '#691616', '#9b6a58', '#6b5c7b']},
  axis: {labelFontSize: 12, titleFontSize: 12, labelColor: '#666666', titleColor: '#262626', domainColor: '#8a8a8a', tickColor: '#8a8a8a', gridColor: '#e6e6e6'},
  legend: {labelColor: '#666666', titleColor: '#262626'},
};
// Round the decimal string using integer arithmetic, without losing database precision.
function percentage(value, ratio) {
  const negative = String(value).startsWith('-');
  const [whole, fraction = ''] = String(value).replace(/^-/, '').split('.');
  let scaled = BigInt(whole + fraction) * (ratio ? 1000n : 10n);
  const divisor = 10n ** BigInt(fraction.length);
  scaled = (scaled + divisor / 2n) / divisor;
  return `${negative && scaled !== 0n ? '-' : ''}${(scaled / 10n).toLocaleString('en-GB')}.${scaled % 10n}%`;
}
// Preserve decimal strings from PostgreSQL, including values beyond JS integer precision.
export function formatCell(value, unit) {
  if (value === null || value === undefined) return 'Unknown';
  if (!/^-?\d+(\.\d+)?$/.test(String(value))) return String(value);
  if (unit === 'ratio' || unit === 'percent') return percentage(value, unit === 'ratio');
  const [whole, fraction] = String(value).split('.');
  const number = whole.replace(/\B(?=(\d{3})+(?!\d))/g, ',') + (fraction === undefined ? '' : '.' + fraction);
  return unit === 'source_currency' ? '£' + number : number + (unit === 'days' ? ' days' : '');
}
export function compareDecimal(a, b) {
  if (a === null) return b === null ? 0 : 1;
  if (b === null) return -1;
  const aa = String(a), bb = String(b);
  if (!/^-?\d+(\.\d+)?$/.test(aa) || !/^-?\d+(\.\d+)?$/.test(bb)) return aa.localeCompare(bb);
  const scale = Math.max(aa.split('.')[1]?.length || 0, bb.split('.')[1]?.length || 0);
  const integer = s => {const [w, f = ''] = s.split('.'); return BigInt(w + f.padEnd(scale, '0'));};
  const delta = integer(aa) - integer(bb);
  return delta < 0n ? -1 : delta > 0n ? 1 : 0;
}
export function chartSpec(result, intent, ownedId) {
  if (result.provenance.kind === 'exploratory') return exploratoryChartSpec(result, intent, ownedId);
  const no = reason => ({spec: null, reason});
  if (result.id !== ownedId || !intent || Object.keys(intent).some(k => !['kind', 'x', 'y'].includes(k))) return no('Chart intent is not valid for this result.');
  if (['table', 'number'].includes(intent.kind)) return no('');
  if (!['bar', 'line'].includes(intent.kind) || !['month', 'category', 'type'].includes(intent.x) || intent.y !== 'value') return no('Unsupported chart fields.');
  const p = result.provenance;
  if (p.truncated || p.dataset_scope === 'limited') return no('Limited results are shown in the table to avoid an incomplete chart.');
  const x = result.columns.indexOf(intent.x), y = result.columns.indexOf('value');
  if (x < 0 || y < 0 || !['source_currency', 'ratio', 'days', 'count'].includes(p.unit) || p.columns.find(c => c.name === 'value')?.unit !== p.unit) return no('Chart fields or units do not match.');
  if (intent.kind === 'line' && intent.x !== 'month') return no('A line chart requires a monthly time axis.');
  if (!result.rows.length || result.rows.length > 240) return no('Use the table for this dataset size.');
  const seriesFields = result.columns.filter(k => ['month', 'category', 'type'].includes(k) && k !== intent.x);
  if (seriesFields.length > 1) return no('Use the table for multiple breakdowns.');
  const seriesIndex = seriesFields.length ? result.columns.indexOf(seriesFields[0]) : -1;
  const seen = new Set(), series = new Set();
  const values = [];
  for (const row of result.rows) {
    if (row[y] === null) return no('Unknown values are shown in the table; they are not plotted as zero.');
    const value = Number(row[y]);
    if (!/^-?\d+(\.\d+)?$/.test(String(row[y])) || !Number.isFinite(value) || Math.abs(value) > Number.MAX_SAFE_INTEGER) return no('Use the table to retain numerical precision.');
    const key = row[x] === null ? '(Unclassified)' : String(row[x]);
    if (intent.x === 'month' && !/^\d{4}-\d{2}-01$/.test(key)) return no('Invalid monthly axis.');
    const group = seriesIndex < 0 ? 'Value' : row[seriesIndex] === null ? '(Unclassified)' : String(row[seriesIndex]);
    const coordinate = JSON.stringify([key, group]);
    if (seen.has(coordinate)) return no('Use the table for repeated chart coordinates.');
    seen.add(coordinate); series.add(group);
    values.push({label: key, value, series: group});
  }
  if (series.size > 8) return no('Use the table for more than eight series.');
  if (intent.x === 'month') values.sort((a, b) => a.label.localeCompare(b.label));
  const horizontal = intent.kind === 'bar' && intent.x !== 'month';
  const categoryAxis = {field: 'label', type: 'ordinal', title: null, sort: horizontal && seriesIndex < 0 ? '-x' : null,
    axis: {labelAngle: horizontal ? 0 : -35, labelLimit: 180}};
  const valueAxis = {field: 'value', type: 'quantitative', title: unitLabel(p.unit), scale: {zero: true}, stack: null,
    axis: {format: p.unit === 'source_currency' ? '~s' : p.unit === 'ratio' ? '.0%' : undefined, tickCount: 5}};
  return {reason: '', spec: {
    data: {values}, width: 560, height: horizontal ? Math.max(250, new Set(values.map(v => v.label)).size * 26) : 250,
    description: `${metricPresentation(p).label} (${unitLabel(p.unit)}). ${p.dataset_scope === 'limited' ? 'Limited result.' : 'Stored result.'} Exact values are in the table.`,
    mark: {type: intent.kind, ...(intent.kind === 'line' ? {point: true} : {})},
    encoding: {
      x: horizontal ? valueAxis : categoryAxis,
      y: horizontal ? categoryAxis : valueAxis,
      ...(seriesIndex >= 0 ? {color: {field: 'series', type: 'nominal', title: seriesFields[0]},
        ...(intent.kind === 'bar' ? {[horizontal ? 'yOffset' : 'xOffset']: {field: 'series'}} : {})} : {}),
    },
    config: chartTheme,
  }};
}

export function exploratoryChartSpec(result, intent, ownedId) {
  const no = reason => ({spec: null, reason});
  if (result.id !== ownedId || !intent) return no('Chart intent is not valid for this result.');
  if (['table', 'number'].includes(intent.kind)) return no('');
  if (Object.keys(intent).some(k => !['kind', 'x', 'series'].includes(k)) || !['line', 'bar'].includes(intent.kind)
      || !Array.isArray(intent.series) || !intent.series.length || intent.series.length > 3
      || new Set(intent.series).size !== intent.series.length) return no('Unsupported chart fields.');
  const p = result.provenance;
  if (p.truncated || p.query_limited || p.dataset_scope === 'limited') return no('Limited results are shown in the table.');
  const dimensions = p.columns.filter(c => c.role === 'dimension');
  const x = result.columns.indexOf(intent.x), axis = p.columns.find(c => c.name === intent.x);
  if (dimensions.length !== 1 || x < 0 || axis?.role !== 'dimension') return no('Chart fields do not match.');
  if (intent.kind === 'line' && (axis.type !== 'date' || axis.time_grain !== 'month')) return no('A line chart requires a monthly time axis.');
  if (intent.kind === 'bar' && axis.type !== 'text') return no('A bar chart requires a category axis.');
  const columns = intent.series.map(name => p.columns.find(c => c.name === name));
  const unit = columns[0]?.unit;
  if (!['source_currency', 'ratio', 'percent', 'days', 'count'].includes(unit)
      || columns.some(c => !c || c.role !== 'measure' || !['integer', 'decimal'].includes(c.type) || c.unit !== unit)
      || intent.series.some(name => !result.columns.includes(name))) return no('Chart fields or units do not match.');
  if (!result.rows.length || result.rows.length > (intent.kind === 'bar' ? 30 : 200)) return no('Use the table for this dataset size.');
  const labels = new Set(), values = [];
  for (const row of result.rows) {
    const label = row[x] === null ? 'Unclassified' : String(row[x]);
    if (labels.has(label)) return no('Use the table for repeated chart coordinates.');
    labels.add(label);
    if (intent.kind === 'line') {
      const parsed = new Date(label + 'T00:00:00Z');
      if (!/^\d{4}-\d{2}-01$/.test(label) || !Number.isFinite(parsed.getTime()) || parsed.toISOString().slice(0, 10) !== label) return no('Invalid monthly axis.');
    }
    for (const column of columns) {
      const raw = row[result.columns.indexOf(column.name)];
      if (raw !== null && (!/^-?\d+(\.\d+)?$/.test(String(raw)) || !Number.isFinite(Number(raw)) || Math.abs(Number(raw)) > Number.MAX_SAFE_INTEGER)) return no('Use the table to retain numerical precision.');
      values.push({label, value: raw === null ? null : Number(raw), series: column.label});
    }
  }
  if (values.every(v => v.value === null)) return no('All values are unknown.');
  if (intent.kind === 'line') {
    const ordered = [...labels].sort();
    const month = new Date(ordered[0] + 'T00:00:00Z'), last = ordered.at(-1);
    // Missing calendar buckets must break the path just like explicit NULLs.
    for (; month.toISOString().slice(0, 10) <= last; month.setUTCMonth(month.getUTCMonth() + 1)) {
      const label = month.toISOString().slice(0, 10);
      if (!labels.has(label)) for (const column of columns) values.push({label, value: null, series: column.label});
      if (values.length > 600) return no('Use the table for this time range.');
    }
  }
  values.sort((a, b) => intent.kind === 'line' ? a.label.localeCompare(b.label) : 0);
  // Reuse the certified theme, but build a closed specification with only owned
  // inline values. No field names from the query enter Vega expressions.
  const format = unit === 'ratio' ? '.1%' : unit === 'source_currency' ? ',.2f' : ',.1f';
  const category = {field: 'label', type: 'ordinal', title: null, sort: intent.kind === 'line' ? 'ascending' : null,
    axis: {labelAngle: intent.kind === 'line' ? -35 : 0, labelLimit: 180}};
  const value = {field: 'value', type: 'quantitative', title: unitLabel(unit), stack: null,
    scale: {zero: true}, axis: {format: unit === 'ratio' ? '.0%' : unit === 'source_currency' ? '~s' : undefined}};
  return {reason: '', spec: {
    data: {values}, width: 560, height: intent.kind === 'bar' ? Math.max(250, labels.size * 28) : 280,
    description: `${p.title}. Missing values are unknown. Exact values are available in the table.`,
    mark: {type: intent.kind, ...(intent.kind === 'line' ? {point: true, invalid: 'break-paths-show-domains'} : {})},
    encoding: {x: intent.kind === 'line' ? category : value, y: intent.kind === 'line' ? value : category,
      color: {field: 'series', type: 'nominal', title: null},
      ...(intent.kind === 'bar' && columns.length > 1 ? {yOffset: {field: 'series'}} : {}),
      tooltip: [{field: 'label', title: axis.label}, {field: 'series', title: 'Measure'}, {field: 'value', title: unitLabel(unit), format}]},
    config: chartTheme,
  }};
}
