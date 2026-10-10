export const unitLabel = unit => ({source_currency: 'GBP', ratio: 'ratio', days: 'days', count: 'count'}[unit] || unit || '');
// Preserve decimal strings from PostgreSQL, including values beyond JS integer precision.
export function formatCell(value, unit) {
  if (value === null || value === undefined) return 'Unknown';
  if (!/^-?\d+(\.\d+)?$/.test(String(value))) return String(value);
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
  return {reason: '', spec: {
    data: {values}, width: 560, height: 250,
    description: `${p.metric_label} (${unitLabel(p.unit)}). ${p.dataset_scope === 'limited' ? 'Limited result.' : 'Stored result.'} Exact values are in the table.`,
    mark: {type: intent.kind, ...(intent.kind === 'line' ? {point: true} : {})},
    encoding: {
      x: {field: 'label', type: 'ordinal', title: intent.x, sort: null, axis: {labelAngle: -35, labelLimit: 140}},
      y: {field: 'value', type: 'quantitative', title: unitLabel(p.unit), scale: {zero: true}, stack: null},
      ...(seriesIndex >= 0 ? {color: {field: 'series', type: 'nominal', title: seriesFields[0]},
        ...(intent.kind === 'bar' ? {xOffset: {field: 'series'}} : {})} : {}),
    },
    config: {
      background: 'transparent', view: {stroke: null}, mark: {color: '#931f1f'}, font: 'Open Sans',
      range: {category: ['#931f1f', '#262626', '#737373', '#c14c4c', '#4b5563', '#691616', '#9b6a58', '#6b5c7b']},
      axis: {labelColor: '#666666', titleColor: '#262626', domainColor: '#8a8a8a', tickColor: '#8a8a8a', gridColor: '#e6e6e6'},
      legend: {labelColor: '#666666', titleColor: '#262626'},
    },
  }};
}
