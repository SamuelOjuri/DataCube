// Closed chart templates: query text and model-provided expressions never enter a spec.
export const safeNumber = raw => raw !== null && raw !== undefined && /^-?\d+(\.\d+)?$/.test(String(raw))
  && Number.isFinite(Number(raw)) && Math.abs(Number(raw)) <= Number.MAX_SAFE_INTEGER;
const units = ['source_currency', 'count', 'ratio', 'percent', 'days', 'none'];
const format = unit => unit === 'ratio' ? '.1%' : unit === 'source_currency' ? ',.2f' : ',.2~f';
const measure = c => c?.role === 'measure' && ['integer', 'decimal'].includes(c.type) && units.includes(c.unit);
export function analyticalChart(result, intent, config, unitLabel) {
  const no = reason => ({spec: null, reason});
  const p = result.provenance;
  if (!result.rows.length || result.rows.length > 200) return no('Use the table for this dataset size.');
  const x = p.columns.find(c => c.name === intent.x), ys = intent.series.map(name => p.columns.find(c => c.name === name));
  if (!measure(x) || ys.some(c => !measure(c)) || [x, ...ys].some(c => !result.columns.includes(c.name))) return no('Chart fields or units do not match.');
  const indices = [x, ...ys].map(c => result.columns.indexOf(c.name));
  if (result.rows.some(row => indices.some(i => row[i] !== null && !safeNumber(row[i])))) return no('Use the table to retain numerical precision.');
  const axis = (field, c) => ({field, type: 'quantitative', title: `${c.label}${unitLabel(c.unit) ? ` (${unitLabel(c.unit)})` : ''}`,
    scale: {zero: false}, axis: {format: format(c.unit)}});
  let values, encoding, mark, description;
  if (intent.kind === 'comparison') {
    if (result.rows.length !== 1 || ys.length !== 2 || ys[0].name !== x.name || ys[0].name === ys[1].name
        || ys[0].label === ys[1].label || ys[0].unit !== ys[1].unit || p.columns.some(c => c.role === 'dimension')) return no('A comparison requires two distinct, comparable values.');
    if (indices.some(i => result.rows[0][i] === null)) return no('Unknown comparison values are retained in the table.');
    values = ys.map(c => ({label: c.label, value: Number(result.rows[0][result.columns.indexOf(c.name)])}));
    encoding = {x: {field: 'label', type: 'nominal', title: null, sort: values.map(v => v.label), axis: {labelAngle: 0, labelLimit: 220}},
      y: {...axis('value', x), title: unitLabel(x.unit), scale: {zero: true}},
      tooltip: [{field: 'label', title: 'Measure'}, {field: 'value', title: unitLabel(x.unit), format: format(x.unit)}]};
    mark = {type: 'bar'}; description = 'Comparison of the two returned values.';
  } else if (intent.kind === 'histogram') {
    if (ys.length !== 1 || ys[0].name !== x.name) return no('A distribution requires one numerical measure.');
    values = result.rows.filter(row => row[indices[0]] !== null).map(row => ({value: Number(row[indices[0]])}));
    if (values.length < 2) return no('There are too few known values to plot a distribution.');
    const bin = {maxbins: Math.min(12, Math.ceil(Math.sqrt(values.length)))};
    encoding = {x: {...axis('value', x), bin}, y: {aggregate: 'count', type: 'quantitative', title: 'Returned values', axis: {tickMinStep: 1, format: 'd'}},
      tooltip: [{field: 'value', bin, title: x.label, format: format(x.unit)}, {aggregate: 'count', title: 'Returned values'}]};
    mark = {type: 'bar'}; description = `Distribution of known ${x.label.toLowerCase()} values. Each observation is one returned value.`;
  } else if (intent.kind === 'scatter') {
    if (ys.length !== 1 || ys[0].name === x.name) return no('A relationship requires two different numerical measures.');
    values = result.rows.filter(row => indices.every(i => row[i] !== null)).map(row => ({x: Number(row[indices[0]]), y: Number(row[indices[1]])}));
    if (values.length < 3) return no('There are too few complete pairs to plot a relationship.');
    encoding = {x: axis('x', x), y: axis('y', ys[0]),
      tooltip: [{field: 'x', title: x.label, format: format(x.unit)}, {field: 'y', title: ys[0].label, format: format(ys[0].unit)}]};
    mark = {type: 'point', filled: true, opacity: 0.7, size: 70};
    description = `Relationship between ${x.label.toLowerCase()} and ${ys[0].label.toLowerCase()}. Each point is a complete returned pair; association does not establish cause.`;
  } else return no('Unsupported chart fields.');
  return {reason: '', spec: {data: {values}, width: 560, height: 280, mark, encoding, description, config}};
}
export function metricComparisonChart(result, config, unitLabel, periodLabel) {
  const p = result.provenance, a = p.total?.value, b = p.comparison?.baseline?.value;
  if (!safeNumber(a) || !safeNumber(b) || !units.includes(p.unit) || p.columns.find(c => c.name === 'value')?.unit !== p.unit) {
    return {spec: null, reason: 'The table retains comparison values that cannot be plotted reliably.'};
  }
  const values = [{label: `Current: ${periodLabel(p.resolved_period)}`, value: Number(a)},
    {label: `Baseline: ${periodLabel(p.comparison_period)}`, value: Number(b)}];
  return {reason: '', spec: {data: {values}, width: 560, height: 250, mark: {type: 'bar'},
    description: `${p.metric_label}: current and baseline aggregates.`,
    encoding: {y: {field: 'label', type: 'nominal', title: null, sort: values.map(v => v.label), axis: {labelLimit: 220}},
      x: {field: 'value', type: 'quantitative', title: unitLabel(p.unit), scale: {zero: true}, axis: {format: format(p.unit)}},
      tooltip: [{field: 'label', title: 'Period'}, {field: 'value', title: unitLabel(p.unit), format: format(p.unit)}]}, config}};
}
