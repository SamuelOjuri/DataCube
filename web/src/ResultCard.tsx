import {useEffect, useState} from 'react';
import type {Auth, MetricDefinition, ProjectPage, ResultAnswer, SavedResult} from './types';
import {isExploratory} from './types';
import {errorMessage, json, post} from './api';
import {chartSpec, formatCell, periodLabel, unitLabel} from './presentation.mjs';
import Chart from './Chart';
import DataTable from './DataTable';
import ExploratoryCard from './ExploratoryCard';
import AnswerSummary from './AnswerSummary';

export default function ResultCard({auth, answer}: {auth: Auth; answer: ResultAnswer}) {
  const [saved, setResult] = useState<SavedResult | null>(null);
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');
  const [busy, setBusy] = useState(false);
  const [projects, setProjects] = useState<ProjectPage | null>(null);
  const [comment, setComment] = useState('');
  const [definition, setDefinition] = useState<MetricDefinition | null>(null);
  const [definitionNotice, setDefinitionNotice] = useState('');
  const [showValues, setShowValues] = useState(false);
  const [showCounts, setShowCounts] = useState(false);
  const [controller] = useState(() => new AbortController());
  useEffect(() => {
    json<SavedResult>(auth, `/v1/results/${answer.result.result_id}`, {signal: controller.signal})
      .then(value => {if (!controller.signal.aborted) setResult(value);}).catch(e => {if (!controller.signal.aborted) setError(errorMessage(e));});
    return () => controller.abort();
  }, [auth, answer.result.result_id, controller]);
  const result = saved && !isExploratory(saved) ? saved : null;
  const act = async (action: () => Promise<void>) => {
    setError(''); setBusy(true);
    try {await action();} catch (e) {if (!controller.signal.aborted) setError(errorMessage(e));} finally {setBusy(false);}
  };
  const drill = (offset = 0) => act(async () => setProjects(await json<ProjectPage>(auth, `/v1/results/${result!.id}/projects`, {signal: controller.signal, query: {limit: 25, offset}})));
  const download = () => act(async () => {
    const response = await auth.request(`/v1/results/${result!.id}/export`, {signal: controller.signal});
    if (!response.ok || response.headers.get('X-Export-Scope') !== 'stored-result-rows') {await response.body?.cancel(); throw new Error('Export unavailable');}
    const blob = await response.blob();
    if (controller.signal.aborted) return;
    const url = URL.createObjectURL(blob), link = document.createElement('a');
    link.href = url; link.download = `datacube-${result!.id}.csv`; link.click(); setTimeout(() => URL.revokeObjectURL(url), 1000);
    setNotice('Exported all stored result rows, including rows on other table pages.');
  });
  const feedback = (rating: 'helpful' | 'not_helpful') => act(async () => {
    await json(auth, `/v1/results/${result!.id}/feedback`, {...post({rating, comment}), signal: controller.signal});
    setNotice('Thank you. Your feedback has been saved.');
  });
  const loadDefinition = async () => {
    if (definition || definitionNotice || !result) return;
    setDefinitionNotice('Loading definition…');
    try {
      const catalogue = await json<{catalogue_sha256: string; metrics: MetricDefinition[]}>(auth, '/v1/metrics', {signal: controller.signal});
      const metric = catalogue.metrics.find(m => m.id === result.provenance.metric_id && m.version === result.provenance.metric_version);
      if (catalogue.catalogue_sha256 !== result.provenance.catalogue_sha256 || !metric) setDefinitionNotice('The catalogue has changed. The saved source and version details below still describe this result.');
      else {setDefinition(metric); setDefinitionNotice('');}
    } catch (e) {if (!controller.signal.aborted) setDefinitionNotice(errorMessage(e));}
  };
  if (saved && isExploratory(saved)) return <ExploratoryCard auth={auth} answer={answer} result={saved} signal={controller.signal} />;
  if (!result) return <div role="status">{error || 'Loading saved result…'}</div>;
  const p = result.provenance;
  const period = p.resolved_period ? `${p.resolved_period.start_date} to ${p.resolved_period.end_date_exclusive || 'onward'}${p.resolved_period.end_date_exclusive ? ' (end exclusive)' : ''}` : 'All stored history';
  const filterLabel = (filters: typeof p.request.filters) => filters.length ? filters.map(f => `${f.dimension}: ${f.operator === 'is_null' ? 'unclassified' : f.values.join(', ')}`).join('; ') : 'None';
  const hasChart = !!chartSpec(result, answer.chart, answer.result.result_id).spec;
  const tableColumns = result.columns.filter(name => showCounts || !['source_rows', 'known_values'].includes(name));
  const tableRows = result.rows.map(row => tableColumns.map(name => row[result.columns.indexOf(name)]));
  const hasCounts = result.columns.some(name => ['source_rows', 'known_values'].includes(name));
  return <section className="result-card" aria-label={p.metric_label}>
    <div className="result-heading"><div><p className="eyebrow">{unitLabel(p.unit)}{p.evaluation_only ? ' · Evaluation data' : ''}</p><h3>{p.metric_label}</h3></div><span className="badge">Source freshness unknown</span></div>
    <AnswerSummary answer={answer} />
    <p className="result-scope">{periodLabel(p.resolved_period)} · {p.source_population === 'reportable' ? 'Reportable projects, including archived projects' : 'Independent hidden inventory'}
      {!!p.request.filters.length && <> · {filterLabel(p.request.filters)}</>}</p>
    {p.total && <div className="total"><strong>{formatCell(p.total.value, p.unit)}</strong><span>{p.unit === 'ratio' ? 'Overall conversion rate' : p.unit === 'days' ? 'Overall average' : p.total.known_values < p.total.source_rows ? 'Known subtotal for this scope' : 'Total for this scope'}</span></div>}
    {p.total && p.total.source_rows === 0 ? <p className="data-warning">No matching records were found.</p> : p.total && p.total.known_values < p.total.source_rows && <p className="data-warning">{formatCell(p.total.source_rows - p.total.known_values)} of {formatCell(p.total.source_rows)} matching records have no value. {p.total.known_values ? 'This subtotal may be incomplete.' : 'The aggregate is unknown.'}</p>}
    {p.truncated && <p className="data-warning">Showing {p.returned_rows} of {p.matched_groups} groups. The total covers all matching records; the table and CSV include only these {p.returned_rows} groups.</p>}
    {p.unit === 'ratio' && p.total && <p className="note">Numerator: {formatCell(p.total.numerator)} · Denominator: {formatCell(p.total.denominator)} · Rates are shown as percentages.</p>}
    {p.comparison && <section className="comparison" aria-label="Comparison"><h4>Comparison of complete filtered aggregates</h4><dl className="scope">
      <div><dt>Baseline period</dt><dd>{periodLabel(p.comparison_period)}</dd></div>
      <div><dt>Baseline filters</dt><dd>{filterLabel(p.request.comparison?.filters ?? p.request.filters)}</dd></div>
      <div><dt>Baseline value</dt><dd>{formatCell(p.comparison.baseline.value, p.unit)}</dd></div>
      <div><dt>Absolute change</dt><dd>{formatCell(p.comparison.absolute_change, p.unit)}</dd></div>
      <div><dt>Percentage change</dt><dd>{p.comparison.percentage_change === null ? 'Undefined' : formatCell(p.comparison.percentage_change, 'percent')}</dd></div>
      {p.unit === 'ratio' && <div><dt>Percentage-point change</dt><dd>{p.comparison.percentage_point_change === null ? 'Unknown' : `${formatCell(p.comparison.percentage_point_change)} percentage points`}</dd></div>}
    </dl>{p.comparison.zero_denominator && <p className="note">Percentage change is undefined because the baseline is zero or unknown.</p>}</section>}
    {Object.keys(answer.scope_changes).length > 0 && <details className="scope-change"><summary>Scope changed from the previous answer</summary><pre>{JSON.stringify(answer.scope_changes, null, 2)}</pre></details>}
    <Chart result={result} intent={answer.chart} ownedId={answer.result.result_id} />
    {hasChart && <button className="text-button values-toggle" aria-expanded={showValues} onClick={() => setShowValues(value => !value)}>{showValues ? 'Hide values' : `View ${p.returned_rows} values`}</button>}
    {(!hasChart || showValues) && <div className="result-values">
      {hasCounts && <label className="checkbox table-options"><input type="checkbox" checked={showCounts} onChange={e => setShowCounts(e.target.checked)} />Show source record counts</label>}
      <DataTable columns={tableColumns} rows={tableRows} metadata={p.columns} caption={`${p.metric_label} — values`} />
    </div>}
    <div className="actions"><button className="secondary" disabled={busy} onClick={download}>Download CSV ({p.returned_rows} rows)</button>
      {p.source_population === 'reportable' && <button className="secondary" disabled={busy} onClick={() => drill()}>Explore contributing projects</button>}</div>
    {projects && <section aria-label="Contributing projects"><h4>Contributing projects</h4><p>{projects.limitation}</p><p className="note">Queried {projects.queried_at}. Page starts at row {projects.offset + 1}.</p>
      <DataTable key={projects.offset} columns={projects.columns} rows={projects.rows} caption="Project source rows" rowScope="source rows on this page"
        metadata={[{name: 'source_value', type: 'decimal', unit: p.unit === 'ratio' ? 'count' : p.unit}, {name: 'denominator', type: 'decimal', unit: 'count'}]} />
      <div className="actions"><button className="secondary" disabled={busy || projects.offset === 0} onClick={() => drill(Math.max(0, projects.offset - 25))}>Previous projects</button>
        <button className="secondary" disabled={busy || !projects.has_more} onClick={() => drill(projects.offset + 25)}>Next projects</button>
        <button className="text-button" onClick={() => setProjects(null)}>Close projects</button></div></section>}
    <details onToggle={event => {if (event.currentTarget.open) void loadDefinition();}}><summary>Definitions and source details</summary>
      <dl className="scope"><div><dt>Period</dt><dd>{period} · {p.business_timezone}</dd></div>
        {p.comparison_period && <div><dt>Comparison period</dt><dd>{p.comparison_period.start_date} to {p.comparison_period.end_date_exclusive ? `${p.comparison_period.end_date_exclusive} (end exclusive)` : 'onward'} · {p.business_timezone}</dd></div>}
        <div><dt>Population</dt><dd>{p.source_population === 'reportable' ? 'Retained reportable projects, including genuine archived projects' : 'Independent hidden inventory'}</dd></div>
        <div><dt>Filters</dt><dd>{filterLabel(p.request.filters)}</dd></div>
        <div><dt>Display</dt><dd>{p.returned_rows} of {p.matched_groups} groups · {p.truncated ? 'Limited result; this display is not the full population' : 'Complete requested dataset'} · no sampling</dd></div>
        {p.total && <div><dt>Known values</dt><dd>{formatCell(p.total.known_values)} of {formatCell(p.total.source_rows)} matching source records</dd></div>}</dl>
      <div className="limitations"><h4>Coverage and freshness</h4><p>{p.freshness.limitation}</p>
        <ul>{[...new Set([...p.limitations, ...answer.notices])].map(text => <li key={text}>{text}</li>)}</ul></div>
      <p className="note">CSV includes all stored rows across table pages{p.truncated ? '; excluded groups are not exported' : ', the complete requested dataset'}. It does not include drill-down rows.</p>
      {definitionNotice && <p role="status">{definitionNotice}</p>}
      {definition && <><p>Aggregation: {definition.calculation.aggregation}</p><ul>{[...definition.source.priority, definition.calculation.nulls, definition.calculation.zeroes, definition.calculation.negatives, definition.calculation.empty].map(text => <li key={text}>{text}</li>)}</ul></>}
      <dl className="scope">
      <div><dt>Metric</dt><dd>{p.metric_id} · version {p.metric_version}</dd></div><div><dt>Catalogue</dt><dd>{p.catalogue_version}</dd></div>
      <div><dt>Source</dt><dd>{p.source_relation} · {p.source_grain}</dd></div><div><dt>Query reference</dt><dd>{p.query_reference}</dd></div>
      <div><dt>Query time</dt><dd>{p.freshness.queried_at} (not a source refresh timestamp)</dd></div>
      {p.unit === 'source_currency' && <div><dt>Currency basis</dt><dd>GBP, as stored; VAT inclusion unspecified</dd></div>}</dl>
      <p>Coverage counters describe the unfiltered gateway population.</p><dl className="scope">{Object.entries(p.coverage).map(([key, value]) => <div key={key}><dt>{key.replaceAll('_', ' ')}</dt><dd>{value}</dd></div>)}</dl></details>
    <details><summary>Supporting figures</summary><p className="answer-text">{answer.text}</p></details>
    <details><summary>Give feedback</summary><label>Optional comment<textarea value={comment} maxLength={2000} onChange={e => setComment(e.target.value)} /></label>
      <div className="actions"><button className="secondary" disabled={busy} onClick={() => feedback('helpful')}>Helpful</button><button className="secondary" disabled={busy} onClick={() => feedback('not_helpful')}>Not helpful</button></div></details>
    {notice && <p role="status">{notice}</p>}{error && <p role="alert" className="error">{error}</p>}
  </section>;
}
