import {useState} from 'react';
import type {Auth, ExploratoryResult, ResultAnswer} from './types';
import {errorMessage, json, post} from './api';
import {chartSpec, formatCell} from './presentation.mjs';
import DataTable from './DataTable';
import Chart from './Chart';

export default function ExploratoryCard({auth, answer, result, signal}: {auth: Auth; answer: ResultAnswer; result: ExploratoryResult; signal: AbortSignal}) {
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');
  const [busy, setBusy] = useState(false);
  const [comment, setComment] = useState('');
  const [showValues, setShowValues] = useState(false);
  const p = result.provenance;
  const act = async (action: () => Promise<void>) => {
    setError(''); setBusy(true);
    try {await action();} catch (e) {if (!signal.aborted) setError(errorMessage(e));} finally {setBusy(false);}
  };
  const download = () => act(async () => {
    const response = await auth.request(`/v1/results/${result.id}/export`, {signal});
    if (!response.ok || response.headers.get('X-Export-Scope') !== 'stored-result-rows') {await response.body?.cancel(); throw new Error('Export unavailable');}
    const blob = await response.blob();
    if (signal.aborted) return;
    const url = URL.createObjectURL(blob), link = document.createElement('a');
    link.href = url; link.download = `datacube-${result.id}.csv`; link.click(); setTimeout(() => URL.revokeObjectURL(url), 1000);
    setNotice('Exported all stored result rows.');
  });
  const feedback = (rating: 'helpful' | 'not_helpful') => act(async () => {
    await json(auth, `/v1/results/${result.id}/feedback`, {...post({rating, comment}), signal});
    setNotice('Thank you. Your feedback has been saved.');
  });
  const measures = p.columns.filter(c => c.role === 'measure');
  const single = result.rows.length === 1 && measures.length === 1 && p.columns.length === 1
    && ['integer', 'decimal'].includes(measures[0].type);
  const metadata = p.columns.map(c => ({name: c.name, label: c.label, type: c.type, unit: c.unit === 'none' ? null : c.unit}));
  const intent = answer.exploratory_chart || answer.chart;
  const hasChart = !!chartSpec(result, intent, answer.result.result_id).spec;
  const caveats = [...new Set([...(p.caveats || []), ...(answer.summary_notices || [])])];
  return <section className="result-card" aria-label={p.title}>
    <div className="result-heading"><div><p className="eyebrow">Exploratory analysis · not a certified metric</p><h3>{p.title}</h3></div><span className="badge">Source freshness unknown</span></div>
    {answer.summary && <p className="answer-summary" aria-label="Summary">{answer.summary}</p>}
    {single && <div className="total"><strong>{formatCell(result.rows[0][0], measures[0].unit)}</strong><span>{measures[0].label}</span></div>}
    {!!caveats.length && <div className="data-warning" role="note" aria-label="What this answer leaves out"><ul className="caveats">{caveats.map(text => <li key={text}>{text}</li>)}</ul></div>}
    {!result.rows.length && <p className="data-warning">No matching records were found for this scope.</p>}
    {p.truncated && <p className="data-warning">Showing the first {p.returned_rows} rows. The query returned more; the table and CSV include only these rows.</p>}
    <Chart result={result} intent={intent} ownedId={answer.result.result_id} />
    {hasChart && <button className="text-button values-toggle" aria-expanded={showValues} onClick={() => setShowValues(value => !value)}>{showValues ? 'Hide values' : `View ${p.returned_rows} values`}</button>}
    {!single && result.rows.length > 0 && (!hasChart || showValues) && <div className="result-values"><DataTable columns={result.columns} rows={result.rows} metadata={metadata} caption={`${p.title} — values`} /></div>}
    <div className="actions"><button className="secondary" disabled={busy} onClick={download}>Download CSV ({p.returned_rows} rows)</button></div>
    <details><summary>How this answer was produced</summary>
      <p className="result-scope">Generated query over {p.relations.map(r => `analyst_query.${r}`).join(', ')} · {p.business_timezone}</p>
      <p>The analyst wrote this read-only query for your question, checked it against approved views and functions, and ran it on governed data. Check important figures against certified reports.</p>
      <pre className="sql">{p.sql}</pre>
      <div className="limitations"><h4>Coverage and freshness</h4><p>{p.freshness.limitation}</p>
        <ul>{[...new Set([...p.limitations, ...answer.notices])].map(text => <li key={text}>{text}</li>)}</ul></div>
      <dl className="scope"><div><dt>Query reference</dt><dd>{p.query_reference}</dd></div><div><dt>Catalogue</dt><dd>{p.catalogue_version}</dd></div>
        <div><dt>Query time</dt><dd>{p.freshness.queried_at} (not a source refresh timestamp)</dd></div></dl></details>
    <details><summary>Answer text</summary><p className="answer-text">{answer.text}</p></details>
    <details><summary>Give feedback</summary><label>Optional comment<textarea value={comment} maxLength={2000} onChange={e => setComment(e.target.value)} /></label>
      <div className="actions"><button className="secondary" disabled={busy} onClick={() => feedback('helpful')}>Helpful</button><button className="secondary" disabled={busy} onClick={() => feedback('not_helpful')}>Not helpful</button></div></details>
    {notice && <p role="status">{notice}</p>}{error && <p role="alert" className="error">{error}</p>}
  </section>;
}
