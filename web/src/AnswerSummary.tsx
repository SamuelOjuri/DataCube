import type {ResultAnswer} from './types';

/** One visible summary slot for both dataset answer paths and older saved text. */
export default function AnswerSummary({answer, caveats = []}: {answer: ResultAnswer; caveats?: string[]}) {
  const text = answer.summary?.trim() || answer.text?.trim()
    || "The available values are shown below. There isn't enough information to draw a reliable conclusion from this result.";
  const notices = [...new Set([...caveats, ...(answer.summary_notices || [])])];
  return <>
    <p className="answer-summary" aria-label="Summary">{text}</p>
    {answer.chart_reason && !text.includes(answer.chart_reason) && <p className="chart-note" aria-label="Presentation note">{answer.chart_reason}</p>}
    {!!notices.length && <div className="data-warning" role="note" aria-label="What this answer leaves out">
      <ul className="caveats">{notices.map(notice => <li key={notice}>{notice}</li>)}</ul>
    </div>}
  </>;
}
