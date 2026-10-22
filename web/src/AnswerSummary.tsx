import type {ResultAnswer} from './types';

/** One visible summary slot for both dataset answer paths and older saved text. */
export default function AnswerSummary({answer, caveats = []}: {answer: ResultAnswer; caveats?: string[]}) {
  const text = answer.summary?.trim() || answer.text?.trim();
  const notices = [...new Set([...caveats, ...(answer.summary_notices || [])])];
  return <>
    {text && <p className="answer-summary" aria-label="Summary">{text}</p>}
    {!!notices.length && <div className="data-warning" role="note" aria-label="What this answer leaves out">
      <ul className="caveats">{notices.map(notice => <li key={notice}>{notice}</li>)}</ul>
    </div>}
  </>;
}
