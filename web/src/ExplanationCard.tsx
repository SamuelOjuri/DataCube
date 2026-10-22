import type {ExplanationAnswer} from './types';

export default function ExplanationCard({answer, onSuggest}: {answer: ExplanationAnswer; onSuggest: (question: string) => void}) {
  return <section className="result-card explanation-card" aria-label={answer.title}>
    <p className="eyebrow">Analyst note · no data was returned</p><h3>{answer.title}</h3>
    <div aria-label="Summary">{answer.text.split('\n').filter(Boolean).map(line => <p className="answer-text" key={line}>{line}</p>)}</div>
    {!!answer.suggestions.length && <div className="explanation-suggestions"><h4>You could ask instead</h4>
      <div className="options">{answer.suggestions.map(suggestion => <button key={suggestion} type="button" className="secondary" onClick={() => onSuggest(suggestion)}>{suggestion}</button>)}</div></div>}
    {!!answer.notices.length && <div className="limitations"><ul>{answer.notices.map(text => <li key={text}>{text}</li>)}</ul></div>}
  </section>;
}
