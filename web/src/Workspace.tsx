import {useEffect, useRef, useState} from 'react';
import type {Auth, Conversation, Run, Workflow} from './types';
import {activeRun, ApiError, errorMessage, json, post, watchRun} from './api';
import ResultCard from './ResultCard';
import ExplanationCard from './ExplanationCard';
import {isExplanation} from './types';
import {historyTime, questionTurns, runOutcome} from './conversation';
import AgentStatus from './AgentStatus';
import type {AgentActivity} from './agentActivity';

const route = () => /^\/conversations\/([0-9a-f-]{36})$/.exec(window.location.pathname)?.[1] || null;
type SubmissionRequest = {path: string; body: Record<string, unknown>; question: string};
type Pending = {request: SubmissionRequest; status: 'submitting' | 'unconfirmed'};
export default function Workspace({auth}: {auth: Auth}) {
  const [conversations, setConversations] = useState<Conversation[]>([]);
  const [conversationId, setConversationId] = useState(route);
  const [historyMore, setHistoryMore] = useState(false);
  const [runs, setRuns] = useState<Run[]>([]);
  const [runMore, setRunMore] = useState(false);
  const [workflows, setWorkflows] = useState<Record<string, Workflow>>({});
  const [question, setQuestion] = useState('');
  const [reply, setReply] = useState('');
  const [progress, setProgress] = useState<AgentActivity | null>(null);
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);
  const [reload, setReload] = useState(0);
  const [reconnect, setReconnect] = useState(0);
  const [followUp, setFollowUp] = useState(true);
  const [pending, setPending] = useState<Pending | null>(null);
  const [creatingConversation, setCreatingConversation] = useState(false);
  const mounted = useRef(true), loading = useRef(false);
  const acknowledgedConversation = useRef<string | null>(null);
  const lifetime = useRef(new AbortController());
  useEffect(() => () => {mounted.current = false; lifetime.current.abort();}, []);
  const fail = (e: unknown) => {if (mounted.current) setError(errorMessage(e));};
  const update = (state: Workflow) => setWorkflows(prev => ({...prev, [state.run_id]: state}));
  const navigate = (id: string | null) => {
    window.history.pushState(null, '', id ? `/conversations/${id}` : '/');
    setConversationId(id); setQuestion(''); setReply(''); setPending(null); setError('');
  };
  useEffect(() => {const changed = () => {setConversationId(route()); setPending(null); setError('');}; window.addEventListener('popstate', changed); return () => window.removeEventListener('popstate', changed);}, []);
  const loadHistory = async (append = false) => {
    const rows = await json<Conversation[]>(auth, '/v1/conversations', {signal: lifetime.current.signal, query: {limit: 30, offset: append ? conversations.length : 0}});
    if (mounted.current) {setConversations(prev => append ? [...prev, ...rows] : rows); setHistoryMore(rows.length === 30);}
  };
  useEffect(() => {loadHistory().catch(fail);}, [auth, reload]);
  useEffect(() => {
    // The first message acknowledgement already supplied the new conversation's run.
    // Keep it visible when adopting the saved URL instead of clearing and refetching it.
    if (conversationId && acknowledgedConversation.current === conversationId) {
      acknowledgedConversation.current = null;
      return;
    }
    const controller = new AbortController();
    setRuns([]); setWorkflows({}); setRunMore(false);
    if (conversationId) {
      setBusy(true);
      (async () => {
        await json(auth, `/v1/conversations/${conversationId}`, {signal: controller.signal});
        const rows = await json<Run[]>(auth, `/v1/conversations/${conversationId}/runs`, {signal: controller.signal, query: {limit: 20}});
        const states = await Promise.all(rows.map(async row => {
          try {return await json<Workflow>(auth, `/v1/runs/${row.id}/workflow`, {signal: controller.signal});}
          catch (e) {if (e instanceof ApiError && e.status === 404) return null; throw e;}
        }));
        if (!controller.signal.aborted) {setRuns(rows); setRunMore(rows.length === 20); setWorkflows(Object.fromEntries(states.filter(s => s !== null).map(s => [s.run_id, s])));}
      })().catch(e => {if (!controller.signal.aborted) fail(e);}).finally(() => {if (!controller.signal.aborted) setBusy(false);});
    } else setBusy(false);
    return () => controller.abort();
  }, [auth, conversationId, reload]);
  const current = runs.map(r => workflows[r.id]).find(w => w && (activeRun(w.status) || w.status === 'awaiting_clarification'));
  const watching = current && activeRun(current.status) ? current.run_id : null;
  useEffect(() => {
    if (!watching) {setProgress(null); return;}
    const controller = new AbortController(); setProgress({phase: 'connecting', message: 'Connecting to your run…'});
    watchRun(auth, watching, controller.signal, activity => {if (!controller.signal.aborted) setProgress(activity);}, state => {if (!controller.signal.aborted) update(state);})
      .catch(e => {if (!controller.signal.aborted) {fail(e); setProgress({phase: 'waiting', message: 'Connection paused. Reconnect to retrieve the saved outcome.'});}});
    return () => controller.abort();
  }, [auth, watching, reconnect]);
  // Explanations carry no dataset, so only result answers can anchor a follow-up.
  const latestAnswer = runs.find(r => workflows[r.id]?.status === 'completed' && workflows[r.id]?.answer && !isExplanation(workflows[r.id].answer));
  const compose = (text: string) => {setQuestion(text); document.getElementById('question')?.focus();};
  const turns = questionTurns(runs, workflows);
  const sendPending = async (request: SubmissionRequest) => {
    setPending({request, status:'submitting'});
    let state: Workflow;
    try {state = await json<Workflow>(auth, request.path, {...post(request.body), signal: lifetime.current.signal});}
    catch (e) {
      // A definite rejection can be revised; uncertain delivery retains its key.
      if (mounted.current) setPending(e instanceof ApiError && [400, 404, 409, 422].includes(e.status) ? null : {request, status:'unconfirmed'});
      throw e;
    }
    if (!mounted.current) return;
    update(state); setRuns(prev => prev.some(r => r.id === state.run_id) ? prev : [{id: state.run_id, conversation_id: state.conversation_id, question: request.question, status: state.status, created_at: new Date().toISOString()}, ...prev]);
    setPending(null); setQuestion(''); setReply(''); setReconnect(n => n + 1);
  };
  const act = async (fn: () => Promise<void>) => {
    if (loading.current) return;
    loading.current = true; setBusy(true); setError('');
    try {await fn();} catch (e) {fail(e);} finally {loading.current = false; if (mounted.current) setBusy(false);}
  };
  const send = (retryRun?: Run) => act(async () => {
    const text = (retryRun?.question || question).trim(); if (!text) return;
    let id = conversationId;
    if (!id) {
      setCreatingConversation(true);
      try {
        const created = await json<Conversation>(auth, '/v1/conversations', {...post({title: text.slice(0, 160)}), signal: lifetime.current.signal});
        if (!mounted.current) return;
        id = created.id;
        const request = {path: `/v1/conversations/${id}/messages`, body: {question: text, idempotency_key: crypto.randomUUID()}, question: text};
        try {
          await sendPending(request);
          if (mounted.current) acknowledgedConversation.current = id;
        } finally {
          if (mounted.current) {
            window.history.pushState(null, '', `/conversations/${id}`); setConversationId(id); setReload(n => n + 1);
          }
        }
      } finally {if (mounted.current) setCreatingConversation(false);}
      return;
    }
    await sendPending({path: `/v1/conversations/${id}/messages`, body: {question: text, idempotency_key: crypto.randomUUID(),
      ...(!retryRun && followUp && latestAnswer ? {follow_up_to: latestAnswer.id} : {})}, question: text});
  });
  const resume = () => act(async () => {
    if (!current?.clarification || !reply.trim()) return;
    await sendPending({path: `/v1/runs/${current.run_id}/resume`, body: {answer: reply.trim(), clarification_id: current.clarification.id, idempotency_key: crypto.randomUUID()}, question: runs.find(r => r.id === current.run_id)!.question});
  });
  const olderRuns = () => act(async () => {
    const rows = await json<Run[]>(auth, `/v1/conversations/${conversationId}/runs`, {signal: lifetime.current.signal, query: {limit: 20, offset: runs.length}});
    const states = await Promise.all(rows.map(r => json<Workflow>(auth, `/v1/runs/${r.id}/workflow`, {signal: lifetime.current.signal}).catch(e => {if (e instanceof ApiError && e.status === 404) return null; throw e;})));
    if (mounted.current) {setRuns(prev => [...prev, ...rows]); states.forEach(s => s && update(s)); setRunMore(rows.length === 20);}
  });
  return <div className="workspace-layout"><aside aria-label="Conversation history"><div className="sidebar-top"><p className="eyebrow">WORKSPACE</p><button className="new-chat" disabled={busy || !!pending} onClick={() => navigate(null)}>+ New conversation</button></div>
    <h2>Recent conversations</h2><nav>{conversations.map(c => <a key={c.id} href={`/conversations/${c.id}`} aria-current={c.id === conversationId ? 'page' : undefined}
      onClick={e => {e.preventDefault(); if (!busy && !pending) navigate(c.id);}}><span>{c.title}</span>{historyTime(c.created_at) && <time dateTime={c.created_at}>{historyTime(c.created_at)}</time>}</a>)}</nav>
    {historyMore && <button className="text-button" disabled={busy} onClick={() => act(() => loadHistory(true))}>Older conversations</button>}
    </aside>
    <main id="workspace" className="conversation"><div className={`conversation-heading${conversationId ? ' saved-conversation-heading' : ''}`}>
      <h1>{conversationId ? 'Conversation' : 'What would you like to understand?'}</h1>
      {!conversationId && <p>Explore revenue, order value and conversion. Start with a question below.</p>}</div>
      {!runs.length && !busy && <div className="suggestions">{[
        {title:'Monthly revenue',question:'Show revenue for the last completed month.'},
        {title:'Order value by category',question:'Show stored order value by category.'},
        {title:'Five-year conversion',question:'What is the five-year conversion rate?'},
      ].map(({title,question: suggestedQuestion}) => <button className="suggestion" key={title} onClick={() => setQuestion(suggestedQuestion)}>
        <span className="suggestion-copy"><strong className="suggestion-title">{title}</strong><span className="suggestion-question">{suggestedQuestion}</span></span>
        <span className="suggestion-icon" aria-hidden="true"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round"><path d="M5 12h14m-6-6 6 6-6 6" /></svg></span>
      </button>)}</div>}
      {runMore && <button className="secondary" disabled={busy} onClick={olderRuns}>Load older answers</button>}
      <div className="messages">{turns.map(({run, previousAttempts}) => {
        const state = workflows[run.id];
        return <article className="message" key={run.id}><div className="question"><span className="eyebrow">YOU ASKED</span><h2>{run.question}</h2></div>
          {state?.answer ? isExplanation(state.answer)
            ? <ExplanationCard answer={state.answer} onSuggest={compose} />
            : <ResultCard auth={auth} answer={state.answer} /> : !state || !['failed', 'cancelled', 'interrupted'].includes(state.status) ? <p className="run-state">{(state?.status || run.status).replaceAll('_', ' ')}</p> : null}
          {state && ['failed', 'cancelled', 'interrupted'].includes(state.status) && <div className="notice"><AgentStatus announce={false} {...runOutcome(state)} />
            <div className="actions"><button className="secondary" disabled={busy || !!current || !!pending} onClick={() => send(run)}>Retry question</button>
              <button className="secondary" disabled={!!current || !!pending} onClick={() => compose(run.question)}>Edit question</button></div></div>}
          {!state && run.status === 'completed' && <p className="note">This earlier run has no conversational answer.</p>}
          {!!previousAttempts.length && <details className="attempt-history"><summary>{previousAttempts.length} earlier unsuccessful {previousAttempts.length === 1 ? 'attempt' : 'attempts'}</summary>
            <ul>{previousAttempts.map(attempt => <li key={attempt.id}><time dateTime={attempt.created_at}>{historyTime(attempt.created_at)}</time> · {(workflows[attempt.id]?.status || attempt.status).replaceAll('_', ' ')}</li>)}</ul>
          </details>}
        </article>;
      })}</div>
      {current && <section className="run-control" aria-label="Current run"><AgentStatus {...(watching
        ? progress ?? {phase: 'connecting', message: 'Connecting to your run…'}
        : {phase: 'waiting', message: 'A clarification is needed.'})} />
        <div className="actions"><button className="secondary" disabled={busy} onClick={() => act(async () => {
          await json(auth, `/v1/runs/${current.run_id}/cancel`, {...post({}), signal: lifetime.current.signal});
          update(await json<Workflow>(auth, `/v1/runs/${current.run_id}/workflow`, {signal: lifetime.current.signal}));
        })}>Cancel run</button>{watching && <button className="text-button" onClick={() => {setError(''); setReconnect(n => n + 1);}}>Reconnect</button>}</div>
        {current.clarification && <form onSubmit={e => {e.preventDefault(); resume();}}><label htmlFor="reply">{current.clarification.question}</label>
          <div className="options">{current.clarification.options.map(option => <button key={option} type="button" className="secondary" onClick={() => setReply(option)}>{option}</button>)}</div>
          <textarea id="reply" maxLength={2000} value={reply} onChange={e => setReply(e.target.value)} required /><button disabled={busy || !!pending || !reply.trim()}>Send clarification</button></form>}
      </section>}
      {error && <div role="alert" className="error"><p>{error}</p><button className="text-button" onClick={() => {setError(''); setReload(n => n + 1); setReconnect(n => n + 1);}}>Reload saved conversation</button></div>}
      {(creatingConversation || pending?.status === 'submitting') && <div className="notice"><AgentStatus phase="submitting" message={pending?.request.path.endsWith('/resume') ? 'Submitting your clarification…' : 'Submitting your query…'} /></div>}
      {pending?.status === 'unconfirmed' && <div className="notice"><AgentStatus announce={false} phase="waiting" message="Submission was not confirmed. Retry safely with the same request." /><button disabled={busy} onClick={() => act(() => sendPending(pending.request))}>Retry submission</button></div>}
      <form className="composer" onSubmit={e => {e.preventDefault(); send();}}><label htmlFor="question">{latestAnswer ? 'Ask a follow-up or a new question' : 'Your question'}</label>
        <textarea id="question" placeholder="e.g. How did revenue compare with the previous month?" maxLength={8000} value={question} onChange={e => setQuestion(e.target.value)} required disabled={creatingConversation || pending?.status === 'submitting' || !!current} />
        <div className="composer-footer">{latestAnswer && <label className="checkbox"><input type="checkbox" checked={followUp} onChange={e => setFollowUp(e.target.checked)} />Use the latest answer as context</label>}
          <button className="ask-analyst" disabled={busy || !!current || !!pending || !question.trim()}>Ask analyst
            <span className="ask-analyst-icon" aria-hidden="true"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M12 19V5m-6 6 6-6 6 6" /></svg></span>
          </button></div>
      </form>
    </main></div>;
}
