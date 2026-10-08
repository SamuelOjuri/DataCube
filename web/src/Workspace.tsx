import {useEffect, useRef, useState} from 'react';
import type {Auth, Conversation, Run, Workflow} from './types';
import {activeRun, ApiError, errorMessage, json, post, watchRun} from './api';
import ResultCard from './ResultCard';

const route = () => /^\/conversations\/([0-9a-f-]{36})$/.exec(window.location.pathname)?.[1] || null;
type Pending = {path: string; body: Record<string, unknown>; question: string};
export default function Workspace({auth}: {auth: Auth}) {
  const [conversations, setConversations] = useState<Conversation[]>([]);
  const [conversationId, setConversationId] = useState(route);
  const [historyMore, setHistoryMore] = useState(false);
  const [runs, setRuns] = useState<Run[]>([]);
  const [runMore, setRunMore] = useState(false);
  const [workflows, setWorkflows] = useState<Record<string, Workflow>>({});
  const [question, setQuestion] = useState('');
  const [reply, setReply] = useState('');
  const [progress, setProgress] = useState('');
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);
  const [reload, setReload] = useState(0);
  const [reconnect, setReconnect] = useState(0);
  const [followUp, setFollowUp] = useState(true);
  const [pending, setPending] = useState<Pending | null>(null);
  const mounted = useRef(true), loading = useRef(false);
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
    const controller = new AbortController();
    setRuns([]); setWorkflows({}); setError(''); setRunMore(false);
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
    if (!watching) {setProgress(''); return;}
    const controller = new AbortController(); setProgress('Connecting to your run…');
    watchRun(auth, watching, controller.signal, text => {if (!controller.signal.aborted) setProgress(text);}, state => {if (!controller.signal.aborted) update(state);})
      .catch(e => {if (!controller.signal.aborted) {fail(e); setProgress('Connection paused. Reconnect to retrieve the saved outcome.');}});
    return () => controller.abort();
  }, [auth, watching, reconnect]);
  const latestAnswer = runs.find(r => workflows[r.id]?.status === 'completed' && workflows[r.id]?.answer);
  const sendPending = async (request: Pending) => {
    setPending(request);
    let state: Workflow;
    try {state = await json<Workflow>(auth, request.path, {...post(request.body), signal: lifetime.current.signal});}
    catch (e) {
      // A definite rejection can be revised; uncertain delivery retains its key.
      if (e instanceof ApiError && [400, 404, 409, 422].includes(e.status)) setPending(null);
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
      const created = await json<Conversation>(auth, '/v1/conversations', {...post({title: text.slice(0, 160)}), signal: lifetime.current.signal});
      id = created.id;
      const request = {path: `/v1/conversations/${id}/messages`, body: {question: text, idempotency_key: crypto.randomUUID()}, question: text};
      try {await sendPending(request);} finally {
        window.history.pushState(null, '', `/conversations/${id}`); setConversationId(id); setReload(n => n + 1);
      }
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
      onClick={e => {e.preventDefault(); if (!busy && !pending) navigate(c.id);}}>{c.title}</a>)}</nav>
    {historyMore && <button className="text-button" disabled={busy} onClick={() => act(() => loadHistory(true))}>Older conversations</button>}
    <p className="sidebar-note">Clear scope.<br />Traceable answers.<br />Your business, as recorded.</p></aside>
    <main id="workspace" className="conversation"><div className="conversation-heading"><p className="eyebrow">BUSINESS INTELLIGENCE</p><h1>{conversationId ? 'Your analysis' : 'What would you like to understand?'}</h1>
      <p>Ask a question. Explore the detail. Follow the evidence.</p></div>
      {!runs.length && !busy && <div className="suggestions">{['Show revenue for the last completed month.', 'Show stored Order Value by category.', 'What is the five-year conversion rate?'].map(text => <button className="suggestion" key={text} onClick={() => setQuestion(text)}>{text}<span aria-hidden="true">↗</span></button>)}</div>}
      {runMore && <button className="secondary" disabled={busy} onClick={olderRuns}>Load older answers</button>}
      <div className="messages">{[...runs].reverse().map(run => {
        const state = workflows[run.id];
        return <article className="message" key={run.id}><div className="question"><span className="eyebrow">YOU ASKED</span><h2>{run.question}</h2></div>
          {state?.answer ? <ResultCard auth={auth} answer={state.answer} /> : <p className="run-state">{(state?.status || run.status).replaceAll('_', ' ')}</p>}
          {state && ['failed', 'cancelled', 'interrupted'].includes(state.status) && <div className="notice"><p>{state.status === 'interrupted' ? 'Execution was interrupted. Retry to start a new run with current access and data.' : state.status === 'cancelled' ? 'This run was cancelled.' : 'The analyst could not complete this request. You can retry or revise the question.'}</p>
            <button className="secondary" disabled={busy || !!current || !!pending} onClick={() => send(run)}>Retry question</button></div>}
          {!state && run.status === 'completed' && <p className="note">This earlier run has no conversational answer.</p>}
        </article>;
      })}</div>
      {current && <section className="run-control" aria-label="Current run"><p role="status" aria-live="polite">{progress || 'A clarification is needed.'}</p>
        <div className="actions"><button className="secondary" disabled={busy} onClick={() => act(async () => {
          await json(auth, `/v1/runs/${current.run_id}/cancel`, {...post({}), signal: lifetime.current.signal});
          update(await json<Workflow>(auth, `/v1/runs/${current.run_id}/workflow`, {signal: lifetime.current.signal}));
        })}>Cancel run</button>{watching && <button className="text-button" onClick={() => {setError(''); setReconnect(n => n + 1);}}>Reconnect</button>}</div>
        {current.clarification && <form onSubmit={e => {e.preventDefault(); resume();}}><label htmlFor="reply">{current.clarification.question}</label>
          <div className="options">{current.clarification.options.map(option => <button key={option} type="button" className="secondary" onClick={() => setReply(option)}>{option}</button>)}</div>
          <textarea id="reply" maxLength={2000} value={reply} onChange={e => setReply(e.target.value)} required /><button disabled={busy || !!pending || !reply.trim()}>Send clarification</button></form>}
      </section>}
      {error && <div role="alert" className="error"><p>{error}</p><button className="text-button" onClick={() => {setReload(n => n + 1); setReconnect(n => n + 1);}}>Reload saved conversation</button></div>}
      {pending && <div className="notice"><p>Submission was not confirmed. Retry safely with the same request.</p><button disabled={busy} onClick={() => act(() => sendPending(pending))}>Retry submission</button></div>}
      <form className="composer" onSubmit={e => {e.preventDefault(); send();}}><label htmlFor="question">{latestAnswer ? 'Ask a follow-up or a new question' : 'Your question'}</label>
        <textarea id="question" placeholder="e.g. How did revenue compare with the previous month?" maxLength={8000} value={question} onChange={e => setQuestion(e.target.value)} required disabled={!!current} />
        <div className="composer-footer">{latestAnswer ? <label className="checkbox"><input type="checkbox" checked={followUp} onChange={e => setFollowUp(e.target.checked)} />Use the latest answer as context</label> : <span>Five core metrics. Grounded in your data.</span>}
          <button disabled={busy || !!current || !!pending || !question.trim()}>Ask analyst <span aria-hidden="true">↑</span></button></div>
      </form><p className="note footer-note">Results use retained reporting history. Review scope and coverage before sharing.</p>
    </main></div>;
}
