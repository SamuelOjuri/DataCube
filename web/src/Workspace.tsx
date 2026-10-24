import {useEffect, useLayoutEffect, useRef, useState} from 'react';
import type {Auth, Conversation, Run, Workflow} from './types';
import {activeRun, ApiError, errorMessage, json, post, watchRun} from './api';
import ResultCard from './ResultCard';
import ExplanationCard from './ExplanationCard';
import {isExplanation} from './types';
import {historyTime, questionTurns, runOutcome} from './conversation';
import AgentStatus, {Elapsed} from './AgentStatus';
import type {AgentActivity} from './agentActivity';
import {readDraft, saveDraft} from './drafts.mjs';

const route = () => /^\/conversations\/([0-9a-f-]{36})$/.exec(window.location.pathname)?.[1] || null;
type SubmissionRequest = {path: string; body: Record<string, unknown>; question: string};
type Pending = {request: SubmissionRequest; status: 'submitting' | 'unconfirmed'};
function sizeQuestion(input: HTMLTextAreaElement) {
  input.style.height = 'auto';
  input.style.height = `${input.scrollHeight}px`;
}
export default function Workspace({auth, subject}: {auth: Auth; subject: string}) {
  const [recovered] = useState(() => readDraft(subject, route()));
  const [conversations, setConversations] = useState<Conversation[]>([]);
  const [conversationId, setConversationId] = useState(route);
  const [historyMore, setHistoryMore] = useState(false);
  const [runs, setRuns] = useState<Run[]>([]);
  const [runMore, setRunMore] = useState(false);
  const [workflows, setWorkflows] = useState<Record<string, Workflow>>({});
  const [question, setQuestion] = useState<string>(recovered.question);
  const [reply, setReply] = useState<string>(recovered.reply);
  const [progress, setProgress] = useState<AgentActivity | null>(null);
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);
  const [reload, setReload] = useState(0);
  const [reconnect, setReconnect] = useState(0);
  const [followUp, setFollowUp] = useState(true);
  const [pending, setPending] = useState<Pending | null>(recovered.pending);
  const [creatingConversation, setCreatingConversation] = useState(false);
  const [finished, setFinished] = useState(false);
  const mounted = useRef(true), loading = useRef(false);
  const questionInput = useRef<HTMLTextAreaElement>(null);
  const lastWatched = useRef<string | null>(null);
  const acknowledgedConversation = useRef<string | null>(null);
  const lifetime = useRef(new AbortController());
  const draft = useRef({question, reply, pending});
  const persist = (change: Partial<typeof draft.current>, id = route()) => {
    draft.current = {...draft.current, ...change}; saveDraft(subject, id, draft.current);
  };
  const editQuestion = (text: string) => {persist({question:text}); setQuestion(text);};
  const editReply = (text: string) => {persist({reply:text}); setReply(text);};
  useLayoutEffect(() => {
    if (questionInput.current) sizeQuestion(questionInput.current);
  }, [question]);
  useLayoutEffect(() => {
    const input = questionInput.current;
    if (!input) return;
    let width = input.clientWidth, active = true;
    const observer = new ResizeObserver(() => {
      if (input.clientWidth === width) return;
      width = input.clientWidth;
      sizeQuestion(input);
    });
    observer.observe(input);
    document.fonts.ready.then(() => {if (active) sizeQuestion(input);});
    return () => {active = false; observer.disconnect();};
  }, []);
  useEffect(() => {
    const interact = () => {if (document.visibilityState === 'visible') auth.activity().catch(fail);};
    const workspace = document.getElementById('workspace');
    for (const event of ['keydown', 'input', 'pointerdown', 'scroll']) workspace?.addEventListener(event, interact, {capture:true, passive:true});
    return () => {for (const event of ['keydown', 'input', 'pointerdown', 'scroll']) workspace?.removeEventListener(event, interact, true);};
  }, [auth]);
  useEffect(() => () => {mounted.current = false; lifetime.current.abort();}, []);
  const fail = (e: unknown) => {if (mounted.current) setError(errorMessage(e));};
  const update = (state: Workflow) => setWorkflows(prev => ({...prev, [state.run_id]: state}));
  const navigate = (id: string | null) => {
    window.history.pushState(null, '', id ? `/conversations/${id}` : '/');
    const restored = readDraft(subject, id); draft.current = restored;
    setConversationId(id); setQuestion(restored.question); setReply(restored.reply); setPending(restored.pending); setError('');
  };
  useEffect(() => {const changed = () => {const restored = readDraft(subject, route()); draft.current = restored; setConversationId(route()); setQuestion(restored.question); setReply(restored.reply); setPending(restored.pending); setError('');}; window.addEventListener('popstate', changed); return () => window.removeEventListener('popstate', changed);}, []);
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
    const controller = new AbortController(); setProgress(prev => ({phase: 'connecting', message: 'Connecting to your run…', fraction: prev?.fraction}));
    // Stages can repeat (exploration, clarification), so the bar only moves forward.
    watchRun(auth, watching, controller.signal, activity => {if (!controller.signal.aborted) setProgress(prev => ({...activity, fraction: Math.max(prev?.fraction ?? 0, activity.fraction ?? 0)}));}, state => {if (!controller.signal.aborted) update(state);})
      .catch(e => {if (!controller.signal.aborted) {fail(e); setProgress(prev => ({phase: 'waiting', message: 'Connection paused. Reconnect to retrieve the saved outcome.', fraction: prev?.fraction}));}});
    return () => controller.abort();
  }, [auth, watching, reconnect]);
  useEffect(() => {
    const previous = lastWatched.current; lastWatched.current = watching;
    if (watching || !previous || workflows[previous]?.status !== 'completed') return;
    setFinished(true);
    const timer = setTimeout(() => setFinished(false), 900);
    return () => {clearTimeout(timer); setFinished(false);};
  }, [watching, conversationId]);
  // Explanations carry no dataset, so only result answers can anchor a follow-up.
  const latestAnswer = runs.find(r => workflows[r.id]?.status === 'completed' && workflows[r.id]?.answer && !isExplanation(workflows[r.id].answer));
  const compose = (text: string) => {editQuestion(text); document.getElementById('question')?.focus();};
  const turns = questionTurns(runs, workflows);
  const sendPending = async (request: SubmissionRequest) => {
    persist({pending:{request, status:'unconfirmed'}});
    setPending({request, status:'submitting'});
    let state: Workflow;
    try {state = await json<Workflow>(auth, request.path, {...post(request.body), signal: lifetime.current.signal});}
    catch (e) {
      // A definite rejection can be revised; uncertain delivery retains its key.
      const failed = e instanceof ApiError && [400, 404, 409, 422].includes(e.status) ? null : {request, status:'unconfirmed' as const};
      if (mounted.current) {persist({pending:failed}); setPending(failed);}
      throw e;
    }
    if (!mounted.current) return;
    persist({question:'', reply:'', pending:null});
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
        // Adopt the durable route before sending so expiry can recover an uncertain run.
        window.history.replaceState(null, '', `/conversations/${id}`);
        saveDraft(subject, null, {question:'', reply:'', pending:null});
        saveDraft(subject, id, draft.current);
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
      ].map(({title,question: suggestedQuestion}) => <button className="suggestion" key={title} onClick={() => editQuestion(suggestedQuestion)}>
        <span className="suggestion-copy"><strong className="suggestion-title">{title}</strong><span className="suggestion-question">{suggestedQuestion}</span></span>
        <span className="suggestion-icon" aria-hidden="true"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round"><path d="M5 12h14m-6-6 6 6-6 6" /></svg></span>
      </button>)}</div>}
      {runMore && <button className="secondary" disabled={busy} onClick={olderRuns}>Load older answers</button>}
      <div className="messages">{turns.map(({run, previousAttempts}) => {
        const state = workflows[run.id];
        return <article className="message" key={run.id}><div className="question"><span className="eyebrow">YOU ASKED</span><h2>{run.question}</h2></div>
          {state?.answer ? isExplanation(state.answer)
            ? <ExplanationCard answer={state.answer} onSuggest={compose} />
            : <ResultCard auth={auth} answer={state.answer} /> : current?.run_id === run.id ? null : !state || !['failed', 'cancelled', 'interrupted'].includes(state.status) ? <p className="run-state">{(state?.status || run.status).replaceAll('_', ' ')}</p> : null}
          {state && ['failed', 'cancelled', 'interrupted'].includes(state.status) && <div className="notice"><AgentStatus announce={false} {...runOutcome(state)} />
            <div className="actions"><button className="secondary" disabled={busy || !!current || !!pending} onClick={() => send(run)}>Retry question</button>
              <button className="secondary" disabled={!!current || !!pending} onClick={() => compose(run.question)}>Edit question</button></div></div>}
          {!state && run.status === 'completed' && <p className="note">This earlier run has no conversational answer.</p>}
          {!!previousAttempts.length && <details className="attempt-history"><summary>{previousAttempts.length} earlier unsuccessful {previousAttempts.length === 1 ? 'attempt' : 'attempts'}</summary>
            <ul>{previousAttempts.map(attempt => <li key={attempt.id}><time dateTime={attempt.created_at}>{historyTime(attempt.created_at)}</time> · {(workflows[attempt.id]?.status || attempt.status).replaceAll('_', ' ')}</li>)}</ul>
          </details>}
        </article>;
      })}</div>
      {current && <section className="run-control" aria-label="Current run"><div className="run-control-head"><AgentStatus {...(watching
        ? progress ?? {phase: 'connecting', message: 'Connecting to your run…'}
        : {phase: 'waiting', message: 'A clarification is needed.'})} />
        <div className="run-control-actions">{watching && <Elapsed since={runs.find(r => r.id === watching)?.created_at ?? ''} />}
          {watching && progress?.phase === 'waiting' && <button className="text-button" onClick={() => {setError(''); setReconnect(n => n + 1);}}>Reconnect</button>}
          <button className="text-button" disabled={busy} onClick={() => act(async () => {
            await json(auth, `/v1/runs/${current.run_id}/cancel`, {...post({}), signal: lifetime.current.signal});
            update(await json<Workflow>(auth, `/v1/runs/${current.run_id}/workflow`, {signal: lifetime.current.signal}));
          })}>Cancel run</button></div></div>
        {current.clarification && <form onSubmit={e => {e.preventDefault(); resume();}}><label htmlFor="reply">{current.clarification.question}</label>
          <div className="options">{current.clarification.options.map(option => <button key={option} type="button" className="secondary" onClick={() => editReply(option)}>{option}</button>)}</div>
          <textarea id="reply" maxLength={2000} value={reply} onChange={e => editReply(e.target.value)} required /><button disabled={busy || !!pending || !reply.trim()}>Send clarification</button></form>}
        {watching && <span className="run-progress" aria-hidden="true" style={{transform: `scaleX(${progress?.fraction ?? 0})`}} />}
      </section>}
      {!current && finished && <div className="run-control run-control--done" aria-hidden="true"><AgentStatus announce={false} phase="completed" message="Answer ready" /></div>}
      {error && <div role="alert" className="error"><p>{error}</p><button className="text-button" onClick={() => {setError(''); setReload(n => n + 1); setReconnect(n => n + 1);}}>Reload saved conversation</button></div>}
      {(creatingConversation || pending?.status === 'submitting') && <div className="notice"><AgentStatus phase="submitting" message={pending?.request.path.endsWith('/resume') ? 'Submitting your clarification…' : 'Submitting your query…'} /></div>}
      {pending?.status === 'unconfirmed' && <div className="notice"><AgentStatus announce={false} phase="waiting" message="Submission was not confirmed. Retry safely with the same request." /><button disabled={busy} onClick={() => act(() => sendPending(pending.request))}>Retry submission</button></div>}
      <form className="composer" onSubmit={e => {e.preventDefault(); send();}}><label htmlFor="question">{latestAnswer ? 'Ask a follow-up or a new question' : 'Your question'}</label>
        <textarea ref={questionInput} id="question" rows={1} placeholder="e.g. How did revenue compare with the previous month?" maxLength={8000} value={question} onChange={e => editQuestion(e.target.value)} required disabled={creatingConversation || pending?.status === 'submitting' || !!current} />
        <div className="composer-footer">{latestAnswer && <label className="checkbox"><input type="checkbox" checked={followUp} onChange={e => setFollowUp(e.target.checked)} />Use the latest answer as context</label>}
          <button className="ask-analyst" disabled={busy || !!current || !!pending || !question.trim()}>Ask analyst
            <span className="ask-analyst-icon" aria-hidden="true"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M12 19V5m-6 6 6-6 6 6" /></svg></span>
          </button></div>
      </form>
    </main></div>;
}
