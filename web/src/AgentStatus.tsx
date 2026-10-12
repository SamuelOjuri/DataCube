import {useEffect, useState} from 'react';
import type {AgentActivity} from './agentActivity';

export function Elapsed({since}: {since: string}) {
  const [now, setNow] = useState(Date.now);
  useEffect(() => {const timer = setInterval(() => setNow(Date.now()), 1000); return () => clearInterval(timer);}, []);
  const start = Date.parse(since);
  if (Number.isNaN(start)) return null;
  const seconds = Math.max(0, Math.floor((now - start) / 1000));
  return <span className="run-elapsed">{seconds < 60 ? `${seconds}s` : `${Math.floor(seconds / 60)}m ${String(seconds % 60).padStart(2, '0')}s`}</span>;
}

export default function AgentStatus({phase, message, announce = true}: AgentActivity & {announce?: boolean}) {
  const active = !['waiting', 'completed', 'failed'].includes(phase);
  return <p className={`agent-status agent-status--${phase}${active ? ' agent-status--active' : ''}`} role={announce ? 'status' : undefined}
    aria-live={announce ? 'polite' : undefined} aria-atomic={announce ? true : undefined}>
    <span className="agent-motion" aria-hidden="true">
      {phase === 'completed' || phase === 'failed' ? <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
        <circle cx="12" cy="12" r="9" />
        <path d={phase === 'completed' ? 'm8 12 3 3 5-6' : 'M12 7v6m0 4h.01'} />
      </svg> : <><i /><i /><i /></>}
    </span>
    <span key={message} className="agent-status-message">{message}</span>
  </p>;
}
