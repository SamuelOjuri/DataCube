import type {AgentActivity} from './agentActivity';

export default function AgentStatus({phase, message, announce = true}: AgentActivity & {announce?: boolean}) {
  return <p className={`agent-status agent-status--${phase}`} role={announce ? 'status' : undefined}
    aria-live={announce ? 'polite' : undefined} aria-atomic={announce ? true : undefined}>
    <span className="agent-motion" aria-hidden="true">
      {phase === 'completed' || phase === 'failed' ? <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
        <circle cx="12" cy="12" r="9" />
        <path d={phase === 'completed' ? 'm8 12 3 3 5-6' : 'M12 7v6m0 4h.01'} />
      </svg> : <><i /><i /><i /></>}
    </span>
    <span className="agent-status-message">{message}</span>
  </p>;
}
