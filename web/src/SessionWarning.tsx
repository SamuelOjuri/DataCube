import {useEffect, useState} from 'react';
import type {Auth, Session} from './types';

export default function SessionWarning({session, auth}: {session:NonNullable<Session>; auth:Auth}) {
  const [now, setNow] = useState(performance.now());
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);
  useEffect(() => {const timer = setInterval(() => setNow(performance.now()), 1000); return () => clearInterval(timer);}, []);
  const absolute = session.absoluteDeadline <= session.idleDeadline;
  const remaining = Math.max(0, Math.ceil((Math.min(session.absoluteDeadline, session.idleDeadline) - now) / 1000));
  if (!session.managed || remaining > (session.warning_seconds || 120)) return null;
  return <section className="session-warning" role="region" aria-labelledby="session-warning-title">
    <h2 id="session-warning-title">{absolute ? 'Sign in again to continue' : 'Your session will end because of inactivity'}</h2>
    <p role="status">{absolute ? 'Another Monday sign-in is required. Your draft will be preserved.' : 'Stay signed in to keep working.'} Session ends in {remaining} seconds.</p>
    {error && <p role="alert">{error}</p>}
    <button disabled={busy} onClick={async () => {setBusy(true); setError(''); try {await (absolute ? auth.login() : auth.activity({force:true}));} catch(e) {setError(e instanceof Error ? e.message : 'Session update failed.');} finally {setBusy(false);}}}>
      {absolute ? 'Sign in with Monday' : 'Stay signed in'}
    </button>
  </section>;
}
