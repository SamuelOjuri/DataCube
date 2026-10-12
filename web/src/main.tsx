import {useEffect, useState} from 'react';
import {createRoot} from 'react-dom/client';
import {createAuth} from './auth.mjs';
import type {Auth, Session} from './types';
import Workspace from './Workspace';
import SignIn from './SignIn';
import SessionWarning from './SessionWarning';
import {clearDrafts, isolateDrafts} from './drafts.mjs';
import '@fontsource/open-sans/latin-400.css';
import '@fontsource/open-sans/latin-600.css';
import '@fontsource/open-sans/latin-700.css';
import './style.css';

function App() {
  const [session, setSession] = useState<Session>(null);
  const [message, setMessage] = useState('');
  const [busy, setBusy] = useState(true);
  const [checking, setChecking] = useState(true);
  const [auth] = useState<Auth>(() => createAuth({apiOrigin: import.meta.env.VITE_API_ORIGIN,
    onChange: (next: Session, reason?: string) => {
      if (reason === 'explicit_logout' || reason === 'logout_pending') clearDrafts();
      if (next) isolateDrafts(next.subject);
      setSession(next); setMessage(next ? '' : reason === 'session_idle_expired' ? 'Your session ended because of inactivity. Sign in to recover your draft.'
        : reason === 'session_absolute_expired' ? 'Your session reached its maximum lifetime. Sign in to continue.'
        : reason === 'permissions_changed' || reason === 'access_denied' ? 'Your account access changed. Sign in again or contact your administrator.'
        : 'Sign in to access or reconnect to your conversations.');
    }}));
  const failure = (error: unknown) => setMessage(error instanceof Error ? error.message : 'Please try again.');
  useEffect(() => {
    auth.finish().then(async signedIn => {if (!signedIn) await auth.restore();})
      .catch(failure).finally(() => {setBusy(false); setChecking(false);});
    return () => auth.dispose();
  }, [auth]);
  const act = async (fn: () => Promise<unknown>) => {
    setBusy(true); setMessage('');
    try {await fn();} catch (error) {failure(error);} finally {setBusy(false);}
  };
  return <><a href="#workspace" className="skip-link">Skip to content</a>
    <header className={`app-header${session ? '' : ' signin-header'}`}><a href="/" className="brand">DataCube <span>Analyst</span></a><div className="header-right"><span>TaperedPlus</span>
      {session && <button className="secondary" onClick={() => act(auth.logout)}>Sign out</button>}</div></header>
    {checking ? <main id="workspace" className="session-checking" role="status">Checking your session…</main>
      : session ? <><SessionWarning session={session} auth={auth} /><Workspace key={session.subject} subject={session.subject} auth={auth} /></>
      : <><SignIn busy={busy} message={message} onLogin={() => act(auth.login)} />{!busy && <button className="session-retry secondary" onClick={() => act(auth.restore)}>Retry session check</button>}</>}
  </>;
}
try {const check = createAuth({apiOrigin: import.meta.env.VITE_API_ORIGIN}); check.dispose(); createRoot(document.getElementById('root')!).render(<App />);}
catch {document.getElementById('root')!.textContent = 'Sign-in is not configured. Please contact your administrator.';}
