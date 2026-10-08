import {useEffect, useState} from 'react';
import {createRoot} from 'react-dom/client';
import {createAuth} from './auth.mjs';
import type {Auth, Session} from './types';
import Workspace from './Workspace';
import './style.css';

function App() {
  const [session, setSession] = useState<Session>(null);
  const [message, setMessage] = useState('');
  const [busy, setBusy] = useState(true);
  const [auth] = useState<Auth>(() => createAuth({apiOrigin: import.meta.env.VITE_API_ORIGIN,
    onChange: (next: Session) => {setSession(next); setMessage(next ? '' : 'Sign in to access or reconnect to your conversations.');}}));
  const failure = (error: unknown) => setMessage(error instanceof Error ? error.message : 'Please try again.');
  useEffect(() => {
    auth.finish().then(async signedIn => {if (signedIn) await auth.request('/auth/session').then(r => r.json());})
      .catch(failure).finally(() => setBusy(false));
  }, [auth]);
  const act = async (fn: () => Promise<unknown>) => {
    setBusy(true); setMessage('');
    try {await fn();} catch (error) {failure(error);} finally {setBusy(false);}
  };
  return <><a href="#workspace" className="skip-link">Skip to content</a>
    <header className="app-header"><a href="/" className="brand">DataCube <span>Analyst</span></a><div className="header-right"><span>Tapered Plus</span>
      {session && <button className="secondary" onClick={() => act(auth.logout)}>Sign out</button>}</div></header>
    {session ? <Workspace key={session.subject} auth={auth} /> : <main id="workspace" className="signin"><section>
      <p className="eyebrow">YOUR COMPANY. CLEARER.</p><h1>Your data,<br />in conversation.</h1>
      <p>Explore enquiries, orders, revenue, conversion and gestation.<br />Every answer comes with its source and scope.</p>
      <button disabled={busy} onClick={() => act(auth.login)}>{busy ? 'Please wait…' : 'Continue with Monday'}</button>
      <p className="note">Access is available to approved team members.</p><p role="status">{message}</p>
    </section></main>}
  </>;
}
try {createAuth({apiOrigin: import.meta.env.VITE_API_ORIGIN}); createRoot(document.getElementById('root')!).render(<App />);}
catch {document.getElementById('root')!.textContent = 'Sign-in is not configured. Please contact your administrator.';}
