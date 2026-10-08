import {useEffect, useState} from 'react';
import {createRoot} from 'react-dom/client';
import {createAuth} from './auth.mjs';
import './style.css';

type Session = {subject: string; expires_at: string} | null;

function App() {
  const [session, setSession] = useState<Session>(null);
  const [message, setMessage] = useState('');
  const [busy, setBusy] = useState(true);
  const [auth] = useState(() => createAuth({apiOrigin: import.meta.env.VITE_API_ORIGIN,
    onChange: (next: Session) => {setSession(next); setMessage('');}}));
  const failure = (error: unknown) => setMessage(error instanceof Error ? error.message : 'Please try again.');
  useEffect(() => {
    auth.finish().then(async signedIn => {
      if (signedIn) await auth.request('/auth/session');
    }).catch(failure).finally(() => setBusy(false));
  }, [auth]);
  const act = async (fn: () => Promise<unknown>) => {
    setBusy(true); setMessage('');
    try {await fn();} catch (error) {failure(error);} finally {setBusy(false);}
  };
  return <main>
    <div className="brand">DataCube <span>Analyst</span></div>
    <section aria-labelledby="title">
      <p className="eyebrow">YOUR COMPANY. CLEARER.</p>
      <h1 id="title">{session ? 'You’re signed in.' : 'Your data starts here.'}</h1>
      <p>{session ? 'Your secure session is ready.' : 'Sign in with your Monday account to access DataCube.'}</p>
      <button disabled={busy} onClick={() => act(session ? auth.logout : auth.login)}>
        {busy ? 'Please wait…' : session ? 'Sign out' : 'Continue with Monday'}
      </button>
      <p className="note">Access is available to approved team members.</p>
      <p role="status" aria-live="polite" className="message">{message}</p>
    </section>
    <footer>DataCube · Tapered Plus</footer>
  </main>;
}

try {
  createAuth({apiOrigin: import.meta.env.VITE_API_ORIGIN});
  createRoot(document.getElementById('root')!).render(<App />);
}
catch {document.getElementById('root')!.textContent = 'Sign-in is not configured. Please contact your administrator.';}
