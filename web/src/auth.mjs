// Application tokens live only in memory. Storage holds only a pending login verifier.
const PENDING = 'datacube.pending-login';
const b64 = bytes => btoa(String.fromCharCode(...new Uint8Array(bytes)))
  .replaceAll('+', '-').replaceAll('/', '_').replaceAll('=', '');

export function createAuth({apiOrigin, browser = window, onChange = (_session) => {}}) {
  const url = new URL(apiOrigin);
  if (url.origin !== apiOrigin || (url.protocol !== 'https:' &&
      !(url.protocol === 'http:' && ['localhost', '127.0.0.1'].includes(url.hostname)))) {
    throw new Error('An explicit API origin is required');
  }
  let session = null;
  let timer;
  let generation = 0;
  const active = new Set();

  function clear() {
    generation += 1;
    session = null;
    browser.clearTimeout(timer);
    for (const request of active) request.abort();
    active.clear();
    onChange(null);
  }

  async function login() {
    clear();
    const verifier = b64(browser.crypto.getRandomValues(new Uint8Array(32)));
    const challenge = b64(await browser.crypto.subtle.digest('SHA-256', new TextEncoder().encode(verifier)));
    const returnTo = /^\/conversations\/[0-9a-f-]{36}$/.test(browser.location.pathname) ? browser.location.pathname : '/';
    browser.sessionStorage.setItem(PENDING, JSON.stringify({verifier, created: Date.now(), returnTo}));
    browser.location.assign(`${apiOrigin}/auth/login?client_challenge=${encodeURIComponent(challenge)}`);
  }

  async function finish() {
    const params = new URLSearchParams(browser.location.hash.slice(1));
    if (!params.has('code') && !params.has('error')) return false;
    browser.history.replaceState(null, '', browser.location.pathname + browser.location.search);
    const raw = browser.sessionStorage.getItem(PENDING);
    browser.sessionStorage.removeItem(PENDING);
    const pending = raw && JSON.parse(raw);
    if (params.has('error') || !pending || Date.now() - pending.created > 300000 ||
        !/^[A-Za-z0-9_-]{43}$/.test(params.get('code') || '')) {
      throw new Error('Sign-in could not be completed. Please try again.');
    }
    const current = generation;
    const response = await browser.fetch(`${apiOrigin}/auth/exchange`, {
      method: 'POST', credentials: 'omit', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({code: params.get('code'), verifier: pending.verifier}),
    });
    if (!response.ok) throw new Error('Sign-in could not be completed. Please try again.');
    const received = await response.json();
    if (current !== generation) return false;
    const lifetime = Date.parse(received.expires_at) - Date.now();
    if (!/^[A-Za-z0-9_-]{43}$/.test(received.access_token) || !Number.isFinite(lifetime) || lifetime <= 0 || lifetime > 900000) {
      throw new Error('Sign-in could not be completed. Please try again.');
    }
    session = received;
    browser.history.replaceState(null, '', /^\/conversations\/[0-9a-f-]{36}$/.test(pending.returnTo) ? pending.returnTo : '/');
    timer = browser.setTimeout(clear, lifetime);
    onChange({subject: session.subject, expires_at: session.expires_at});
    return true;
  }

  async function request(path, options = {}) {
    // Restrict all destinations so an API caller cannot exfiltrate the bearer.
    if (!path.startsWith('/v1/') && path !== '/auth/session') throw new Error('Unsupported API path');
    if (/[?#\\]/.test(path) || path.includes('..')) throw new Error('Unsupported API path');
    if (!session || Date.parse(session.expires_at) <= Date.now()) {
      clear();
      throw new Error('Please sign in to continue.');
    }
    const current = generation;
    const controller = new AbortController();
    active.add(controller);
    const abort = () => controller.abort();
    options.signal?.addEventListener('abort', abort, {once: true});
    if (options.signal?.aborted) controller.abort();
    const release = () => {active.delete(controller); options.signal?.removeEventListener('abort', abort);};
    const query = new URLSearchParams();
    for (const [key, value] of Object.entries(options.query || {})) {
      if (!['after', 'limit', 'offset'].includes(key) || !Number.isSafeInteger(value) || value < 0 || value > 10000) {
        release(); throw new Error('Unsupported API query');
      }
      query.set(key, String(value));
    }
    const headers = new Headers(options.headers);
    headers.set('Authorization', `Bearer ${session.access_token}`);
    try {
      const response = await browser.fetch(apiOrigin + path + (query.size ? '?' + query : ''), {
        ...options, headers, credentials: 'omit', redirect: 'error', signal: controller.signal,
      });
      if (current !== generation) throw new Error('Your session has ended.');
      if ([401, 403].includes(response.status)) {
        clear();
        throw new Error('Your session has ended. Please sign in again.');
      }
      if (!response.body) {release(); return response;}
      const reader = response.body.getReader();
      // Retain cancellation through body consumption, including long-lived SSE.
      return new Response(new ReadableStream({
        async pull(stream) {
          try {
            const {done, value} = await reader.read();
            if (current !== generation || controller.signal.aborted) throw new Error('Your session has ended.');
            if (done) {release(); stream.close();} else stream.enqueue(value);
          } catch (error) {release(); stream.error(error); await reader.cancel().catch(() => {});}
        },
        async cancel() {controller.abort(); release(); await reader.cancel().catch(() => {});},
      }), {status: response.status, statusText: response.statusText, headers: response.headers});
    } catch (error) {controller.abort(); release(); throw error;}
  }

  async function logout() {
    const token = session?.access_token;
    clear();
    browser.sessionStorage.removeItem(PENDING);
    if (!token) return;
    try {
      const response = await browser.fetch(`${apiOrigin}/auth/logout`, {
        method: 'POST', credentials: 'omit', redirect: 'error', headers: {Authorization: `Bearer ${token}`},
      });
      if (!response.ok) throw new Error('logout_failed');
    } catch {
      throw new Error('Signed out here. Server sign-out could not be confirmed; the session expires within 15 minutes.');
    }
  }
  return {login, finish, request, logout};
}
