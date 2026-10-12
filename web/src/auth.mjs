// API credentials are scoped to this tab and API origin. Broadcasts contain status only.
const PENDING = 'datacube.pending-login';
const LOGOUT = 'datacube.logout-pending';
const VERSION = 'datacube.logout-version';
const SESSION = 'datacube.session';
const REVOKE = 'datacube.logout-token';
const b64 = bytes => btoa(String.fromCharCode(...new Uint8Array(bytes)))
  .replaceAll('+', '-').replaceAll('/', '_').replaceAll('=', '');
const ended = new Set(['authentication_required', 'session_expired', 'session_idle_expired',
  'session_absolute_expired', 'session_revoked', 'permissions_changed', 'access_denied', 'pilot_access_required']);
const messages = {
  session_idle_expired: 'Your session ended because of inactivity. Please sign in again.',
  session_absolute_expired: 'Your session reached its maximum lifetime. Please sign in again.',
  permissions_changed: 'Your access permissions changed. Please sign in again.',
  access_denied: 'Your account no longer has access.',
  origin_denied: 'This application origin is not permitted. Contact your administrator.',
};
export class AuthError extends Error {
  constructor(status, code, message = messages[code] || 'Your session has ended. Please sign in again.') {
    super(message); this.status = status; this.code = code;
  }
}

export function createAuth({apiOrigin, browser = window, onChange = (_session, _reason) => {}}) {
  const url = new URL(apiOrigin);
  if (url.origin !== apiOrigin || (url.protocol !== 'https:' &&
      !(url.protocol === 'http:' && ['localhost', '127.0.0.1'].includes(url.hostname)))) {
    throw new Error('An explicit API origin is required');
  }
  let session = null, timer, idleDeadline = 0, absoluteDeadline = 0, generation = 0;
  let activityPromise, restorePromise, bootstrapping = false, lastActivity = -Infinity, sequence = 0, accepted = 0;
  const active = new Set();
  const storage = browser.localStorage || browser.sessionStorage;
  const get = key => {try {return storage.getItem(key);} catch {return null;}};
  const put = (key, value) => {try {value === null ? storage.removeItem(key) : storage.setItem(key, value);} catch {throw new Error('Browser storage is unavailable for safe sign-out.');}};
  const channel = browser.BroadcastChannel ? new browser.BroadcastChannel('datacube.session') : null;
  const broadcast = type => channel?.postMessage({type});
  const now = () => browser.performance.now();
  const tokenRecord = key => {
    let value;
    try {value = JSON.parse(browser.sessionStorage.getItem(key));} catch { /* discard malformed state */ }
    if (value?.apiOrigin === apiOrigin && /^[A-Za-z0-9_-]{43}$/.test(value.token)) return value.token;
    browser.sessionStorage.removeItem(key); return null;
  };
  const staleSession = () => {
    try {
      const value = JSON.parse(browser.sessionStorage.getItem(SESSION));
      return value && (value.logoutVersion || '') !== (get(VERSION) || '');
    } catch {return false;}
  };
  const saveToken = (key, token) => browser.sessionStorage.setItem(key, JSON.stringify({apiOrigin, token, logoutVersion:get(VERSION) || ''}));

  function clear(reason) {
    generation += 1; session = null; idleDeadline = absoluteDeadline = 0; lastActivity = -Infinity;
    browser.sessionStorage.removeItem(SESSION);
    browser.clearTimeout(timer);
    for (const request of active) request.abort();
    active.clear(); onChange(null, reason);
  }
  function publish() {
    const {access_token: _access, logoutVersion: _version, ...safe} = session;
    onChange({...safe, idleDeadline, absoluteDeadline});
  }
  function accept(received, started, ticket) {
    if (!/^[A-Za-z0-9_-]{43}$/.test(received.access_token)) throw new Error('Sign-in could not be completed. Please try again.');
    const managed = received.absolute_expires_in_ms !== undefined;
    let absolute, idle;
    if (managed) {
      if (!Number.isInteger(received.absolute_expires_in_ms) || received.absolute_expires_in_ms <= 0 || received.absolute_expires_in_ms > 28800000 ||
          !Number.isInteger(received.idle_expires_in_ms) || received.idle_expires_in_ms <= 0 || received.idle_expires_in_ms > 1800000 ||
          !Number.isInteger(received.idle_timeout_seconds) || received.idle_timeout_seconds < 60 || received.idle_timeout_seconds > 1800 ||
          !Number.isInteger(received.warning_seconds) || received.warning_seconds <= 0 ||
          received.warning_seconds >= received.idle_timeout_seconds || !Number.isFinite(Date.parse(received.absolute_expires_at)) ||
          !Number.isFinite(Date.parse(received.idle_expires_at))) throw new Error('Sign-in could not be completed. Please try again.');
      absolute = started + received.absolute_expires_in_ms; idle = started + received.idle_expires_in_ms;
    } else {
      if (!/^[A-Za-z0-9_-]{43}$/.test(received.access_token) || !Number.isFinite(Date.parse(received.expires_at)) ||
          !Number.isInteger(received.expires_in) || received.expires_in <= 0 || received.expires_in > 900) {
        throw new Error('Sign-in could not be completed. Please try again.');
      }
      absolute = idle = started + received.expires_in * 1000;
    }
    if (!received.subject || Math.min(absolute, idle) <= now()) throw new Error('Sign-in could not be completed. Please try again.');
    if (ticket < accepted) return;
    accepted = ticket;
    if (session && session.subject !== received.subject) clear('identity_changed');
    // Schema-7 responses remain memory-only; schema-8 restoration verifies the token on the server.
    if (managed) saveToken(SESSION, received.access_token);
    session = {...received, transport: 'bearer', managed, logoutVersion:get(VERSION) || ''};
    idleDeadline = idle; absoluteDeadline = absolute;
    browser.clearTimeout(timer);
    timer = browser.setTimeout(() => clear(idleDeadline < absoluteDeadline ? 'session_idle_expired' : 'session_absolute_expired'), Math.min(idle, absolute) - now());
    publish();
  }
  async function failure(response) {
    const body = await response.clone().json().catch(() => ({}));
    const code = body.detail || 'authentication_required';
    if (response.status === 401 || response.status === 403 && ended.has(code)) clear(code);
    return new AuthError(response.status, code);
  }
  function headersFor(value, options = {}) {
    const headers = new Headers(options.headers);
    headers.delete('Authorization');
    if (value?.access_token) headers.set('Authorization', `Bearer ${value.access_token}`);
    return headers;
  }

  async function login() {
    const previous = session?.access_token || tokenRecord(SESSION) || tokenRecord(REVOKE);
    clear();
    if (previous) {
      saveToken(REVOKE, previous);
      await retryLogout(previous);
    }
    const verifier = b64(browser.crypto.getRandomValues(new Uint8Array(32)));
    const challenge = b64(await browser.crypto.subtle.digest('SHA-256', new TextEncoder().encode(verifier)));
    const returnTo = /^\/conversations\/[0-9a-f-]{36}$/.test(browser.location.pathname) ? browser.location.pathname : '/';
    browser.sessionStorage.setItem(PENDING, JSON.stringify({verifier, created: Date.now(), returnTo, logoutVersion:get(VERSION) || ''}));
    browser.location.assign(`${apiOrigin}/auth/login?client_challenge=${encodeURIComponent(challenge)}`);
  }
  async function finishHandoff() {
    const params = new URLSearchParams(browser.location.hash.slice(1));
    if (!params.has('code') && !params.has('error')) return false;
    browser.history.replaceState(null, '', browser.location.pathname + browser.location.search);
    const raw = browser.sessionStorage.getItem(PENDING);
    browser.sessionStorage.removeItem(PENDING);
    let pending; try {pending = raw && JSON.parse(raw);} catch { /* fail below */ }
    if (params.has('error') || !pending || Date.now() - pending.created > 300000 ||
        (pending.logoutVersion || '') !== (get(VERSION) || '') ||
        !/^[A-Za-z0-9_-]{43}$/.test(params.get('code') || '')) throw new Error('Sign-in could not be completed. Please try again.');
    const current = generation, logoutVersion = get(VERSION), started = now(), ticket = ++sequence;
    const response = await browser.fetch(`${apiOrigin}/auth/exchange`, {
      method: 'POST', credentials: 'omit', redirect: 'error', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({code: params.get('code'), verifier: pending.verifier}),
    });
    if (!response.ok) throw new Error('Sign-in could not be completed. Please try again.');
    const received = await response.json();
    if (current !== generation || logoutVersion !== get(VERSION)) return false;
    accept(received, started, ticket); put(LOGOUT, null); browser.sessionStorage.removeItem(REVOKE);
    browser.history.replaceState(null, '', /^\/conversations\/[0-9a-f-]{36}$/.test(pending.returnTo) ? pending.returnTo : '/');
    return true;
  }
  function finish() {
    bootstrapping = true;
    return finishHandoff().finally(() => {bootstrapping = false;});
  }
  async function restore() {
    if (restorePromise) return restorePromise;
    restorePromise = (async () => {
      if (get(LOGOUT) || tokenRecord(REVOKE) || staleSession()) {
        const token = tokenRecord(REVOKE) || session?.access_token || tokenRecord(SESSION);
        if (token) saveToken(REVOKE, token);
        clear('logout_pending');
        try {await retryLogout(token);} catch {throw new Error('Server sign-out could not be confirmed. Reconnect to finish signing out.');}
        return false;
      }
      if (session?.access_token && !session.managed) {
        try {await request('/auth/session').then(r => {if (!r.ok) throw new Error('Session checking is unavailable.'); return r.json();}); return true;}
        catch (error) {if (error instanceof AuthError && ended.has(error.code)) return false; throw error;}
      }
      const token = session?.access_token || tokenRecord(SESSION);
      if (!token) return false;
      const current = generation, logoutVersion = get(VERSION), started = now(), ticket = ++sequence;
      let response;
      try {response = await browser.fetch(`${apiOrigin}/auth/session`, {headers:headersFor({access_token:token}), credentials: 'omit', redirect: 'error'});}
      catch {throw new AuthError(503, 'restore_unavailable', 'Session checking is unavailable. Retry when connected.');}
      if (current !== generation) return false;
      if (logoutVersion !== get(VERSION)) {await logout(false); return false;}
      if ([401,403].includes(response.status)) {
        const error = await failure(response);
        if (response.status === 401 || ended.has(error.code)) return false;
        throw error;
      }
      if (!response.ok) throw new AuthError(response.status, 'restore_unavailable', 'Session checking is unavailable. Retry when connected.');
      const received = {...await response.json(), access_token:token};
      if (current !== generation) return false;
      if (received.absolute_expires_in_ms === undefined) {clear(); return false;}
      accept(received, started, ticket); return true;
    })().finally(() => {restorePromise = null;});
    return restorePromise;
  }
  async function activity({force = false} = {}) {
    if (get(LOGOUT) || staleSession()) {await logout(false); throw new AuthError(401, 'authentication_required');}
    if (!session) throw new AuthError(401, 'authentication_required');
    if (!session.managed || browser.document?.visibilityState === 'hidden') return;
    if (activityPromise) return activityPromise;
    if (!force && now() - lastActivity < 60000) return;
    lastActivity = now();
    const value = session, current = generation, logoutVersion = get(VERSION), started = now(), ticket = ++sequence;
    activityPromise = (async () => {
      const options = {method: 'POST', headers: {'Content-Type': 'application/json'}, body: '{}'};
      const response = await browser.fetch(`${apiOrigin}/auth/activity`, {...options, headers: headersFor(value, options), credentials: 'omit', redirect: 'error'});
      if (current !== generation) return;
      if (logoutVersion !== get(VERSION)) {await logout(false); return;}
      if (!response.ok) throw await failure(response);
      const received = {...await response.json(), access_token:value.access_token};
      if (current !== generation) return;
      accept(received, started, ticket); broadcast('activity');
    })().finally(() => {activityPromise = null;});
    return activityPromise;
  }
  async function request(path, options = {}) {
    if ((!path.startsWith('/v1/') && path !== '/auth/session') || /[?#\\]/.test(path) || path.includes('..')) throw new Error('Unsupported API path');
    if (get(LOGOUT) || staleSession()) {await logout(false); throw new Error('Please sign in to continue.');}
    if (!session || Math.min(idleDeadline, absoluteDeadline) <= now()) {
      clear(); throw new Error('Please sign in to continue.');
    }
    if ((options.method || 'GET').toUpperCase() === 'POST' && Math.min(idleDeadline, absoluteDeadline) - now() <= (session.warning_seconds || 120) * 1000) await activity({force:true});
    const current = generation, logoutVersion = get(VERSION), controller = new AbortController(); active.add(controller);
    const abort = () => controller.abort();
    options.signal?.addEventListener('abort', abort, {once:true});
    if (options.signal?.aborted) controller.abort();
    const release = () => {active.delete(controller); options.signal?.removeEventListener('abort', abort);};
    const query = new URLSearchParams();
    for (const [key, value] of Object.entries(options.query || {})) {
      if (!['after', 'limit', 'offset'].includes(key) || !Number.isSafeInteger(value) || value < 0 || value > 10000) {release(); throw new Error('Unsupported API query');}
      query.set(key, String(value));
    }
    try {
      const response = await browser.fetch(apiOrigin + path + (query.size ? '?' + query : ''), {
        ...options, headers: headersFor(session, options), credentials: 'omit', redirect: 'error', signal:controller.signal,
      });
      if (logoutVersion !== get(VERSION)) {logout(false).catch(() => {});}
      if (current !== generation) throw new Error('Your session has ended.');
      if ([401, 403].includes(response.status)) throw await failure(response);
      if (!response.body) {release(); return response;}
      const reader = response.body.getReader();
      return new Response(new ReadableStream({
        async pull(stream) {
          try {
            const {done, value} = await reader.read();
            if (logoutVersion !== get(VERSION)) {logout(false).catch(() => {});}
            if (current !== generation || controller.signal.aborted) throw new Error('Your session has ended.');
            if (done) {release(); stream.close();} else stream.enqueue(value);
          } catch (error) {release(); stream.error(error); await reader.cancel().catch(() => {});}
        },
        async cancel() {controller.abort(); release(); await reader.cancel().catch(() => {});},
      }), {status:response.status, statusText:response.statusText, headers:response.headers});
    } catch (error) {controller.abort(); release(); throw error;}
  }
  async function retryLogout(token = null) {
    if (!token) {put(LOGOUT, null); return;}
    const options = {method:'POST', headers:{'Content-Type':'application/json'}, body:'{}'};
    const response = await browser.fetch(`${apiOrigin}/auth/logout`, {...options, headers:headersFor({access_token:token}, options),
      credentials:'omit', redirect:'error'});
    if (!response.ok) throw new Error('logout_failed');
    browser.sessionStorage.removeItem(REVOKE);
    put(LOGOUT, null);
  }
  async function logout(notify = true) {
    const token = session?.access_token || tokenRecord(SESSION) || tokenRecord(REVOKE);
    if (token) saveToken(REVOKE, token);
    if (notify) {
      put(VERSION, b64(browser.crypto.getRandomValues(new Uint8Array(16))));
      put(LOGOUT, '1');
    }
    clear('explicit_logout'); if (notify) broadcast('logout'); browser.sessionStorage.removeItem(PENDING);
    try {await retryLogout(token);} catch {throw new Error('Signed out here. Server sign-out could not be confirmed. Reconnect to finish signing out.');}
  }
  const recheck = () => {if (!bootstrapping && browser.document?.visibilityState !== 'hidden') restore().catch(() => {});};
  const remoteLogout = () => {
    // A delayed notification must not end a deliberate sign-in after that logout.
    if (session && session.logoutVersion === (get(VERSION) || '') && !staleSession()) return;
    let pending;
    try {pending = JSON.parse(browser.sessionStorage.getItem(PENDING));} catch { /* no pending login */ }
    if (pending?.logoutVersion === (get(VERSION) || '')) return;
    if (session || tokenRecord(SESSION) || tokenRecord(REVOKE)) logout(false).catch(() => {});
    else {clear('explicit_logout'); browser.sessionStorage.removeItem(PENDING);}
  };
  const storageChanged = event => {if (event.key === VERSION && event.newValue) remoteLogout();};
  browser.addEventListener?.('pageshow', recheck);
  browser.addEventListener?.('online', recheck);
  browser.addEventListener?.('storage', storageChanged);
  browser.document?.addEventListener('visibilitychange', recheck);
  if (channel) channel.onmessage = event => {
    if (event.data?.type === 'logout') remoteLogout();
    else if (event.data?.type === 'activity') recheck();
  };
  function dispose() {
    browser.clearTimeout(timer); for (const request of active) request.abort(); active.clear(); channel?.close();
    browser.removeEventListener?.('pageshow', recheck); browser.removeEventListener?.('online', recheck);
    browser.removeEventListener?.('storage', storageChanged); browser.document?.removeEventListener('visibilitychange', recheck);
  }
  return {login, finish, restore, activity, request, logout, dispose};
}
