import type {Auth, Workflow} from './types';
import {readEvents} from './sse.mjs';

export class ApiError extends Error {
  constructor(public status: number, public code: string) {super(code);}
}
export async function json<T>(auth: Auth, path: string, options: RequestInit & {query?: Record<string, number>} = {}): Promise<T> {
  const response = await auth.request(path, options);
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw new ApiError(response.status, body.detail || 'request_failed');
  }
  return response.json();
}
export const post = (body: unknown): RequestInit => ({method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body)});
export const activeRun = (status: string) => ['registered', 'running'].includes(status);
export function errorMessage(error: unknown): string {
  if (error instanceof ApiError) {
    if (error.status === 429) return 'The analyst is busy. Please wait a moment and retry.';
    if (error.status === 404) return 'This conversation or result is unavailable to your account.';
    if (error.code === 'workflow_disabled') return 'Conversations are not enabled on this API yet.';
    if (error.status === 409) return 'This conversation has changed. Reload it before continuing.';
    return 'The request could not be completed. Please retry.';
  }
  return error instanceof Error && /session|sign in/i.test(error.message) ? error.message : 'Connection lost. Please retry.';
}
function pause(ms: number, signal: AbortSignal) {
  return new Promise<void>((resolve, reject) => {
    const abort = () => {clearTimeout(timer); reject(new DOMException('Aborted', 'AbortError'));};
    const timer = setTimeout(() => {signal.removeEventListener('abort', abort); resolve();}, ms);
    signal.addEventListener('abort', abort, {once: true});
    if (signal.aborted) abort();
  });
}
// A stream is a notification channel. Always recover the authoritative saved outcome.
export async function watchRun(auth: Auth, id: string, signal: AbortSignal,
  progress: (message: string) => void, update: (run: Workflow) => void) {
  let after = 0;
  for (let attempt = 0; attempt < 6 && !signal.aborted; attempt++) {
    const state = await json<Workflow>(auth, `/v1/runs/${id}/workflow`, {signal});
    update(state);
    if (!activeRun(state.status)) return;
    const segment = new AbortController();
    const abort = () => segment.abort();
    signal.addEventListener('abort', abort, {once: true});
    let watchdog: ReturnType<typeof setTimeout>;
    const heartbeat = () => {clearTimeout(watchdog); watchdog = setTimeout(abort, 25000);};
    heartbeat();
    try {
      const response = await auth.request(`/v1/runs/${id}/events`, {signal: segment.signal, query: {after}});
      if (!response.ok) {await response.body?.cancel(); throw new ApiError(response.status, 'stream_failed');}
      await readEvents(response, (event: {id: number; kind: string; payload: Record<string, unknown>}) => {
        if (event.id > after) after = event.id;
        if (event.kind === 'progress') {
          const stages: Record<string, string> = {authorizing: 'Checking your access', interpreting: 'Understanding your question', retrieving_definitions: 'Checking definitions',
            planning: 'Preparing the query', resolving_entities: 'Checking filters', querying: 'Reading your data', validating_evidence: 'Checking the evidence',
            selecting_presentation: 'Preparing your answer', grounding_answer: 'Linking the answer to its evidence'};
          progress(stages[String(event.payload.stage)] || 'Analysing your question');
        }
        // Access changes must be rechecked through the authenticated status request.
      }, heartbeat);
    } catch (error) {
      if (signal.aborted) return;
      if (error instanceof ApiError && [401, 403].includes(error.status)) throw error;
      progress('Reconnecting to the saved run…');
    } finally {clearTimeout(watchdog!); signal.removeEventListener('abort', abort); segment.abort();}
    const saved = await json<Workflow>(auth, `/v1/runs/${id}/workflow`, {signal});
    update(saved);
    if (!activeRun(saved.status)) return;
    await pause(Math.min(500 * 2 ** attempt, 8000), signal);
  }
  if (!signal.aborted) throw new Error('Reconnect limit reached');
}
