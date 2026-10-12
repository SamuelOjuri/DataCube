// Tab-local company text; no results, credentials, CSRF or provider tokens.
const PREFIX = 'datacube.draft.';
const TTL = 8 * 60 * 60 * 1000;
const UUID = '[0-9a-f-]{36}';
const key = (subject, conversation) => PREFIX + encodeURIComponent(subject) + '.' + (conversation || 'new');
export function clearDrafts(storage = sessionStorage, subject = null) {
  const prefix = subject ? PREFIX + encodeURIComponent(subject) + '.' : PREFIX;
  try {for (let i = storage.length - 1; i >= 0; i--) {const name = storage.key(i); if (name?.startsWith(prefix)) storage.removeItem(name);}} catch { /* unavailable */ }
}
export function isolateDrafts(subject, storage = sessionStorage) {
  const prefix = PREFIX + encodeURIComponent(subject) + '.';
  try {for (let i = storage.length - 1; i >= 0; i--) {const name = storage.key(i); if (name?.startsWith(PREFIX) && !name.startsWith(prefix)) storage.removeItem(name);}} catch { /* unavailable */ }
}
function valid(value) {
  if (!value || typeof value.question !== 'string' || value.question.length > 8000 || typeof value.reply !== 'string' || value.reply.length > 2000) return false;
  if (!value.pending) return true;
  const r = value.pending.request, body = r?.body;
  return !!r && typeof r.question === 'string' && r.question.length <= 8000 &&
    (new RegExp(`^/v1/conversations/${UUID}/messages$`).test(r.path) || new RegExp(`^/v1/runs/${UUID}/resume$`).test(r.path)) &&
    !!body && typeof body.idempotency_key === 'string' && new RegExp(`^${UUID}$`).test(body.idempotency_key) &&
    Object.keys(body).every(k => ['question', 'answer', 'idempotency_key', 'clarification_id', 'follow_up_to'].includes(k)) &&
    Object.values(body).every(v => typeof v === 'string' && v.length <= 8000);
}
export function saveDraft(subject, conversation, value, storage = sessionStorage) {
  if (!valid(value)) return;
  const raw = JSON.stringify({question:value.question, reply:value.reply,
    pending:value.pending ? {status:'unconfirmed',request:{path:value.pending.request.path,body:value.pending.request.body,question:value.pending.request.question}} : null,
    savedAt:Date.now()});
  try {
    if (!value.question && !value.reply && !value.pending) storage.removeItem(key(subject, conversation));
    else if (raw.length <= 24000) {
      // Bound total tab storage too, including old conversations.
      const names = [];
      for (let i=0; i<storage.length; i++) if (storage.key(i)?.startsWith(PREFIX)) names.push(storage.key(i));
      if (names.length >= 20 && !names.includes(key(subject, conversation))) storage.removeItem(names[0]);
      storage.setItem(key(subject, conversation), raw);
    }
  } catch { /* Storage may be disabled or full; in-memory editing still works. */ }
}
export function readDraft(subject, conversation, storage = sessionStorage) {
  const name = key(subject, conversation);
  try {
    const raw = storage.getItem(name), value = raw && raw.length <= 24000 && JSON.parse(raw);
    if (valid(value) && Number.isFinite(value.savedAt) && Date.now() - value.savedAt >= 0 && Date.now() - value.savedAt < TTL) {
      return {...value, pending:value.pending ? {...value.pending, status:'unconfirmed'} : null};
    }
    storage.removeItem(name);
  } catch {try {storage.removeItem(name);} catch { /* unavailable */ }}
  return {question:'', reply:'', pending:null};
}
