// Fetch SSE supports bearer headers, split UTF-8/CRLF chunks and comment heartbeats.
export async function readEvents(response, emit, heartbeat = () => {}) {
  if (!response.headers.get('Content-Type')?.startsWith('text/event-stream') || !response.body) throw new Error('Invalid event stream');
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '', lines = [], size = 0;
  function line(value) {
    heartbeat();
    if (value.startsWith(':')) return;
    if (value !== '') {lines.push(value); size += value.length; if (size > 131072) throw new Error('Event too large'); return;}
    let id = 0, kind = 'message'; const data = [];
    for (const entry of lines) {
      const index = entry.indexOf(':');
      const key = index < 0 ? entry : entry.slice(0, index);
      const val = index < 0 ? '' : entry.slice(index + 1).replace(/^ /, '');
      if (key === 'id' && /^\d+$/.test(val)) id = Number(val);
      if (key === 'event') kind = val;
      if (key === 'data') data.push(val);
    }
    lines = []; size = 0;
    if (data.length && ['progress', 'answer', 'clarification', 'terminal'].includes(kind)) {
      if (id > 128) throw new Error('Invalid event ID');
      emit({id, kind, payload: JSON.parse(data.join('\n'))});
    }
  }
  try {
    while (true) {
      const {value, done} = await reader.read();
      if (done) break;
      heartbeat(); buffer += decoder.decode(value, {stream: true});
      if (buffer.length > 262144) throw new Error('Event buffer too large');
      let end;
      while ((end = buffer.indexOf('\n')) >= 0) {line(buffer.slice(0, end).replace(/\r$/, '')); buffer = buffer.slice(end + 1);}
    }
    // Incomplete frames are discarded; the persisted outcome is fetched by the caller.
  } finally {await reader.cancel().catch(() => {}); reader.releaseLock();}
}
