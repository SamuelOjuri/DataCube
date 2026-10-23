// Calendar buckets, never browser-local instants. Match the server's grain checks.
export const grains = ['day', 'week', 'month', 'quarter', 'year'];
export const iso = date => date.toISOString().slice(0, 10);
export function periodStart(date, grain) {
  const value = new Date(date);
  if (!grains.includes(grain)) return null;
  if (grain === 'week') value.setUTCDate(value.getUTCDate() - (value.getUTCDay() + 6) % 7);
  if (['month', 'quarter', 'year'].includes(grain)) value.setUTCDate(1);
  if (grain === 'quarter') value.setUTCMonth(Math.floor(value.getUTCMonth() / 3) * 3);
  if (grain === 'year') value.setUTCMonth(0);
  return value;
}
export function bucket(raw, grain) {
  if (typeof raw !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(raw)) return null;
  const date = new Date(raw + 'T00:00:00Z');
  return Number.isFinite(date.getTime()) && iso(date) === raw && grains.includes(grain)
    && iso(periodStart(date, grain)) === raw ? date : null;
}
export function coordinate(date, grain) {
  const day = Math.floor(date.getTime() / 86400000);
  return grain === 'day' ? day : grain === 'week' ? Math.floor(day / 7)
    : grain === 'month' ? date.getUTCFullYear() * 12 + date.getUTCMonth()
    : grain === 'quarter' ? date.getUTCFullYear() * 4 + Math.floor(date.getUTCMonth() / 3) : date.getUTCFullYear();
}
export function advance(date, grain, steps = 1) {
  const value = new Date(date);
  if (grain === 'day' || grain === 'week') value.setUTCDate(value.getUTCDate() + steps * (grain === 'week' ? 7 : 1));
  else value.setUTCMonth(value.getUTCMonth() + steps * (grain === 'quarter' ? 3 : grain === 'year' ? 12 : 1));
  return value;
}
export function bucketLabel(raw, grain) {
  const date = new Date(raw + 'T00:00:00Z');
  if (grain === 'year') return String(date.getUTCFullYear());
  if (grain === 'quarter') return `Q${Math.floor(date.getUTCMonth() / 3) + 1} ${date.getUTCFullYear()}`;
  const text = date.toLocaleDateString('en-GB', {timeZone: 'UTC', month: 'short', year: 'numeric',
    ...(grain === 'month' ? {} : {day: 'numeric'})});
  return grain === 'week' ? `Week of ${text}` : text;
}
export function samePriorYear(current, baseline, grain) {
  if (grain !== 'week') return current.getUTCFullYear() === baseline.getUTCFullYear() + 1
    && current.getUTCMonth() === baseline.getUTCMonth() && current.getUTCDate() === baseline.getUTCDate();
  const isoWeek = date => {
    const thursday = advance(date, 'day', 3);
    const year = thursday.getUTCFullYear(), jan4 = new Date(`${year}-01-04T00:00:00Z`);
    const firstMonday = periodStart(jan4, 'week');
    return [year, Math.round((date - firstMonday) / 604800000) + 1];
  };
  const [cy, cw] = isoWeek(current), [by, bw] = isoWeek(baseline);
  return cy === by + 1 && cw === bw;
}
