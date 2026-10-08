// Build-only configuration. Nothing except the explicit API origin enters client assets.
export function apiOrigin(environment) {
  for (const key of Object.keys(environment)) {
    if (key.startsWith('VITE_') && key !== 'VITE_API_ORIGIN') throw new Error('Only VITE_API_ORIGIN is permitted in frontend configuration');
  }
  const value = environment.VITE_API_ORIGIN;
  let url;
  try {url = new URL(value);} catch {throw new Error('VITE_API_ORIGIN must be an explicit API origin');}
  if (url.origin !== value || !(url.protocol === 'https:' || (url.protocol === 'http:' && ['127.0.0.1','localhost'].includes(url.hostname)))) throw new Error('VITE_API_ORIGIN must be an HTTPS origin (HTTP is allowed only for local tests)');
  if (environment.NETLIFY === 'true') {
    if (url.protocol !== 'https:') throw new Error('Netlify builds require HTTPS');
    if (environment.CONTEXT !== 'production' && value !== environment.STAGING_API_ORIGIN) throw new Error('Preview builds require VITE_API_ORIGIN to match the explicitly configured STAGING_API_ORIGIN');
  }
  return value;
}
export function securityHeaders(origin) {
  return `/*\n  Content-Security-Policy: default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self' ${origin}; img-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'\n`;
}
