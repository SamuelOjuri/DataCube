// Production assets + SPA fallback + the deployed script/style policy. Synthetic API is intercepted by tests.
import {createServer} from 'node:http';
import {readFile} from 'node:fs/promises';
import {resolve, extname, sep} from 'node:path';
const dist = resolve('dist');
const policy = (await readFile(resolve(dist, '_headers'), 'utf8')).split('Content-Security-Policy: ')[1].trim();
createServer(async (req, res) => {
  let path = resolve(dist, '.' + new URL(req.url, 'http://localhost').pathname);
  if (path !== dist && !path.startsWith(dist + sep)) {res.writeHead(400).end(); return;}
  let body;
  try {body = await readFile(path);} catch {path = resolve(dist, 'index.html'); body = await readFile(path);}
  res.writeHead(200, {'Content-Type': {'.html':'text/html','.js':'text/javascript','.css':'text/css'}[extname(path)] || 'application/octet-stream',
    'Content-Security-Policy': policy,
    'Cache-Control':'no-store'}).end(body);
}).listen(4173,'127.0.0.1');
