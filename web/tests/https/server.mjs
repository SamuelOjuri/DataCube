import {createServer} from 'node:https';
import {readFile} from 'node:fs/promises';
import {resolve, extname, sep} from 'node:path';
import {randomBytes} from 'node:crypto';

export const FRONT = 'https://datacube-bi-analyst.netlify.app:4473', API = 'https://datacube-bi-analyst.onrender.com:4474';
export async function start() {
  const tls = {key:await readFile('tests/https/key.pem'),cert:await readFile('tests/https/cert.pem')};
  const sessions = new Map(), calls = [];
  const state = {idle:1800000, absolute:28800000, subject:'https-user-one', offline:false};
  const dist = resolve('dist'), policy = (await readFile(resolve(dist,'_headers'),'utf8')).split('Content-Security-Policy: ')[1].trim();
  const frontend = createServer(tls, async(req,res) => {
    let path = resolve(dist, '.' + new URL(req.url,FRONT).pathname);
    if (path !== dist && !path.startsWith(dist+sep)) {res.writeHead(400).end();return;}
    let body; try {body=await readFile(path);} catch {path=resolve(dist,'index.html');body=await readFile(path);}
    res.writeHead(200,{'Content-Type':{'.html':'text/html','.js':'text/javascript','.css':'text/css'}[extname(path)] || 'application/octet-stream','Content-Security-Policy':policy,'Cache-Control':'no-store'}).end(body);
  });
  const api = createServer(tls, async(req,res) => {
    const path=new URL(req.url,API).pathname;
    calls.push({path, method:req.method, origin:req.headers.origin, cookie:req.headers.cookie, authorization:req.headers.authorization});
    if (state.offline) {req.socket.destroy();return;}
    const headers = {'Access-Control-Allow-Origin':FRONT,
      'Access-Control-Allow-Methods':'GET,POST','Access-Control-Allow-Headers':'content-type,authorization','Cache-Control':'no-store','Content-Type':'application/json'};
    const reply = (body,status=200,extra={}) => res.writeHead(status,{...headers,...extra}).end(JSON.stringify(body));
    if (req.method==='OPTIONS') {reply({});return;}
    if (path==='/auth/login') {res.writeHead(303,{Location:FRONT+'/auth/callback#code='+'C'.repeat(43)}).end();return;}
    let body=''; for await(const chunk of req) body+=chunk;
    const token=/^Bearer ([A-Za-z0-9_-]{43})$/i.exec(req.headers.authorization || '')?.[1];
    const session=sessions.get(token);
    if (path==='/auth/logout') {
      if (!token) {reply({detail:'authentication_required'},401);return;}
      sessions.delete(token); reply({});return;
    }
    const meta = s => ({subject:s.subject,permissions_version:1,absolute_expires_at:new Date(s.absolute).toISOString(),idle_expires_at:new Date(s.idle).toISOString(),
      absolute_expires_in_ms:Math.max(0,s.absolute-Date.now()),idle_expires_in_ms:Math.max(0,s.idle-Date.now()),idle_timeout_seconds:1800,warning_seconds:120});
    if (path==='/auth/exchange') {
      if (req.headers.origin!==FRONT) {reply({detail:'origin_denied'},403);return;}
      if (!JSON.parse(body).verifier) {reply({},400);return;}
      const credential=randomBytes(32).toString('base64url'), now=Date.now();
      const created={token:credential,subject:state.subject,absolute:now+state.absolute,idle:now+state.idle};
      sessions.set(credential,created); reply({...meta(created),access_token:credential,token_type:'Bearer',expires_in:28800,expires_at:new Date(created.absolute).toISOString()});return;
    }
    if (!session) {reply({detail:'authentication_required'},401);return;}
    if (Math.min(session.idle,session.absolute)<=Date.now()) {reply({detail:session.idle<session.absolute?'session_idle_expired':'session_absolute_expired'},401);return;}
    if (path==='/auth/activity') {session.idle=Date.now()+1800000;reply(meta(session));return;}
    if (path==='/auth/session') {reply(meta(session));return;}
    if (path==='/v1/conversations') {reply([]);return;}
    if (path==='/v1/metrics') {reply({metrics:[]});return;}
    reply({detail:'not_found'},404);
  });
  await Promise.all([new Promise(r=>frontend.listen(4473,'127.0.0.1',r)),new Promise(r=>api.listen(4474,'127.0.0.1',r))]);
  return {state,calls,sessions,reset(){sessions.clear();calls.length=0;Object.assign(state,{idle:1800000,absolute:28800000,subject:'https-user-one',offline:false});},
    async close(){frontend.closeAllConnections();api.closeAllConnections();await Promise.all([new Promise(r=>frontend.close(r)),new Promise(r=>api.close(r))]);}};
}
