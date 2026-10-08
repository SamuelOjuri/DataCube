import {test} from 'node:test';
import assert from 'node:assert/strict';
import {apiOrigin,securityHeaders} from '../build-policy.mjs';
test('only explicit API origin is public and preview contexts fail closed',()=>{
  assert.equal(apiOrigin({VITE_API_ORIGIN:'https://api.example.test'}),'https://api.example.test');
  assert.throws(()=>apiOrigin({VITE_API_ORIGIN:'https://api.example.test',VITE_SUPABASE_KEY:'secret'}),/Only/);
  assert.throws(()=>apiOrigin({VITE_API_ORIGIN:'https://api.example.test/path'}),/HTTPS origin/);
  assert.throws(()=>apiOrigin({VITE_API_ORIGIN:'https://prod.test',NETLIFY:'true',CONTEXT:'deploy-preview'}),/Preview/);
  assert.equal(apiOrigin({VITE_API_ORIGIN:'https://stage.test',NETLIFY:'true',CONTEXT:'deploy-preview',STAGING_API_ORIGIN:'https://stage.test'}),'https://stage.test');
  const headers=securityHeaders('https://stage.test');assert.match(headers,/connect-src 'self' https:\/\/stage.test;/);assert.doesNotMatch(headers,/unsafe-eval|unsafe-inline/);
});
