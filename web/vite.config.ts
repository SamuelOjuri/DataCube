import {defineConfig, loadEnv} from 'vite';
import {apiOrigin, securityHeaders} from './build-policy.mjs';
export default defineConfig(({mode}) => {
  const environment = {...loadEnv(mode, process.cwd(), 'VITE_'), ...process.env};
  const origin = apiOrigin(environment);
  return {plugins: [{name:'explicit-api-security-policy', generateBundle() {
    this.emitFile({type:'asset', fileName:'_headers', source:securityHeaders(origin)});
  }}]};
});
