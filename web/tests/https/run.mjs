import {spawnSync} from 'node:child_process';
import {resolve} from 'node:path';
const build = spawnSync(process.execPath, [process.env.npm_execpath, 'run', 'build'], {
  stdio:'inherit', env:{...process.env,VITE_API_ORIGIN:'https://datacube-bi-analyst.onrender.com:4474'},
});
if (build.status !== 0) process.exit(build.status || 1);
const tests = spawnSync(process.execPath, [resolve('node_modules/@playwright/test/cli.js'), 'test', '--config=playwright.https.config.ts'], {stdio:'inherit'});
process.exit(tests.status || 0);
