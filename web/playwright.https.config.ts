import {defineConfig} from '@playwright/test';
export default defineConfig({
  testDir:'./tests/https', testMatch:'*.spec.ts', workers:1, fullyParallel:false, timeout:30000,
  outputDir:'../outputs/session-https-browser', reporter:'list',
  use:{baseURL:'https://datacube-bi-analyst.netlify.app:4473', ignoreHTTPSErrors:true, headless:true,
    launchOptions:{args:['--host-resolver-rules=MAP datacube-bi-analyst.netlify.app 127.0.0.1, MAP datacube-bi-analyst.onrender.com 127.0.0.1, MAP * ~NOTFOUND','--no-proxy-server','--test-third-party-cookie-phaseout']},
    trace:'retain-on-failure'},
});
