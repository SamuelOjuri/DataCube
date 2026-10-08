import {defineConfig} from '@playwright/test';
export default defineConfig({
  testDir: './tests/browser', fullyParallel: false, workers: 1, timeout: 30000,
  use: {baseURL: 'http://127.0.0.1:4173', headless: true, trace: 'retain-on-failure'},
  outputDir: '../outputs/phase6-browser', reporter: 'list',
  webServer: {command: 'npm run build && node tests/serve.mjs', url: 'http://127.0.0.1:4173', timeout: 120000,
    env: {VITE_API_ORIGIN: 'http://127.0.0.1:4174', BI_ANALYST_GEMINI_API_KEY: 'phase6-backend-secret-sentinel'}, reuseExistingServer: false},
});
