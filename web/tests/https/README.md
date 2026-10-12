Synthetic HTTPS transport fixture. The certificate/private key in this directory
are public test data, trusted only by Playwright's isolated ignoreHTTPSErrors context.
They must never be deployed. Chromium maps the existing Netlify/Render hostnames to
loopback on test ports 4473/4474 and blocks resolution of other hosts. No production
service is contacted. These hosts are different browser sites; API calls use explicit
Authorization headers with cookies omitted. Third-party-cookie phaseout is enabled.
The API fixture enforces the exchange Origin and bearer authentication over real requests;
restricted PostgreSQL and OAuth logic are covered by the Python integration suite.
