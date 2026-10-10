# Phase 6: analytical web workspace

Implemented on 8 October 2026 in frontend/API package **0.6.0**. This is a code
delivery, with local integration evidence. No hosted migration, Monday provider
configuration, Render deployment or Netlify publication was performed. The
hosted checks were originally planned for staging. The owner-approved
[Phase 7 single-deployment plan](bi-analyst-phase7.md) now uses one production API
and frontend with a restricted pilot, not separate staging sites. The historical
test results below do not establish hosted acceptance.

## Assessment and implementation decisions

The plan correctly keeps metric calculations and access control on the API and
uses owned results as the basis for charts and exports. Phase 5 supplies the
required graph, clarification and replay contracts. The existing `web/` shell
provided authentication only. Implementation exposed several details that the
plan needed to make explicit:

- The API had no project drill-down or feedback endpoint. Phase 6 adds bounded
  project evidence using the saved metric plan, and owner-scoped feedback.
- A saved aggregate is not a saved project snapshot. Drill-down is labelled as
  a new read of live evidence using the saved filters and resolved period.
  The original answer and CSV retain their persisted rows. Independent hidden
  inventory cannot be allocated to projects and has no project drill-down.
- CSV exports contain **all stored result rows**, including other table pages.
  If the result was limited, the export is limited too. A full-population export
  outside the original row/byte budget is not silently implied or re-queried.
- Vega's default expression runtime conflicts with the existing CSP. The
  renderer uses the [official expression interpreter](https://vega.github.io/vega/usage/interpreter/),
  application-generated specifications and inline owned datasets, with a loader
  that rejects external resources. No `unsafe-eval` or `unsafe-inline` is added.
- Cancellation must remain attached after fetch returns headers. The auth
  client now tracks response bodies and SSE readers through completion; session
  expiry and sign-out abort them and unmount all client-held analytical data.
- Reconnecting is a status read and stream replay, not a new model submission.
  Uncertain submissions retain their idempotency key; retries of failed or
  interrupted runs create a new key. Definitive client errors allow revision.

The owner-approved retained-project population and all existing metric formulas
are unchanged. Source freshness remains **unknown**; query timestamps are not
presented as ingestion or refresh evidence. Ratio and gestation summaries are
labelled aggregates, not additive totals. Currency is GBP, as stored, with VAT
inclusion unspecified.

## Frontend

`web/src/Workspace.tsx` provides paginated conversation history, direct
`/conversations/{uuid}` links, questions, authenticated clarification, explicit
follow-up context, streaming progress, cancellation and retry. The client loads
persisted outcomes after each stream closes, and fetches the referenced result.
History and results stay in React memory; tokens are never stored in browser
storage or URLs. Only a short-lived pending sign-in verifier and a validated
conversation return path are stored in session storage.

Fetch SSE handles split UTF-8/CRLF frames, public event IDs and heartbeat
comments. A 25-second inactivity deadline aborts a stalled stream. Reconnection
uses bounded backoff and the highest received event ID; the user can explicitly
reconnect after the retry budget. Each reconnect first rechecks the authenticated
saved workflow. An expired or denied session clears the workspace and requires
Monday sign-in; the restored conversation is then authorised again. Closing the
browser stream leaves server execution running, consistent with Phase 5.

Result cards show period, filters, units, retained population, complete filtered
aggregate, known-value counts, coverage, freshness, limited-display status,
scope changes and source/version details. Definitions are fetched only when
expanded and displayed only if the catalogue fingerprint and metric version
match the saved result. All CRM text is rendered as text.

[TanStack Table v8](https://tanstack.com/table/v8/docs/guide/sorting) handles
sorting and page navigation over the stored rows. Decimal strings are formatted
and compared without conversion to floating-point numbers. Chart coordinates
use finite bounded numbers; the table retains exact values. Application code
permits only bar/line charts on approved dimensions and the `value` field, checks
units and duplicate coordinates, sorts month axes, caps the display at 240 points
and eight series, and uses a zero baseline. Unsupported shapes, unknown values
or unsafe numeric ranges fall back to the table. Limited results also use tables;
Phase 5 currently imposes its stricter 100-row, single-breakdown chart rule.
No model-controlled Vega spec,
URL, expression, link, configuration or transform is accepted. See the
[Vega-Lite inline-data contract](https://vega.github.io/vega-lite/docs/data.html).

The UI includes labels, keyboard focus styles, a skip link, live progress,
semantic table headings/sort state, accessible scroll regions, responsive
layouts and text/table alternatives for charts. Chart libraries load on demand.
The Vega runtime remains a large separate bundle (approximately 517 KB before
gzip); it is not needed for sign-in or table-only answers.

### Tapered Plus visual theme

The interface follows [the Tapered Plus website](https://taperedplus.co.uk/):
deep red (`#931f1f`, the site's `hsl(0 65% 35%)` primary), darker red
(`#691616`) for hover states, charcoal text (`#262626`), white and light-grey
surfaces, rounded cards and restrained shadows. Shared CSS tokens in
[`web/src/style.css`](../web/src/style.css) cover sign-in, workspace navigation,
controls, tables, notices and keyboard focus. Warning and error styling retains
its semantic distinction, and muted text uses a darker grey for contrast.

Open Sans weights 400, 600 and 700 are bundled locally from
`@fontsource/open-sans`; no external font requests or CSP changes are needed.
The font's copyright notice and SIL Open Font License are shipped with the
frontend in [`open-sans-license.txt`](../web/public/open-sans-license.txt).
[`web/src/presentation.mjs`](../web/src/presentation.mjs) applies the same font,
primary red, neutral axes and a red-led eight-series palette to bar/line charts.
This is a presentation-only change: authentication, analytical results, source
scope and responsive layout behavior are unchanged. Build and redeploy the
frontend to publish the theme; no backend deployment or migration is required.

## Backend and migration

| Endpoint | Contract |
|---|---|
| `GET /v1/results/{id}/projects?limit=25&offset=0` | Owned saved plan and fingerprint; 1–100 source rows, offset at most 10,000; live-read timestamp and explicit scope limitation |
| `POST /v1/results/{id}/feedback` | `rating: helpful/not_helpful`, optional comment up to 2,000 characters; one updateable feedback row per owned result |
| `GET /v1/results/{id}/export` | Existing safe CSV endpoint; `X-Export-Scope: stored-result-rows`; all stored rows across client pages |

Project reads use existing restricted analytical gateways, the compiler's
shared filtered CTE, read-only transactions, existing concurrency and timeout
budgets, a response byte limit and a final permission recheck. Monthly enquiry
and bookings map their aggregated gateways to the existing positive-value,
date and bookings-stage contracts over reportable projects. Invoice detail
retains source child rows and can repeat a project; ratios expose source counts
and their denominators, not invented project-level rates. SQL and identifiers
are application-owned; request filter values remain parameters.

Apply `src/database/migrations/20261008_007_analyst_presentation.sql` after 006,
in one reviewed administrator transaction. It adds `result_feedback`, forced
RLS, owner/current-permission checks and restricted grants, with no changes to
source data, operational privileges or metric views. The migration and startup
share `permissions_presentation.sql`. Runtime refuses feedback ownership-update
privilege drift. The API package includes the new audit file.

Package 0.6.0 supports earlier schema versions as before; feedback requires
version 7. It continues to support workflow execution and cancellation on both
6 and 7. To roll back the frontend, redeploy the previous assets. After migration
007, keep a schema-7-compatible API build; older package 0.5.0 rejects that
schema. Disable workflow execution using its existing feature flag if needed.
Do not delete conversation, feedback, checkpoint or result tables for rollback.

## Environment matrix

The hostnames below are placeholders. Use local/CI fixtures for development and
one hosted production deployment for the restricted pilot. No staging Render or
Netlify site is required by the revised Phase 7 plan.

| Setting | Local synthetic tests | Single production deployment |
|---|---|---|
| Frontend | `http://127.0.0.1:4173` | `https://PRODUCTION_FRONTEND.netlify.app` |
| `VITE_API_ORIGIN` | `http://127.0.0.1:4174` (intercepted) | `https://PRODUCTION_API.onrender.com` |
| `BI_ANALYST_ENVIRONMENT` | `test` for backend fixtures | `production` |
| `BI_ANALYST_CORS_ORIGINS` | Exact loopback test origin | Exact production frontend origin |
| `BI_ANALYST_AUTH_FRONTEND_URL` | Synthetic `/auth/callback` | Exact production frontend `/auth/callback` |
| `BI_ANALYST_MONDAY_REDIRECT_URI` | Synthetic provider | Exact production API `/auth/callback` |
| Database | Disposable loopback fixtures | Explicitly approved production database |
| `STAGING_API_ORIGIN` | Unneeded | Unset; no deploy previews or branch deploys |

Only `VITE_API_ORIGIN` is public frontend configuration; builds reject other
`VITE_` variables. Monday credentials, read/state DSNs, service credentials and
Gemini keys belong on Render. There is no Supabase browser key. Changing the
Vite API origin requires rebuilding the assets. The build emits `dist/_headers`
with an exact API origin in `connect-src`, alongside the root Netlify SPA
fallback and other security headers. The standard build remains `web/` to
`dist`, following [Netlify's Vite setup](https://docs.netlify.com/build/frameworks/framework-setup-guides/vite/).

The existing nonproduction build guard remains unchanged. For the single-hosted
deployment, disable Netlify deploy previews and branch deploys rather than setting
`STAGING_API_ORIGIN` to the production API. The API has one fixed frontend callback,
and PKCE storage is origin-bound. Do not add preview URLs to production CORS.
Synthetic browser fixtures run only in local/CI tests and are not bundled in the
product.

## Validation and remaining hosted acceptance

Run frontend checks from `web/`:

```powershell
npm ci
npm test
npx playwright install chromium
npm run test:browser
# For a normal build, set the intended explicit origin first:
$env:VITE_API_ORIGIN='https://PRODUCTION_API.onrender.com'
npm run build
```

The browser suite builds real production assets, serves SPA fallback and CSP,
and intercepts only the synthetic API/provider origin. It exercises sign-in,
direct links, stream replay after disconnect, persisted result retrieval,
clarification idempotency, follow-ups, cancellation/new-run retry, sorting,
pagination, CSV download, project detail, definitions, feedback, expired-session
recovery and sign-out. Desktop/mobile axe scans and keyboard/overflow checks
run against the rendered interface. This is not a hosted Monday/Netlify/Render
test, and does not measure proxy buffering or deployment interruptions.

Run backend checks from the repository root, with a disposable local PostgreSQL
cluster available (never use TEST or production for this fixture suite):

```powershell
$env:BI_ANALYST_TEST_DSN='host=127.0.0.1 port=55439 dbname=postgres user=postgres'
& ./report.venv/Scripts/python.exe -m pytest services/bi_analyst/tests -q -p no:cacheprovider --basetemp=tmp/UNIQUE_TEST_DIRECTORY
```

The full regression run passed **380 tests** before final shared-filter and
presentation refinements. The final focused backend run passed **74 tests**,
including schema-7 workflow execution/cancellation and denial of feedback grant
drift. The frontend has **14 passing unit tests** and **8 browser scenarios**,
including desktop/mobile accessibility scans with no violations. A backend-only
secret sentinel and synthetic fixture identifiers are absent from built assets.
Unit tests
cover decimal precision, hostile chart intent, unknown values, series bounds,
split SSE frames, large-frame rejection, stream cancellation and build policy.

Before accepting the hosted workspace in the restricted production pilot,
record evidence for:

1. Reviewed migrations and restricted role startup, enabled Monday identity,
   provisioned pilot users and explicitly enabled workflow.
2. Real Monday sign-in/callback and a direct conversation SPA link, including
   expiry followed by sign-in and reauthorisation.
3. Allowed/disallowed CORS origins, built asset/secret review and preview
   isolation from production.
4. Stream heartbeat visibility through Render, disconnect/replay, cancel during
   a query and reconnect across an API deployment/interrupted run.
5. Chart/table/CSV scope consistency, project access, sign-out and pilot user
   accessibility checks.

Perform these controlled checks on the single deployment, with authorized pilot
users and planned maintenance for disruptive checks. Database outages and other
destructive failure injection stay in local/CI fixtures. This is not a claim that
the original staging-based full-release validator passed; follow the revised
[Phase 7 acceptance and rollback procedure](bi-analyst-phase7.md).
