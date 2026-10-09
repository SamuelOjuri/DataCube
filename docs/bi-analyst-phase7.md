# Phase 7: controlled-release deployment code

Implemented on 9 October 2026 in API package **0.7.0**. Frontend 0.6.0 retains
the v1 contract. This delivers deployment and qualification tooling; it does
not assert that hosted Phase 6 acceptance, production promotion or the business
pilot has happened. No hosted migrations, deployments, identity grants or
Monday changes were performed.

## Next step after the TEST migrations: deploy the API

The first staging deployment needs **two connection strings and the TEST SSL
certificate**. The staging Blueprint generates its own telemetry token and runs
the read-only preflight automatically before starting the API. Frontend, Monday,
Gemini and pilot-user settings are deferred until those features are enabled.

1. Commit and push the updated `services/bi_analyst/render-staging.yaml` to the
   deployment branch. Run the BI Analyst CI workflow on that commit. Local edits
   do not change Render's setup form until that branch contains them.
2. If still on the unsubmitted Blueprint form, go back and open **New > Blueprint**
   again, select that branch, and set **Blueprint Path** to
   `services/bi_analyst/render-staging.yaml`. It now asks only for
   `BI_ANALYST_READ_DSN` and `BI_ANALYST_STATE_DSN`. If a service already exists,
   sync its existing Blueprint instead of creating another service.
3. Enter the two TEST connections using the role passwords already provisioned.
   Replace `PROJECT_REF`, `POOLER_HOST` and the password placeholders below with
   the TEST values. Copy the actual session-pooler host from Supabase's **Connect**
   panel. Percent-encode reserved characters in passwords, such as `@` as `%40`.
   Paste each entire URI into its Render Value field, without surrounding quotes.

   `BI_ANALYST_READ_DSN`:

   ```text
   postgresql://bi_analyst_reader.PROJECT_REF:READER_PASSWORD_URL_ENCODED@POOLER_HOST:5432/postgres?sslmode=verify-full&sslrootcert=/etc/secrets/supabase-test.cer
   ```

   `BI_ANALYST_STATE_DSN`:

   ```text
   postgresql://bi_analyst_state.PROJECT_REF:STATE_PASSWORD_URL_ENCODED@POOLER_HOST:5432/postgres?sslmode=verify-full&sslrootcert=/etc/secrets/supabase-test.cer
   ```

4. Create the service, then open **Environment > Secret Files > Add Secret File**.
   Name it `supabase-test.cer` and paste the full TEST certificate contents,
   including the certificate markers. Render exposes it at
   `/etc/secrets/supabase-test.cer`; a Windows Downloads path will not work there.
   If an initial deployment starts before the certificate is present, let the
   deployment triggered by saving the file complete, or manually redeploy.
5. Check the deployment logs. Preflight should print `"passed": true` and
   `"errors": []`, then Uvicorn starts. A failed preflight stops startup and
   reports fixed error codes without exposing credentials. It does not run
   migrations, grant permissions or contact Monday/Gemini.
6. Open `https://YOUR-STAGING-API.onrender.com/health/ready`, using the actual URL
   assigned by Render. Expect these fields (additional metadata is normal):

   ```json
   {
     "status": "ready",
     "environment": "staging",
     "api_version": "0.7.0",
     "schema_version": 7,
     "identity": "deferred",
     "metric_execution": "disabled",
     "workflow": "disabled",
     "pilot_only": true
   }
   ```

This completes the initial API/database deployment check. No local package
installation, Render shell, manual telemetry-token generation, new database
roles or repeated migrations are needed for this step. Keep the existing one
instance/eight-connection allocation within TEST's available database budget.

Render generates `BI_ANALYST_TELEMETRY_TOKEN` only when it does not already exist;
retrieve it from the service's Environment settings when configuring monitoring.
Blueprint sync also preserves previously configured variables omitted from the
file. On an existing service, remove any dummy deferred values (especially invalid
pilot UUIDs or CORS URLs) before redeploying; preserve real configuration.

Next, configure the staging frontend and Monday sign-in using the
[authentication runbook](bi-analyst-auth.md), then Gemini and the nominated pilot.
Add the deferred settings through Render's Environment page. Update the staging
Blueprint's authentication/analyst/workflow flags when activating those features,
so a later Blueprint sync does not restore their disabled defaults. Full pilot
qualification and production promotion still follow the sequence below.

Render documents the [Blueprint creation and sync flow](https://render.com/docs/infrastructure-as-code),
[generated secrets](https://render.com/docs/blueprint-spec#generating-random-secrets)
and [secret files](https://render.com/docs/configure-environment-variables#secret-files).

## Assessment of the plan

The architecture and promotion order are appropriate. Existing restricted roles,
deterministic metric queries, owner-scoped checkpoints and browser tests provide
a useful foundation. Four delivery details needed explicit treatment:

- Phase 6 code completion does not establish real Monday/Netlify/Render acceptance.
  The release gate requires that evidence separately from local synthetic tests.
- `deploy/release-policy.toml` records the performance/cost targets and their
  approval reference. Qualification verifies that recorded approval alongside
  actual measurements; accepted targets alone do not establish hosted performance.
- A rolling replacement temporarily has old and new database pools. Deployment
  settings reserve eight connections for one instance with two read and two state
  connections per process. This allocation must be confirmed against the actual
  database/ETL budget before deployment. It is not a measured sizing recommendation.
- Source ingestion, rollup, materialised refresh and snapshot timestamps remain
  unknown to the analyst. Monitoring explicitly exposes four unknown signals.
  The existing coverage counters remain limitations, not an active-only population
  gate. Nothing changes the owner's accepted retained-history definitions.

## Delivered components

| Location | Purpose |
|---|---|
| `.github/workflows/bi-analyst.yml` | Isolated PostgreSQL metric/access/recovery tests, frontend unit/browser/accessibility checks, wheel and candidate evidence artifacts |
| `services/bi_analyst/render.yaml` | Dedicated production public API; one process, bounded shutdown, readiness health check, deployment disabled until configured |
| `services/bi_analyst/render-staging.yaml` | Separate staging API and secrets, with the same process/connection boundary |
| `services/bi_analyst/deploy/render-maintenance.yaml` | Optional independent hourly retention job; requires its own restricted credential |
| `services/bi_analyst/bi_analyst/operations/` | Read-only preflight, release fingerprint/evidence gate, bounded staging load, retention and pilot summary commands |
| `services/bi_analyst/deploy/prometheus.yml`, `alerts.yml`, `dashboard.json` | Authenticated external collection, operational alerts and importable Grafana dashboard |
| `src/database/migrations/20261009_008_analyst_operations.sql` | Additive maintenance role/policies, preserving schema 7 and existing source/runtime grants |

The Render API remains a **public authenticated web service** so a browser served
by Netlify can reach it. The configuration follows Render's
[FastAPI deployment](https://render.com/docs/deploy-fastapi),
[Blueprint specification](https://render.com/docs/blueprint-spec) and
[readiness health checks](https://render.com/docs/health-checks).
Automatic deployment is off; a successful CI run produces a candidate, not a
production release. Existing ETL services are not imported into these Blueprints.

## Release controls and compatibility

`BI_ANALYST_ANALYST_ENABLED=false` rejects authenticated analytical access while
keeping health checks and authentication infrastructure reachable. It also fences
workflow state reads/writes. `BI_ANALYST_WORKFLOW_ENABLED=false` disables new
workflow submission/resume. Redeployment drains local execution; interrupted jobs
remain durable and the existing expiry/recovery path applies on reconnect.

`BI_ANALYST_PILOT_ONLY=true` requires the subject UUID to appear in the comma-separated
`BI_ANALYST_PILOT_SUBJECTS`, **in addition** to its enabled company-wide principal,
current permissions version, approved Monday account/user mapping and valid session.
An empty pilot list admits nobody. Both Blueprints start with analyst/workflow/auth
disabled and pilot restriction enabled. Provision only nominated pilot principals,
using the existing [authentication runbook](bi-analyst-auth.md).

`BI_ANALYST_DISABLED_METRICS` is a comma-separated list of catalogue metric IDs.
Unknown IDs fail configuration validation. Disabled entries cannot be executed
through either the direct compiler endpoint or the graph, and are labelled disabled
in the catalogue response. Previously persisted results retain their original
provenance and existing ownership controls; this flag prevents new execution.

`GET /health/ready` returns v1 contract, API, schema and catalogue versions/hash,
environment and feature availability, without secrets or pilot identities.
Infrastructure readiness remains distinct from enabled features. Migration 008
does not advance schema 7 or change the v1 result/stream contract.

## Promotion sequence

1. Run CI on the exact candidate. Preserve the wheel, manifest and test artifacts.
   Configure this workflow as a required branch check in repository settings.
   It never executes root integration scripts that might sync Monday.
2. In dedicated TEST, inventory already-applied migrations and effective grants.
   Apply only missing compatible changes, in reviewed administrator transactions:
   001, 003, 004, 005, 006, 007, then 008. Migration 002 is optional archive diagnostics
   and is not required for retained-project reporting. Do not rerun role-creating
   bootstraps. Runtime/build/start commands never apply migrations.
3. Provision reader/state LOGIN credentials and verified TLS trust as documented
   in Phase 3. Provision the maintenance role separately only if enabling retention.
   No migrator/administrator/maintenance credential belongs in the API service.
4. Follow the initial API deployment steps above. Import the **staging** Blueprint,
   supply only the dedicated TEST DSN pair, and upload the TEST SSL certificate.
   Render generates the telemetry token and staging startup runs preflight.
   Confirm the reserved eight-connection budget and select the initial instance
   plan. Record its CPU/RAM and database region with subsequent load evidence.
5. Confirm automatic `bi-analyst-preflight` succeeds and `/health/ready` reports
   schema 7. It opens restricted connections, checks actual privileges/schema/
   operations-role presence, and performs no grants or writes. Then configure the
   staging Monday OAuth app, exact frontend CORS/callback URLs, Gemini key and
   provisioned pilot subjects. Configure reviewed model input/output USD per
   million token prices together; unset prices mean unknown cost. Enable Monday
   authentication and analyst/workflow flags, then run `bi-analyst-preflight --pilot`.
   Failed checks emit fixed codes, not DSNs.
6. Deploy Netlify using the existing root `netlify.toml`: build `web/`, publish
   `dist`, exact API-origin CSP and SPA fallback. Set `VITE_API_ORIGIN` to staging;
   nonproduction contexts also require the same `STAGING_API_ORIGIN`. Rebuild on
   origin changes. Keep production and arbitrary preview origins separate, following
   [Netlify's configuration contexts](https://docs.netlify.com/build/configure-builds/file-based-configuration/).
7. Exercise real sign-in, expiry/reauthentication, direct links, follow-ups,
   cross-user denial, chart/table/CSV agreement, stream heartbeats/replay and sign-out.
   Disconnect and reconnect across a Render deployment; verify persisted terminal
   outcomes and explicit interruption. Test cancellation and model/database outage
   recovery in staging. Record independent frontend/API rollback evidence.
8. Run representative staging load, review operational/pilot evidence and pass the
   offline release gate below. Repeat database → Render API → Netlify → restricted
   pilot promotion in production using **production-specific** credentials, origins
   and identity mappings. Keep the reviewed release and rollback artifacts together.

The supplied configuration has one worker and one instance. Before scaling, measure
latency, pool pressure, failures, CPU/memory and model usage. Increase the reserved
connection budget and `REPLICA_COUNT` together, accounting for `DEPLOYMENT_OVERLAP=2`.
Process-local metrics must be collected per instance; do not scrape a load-balanced
multi-instance public hostname and assume its counters describe every process.

## Qualification commands and evidence

Install the locked analyst package in its isolated environment. Commands below
assume its console scripts are on PATH and run from the repository root.

```powershell
bi-analyst-release manifest --root . --output outputs/phase7/manifest.json
bi-analyst-release template --root . --output outputs/phase7/evidence.json
# Session tokens: set BI_ANALYST_LOAD_TOKENS securely to a JSON array of staging
# pilot sessions. Do not put tokens in commands, files, URLs or committed output.
bi-analyst-qualify --origin https://STAGING_API.onrender.com --execute --mode query --samples 50 --concurrency 2 --output outputs/phase7/query-load.json
bi-analyst-qualify --origin https://STAGING_API.onrender.com --execute --mode answer --samples 50 --concurrency 2 --output outputs/phase7/answer-load.json
bi-analyst-release check --root . --policy services/bi_analyst/deploy/release-policy.toml --evidence outputs/phase7/evidence.json
```

The load command first checks an unauthenticated readiness response and refuses
production, unaccepted metrics, disabled identity/features or non-pilot staging.
It cycles through the 16 catalogue variants using bounded authenticated concurrent
requests. It creates analyst conversations/results and incurs model usage in answer
mode; `--execute` is required. Tokens are sent only to the fixed HTTPS origin,
redirects/environment proxies are disabled, and output contains only aggregate
timings/outcomes and deployment versions. Session expiry and 429 responses count
as failures. Use multiple pilot sessions or explicitly reviewed staging rate limits
to measure intended load; do not silently remove production protections.

Query timing includes conversation/run creation and response retrieval. Answer
timing includes workflow completion polling. Success p95 and the separate failure
rate are both reported; failed samples are not silently treated as fast successes.
Clarification is counted separately from completion. The command exits 1 on any
failed sample so CI/operator review cannot silently overlook it, even if a reviewed
release failure-rate target permits a small rate.

Fill the template with the reviewer, deployment and reference-dataset versions, all core metric IDs,
and each required check's actual artifact path, SHA-256 and `passed` status. Include
live model evaluation, precision/parity, permission tests, hosted integration,
recovery, rollbacks, monitoring, retention, freshness/coverage review and business
pilot acceptance. Set the load summary from measured query/answer results, the lower
sample/concurrency count across both runs, the worse failure rate, and cost per
completed answer from collector/provider usage. Review token prices, thinking-token
billing and any provider costs not represented by token counts.

The gate rejects missing/future/old evidence, changed artifacts, changed source or
lockfiles, missing enabled metrics, unapproved targets and exceeded budgets. It is
a file-backed review attestation, not an automated substitute for observing staging
or obtaining business sign-off. The template intentionally fails qualification.
Re-run model evaluation when model/prompt changes; the fingerprint also covers
compiler, catalogue, SQL, frontend, tests and deployment inputs.

## Monitoring and retention

Import `deploy/dashboard.json` into Grafana and configure the supplied Prometheus
scrape and alert files on the existing **independent** monitoring host. Use its
existing Alertmanager receiver; no notification is sent by this implementation.
Store the telemetry bearer token in the collector's protected credentials file.
The syntax follows [Prometheus scrape configuration](https://prometheus.io/docs/prometheus/latest/configuration/configuration/)
and [alerting rules](https://prometheus.io/docs/prometheus/latest/configuration/alerting_rules/).
Run `promtool check rules alerts.yml` and `promtool check config prometheus.yml`
on that host after replacing the target and installing its credentials file.

`/ops/metrics` returns 404 for missing/wrong tokens. Series cover query/model/workflow
failure rates, bounded latency histograms, active work, pool utilisation/waiters,
token counts (including thinking), estimated cost, coverage and unknown source
freshness. HTTP latency ends at response headers; workflow latency covers each
execution segment, including clarification/resume segments. Neither is labelled
as full browser answer latency. Prices are operator-configured estimates and
unknown costs are absent, not zero. All alert thresholds are proposed pilot defaults.

Counters reset on process replacement; use `rate`/`increase`. The external `up`
alert detects a dead process; local code cannot emit a shutdown event after SIGKILL.
`bi-analyst-maintain --report` provides redacted status/feedback counts and overdue
durable runs, including those left by a hard kill. Alert externally on failed or
missing scheduler runs and overdue jobs. Test receiver delivery in staging.

JSON application events contain only fixed stage/outcome codes, durations, request
IDs and route templates. Subject IDs remain in restricted database audit records,
not application traces. Questions, CRM cells, SQL, credentials, raw provider output
and exception text are excluded. LangSmith tracing remains explicitly disabled;
Uvicorn access logs are disabled to avoid OAuth query strings in request logs.

The proposed retention policy is **30 days since conversation activity**, **90 days
for database audit records**, and prompt deletion of expired sessions/OAuth attempts.
Apply it only after policy review. There is no stored server export copy: CSV is
generated from retained result rows. Downloaded client files and provider/platform
backups require their own retention policy and cannot be recalled by this code.
Configure collector/application-log retention (proposed 14 days) and database backup
expiry separately; the application does not control those hosted settings.

```powershell
# BI_ANALYST_MAINTENANCE_DSN uses the dedicated role, never an administrator DSN.
bi-analyst-maintain --conversation-days 30 --audit-days 90 --batch 100
# After reviewing the preview and policy, commit one bounded batch:
bi-analyst-maintain --apply --conversation-days 30 --audit-days 90 --batch 100
bi-analyst-maintain --report
```

Cleanup uses one transaction with a one-second lock timeout and 30-second statement
budget. It briefly locks analyst state against writes while selecting/deleting a
batch, preventing a concurrent resume/new run from racing deletion. Recent runs,
results, execution segments and live leases protect the conversation. Checkpoint
blobs/writes, feedback, events, jobs, results and runs are removed in dependency
order; audit has its separate cutoff. Source/business tables, principal grants and
identity mappings are never changed. A full batch requires another invocation;
oversized conversations may require smaller scheduled batches and operator review.
The dedicated role cannot INSERT/UPDATE or read business tables. Runtime roles
still cannot delete their state. Do not enable retention from API startup.

The `batch_full` flag covers conversation, audit and authentication batches. Monitor
it and increase batch size (maximum 1,000) or invocation frequency if a backlog
persists; the supplied hourly schedule and batch 100 are starting parameters,
not a guarantee that cleanup keeps pace with traffic. Repeat until the flag clears.

Review unsuccessful/low-confidence answers and clarification quality with nominated
users. Use owner-authorised history for detailed review; the report command exposes
counts only. Promote anonymised, reviewed examples into the isolated regression
suite and record the evaluation/dataset versions. Feedback counts alone are not
business acceptance.

## Rollback and incident procedure

1. For an API incident, disable workflow and/or analyst access in Render, then
   redeploy; readiness remains available. For a single metric, disable its ID.
   A credentials/access incident also requires principal disable/version increment
   and session revocation using the existing auth administration procedure.
2. Roll Netlify back to the last tested assets independently. Its built API origin
   must still match that environment. Retest sign-in and a direct conversation link.
3. Roll the API back only to a release verified against schema 7 and the v1 frontend.
   API 0.6.0 understands schema 7 but **does not implement the new pilot/kill switches**;
   keep only pilot database grants and disable Monday auth before using that older
   fallback. Prefer a tested 0.7.x rollback artifact that preserves these controls.
4. Do not reverse migrations by dropping conversations, checkpoints, results,
   feedback or audit data. Migration 008 is additive and can remain installed.
   Pause its independent scheduler separately if retention is implicated.
5. Reconnect and inspect durable run outcomes, health/version metadata, pool pressure
   and errors. Re-run qualification for the corrected candidate before re-enabling.

## Local evidence and remaining hosted work

Validation on 9 October 2026: **405 backend regression tests passed**, followed
by **23 passing focused operations checks** after the final backlog/fingerprint
refinements; **14 frontend unit tests** and **8 browser/accessibility scenarios**
passed. The API 0.7.0 wheel builds offline. Results, candidate manifest and the
intentionally pending hosted-evidence template are under `outputs/phase7/`;
browser artifacts are under `outputs/phase6-browser/`. `git diff --check` passes.
The existing large Vega bundle warning remains unchanged. Hosted Blueprint import,
Prometheus/Grafana loading and alert delivery have not been exercised here.

Local checks cover the isolated analyst regression suite and the unchanged frontend
unit/browser/accessibility suite. Tests apply 008 in disposable loopback databases,
check source ACL/data preservation, deny unauthorized maintenance operations,
exercise pilot/metric switches and telemetry protection, and verify retention preview,
commit, idempotence and live-run preservation. Evidence-gate tests reject stale or
altered files and unapproved budgets; the load harness refuses production before
sending session credentials.

Hosted services, provider settings, secrets, accepted sizing/targets, actual concurrent
load/cost, monitoring receiver delivery and business pilot evidence remain delivery
tasks. Phase 1 owner acceptance is preserved. Phase 7's release exit gate stays open
until those observations exist; synthetic/local results do not close it.
