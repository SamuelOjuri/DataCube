# Phase 7: fresh production deployment, restricted pilot

Revised with the owner's approval on 9 October 2026 to reduce cost and setup work.
The owner subsequently reported deleting the staging Render and Netlify instances
and requested new, production-named services. This replaces both the original
two-environment plan and the later proposal to repurpose staging. API **0.7.0**,
frontend **0.6.0**, database schema **7** and the v1 contract remain unchanged.

**Create one new production Render API and one new production Netlify frontend.
Do not recreate staging sites or overwrite unrelated existing production sites.**
Start with approved pilot users on that deployment, then expand access on the
same deployment. There is no second application stack to build and pay for.

This is a documentation change, not a production cutover. It does not change
hosted settings, migrate databases, grant access, delete services or relax the
application's security checks.

## Keep the infrastructure small

| Component | Single-deployment plan |
|---|---|
| Render | Create one production Python web service, preferably named `datacube-bi-analyst`; one instance and one worker. |
| Netlify | Create one production project, preferably named `datacube-bi-analyst`; leave unrelated existing projects untouched. |
| Supabase | Use the explicitly approved production database with the two restricted analyst runtime roles. Do not create another database for this plan. |
| Monday | One dedicated identity-only app, separate from ETL, for the final API/frontend URLs. |
| Gemini | One backend-only key for the analytical workflow; a small pilot with reviewed usage and cost. |
| Tests | Existing local/CI tests with disposable PostgreSQL and synthetic browser fixtures, not another permanently hosted environment. |

Defer dedicated Prometheus/Grafana/Alertmanager hosting, the optional Render
retention cron job, additional replicas and optional live Monday source reads.
Existing platform logs, available platform notifications, provider usage reports
and an assigned operator are the initial operational tools. They are not equivalent
to continuous independent monitoring.

Two database roles are not two database servers. The eight-connection reservation
is a database capacity allowance, not eight paid service instances. This plan
removes duplicate application hosting; it does not eliminate existing platform
charges, database consumption or Gemini usage. Check those bills before choosing
a spending limit; no free-tier suitability or monthly price is assumed here.

## Starting point and names

The earlier TEST API passed preflight/readiness before deletion. That is useful
diagnostic history, not proof that the intended production database is ready.
The owner reports both staging application instances deleted; no replacement
production deployment has been verified by this documentation update.

Before proceeding, confirm the old staging Blueprint is disconnected so it cannot
recreate the deleted service. Do not import either legacy Blueprint for this
dashboard-managed route. Creating a Render **Web Service** directly avoids
Blueprint sync overwriting later dashboard settings.

Use `datacube-bi-analyst` as the requested name on each platform, if available.
Record the actual HTTPS URLs assigned after creation:

- `API_ORIGIN`: the new Render API origin, with no trailing slash or path.
- `FRONTEND_ORIGIN`: the new Netlify site's stable production origin.

These are placeholders in this guide, not additional environment variables.
Do not use the deleted staging URLs. A custom domain is optional, not required.

## Deployment sequence

### 1. Prepare the approved production database

- Identify the intended production Supabase project. Do not create another
  database or change existing ETL services as part of this application setup.
- Review production's migration inventory, effective grants, connection capacity
  and backup/restore arrangements. Apply only missing compatible migrations in
  reviewed administrator transactions: 001, 003, 004, 005, 006, 007 and 008.
  Migration 002 is optional archive diagnostics. Never rerun role bootstraps
  blindly or assume the TEST schema proves the production schema.
- Production needs its own reader/state passwords and verified TLS configuration.
  Do not simply label a TEST connection `production`. The existing TEST work is
  useful verification, but production credentials and schema review cannot be
  replaced by reusing its secrets.

See the [database deployment and role guidance](bi-analyst-phase3.md). Keep the
existing ETL services and credentials separate from the analyst.

In the selected production project's SQL Editor, start with read-only checks:

```sql
SELECT to_regclass('analyst_state.schema_version') AS version_table;
SELECT rolname, rolcanlogin
FROM pg_roles
WHERE rolname IN ('bi_analyst_reader', 'bi_analyst_state', 'bi_analyst_maintenance')
ORDER BY rolname;
```

If the version table exists, run
`SELECT version FROM analyst_state.schema_version;`. Expect schema 7, reader/state
LOGIN enabled, and the maintenance role present. Version 7 alone does not prove
all grants and reporting relations are correct; startup preflight checks those.
If required objects are absent, stop and review missing migrations before paying
for a service that cannot start.

Provision passwords only if needed. In an administrator `psql` session explicitly
connected to production, use `\password bi_analyst_reader` and
`\password bi_analyst_state` so passwords are entered at prompts. Enable LOGIN
for those two roles if necessary. Do not change the offline owner roles to LOGIN.
Test both restricted connections before configuring Render.

Download the CA certificate appropriate to the production connection from that
project's SSL settings. It must be multiline PEM text including the certificate
markers; export binary DER as Base-64 X.509 first.

### 2. Create the new production Render API

In Render select **New > Web Service**, connect GitHub and select
`SamuelOjuri/DataCube`. Do not select **Blueprint** or a static site.
Use a deployment branch/commit that passed the existing analyst CI workflow.

| Setting | Value |
|---|---|
| Name | `datacube-bi-analyst`, if available |
| Runtime | Python |
| Root directory | `services/bi_analyst` |
| Region | An available region close to the approved production database |
| Instances | One; choose a plan within the approved budget |
| Auto-deploy | Off |
| Health check path | `/health/ready` |

Build command:

```sh
pip install -r requirements.lock && pip install --no-deps --no-build-isolation .
```

Initial start command (one line, executed by Render's Linux shell):

```sh
bi-analyst-preflight && exec uvicorn bi_analyst.api:application --factory --host 0.0.0.0 --port $PORT --workers 1 --no-access-log --no-proxy-headers --timeout-keep-alive 5 --timeout-graceful-shutdown 20 --limit-concurrency 32
```

Add these non-secret environment variables:

```dotenv
PYTHON_VERSION=3.13.5
BI_ANALYST_ENVIRONMENT=production
BI_ANALYST_AUTH_PROVIDER=disabled
BI_ANALYST_ANALYST_ENABLED=false
BI_ANALYST_WORKFLOW_ENABLED=false
BI_ANALYST_PILOT_ONLY=true
BI_ANALYST_METRIC_EVALUATION_ENABLED=false
BI_ANALYST_REPLICA_COUNT=1
BI_ANALYST_DEPLOYMENT_OVERLAP=2
BI_ANALYST_CONNECTION_BUDGET=8
BI_ANALYST_READ_POOL_SIZE=2
BI_ANALYST_STATE_POOL_SIZE=2
BI_ANALYST_METRIC_CONCURRENCY=2
BI_ANALYST_WORKFLOW_CONCURRENCY=2
BI_ANALYST_BUSINESS_TIMEZONE=Europe/London
```

Confirm the eight-connection allowance fits production's database/ETL capacity.
Keep the initial pool and concurrency limits while measuring actual behaviour.

Add three secret values in Render's Environment page:

| Variable | Value |
|---|---|
| `BI_ANALYST_READ_DSN` | Production connection for `bi_analyst_reader` |
| `BI_ANALYST_STATE_DSN` | Production connection for `bi_analyst_state` |
| `BI_ANALYST_TELEMETRY_TOKEN` | A new cryptographically random secret of at least 32 characters |

The dashboard-created service does not generate the telemetry token automatically.
Generate it in a password manager or locally with the command below, which copies
it to the Windows clipboard without displaying it. Paste only into Render, store
securely and clear the clipboard afterward; do not send the token in chat.

```powershell
python -c "import secrets; print(secrets.token_urlsafe(32))" | Set-Clipboard
```

For the Supabase session pooler, replace every uppercase placeholder in these
URI templates with the approved production values. Encode each role password
once for use in a URI. Paste the completed URI without surrounding quotes.

`BI_ANALYST_READ_DSN`:

```text
postgresql://bi_analyst_reader.PROJECT_REF:ENCODED_READER_PASSWORD@POOLER_HOST:5432/postgres?sslmode=verify-full&sslrootcert=/etc/secrets/supabase-production.crt
```

`BI_ANALYST_STATE_DSN`:

```text
postgresql://bi_analyst_state.PROJECT_REF:ENCODED_STATE_PASSWORD@POOLER_HOST:5432/postgres?sslmode=verify-full&sslrootcert=/etc/secrets/supabase-production.crt
```

Copy the actual session-pooler host from the production project's Connect panel.
Both DSNs must target that same project, not merely the same shared pooler host.
Never use an administrator or ETL credential.

Add a Render secret file named **`supabase-production.crt`** containing the entire
PEM CA certificate. If secret files are only available after creating the service,
add it immediately under **Environment > Secret Files** and redeploy. An initial
startup before the file exists will fail; do not weaken TLS to work around it.

Leave pilot UUIDs, CORS, Monday credentials and Gemini settings unset for now; do
not invent placeholder users or integration secrets. Save and deploy, then record
the actual `API_ORIGIN`. Expect preflight `"passed": true`, `"errors": []` and
`GET API_ORIGIN/health/ready` to return HTTP 200 with:

```json
{
  "status": "ready",
  "environment": "production",
  "api_version": "0.7.0",
  "schema_version": 7,
  "identity": "deferred",
  "metric_execution": "disabled",
  "workflow": "disabled",
  "pilot_only": true
}
```

Additional metadata is normal. Do not continue if this checkpoint fails.

**Readiness needs attention before the pilot:** the initial logs contained
4.2-7 second responses. [Render allows five seconds for an HTTP health check](https://render.com/docs/health-checks).
Investigate database round trips, connection latency and pool pressure, then
verify timely checks on the final deployment. An HTTP 200 logged after the
timeout is not sufficient. Do not bypass readiness or buy a larger instance
without evidence that it addresses the cause.

### 3. Create the new production Netlify project

In Netlify select **Add new project > Import an existing project > GitHub**.
Select `SamuelOjuri/DataCube` and the tested deployment branch. This creates a
new project from the repository, not a replacement for another hosted project.
Request the name `datacube-bi-analyst`, if available; use a different clean
production name if already taken. Leave unrelated projects unchanged.

Use these settings from [netlify.toml](../netlify.toml):

| Setting | Value |
|---|---|
| Base directory | `web` |
| Build command | `npm run build` |
| Publish directory | `dist`, relative to `web` |
| Build environment | `NODE_VERSION=22`; `VITE_API_ORIGIN` set to the new Render `API_ORIGIN` |

Use the API origin without a trailing slash or path. No database password,
Supabase browser key, Monday secret or Gemini key belongs in Netlify. The build
generates CSP for that exact API origin; changing the origin requires rebuilding.

Before publishing, add the two build variables above and disable deploy previews and branch
deploys for this single-environment route. Do not configure `STAGING_API_ORIGIN`
to point at production to get around the preview guard, and do not add preview
origins to production CORS. Test changes locally and in CI instead.

Deploy this project's main production branch and record its actual
`FRONTEND_ORIGIN`. Confirm the frontend loads. Monday sign-in remains disabled
until the later activation step; that is expected, not a reason to change CORS
or disable authentication checks.

### 4. Configure one Monday app and the pilot users

Create one dedicated identity-only Monday app with `me:read`, **New OAuth Flow**
enabled and installation approved for the intended account. Its redirect is the
final API URL followed by `/auth/callback`. This app is not the ETL integration.

Set these on Render, not Netlify:

| Variable | Value |
|---|---|
| `BI_ANALYST_MONDAY_CLIENT_ID` / `BI_ANALYST_MONDAY_CLIENT_SECRET` | That app's credentials |
| `BI_ANALYST_MONDAY_ACCOUNT_ID` | The approved Monday account ID |
| `BI_ANALYST_MONDAY_REDIRECT_URI` | Final API origin + `/auth/callback` |
| `BI_ANALYST_AUTH_FRONTEND_URL` | Final frontend origin + `/auth/callback` |
| `BI_ANALYST_CORS_ORIGINS` | Exact final frontend origin, no wildcard or path |
| `BI_ANALYST_PILOT_SUBJECTS` | Comma-separated internal UUIDs of explicitly approved pilot users |

Use the [authentication provisioning instructions](bi-analyst-auth.md) to map
verified Monday account/user IDs to enabled, company-wide analyst principals.
Reuse existing principal UUIDs where appropriate; never grant access based only
on an email address or possession of a Monday login. Include a second approved
tester for cross-user isolation checks if the owner approves their access.

Save the settings while authentication and analyst/workflow execution remain
disabled. Enable them together in the next step after the pilot mapping and
model configuration are ready. No separate staging Monday app or optional
`BI_ANALYST_MONDAY_READ_TOKEN` is needed for this route.

### 5. Enable the analytical workflow for the approved pilot

Configure the backend-only `BI_ANALYST_GEMINI_API_KEY` and both reviewed model
prices: `BI_ANALYST_MODEL_INPUT_USD_PER_MILLION` and
`BI_ANALYST_MODEL_OUTPUT_USD_PER_MILLION`. Agree a pilot usage/spend limit and who
checks it. Use provider quota/budget controls where available, but do not assume
a billing alert or application cost estimate is an enforced spending cap.

Set `BI_ANALYST_AUTH_PROVIDER=monday`, `BI_ANALYST_ANALYST_ENABLED=true` and
`BI_ANALYST_WORKFLOW_ENABLED=true`.
Keep `BI_ANALYST_PILOT_ONLY=true`, the approved subject list and all existing
rate, concurrency, SQL/GraphQL execution, row and byte limits. Leave
`BI_ANALYST_METRIC_EVALUATION_ENABLED=false`.

For the owner-approved 10 October reasoning allowance, apply
`src/database/migrations/20261010_009_analyst_reasoning_budget.sql` once after 007
and 008 in a reviewed administrator transaction, then deploy the backend. The
additive migration retains schema version 7 and existing deadlines. Set these
values explicitly on an existing dashboard-created Render service; changing code
defaults or a Blueprint does not overwrite its saved environment variables:

```text
BI_ANALYST_MODEL_TIMEOUT_SECONDS=90
BI_ANALYST_WORKFLOW_TIMEOUT_SECONDS=600
```

These are maximums, not minimum response times. High model thinking, one retry
per node and cumulative attempt budgets remain unchanged. Preflight/readiness
reject a workflow budget above 300 seconds if migration 009 is missing. After
deployment, retry a failed question with a new run; old failed/expired runs are
not extended. No frontend deployment is needed. Qualify real planner/presentation
latency, concurrent load and failure rate before widening pilot access.

For this pilot deployment, replace the existing startup preflight prefix with
`bi-analyst-preflight --pilot`, retaining the rest of the working Uvicorn command
and its `&&` startup gate. This requires no paid Render shell. The pilot preflight
supports production, checks configuration/schema/grants and does not call Gemini
or Monday. A successful preflight does not prove the real sign-in or answer flow.
Manage these settings on the new dashboard-created service; do not reconnect an
old Blueprint. Save and redeploy, then require successful pilot preflight and
readiness before performing the real sign-in and answer checks below.

### 6. Complete a short, recorded pilot checklist

Record the deployed commit, final URLs, reviewer, date, results and observed
model usage in the existing release/ticket record. Do not record secrets or raw
customer questions in public artifacts.

- Candidate backend/frontend CI passes; database preflight and timely readiness
  pass on the actual production destination.
- Approved sign-in works; an unapproved user is denied. Expiry and logout work.
  One approved user cannot open another user's conversation/result.
- A small agreed set of representative questions gives reviewed answers; verify
  source scope, chart/table/CSV agreement, clarification and a direct conversation
  link. Confirm limitations and unavailable source freshness are visible.
- Browser disconnect/reconnect and cancellation produce clear outcomes. Exercise
  destructive failure scenarios in local/CI fixtures, not against the production
  database. Plan any live restart/rollback check in a maintenance window.
- Review latency, errors and provider usage against the agreed pilot budget.
  Resolve material failures before expanding access; do not call a few manual
  examples a statistically qualified load or cost result.
- Confirm backups, the rollback procedure below and an assigned operator for
  usage review, incident response and retention.

This establishes readiness for a **restricted production pilot**, not a claim
that every historical Phase 7 qualification target has been met. Expand access
only with explicit owner approval, on the same deployment. There is no repeat
Render/Netlify setup or second set of application sites.

## Operations without another paid service

- During the pilot, assign daily review of Render errors/readiness, database
  capacity and Gemini usage. Configure available platform failure notifications.
  Manual checks can miss incidents between reviews; record that limitation.
- Keep the telemetry token and existing metrics endpoint, but defer a dedicated
  monitoring host. Never disable protected logging, database audit or access
  checks to reduce cost.
- Assign a cleanup cadence and operator before the pilot. Use the existing
  maintenance CLI from an approved workstation or existing scheduler, with the
  dedicated maintenance credential, not an API or administrator credential.
  Review the proposed 30-day conversation and 90-day audit retention first.
  Expired sessions stop authorizing access even before their rows are deleted.

```powershell
bi-analyst-maintain --report
bi-analyst-maintain --conversation-days 30 --audit-days 90 --batch 100
# Only after reviewing the preview and approving the retention policy:
bi-analyst-maintain --apply --conversation-days 30 --audit-days 90 --batch 100
```

These commands require the installed analyst package and securely supplied
`BI_ANALYST_MAINTENANCE_DSN`. Keep that credential off the API. Record failed or
missed runs, and repeat bounded batches when `batch_full` is true. Migration 008
and its restricted role are still required; its optional Render cron service is
not. Downloads, provider logs and database backups need their own retention review.

## Later releases and rollback

Without a permanent staging environment, hosted integration regressions can
affect the single live service. Compensate with local/CI tests, manual deployments,
backups, a maintenance window and a known compatible rollback artifact.

1. Preserve the last working API release, frontend deploy and configuration
   references without copying secrets into Git. Review schema changes separately.
2. Disable analytical/workflow access while performing a risky change, deploy the
   tested candidate to the same services, and rerun the pilot smoke checks.
3. If checks fail, keep access disabled and restore the compatible API/frontend
   releases. Prefer a tested 0.7.x API preserving pilot/kill switches and schema 7.
   The older 0.6.0 API does not implement those operational controls.
4. Never drop conversations, checkpoints, results, audit records or source data
   to roll back. Disabling features does not undo database migrations or revoke
   identities; credential/access incidents also require session/grant revocation.

## What happens to the original qualification tooling

The earlier full-release programme is deferred, not silently marked complete.
The retained [release validator](../services/bi_analyst/bi_analyst/operations/release.py)
requires staging evidence and all its original checks.
[bi-analyst-qualify](../services/bi_analyst/bi_analyst/operations/qualify.py) refuses
production and sends real model requests in answer mode. **Do not run it against
production or relabel production as staging to bypass that guard.**

Neither command is a runtime dependency or a launch gate for this owner-approved
single-deployment pilot. Do not fabricate evidence, change `qualified` to true or
claim the original gate passed. The
[existing performance/cost targets](../services/bi_analyst/deploy/release-policy.toml)
remain review targets, not spending caps or measured outcomes. If broader automated
qualification is required later, review a suitable process explicitly; no new
staging sites are part of the approved current plan.

## Resolved startup issues to remember

- **`no certificate or crl found`**: the Render secret must contain the actual
  multiline PEM CA certificate, including BEGIN/END markers, for the target
  connection. Binary DER must be exported as Base-64 X.509 first. Both DSNs'
  `sslrootcert` paths must match the secret filename; `.cer` versus `.crt` alone
  does not fix invalid contents. Keep `sslmode=verify-full`.
- **Password authentication failed**: provision LOGIN and independent passwords
  for reader/state roles. Test each with a password prompt, then update Render's
  corresponding DSN. Pooler usernames use `role.PROJECT_REF`; URI passwords must
  be percent-encoded once. The main `postgres` password is not the role password.
- **`catalogue_acceptance_required`**: the packaged
  [acceptance record](../services/bi_analyst/bi_analyst/semantic/acceptance.json)
  must be in the deployment commit. Its JSON ignore exception is already fixed.
  Do not replace its hash or bypass acceptance.
- A root/unmatched `HEAD` returning 404 is not the configured health check.
  Verify the actual `GET /health/ready`, including response time.
