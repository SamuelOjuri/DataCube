# Automation Overview

This document outlines how the automation pipeline operates, covering the core
webhook-driven flow, scheduled background jobs, and the pipeline forecast layer.

## Components

- **Task helpers** (`src/tasks/pipeline.py`)
  - Provide callable functions for rehydrate flows, LLM backfill, Monday sync, and
    project-by-ID refreshes.
- **Queue worker** (`src/services/queue_worker.py`)
  - In-process async worker that runs rehydrate → analyse → Monday push jobs.
  - Persists job status to the `job_queue` table for observability.
- **Webhook integration** (`src/webhooks/webhook_server.py`)
  - Subitem/hidden item updates enqueue rehydrate jobs for affected parent projects.
  - Parent item updates still perform immediate analysis and enqueue a Monday push,
    except order-value mirror changes, which enqueue source rehydration.
- **Postgres maintenance** (`src/tasks/postgres_maintenance.py`)
  - Materialized view refresh (conversion metrics, forecast aggregates, and smoothing artifacts).
  - Daily base forecast and smoothing snapshot creation plus retention cleanup.
- **Forecast API** (`src/api/routes/forecast.py`)
  - Read-only endpoints for Power BI and other consumers:
    - `GET /forecast/pipeline` — monthly 12-month forecast from the materialized view.
    - `GET /forecast/snapshot` — historical snapshot data with pagination.
    - `GET /forecast/smoothing/projects` — project-level smoothing scores and explanations.
    - `GET /forecast/smoothing/monthly` — smoothed monthly allocation totals.
    - `GET /forecast/smoothing/snapshot` — historical project-month smoothing allocations.
    - `GET /forecast/smoothing/snapshot/totals` — full filtered smoothing totals independent of pagination.
- **Scheduler** (`src/api/app.py`)
  - APScheduler runs the following periodic jobs:
    - **Hourly delta rehydrate** (`rehydrate_delta`) — re-syncs recently changed projects.
    - **Nightly LLM backfill** (`backfill_llm`, 02:15 UTC) — fills missing LLM analyses.
    - **25-minute Monday sync** (`sync_projects_to_monday`) — pushes updated analyses back to Monday.
    - **6-hour recent rehydrate** (`rehydrate_recent`) — broader catch-up rehydrate.
    - **30-minute materialized view refresh** (`refresh_conversion_views`) — runs `refresh_analytics_views()`, including smoothing signal refresh before smoothed monthly allocation refresh.
    - **Daily forecast snapshot maintenance** (`forecast_snapshot_maintenance`, default 03:10 UTC) — creates today's base and smoothing snapshots and deletes expired rows.
  - Queue worker is started alongside the FastAPI app.

## Customer Order Value

`projects.total_order_value` is the sum of `cust_order_value_material` plus
`cust_additional_charges` across the project's persisted subitems. Both amounts
come from the same resolved hidden item: Monday columns `numbers98__1` and
`numbers3__1`. The material component remains separately available. Order dates,
invoice values, enquiry values and reporting-stage filters are unchanged.

Order inputs use strict decimal parsing. A fetched blank numeric source is stored
as zero; a missing or invalid source is stored as NULL. A project total is withheld
if any child has an unknown component, if a hidden source is repeated in the
loaded rollup population, or if the total exceeds NUMERIC(12,2). These conditions
are logged. The existing project total is retained, not certified as current.
Fully loaded zero-value projects, including projects with no persisted subitems,
are updated to zero. Child reads use keyset pagination until an empty page.

Order webhooks queue rehydration of both current source amounts instead of
writing event values directly. Parent order mirrors cannot overwrite the rollup.
Explicit hidden IDs take precedence over name matching; unresolved explicit IDs
and multiple links do not fall back to another source. Name-based fallbacks still
require relationship verification during reconciliation.

Before the order-only backfill, ensure `cust_additional_charges NUMERIC(12,2)` exists
on both `hidden_items` and `subitems`, without a zero default for unfetched history.
Deploy the corrected sync and webhook code to all writers. The code change does
not itself backfill existing rows, refresh forecast materialized views, or restate
historical snapshots. Reconcile the backfilled totals against Monday before using
them as certified full customer order values.

Offline regression checks (the two live webhook smoke tests are excluded):

```powershell
& .\report.venv\Scripts\python.exe -m pytest tests/test_sync_service.py tests/test_webhooks.py -k 'not test_parent_update and not test_subitem_update' -q
```

### Controlled Order Backfill

Use `scripts/backfill_order_values.py` for the order-only historical backfill.
It does not invoke the broad rehydration pipeline, change order dates, refresh
forecast views, replay webhooks, or modify historical snapshots.

Prerequisites:

- Set `MONDAY_API_KEY` and `SUPABASE_DB_URL` in the environment or local `.env`.
  The database URL must be a PostgreSQL connection string, not the Supabase REST
  URL. Use direct PostgreSQL or session pooling and credentials permitted to read,
  lock and update these three tables. The script does not use the service-role
  REST client and does not print connection credentials.
- Keep the corrected services deployed, but pause scheduled syncs, queue consumers,
  snapshot jobs and other writers for the capture/review/apply/verify window.
  Retain incoming webhooks durably for replay; do not discard events. Coordinate
  a quiet Monday editing window too. The script cannot pause these services for
  you; `--writers-paused` is your explicit confirmation, not an automatic pause.
- Ensure there is enough local space for the baseline, source and plan artifacts.
  These contain commercially sensitive amounts and identifiers: keep them private
  and out of source control. Use a new run directory for every preparation.

1. Prepare a read-only dry run from the repository root:

```powershell
python -m scripts.backfill_order_values prepare --run-dir outputs/order_value_backfill/run1
```

`prepare` reads all database rows using keyset pagination and starts fresh Monday
board traversals, without reusing sync cursors. It checks board counts and exact
detail-response coverage. Missing pages, API failures and count changes abort
preparation; an incomplete directory has no usable manifest and cannot be applied.
Parent IDs, subitem parent/hidden links and numeric hidden-board inputs are captured.
The computed material-plus-charges amount is compared with Monday's total formula.

The run directory contains `baseline.json`, `source.json`, `plan.json`,
`review.csv` and `manifest.json`. Review the CSV's old total, material sum, charge
sum, proposed total, difference and validation status. Inspect `diagnostics` in
the plan for orphaned records and unlinked nonzero hidden orders. The manifest
prints a run ID, per-table update counts and blocked-project counts.

Missing or conflicting links, duplicate source orders, unknown amounts and formula
disagreements block the whole affected project. Unlike ordinary sync's name-based
fallbacks, this backfill requires a verified single Monday link agreeing with the
stored relationship. Resolve relationships using the corrected sync process and
prepare a new run; this script does not guess links, insert missing records or
delete stale children. A project with no stored or live children is withheld unless
individually approved when preparing a new run, for example:

```powershell
python -m scripts.backfill_order_values prepare --run-dir outputs/order_value_backfill/run2 --approve-empty-project 123456789
```

2. Apply the reviewed run, using its printed run ID:

```powershell
python -m scripts.backfill_order_values apply --run-dir outputs/order_value_backfill/run1 --confirm-run-id RUN_ID_FROM_PREPARE --writers-paused
```

Apply refuses unresolved projects or diagnostics by default. `--allow-blocked` is
an explicit opt-in to a PARTIAL backfill: only verified projects and their linked
order components are updated; blocked rows are untouched. This is not certification
of the full dataset. Keep the unresolved report for step 4.

The script checks artifact hashes, code/mapping versions and database target, then
fetches Monday again and rejects source drift. Inside one transaction it acquires
write-conflicting table locks with a five-second lock timeout, compares the current
database with the captured baseline, replaces order fields and verifies the complete
expected state before commit. Other captured invoice/enquiry/date fields must remain
unchanged. Database triggers may update normal metadata timestamps.

Existing nonzero totals are replaced, never incremented. Any write or reconciliation
failure rolls back the transaction. Retrying the same unchanged run after a confirmed
commit is a no-op. Each successful apply writes a separate `apply-*.json` receipt.
If the connection or receipt write fails near commit, do not assume rollback: run
`verify` before retrying. Never edit staged files to bypass a refusal.

3. Verify while writers are still paused:

```powershell
python -m scripts.backfill_order_values verify --run-dir outputs/order_value_backfill/run1
```

This read-only check compares the stored values with the reviewed post-apply state
and writes a `verify-*.json` result. It exits with code 2 on disagreement and code 1
on errors. A matching result can still have blocked projects after a partial run;
check those counts. Source or database drift requires a fresh preparation, not
overwriting the old audit files.

After verification, follow step 4's reconciliation and forecast-view refresh gates.
Replay retained events and resume writers under the deployment runbook; neither is
automated here. Preserved historical snapshots still represent their original values.

Offline backfill tests:

```powershell
python -m pytest tests/test_backfill_order_values.py -q --basetemp "$env:TEMP/order-$([guid]::NewGuid().ToString('N'))"
```

## Scheduled Jobs Summary

| Job | Schedule | Function | Description |
|---|---|---|---|
| Delta rehydrate | Every 1 hour | `rehydrate_delta` | Re-sync projects changed in the last 3 days |
| LLM backfill | Cron 02:15 UTC | `backfill_llm` | Fill missing LLM analyses for recent projects |
| Monday sync | Every 25 minutes | `sync_projects_to_monday` | Push updated analysis results to Monday.com |
| Recent rehydrate | Every 6 hours | `rehydrate_recent` | Broader catch-up rehydrate of recent changes |
| Materialized view refresh | Every 30 minutes | `refresh_conversion_views` | Refresh analytics, forecast, smoothing signal, and smoothed monthly artifacts where deployed |
| Forecast snapshot maintenance | Cron (default 03:10 UTC) | `run_daily_forecast_snapshot_maintenance` | Insert daily base + smoothing snapshots and clean up expired rows |

## Pipeline Forecast Layer

The forecast layer runs in parallel to the Monday push flow and does not interfere with it. It produces SQL-based forecast artifacts that Power BI (or any Postgres/API consumer) can query.

### Architecture

```
Monday Sync + Webhooks
        │
        ▼
  projects + analysis_results  (core data)
        │
        ├──► vw_pipeline_forecast_project_v1    (real-time project-level forecast view)
        │         │
        │         ├──► mv_pipeline_forecast_monthly_12m_v1  (monthly aggregate, refreshed every 30 min)
        │         │
      │         ├──► pipeline_forecast_snapshot            (daily snapshot table)
      │         │
      │         └──► vw_pipeline_smoothing_score_v1        (project smoothing scores)
      │                   │
      │                   ├──► mv_pipeline_smoothed_revenue_monthly_12m_v1
      │                   │
      │                   └──► pipeline_smoothing_forecast_snapshot
      │
      ├──► mv_invoice_smoothing_signal_v1      (invoice timing signals)
        │
        └──► Monday push  (unchanged)
```

### Forecast Artifacts

- **`vw_pipeline_forecast_project_v1`** (view) — Joins `projects` to the latest `analysis_results` and computes stage bucket, contract value (with practical fallback), probability (with precedence rules), forecast date, and committed/expected/best-case/worst-case value bands. This is the single source of truth for all forecast formulas.

- **`mv_pipeline_forecast_monthly_12m_v1`** (materialized view) — Aggregates the project-level view into monthly totals by stage bucket for the next 12 months. Refreshed concurrently every 30 minutes by the scheduler. Required unique index ensures concurrent refresh works without downtime.

- **`pipeline_forecast_snapshot`** (table) — Stores one daily snapshot set of all project-level forecasts within the 12-month window. Populated by the `create_pipeline_forecast_snapshot()` SQL function. The snapshot is idempotent (re-running for the same date replaces that day's rows). Primary key: `(snapshot_date, project_id, forecast_month)`.

- **`pipeline_smoothing_forecast_snapshot`** (table) — Stores daily project-month smoothing allocation rows. One project can appear in multiple forecast months, so `expected_value` is repeated on each project-month row as project context. Do not sum `expected_value` across smoothing snapshot rows for reporting totals. Use `allocated_expected_value` instead; in the REST API this is exposed as `allocated_monthly_value`, with `smoothed_allocated_value` and `unsmoothed_allocated_value` providing the component split. Primary key: `(snapshot_date, project_id, forecast_month)`.

- **`mv_invoice_smoothing_signal_v1`** (materialized view) — Stores live Empirical-Bayes invoice smoothing signals by dimension and group. It is refreshed through `refresh_invoice_smoothing_signal_v1(CURRENT_DATE)` before smoothed monthly allocation is refreshed.

- **`vw_pipeline_smoothing_score_v1`** (view) — Scores live forecast projects with Category 30%, Type 10%, Product 30%, Account 30% smoothing weights, global fallback values, risk bands, confidence counts, and treatment recommendations.

- **`mv_pipeline_smoothed_revenue_monthly_12m_v1`** (materialized view) — Allocates expected value into unsmoothed and smoothed components across the current 12-month window using day-weighted month overlap.

### Snapshot Retention

Old base forecast and smoothing snapshot rows are automatically deleted after the daily insert. The default retention window is **730 days** (approximately 2 years).

**Configuration:**

| Environment Variable | Default | Description |
|---|---|---|
| `FORECAST_SNAPSHOT_RETENTION_DAYS` | `730` | Days of snapshot history to retain |
| `FORECAST_SNAPSHOT_CRON_HOUR_UTC` | `3` | Hour (UTC) for the daily snapshot job |
| `FORECAST_SNAPSHOT_CRON_MINUTE_UTC` | `10` | Minute for the daily snapshot job |

To change the retention window without redeploying, set the `FORECAST_SNAPSHOT_RETENTION_DAYS` environment variable and restart the service.

To run snapshot maintenance manually (e.g., to seed the first snapshot or re-run after a failure):

```python
from src.tasks.postgres_maintenance import run_daily_forecast_snapshot_maintenance
import logging
run_daily_forecast_snapshot_maintenance(logger=logging.getLogger("manual"))
```

This wrapper refreshes analytics and smoothing materialized views, creates the base forecast snapshot, cleans old base rows, creates the smoothing snapshot, and cleans old smoothing rows.

Or target a specific date:

```python
from datetime import date
from src.tasks.postgres_maintenance import create_pipeline_forecast_snapshot, create_pipeline_smoothing_forecast_snapshot
import logging
create_pipeline_forecast_snapshot(task_logger=logging.getLogger("manual"), snapshot_date=date(2026, 2, 15))
create_pipeline_smoothing_forecast_snapshot(task_logger=logging.getLogger("manual"), snapshot_date=date(2026, 2, 15))
```

### Power BI Consumption

Power BI connects to the forecast layer in one of two ways:

**1. Direct Database Connection (recommended)**

- Connect Power BI Desktop to the Supabase PostgreSQL instance.
- Import or DirectQuery the following:
  - `pipeline_forecast_snapshot` — use `snapshot_date` as the incremental refresh partition key.
  - `pipeline_smoothing_forecast_snapshot` — use `snapshot_date` as the incremental refresh partition key and `allocated_expected_value` as the additive forecast value.
  - `mv_pipeline_forecast_monthly_12m_v1` — current-state monthly summary.
  - `mv_pipeline_smoothed_revenue_monthly_12m_v1` — current-state smoothed monthly revenue allocation.
  - `vw_pipeline_forecast_project_v1` — project-level drill-down.
  - `vw_pipeline_smoothing_score_v1` — project-level smoothing explanations, risk bands, confidence, and treatment fields.
- For incremental refresh on the snapshot table, configure a daily refresh with a rolling retention window (e.g., 24 months) keyed on `snapshot_date`.

**2. REST API**

- `GET /forecast/pipeline?months=12` — monthly aggregates.
- `GET /forecast/snapshot?snapshot_date=2026-02-15` — snapshot rows for a specific date, with pagination (`offset`, `limit`).
- `GET /forecast/smoothing/projects` — project-level smoothing scores and explanation fields.
- `GET /forecast/smoothing/monthly?months=12` — current smoothed monthly allocation totals.
- `GET /forecast/smoothing/snapshot` — paginated project-month smoothing snapshot rows. Row-level `expected_value` is the original project expected value and may repeat for projects allocated across multiple months.
- `GET /forecast/smoothing/snapshot/totals` — full filtered smoothing snapshot totals independent of pagination. Use `totals.allocated_monthly_value` for the additive forecast total; `totals.expected_value` is explanatory and should not be summed across project-month rows.

### Validation

Run the automated forecast validation checks:

```bash
# Pytest suite — validates SQL invariants against live data
pytest tests/test_pipeline_forecast_service.py -q

# Standalone CLI validation script
python scripts/validate_forecast_sql.py

# Focused Phase 7 smoothing SQL checks
python scripts/validate_smoothing_phase7_sql.py

# Full smoothing SQL health check
python scripts/validate_smoothing_sql.py

# Manual smoothing refresh plus validation after deployment/backfill
python scripts/validate_smoothing_sql.py --refresh

# Smoothing SQL/integration tests
pytest tests/test_smoothing_phase7_sql.py tests/test_smoothing_forecast_service.py -q

# Backtest against invoiced actuals (requires accumulated snapshot history)
python scripts/forecast_backtest.py --months-back 6
```

The backtest script compares historical snapshots against invoiced subitems and generates a markdown report with WAPE, bias ratio, band coverage rate, and calibration recommendations.

## Operations

### Monitoring Jobs

The `job_queue` table records every queue task with status transitions (`queued`,
`running`, `completed`, `failed`). Use Supabase SQL or dashboards to monitor queue
health.

Forecast-specific monitoring:

- Check the latest snapshot date: `SELECT MAX(snapshot_date) FROM pipeline_forecast_snapshot;`
- Count rows per snapshot: `SELECT snapshot_date, COUNT(*) FROM pipeline_forecast_snapshot GROUP BY 1 ORDER BY 1 DESC LIMIT 7;`
- Check the latest smoothing snapshot date: `SELECT MAX(snapshot_date) FROM pipeline_smoothing_forecast_snapshot;`
- Count smoothing rows per snapshot: `SELECT snapshot_date, COUNT(*) FROM pipeline_smoothing_forecast_snapshot GROUP BY 1 ORDER BY 1 DESC LIMIT 7;`
- Check smoothing signal date: `SELECT as_of_date FROM mv_invoice_smoothing_signal_v1 WHERE dimension = 'global' AND group_key = '__global__';`
- Compare smoothing monthly and snapshot totals: `SELECT ROUND(SUM(allocated_expected_value), 2) FROM mv_pipeline_smoothed_revenue_monthly_12m_v1;` and latest `pipeline_smoothing_forecast_snapshot` totals.
- Verify materialized view freshness by comparing row counts to the live view.

### Running Manually

All previous scripts still exist as thin wrappers around the task helpers. They can
be invoked manually if required for smoke tests or emergency replays.

Forecast-specific manual operations:

- **Force snapshot re-creation**: Call `create_pipeline_forecast_snapshot()` with a target date.
- **Force smoothing snapshot re-creation**: Call `create_pipeline_smoothing_forecast_snapshot()` with a target date.
- **Force materialized view refresh**: Call `refresh_conversion_views()` from `postgres_maintenance.py`; this uses `refresh_analytics_views()` and refreshes smoothing signals before smoothed monthly allocation where deployed.
- **Adjust retention**: Call `cleanup_old_pipeline_forecast_snapshots(retain_days=N)` directly.
- **Adjust smoothing retention**: Call `cleanup_old_pipeline_smoothing_forecast_snapshots(retain_days=N)` directly.

Smoothing-specific runbook after SQL deployment, invoice-date backfill, or stale materialized-view validation failures:

```powershell
python scripts/backfill_invoice_dates.py --dry-run
python scripts/backfill_invoice_dates.py
python scripts/validate_smoothing_phase7_sql.py
python scripts/validate_smoothing_sql.py --refresh
python scripts/validate_smoothing_sql.py
python -m pytest tests/test_smoothing_phase7_sql.py tests/test_smoothing_forecast_service.py -q
```

Use `--refresh` as an operator action after deployment/backfill or when a health check indicates stale smoothing materialized views. Routine production refresh should come from the APScheduler `refresh_conversion_views` job and daily snapshot maintenance.

### Smoothing Deployment Order

1. Apply `src/database/schema/schema.sql` so invoice rollup columns, smoothing signal functions/views, scoring views, monthly allocation, and snapshot table exist.
2. Apply `src/database/schema/functions.sql` so `refresh_analytics_views()`, smoothing snapshot creation, and cleanup functions are current.
3. Run `python scripts/backfill_invoice_dates.py --dry-run`, review prepared updates, then run `python scripts/backfill_invoice_dates.py`.
4. Run `python scripts/validate_smoothing_sql.py --refresh` to rebuild smoothing signal, monthly allocation, and today's smoothing snapshot.
5. Run `python scripts/validate_smoothing_phase7_sql.py` and `python scripts/validate_smoothing_sql.py`.
6. Run `python -m pytest tests/test_smoothing_phase7_sql.py tests/test_smoothing_forecast_service.py tests/test_pipeline_forecast_service.py tests/test_postgres_maintenance_phase5.py -q`.
7. Deploy or restart the FastAPI service so APScheduler and `/forecast/smoothing/*` endpoints use the current code.
8. Smoke test `/forecast/smoothing/projects`, `/forecast/smoothing/monthly`, `/forecast/smoothing/snapshot`, and `/forecast/smoothing/snapshot/totals`.

### First Production Smoothing Validation

First production validation should verify live database values, not hard-coded workbook fixture outputs:

- invoice rollups match subitem invoice dates;
- mature-cohort eligibility uses the deployed `as_of_date`;
- global fallback rate/spread are recomputed from the live mature cohort;
- shrinkage formulas use `k = 20`;
- scoring uses Category 30%, Type 10%, Product 30%, Account 30%;
- risk bands and treatment fields match the implementation contract;
- smoothed monthly allocation sums back to project expected value within rounding tolerance;
- latest smoothing snapshot rows are project-month rows with project-level explanation fields;
- Power BI uses `allocated_expected_value` or API `allocated_monthly_value` for additive smoothing snapshot totals.

Only compare against workbook metrics in a deliberate fixture/parity run using frozen workbook-style data and `as_of_date = 2026-04-28`; live production values are expected to differ.

### Adding New Tasks

1. Implement the core logic inside `src/tasks/pipeline.py` or `src/tasks/postgres_maintenance.py`.
2. Add a queue handler in `queue_worker.py` and expose enqueue helpers.
3. Wire the new tasks via webhooks or scheduler as appropriate.

### Monday Push — Unchanged

The existing webhook → rehydrate → analyse → Monday push flow remains active and is
completely independent of the forecast layer. Forecast artifacts are a parallel,
read-only output path that does not write back to Monday.com.
