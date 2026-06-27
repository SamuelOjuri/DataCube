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
  - Parent item updates still perform immediate analysis and enqueue a Monday push.
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
