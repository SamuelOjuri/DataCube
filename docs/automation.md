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
  - Materialized view refresh (conversion metrics + forecast aggregates).
  - Daily forecast snapshot creation and retention cleanup.
- **Forecast API** (`src/api/routes/forecast.py`)
  - Read-only endpoints for Power BI and other consumers:
    - `GET /forecast/pipeline` — monthly 12-month forecast from the materialized view.
    - `GET /forecast/snapshot` — historical snapshot data with pagination.
- **Scheduler** (`src/api/app.py`)
  - APScheduler runs the following periodic jobs:
    - **Hourly delta rehydrate** (`rehydrate_delta`) — re-syncs recently changed projects.
    - **Nightly LLM backfill** (`backfill_llm`, 02:15 UTC) — fills missing LLM analyses.
    - **25-minute Monday sync** (`sync_projects_to_monday`) — pushes updated analyses back to Monday.
    - **6-hour recent rehydrate** (`rehydrate_recent`) — broader catch-up rehydrate.
    - **30-minute materialized view refresh** (`refresh_conversion_views`) — refreshes `conversion_metrics`, `conversion_metrics_recent`, and `mv_pipeline_forecast_monthly_12m_v1`.
    - **Daily forecast snapshot maintenance** (`forecast_snapshot_maintenance`, default 03:10 UTC) — creates today's snapshot and deletes expired rows.
  - Queue worker is started alongside the FastAPI app.

## Scheduled Jobs Summary

| Job | Schedule | Function | Description |
|---|---|---|---|
| Delta rehydrate | Every 1 hour | `rehydrate_delta` | Re-sync projects changed in the last 3 days |
| LLM backfill | Cron 02:15 UTC | `backfill_llm` | Fill missing LLM analyses for recent projects |
| Monday sync | Every 25 minutes | `sync_projects_to_monday` | Push updated analysis results to Monday.com |
| Recent rehydrate | Every 6 hours | `rehydrate_recent` | Broader catch-up rehydrate of recent changes |
| Materialized view refresh | Every 30 minutes | `refresh_conversion_views` | Concurrent refresh of conversion metrics and forecast monthly aggregate |
| Forecast snapshot maintenance | Cron (default 03:10 UTC) | `run_daily_forecast_snapshot_maintenance` | Insert daily snapshot + clean up expired rows |

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
        │         └──► pipeline_forecast_snapshot            (daily snapshot table)
        │
        └──► Monday push  (unchanged)
```

### Forecast Artifacts

- **`vw_pipeline_forecast_project_v1`** (view) — Joins `projects` to the latest `analysis_results` and computes stage bucket, contract value (with practical fallback), probability (with precedence rules), forecast date, and committed/expected/best-case/worst-case value bands. This is the single source of truth for all forecast formulas.

- **`mv_pipeline_forecast_monthly_12m_v1`** (materialized view) — Aggregates the project-level view into monthly totals by stage bucket for the next 12 months. Refreshed concurrently every 30 minutes by the scheduler. Required unique index ensures concurrent refresh works without downtime.

- **`pipeline_forecast_snapshot`** (table) — Stores one daily snapshot set of all project-level forecasts within the 12-month window. Populated by the `create_pipeline_forecast_snapshot()` SQL function. The snapshot is idempotent (re-running for the same date replaces that day's rows). Primary key: `(snapshot_date, project_id, forecast_month)`.

### Snapshot Retention

Old snapshot rows are automatically deleted by `cleanup_old_pipeline_forecast_snapshots()` after the daily insert. The default retention window is **730 days** (approximately 2 years).

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

Or target a specific date:

```python
from datetime import date
from src.tasks.postgres_maintenance import create_pipeline_forecast_snapshot
import logging
create_pipeline_forecast_snapshot(logger=logging.getLogger("manual"), snapshot_date=date(2026, 2, 15))
```

### Power BI Consumption

Power BI connects to the forecast layer in one of two ways:

**1. Direct Database Connection (recommended)**

- Connect Power BI Desktop to the Supabase PostgreSQL instance.
- Import or DirectQuery the following:
  - `pipeline_forecast_snapshot` — use `snapshot_date` as the incremental refresh partition key.
  - `mv_pipeline_forecast_monthly_12m_v1` — current-state monthly summary.
  - `vw_pipeline_forecast_project_v1` — project-level drill-down.
- For incremental refresh on the snapshot table, configure a daily refresh with a rolling retention window (e.g., 24 months) keyed on `snapshot_date`.

**2. REST API**

- `GET /forecast/pipeline?months=12` — monthly aggregates.
- `GET /forecast/snapshot?snapshot_date=2026-02-15` — snapshot rows for a specific date, with pagination (`offset`, `limit`).

### Validation

Run the automated forecast validation checks:

```bash
# Pytest suite — validates SQL invariants against live data
pytest tests/test_pipeline_forecast_service.py -q

# Standalone CLI validation script
python scripts/validate_forecast_sql.py

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
- Verify materialized view freshness by comparing row counts to the live view.

### Running Manually

All previous scripts still exist as thin wrappers around the task helpers. They can
be invoked manually if required for smoke tests or emergency replays.

Forecast-specific manual operations:

- **Force snapshot re-creation**: Call `create_pipeline_forecast_snapshot()` with a target date.
- **Force materialized view refresh**: Call `refresh_conversion_views()` from `postgres_maintenance.py`.
- **Adjust retention**: Call `cleanup_old_pipeline_forecast_snapshots(retain_days=N)` directly.

### Adding New Tasks

1. Implement the core logic inside `src/tasks/pipeline.py` or `src/tasks/postgres_maintenance.py`.
2. Add a queue handler in `queue_worker.py` and expose enqueue helpers.
3. Wire the new tasks via webhooks or scheduler as appropriate.

### Monday Push — Unchanged

The existing webhook → rehydrate → analyse → Monday push flow remains active and is
completely independent of the forecast layer. Forecast artifacts are a parallel,
read-only output path that does not write back to Monday.com.
