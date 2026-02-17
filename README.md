# DataCube Operations

## Webhook Listener (FastAPI) Setup

1. Duplicate `env.example` to `.env` (or ensure the variables are present in your environment) and provide:
   - `MONDAY_WEBHOOK_URL` pointing to `https://<host>/webhooks/monday`
   - `WEBHOOK_SECRET` matching the signing secret configured in your Monday integration
   - Supabase credentials (`SUPABASE_URL`, `SUPABASE_SERVICE_KEY`, `SUPABASE_KEY`)
2. Install dependencies inside the existing virtualenv or via `pip install -r requirements.txt`.
3. Launch the listener:

   ```bash
   uvicorn src.webhooks.webhook_server:app --host 0.0.0.0 --port ${WEBHOOK_PORT:-8000}
   ```

4. When Monday issues the webhook `challenge`, the server echoes it (per [Monday webhook verification](https://developer.monday.com/api-reference/reference/webhooks)). Ensure ports/firewalls allow the inbound request.
5. Review logs for `processed_with_warnings` entries—these indicate the webhook executed but the downstream analysis raised an exception. Details are written to the `webhook_events` table.

## Data Quality Checks

Run `python scripts/data_quality_checks.py` to see mirroring coverage and numeric null counts before relying on webhook-triggered analysis. The script requires Supabase service credentials in your environment.

## Automation & Background Jobs

- Webhook processing now enqueues long-running work (rehydrate, reanalyse, Monday sync) via the in-process queue at `src/services/queue_worker.py`.
- Periodic catch-ups (hourly delta hydrate, nightly LLM backfill, 25-minute Monday sync) are scheduled through APScheduler in `src/api/app.py`.
- A daily forecast snapshot job captures pipeline state for trend analysis and Power BI consumption.
- Job history is stored in the `job_queue` table; see `docs/automation.md` for operational details.

## Pipeline Forecast Layer

DataCube produces a 12-month rolling sales pipeline forecast that sits alongside the existing Monday push flow. The forecast layer is designed for consumption by Power BI but is also available via REST API.

### Forecast Artifacts

| Artifact | Type | Purpose |
|---|---|---|
| `vw_pipeline_forecast_project_v1` | View | Real-time project-level forecasts with stage bucketing, probability bands, and contract-value fallback logic |
| `mv_pipeline_forecast_monthly_12m_v1` | Materialized view | Pre-aggregated monthly totals for the next 12 months, grouped by stage bucket. Refreshed every 30 minutes |
| `pipeline_forecast_snapshot` | Table | Daily point-in-time snapshots of the full project forecast. Used for drift tracking and historical trend analysis |

### Data Contract

Each forecast row includes:

- **stage_bucket** — `Committed`, `Open`, or `Lost` (derived from `pipeline_stage`)
- **contract_value** — `total_order_value` with practical fallback to `new_enquiry_value` when order value is zero or null
- **probability** — precedence: Lost=0 / Committed=1 / model conversion rate / Monday probability / stage-based defaults
- **committed_value** — contract value for Committed projects, else 0
- **expected_value** — contract value × probability
- **best_case_value** — contract value × (probability + spread), clamped [0, 1]
- **worst_case_value** — contract value × (probability − spread), clamped [0, 1]

Band monotonicity (`worst_case_value ≤ expected_value ≤ best_case_value`) is enforced by the SQL formulas and validated by automated tests.

### Refresh Cadence

| Job | Schedule | Description |
|---|---|---|
| Materialized view refresh | Every 30 minutes | Refreshes `mv_pipeline_forecast_monthly_12m_v1` (and conversion metrics) concurrently |
| Daily forecast snapshot | Cron (default 03:10 UTC) | Inserts one snapshot set into `pipeline_forecast_snapshot` and cleans up expired rows |

The snapshot cron time and retention window are configurable via environment variables (see below).

### Environment Variables

| Variable | Default | Description |
|---|---|---|
| `FORECAST_SNAPSHOT_RETENTION_DAYS` | `730` | Number of days to retain snapshot rows before cleanup |
| `FORECAST_SNAPSHOT_CRON_HOUR_UTC` | `3` | Hour (UTC) at which the daily snapshot job runs |
| `FORECAST_SNAPSHOT_CRON_MINUTE_UTC` | `10` | Minute at which the daily snapshot job runs |

### API Endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/forecast/pipeline` | GET | Monthly 12-month forecast from the materialized view. Supports `months`, `as_of_month`, and `stage_bucket` query parameters |
| `/forecast/snapshot` | GET | Snapshot data with pagination. Supports `snapshot_date`, `project_id`, `stage_bucket`, `offset`, and `limit` query parameters |

### Power BI Dataset Setup

Power BI can consume forecast data via **direct database connection** (recommended) or the REST API.

**Direct Database Connection (recommended):**

1. In Power BI Desktop, choose **Get Data → PostgreSQL database**.
2. Enter the Supabase connection host and database credentials.
3. Import or DirectQuery the following sources:
   - `pipeline_forecast_snapshot` — for historical trend dashboards. Use `snapshot_date` as the incremental refresh partition key.
   - `mv_pipeline_forecast_monthly_12m_v1` — for current-month pipeline summary visuals.
   - `vw_pipeline_forecast_project_v1` — for project-level drill-down (use Import mode or limit row count in DirectQuery).
4. **Incremental refresh** (recommended for snapshots):
   - Set the partition key to `snapshot_date`.
   - Configure a rolling retention window (e.g., 24 months of snapshots).
   - Set the refresh cadence to daily to align with the snapshot job.
5. Build visuals: monthly forecast waterfall, committed vs. expected vs. best/worst bands, and stage-bucket breakdowns.

**REST API path (alternative):**

Use Power BI Web connector pointed at `/forecast/pipeline` or `/forecast/snapshot` endpoints.

### Validation and Backtest

- **Automated tests**: Run `pytest tests/test_pipeline_forecast_service.py` to validate stage bucketing, contract-value fallback, window clamping, and band monotonicity against live data.
- **SQL validation script**: Run `python scripts/validate_forecast_sql.py` for a quick pass/fail check of all forecast SQL invariants.
- **Backtest script**: Run `python scripts/forecast_backtest.py` to compare historical snapshots against invoiced actuals. Outputs a markdown report with WAPE, bias, coverage rate, and calibration recommendations. Requires accumulated snapshot history to produce meaningful results.

### Monday Push — Unchanged

The existing webhook → rehydrate → analyse → Monday push flow in `queue_worker.py`, `pipeline.py`, and `monday_update_service.py` is completely unaffected by the forecast layer. Forecast artifacts are a parallel, read-only output path.

