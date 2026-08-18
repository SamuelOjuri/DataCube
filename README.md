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

## Pipeline Forecast and Smoothing Layer

DataCube produces a 12-month rolling sales pipeline forecast that sits alongside the existing Monday push flow. It also exposes a smoothing adoption layer that models invoice timing after value has entered the forecast. Smoothing is additive and backward-compatible: it does not replace conversion probability, gestation analysis, Monday push behavior, or the existing `/forecast/pipeline` contract.

### Forecast Artifacts

| Artifact | Type | Purpose |
|---|---|---|
| `vw_pipeline_forecast_project_v1` | View | Real-time project-level forecasts with stage bucketing, probability bands, and contract-value fallback logic |
| `mv_pipeline_forecast_monthly_12m_v1` | Materialized view | Pre-aggregated monthly totals for the next 12 months, grouped by stage bucket. Refreshed every 30 minutes |
| `pipeline_forecast_snapshot` | Table | Daily point-in-time snapshots of the full project forecast. Used for drift tracking and historical trend analysis |
| `vw_invoice_smoothing_training_v1` | View | Invoice timing training rows built from project first/last invoice dates |
| `mv_invoice_smoothing_signal_v1` | Materialized view | Empirical-Bayes smoothing signals by category, type, product, account, and global fallback |
| `vw_pipeline_smoothing_score_v1` | View | Project-level smoothing scores using Category 30%, Type 10%, Product 30%, Account 30% weights |
| `mv_pipeline_smoothed_revenue_monthly_12m_v1` | Materialized view | Day-weighted monthly allocation of forecast expected value into smoothed and unsmoothed components |
| `pipeline_smoothing_forecast_snapshot` | Table | Daily project-month smoothing snapshot rows. One project can allocate value across multiple months |

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

### Smoothing Data Contract

The smoothing layer predicts invoice timing tendency, not win probability. It uses project invoice ranges rolled up from subitem invoice dates and recomputes live smoothing signals for the current `as_of_date`.

- **Smoothing definition** — `last_date_invoiced > first_date_invoiced`.
- **Mature cohort** — `first_date_invoiced <= as_of_date - 180 days`.
- **Shrinkage** — group rates and spreads shrink toward the live global mature cohort with `k = 20`.
- **Scoring weights** — Category 30%, Type 10%, Product 30%, Account 30%.
- **Risk bands** — `Very High >= 0.55`, `High >= 0.45`, `Moderate >= 0.35`, otherwise `Low`.
- **Snapshot totals** — smoothing snapshot rows are project-month allocations. `expected_value` is repeated on each project-month row for context, so use `allocated_expected_value` or API `allocated_monthly_value` for additive totals.

### Refresh Cadence

| Job | Schedule | Description |
|---|---|---|
| Materialized view refresh | Every 30 minutes | Runs `refresh_analytics_views()`, including smoothing signal refresh before smoothed monthly allocation refresh |
| Daily forecast snapshot | Cron (default 03:10 UTC) | Inserts base and smoothing snapshot sets, then cleans up expired rows |

The snapshot cron time and retention window are configurable via environment variables (see below).

### Environment Variables

| Variable | Default | Description |
|---|---|---|
| `FORECAST_SNAPSHOT_RETENTION_DAYS` | `730` | Number of days to retain base and smoothing snapshot rows before cleanup |
| `FORECAST_SNAPSHOT_CRON_HOUR_UTC` | `3` | Hour (UTC) at which the daily snapshot job runs |
| `FORECAST_SNAPSHOT_CRON_MINUTE_UTC` | `10` | Minute at which the daily snapshot job runs |

### API Endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/forecast/pipeline` | GET | Monthly 12-month forecast from the materialized view. Supports `months`, `as_of_month`, and `stage_bucket` query parameters |
| `/forecast/snapshot` | GET | Snapshot data with pagination. Supports `snapshot_date`, `project_id`, `stage_bucket`, `offset`, and `limit` query parameters |
| `/forecast/smoothing/projects` | GET | Project-level smoothing scores and explanation fields. Supports risk, stage, project, and text filters plus pagination |
| `/forecast/smoothing/monthly` | GET | Current smoothed monthly allocation totals. Supports `months`, `as_of_month`, and `stage_bucket` query parameters |
| `/forecast/smoothing/snapshot` | GET | Paginated project-month smoothing snapshot rows. Row-level `expected_value` repeats the original project value for context |
| `/forecast/smoothing/snapshot/totals` | GET | Full filtered smoothing snapshot totals independent of pagination. Use `allocated_monthly_value` as the additive forecast total |

### Power BI Dataset Setup

Power BI can consume forecast data via **direct database connection** (recommended) or the REST API.

**Direct Database Connection (recommended):**

1. In Power BI Desktop, choose **Get Data → PostgreSQL database**.
2. Enter the Supabase connection host and database credentials.
3. Import or DirectQuery the following sources:
   - `pipeline_forecast_snapshot` — for historical trend dashboards. Use `snapshot_date` as the incremental refresh partition key.
   - `mv_pipeline_forecast_monthly_12m_v1` — for current-month pipeline summary visuals.
   - `vw_pipeline_forecast_project_v1` — for project-level drill-down (use Import mode or limit row count in DirectQuery).
   - `pipeline_smoothing_forecast_snapshot` — for historical smoothing allocation snapshots. Use `snapshot_date` as the incremental refresh partition key and `allocated_expected_value` for additive totals.
   - `mv_pipeline_smoothed_revenue_monthly_12m_v1` — for current smoothed monthly revenue allocation.
   - `vw_pipeline_smoothing_score_v1` — for project-level smoothing explanation fields and risk bands.
4. **Incremental refresh** (recommended for snapshots):
   - Set the partition key to `snapshot_date`.
   - Configure a rolling retention window (e.g., 24 months of snapshots).
   - Set the refresh cadence to daily to align with the snapshot job.
5. Build visuals: monthly forecast waterfall, committed vs. expected vs. best/worst bands, and stage-bucket breakdowns.

**REST API path (alternative):**

Use Power BI Web connector pointed at `/forecast/pipeline`, `/forecast/snapshot`, or the `/forecast/smoothing/*` endpoints. For smoothing snapshot reporting, use `allocated_monthly_value` as the additive total; `expected_value` is explanatory on project-month rows and may repeat for projects allocated across multiple months.

### Live Segmented Weighted-Enquiry Forecast

The segmented report uses this refresh path:

`Supabase raw tables -> SQL allocation/monthly views -> Power BI PostgreSQL import -> embedded Python/XGBoost -> report`

- Power BI reads `public.vw_weighted_enquiry_leaf_monthly_v1`; the audit table reads `public.vw_weighted_enquiry_project_leaf_allocation_v1`.
- The embedded [powerbi_segmented_xgb_script.py](scripts/powerbi_segmented_xgb_script.py) produces the eight-leaf production forecast and model diagnostics. The aggregate benchmark remains diagnostic-only.
- Use Python 3.11.8 with the exact packages in [requirements-powerbi.txt](requirements-powerbi.txt). A missing or mismatched XGBoost installation must fail refresh.
- Store PostgreSQL credentials in Power BI Data Source Settings or the on-premises data gateway. Never put credentials in the PBIX updater, M source, or repository.
- Run `python scripts/validate_segmented_weighted_enquiry_sql.py` before model refresh. After refreshing Desktop, export acceptance rows with `update_segmented_pbix_model.ps1`, then run `python scripts/validate_segmented_pbix_acceptance.py`.
- Gateway cutover requires a successful scheduled refresh using the pinned Python environment and PostgreSQL connectivity, followed by visual review of all seven production pages.
- Retain `Forward-Looking-Monthly-Outlook-Segmented.pbix` as the CSV-backed rollback report for one release cycle. If SQL, Python, Desktop, gateway, or reconciliation gates fail, keep the live report unapproved and restore the CSV-backed report.
- [build_segmented_weighted_enquiry_reports.py](scripts/build_segmented_weighted_enquiry_reports.py) is an offline audit/export utility over the SQL views. `--raw-extract` invokes the legacy allocator only as a parity oracle; it is not a production input path.

### Validation and Backtest

- **Automated tests**: Run `pytest tests/test_pipeline_forecast_service.py` to validate stage bucketing, contract-value fallback, window clamping, and band monotonicity against live data.
- **SQL validation script**: Run `python scripts/validate_forecast_sql.py` for a quick pass/fail check of all forecast SQL invariants.
- **Smoothing tests**: Run `pytest tests/test_smoothing_phase7_sql.py tests/test_smoothing_forecast_service.py -q` to validate invoice rollups, token expansion, shrinkage formulas, smoothing scores, monthly allocation, and snapshots.
- **Smoothing SQL validation**: Run `python scripts/validate_smoothing_sql.py` for health checks. Use `python scripts/validate_smoothing_sql.py --refresh` manually after deployments or backfills to rebuild smoothing materialized views before validation.
- **Backtest script**: Run `python scripts/forecast_backtest.py` to compare historical snapshots against invoiced actuals. Outputs a markdown report with WAPE, bias, coverage rate, and calibration recommendations. Requires accumulated snapshot history to produce meaningful results.

### Smoothing Runbook

Use these commands after schema deployment, invoice-date backfills, or live-data incidents that may leave materialized views stale:

```powershell
python scripts/backfill_invoice_dates.py --dry-run
python scripts/backfill_invoice_dates.py
python scripts/validate_smoothing_phase7_sql.py
python scripts/validate_smoothing_sql.py --refresh
python scripts/validate_smoothing_sql.py
python -m pytest tests/test_smoothing_phase7_sql.py tests/test_smoothing_forecast_service.py -q
```

The normal production scheduler refreshes smoothing artifacts through `refresh_analytics_views()`. The `--refresh` validator option is for manual operations, not the primary production scheduler.

### Deployment Order

1. Apply `src/database/schema/schema.sql` so invoice fields, smoothing functions/views, and materialized views exist.
2. Apply `src/database/schema/functions.sql` so maintenance and snapshot functions are current.
3. Run `python scripts/backfill_invoice_dates.py --dry-run`, inspect counts, then run `python scripts/backfill_invoice_dates.py`.
4. Run `python scripts/validate_smoothing_sql.py --refresh` to rebuild smoothing signals, smoothed monthly allocation, and today's smoothing snapshot.
5. Run `python scripts/validate_smoothing_phase7_sql.py`, `python scripts/validate_smoothing_sql.py`, and smoothing pytest checks.
6. Deploy/restart the FastAPI service so APScheduler uses the current maintenance code and API routes.
7. Smoke test `/forecast/smoothing/projects`, `/forecast/smoothing/monthly`, `/forecast/smoothing/snapshot`, and `/forecast/smoothing/snapshot/totals`.

### First Production Validation

The first production validation should verify formula correctness, probability/spread bounds, mature-cohort eligibility, fallback behavior, monthly allocation sum-back, and snapshot totals against live database values. Workbook values are fixture outputs only; compare to workbook metrics only when running a deliberate frozen-date parity check with workbook-style data and `as_of_date = 2026-04-28`.

### Monday Push — Unchanged

The existing webhook → rehydrate → analyse → Monday push flow in `queue_worker.py`, `pipeline.py`, and `monday_update_service.py` is completely unaffected by the forecast layer. Forecast artifacts are a parallel, read-only output path.
