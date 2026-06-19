## Plan: Smoothing Adoption Model

Implement the smoothing adoption model as a deterministic SQL-first invoice-timing layer on top of the existing DataCube forecast stack. The model will roll up first/last invoice dates, compute mature-cohort Empirical-Bayes smoothing signals by category/type/product/account, score live pipeline projects with the workbook's 30/10/30/30 weights, and optionally generate a smoothed monthly revenue forecast without replacing the current pipeline forecast.

**Model Contract and Workbook Fixture Notes**
- Source workbook sheets: Historic_Data, Dashboard, Category_Summary, Type_Summary, Category_Type_Summary, Product_Summary, Account_Summary, Future_Scoring, Methodology.
- Smoothing definition: `Invoicing spread (DAYS) > 0`, equivalent to `last_date_invoiced > first_date_invoiced`.
- Mature cohort: `first_date_invoiced <= as_of_date - 180 days`; production `as_of_date` defaults to the scheduler-supplied snapshot/refresh date, with ad-hoc live views allowed to use `CURRENT_DATE`. The workbook date `2026-04-28` and cutoff `2025-10-30` are validation fixture values only.
- Global fallback defaults must be recomputed from the live mature database cohort for each refresh/as-of date. Workbook values such as smoothed probability `0.36795774647887325` and expected spread days `78.394366197183103` are fixture outputs only, not production constants.
- Production must not hard-code workbook row counts, workbook `as_of_date`, workbook cutoff date, workbook global probability, or workbook global spread days. All model inputs and aggregates should come from current database values unless a fixture/backtest explicitly supplies frozen data and an `as_of_date`.
- Shrinkage `k = 20`: shrunk rate = `(mature_smoothed_count + 20 * global_mature_rate) / (mature_project_count + 20)`.
- Expected spread shrinkage: `(mature_group_spread_days_sum + 20 * global_mature_avg_spread) / (mature_project_count + 20)`.
- Future scoring weights: Category 30%, Type 10%, Product 30%, Account 30%.
- Risk bands: `>=0.55` Very High, `>=0.45` High, `>=0.35` Moderate, else Low.
- Confidence bands are mature project counts: High `>=50`, Medium `15-49`, Low `5-14`, Very low `<5`.
- Extra methodology caveat: expected spread around `120+` days should normally trigger smoothing attention even where probability is only moderate.
- Product/account mirrors must be parsed and counted once per historic project when present.

**Steps**

**Phase 1: Invoice Date Rollup Foundation**
1. Add project-level invoice timing fields in `src/database/schema/schema.sql`: `last_date_invoiced DATE` and `invoicing_spread_days`, preferably as a generated stored column from `first_date_invoiced` and `last_date_invoiced` where supported.
2. Add indexes in `src/database/schema/schema.sql` for invoice timing queries, at minimum `(first_date_invoiced)`, `(last_date_invoiced)`, and optionally `(first_date_invoiced, last_date_invoiced)`.
3. In `src/database/sync_service.py`, add a helper that computes per-parent invoice date ranges from subitems: min invoice date, max invoice date, invoice-date count, and spread days. Keep the existing `_rollup_design_invoice_dates_from_subitems` behavior stable or update all call sites carefully.
4. Update `_compute_project_rollups_from_persisted_subitems` and `_refresh_project_order_invoice_rollups` in `src/database/sync_service.py` so persisted rollup refreshes maintain both `first_date_invoiced` and `last_date_invoiced`, not only totals.
5. Update full-sync and delta/rehydrate paths in `src/database/sync_service.py` so subitem/hidden item invoice updates refresh the parent invoice range immediately.
6. Add a one-off backfill script, likely `scripts/backfill_invoice_dates.py`, to recompute first/last invoice dates for all existing projects from `subitems.invoice_date`.
7. Verify this phase independently before any smoothing math: projects with one invoice date must have spread `0`; projects with multiple invoice dates must match min/max/spread from subitems.

**Phase 2: Workbook-Faithful Training and Signal Views**
8. Create `vw_invoice_smoothing_training_v1` in `src/database/schema/schema.sql` over `projects`, with `project_id`, category, type, raw account mirror, raw product mirror, first invoice, last invoice, spread days, `is_smoothed`, and `is_mature`.
9. Make the mature-cohort date configurable for functions/snapshots: scheduled refreshes should pass an explicit `as_of_date` or snapshot date for reproducibility, while ad-hoc live views may default to `CURRENT_DATE`. Validation/backtests can pass `2026-04-28` only when intentionally reproducing workbook fixture outputs.
10. Add smoothing-specific token normalization. Do not rely only on the existing broad `product_key`, because the workbook separates products like `Torch On PIR (Prebonded)` and `Tissue Faced PIR (Prebonded)` that current canonical product keys may collapse together.
11. Create a signal-membership view, e.g. `vw_invoice_smoothing_signal_members_v1`, that emits distinct `(project_id, dimension, group_key, group_display)` rows for category, type, category+type, product token, and account token. Product/account comma-separated mirrors should be split, trimmed, normalized, and counted once per project per group.
12. Create `mv_invoice_smoothing_signal_v1` in `src/database/schema/schema.sql`, grouped by `dimension` and `group_key`, with all workbook columns: all projects, all smoothed %, mature projects, mature smoothed %, shrunk mature smoothed %, mature expected spread days, avg spread if smoothed, median smoothed days, P75 smoothed days, 90+ day rate, 180+ day rate, confidence, and suggested use.
13. Create or expose a global mature signal row/view containing the live database-derived fallback values for the selected `as_of_date`. It should use the workbook Dashboard global mature row only as a fixture-parity check when the workbook data/date are intentionally reproduced.
14. Add category+type signals for reporting and high-priority adoption lists, but do not use category+type in the default Future_Scoring formula unless deliberately adding an enhanced model variant.

**Phase 3: Future Project Scoring View**
15. Add `vw_pipeline_smoothing_score_v1` in `src/database/schema/schema.sql`, joining `vw_pipeline_forecast_project_v1` to category, type, product, and account signals.
16. Implement workbook lookup fallback semantics with live values: if a project signal is missing or blank, use the global mature smoothed probability and global mature expected spread days recomputed from the database for the selected `as_of_date`.
17. Compute `combined_smoothed_probability = category_rate*0.30 + type_rate*0.10 + product_rate*0.30 + account_rate*0.30`.
18. Compute `expected_spread_days = category_spread*0.30 + type_spread*0.10 + product_spread*0.30 + account_spread*0.30`.
19. Emit separate fields for each signal contribution: rates, spread days, mature project counts, and confidence by dimension. This is important for explaining why a project received its smoothing score.
20. Emit `risk_band` exactly from workbook thresholds: Very High, High, Moderate, Low.
21. Emit `workbook_suggested_treatment` exactly from the workbook formula: Very High adopts smoothing by default; High models a smoothing scenario; Moderate flags for commercial review/light smoothing; Low assumes no default smoothing.
22. Emit a second implementation-friendly field such as `adoption_recommendation` or `default_smoothing_recommended` that incorporates the methodology note: expected spread around `120+` days should escalate smoothing treatment even when the probability band is moderate/high. Keep this field separate from `risk_band` so Power BI can show both.
23. Keep this smoothing score separate from `analysis_results.expected_conversion_rate`; conversion probability predicts whether the project wins, while smoothing predicts invoice timing once value is forecast.

**Phase 4: Smoothed Monthly Revenue Forecast**
24. Add `mv_pipeline_smoothed_revenue_monthly_12m_v1` after the project scoring view is validated.
25. Use `vw_pipeline_forecast_project_v1.expected_value` as the first version's value basis, because the workbook lacks revenue weighting and current forecast already handles conversion probability.
26. Split value into unsmoothed and smoothed components: unsmoothed value stays in the base forecast month; smoothed value is phased over `expected_spread_days` from `forecast_date`.
27. Prefer day-weighted calendar-month allocation using `generate_series` and overlap days, rather than equal-month buckets, so a 45-day spread across month-end allocates proportionally.
28. Ensure monthly allocation sums back to project `expected_value` within rounding tolerance.
29. Keep this as a new artifact, not a replacement for `mv_pipeline_forecast_monthly_12m_v1`, so existing Power BI reports and API consumers remain stable.

**Phase 5: Snapshot and Refresh Jobs**
30. Extend `src/database/schema/functions.sql` refresh logic so smoothing materialized views refresh alongside forecast/conversion views where present.
31. Update `src/tasks/postgres_maintenance.py` to include smoothing materialized views in the scheduled refresh list, preserving the existing relation-existence guard pattern.
32. Add a separate snapshot table, e.g. `pipeline_smoothing_forecast_snapshot`, for smoothed monthly revenue rows. Keep it separate from `pipeline_forecast_snapshot` because one project may allocate value across multiple months.
33. Add SQL functions in `src/database/schema/functions.sql` for creating and cleaning smoothing snapshots, mirroring `create_pipeline_forecast_snapshot` and `cleanup_old_pipeline_forecast_snapshots`.
34. Wire smoothing snapshot maintenance into the existing daily forecast maintenance only after the smoothed monthly artifact is validated.

**Phase 6: API Exposure**
35. Extend `src/api/routes/forecast.py` rather than creating a disconnected API namespace, unless the project owner prefers a separate router.
36. Add `GET /forecast/smoothing/projects` to expose project-level smoothing scores with filters for `risk_band`, `stage_bucket`, `project_id`, category, type, product/account, and pagination.
37. Add `GET /forecast/smoothing/monthly` to expose `mv_pipeline_smoothed_revenue_monthly_12m_v1` with `months`, `as_of_month`, and `stage_bucket` filters matching existing forecast endpoint conventions.
38. Add `GET /forecast/smoothing/snapshot` if the separate smoothing snapshot table is implemented.
39. Include totals in API responses: project count, expected value, smoothed expected value, unsmoothed expected value, and allocated monthly value.

**Phase 7: Tests and Validation**
40. Add focused sync tests in `tests/test_sync_service.py` or a new `tests/test_invoice_rollup.py`: single invoice date, multiple invoice dates, null invoice dates, invalid/negative spread prevention, and parent rollup update behavior.
41. Add SQL/integration tests in `tests/test_pipeline_forecast_service.py` or `tests/test_smoothing_forecast_service.py` for relation existence, probability bounds, spread non-negativity, confidence bands, and unknown-signal global fallback.
42. Add formula tests for shrinkage `k=20`, expected spread shrinkage, 30/10/30/30 weighted scoring, and risk band thresholds.
43. Add tests for product/account token expansion so comma-separated mirrors are counted once per project per token.
44. Add allocation tests proving monthly smoothed revenue sums back to project expected value and remains inside the 12-month forecast window.
45. Extend `scripts/validate_forecast_sql.py` or create `scripts/validate_smoothing_sql.py` for operational validation: missing last invoice dates, invalid spreads, stale smoothing materialized views, and monotonic allocation totals.

**Phase 8: Documentation and Deployment**
46. Update `README.md` to describe the smoothing adoption layer as a parallel invoice-timing forecast artifact.
47. Update `docs/automation.md` with smoothing refresh cadence, snapshot behavior, and Power BI sources.
48. Add a short runbook section with manual commands for invoice-date backfill, smoothing view refresh, and smoothing SQL validation.
49. Deployment order: schema migration, invoice-date backfill, smoothing signal view build, scoring view validation, monthly allocation view, API exposure, snapshots, documentation.
50. First production validation should verify formula correctness, bounds, mature-cohort eligibility, and fallback behavior against live database values. Compare against workbook metrics only in an intentional fixture/parity run using frozen workbook-style data and `as_of_date = 2026-04-28`; otherwise differences are expected because production recomputes from current database state.

**Relevant Files**
- `src/database/schema/schema.sql` — add invoice fields, indexes, training/signal/scoring/monthly smoothing views, and optional snapshot table.
- `src/database/schema/functions.sql` — add smoothing refresh/snapshot functions and hook refresh logic.
- `src/database/sync_service.py` — add invoice date range rollups and parent update paths.
- `src/tasks/postgres_maintenance.py` — refresh smoothing materialized views and later run smoothing snapshot maintenance.
- `src/api/routes/forecast.py` — expose project/monthly/snapshot smoothing endpoints under the existing forecast route style.
- `src/core/normalization.py` — reuse text normalization, but add smoothing-specific product token handling that preserves workbook product granularity.
- `scripts/backfill_invoice_dates.py` — one-off invoice date rollup backfill.
- `scripts/validate_smoothing_sql.py` — operational validation for smoothing artifacts.
- `tests/test_sync_service.py` or `tests/test_invoice_rollup.py` — invoice date rollup tests.
- `tests/test_pipeline_forecast_service.py` or `tests/test_smoothing_forecast_service.py` — SQL artifact and formula validation.
- `README.md` and `docs/automation.md` — document the smoothing layer and Power BI consumption path.

**Verification**
1. Run unit tests for rollup helpers and token parsing.
2. Run SQL/integration tests with `SUPABASE_DB_URL` configured.
3. Run the invoice-date backfill in dry-run/count mode first, then validate counts of projects with first/last invoice dates.
4. Rebuild/refresh smoothing signals and verify global mature rate/spread are derived from the current database cohort for the selected `as_of_date`. Compare against workbook values only in a fixture/parity run using the workbook's frozen inputs and `2026-04-28` analysis date.
5. Validate project scoring formula with known workbook examples from `Future_Scoring`.
6. Validate smoothed monthly allocation totals equal project expected values within rounding tolerance.
7. Confirm current `/forecast/pipeline` and `/forecast/snapshot` behavior remains unchanged.
8. Confirm Power BI can consume new smoothing views without requiring changes to existing forecast reports.

**Decisions**
- Keep smoothing separate from conversion/gestation analysis; it predicts invoice timing, not win probability.
- Use workbook risk bands exactly, but add a separate recommendation field for the `120+` expected-spread caveat.
- Preserve workbook product granularity for smoothing instead of relying only on broad canonical `product_key`.
- Use SQL/materialized views for core scoring and allocation; use Python/API only for orchestration and exposure.
- Prefer separate smoothing snapshot artifacts because smoothed revenue can create multiple rows per project/month allocation.

**Excluded From First Pass**
- Revenue-weighted retraining using actual contract value as a model feature. The current workbook explicitly lacks revenue weighting.
- Planned invoice profile, PM/customer behavior beyond account, project duration, and forecast-period seasonality. These are workbook-recommended enhancements for a later version.
- Replacing the current pipeline forecast layer. Smoothing should be additive and backward-compatible.
