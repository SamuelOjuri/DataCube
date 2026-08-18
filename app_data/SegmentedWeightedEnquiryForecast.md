## Plan: Live Segmented Forecast

Target flow:

**Supabase raw tables → SQL eight-leaf actuals view → Power BI PostgreSQL connection → embedded Python/XGBoost → Power BI report**

### Phase 1: Freeze Current Behaviour

1. Preserve the current generated CSVs as a fixed comparison baseline.

2. Record the required forecasting configuration:
   - Lags: `1, 2, 3, 6, 9, 12`
   - Rolling windows: `3, 6`
   - Cyclic month features
   - Log transformation
   - Recursive prediction
   - Existing XGBoost hyperparameters
   - 24-month seasonal forecast
   - 75/25 blend
   - Fallback below 12 non-zero months
   - Rolling 15-month horizon

3. Preserve the reporting contract:
   - Eight independently forecast leaves
   - Two product pages
   - Four category pages
   - One overall page calculated by summing leaves
   - No separately modelled overall forecast used as the displayed total

### Phase 2: Build the Supabase Layer

4. Add product and category taxonomy tables to `schema.sql`.

   Seed them from `config.py`, including canonical product keys, aliases, category aliases, and reporting-segment assignments.

5. Add SQL normalization functions matching `normalization.py`.

   They must reproduce case normalization, whitespace handling, comma splitting, alias matching, missing values, unmapped identities, and product deduplication.

6. Create `public.vw_weighted_enquiry_project_leaf_allocation_v1`.

   This audit view will:

   - Select the latest analysis result per project.
   - Clamp enquiry values and conversion rates.
   - Prefer positive subitem values for product allocation.
   - Fall back to equal canonical-product allocation.
   - Deduplicate products by canonical identity.
   - Deduplicate categories after final segment mapping.
   - Cross product and category shares.
   - Preserve mapping methods, statuses, and allocation shares.

7. Create `public.vw_weighted_enquiry_leaf_monthly_v1`.

   Its contract will be:

   - `month_start`
   - `product_segment`
   - `category_segment`
   - `actual_weighted_enquiry_value`

   It will return exactly eight rows per completed month from January 2022 onward, including zero-filled leaves.

8. Grant the Power BI database role read-only access to the new views. Do not grant write access to the underlying tables.

9. Start with ordinary views. Introduce materialized views only if refresh profiling shows that the live views are too slow.

### Phase 3: Validate SQL Allocation

10. Add `tests/test_segmented_weighted_enquiry_sql.py` covering:

   - Product alias mapping
   - T3, HardRock, and ROCKDeck classification
   - Repeated and unmapped products
   - Subitem-value precedence
   - Zero-value subitem fallback
   - Category deduplication
   - Conversion-rate clamps
   - Latest-analysis selection
   - Project-to-leaf allocation

11. Add `scripts/validate_segmented_weighted_enquiry_sql.py`.

   It will verify:

   - Allocation shares sum to `1.0`
   - Allocated values reconcile within £0.01
   - Every month contains eight leaves
   - Monthly keys are unique
   - Values are finite and non-negative
   - Leaf totals reconcile to `vw_weighted_enquiry_value_monthly_v1`

12. Compare the SQL allocation and monthly views with the frozen outputs from the existing Python allocator in `segmented_weighted_enquiry_forecast.py`.

   Every classification or allocation mismatch must be resolved before changing the PBIX.

### Phase 4: Embed the Forecasting Script

13. Create `scripts/powerbi_segmented_xgb_script.py`.

   It will accept Power BI’s injected `dataset`, validate the eight-leaf monthly grid, and execute the current segmented forecasting logic without requiring repository imports.

14. Produce three Python outputs:

   - `forecast_report`: production Actual, Bridge, and Forecast rows
   - `segment_model_summary`: model and fallback diagnostics
   - `overall_benchmark_report`: original aggregate model, diagnostic only

15. Keep `overall_benchmark_report` separate and load-disabled. Never union it into `forecast_report` or reference it from production measures.

16. Extend `test_segmented_weighted_enquiry_forecast.py` to prove:

   - Exact feature configuration
   - Rolling features use only prior months
   - Recursive predictions feed later months
   - Log transformation is reversed
   - Sparse fallback works
   - Repeated runs are deterministic
   - Embedded-script output matches the existing service
   - All displayed totals reconcile to the eight leaves

17. Add `requirements-powerbi.txt` with the verified Python 3.11, pandas, NumPy, and XGBoost versions used by Power BI.

   Missing XGBoost should fail refresh rather than silently substitute a different estimator.

### Phase 5: Rewire Power BI

18. Update `update_segmented_pbix_model.ps1`.

   Replace `File.Contents` with:

   - `PostgreSQL.Database`
   - Navigation to `public.vw_weighted_enquiry_leaf_monthly_v1`
   - Explicit input column types
   - `Python.Execute`
   - Selection of `forecast_report`

19. Make the updater accept:

   - Local Analysis Services port
   - Local catalog
   - Supabase server
   - Database name
   - Embedded Python script path

   Credentials must remain in Power BI Data Source Settings or the gateway, not in source control.

20. Preserve the existing semantic table name `vw_weighted_enquiry_value_monthly_oct2027`, measures, columns, and page filters to avoid rebuilding visuals.

21. Keep `build_segmented_pbix_layout.py` focused on page creation and filtering. It should not calculate forecasts.

22. Produce a new derived PBIX. Do not overwrite either the original PBIX or the current CSV-backed segmented PBIX.

### Phase 6: Acceptance and Cutover

23. Refresh the new PBIX in Power BI Desktop and verify:

   - Two product segments
   - Four category segments
   - Eight leaves per month
   - One Bridge row per leaf
   - Fifteen forecast months per leaf
   - Correct history and forecast endpoints

24. Compare Power BI Actual rows with the Supabase monthly view and Power BI forecasts with a local execution of the same embedded script.

25. Verify these monthly identities:

$$
\text{Overall}=\sum_{l=1}^{8}\text{Leaf}_l
$$

$$
\sum \text{Product totals}
=
\sum \text{Category totals}
=
\text{Overall total}
$$

26. Regenerate the rolling backtest and compare WAPE and bias with the frozen reference. Report the direct aggregate model only as a benchmark.

27. Inspect all seven pages for filters, titles, Bridge continuity, dates, currency formatting, and reconciliation.

28. Configure and test gateway refresh with PostgreSQL connectivity and the pinned Python environment.

29. Cut over only after SQL parity, model parity, Desktop refresh, gateway refresh, and report reconciliation pass.

30. Retain the CSV-backed PBIX for one release cycle as the rollback option.

### Phase 7: Documentation

31. Update `README.md` with the new live-refresh architecture, credentials, Python environment, validation process, and rollback procedure.

32. Refactor `build_segmented_weighted_enquiry_reports.py` into an offline validation and audit-export utility reading the new SQL view.

33. Keep the old Python allocator temporarily as a parity oracle, but remove it from the production PBIX refresh path.

The full implementation plan has also been saved to `/memories/session/plan.md`.