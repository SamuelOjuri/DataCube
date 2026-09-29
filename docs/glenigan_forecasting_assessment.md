# Glenigan forecasting assessment

Assessment date: 7 September 2026. Scope: repository code, saved local data and public Glenigan information. No live database or authenticated Glenigan API was queried. Benefits below are testable hypotheses, not measured accuracy improvements.

Glenigan is a credible candidate for improving DataCube's forecast timing and forward demand signals. The recommended first experiments are (1) regional/sector activity inputs to monthly enquiry forecasting and (2) project progress inputs to first-invoice timing on confidently matched projects. Conversion, invoice allocation and specification enrichment are subsequent candidates. Several baseline and evaluation issues must be isolated before attributing improvements to the integration.

## Current implementation and integration opportunities

| Component | Current implementation | Integration opportunity |
|---|---|---|
| Project conversion | `src/core/numeric_analyzer.py`, `calculate_conversion_rate()` and `analyze_project()`: time-weighted segment statistics; the primary rate counts closed-invoiced wins over all projects, including unresolved projects, with Laplace smoothing. Segments back off from account/type/category/product to broader cohorts. | Test stage, contractor appointment, project age, activity recency and material suitability as additional predictors. First define whether the target is an order, eventual invoicing or invoicing within a specified horizon. |
| Gestation | `calculate_gestation_baseline()`: weighted median and quantiles, outlier filtering, support-aware confidence, with configurable bias and tail adjustments elsewhere in the engine. | Add external project milestones and changes to expected dates to predict remaining time to a clearly defined event. |
| Base monthly pipeline | `src/database/schema/schema.sql`, `vw_pipeline_forecast_project_v1`: selects one forecast date, weights internal contract/enquiry value by probability and applies probability spreads. | Introduce explicit event dates/distributions through a separately evaluated enriched version. Adding data to an unused column will not change predictions. |
| Invoice smoothing | Same schema, `vw_pipeline_smoothing_score_v1` and `mv_pipeline_smoothed_revenue_monthly_12m_v1`: category/type/product/account weights of 30/10/30/30, mature-cohort shrinkage and allocation proportional to elapsed days. | Test project phase, duration and milestone inputs against first-invoice delay and subsequent invoice amounts. |
| Monthly enquiry forecasting | `notebooks/weighted_enquiry_value_forecast.ipynb`: seasonal, ETS, SARIMAX, Prophet, XGBoost and ensemble candidates with rolling-origin validation. External regressors currently describe working days and holidays. `scripts/powerbi_xgb_script.py` uses internal lags, rolling statistics and seasonality. | Add lagged external construction activity and origin-known schedules through both training and future feature builders. |
| Experimental win/time models | `notebooks/optimized_colab_win_aft_v3.ipynb`: CatBoost classification and accelerated failure time modelling with landmark snapshots and censoring. | A useful experimental route for enriched features; this is not the forecasting inference path observed in `AnalysisService`. Audit reconstructed historical features and time origins before reuse. |
| LLM analysis | `src/core/llm_analyzer.py`, `create_final_analysis()`: copies numerical predictions and permits a rating adjustment. | Supplying Glenigan text to the LLM alone will not change the conversion or gestation predictions in this path. Use structured model features; explanations can reference the same evidence. |

## What the public sources establish

Glenigan describes a REST API for integrating and blending construction intelligence with internal systems. Its public sales-leads product supports project stages, location, sector, dates, values, materials and company information. These product capabilities do not establish which fields or historical versions an individual API subscription exposes. [API product page](https://www.glenigan.com/our-products/construction-leads-api/), [sales-leads product](https://www.glenigan.com/our-products/increased-sales/).

Glenigan also describes project specification intelligence enriched by NBS, including roofing-related materials. Its July 2026 update describes added-to-spec functionality for Glenigan + NBS customers. Treat specification access as an entitlement to verify, rather than an assumed standard API feature. [Specification information](https://www.glenigan.com/why-glenigan/choosing-a-provider/), [product/data update](https://www.glenigan.com/construction-data-updates-smarter-insights/).

Start and completion information appears in Glenigan's published project coverage, but availability, precision, update timestamps and revision history through the purchased API require confirmation. [Example project coverage](https://www.glenigan.com/office-construction-set-to-occupy-a-prime-space-in-2026/).

No authenticated response schema was available for this assessment. The data concepts below are proposed requirements, not verified API field names. Postponement/cancellation indicators, explicit stage history, planning references, package milestones and historical vintages particularly need verification.

## Ranked use cases

**1. Project progress and slippage: highest potential for monthly timing accuracy.**

For confidently matched projects, use planning/contract stage, expected site dates, appointment milestones, time in stage and observed date revisions. A project whose expected site start moves out should have its invoice timing reassessed even when its internal category and account remain unchanged. A dormant project and an actively progressing project should not receive identical timing simply because they share a historical segment.

Model remaining days from the forecast origin to first invoice, using known design/order events as anchors. Learn the relationship between construction milestones and Tapered Plus delivery/invoicing from internal outcomes: construction start is not a roofing delivery or invoice date. Test a small regularised model or residual correction before a more complex survival model. Keep unresolved cases censored and distinguish delay from loss. Measure first-invoice date MAE, early/late bias, and 1/3/6/12-month revenue error. Start with a matched open-project cohort; this is medium-to-high effort because matching and historical observations are prerequisites.

**2. Regional and sector activity: strongest first experiment without project matching.**

Aggregate relevant new projects, approvals, tender movements and awards by month and the geographies/categories served by DataCube. Test a compact set of lagged counts and value measures against monthly enquiry count and weighted enquiry value. Learn lag lengths from training periods instead of assuming planning activity immediately produces orders.

Extend `build_monthly_exog()` and the relevant forecasters in the weighted enquiry notebook; update the Power BI feature matrix and recursive feature-row builder if that model is used for delivery. Reconcile sector and region taxonomies and cap the influence of very large schemes. Check exposure to refurbishment and smaller work rather than letting unrelated infrastructure activity dominate the signal.

Future calendar variables are known; future realised approvals and awards are not. At each backtest origin, use only available lags, schedules already published then, or separately forecast external inputs. Direct models for individual horizons are another option. Subscription scope must provide a consistent market sample; a personalised or changing lead feed can create artificial demand trends. Effort is medium, conditional on historical data availability.

**3. Win probability and account/contractor relationships: potentially high value, with target-definition work first.**

Combine project progress with whether a known customer is the appointed contractor, whether a relevant package remains available and, if licensed, product specification fit. A main contract award can improve timing evidence while reducing Tapered Plus's chance of winning through the currently quoted customer. Contractor changes should therefore be features, not automatic probability increases.

Add enrichment to `ProjectFeatures` and `AnalysisService._to_project_features()` and train/calibrate a model against a clearly defined internal outcome. Avoid multiplying the existing probability by an arbitrary external confidence score. Preserve recorded internal outcomes; external awards do not imply a Tapered Plus win. Evaluate Brier score, log loss, calibration and aggregate expected-value bias; report PR-AUC as a secondary ranking measure. Use unresolved projects appropriately for a horizon target or survival framework rather than treating them as established losses.

**4. Invoice phasing and outstanding revenue: promising after timing semantics are corrected.**

Test whether external duration, phasing and milestone information improves the current day-weighted allocation. Learn distributions of first-invoice delay and subsequent invoice shares using `subitems.invoice_date` and `amount_invoiced`, supplemented by internal delivery/order dates. Whole-site construction duration is only a covariate; it is not the correct invoice spread by definition.

For existing orders, forecast remaining uninvoiced value as of the origin. Include projects ordered in previous months when they still have future invoicing. Retain the current smoothing model as the comparator and use allocation amounts, not repeated project expected value, for monthly totals. Evaluate monthly WAPE and bias, especially across the committed backlog. Effort is high because both target and allocation logic need work.

**5. Specification, scope and missing-data enrichment: selective supporting value.**

Use externally sourced sector, funding, project type and relevant material information to improve sparse segments and identify unsuitable scope. Internal fields include area, deck type, U-value and thickness at subitem level, so test external suitability against the actual product/quote details. Confirm that usable specification data is in the API package.

Glenigan's whole-project construction value must not replace Tapered Plus's quotation or order value. At most, test it as a size covariate or use a separately validated package-value model for missing quotations. Preserve source provenance and confidence when reconciling conflicting classifications. Effort is medium-to-high and likely secondary to timing/activity inputs.

**6. Demand from projects not yet in Monday: a later extension.**

Model the chance of a relevant external project generating a Tapered Plus enquiry, then its expected conversion, package value and timing. This could improve longer-horizon total-demand forecasts. It requires historical acquisition/conversion evidence and deduplication against existing enquiries and against the enquiry time-series forecast. External lead value cannot simply be added to the current pipeline. Treat this as a new forecast component with its own validation, not a quick accuracy adjustment.

## Local data readiness

The saved export `data/processed/enquiry_training_raw_20260421_132507.csv` contains 8,429 rows dated 4 January 2021 through 21 April 2026. This is a historical local extract, not a current production profile.

| Field | Observed readiness |
|---|---|
| Project name | 8,424 nonempty values; identity accuracy not assessed. |
| Account | 8,348 values after excluding common missing-value tokens; approximately 99.0%. |
| ZIP/postcode field | 5,711 values after excluding common missing-value tokens; 5,632 are only one or two letters. No trimmed value matched a basic full UK postcode format check. These look predominantly like area codes, not unique site locators. |
| Funding | 106 values after excluding common missing-value tokens; approximately 1.3%. |

Nonempty strings and postcode-shaped strings are not proof of valid matches. No Glenigan match rate can be estimated from this file alone. Use delivery addresses in `subitems` where usable, richer source records, planning references if obtainable, and project/company names. Area code plus name alone can be ambiguous. Resolve at construction-project and phase level: several internal enquiries can concern one external project, and different schemes can share an address.

A saved `outputs/enquiry_value_forecast/overall_best_model_summary.csv` reports six-step backtest MAE of about 23.6 enquiries and £375,718 weighted enquiry value, with 132 predictions per target. These are historical artifact metrics, not rerun or verified production results; their target construction and evaluation vintage must be fixed before comparison with a Glenigan experiment.

## Baseline issues to isolate

1. **Gestation origin mismatch.** `src/core/data_processor.py:540` and `src/database/sync_service.py:1426` calculate gestation from first design to first invoice. The forecast SQL at `src/database/schema/schema.sql:1338` prioritises order date, then adds gestation to enquiry creation date. Separate order, first-invoice and subsequent-invoice targets and use consistent origins. Do not assume adding an external start date to `expected_start_date` will affect forecasts: it currently has lower precedence than model gestation.
2. **Conversion is not an explicit eventual-win probability.** The primary numeric rate at `src/core/numeric_analyzer.py:1952` includes unresolved projects in its denominator and counts invoiced wins. Cohort maturity can affect it. SQL also uses committed/lost overrides and gives model estimates precedence over Monday probabilities. Evaluate a corrected internal baseline alongside the enriched model.
3. **The current backtest misses some errors.** `scripts/forecast_backtest.py:41` starts from invoice actuals, filters by minimum value (default £1), joins matching project-month forecasts and drops missing snapshots. Thus forecast-only rows and unmatched actuals do not contribute to the main error calculation. Build a complete forecast/actual population with explicit zero values and fixed forecast origins; report missing-history coverage separately. Score smoothing snapshots for invoice revenue instead of only single-month base snapshots.
4. **Weighted enquiry history uses latest analysis.** `scripts/export_forecast_data.py:34` selects the latest project conversion estimate without a historical cutoff. Backtests of operationally available weighted value need probabilities and project values as known at each origin, or a clearly defined retrospective target. Today's enriched data must not silently rewrite a supposedly historical forecast experiment.
5. **Smoothing window boundaries need separate validation.** The SQL filters projects by base forecast month before allocation, potentially excluding old orders with future invoices. It also clips the allocation end at the 12-month horizon and divides by the clipped duration, compressing a longer allocation into the visible window. Define outside-horizon value and backlog treatment before testing longer external project durations.
6. **Existing bands are not fully calibrated uncertainty.** Base best/worst values use heuristic probability spreads. Glenigan revisions could help model timing uncertainty, but interval coverage must be measured. Include value, timing and common sector/contractor shocks where supported rather than assuming project risks are independent.

## Proposed integration and proof of value

Use a scheduled ingestion job into separate, versioned external tables, then a reviewed/confidence-scored project link and an as-of feature view. Proposed entities are `glenigan_project_versions`, `project_external_links` and `glenigan_market_monthly_vintages`; these names are design suggestions. Store the vendor identifier, source revision time when available, ingestion time, raw payload/version, match method/confidence and model version. Keep internal Monday identifiers and operational outcomes authoritative.

Do not call the external API inside each forecast request or project webhook. Use bounded retries, pagination and incremental updates where supported, then refresh features before affected analyses and analytics snapshots. Retain a usable internal baseline when data is absent, stale or unmatched. Versioned history is essential: a final completion date retrieved today is not a feature known last year.

Before implementation, obtain the vendor's API schema and a representative sample covering DataCube's sectors and project sizes. Confirm stable identifiers, matching fields, stage/date semantics, historical versions and publication timestamps, update/deletion handling, query limits, regional coverage, specification entitlement, and permitted storage/model-training/derived-output use. Public marketing does not answer these questions.

Run the experiment in three arms: current baseline, corrected internal baseline, and the same corrected baseline plus Glenigan. Use rolling chronological origins at 1/3/6/12 months, train-only preprocessing and calibration, and grouped handling of related projects so the same construction scheme does not contaminate train/holdout evaluation. Freeze both internal and external information at each origin, including only outcomes available then for training. If historical vintages cannot be supplied, begin prospective snapshots and defer any retrospective uplift claim.

Report monthly WAPE/MAE and bias, date error, probability calibration and interval coverage by stage, category, size, horizon and match status. Include uncertainty across forecast origins and the effect of unmatched projects on the whole portfolio. Match precision, coverage weighted by forecast value, data freshness and sample-selection stability are first-class results.

Choose the minimum commercially meaningful gain before inspecting results; do not adopt an arbitrary promised uplift. Promote enriched forecasts only when the incremental benefit over the corrected baseline is stable, materially useful and does not create unacceptable errors in important cohorts. The initial deliverable should be a parallel comparison report; production model changes follow demonstrated benefit.
