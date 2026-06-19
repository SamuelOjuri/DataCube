## Plan: Good Customer Profile Ranking

Build a deterministic, auditable customer-ranking pipeline that mirrors the PDF V3 league table. The core recommendation is to anchor the ranking at the invoiced CRM Customer grain from subitems, enrich it with CRM/Monday, Finance, Xero, and Strategic Engagement sources, calculate the eight PDF metric grades with fixed scoring bands, and expose the result through Supabase views plus a small refresh/export service.

**Steps**
1. Discovery and live-schema audit
   - Confirm live Supabase columns before migration using `information_schema.columns` for `projects`, `subitems`, and `hidden_items`.
   - Record coverage for current fields needed by the score: `subitems.account`, `subitems.product_type`, `subitems.final_delivery_date`, `subitems.invoice_date`, `subitems.amount_invoiced`, `subitems.date_design_completed`, `projects.account`, `projects.gestation_period`, `projects.pipeline_stage`, and `projects.date_created`.
   - Confirm fiscal-year start/date rules for FY26 YTD reporting and store them in config/env so April 2026 reports are reproducible.

2. CRM Customer sync foundation depends on step 1
   - Fix the repo schema mismatch by adding missing `hidden_items.invoice_number` and `hidden_items.amount_invoiced` to the `hidden_items` table definition, keeping `idx_hidden_items_invoice_number`.
   - Add `subitems.customer TEXT` as the ranking customer grain.
   - Add `subitems.call_off TEXT` and/or `subitems.is_call_off TEXT` for the rework-rate exclusion rule.
   - Optionally add `hidden_items.final_delivery_date DATE` for direct hidden-item auditability, even though `subitems.final_delivery_date` already mirrors it.
   - Add indexes on `subitems(customer)`, `subitems(customer, final_delivery_date)`, and `subitems(customer, product_type)`.

3. Monday mapping and transform changes depends on step 2
   - Add `SUBITEM_COLUMNS['customer'] = 'lookup_mkkdwvs1'`; this is the subitem Customer mirror from hidden-items `dup__of_accounts_mkkd7bew`.
   - Add `SUBITEM_COLUMNS['call_off'] = 'mirror41__1'` and consider `SUBITEM_COLUMNS['is_call_off'] = 'status_12__1'` if status is more reliable than the text mirror.
   - Add `HIDDEN_ITEMS_COLUMNS['final_delivery_date'] = 'date01__1'` and `HIDDEN_ITEMS_COLUMNS['call_off'] = 'text210__1'` if hidden-item audit/backfill needs direct source fields.
   - Update `SyncService._transform_for_subitems_table` to persist `customer` and call-off fields.
   - Update `SyncService._transform_for_hidden_table` if hidden direct fields are added.
   - Check webhook handling so column-change events for these new fields use the board-scoped config mapping and do not silently ignore updates.

4. Backfill and verify synced customer data depends on step 3
   - Run hidden/subitem rehydrate after deploying the schema and config changes.
   - Validate customer coverage: total subitems, populated customers, distinct customers, and rows with `account` populated but `customer` missing.
   - Compare a small Monday sample against Supabase rows to verify `lookup_mkkdwvs1` is storing display names rather than linked IDs.

5. External/reference data model parallel with steps 2-4
   - Add `customer_aliases` to reconcile CRM Customer names, project Account names, Finance customer names, and Xero contact names.
   - Add `customer_strategic_engagement` for the attached Strategic Engagement spreadsheet, with customer, numeric engagement value, grade/label, source file, effective date, and updated timestamp.
   - Add `customer_margin_actuals` for Finance net margin by customer and period, explicitly excluding consultancy orders in the source/import rules.
   - Add `xero_invoice_payments` for invoice number, customer/contact, invoice date, due date, payment date, invoice amount, and days to pay.
   - Add `ingest_audit_log` for all manual/automated imports.

6. External ingestion scripts parallel with step 5 after tables exist
   - Add `openpyxl` to requirements for `.xlsx` imports.
   - Create `scripts/ingest_strategic_engagement.py` for `docs/Strategic Engagement.xlsx` with dry-run validation and upsert by customer/effective date.
   - Create `scripts/ingest_finance_margins.py` for Finance CSV/XLSX exports, with validation and alias reconciliation.
   - Create `scripts/ingest_xero_payment_timeliness.py` for Xero CSV first, then leave a seam for API ingestion later.
   - Create `scripts/reconcile_customer_names.py` to report orphan names and populate/maintain aliases before scoring.

7. Metric calculation views depends on steps 4-6
   - Create `vw_good_customer_profile_metric_inputs_v1` with one row per customer/report period.
   - Average revenue/month: group by `subitems.customer`, filter FY26 YTD using `final_delivery_date`, sum invoice revenue, divide by elapsed FY months.
   - Conversion rate: use last 3 years of `projects`, joined through `customer_aliases` from customer to account; compute won projects over total projects.
   - Gestation period: use previous 2 years of delivered/won projects, joined through aliases; convert average `gestation_period` days to months.
   - Net margin: use latest FY/YTD Finance margin extract.
   - Payment timeliness: average Xero days from invoice date to payment date by customer.
   - Rework rate: count design rows/revisions per project over the previous 12 months, excluding rows where Call Off is populated/statused as Call Off.
   - Strategic engagement: use the strategic engagement reference score.
   - Product mix: compute non-combustible revenue share from subitems revenue and product type classification.

8. PDF V3 scoring view depends on step 7
   - Create `vw_good_customer_profile_scores_v1` to assign each metric a grade, weighted points, raw value, and source status.
   - Use the PDF weights exactly: average revenue/month 17, conversion rate 15, gestation 15, net margin 18, payment timeliness 10, rework 8, strategic engagement 5, product mix 12.
   - Use the PDF total grade thresholds: A >= 85, B >= 75, C >= 65, D > 55, E <= 55.
   - Treat missing source metrics as `No Score` with zero weighted points, and expose coverage flags so the business can separate poor performance from missing data.
   - Create `mv_good_customer_profile_rankings_v1` for fast Power BI/API reads, sorted by total score and optionally filtered to top 30 by YTD revenue.

9. Product mix classification depends on step 7
   - Define a reusable non-combustible product set in config or a reference SQL table.
   - Initial values should include Rockwool/HardRock and ROCKDeck families; reconcile whether the existing CRM formula list also includes T3+, RoofBlock G1T3+, and Ready T3+ for this report.
   - Add validation showing each product type’s classified revenue and unknown/unclassified revenue.

10. Service, API, and exports depends on step 8
   - Create `CustomerProfileService` to refresh the materialized ranking view, fetch a customer profile, and return league-table rows.
   - Add a read endpoint for one customer and a league-table endpoint; optionally add a refresh endpoint guarded for service use.
   - Add `scripts/refresh_good_customer_profiles.py` to refresh inputs/views and optionally export CSV/Excel to `outputs/customer_profiles/`.
   - Add the customer-profile materialized view refresh to the existing analytics refresh function or scheduler flow.

11. Tests and validation depends on steps 2-10
   - Add unit tests for each PDF band boundary, especially lower-is-better categories: gestation, payment timeliness, and rework.
   - Add tests that total score equals the sum of weighted category points and maps to the correct total grade.
   - Add integration-style tests with a tiny synthetic dataset covering `No Score`, alias matching, product mix, and call-off exclusion.
   - Add SQL validation queries for source coverage, top-30 revenue selection, score distribution, and comparison against the attached PDF sample accounts where source data exists.

12. Rollout depends on all previous steps
   - Deploy schema changes to Supabase first.
   - Rehydrate hidden/subitems to populate `customer` and call-off fields.
   - Import Strategic Engagement, Finance margins, and Xero payment files in dry-run mode, then live mode.
   - Run customer-name reconciliation and resolve high-value orphan names.
   - Refresh rankings and compare the first report against the April/May PDF manually for at least the top 30 customers.
   - Schedule refreshes only after the manual comparison is acceptable.

**Relevant files**
- `src/database/schema/schema.sql` — add customer/call-off columns, external tables, scoring views, materialized ranking view, and fixed hidden invoice columns.
- `src/database/schema/functions.sql` — include customer-profile materialized view refresh in analytics refresh function.
- `src/config.py` — add Monday column mappings, fiscal/report config, product classification constants, and scoring version metadata.
- `src/database/sync_service.py` — persist customer and call-off fields in subitem/hidden transforms.
- `src/webhooks/webhook_server.py` — ensure webhook-driven single-item updates include new mapped fields.
- `scripts/rehydrate_sync.py` — existing backfill path to repopulate new Monday-derived fields after mapping changes.
- `scripts/data_quality_checks.py` — extend with customer/revenue/profile coverage checks.
- `scripts/ingest_strategic_engagement.py` — new spreadsheet import.
- `scripts/ingest_finance_margins.py` — new Finance extract import.
- `scripts/ingest_xero_payment_timeliness.py` — new Xero CSV/API import path.
- `scripts/reconcile_customer_names.py` — new alias/orphan report.
- `scripts/refresh_good_customer_profiles.py` — new ranking refresh/export command.
- `src/services/customer_profile_service.py` — new orchestration/read service.
- `src/api/routes/customer_profile.py` and `src/api/app.py` — new API route and router registration.
- `requirements.txt` — add Excel import dependency.
- `docs/Strategic Engagement.xlsx` — initial strategic engagement source file.
- `app_data/BoardSchema.txt` — source of confirmed Monday IDs such as `lookup_mkkdwvs1` and `mirror41__1`.
- `tests/test_good_customer_profile_scoring.py` — new deterministic scoring tests.
- `tests/test_good_customer_profile_inputs.py` — new metric input/alias/product-mix tests.

**Verification**
1. Schema verification: `information_schema.columns` confirms new DB columns and external tables exist.
2. Sync verification: after rehydrate, `subitems.customer` has high coverage and distinct names match Monday sample rows.
3. Source verification: strategic engagement, margin, and Xero imports produce audit log rows, zero rejected required columns, and manageable orphan-name reports.
4. Metric verification: inspect `vw_good_customer_profile_metric_inputs_v1` for top revenue customers and compare raw metric values against business-calculated spreadsheet examples.
5. Scoring verification: boundary tests confirm all PDF band thresholds and total grade thresholds.
6. Ranking verification: `mv_good_customer_profile_rankings_v1` returns top 30 by YTD revenue and total score/grade; manually compare a sample against the attached PDF.
7. Regression verification: run existing tests plus the new customer-profile tests; run data-quality script before scheduling refreshes.

**Decisions**
- Ranking grain is CRM/invoiced `customer` from subitems, not project `account`.
- Project-level conversion and gestation remain sourced from `projects`, joined to customer through aliases because the historical PDF method used account-name inclusion.
- Revenue uses `amount_invoiced` with `final_delivery_date` for the PDF revenue/month calculation; any fallback to order value should be explicitly flagged, not silent.
- V1 scoring is deterministic SQL/Python, no LLM, no ML, and no Monday write-back.
- Missing metric sources produce `No Score` and zero weighted points, matching observed PDF behavior.
- Power BI/API should read from materialized ranking output, while raw component views stay available for audit.

**Further Considerations**
1. Finance/Xero source maturity: start with CSV/XLSX imports and audit logs; upgrade to Xero API after ranking logic is accepted.
2. Product mix definition: align business wording “Rockwool and RockDeck” with the broader existing CRM non-combustible formula before final scoring acceptance.
3. Customer aliases: expect this to be the messiest part; make orphan-name reports part of every refresh until name quality stabilizes.
