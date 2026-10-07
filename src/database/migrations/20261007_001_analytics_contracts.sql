-- Phase 2 candidate surface. PostgreSQL 15+. Apply inside a migration transaction.
-- Additive only: no source rewrites, refreshes, grants, or business certification.
-- Existing invoker views require caller privileges throughout their dependencies.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
CREATE SCHEMA IF NOT EXISTS analytics;
COMMENT ON SCHEMA analytics IS 'Versioned candidate BI contracts; certification and access grants are separate release gates.';

CREATE OR REPLACE VIEW analytics.projects_v1 WITH (security_invoker=true, security_barrier=true) AS
SELECT monday_id, pipeline_stage, nullif(trim(category),'') AS category,
       nullif(trim(type),'') AS type, nullif(trim(account),'') AS account,
       nullif(trim(product_type),'') AS product_type,
       date_created, date_order_received, first_date_designed, first_date_invoiced,
       new_enquiry_value, total_order_value, total_amount_invoiced, gestation_period
FROM public.reportable_projects;

CREATE OR REPLACE VIEW analytics.children_v1 WITH (security_invoker=true, security_barrier=true) AS
SELECT s.monday_id, s.parent_monday_id, s.hidden_item_id, s.reason_for_change,
       s.quote_amount, s.new_enquiry_value, s.amount_invoiced, s.invoice_date,
       CASE WHEN s.reason_for_change='New Enquiry' THEN s.quote_amount ELSE 0 END AS formula_enquiry_value
FROM public.subitems s
JOIN analytics.projects_v1 p ON p.monday_id=s.parent_monday_id;

CREATE OR REPLACE VIEW analytics.child_totals_v1 WITH (security_invoker=true, security_barrier=true) AS
SELECT parent_monday_id, count(*) AS child_count,
       count(amount_invoiced) AS known_invoice_count,
       sum(amount_invoiced) AS child_invoice_amount,
       sum(formula_enquiry_value) AS child_formula_enquiry_amount,
       count(*) FILTER (WHERE hidden_item_id IS NULL) AS missing_source_count
FROM analytics.children_v1 GROUP BY parent_monday_id;

CREATE OR REPLACE VIEW analytics.hidden_values_v1 WITH (security_invoker=true, security_barrier=true) AS
SELECT monday_id, cust_order_value_material, cust_additional_charges, amount_invoiced,
       cust_order_value_material + cust_additional_charges AS complete_input_order_value,
       cust_order_value_material IS NOT NULL AND cust_additional_charges IS NOT NULL AS order_inputs_complete
FROM public.hidden_items;

CREATE OR REPLACE VIEW analytics.latest_analysis_v1 WITH (security_invoker=true, security_barrier=true) AS
SELECT DISTINCT ON (ar.project_id) ar.project_id, ar.id AS analysis_id,
       ar.analysis_timestamp, ar.expected_conversion_rate, ar.expected_gestation_days
FROM public.analysis_results ar JOIN analytics.projects_v1 p ON p.monday_id=ar.project_id
ORDER BY ar.project_id, ar.analysis_timestamp DESC NULLS LAST, ar.id DESC;

CREATE OR REPLACE VIEW analytics.enquiry_monthly_v1 WITH (security_invoker=true, security_barrier=true) AS
SELECT enquiry_month, project_count, actual_enquiry_value, actual_pipeline_value
FROM public.vw_actual_enquiry_monthly_v1;

CREATE OR REPLACE VIEW analytics.bookings_monthly_v1 WITH (security_invoker=true, security_barrier=true) AS
SELECT booking_month, project_count, actual_bookings
FROM public.vw_actual_bookings_monthly_v1;

-- Diagnostic only: the restored deployed view differs from the intended contract.
CREATE OR REPLACE VIEW analytics.revenue_monthly_baseline_v1 WITH (security_invoker=true, security_barrier=true) AS
SELECT revenue_month, project_count, invoice_count, actual_revenue
FROM public.vw_actual_revenue_monthly_v1;

-- Candidate intended invoice population, matching schema.sql/reference.sql.
-- The caller must set the reviewed business timezone in the read transaction.
CREATE OR REPLACE VIEW analytics.invoice_reporting_facts_v1 WITH (security_invoker=true, security_barrier=true) AS
SELECT s.monday_id, s.parent_monday_id, s.invoice_date, s.amount_invoiced,
       p.category, p.type
FROM analytics.children_v1 s JOIN analytics.projects_v1 p ON p.monday_id=s.parent_monday_id
WHERE s.invoice_date IS NOT NULL AND s.amount_invoiced>0
  AND p.pipeline_stage='Won - Closed (Invoiced)'
  AND s.invoice_date < date_trunc('month', CURRENT_DATE)::date;

-- Keep counts additive. Consumers sum counts before dividing, never average rates.
-- Existing historical cohorts intentionally have no upper date bound.
CREATE OR REPLACE VIEW analytics.conversion_cohorts_v1 WITH (security_invoker=true, security_barrier=true) AS
SELECT p.monday_id, p.category, p.type, p.date_created, c.cohort_years,
       1::bigint AS eligible_count,
       CASE WHEN p.pipeline_stage='Won - Closed (Invoiced)' THEN 1 ELSE 0 END::bigint AS win_count,
       CASE WHEN p.pipeline_stage IN ('Won - Closed (Invoiced)','Lost') THEN 1 ELSE 0 END::bigint AS closed_count
FROM analytics.projects_v1 p CROSS JOIN (VALUES (5),(2)) c(cohort_years)
WHERE p.date_created >= CURRENT_DATE - make_interval(years => c.cohort_years);

-- Aggregate-only coverage interface: no lifecycle payloads or maintenance actions.
-- Repeated links are evidence to reconcile, never an instruction to deduplicate.
CREATE OR REPLACE VIEW analytics.coverage_v1 WITH (security_invoker=true, security_barrier=true) AS
SELECT
 (SELECT count(*) FROM analytics.projects_v1) AS reportable_projects,
 (SELECT count(*) FROM analytics.projects_v1 p LEFT JOIN analytics.child_totals_v1 c
    ON c.parent_monday_id=p.monday_id WHERE c.parent_monday_id IS NULL) AS projects_without_children,
 (SELECT count(*) FROM analytics.children_v1 WHERE hidden_item_id IS NULL) AS children_without_source,
 (SELECT count(*) FROM analytics.children_v1 s LEFT JOIN public.hidden_items h
    ON h.monday_id=s.hidden_item_id WHERE s.hidden_item_id IS NOT NULL AND h.monday_id IS NULL) AS unresolved_source_links,
 (SELECT count(*) FROM (SELECT hidden_item_id FROM analytics.children_v1
    WHERE hidden_item_id IS NOT NULL GROUP BY hidden_item_id HAVING count(*)>1) repeated) AS repeated_source_ids,
 (SELECT count(*) FROM analytics.hidden_values_v1 WHERE NOT order_inputs_complete) AS incomplete_order_inputs;

-- Scope revocations to these new objects; never grant broad future-object access.
REVOKE ALL ON SCHEMA analytics FROM PUBLIC;
REVOKE ALL ON analytics.projects_v1, analytics.children_v1, analytics.child_totals_v1,
 analytics.hidden_values_v1, analytics.latest_analysis_v1, analytics.enquiry_monthly_v1,
 analytics.bookings_monthly_v1, analytics.revenue_monthly_baseline_v1,
 analytics.invoice_reporting_facts_v1, analytics.conversion_cohorts_v1, analytics.coverage_v1 FROM PUBLIC;
DO $revoke$
DECLARE role_name text;
BEGIN
 FOR role_name IN SELECT rolname FROM pg_roles WHERE rolname IN ('anon','authenticated','service_role') LOOP
   EXECUTE format('REVOKE ALL ON SCHEMA analytics FROM %I',role_name);
   EXECUTE format('REVOKE ALL ON analytics.projects_v1, analytics.children_v1, analytics.child_totals_v1,
     analytics.hidden_values_v1, analytics.latest_analysis_v1, analytics.enquiry_monthly_v1,
     analytics.bookings_monthly_v1, analytics.revenue_monthly_baseline_v1,
     analytics.invoice_reporting_facts_v1, analytics.conversion_cohorts_v1, analytics.coverage_v1 FROM %I',role_name);
 END LOOP;
END $revoke$;

-- Catalogue documentation.
COMMENT ON VIEW analytics.projects_v1 IS 'one row per approved reportable project';
COMMENT ON COLUMN analytics.projects_v1.monday_id IS 'monday_id: one row per approved reportable project; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.projects_v1.pipeline_stage IS 'pipeline_stage: one row per approved reportable project; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.projects_v1.category IS 'category: one row per approved reportable project; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.projects_v1.type IS 'type: one row per approved reportable project; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.projects_v1.account IS 'account: one row per approved reportable project; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.projects_v1.product_type IS 'product_type: one row per approved reportable project; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.projects_v1.date_created IS 'date_created: one row per approved reportable project; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.projects_v1.date_order_received IS 'date_order_received: one row per approved reportable project; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.projects_v1.first_date_designed IS 'first_date_designed: one row per approved reportable project; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.projects_v1.first_date_invoiced IS 'first_date_invoiced: one row per approved reportable project; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.projects_v1.new_enquiry_value IS 'new_enquiry_value: one row per approved reportable project; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.projects_v1.total_order_value IS 'total_order_value: one row per approved reportable project; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.projects_v1.total_amount_invoiced IS 'total_amount_invoiced: one row per approved reportable project; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.projects_v1.gestation_period IS 'gestation_period: one row per approved reportable project; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON VIEW analytics.children_v1 IS 'one stored child of a reportable parent';
COMMENT ON COLUMN analytics.children_v1.monday_id IS 'monday_id: one stored child of a reportable parent; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.children_v1.parent_monday_id IS 'parent_monday_id: one stored child of a reportable parent; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.children_v1.hidden_item_id IS 'hidden_item_id: one stored child of a reportable parent; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.children_v1.reason_for_change IS 'reason_for_change: one stored child of a reportable parent; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.children_v1.quote_amount IS 'quote_amount: one stored child of a reportable parent; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.children_v1.new_enquiry_value IS 'new_enquiry_value: one stored child of a reportable parent; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.children_v1.amount_invoiced IS 'amount_invoiced: one stored child of a reportable parent; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.children_v1.invoice_date IS 'invoice_date: one stored child of a reportable parent; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.children_v1.formula_enquiry_value IS 'formula_enquiry_value: one stored child of a reportable parent; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON VIEW analytics.child_totals_v1 IS 'one pre-aggregated row per parent; diagnostic stored-child totals';
COMMENT ON COLUMN analytics.child_totals_v1.parent_monday_id IS 'parent_monday_id: one pre-aggregated row per parent; diagnostic stored-child totals; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.child_totals_v1.child_count IS 'child_count: one pre-aggregated row per parent; diagnostic stored-child totals; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.child_totals_v1.known_invoice_count IS 'known_invoice_count: one pre-aggregated row per parent; diagnostic stored-child totals; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.child_totals_v1.child_invoice_amount IS 'child_invoice_amount: one pre-aggregated row per parent; diagnostic stored-child totals; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.child_totals_v1.child_formula_enquiry_amount IS 'child_formula_enquiry_amount: one pre-aggregated row per parent; diagnostic stored-child totals; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.child_totals_v1.missing_source_count IS 'missing_source_count: one pre-aggregated row per parent; diagnostic stored-child totals; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON VIEW analytics.hidden_values_v1 IS 'one hidden-board item; independent of project membership';
COMMENT ON COLUMN analytics.hidden_values_v1.monday_id IS 'monday_id: one hidden-board item; independent of project membership; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.hidden_values_v1.cust_order_value_material IS 'cust_order_value_material: one hidden-board item; independent of project membership; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.hidden_values_v1.cust_additional_charges IS 'cust_additional_charges: one hidden-board item; independent of project membership; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.hidden_values_v1.amount_invoiced IS 'amount_invoiced: one hidden-board item; independent of project membership; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.hidden_values_v1.complete_input_order_value IS 'complete_input_order_value: one hidden-board item; independent of project membership; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.hidden_values_v1.order_inputs_complete IS 'order_inputs_complete: one hidden-board item; independent of project membership; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON VIEW analytics.latest_analysis_v1 IS 'latest analysis per reportable project; timestamp DESC NULLS LAST, id DESC';
COMMENT ON COLUMN analytics.latest_analysis_v1.project_id IS 'project_id: latest analysis per reportable project; timestamp DESC NULLS LAST, id DESC; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.latest_analysis_v1.analysis_id IS 'analysis_id: latest analysis per reportable project; timestamp DESC NULLS LAST, id DESC; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.latest_analysis_v1.analysis_timestamp IS 'analysis_timestamp: latest analysis per reportable project; timestamp DESC NULLS LAST, id DESC; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.latest_analysis_v1.expected_conversion_rate IS 'expected_conversion_rate: latest analysis per reportable project; timestamp DESC NULLS LAST, id DESC; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.latest_analysis_v1.expected_gestation_days IS 'expected_gestation_days: latest analysis per reportable project; timestamp DESC NULLS LAST, id DESC; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON VIEW analytics.enquiry_monthly_v1 IS 'one completed calendar month';
COMMENT ON COLUMN analytics.enquiry_monthly_v1.enquiry_month IS 'enquiry_month: one completed calendar month; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.enquiry_monthly_v1.project_count IS 'project_count: one completed calendar month; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.enquiry_monthly_v1.actual_enquiry_value IS 'actual_enquiry_value: one completed calendar month; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.enquiry_monthly_v1.actual_pipeline_value IS 'actual_pipeline_value: one completed calendar month; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON VIEW analytics.bookings_monthly_v1 IS 'one completed calendar month';
COMMENT ON COLUMN analytics.bookings_monthly_v1.booking_month IS 'booking_month: one completed calendar month; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.bookings_monthly_v1.project_count IS 'project_count: one completed calendar month; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.bookings_monthly_v1.actual_bookings IS 'actual_bookings: one completed calendar month; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON VIEW analytics.revenue_monthly_baseline_v1 IS 'Diagnostic baseline only. Deployed TEST lacks reportable-parent and stage restrictions; never use as intended monthly invoice metric.';
COMMENT ON COLUMN analytics.revenue_monthly_baseline_v1.revenue_month IS 'revenue_month: one month of deployed revenue; diagnostic only, population mismatch unresolved; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.revenue_monthly_baseline_v1.project_count IS 'project_count: one month of deployed revenue; diagnostic only, population mismatch unresolved; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.revenue_monthly_baseline_v1.invoice_count IS 'invoice_count: one month of deployed revenue; diagnostic only, population mismatch unresolved; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.revenue_monthly_baseline_v1.actual_revenue IS 'actual_revenue: one month of deployed revenue; diagnostic only, population mismatch unresolved; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON VIEW analytics.invoice_reporting_facts_v1 IS 'positive dated child invoice for a currently closed-invoiced parent in a completed month';
COMMENT ON COLUMN analytics.invoice_reporting_facts_v1.monday_id IS 'monday_id: positive dated child invoice for a currently closed-invoiced parent in a completed month; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.invoice_reporting_facts_v1.parent_monday_id IS 'parent_monday_id: positive dated child invoice for a currently closed-invoiced parent in a completed month; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.invoice_reporting_facts_v1.invoice_date IS 'invoice_date: positive dated child invoice for a currently closed-invoiced parent in a completed month; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.invoice_reporting_facts_v1.amount_invoiced IS 'amount_invoiced: positive dated child invoice for a currently closed-invoiced parent in a completed month; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.invoice_reporting_facts_v1.category IS 'category: positive dated child invoice for a currently closed-invoiced parent in a completed month; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.invoice_reporting_facts_v1.type IS 'type: positive dated child invoice for a currently closed-invoiced parent in a completed month; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON VIEW analytics.conversion_cohorts_v1 IS 'one project per cohort; selecting exactly one cohort is mandatory';
COMMENT ON COLUMN analytics.conversion_cohorts_v1.monday_id IS 'monday_id: one project per cohort; selecting exactly one cohort is mandatory; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.conversion_cohorts_v1.category IS 'category: one project per cohort; selecting exactly one cohort is mandatory; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.conversion_cohorts_v1.type IS 'type: one project per cohort; selecting exactly one cohort is mandatory; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.conversion_cohorts_v1.date_created IS 'date_created: one project per cohort; selecting exactly one cohort is mandatory; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.conversion_cohorts_v1.cohort_years IS 'cohort_years: one project per cohort; selecting exactly one cohort is mandatory; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.conversion_cohorts_v1.eligible_count IS 'eligible_count: one project per cohort; selecting exactly one cohort is mandatory; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.conversion_cohorts_v1.win_count IS 'win_count: one project per cohort; selecting exactly one cohort is mandatory; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.conversion_cohorts_v1.closed_count IS 'closed_count: one project per cohort; selecting exactly one cohort is mandatory; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON VIEW analytics.coverage_v1 IS 'one aggregate coverage row';
COMMENT ON COLUMN analytics.coverage_v1.reportable_projects IS 'reportable_projects: one aggregate coverage row; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.coverage_v1.projects_without_children IS 'projects_without_children: one aggregate coverage row; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.coverage_v1.children_without_source IS 'children_without_source: one aggregate coverage row; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.coverage_v1.unresolved_source_links IS 'unresolved_source_links: one aggregate coverage row; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.coverage_v1.repeated_source_ids IS 'repeated_source_ids: one aggregate coverage row; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
COMMENT ON COLUMN analytics.coverage_v1.incomplete_order_inputs IS 'incomplete_order_inputs: one aggregate coverage row; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules.';
