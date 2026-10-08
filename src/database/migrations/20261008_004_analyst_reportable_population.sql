-- Catalogue 1.1.0: retained reportable parents, independent of API lifecycle state.
-- Preserve migration 001 and sealed references as the historical 1.0.0 contract.
-- Apply after 001; no source rows, classifications or snapshots are changed.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

CREATE OR REPLACE VIEW analytics.invoice_reporting_facts_v1 WITH (security_invoker=true, security_barrier=true) AS
SELECT s.monday_id, s.parent_monday_id, s.invoice_date, s.amount_invoiced,
       p.category, p.type
FROM analytics.children_v1 s JOIN analytics.projects_v1 p ON p.monday_id=s.parent_monday_id
WHERE s.invoice_date IS NOT NULL AND s.amount_invoiced>0
  AND s.invoice_date < date_trunc('month', CURRENT_DATE)::date;

COMMENT ON VIEW analytics.invoice_reporting_facts_v1 IS
    'Catalogue 1.1.0: positive dated child invoices for retained reportable parents in completed months; no parent business-stage or API lifecycle filter.';
COMMENT ON COLUMN analytics.invoice_reporting_facts_v1.monday_id IS 'Unique persisted child invoice ID; reportable-parent population.';
COMMENT ON COLUMN analytics.invoice_reporting_facts_v1.parent_monday_id IS 'Retained reportable parent ID, including genuine archived projects.';
COMMENT ON COLUMN analytics.invoice_reporting_facts_v1.invoice_date IS 'Source invoice date in a completed month in the reviewed business timezone.';
COMMENT ON COLUMN analytics.invoice_reporting_facts_v1.amount_invoiced IS 'Positive source invoice amount; signed source totals remain separate.';
COMMENT ON COLUMN analytics.invoice_reporting_facts_v1.category IS 'Parent category, independent of business stage and API lifecycle state.';
COMMENT ON COLUMN analytics.invoice_reporting_facts_v1.type IS 'Parent type, independent of business stage and API lifecycle state.';
