-- Descriptive project names alongside the references added by migration 011.
-- Apply in one administrator transaction after 011. Preserve the accepted
-- analytics catalogue, project grain, gateway owner and existing privileges.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

CREATE OR REPLACE VIEW analyst_query.projects_v1
WITH (security_barrier=true, security_invoker=false) AS
SELECT p.monday_id, p.pipeline_stage, p.category, p.type, p.account, p.product_type,
       p.date_created, p.date_order_received, p.first_date_designed, p.first_date_invoiced,
       p.new_enquiry_value, p.total_order_value, p.total_amount_invoiced, p.gestation_period,
       source.item_name, source.project_name
FROM analytics.projects_v1 p
LEFT JOIN public.projects source ON source.monday_id=p.monday_id;

COMMENT ON COLUMN analyst_query.projects_v1.project_name IS
    'Descriptive Monday Project Name, shown alongside the item_name Project reference in reports and visuals. Not a unique key; join and group projects by monday_id.';
