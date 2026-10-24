-- Project display labels for exploratory reports. Apply transactionally after 003.
-- Keep the accepted analytics catalogue unchanged: this is a gateway-only field.
-- The existing project population and one-row-per-monday_id grain are retained.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

CREATE OR REPLACE VIEW analyst_query.projects_v1
WITH (security_barrier=true, security_invoker=false) AS
SELECT p.monday_id, p.pipeline_stage, p.category, p.type, p.account, p.product_type,
       p.date_created, p.date_order_received, p.first_date_designed, p.first_date_invoiced,
       p.new_enquiry_value, p.total_order_value, p.total_amount_invoiced, p.gestation_period,
       source.item_name
FROM analytics.projects_v1 p
LEFT JOIN public.projects source ON source.monday_id=p.monday_id;

-- CREATE OR REPLACE preserves the gateway's owner and existing ACLs. Both
-- dependencies are already approved; no new grants or source changes are needed.
COMMENT ON COLUMN analyst_query.projects_v1.item_name IS
    'Monday item name (the Project reference), stored as text. Preferred project display label; not a unique key. Join and group projects by monday_id, and use it as fallback for blank labels.';
