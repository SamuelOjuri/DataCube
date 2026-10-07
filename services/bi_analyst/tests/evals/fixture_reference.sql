-- name: fixture_charges
SELECT count(*) FILTER(WHERE cust_order_value_material IS NOT NULL AND cust_additional_charges IS NOT NULL) AS complete_rows,
 count(*) FILTER(WHERE cust_order_value_material IS NULL OR cust_additional_charges IS NULL) AS incomplete_rows,
 sum(cust_order_value_material+cust_additional_charges) AS amount FROM fixture_hidden_items;

-- name: fixture_blank_zero
SELECT parent_monday_id,sum(amount_invoiced) AS amount FROM fixture_subitems
WHERE parent_monday_id IN ('E2','E3') GROUP BY 1 ORDER BY 1;

-- name: fixture_signed
SELECT sum(amount_invoiced) AS signed_amount,sum(amount_invoiced) FILTER(WHERE amount_invoiced>0) AS positive_only_amount
FROM fixture_subitems;

-- name: fixture_exact_enquiry
SELECT sum(CASE WHEN reason_for_change='New Enquiry' THEN quote_amount ELSE 0 END) AS amount FROM fixture_subitems;

-- name: fixture_conversion
SELECT count(*) AS eligible,count(*) FILTER(WHERE pipeline_stage='Won - Closed (Invoiced)') AS wins,
 round(count(*) FILTER(WHERE pipeline_stage='Won - Closed (Invoiced)')::numeric/nullif(count(*),0),3) AS rate FROM fixture_projects;

-- name: fixture_gestation
SELECT count(*) AS projects,avg(gestation_period) AS days FROM fixture_projects WHERE gestation_period>0;

-- name: fixture_join
WITH children AS (SELECT parent_monday_id,count(*) AS child_count FROM fixture_subitems GROUP BY 1)
SELECT sum(p.total_order_value) AS amount FROM fixture_projects p LEFT JOIN children c ON c.parent_monday_id=p.monday_id;

-- name: fixture_repeated_source
WITH linked AS (
 SELECT h.monday_id,sum(h.cust_order_value_material+h.cust_additional_charges) AS linked_contributions,count(*) AS links
 FROM fixture_hidden_items h JOIN fixture_subitems s ON s.hidden_item_id=h.monday_id GROUP BY h.monday_id)
SELECT h.cust_order_value_material+h.cust_additional_charges AS hidden_amount_once,
 l.linked_contributions,l.links FROM fixture_hidden_items h JOIN linked l ON l.monday_id=h.monday_id WHERE h.monday_id='H1';

-- name: fixture_completed_month
SELECT count(*) AS projects,sum(new_enquiry_value) AS amount FROM fixture_projects,context
WHERE date_created >= (date_trunc('month',as_of_date)-interval '1 month')::date
 AND date_created<date_trunc('month',as_of_date)::date AND new_enquiry_value>0;

-- name: fixture_zero_denominator
SELECT round(count(*) FILTER(WHERE pipeline_stage='Won - Closed (Invoiced)')::numeric/
 nullif(count(*) FILTER(WHERE pipeline_stage IN ('Won - Closed (Invoiced)','Lost')),0),3) AS rate
FROM fixture_projects WHERE pipeline_stage='Open Enquiry';
