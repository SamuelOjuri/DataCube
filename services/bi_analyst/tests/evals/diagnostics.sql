-- name: population
SELECT (SELECT count(*) FROM projects) AS source_projects,
 (SELECT count(*) FROM reportable_projects) AS reportable_projects,
 (SELECT count(*) FROM projects p WHERE NOT EXISTS(SELECT FROM reportable_projects r WHERE r.monday_id=p.monday_id)) AS excluded_projects,
 (SELECT count(*) FROM current_projects) AS verified_active_projects,
 (SELECT count(*) FROM project_reporting_classifications WHERE classification='needs_review') AS classifications_needing_review;

-- name: relationships
SELECT count(*) FILTER(WHERE p.monday_id IS NULL) AS children_without_parent,
 count(*) FILTER(WHERE s.hidden_item_id IS NULL) AS children_without_source_link,
 count(*) FILTER(WHERE s.hidden_item_id IS NOT NULL AND h.monday_id IS NULL) AS children_with_missing_source,
 (SELECT count(*) FROM (SELECT hidden_item_id FROM subitems WHERE hidden_item_id IS NOT NULL GROUP BY hidden_item_id HAVING count(*)>1) r) AS reused_hidden_sources
FROM subitems s LEFT JOIN projects p ON p.monday_id=s.parent_monday_id LEFT JOIN hidden_items h ON h.monday_id=s.hidden_item_id;

-- name: parent_invoice_reconciliation
WITH children AS (SELECT parent_monday_id,count(*) AS members,sum(amount_invoiced) AS invoice_total FROM subitems GROUP BY 1)
SELECT count(*) FILTER(WHERE p.total_amount_invoiced IS DISTINCT FROM c.invoice_total) AS differing_projects,
 count(*) FILTER(WHERE c.members IS NULL) AS projects_without_stored_children,
 count(*) AS reportable_projects
FROM reportable_projects p LEFT JOIN children c ON c.parent_monday_id=p.monday_id;

-- name: enquiry_child_reconciliation
WITH children AS (SELECT parent_monday_id,sum(CASE WHEN reason_for_change='New Enquiry' THEN quote_amount ELSE 0 END) AS amount FROM subitems GROUP BY 1)
SELECT count(*) FILTER(WHERE p.status_category='Open' AND p.new_enquiry_value IS DISTINCT FROM c.amount) AS open_parent_differences,
 (SELECT count(*) FROM subitems WHERE new_enquiry_value IS DISTINCT FROM CASE WHEN reason_for_change='New Enquiry' THEN quote_amount ELSE 0 END) AS child_formula_differences
FROM reportable_projects p LEFT JOIN children c ON c.parent_monday_id=p.monday_id;

-- name: archive_coverage
SELECT
 (SELECT count(*) FROM reportable_projects p LEFT JOIN lifecycle l ON l.table_name='projects' AND l.monday_id=p.monday_id
  WHERE l.monday_state IS NULL AND NOT coalesce(l.blocked,false)) AS unverified_projects,
 (SELECT count(*) FROM subitems s JOIN reportable_projects p ON p.monday_id=s.parent_monday_id
  JOIN lifecycle pl ON pl.table_name='projects' AND pl.monday_id=p.monday_id
  LEFT JOIN lifecycle l ON l.table_name='subitems' AND l.monday_id=s.monday_id
  WHERE pl.monday_state='active' AND NOT pl.blocked AND NOT coalesce(l.blocked,false)
   AND (l.monday_state IS NULL OR (l.monday_state='active' AND l.observed_parent_id IS DISTINCT FROM s.parent_monday_id))) AS unverified_subitems,
 (SELECT count(DISTINCT s.hidden_item_id) FROM current_subitems s LEFT JOIN lifecycle l ON l.table_name='hidden_items' AND l.monday_id=s.hidden_item_id
  WHERE s.hidden_item_id IS NOT NULL AND l.monday_state IS NULL AND NOT coalesce(l.blocked,false)) AS unverified_sources,
 (SELECT count(*) FROM current_projects p JOIN lifecycle l ON l.table_name='projects' AND l.monday_id=p.monday_id
  WHERE l.transaction_values_verified IS DISTINCT FROM true OR l.membership_array_present IS DISTINCT FROM true
    OR EXISTS(SELECT FROM unnest(l.observed_active_child_ids) member(id) WHERE NOT EXISTS(
        SELECT FROM current_subitems s WHERE s.monday_id=member.id AND s.parent_monday_id=p.monday_id))) AS unverified_current_values,
 (SELECT count(*) FROM lifecycle_event_status e WHERE e.archive_policy='verified_archive_v1'
  AND (e.status IN ('retry','review') OR (e.status IN ('pending','processing') AND e.cause IS DISTINCT FROM 'periodic_lifecycle_check'))
  AND ((e.board_id='1825117125' AND EXISTS(SELECT FROM reportable_projects p WHERE p.monday_id=e.item_id))
    OR (e.board_id='1825117144' AND EXISTS(SELECT FROM subitems s JOIN reportable_projects p ON p.monday_id=s.parent_monday_id WHERE s.monday_id=e.item_id))
    OR (e.board_id='1825138260' AND EXISTS(SELECT FROM subitems s JOIN reportable_projects p ON p.monday_id=s.parent_monday_id WHERE s.hidden_item_id=e.item_id)))) AS unresolved_archive_jobs;

-- name: gestation_source_check
SELECT count(*) FILTER(WHERE gestation_period IS DISTINCT FROM first_date_invoiced-first_date_designed) AS differences_with_both_dates,
 count(*) AS rows_with_both_dates FROM reportable_projects WHERE first_date_designed IS NOT NULL AND first_date_invoiced IS NOT NULL;

-- name: future_dates
SELECT count(*) FILTER(WHERE date_created>as_of_date) AS future_creation_dates,
 count(*) FILTER(WHERE first_date_invoiced>as_of_date) AS future_first_invoice_dates FROM reportable_projects,context;

-- name: revenue_definition_difference
WITH planned AS (
 SELECT date_trunc('month',s.invoice_date)::date AS month,sum(s.amount_invoiced)::numeric(14,2) AS amount
 FROM subitems s JOIN reportable_projects p ON p.monday_id=s.parent_monday_id CROSS JOIN context
 WHERE s.invoice_date<date_trunc('month',as_of_date)::date AND s.amount_invoiced>0
 AND p.pipeline_stage='Won - Closed (Invoiced)' GROUP BY 1)
SELECT b.revenue_month AS month,b.actual_revenue AS deployed_amount,coalesce(p.amount,0)::numeric(14,2) AS plan_amount,
 b.actual_revenue-coalesce(p.amount,0) AS difference FROM baseline_revenue b LEFT JOIN planned p ON p.month=b.revenue_month
WHERE b.actual_revenue IS DISTINCT FROM coalesce(p.amount,0)::numeric(14,2) ORDER BY b.revenue_month;

-- name: enquiry_view_parity
WITH expected AS (SELECT date_trunc('month',date_created)::date AS month,sum(new_enquiry_value)::numeric(14,2) AS amount
 FROM reportable_projects,context WHERE date_created<date_trunc('month',as_of_date)::date AND new_enquiry_value>0 GROUP BY 1)
SELECT count(*) AS differing_months FROM baseline_enquiry b LEFT JOIN expected e ON e.month=b.enquiry_month
WHERE b.actual_enquiry_value IS DISTINCT FROM coalesce(e.amount,0)::numeric(14,2);

-- name: bookings_view_parity
WITH expected AS (SELECT date_trunc('month',date_order_received)::date AS month,sum(total_order_value)::numeric(14,2) AS amount
 FROM reportable_projects,context WHERE date_order_received<date_trunc('month',as_of_date)::date AND total_order_value>0
 AND pipeline_stage IN ('Won - Open (Order Received)','Won - Closed (Invoiced)','Won Via Other Ref') GROUP BY 1)
SELECT count(*) AS differing_months FROM baseline_bookings b LEFT JOIN expected e ON e.month=b.booking_month
WHERE b.actual_bookings IS DISTINCT FROM coalesce(e.amount,0)::numeric(14,2);

-- name: conversion_refresh_difference
SELECT 'five_year' AS cohort,(SELECT sum(total_projects) FROM baseline_conversion) AS materialized_count,
 (SELECT count(*) FROM reportable_projects,context WHERE date_created>=as_of_date-interval '5 years') AS fixed_date_count
UNION ALL
SELECT 'two_year',(SELECT sum(total_projects) FROM baseline_conversion_recent),
 (SELECT count(*) FROM reportable_projects,context WHERE date_created>=as_of_date-interval '2 years');
