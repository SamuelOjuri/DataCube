-- Bounded, deterministic selections from the frozen dataset, never live Monday.
-- Each query returns all candidate IDs; Python retains counts and a small sample.
-- name: nonzero_additional_charges
SELECT h.monday_id AS hidden_id,s.monday_id AS child_id,s.parent_monday_id AS project_id
FROM hidden_items h LEFT JOIN subitems s ON s.hidden_item_id=h.monday_id
WHERE h.cust_additional_charges<>0 ORDER BY h.monday_id,s.monday_id;

-- name: signed_invoices
SELECT h.monday_id AS hidden_id,s.monday_id AS child_id,s.parent_monday_id AS project_id
FROM hidden_items h LEFT JOIN subitems s ON s.hidden_item_id=h.monday_id
WHERE h.amount_invoiced<0 ORDER BY h.monday_id,s.monday_id;

-- name: missing_order_inputs
SELECT h.monday_id AS hidden_id,s.monday_id AS child_id,s.parent_monday_id AS project_id
FROM hidden_items h LEFT JOIN subitems s ON s.hidden_item_id=h.monday_id
WHERE h.cust_order_value_material IS NULL OR h.cust_additional_charges IS NULL
ORDER BY h.monday_id,s.monday_id;

-- name: blank_invoices
SELECT monday_id AS child_id,parent_monday_id AS project_id,hidden_item_id AS hidden_id
FROM subitems WHERE amount_invoiced IS NULL ORDER BY monday_id;

-- name: zero_invoices
SELECT monday_id AS child_id,parent_monday_id AS project_id,hidden_item_id AS hidden_id
FROM subitems WHERE amount_invoiced=0 ORDER BY monday_id;

-- name: multiple_children
SELECT parent_monday_id AS project_id,count(*) AS child_count FROM subitems
GROUP BY parent_monday_id HAVING count(*)>1 ORDER BY parent_monday_id;

-- name: repeated_source_links
SELECT hidden_item_id AS hidden_id,count(*) AS link_count FROM subitems
WHERE hidden_item_id IS NOT NULL GROUP BY hidden_item_id HAVING count(*)>1 ORDER BY hidden_item_id;

-- name: unresolved_relationships
SELECT s.monday_id AS child_id,s.parent_monday_id AS project_id,s.hidden_item_id AS hidden_id
FROM subitems s LEFT JOIN projects p ON p.monday_id=s.parent_monday_id
LEFT JOIN hidden_items h ON h.monday_id=s.hidden_item_id
WHERE p.monday_id IS NULL OR h.monday_id IS NULL ORDER BY s.monday_id;

-- name: parent_invoice_differences
WITH totals AS (SELECT parent_monday_id,sum(amount_invoiced) AS amount FROM subitems GROUP BY 1)
SELECT p.monday_id AS project_id FROM reportable_projects p
LEFT JOIN totals t ON t.parent_monday_id=p.monday_id
WHERE p.total_amount_invoiced IS DISTINCT FROM t.amount ORDER BY p.monday_id;

-- name: open_enquiry_differences
WITH totals AS (SELECT parent_monday_id,sum(CASE WHEN reason_for_change='New Enquiry'
 THEN quote_amount ELSE 0 END) AS amount FROM subitems GROUP BY 1)
SELECT p.monday_id AS project_id FROM reportable_projects p
LEFT JOIN totals t ON t.parent_monday_id=p.monday_id
WHERE p.status_category='Open' AND p.new_enquiry_value IS DISTINCT FROM t.amount ORDER BY p.monday_id;

-- name: child_enquiry_formula_differences
SELECT monday_id AS child_id,parent_monday_id AS project_id,hidden_item_id AS hidden_id
FROM subitems WHERE new_enquiry_value IS DISTINCT FROM
 CASE WHEN reason_for_change='New Enquiry' THEN quote_amount ELSE 0 END ORDER BY monday_id;

-- name: retained_closed_enquiries
SELECT monday_id AS project_id FROM reportable_projects
WHERE status_category IN ('Won','Lost') AND new_enquiry_value IS NOT NULL ORDER BY monday_id;

-- name: gestation_fallback_differences
SELECT monday_id AS project_id FROM reportable_projects
WHERE first_date_designed IS NOT NULL AND first_date_invoiced IS NOT NULL
AND gestation_period IS DISTINCT FROM first_date_invoiced-first_date_designed ORDER BY monday_id;

-- name: conversion_wins
SELECT monday_id AS project_id FROM reportable_projects CROSS JOIN context
WHERE date_created>=as_of_date-interval '5 years' AND pipeline_stage='Won - Closed (Invoiced)'
ORDER BY monday_id;

-- name: conversion_nonwins
SELECT monday_id AS project_id FROM reportable_projects CROSS JOIN context
WHERE date_created>=as_of_date-interval '5 years' AND pipeline_stage IS DISTINCT FROM 'Won - Closed (Invoiced)'
ORDER BY monday_id;

-- name: placeholder_exclusions
SELECT p.monday_id AS project_id FROM projects p WHERE NOT EXISTS
 (SELECT FROM reportable_projects r WHERE r.monday_id=p.monday_id) ORDER BY p.monday_id;
