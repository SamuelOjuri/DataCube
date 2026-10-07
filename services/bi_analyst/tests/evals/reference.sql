-- Phase 1 reference SQL, independent of any future analyst compiler.
-- Run only against a versioned evaluation schema, using its fixed context.
-- Amounts use the source currency; currency/tax presentation awaits owner review.
-- Empty SUMs remain NULL except monthly actuals, whose published contract uses zero.
-- Conversion ratios retain the existing three-decimal rounding, BEFORE presentation.
-- The existing cohort has a lower date bound only; this intentionally preserves it.

-- name: enquiry_total
SELECT count(*) AS projects, count(new_enquiry_value) AS known_values,
       sum(new_enquiry_value) AS amount FROM reportable_projects;

-- name: enquiry_last_month
SELECT count(*) AS projects, coalesce(sum(new_enquiry_value),0)::numeric(14,2) AS amount
FROM reportable_projects, context
WHERE date_created >= (date_trunc('month',as_of_date)-interval '1 month')::date
  AND date_created < date_trunc('month',as_of_date)::date AND new_enquiry_value > 0;

-- name: enquiry_previous_month
SELECT count(*) AS projects, coalesce(sum(new_enquiry_value),0)::numeric(14,2) AS amount
FROM reportable_projects, context
WHERE date_created >= (date_trunc('month',as_of_date)-interval '2 months')::date
  AND date_created < (date_trunc('month',as_of_date)-interval '1 month')::date AND new_enquiry_value > 0;

-- name: enquiry_monthly
WITH months AS (
 SELECT generate_series(date_trunc('month',as_of_date)-interval '12 months',
                        date_trunc('month',as_of_date)-interval '1 month',interval '1 month')::date AS month
 FROM context)
SELECT m.month,count(p.monday_id) AS projects,coalesce(sum(p.new_enquiry_value),0)::numeric(14,2) AS amount
FROM months m LEFT JOIN reportable_projects p ON p.date_created>=m.month
 AND p.date_created<(m.month+interval '1 month')::date AND p.new_enquiry_value>0
GROUP BY m.month ORDER BY m.month;

-- name: enquiry_category
SELECT nullif(trim(category),'') AS category,count(*) AS projects,sum(new_enquiry_value) AS amount
FROM reportable_projects GROUP BY 1 ORDER BY 1 NULLS LAST;

-- name: enquiry_type
SELECT nullif(trim(type),'') AS type,count(*) AS projects,sum(new_enquiry_value) AS amount
FROM reportable_projects GROUP BY 1 ORDER BY 1 NULLS LAST;

-- name: enquiry_top
SELECT monday_id,new_enquiry_value AS amount FROM reportable_projects
WHERE new_enquiry_value>0 ORDER BY new_enquiry_value DESC,monday_id LIMIT 10;

-- name: enquiry_missing
SELECT count(*) FILTER(WHERE new_enquiry_value IS NULL) AS blanks,
       count(*) FILTER(WHERE new_enquiry_value=0) AS zeroes,
       count(*) FILTER(WHERE new_enquiry_value<0) AS negatives FROM reportable_projects;

-- name: order_parent_total
SELECT count(*) AS projects,count(total_order_value) AS known_values,sum(total_order_value) AS amount
FROM reportable_projects;

-- name: order_hidden_complete
SELECT count(*) AS hidden_rows,
 count(*) FILTER(WHERE cust_order_value_material IS NOT NULL AND cust_additional_charges IS NOT NULL) AS complete_rows,
 count(*) FILTER(WHERE cust_order_value_material IS NULL OR cust_additional_charges IS NULL) AS incomplete_rows,
 sum(cust_order_value_material+cust_additional_charges) AS complete_input_subtotal FROM hidden_items;

-- name: order_last_month
SELECT count(*) AS projects,coalesce(sum(total_order_value),0)::numeric(14,2) AS amount
FROM reportable_projects,context WHERE date_order_received >= (date_trunc('month',as_of_date)-interval '1 month')::date
 AND date_order_received<date_trunc('month',as_of_date)::date AND total_order_value>0
 AND pipeline_stage IN ('Won - Open (Order Received)','Won - Closed (Invoiced)','Won Via Other Ref');

-- name: order_previous_month
SELECT count(*) AS projects,coalesce(sum(total_order_value),0)::numeric(14,2) AS amount
FROM reportable_projects,context WHERE date_order_received >= (date_trunc('month',as_of_date)-interval '2 months')::date
 AND date_order_received<(date_trunc('month',as_of_date)-interval '1 month')::date AND total_order_value>0
 AND pipeline_stage IN ('Won - Open (Order Received)','Won - Closed (Invoiced)','Won Via Other Ref');

-- name: order_monthly
WITH months AS (SELECT generate_series(date_trunc('month',as_of_date)-interval '12 months',
 date_trunc('month',as_of_date)-interval '1 month',interval '1 month')::date AS month FROM context)
SELECT m.month,count(p.monday_id) AS projects,coalesce(sum(p.total_order_value),0)::numeric(14,2) AS amount
FROM months m LEFT JOIN reportable_projects p ON p.date_order_received>=m.month
 AND p.date_order_received<(m.month+interval '1 month')::date AND p.total_order_value>0
 AND p.pipeline_stage IN ('Won - Open (Order Received)','Won - Closed (Invoiced)','Won Via Other Ref')
GROUP BY m.month ORDER BY m.month;

-- name: order_category
SELECT nullif(trim(category),'') AS category,count(*) AS projects,sum(total_order_value) AS amount
FROM reportable_projects GROUP BY 1 ORDER BY 1 NULLS LAST;

-- name: order_top
SELECT monday_id,total_order_value AS amount FROM reportable_projects WHERE total_order_value>0
ORDER BY total_order_value DESC,monday_id LIMIT 10;

-- name: order_missing
SELECT count(*) FILTER(WHERE total_order_value IS NULL) AS blanks,
 count(*) FILTER(WHERE total_order_value=0) AS zeroes,
 count(*) FILTER(WHERE total_order_value<0) AS negatives FROM reportable_projects;

-- name: invoice_parent_total
SELECT count(*) AS projects,count(total_amount_invoiced) AS known_values,sum(total_amount_invoiced) AS amount
FROM reportable_projects;

-- name: invoice_hidden_total
SELECT count(*) AS hidden_rows,count(amount_invoiced) AS known_values,sum(amount_invoiced) AS amount
FROM hidden_items;

-- name: invoice_children_total
SELECT count(*) AS child_rows,count(s.amount_invoiced) AS known_values,sum(s.amount_invoiced) AS amount
FROM subitems s JOIN reportable_projects p ON p.monday_id=s.parent_monday_id;

-- name: invoice_last_month
SELECT count(*) AS invoice_rows,count(DISTINCT p.monday_id) AS projects,
 coalesce(sum(s.amount_invoiced),0)::numeric(14,2) AS amount
FROM subitems s JOIN reportable_projects p ON p.monday_id=s.parent_monday_id CROSS JOIN context
WHERE s.invoice_date >= (date_trunc('month',as_of_date)-interval '1 month')::date
 AND s.invoice_date<date_trunc('month',as_of_date)::date AND s.amount_invoiced>0
 AND p.pipeline_stage='Won - Closed (Invoiced)';

-- name: invoice_previous_month
SELECT count(*) AS invoice_rows,count(DISTINCT p.monday_id) AS projects,
 coalesce(sum(s.amount_invoiced),0)::numeric(14,2) AS amount
FROM subitems s JOIN reportable_projects p ON p.monday_id=s.parent_monday_id CROSS JOIN context
WHERE s.invoice_date >= (date_trunc('month',as_of_date)-interval '2 months')::date
 AND s.invoice_date<(date_trunc('month',as_of_date)-interval '1 month')::date AND s.amount_invoiced>0
 AND p.pipeline_stage='Won - Closed (Invoiced)';

-- name: invoice_monthly
WITH months AS (SELECT generate_series(date_trunc('month',as_of_date)-interval '12 months',
 date_trunc('month',as_of_date)-interval '1 month',interval '1 month')::date AS month FROM context),
 facts AS (SELECT s.* FROM subitems s JOIN reportable_projects p ON p.monday_id=s.parent_monday_id
 WHERE p.pipeline_stage='Won - Closed (Invoiced)' AND s.amount_invoiced>0)
SELECT m.month,count(s.monday_id) AS invoice_rows,count(DISTINCT s.parent_monday_id) AS projects,
 coalesce(sum(s.amount_invoiced),0)::numeric(14,2) AS amount
FROM months m LEFT JOIN facts s ON s.invoice_date>=m.month AND s.invoice_date<(m.month+interval '1 month')::date
GROUP BY m.month ORDER BY m.month;

-- name: invoice_category
SELECT nullif(trim(category),'') AS category,count(*) AS projects,sum(total_amount_invoiced) AS amount
FROM reportable_projects GROUP BY 1 ORDER BY 1 NULLS LAST;

-- name: invoice_signed
SELECT count(*) FILTER(WHERE amount_invoiced<0) AS negative_rows,
 sum(amount_invoiced) FILTER(WHERE amount_invoiced<0) AS negative_amount,
 count(*) FILTER(WHERE amount_invoiced IS NULL) AS blanks,
 count(*) FILTER(WHERE amount_invoiced=0) AS zeroes FROM hidden_items;

-- name: conversion_five_year
SELECT count(*) AS eligible,count(*) FILTER(WHERE pipeline_stage='Won - Closed (Invoiced)') AS wins,
 round(count(*) FILTER(WHERE pipeline_stage='Won - Closed (Invoiced)')::numeric/nullif(count(*),0),3) AS rate
FROM reportable_projects,context WHERE date_created>=as_of_date-interval '5 years';

-- name: conversion_two_year
SELECT count(*) AS eligible,count(*) FILTER(WHERE pipeline_stage='Won - Closed (Invoiced)') AS wins,
 round(count(*) FILTER(WHERE pipeline_stage='Won - Closed (Invoiced)')::numeric/nullif(count(*),0),3) AS rate
FROM reportable_projects,context WHERE date_created>=as_of_date-interval '2 years';

-- name: conversion_closed_five_year
SELECT count(*) FILTER(WHERE pipeline_stage IN ('Won - Closed (Invoiced)','Lost')) AS closed,
 count(*) FILTER(WHERE pipeline_stage='Won - Closed (Invoiced)') AS wins,
 round(count(*) FILTER(WHERE pipeline_stage='Won - Closed (Invoiced)')::numeric/
 nullif(count(*) FILTER(WHERE pipeline_stage IN ('Won - Closed (Invoiced)','Lost')),0),3) AS rate
FROM reportable_projects,context WHERE date_created>=as_of_date-interval '5 years';

-- name: conversion_closed_two_year
SELECT count(*) FILTER(WHERE pipeline_stage IN ('Won - Closed (Invoiced)','Lost')) AS closed,
 count(*) FILTER(WHERE pipeline_stage='Won - Closed (Invoiced)') AS wins,
 round(count(*) FILTER(WHERE pipeline_stage='Won - Closed (Invoiced)')::numeric/
 nullif(count(*) FILTER(WHERE pipeline_stage IN ('Won - Closed (Invoiced)','Lost')),0),3) AS rate
FROM reportable_projects,context WHERE date_created>=as_of_date-interval '2 years';

-- name: conversion_category
SELECT nullif(trim(category),'') AS category,count(*) AS eligible,
 count(*) FILTER(WHERE pipeline_stage='Won - Closed (Invoiced)') AS wins,
 round(count(*) FILTER(WHERE pipeline_stage='Won - Closed (Invoiced)')::numeric/nullif(count(*),0),3) AS rate
FROM reportable_projects,context WHERE date_created>=as_of_date-interval '5 years'
GROUP BY 1 ORDER BY 1 NULLS LAST;

-- name: conversion_type
SELECT nullif(trim(type),'') AS type,count(*) AS eligible,
 count(*) FILTER(WHERE pipeline_stage='Won - Closed (Invoiced)') AS wins,
 round(count(*) FILTER(WHERE pipeline_stage='Won - Closed (Invoiced)')::numeric/nullif(count(*),0),3) AS rate
FROM reportable_projects,context WHERE date_created>=as_of_date-interval '5 years'
GROUP BY 1 ORDER BY 1 NULLS LAST;

-- name: conversion_stage_counts
SELECT pipeline_stage,count(*) AS projects FROM reportable_projects,context
WHERE date_created>=as_of_date-interval '5 years' GROUP BY 1 ORDER BY 1 NULLS LAST;

-- name: conversion_undated
SELECT count(*) AS projects FROM reportable_projects WHERE date_created IS NULL;

-- name: gestation_five_year
SELECT count(*) AS projects,avg(gestation_period) AS days FROM reportable_projects,context
WHERE date_created>=as_of_date-interval '5 years' AND gestation_period>0;

-- name: gestation_two_year
SELECT count(*) AS projects,avg(gestation_period) AS days FROM reportable_projects,context
WHERE date_created>=as_of_date-interval '2 years' AND gestation_period>0;

-- name: gestation_median
SELECT percentile_cont(0.5) WITHIN GROUP(ORDER BY gestation_period) AS days
FROM reportable_projects,context WHERE date_created>=as_of_date-interval '5 years' AND gestation_period>0;

-- name: gestation_percentiles
SELECT percentile_cont(0.25) WITHIN GROUP(ORDER BY gestation_period) AS p25_days,
 percentile_cont(0.75) WITHIN GROUP(ORDER BY gestation_period) AS p75_days
FROM reportable_projects,context WHERE date_created>=as_of_date-interval '5 years' AND gestation_period>0;

-- name: gestation_category
SELECT nullif(trim(category),'') AS category,count(*) AS projects,avg(gestation_period) AS days
FROM reportable_projects,context WHERE date_created>=as_of_date-interval '5 years' AND gestation_period>0
GROUP BY 1 ORDER BY 1 NULLS LAST;

-- name: gestation_type
SELECT nullif(trim(type),'') AS type,count(*) AS projects,avg(gestation_period) AS days
FROM reportable_projects,context WHERE date_created>=as_of_date-interval '5 years' AND gestation_period>0
GROUP BY 1 ORDER BY 1 NULLS LAST;

-- name: gestation_exclusions
SELECT count(*) FILTER(WHERE gestation_period IS NULL) AS blanks,
 count(*) FILTER(WHERE gestation_period=0) AS zeroes,count(*) FILTER(WHERE gestation_period<0) AS negatives
FROM reportable_projects,context WHERE date_created>=as_of_date-interval '5 years';

-- name: gestation_longest
SELECT monday_id,gestation_period AS days FROM reportable_projects,context
WHERE date_created>=as_of_date-interval '5 years' AND gestation_period>0
ORDER BY gestation_period DESC,monday_id LIMIT 10;
