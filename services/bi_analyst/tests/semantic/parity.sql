-- Independent Phase 2 acceptance queries over the curated surface.
-- The evaluation harness supplies the frozen context; application SQL is Phase 4.
-- name: enquiry_total
SELECT count(*) AS projects,count(new_enquiry_value) AS known_values,sum(new_enquiry_value) AS amount
FROM analytics.projects_v1;
-- name: order_parent_total
SELECT count(*) AS projects,count(total_order_value) AS known_values,sum(total_order_value) AS amount
FROM analytics.projects_v1;
-- name: order_hidden_complete
SELECT count(*) AS hidden_rows,count(*) FILTER(WHERE order_inputs_complete) AS complete_rows,
 count(*) FILTER(WHERE NOT order_inputs_complete) AS incomplete_rows,
 sum(complete_input_order_value) AS complete_input_subtotal FROM analytics.hidden_values_v1;
-- name: invoice_parent_total
SELECT count(*) AS projects,count(total_amount_invoiced) AS known_values,sum(total_amount_invoiced) AS amount
FROM analytics.projects_v1;
-- name: invoice_hidden_total
SELECT count(*) AS hidden_rows,count(amount_invoiced) AS known_values,sum(amount_invoiced) AS amount
FROM analytics.hidden_values_v1;
-- name: invoice_children_total
SELECT count(*) AS child_rows,count(amount_invoiced) AS known_values,sum(amount_invoiced) AS amount
FROM analytics.children_v1;
-- name: enquiry_monthly
WITH months AS (SELECT generate_series(date_trunc('month',as_of_date)-interval '12 months',
 date_trunc('month',as_of_date)-interval '1 month',interval '1 month')::date AS month FROM context)
SELECT m.month,coalesce(a.project_count,0)::bigint AS projects,
 coalesce(a.actual_enquiry_value,0)::numeric(14,2) AS amount
FROM months m LEFT JOIN analytics.enquiry_monthly_v1 a ON a.enquiry_month=m.month ORDER BY m.month;
-- name: order_monthly
WITH months AS (SELECT generate_series(date_trunc('month',as_of_date)-interval '12 months',
 date_trunc('month',as_of_date)-interval '1 month',interval '1 month')::date AS month FROM context)
SELECT m.month,coalesce(a.project_count,0)::bigint AS projects,
 coalesce(a.actual_bookings,0)::numeric(14,2) AS amount
FROM months m LEFT JOIN analytics.bookings_monthly_v1 a ON a.booking_month=m.month ORDER BY m.month;
-- name: invoice_monthly
WITH months AS (SELECT generate_series(date_trunc('month',as_of_date)-interval '12 months',
 date_trunc('month',as_of_date)-interval '1 month',interval '1 month')::date AS month FROM context)
SELECT m.month,count(s.monday_id) AS invoice_rows,count(DISTINCT s.parent_monday_id) AS projects,
 coalesce(sum(s.amount_invoiced),0)::numeric(14,2) AS amount
FROM months m LEFT JOIN analytics.invoice_reporting_facts_v1 s ON s.invoice_date>=m.month
 AND s.invoice_date<(m.month+interval '1 month')::date GROUP BY m.month ORDER BY m.month;
-- name: conversion_five_year
SELECT coalesce(sum(eligible_count),0)::bigint AS eligible,coalesce(sum(win_count),0)::bigint AS wins,
 round(sum(win_count)::numeric/nullif(sum(eligible_count),0),3) AS rate
FROM analytics.conversion_cohorts_v1 WHERE cohort_years=5;
-- name: conversion_two_year
SELECT coalesce(sum(eligible_count),0)::bigint AS eligible,coalesce(sum(win_count),0)::bigint AS wins,
 round(sum(win_count)::numeric/nullif(sum(eligible_count),0),3) AS rate
FROM analytics.conversion_cohorts_v1 WHERE cohort_years=2;
-- name: conversion_closed_five_year
SELECT coalesce(sum(closed_count),0)::bigint AS closed,coalesce(sum(win_count),0)::bigint AS wins,
 round(sum(win_count)::numeric/nullif(sum(closed_count),0),3) AS rate
FROM analytics.conversion_cohorts_v1 WHERE cohort_years=5;
-- name: conversion_closed_two_year
SELECT coalesce(sum(closed_count),0)::bigint AS closed,coalesce(sum(win_count),0)::bigint AS wins,
 round(sum(win_count)::numeric/nullif(sum(closed_count),0),3) AS rate
FROM analytics.conversion_cohorts_v1 WHERE cohort_years=2;
-- name: gestation_five_year
SELECT count(*) AS projects,avg(gestation_period) AS days FROM analytics.projects_v1,context
WHERE date_created>=as_of_date-interval '5 years' AND gestation_period>0;
-- name: gestation_two_year
SELECT count(*) AS projects,avg(gestation_period) AS days FROM analytics.projects_v1,context
WHERE date_created>=as_of_date-interval '2 years' AND gestation_period>0;
-- name: gestation_median
SELECT percentile_cont(0.5) WITHIN GROUP(ORDER BY gestation_period) AS days
FROM analytics.projects_v1,context WHERE date_created>=as_of_date-interval '5 years' AND gestation_period>0;
