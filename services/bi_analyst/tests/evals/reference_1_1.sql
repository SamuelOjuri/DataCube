-- Catalogue 1.1.0 overrides. Other references retain the historical SQL.
-- Retained reportable parents, including archived history; no business-stage gate.
-- name: invoice_last_month
SELECT count(*) AS invoice_rows,count(DISTINCT p.monday_id) AS projects,
 coalesce(sum(s.amount_invoiced),0)::numeric(14,2) AS amount
FROM subitems s JOIN reportable_projects p ON p.monday_id=s.parent_monday_id CROSS JOIN context
WHERE s.invoice_date >= (date_trunc('month',as_of_date)-interval '1 month')::date
 AND s.invoice_date<date_trunc('month',as_of_date)::date AND s.amount_invoiced>0;

-- name: invoice_previous_month
SELECT count(*) AS invoice_rows,count(DISTINCT p.monday_id) AS projects,
 coalesce(sum(s.amount_invoiced),0)::numeric(14,2) AS amount
FROM subitems s JOIN reportable_projects p ON p.monday_id=s.parent_monday_id CROSS JOIN context
WHERE s.invoice_date >= (date_trunc('month',as_of_date)-interval '2 months')::date
 AND s.invoice_date<(date_trunc('month',as_of_date)-interval '1 month')::date AND s.amount_invoiced>0;

-- name: invoice_monthly
WITH months AS (SELECT generate_series(date_trunc('month',as_of_date)-interval '12 months',
 date_trunc('month',as_of_date)-interval '1 month',interval '1 month')::date AS month FROM context),
 facts AS (SELECT s.* FROM subitems s JOIN reportable_projects p ON p.monday_id=s.parent_monday_id
 WHERE s.amount_invoiced>0)
SELECT m.month,count(s.monday_id) AS invoice_rows,count(DISTINCT s.parent_monday_id) AS projects,
 coalesce(sum(s.amount_invoiced),0)::numeric(14,2) AS amount
FROM months m LEFT JOIN facts s ON s.invoice_date>=m.month AND s.invoice_date<(m.month+interval '1 month')::date
GROUP BY m.month ORDER BY m.month;
