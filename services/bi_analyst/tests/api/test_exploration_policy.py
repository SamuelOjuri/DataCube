"""Exploratory SQL allowlist: realistic analytical queries pass; everything else fails closed."""
import pytest

from bi_analyst.database import GATEWAYS
from bi_analyst.exploration.policy import MAX_SQL_CHARS, UnsafeQuery, validate

RELATIONS = frozenset(GATEWAYS)

VALID = {
    'calendar_month': """SELECT sum(amount_invoiced) AS revenue
        FROM analyst_query.invoice_reporting_facts_v1
        WHERE invoice_date >= DATE '2026-01-01' AND invoice_date < DATE '2026-02-01'""",
    'monthly_growth': """WITH monthly AS (
          SELECT date_trunc('month', invoice_date)::date AS month, sum(amount_invoiced) AS revenue
          FROM analyst_query.invoice_reporting_facts_v1 GROUP BY 1)
        SELECT month, revenue,
          round(100.0 * (revenue - lag(revenue) OVER (ORDER BY month)) / nullif(lag(revenue) OVER (ORDER BY month), 0), 2) AS growth_percent
        FROM monthly ORDER BY month""",
    'pareto_accounts': """WITH totals AS (
          SELECT coalesce(p.account, 'Unclassified') AS account, sum(f.amount_invoiced) AS revenue
          FROM analyst_query.invoice_reporting_facts_v1 f
          JOIN analyst_query.projects_v1 p ON p.monday_id = f.parent_monday_id
          GROUP BY 1),
        ranked AS (
          SELECT account, revenue, sum(revenue) OVER (ORDER BY revenue DESC, account) / nullif(sum(revenue) OVER (), 0) AS cumulative_share
          FROM totals)
        SELECT account, revenue, cumulative_share FROM ranked WHERE cumulative_share <= 0.8 ORDER BY revenue DESC LIMIT 50""",
    'quarter_comparison': """SELECT
          sum(amount_invoiced) FILTER (WHERE invoice_date >= '2025-01-01' AND invoice_date < '2025-04-01') AS q1_2025,
          sum(amount_invoiced) FILTER (WHERE invoice_date >= '2026-01-01' AND invoice_date < '2026-04-01') AS q1_2026
        FROM analyst_query.invoice_reporting_facts_v1""",
    'gestation_median': """SELECT category, percentile_cont(0.5) WITHIN GROUP (ORDER BY gestation_period) AS median_days,
          count(*) AS projects
        FROM analyst_query.projects_v1 WHERE gestation_period > 0 GROUP BY category ORDER BY median_days DESC NULLS LAST""",
    'conversion_by_type': """SELECT type, round(sum(win_count)::numeric / nullif(sum(eligible_count), 0), 4) AS conversion_rate
        FROM analyst_query.conversion_cohorts_v1 WHERE cohort_years = 5 GROUP BY type""",
    'calendar_spine': """WITH months AS (
          SELECT g::date AS month FROM generate_series(DATE '2025-11-01', DATE '2026-09-01', INTERVAL '1 month') AS g),
        revenue AS (
          SELECT date_trunc('month', invoice_date)::date AS month, sum(amount_invoiced) AS revenue
          FROM analyst_query.invoice_reporting_facts_v1 GROUP BY 1)
        SELECT m.month, coalesce(r.revenue, 0) AS revenue FROM months m LEFT JOIN revenue r ON r.month = m.month ORDER BY m.month""",
    'children_pre_aggregated': """SELECT p.category, count(*) AS projects, sum(t.child_invoice_amount) AS child_invoices
        FROM analyst_query.projects_v1 p
        LEFT JOIN analyst_query.child_totals_v1 t ON t.parent_monday_id = p.monday_id GROUP BY p.category""",
    'exists_and_in': """SELECT count(*) AS invoiced_projects FROM analyst_query.projects_v1 p
        WHERE EXISTS (SELECT 1 FROM analyst_query.children_v1 c WHERE c.parent_monday_id = p.monday_id AND c.amount_invoiced > 0)
          AND p.monday_id IN (SELECT parent_monday_id FROM analyst_query.invoice_reporting_facts_v1)""",
    'union_with_cte': """WITH enquiries AS (SELECT enquiry_month AS month, actual_enquiry_value AS value FROM analyst_query.enquiry_monthly_v1)
        SELECT 'enquiries' AS measure, month, value FROM enquiries
        UNION ALL SELECT 'bookings', booking_month, actual_bookings FROM analyst_query.bookings_monthly_v1""",
    'fiscal_year': """SELECT CASE WHEN extract(month FROM invoice_date) >= 11 THEN extract(year FROM invoice_date) + 1
              ELSE extract(year FROM invoice_date) END AS fiscal_year, sum(amount_invoiced) AS revenue
        FROM analyst_query.invoice_reporting_facts_v1 GROUP BY 1 ORDER BY 1""",
    'text_functions': """SELECT to_char(date_created, 'YYYY-MM') AS month, lower(trim(coalesce(type, ''))) AS project_type,
          string_agg(DISTINCT account, ', ') AS accounts, count(DISTINCT monday_id) AS projects
        FROM analyst_query.projects_v1 WHERE account ILIKE '%school%' AND gestation_period % 2 = 0 GROUP BY 1, 2""",
    'consecutive_decline': """WITH quarterly AS (
          SELECT p.account, date_trunc('quarter', f.invoice_date)::date AS quarter, sum(f.amount_invoiced) AS revenue
          FROM analyst_query.invoice_reporting_facts_v1 f JOIN analyst_query.projects_v1 p ON p.monday_id = f.parent_monday_id
          GROUP BY 1, 2),
        changes AS (
          SELECT account, quarter, revenue, lag(revenue, 1) OVER w AS previous_1, lag(revenue, 2) OVER w AS previous_2,
            lag(revenue, 3) OVER w AS previous_3
          FROM quarterly WINDOW w AS (PARTITION BY account ORDER BY quarter))
        SELECT account, quarter, revenue FROM changes
        WHERE revenue < previous_1 AND previous_1 < previous_2 AND previous_2 < previous_3 ORDER BY account, quarter""",
    'date_helpers': """SELECT date_part('year', date_created) AS year, make_date(2026, 1, 1) AS fiscal_reference,
          avg(extract(day FROM age(current_date, date_created))) AS average_age_days
        FROM analyst_query.projects_v1 GROUP BY 1""",
    'ranking': """SELECT account, total_order_value, rank() OVER (ORDER BY total_order_value DESC NULLS LAST) AS position
        FROM analyst_query.projects_v1 ORDER BY position LIMIT 10""",
}


@pytest.mark.parametrize('name', VALID)
def test_realistic_analytical_queries_are_accepted_and_normalised(name):
    validated = validate(VALID[name], RELATIONS)
    assert validated.relations and set(validated.relations) <= RELATIONS
    assert validated.columns
    # The executed statement is the regenerated tree, and regeneration is stable.
    assert validate(validated.sql, RELATIONS) == validated


def test_validated_query_reports_relations_and_output_columns():
    validated = validate(VALID['pareto_accounts'], RELATIONS)
    assert validated.relations == ('invoice_reporting_facts_v1', 'projects_v1')
    assert validated.columns == ('account', 'revenue', 'cumulative_share')
    assert "'%school%'" in validate(VALID['text_functions'], RELATIONS).sql


REJECTED = {
    'delete': ('DELETE FROM analyst_query.projects_v1', 'single SELECT'),
    'update': ("UPDATE analyst_query.projects_v1 SET account = 'x'", 'single SELECT'),
    'insert': ("INSERT INTO analyst_query.projects_v1 (account) VALUES ('x')", 'single SELECT'),
    'drop': ('DROP VIEW analyst_query.projects_v1', 'single SELECT'),
    'create': ('CREATE TABLE x AS SELECT 1', 'single SELECT'),
    'set': ("SET statement_timeout = 0", 'single SELECT'),
    'copy': ("COPY analyst_query.projects_v1 TO '/tmp/x'", 'single SELECT'),
    'truncate': ('TRUNCATE analyst_query.projects_v1', 'single SELECT'),
    'do_block': ("DO $$ BEGIN PERFORM 1; END $$", 'single SELECT'),
    'stacked': ('SELECT 1 AS a FROM analyst_query.projects_v1; DELETE FROM analyst_query.projects_v1', 'exactly one'),
    'cte_dml': ('WITH d AS (DELETE FROM analyst_query.projects_v1 RETURNING *) SELECT count(*) AS n FROM d', 'not permitted'),
    'select_into': ('SELECT account INTO stolen FROM analyst_query.projects_v1', 'not permitted'),
    'locking': ('SELECT account FROM analyst_query.projects_v1 FOR UPDATE', 'not permitted'),
    'recursive': ('WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r) SELECT n FROM r', 'Recursive'),
    'sleep': ('SELECT pg_sleep(10) AS x FROM analyst_query.projects_v1', 'pg_sleep'),
    'query_to_xml': ("SELECT query_to_xml('select 1', true, true, '') AS x", 'query_to_xml'),
    'set_config': ("SELECT set_config('statement_timeout', '0', false) AS x", 'set_config'),
    'current_setting': ("SELECT current_setting('role') AS x", 'current_setting'),
    'large_object': ('SELECT lo_get(1) AS x', 'lo_get'),
    'dblink': ("SELECT dblink('host=x', 'select 1') AS x", 'dblink'),
    'read_file': ("SELECT pg_read_file('postgresql.conf') AS x", 'pg_read_file'),
    'qualified_function': ("SELECT pg_catalog.lower('A') AS x FROM analyst_query.projects_v1", 'not permitted'),
    'advisory_lock': ('SELECT pg_advisory_lock(1) AS x FROM analyst_query.projects_v1', 'pg_advisory_lock'),
    'regex_operator': ("SELECT account FROM analyst_query.projects_v1 WHERE account ~ '(a+)+$'", 'not permitted'),
    'catalog_view': ('SELECT pid FROM pg_stat_activity', 'not an approved view'),
    'qualified_catalog': ('SELECT rolname FROM pg_catalog.pg_authid', 'not an approved view'),
    'raw_source_table': ('SELECT account FROM public.projects', 'not an approved view'),
    'analytics_schema': ('SELECT account FROM analytics.projects_v1', 'not an approved view'),
    'database_qualified': ('SELECT account FROM postgres.analyst_query.projects_v1', 'Database-qualified'),
    'unapproved_gateway_table': ('SELECT secret FROM analyst_query.future_data', 'not an approved view'),
    'case_sensitive_name': ('SELECT account FROM analyst_query."Projects_v1"', 'not an approved view'),
    'unqualified_view': ('SELECT account FROM projects_v1', 'not an approved view'),
    'later_sibling_cte': ('WITH a AS (SELECT name FROM pg_settings), pg_settings AS (SELECT 1 AS name) SELECT name FROM a', 'not an approved view'),
    'nested_cte_leak': ('SELECT x.one FROM (WITH pg_stat_activity AS (SELECT 1 AS one) SELECT one FROM pg_stat_activity) x, pg_stat_activity', 'not an approved view'),
    'psycopg_placeholder': ('SELECT account FROM analyst_query.projects_v1 WHERE account = %s', 'not permitted'),
    'named_placeholder': ('SELECT account FROM analyst_query.projects_v1 WHERE account = %(a)s', 'not permitted'),
    'positional_parameter': ('SELECT account FROM analyst_query.projects_v1 WHERE account = $1', 'not permitted'),
    'regclass_cast': ("SELECT 'pg_authid'::regclass AS x FROM analyst_query.projects_v1", 'not permitted'),
    'json_cast': ("SELECT account::json AS x FROM analyst_query.projects_v1", 'Cast only'),
    'array_cast': ("SELECT '{1}'::int[] AS x FROM analyst_query.projects_v1", 'Cast only'),
    'select_star': ('SELECT * FROM analyst_query.projects_v1', 'explicit'),
    'unaliased_expression': ('SELECT sum(total_order_value) FROM analyst_query.projects_v1', 'explicit'),
    'duplicate_output': ('SELECT account, category AS account FROM analyst_query.projects_v1', 'unique'),
    'quoted_alias': ('SELECT account AS "Account Name" FROM analyst_query.projects_v1', 'snake_case'),
    'session_keyword': ('SELECT current_user AS who FROM analyst_query.projects_v1', 'not permitted'),
    'tablesample': ('SELECT account FROM analyst_query.projects_v1 TABLESAMPLE SYSTEM (10)', 'not permitted'),
    'no_view': ('SELECT 1 AS one', 'approved analyst_query view'),
    'series_only': ('SELECT g AS n FROM generate_series(1, 3) AS g', 'approved analyst_query view'),
    'other_table_function': ("SELECT f AS name FROM pg_ls_dir('.') AS f", 'generate_series'),
    'syntax_error': ('SELEC account FROM analyst_query.projects_v1', 'not valid'),
    'empty': ('  ', 'Provide one'),
}


@pytest.mark.parametrize('name', REJECTED)
def test_unsafe_or_unsupported_sql_is_rejected_with_repairable_message(name):
    sql, message = REJECTED[name]
    with pytest.raises(UnsafeQuery, match=message):
        validate(sql, RELATIONS)


def test_oversized_query_is_rejected():
    clauses = ' OR '.join(["account = 'x'"] * (MAX_SQL_CHARS // 10))
    with pytest.raises(UnsafeQuery, match='exceeds'):
        validate('SELECT account FROM analyst_query.projects_v1 WHERE ' + clauses, RELATIONS)


def test_only_supplied_relations_are_approved():
    with pytest.raises(UnsafeQuery, match='not an approved view'):
        validate(VALID['calendar_month'], frozenset({'projects_v1'}))


def test_rejection_messages_never_echo_literals():
    try:
        validate("SELECT pg_sleep(1) AS x WHERE 'private-crm-value' = 'x'", RELATIONS)
    except UnsafeQuery as error:
        assert 'private-crm-value' not in str(error)
