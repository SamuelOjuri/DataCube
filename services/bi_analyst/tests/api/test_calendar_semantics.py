"""Calendar lineage is independent of measure additivity and join cardinality."""
import pytest

from bi_analyst.exploration.contracts import ExploratoryColumn
from bi_analyst.exploration.semantics import describe_columns


def period(sql):
    columns, _ = describe_columns(sql, [ExploratoryColumn(
        name='period', label='Period', role='dimension', unit='none', type='date')])
    return columns[0]


@pytest.mark.parametrize('expression', [
    "generate_series(DATE '2025-11-01', DATE '2026-09-01', INTERVAL '1 month')::date",
    "CAST(generate_series(CAST('2025-11-01' AS DATE), CAST('2026-09-01' AS DATE), CAST('1 month' AS INTERVAL)) AS DATE)",
    "g::date FROM generate_series(DATE '2025-11-01', DATE '2026-09-01', INTERVAL '1 month') g",
    "g.bucket::date FROM generate_series(DATE '2025-11-01', DATE '2026-09-01', INTERVAL '1 month') g(bucket)",
])
def test_generated_calendar_select_and_table_function_forms(expression):
    value, _, source = expression.partition(' FROM ')
    assert period(f"SELECT {value} AS period" + (f" FROM {source}" if source else '')).time_grain == 'month'


@pytest.mark.parametrize('step,grain', [
    ("INTERVAL '1 day'", 'day'), ("INTERVAL '1 week'", 'week'), ("INTERVAL '7 days'", 'week'),
    ("INTERVAL '1 month'", 'month'), ("INTERVAL '3 months'", 'quarter'),
    ("INTERVAL '1 year'", 'year'), ("INTERVAL '12 months'", 'year'),
    ("'1 MONTH'::interval", 'month'),
    ("INTERVAL '2 months'", None), ("INTERVAL '-1 month'", None),
    ("CAST('1 month 1 day' AS INTERVAL)", None), ("INTERVAL '24 hours'", None), ("1", None),
])
def test_only_canonical_calendar_steps_are_inferred(step, grain):
    sql = f"SELECT generate_series(DATE '2025-01-01', DATE '2026-01-01', {step})::date AS period"
    assert period(sql).time_grain == grain


@pytest.mark.parametrize('projection,grain', [('c.bucket', 'month'), ('d.bucket', 'day'), ('bucket', None)])
def test_qualified_calendar_lineage_is_preserved_through_joins(projection, grain):
    sql = f"""WITH months AS (
        SELECT generate_series(DATE '2025-11-01', DATE '2026-09-01', INTERVAL '1 month')::date AS bucket),
        days AS (SELECT date_trunc('day', invoice_date)::date AS bucket FROM invoices)
        SELECT {projection} AS period FROM months c FULL JOIN days d USING (bucket)"""
    assert period(sql).time_grain == grain


def test_subquery_and_cte_column_aliases_preserve_calendar_lineage():
    sql = """WITH calendar(bucket) AS (
        SELECT generate_series(DATE '2025-11-01', DATE '2026-09-01', INTERVAL '1 month')::date AS original)
        SELECT nested.period FROM (SELECT c.bucket FROM calendar c) nested(period)
        LEFT JOIN invoices i ON i.invoice_date = nested.period"""
    assert period(sql).time_grain == 'month'


def test_joined_measure_lineage_does_not_become_additive_with_its_calendar():
    columns = [ExploratoryColumn(name='period', label='Period', role='dimension', unit='none', type='date'),
               ExploratoryColumn(name='revenue', label='Revenue', role='measure', unit='source_currency', type='decimal')]
    sql = """WITH totals AS (
        SELECT date_trunc('month', invoice_date)::date AS period, sum(amount) AS revenue FROM invoices GROUP BY 1)
        SELECT t.period,t.revenue FROM totals t JOIN invoices i ON date_trunc('month',i.invoice_date)::date=t.period"""
    described, _ = describe_columns(sql, columns)
    assert described[0].time_grain == 'month'
    assert not described[1].additive and not described[1].grouped


@pytest.mark.parametrize('projection', ['i.bucket', 'c.bucket + INTERVAL \'1 day\'', 'coalesce(c.bucket,i.invoice_date)'])
def test_unknown_or_changed_dates_do_not_inherit_a_calendar_by_name(projection):
    sql = f"""WITH calendar AS (
        SELECT generate_series(DATE '2025-11-01', DATE '2026-09-01', INTERVAL '1 month')::date AS bucket)
        SELECT ({projection})::date AS period FROM calendar c JOIN invoices i ON true"""
    assert period(sql).time_grain is None
