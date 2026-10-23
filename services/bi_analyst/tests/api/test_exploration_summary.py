"""Summary arithmetic and its source references, independent of models and live data."""
from decimal import Decimal

import pytest

from bi_analyst.exploration.contracts import ExploratoryColumn
from bi_analyst.exploration.evidence import ground_exploration
from bi_analyst.exploration.semantics import describe_columns
from bi_analyst.workflow.contracts import GroundedAnswer
from test_exploration_workflow import exploratory_result

SQL = """SELECT date_trunc('month', date_created)::date AS creation_month,
sum(win_count) AS wins, sum(eligible_count) AS eligible,
round(sum(win_count)::numeric / nullif(sum(eligible_count), 0), 4) AS inclusive_conversion
FROM analyst_query.conversion_cohorts_v1 GROUP BY 1 ORDER BY 1"""


def monthly(rows=None, sql=SQL):
    columns = [ExploratoryColumn(name=n, label=label, role=role, unit=unit, type=kind)
               for n, label, role, unit, kind in [
                   ('creation_month', 'Creation month', 'dimension', 'none', 'date'),
                   ('wins', 'Won invoiced projects', 'measure', 'count', 'integer'),
                   ('eligible', 'Eligible projects', 'measure', 'count', 'integer'),
                   ('inclusive_conversion', 'Inclusive conversion', 'measure', 'ratio', 'decimal')]]
    columns, limited = describe_columns(sql, columns)
    if rows is None:
        rows = [[f'{2024 + i // 12}-{i % 12 + 1:02}-01', 1 if i < 12 else 9,
                 2 if i < 12 else 100, '0.5' if i < 12 else '0.09'] for i in range(24)]
    result = exploratory_result(rows, columns)
    result.provenance.sql = sql
    result.provenance.relations = ['conversion_cohorts_v1']
    result.provenance.query_limited = limited
    return result


def facts(answer):
    return {c.id: c for c in answer.claims if c.id.startswith('summary_')}


def test_weighted_conversion_uses_all_months_with_reproducible_evidence():
    result = monthly()
    answer = ground_exploration(result)
    claims = facts(answer)
    assert claims['summary_overall'].value == Decimal(120) / 1224
    assert claims['summary_overall'].denominator == 1224
    assert claims['summary_overall'].operation == 'ratio_of_sums'
    assert len(claims['summary_overall'].sources) == 48
    assert 'rows.23.2' in claims['summary_overall'].sources
    assert claims['summary_first_half'].value == Decimal('0.5')
    assert claims['summary_second_half'].value == Decimal('0.09')
    assert claims['summary_change'].value == Decimal('-41.00')
    assert claims['summary_change'].unit == 'percentage_points'
    assert 'overall for these months was 9.8%' in answer.summary
    assert '41.0 percentage points lower' in answer.summary
    assert 'December 2025' in answer.summary
    assert answer.exploratory_chart.series == ['inclusive_conversion']
    assert any('less time' in notice for notice in answer.summary_notices)
    # Decimal values loaded from persisted JSON follow exactly the same path.
    reloaded = type(result).model_validate_json(result.model_dump_json())
    assert ground_exploration(reloaded).summary == answer.summary


def test_months_are_sorted_independently_of_query_order():
    result = monthly()
    result.rows.reverse()
    answer = ground_exploration(result)
    assert facts(answer)['summary_latest'].sources == ['rows.0.3']
    assert '50.0% in January 2024 and 9.0% in December 2025' in answer.summary


def test_single_month_has_one_plain_sentence_without_duplicate_endpoints_or_extrema():
    answer = ground_exploration(monthly(rows=[['2024-01-01', 1, 2, '.5']]))
    assert answer.summary == 'Inclusive conversion was 50.0% in January 2024.'
    assert facts(answer)['summary_overall'].value == Decimal('.5')
    assert any('less time' in n for n in answer.summary_notices)


@pytest.mark.parametrize('change', ['null', 'zero', 'negative', 'absent'])
def test_invalid_or_missing_denominators_are_never_replaced_with_monthly_means(change):
    result = monthly()
    if change == 'absent':
        result.provenance.columns[3].denominator = None
    else:
        for row in result.rows:
            row[2] = {'null': None, 'zero': 0, 'negative': -1}[change]
    answer = ground_exploration(result)
    assert 'summary_overall' not in facts(answer)
    assert 'summary_change' not in facts(answer)


@pytest.mark.parametrize('mode', ['truncated', 'sql_limit', 'gap', 'duplicate'])
def test_incomplete_or_ambiguous_monthly_windows_do_not_claim_equal_halves(mode):
    result = monthly(sql=SQL + ' LIMIT 24' if mode == 'sql_limit' else SQL)
    if mode == 'truncated':
        result.provenance.truncated = True
    elif mode == 'gap':
        result.rows.pop(5)
    elif mode == 'duplicate':
        result.rows[0][0] = result.rows[1][0]
    answer = ground_exploration(result)
    assert 'summary_change' not in facts(answer)
    if mode in {'truncated', 'sql_limit', 'duplicate'}:
        assert answer.exploratory_chart is None
    if mode in {'truncated', 'sql_limit'}:
        assert any('only the returned rows' in n for n in answer.summary_notices)


def test_unknown_month_is_not_zero_and_is_not_used_for_an_arithmetic_mean():
    result = monthly()
    result.rows[-1][3] = None
    answer = ground_exploration(result)
    assert facts(answer)['summary_latest'].value == Decimal('.09')
    assert facts(answer)['summary_latest'].sources == ['rows.22.3']
    assert any('missing values are not zero' in n for n in answer.summary_notices)


def test_sql_metadata_does_not_sum_windows_or_attach_unrelated_counts_to_a_rate():
    result = monthly(sql=SQL.replace('sum(win_count)::numeric', '(sum(win_count) + 1)::numeric'))
    assert result.provenance.columns[3].numerator is None
    columns, _ = describe_columns('SELECT sum(wins) OVER (ORDER BY month) AS wins FROM monthly', result.provenance.columns)
    assert not columns[1].additive


def test_cte_projection_retains_monthly_axis_and_exact_ratio_components():
    sql = """WITH monthly AS (
        SELECT date_trunc('month',date_created)::date AS creation_month,
        sum(win_count) AS wins, sum(eligible_count) AS eligible
        FROM analyst_query.conversion_cohorts_v1 GROUP BY 1)
        SELECT m.creation_month, m.wins, m.eligible,
        round(m.wins::numeric/nullif(m.eligible,0),4) AS inclusive_conversion
        FROM monthly m ORDER BY 1"""
    result = monthly(sql=sql)
    assert result.provenance.columns[0].time_grain == 'month'
    assert result.provenance.columns[3].numerator == 'wins'
    assert facts(ground_exploration(result))['summary_overall'].value == Decimal(120) / 1224


def test_category_shares_are_complete_nonnegative_returned_totals_only():
    columns = [ExploratoryColumn(name='account', label='Account', role='dimension', unit='none', type='text'),
               ExploratoryColumn(name='revenue', label='Revenue', role='measure', unit='source_currency', type='decimal')]
    columns, _ = describe_columns('SELECT account, sum(amount) AS revenue FROM data GROUP BY account', columns)
    result = exploratory_result([['Small', '10'], ['Large', '90']], columns)
    answer = ground_exploration(result)
    assert 'Large' in answer.summary and '90.0%' in answer.summary
    assert facts(answer)['summary_share'].value == Decimal('.9')
    assert answer.exploratory_chart.kind == 'bar'
    for limited, rows in [(True, result.rows), (False, [['Small', None], ['Large', '90']]),
                          (False, [['Small', '-10'], ['Large', '90']])]:
        result.provenance.query_limited, result.rows = limited, rows
        assert 'summary_share' not in facts(ground_exploration(result))


def test_supporting_category_share_does_not_replace_the_primary_money_measure():
    columns = [ExploratoryColumn(name=n, label=n, role=r, unit=u, type=t)
               for n, r, u, t in [('category', 'dimension', 'none', 'text'),
                                 ('revenue', 'measure', 'source_currency', 'decimal'),
                                 ('share', 'measure', 'ratio', 'decimal')]]
    columns, _ = describe_columns('SELECT category, sum(amount) AS revenue, avg(amount) AS share FROM data GROUP BY category', columns)
    answer = ground_exploration(exploratory_result([['A', 10, '.1'], ['B', 90, '.9']], columns))
    assert '£90.00' in answer.summary
    assert answer.exploratory_chart.series == ['revenue']


def test_lists_total_amounts_but_never_identifiers_rates_or_averages():
    columns = [ExploratoryColumn(name=n, label=n, role=role, unit=unit, type=kind)
               for n, role, unit, kind in [('id', 'dimension', 'none', 'text'),
                                          ('value', 'measure', 'source_currency', 'decimal'),
                                          ('days', 'measure', 'days', 'decimal')]]
    columns, _ = describe_columns('SELECT id, value, days FROM projects', columns)
    result = exploratory_result([['a', '10.10', 5], ['b', '20.20', 7]], columns)
    answer = ground_exploration(result)
    assert '2 matching records' in answer.summary and '£30.30' in answer.summary
    assert facts(answer)['summary_total_value'].value == Decimal('30.30')
    assert 'summary_total_days' not in facts(answer)
    assert answer.exploratory_chart is None
    identifiers = [ExploratoryColumn(name='project_number', label='Project', role='measure', unit='count', type='integer')]
    identifiers, _ = describe_columns('SELECT monday_id AS project_number FROM projects', identifiers)
    assert not identifiers[0].additive


def test_older_saved_answers_default_to_no_summary_or_exploratory_chart():
    answer = ground_exploration(monthly()).model_dump(mode='json')
    for key in ('summary', 'summary_version', 'summary_notices', 'exploratory_chart'):
        answer.pop(key)
    old = GroundedAnswer.model_validate(answer)
    assert old.summary is None and old.exploratory_chart is None
