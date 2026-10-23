"""Paired monthly summaries across measures, shapes, periods and saved answers."""
import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from bi_analyst.exploration.contracts import ExploratoryColumn, MonthlyComparison
from bi_analyst.exploration.evidence import ground_exploration
from bi_analyst.workflow.service import WorkflowService
from bi_analyst.workflow.summary_language import SUMMARY_VERSION
from test_exploration_workflow import exploratory_result


def comparison(*, unit='source_currency', subject='Revenue', additive=True, explicit=True):
    columns = [ExploratoryColumn(name=n, label=label, role=role, unit=u, type=kind, additive=additive and role == 'measure')
        for n, label, role, u, kind in [
            ('current_month', 'Current month', 'dimension', 'none', 'date'),
            ('prior_month', 'Prior month', 'dimension', 'none', 'date'),
            ('current_value', f'Current {subject}', 'measure', unit, 'decimal'),
            ('prior_value', f'Prior {subject}', 'measure', unit, 'decimal')]]
    rows = [[f'2026-{i:02}-01', f'2025-{i:02}-01', str(value), '100'] for i, value in enumerate([80, 90, 120, 150], 1)]
    result = exploratory_result(rows, columns)
    result.provenance.freshness.queried_at = datetime(2026, 10, 23, tzinfo=timezone.utc)
    if explicit:
        result.provenance.monthly_comparison = MonthlyComparison(current_month='current_month', baseline_month='prior_month',
            current_value='current_value', baseline_value='prior_value')
    return result


def facts(answer):
    return {c.id: c for c in answer.claims if c.id.startswith('summary_')}


@pytest.mark.parametrize('unit,subject', [('source_currency','Revenue'), ('source_currency','Bookings'), ('count','Invoiced projects')])
def test_comparisons_lead_with_grounded_totals_and_explain_the_change_in_direction(unit, subject):
    result = comparison(unit=unit, subject=subject)
    result.rows.reverse()
    answer = ground_exploration(result)
    claims = facts(answer)
    assert answer.summary.startswith(subject + ' totalled')
    assert '10.0%' in answer.summary
    assert 'lower than a year earlier from January 2026 to February 2026, then higher' in answer.summary
    assert 'April 2026' in answer.summary and 'April 2025' in answer.summary
    assert 'matching records' not in answer.summary
    assert claims['summary_comparison_current'].value == 440
    assert claims['summary_comparison_baseline'].value == 400
    assert claims['summary_comparison_percentage_change'].value == 10
    assert claims['summary_comparison_latest'].sources == ['rows.0.2']
    assert claims['summary_comparison_month_0'].sources == ['rows.0.3', 'rows.0.2']
    assert answer.exploratory_chart.comparison == result.provenance.monthly_comparison


@pytest.mark.parametrize('unit,current,baseline,expected', [('ratio','.3','.2','10.0 percentage points'),
    ('percent','30','20','10.0 percentage points'), ('days','30','20','10 days')])
def test_rates_and_averages_compare_in_their_units_without_inventing_a_total(unit, current, baseline, expected):
    result = comparison(unit=unit, subject='Conversion' if unit != 'days' else 'Average gestation')
    for row in result.rows:
        row[2:] = [current, baseline]
    answer = ground_exploration(result)
    assert expected in answer.summary
    assert 'summary_comparison_current' not in facts(answer)
    assert 'totalled' not in answer.summary
    assert answer.exploratory_chart is not None


def test_explicit_pairing_supports_arbitrary_aliases_and_previous_periods():
    result = comparison(subject='Bookings')
    names = ['reporting_date', 'comparison_date', 'bookings_now', 'bookings_before']
    result.columns = names
    for c, name in zip(result.provenance.columns, names):
        c.name = name
    for i, row in enumerate(result.rows):
        row[0], row[1] = f'2026-{i+5:02}-01', f'2026-{i+1:02}-01'
    result.provenance.monthly_comparison = MonthlyComparison(current_month=names[0], baseline_month=names[1],
        current_value=names[2], baseline_value=names[3], kind='previous_period')
    answer = ground_exploration(result)
    assert 'corresponding month of the earlier period' in answer.summary
    assert 'May 2026 to August 2026' in answer.summary
    assert answer.exploratory_chart.x == names[0]


def test_legacy_shape_includes_redundant_month_labels_and_supporting_invoice_counts():
    result = comparison(explicit=False, additive=False)
    for row in result.rows:
        row.insert(0, datetime.fromisoformat(row[0]).strftime('%b'))
        row.append(2)
    result.provenance.columns.insert(0, ExploratoryColumn(name='fiscal_month_label', label='Month', role='dimension', type='text', unit='none'))
    result.provenance.columns.append(ExploratoryColumn(name='current_invoices', label='Invoices', role='measure', type='integer', unit='count'))
    result.columns = [c.name for c in result.provenance.columns]
    answer = ground_exploration(result)
    assert 'lower than a year earlier' in answer.summary
    assert answer.exploratory_chart.series == ['current_value', 'prior_value']
    assert 'summary_comparison_current' not in facts(answer)  # Pairing alone cannot establish additivity.


@pytest.mark.parametrize('mode', ['duplicate', 'offset', 'overlap', 'date', 'units', 'category', 'missing_field'])
def test_ambiguous_or_invalid_pairs_do_not_produce_comparative_claims_or_a_chart(mode):
    result = comparison()
    if mode == 'duplicate':
        result.rows.append(result.rows[0])
    elif mode == 'offset':
        result.rows[0][1] = '2024-01-01'
    elif mode == 'overlap':
        result.provenance.monthly_comparison.kind = 'previous_period'
        for row in result.rows:
            month = int(row[0][5:7])
            row[1] = f'2026-{month-1:02}-01' if month > 1 else '2025-12-01'
    elif mode == 'date':
        result.rows[0][0] = '2026-01-02'
    elif mode == 'units':
        result.provenance.columns[-1].unit = 'count'
    elif mode == 'category':
        result.provenance.columns.append(ExploratoryColumn(name='category', label='Category', role='dimension', type='text', unit='none'))
        result.columns.append('category')
        for row in result.rows:
            row.append('A')
    else:
        result.provenance.monthly_comparison.baseline_value = 'missing'
    answer = ground_exploration(result)
    assert answer.exploratory_chart is None
    assert not any(k.startswith('summary_comparison_') for k in facts(answer))
    assert "couldn't reliably align" in answer.summary


@pytest.mark.parametrize('mode', ['null', 'gap', 'truncated', 'query_limit'])
def test_partial_results_never_claim_full_period_totals_or_uninterrupted_trends(mode):
    result = comparison()
    if mode == 'null':
        result.rows[1][3] = None
    elif mode == 'gap':
        result.rows.pop(1)
    elif mode == 'truncated':
        result.provenance.truncated = True
    else:
        result.provenance.query_limited = True
    answer = ground_exploration(result)
    assert 'summary_comparison_current' not in facts(answer)
    assert 'Among the comparable months' in answer.summary
    assert answer.summary_notices
    if mode in {'truncated', 'query_limit'}:
        assert answer.exploratory_chart is None
        assert 'returned months' in answer.summary


@pytest.mark.parametrize('baseline', ['0', '-10'])
def test_nonpositive_baseline_has_absolute_comparison_without_a_misleading_percentage(baseline):
    result = comparison()
    for row in result.rows:
        row[3] = baseline
    answer = ground_exploration(result)
    assert 'higher' in answer.summary
    assert 'summary_comparison_percentage_change' not in facts(answer)


def test_current_incomplete_month_is_excluded_using_the_saved_business_time():
    result = comparison()
    result.provenance.freshness.queried_at = datetime(2026, 4, 20, tzinfo=timezone.utc)
    answer = ground_exploration(result)
    assert facts(answer)['summary_comparison_current'].value == 290
    assert facts(answer)['summary_comparison_latest'].sources == ['rows.2.2']
    assert 'April 2026' not in answer.summary
    assert any('incomplete month' in n for n in answer.summary_notices)
    # Explicit partial-month comparisons can opt in; their scope remains visible.
    result.provenance.monthly_comparison.completed_months_only = False
    assert facts(ground_exploration(result))['summary_comparison_current'].value == 440


def test_all_unknown_values_do_not_become_zero_and_no_completed_months_do_not_make_a_chart():
    result = comparison()
    for row in result.rows:
        row[3] = None
    assert 'no month has known values in both periods' in ground_exploration(result).summary
    result.provenance.freshness.queried_at = datetime(2026, 1, 15, tzinfo=timezone.utc)
    answer = ground_exploration(result)
    assert 'no completed months' in answer.summary
    assert answer.exploratory_chart is None


def test_saved_row_count_answer_is_refreshed_without_source_queries_or_writes():
    result = comparison(explicit=False, additive=False)
    answer = ground_exploration(result).model_dump(mode='json')
    answer.update(summary='There are 4 matching records in this result.', summary_version='2', exploratory_chart=None)
    service = object.__new__(WorkflowService)
    service.store = SimpleNamespace(result=AsyncMock(return_value=result.model_dump(mode='json')))
    presented = asyncio.run(service.present_answer('actor', answer, result.run_id))
    assert presented['summary_version'] == SUMMARY_VERSION
    assert 'matching records' not in presented['summary']
    assert presented['exploratory_chart']['comparison']['current_value'] == 'current_value'
    service.store.result.assert_awaited_once()
    assert answer['summary_version'] == '2'


def test_fiscal_window_crosses_years_and_preserves_matching_months_when_excluding_october():
    result = comparison(explicit=False, additive=False)
    months = [(2025, 11), (2025, 12), *[(2026, m) for m in range(1, 11)]]
    result.rows = [[f'{year}-{month:02}-01', f'{year-1}-{month:02}-01', '80' if i < 5 else '120', '100']
                   for i, (year, month) in enumerate(months)]
    result.provenance.columns.insert(0, ExploratoryColumn(name='fiscal_month_label', label='Month', role='dimension', type='text', unit='none'))
    result.columns.insert(0, 'fiscal_month_label')
    for row in result.rows:
        row.insert(0, 'Sept' if '-09-' in row[0] else datetime.fromisoformat(row[0]).strftime('%b'))
    answer = ground_exploration(result)
    assert 'November 2025 to March 2026, then higher' in answer.summary
    assert 'April 2026 to September 2026' in answer.summary
    assert 'November 2024 to September 2025' in answer.summary
    assert 'October 2026' not in answer.summary
    assert facts(answer)['summary_comparison_latest'].sources == ['rows.10.3']


def test_cutoff_is_the_business_month_at_query_time_not_the_host_or_viewing_date():
    result = comparison()
    # Already April in London, still March in UTC: March is a completed month.
    result.provenance.freshness.queried_at = datetime(2026, 3, 31, 23, 30, tzinfo=timezone.utc)
    answer = ground_exploration(result)
    assert facts(answer)['summary_comparison_latest'].sources == ['rows.2.2']
    assert facts(answer)['summary_comparison_current'].value == 290
