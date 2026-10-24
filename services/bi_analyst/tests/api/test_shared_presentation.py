"""Behavioural cases shared with the browser: intent is checked against evidence."""
import asyncio
from datetime import datetime, timezone
from decimal import Decimal
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from pydantic import ValidationError

from bi_analyst.exploration.contracts import AnalysisIntent, ExploratoryColumn, PeriodComparison
from bi_analyst.exploration.evidence import ground_exploration
from bi_analyst.exploration.semantics import describe_columns
from bi_analyst.answer_presentation.service import present_explanation, present_result
from bi_analyst.workflow.errors import EvidenceError
from bi_analyst.workflow.explanation import explain
from bi_analyst.workflow.service import WorkflowService
from bi_analyst.workflow.summary_language import SUMMARY_VERSION
from bi_analyst.metrics.contracts import Ordering
from test_answer_summaries import metric_result
from test_exploration_summary import calendar
from test_exploration_workflow import exploratory_result

NOW = datetime(2026, 10, 23, 12, tzinfo=timezone.utc)
RESULT_ID = UUID('20000000-0000-4000-8000-000000000001')
RUN_ID = UUID('30000000-0000-4000-8000-000000000001')
FIXTURE = Path(__file__).resolve().parents[4] / 'web/tests/summary-fixtures.json'


def col(name, *, unit='source_currency', role='measure', grain=None, grouped=False, additive=False, label=None):
    return ExploratoryColumn(name=name, label=label or name.replace('_', ' ').capitalize(), role=role, unit=unit,
        type='date' if grain else 'text' if role == 'dimension' else 'decimal', time_grain=grain,
        grouped=grouped, additive=additive)


def dataset(rows, columns, intent=None, title='Returned analysis'):
    result = exploratory_result(rows, columns, title=title)
    result.id, result.run_id, result.created_at = RESULT_ID, RUN_ID, NOW
    result.provenance.freshness.queried_at = NOW
    result.provenance.analysis_intent = AnalysisIntent.model_validate(intent) if intent else None
    return result


def cases():
    category = col('account', unit='none', role='dimension')
    revenue = col('revenue', grouped=True, additive=True)
    duration = col('delivery_time', unit='days', label='Delivery time')
    return {
        'calendar': calendar().model_copy(update={'id': RESULT_ID, 'run_id': RUN_ID, 'created_at': NOW}),
        'total': dataset([['1234.56']], [revenue], {'kind': 'total'}, 'Total revenue'),
        'ranking': dataset([['A', 10], ['B', 30], ['C', 5]], [category, revenue],
            {'kind': 'ranking', 'primary_measure': 'revenue', 'direction': 'lowest'}, 'Lowest revenue by account'),
        'comparison': dataset([['0.6', '0.4']], [col('current_rate', unit='ratio'), col('baseline_rate', unit='ratio')],
            {'kind': 'comparison', 'primary_measure': 'current_rate', 'secondary_measure': 'baseline_rate'}, 'Conversion comparison'),
        'daily': dataset([['2026-10-20', 20], ['2026-10-21', None], ['2026-10-23', 50]],
            [col('day', unit='none', role='dimension', grain='day'), revenue], {'kind': 'trend'}, 'Daily revenue'),
        'quarterly': dataset([['2026-01-01', 20], ['2026-04-01', 30], ['2026-07-01', 50]],
            [col('quarter', unit='none', role='dimension', grain='quarter'), revenue], {'kind': 'trend'}, 'Quarterly revenue'),
        'distribution': dataset([[1], [2], [3], [4], [None]], [duration],
            {'kind': 'distribution', 'primary_measure': 'delivery_time'}, 'Delivery time distribution'),
        'relationship': dataset([[10, 1], [20, 2], [30, 3], [None, 4]], [revenue, duration],
            {'kind': 'relationship', 'primary_measure': 'revenue', 'secondary_measure': 'delivery_time'}, 'Revenue and delivery time'),
        'fallback': dataset([['A', 10], ['B', 30]], [category, revenue],
            {'kind': 'trend', 'axis': 'account'}, 'Revenue details'),
        'records': dataset([['A'], ['B']], [category], {'kind': 'records'}, 'Matching accounts'),
        'empty': dataset([], [category, revenue], {'kind': 'breakdown'}, 'No matching accounts'),
        'unknown': dataset([[None]], [revenue], {'kind': 'total'}, 'Revenue unavailable'),
    }


def ui_fixtures():
    output = {}
    for name, result in cases().items():
        result.provenance.freshness.queried_at = NOW
        answer = ground_exploration(result).model_dump(mode='json')
        answer.pop('claims')
        output[name] = {'result': result.model_dump(mode='json'), 'answer': answer}
    return output


def test_browser_fixtures_are_actual_backend_presentations():
    assert json.loads(FIXTURE.read_text(encoding='utf-8')) == ui_fixtures()


@pytest.mark.parametrize('name', list(cases()))
def test_every_result_shape_has_a_useful_summary_and_owned_claims(name):
    result = cases()[name]
    rendered = present_result(result)
    assert rendered.summary.strip() and rendered.summary != f'There are {len(result.rows)} matching records in this result.'
    assert ground_exploration(result).summary == rendered.summary
    assert present_result(type(result).model_validate_json(result.model_dump_json())).summary == rendered.summary
    for claim in rendered.claims:
        assert claim.result_id == result.id and claim.sources
        for source in claim.sources:
            if source.startswith('rows.'):
                _, row, column = source.split('.')
                assert int(row) < len(result.rows) and int(column) < len(result.columns)


def test_scalar_comparison_uses_percentage_points_and_has_no_relative_percentage():
    rendered = present_result(cases()['comparison'])
    assert '20.0 percentage points higher' in rendered.summary
    assert rendered.chart.kind == 'comparison'
    assert not any(c.kind == 'percentage_change' for c in rendered.claims)
    result = cases()['comparison']
    result.provenance.columns[1].unit = 'days'
    fallback = present_result(result)
    assert fallback.chart is None and 'reliable baseline' in fallback.summary


def test_ranking_direction_ties_and_limited_populations_are_not_overstated():
    result = cases()['ranking']
    rendered = present_result(result)
    assert 'C had the lowest revenue' in rendered.summary and rendered.chart.sort == 'ascending'
    result.rows[0][1] = 5
    tied = present_result(result)
    assert 'A, C tied' in tied.summary and not any(c.kind == 'share' for c in tied.claims)
    result.provenance.query_limited = True
    limited = present_result(result)
    assert limited.chart is None and any('only the returned rows' in n for n in limited.notices)
    assert limited.chart_reason


@pytest.mark.parametrize('grain,dates,expected', [
    ('day', ['2026-10-20', '2026-10-21'], '21 October 2026'),
    ('week', ['2026-10-05', '2026-10-12'], 'week of 12 October 2026'),
    ('month', ['2026-08-01', '2026-09-01'], 'September 2026'),
    ('quarter', ['2026-04-01', '2026-07-01'], 'Q3 2026'),
    ('year', ['2024-01-01', '2025-01-01'], '2025'),
])
def test_trend_grains_are_derived_from_sql_and_sorted_by_date(grain, dates, expected):
    columns = [col('period', role='dimension', unit='none', grain=grain), col('value', additive=True)]
    columns, _ = describe_columns(f"SELECT date_trunc('{grain}', event_date)::date AS period, sum(amount) AS value FROM events GROUP BY 1", columns)
    assert columns[0].time_grain == grain
    result = dataset([[dates[1], 30], [dates[0], 10]], columns, {'kind': 'trend'})
    rendered = present_result(result)
    assert expected in rendered.summary and rendered.chart.kind == 'line'
    assert next(c for c in rendered.claims if c.kind == 'latest').sources == ['rows.0.1']
    result.rows[0][0] = dates[0]
    assert present_result(result).chart is None  # Duplicate periods do not create a trend.


@pytest.mark.parametrize('grain,current,prior', [
    ('day', ['2026-10-20', '2026-10-21'], ['2025-10-20', '2025-10-21']),
    ('week', ['2026-01-05', '2026-01-12'], ['2025-01-06', '2025-01-13']),
    ('quarter', ['2026-04-01', '2026-07-01'], ['2025-04-01', '2025-07-01']),
    ('year', ['2025-01-01'], ['2024-01-01']),
])
def test_period_comparisons_support_nonmonthly_grains(grain, current, prior):
    columns = [col('now', role='dimension', unit='none', grain=grain), col('then', role='dimension', unit='none', grain=grain),
               col('value', additive=True), col('prior', additive=True)]
    pairing = PeriodComparison(current_axis='now', baseline_axis='then', current_value='value', baseline_value='prior',
        time_grain=grain, kind='year_over_year')
    result = dataset([[a, b, 120, 100] for a, b in zip(current, prior)], columns,
        {'kind': 'comparison', 'period_comparison': pairing.model_dump()})
    rendered = present_result(result)
    assert rendered.chart.period_comparison == pairing and rendered.chart.time_grain == grain
    assert '20.0%' in rendered.summary
    result.rows[0][1] = '2024-01-02'
    assert present_result(result).chart is None


def test_distribution_median_uses_known_values_and_preserves_group_scope():
    result = cases()['distribution']
    rendered = present_result(result)
    median = next(c for c in rendered.claims if c.kind == 'median')
    assert median.value == Decimal('2.5') and median.sources == ['rows.0.0', 'rows.1.0', 'rows.2.0', 'rows.3.0']
    assert rendered.chart.kind == 'histogram'
    result.provenance.columns[0].grouped = True
    assert any('group values' in n for n in present_result(result).notices)
    result.rows = [[8], [2], [3]]
    assert next(c for c in present_result(result).claims if c.kind == 'median').value == 3


def test_relationship_only_uses_complete_pairs_without_inventing_a_cause():
    result = cases()['relationship']
    rendered = present_result(result)
    correlation = next(c for c in rendered.claims if c.kind == 'correlation')
    assert correlation.value == 1 and len(correlation.sources) == 6
    assert 'not a cause' in rendered.summary and rendered.chart.kind == 'scatter'
    result.rows = [[10, 1], [20, 1], [30, 1]]
    constant = present_result(result)
    assert 'constant' in constant.summary and not any(c.kind == 'correlation' for c in constant.claims)
    result.rows = [[10, 1], [20, 2]]
    assert present_result(result).chart is None


def test_unsupported_intent_keeps_useful_descriptive_findings_and_a_reason():
    result = cases()['fallback']
    rendered = present_result(result)
    assert 'distinct reporting periods' in rendered.summary and '£40.00' in rendered.summary
    assert rendered.chart_reason and rendered.chart is None
    result.provenance.analysis_intent.primary_measure = 'nonexistent'
    assert "couldn't match" in present_result(result).summary


def test_intent_cannot_supply_numerical_evidence_or_chart_code():
    with pytest.raises(ValidationError):
        AnalysisIntent.model_validate({'kind': 'total', 'finding': 'Revenue was £10000000'})
    result = cases()['total']
    result.rows[0][0] = 'NaN'
    with pytest.raises(EvidenceError):
        present_result(result)


def test_saved_nonmonthly_answer_is_refreshed_without_requery_or_write():
    result = cases()['quarterly']
    result.provenance.sql = "SELECT date_trunc('quarter', date_created)::date AS quarter, sum(amount) AS revenue FROM events GROUP BY 1"
    answer = ground_exploration(result)
    old = answer.model_dump(mode='json')
    old.update(summary='There are 3 matching records in this result.', summary_version='3')
    service = object.__new__(WorkflowService)
    service.store = SimpleNamespace(result=AsyncMock(return_value=result.model_dump(mode='json')))
    refreshed = asyncio.run(service.present_answer('actor', old, result.run_id))
    assert refreshed['summary'] == answer.summary and refreshed['exploratory_chart']['time_grain'] == 'quarter'
    assert old['summary_version'] == '3'
    service.store.result.assert_awaited_once_with('actor', result.id)


def test_saved_calendar_answer_is_refreshed_with_explicit_grain_without_changing_stored_metadata():
    result = calendar()
    result.provenance.columns[0].time_grain = None
    old = ground_exploration(result).model_dump(mode='json')
    old['summary_version'] = '6'
    stored = result.model_dump(mode='json')
    service = object.__new__(WorkflowService)
    service.store = SimpleNamespace(result=AsyncMock(return_value=stored))
    refreshed = asyncio.run(service.present_answer('actor', old, result.run_id))
    assert old['summary_kind'] == 'records' and old['exploratory_chart'] is None
    assert refreshed['summary_kind'] == 'trend' and refreshed['chart_reason'] is None
    assert refreshed['summary_version'] == SUMMARY_VERSION
    assert refreshed['exploratory_chart']['time_grain'] == 'month'
    assert stored['provenance']['columns'][0]['time_grain'] is None
    service.store.result.assert_awaited_once_with('actor', result.id)


def test_blank_explanations_still_have_a_conversational_summary():
    answer = explain('data_unavailable').model_copy(update={'text': '  '})
    rendered = present_explanation(answer)
    assert 'available data' in rendered.summary and rendered.summary_version == SUMMARY_VERSION


def test_certified_rankings_respect_the_requested_direction_and_acknowledge_ties():
    result = metric_result(dimensions=['category'], rows=[['A', 10], ['B', 30], ['C', 10]])
    result.provenance.request.ordering = [Ordering(column='value', direction='asc')]
    rendered = present_result(result)
    assert rendered.kind == 'ranking'
    assert rendered.summary.startswith('Among the groups, A was tied for the lowest value at £10.00.')


def test_existing_result_access_module_remains_importable():
    from bi_analyst.api import create_app
    from bi_analyst.presentation import Feedback, projects, save_feedback
    assert callable(create_app) and callable(projects) and callable(save_feedback)
    assert Feedback(rating='helpful').rating == 'helpful'


def test_project_reference_and_name_share_a_category_axis_and_preserve_identity():
    columns = [col('project', role='dimension', unit='none'), col('project_name', role='dimension', unit='none'),
               col('order_value', additive=True, grouped=True)]
    result = dataset([['0018952', 'North site', 30], ['18951', 'School', 10], ['18950', None, 5]], columns,
                     {'kind': 'ranking', 'axis': 'project', 'primary_measure': 'order_value'})
    rendered = present_result(result)
    assert '0018952 — North site' in rendered.summary
    assert rendered.chart.kind == 'bar' and rendered.chart.x == 'project'
    compared = dataset(result.rows[:2], columns,
                       {'kind': 'comparison', 'axis': 'project', 'primary_measure': 'order_value'})
    assert present_result(compared).chart.kind == 'bar'
    assert '0018952 — North site' in present_result(compared).summary
    result.rows[0][1] = None
    assert '0018952 had the highest' in present_result(result).summary
    result.rows[0][1] = 'North site'
    result.rows[1][:2] = ['0018952', 'South site']
    assert present_result(result).chart.kind == 'bar'
    result.rows[1][1] = 'North site'
    assert present_result(result).chart is None
    result.provenance.columns.append(col('account', role='dimension', unit='none'))
    result.columns.append('account')
    result.rows = [row + ['Account A'] for row in result.rows]
    result.rows[1][1] = 'South site'
    assert present_result(result).chart is None


if __name__ == '__main__':
    FIXTURE.write_text(json.dumps(ui_fixtures(), indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
