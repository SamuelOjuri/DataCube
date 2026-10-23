"""All certified shapes, metric variants and saved-answer presentation paths."""
import asyncio
from datetime import date, datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from fastapi import HTTPException

from bi_analyst.metrics.compiler import Compiler
from bi_analyst.metrics.contracts import Aggregate, Column, Freshness, MetricProvenance, MetricRequest, MetricResult
from bi_analyst.metrics.calculations import compare
from bi_analyst.semantic.periods import resolve_period
from bi_analyst.workflow.evidence import ground, select_presentation
from bi_analyst.workflow.explanation import LEADS, explain
from bi_analyst.workflow.service import WorkflowService
from bi_analyst.workflow.summary import summarize_metric
from bi_analyst.workflow.summary_language import SUMMARY_VERSION, period_scope, period_text
from bi_analyst.exploration.evidence import ground_exploration
from test_exploration_summary import monthly

NOW = datetime(2026, 10, 22, 12, tzinfo=timezone.utc)


def metric_result(metric_id='order_parent_value', *, rows=None, grain='total', dimensions=(), total=None,
                  truncated=False, comparison=None):
    compiler = Compiler()
    metric = compiler.metrics[metric_id]
    request = MetricRequest(metric_id=metric.id, metric_version=metric.version, population=metric.population,
        period=metric.periods[0], grain=grain, dimensions=list(dimensions), comparison=comparison)
    aggregate = Aggregate(value='0.582' if metric.unit == 'ratio' else '20' if metric.unit == 'days' else '100.50',
        source_rows=1000, known_values=1000, numerator=405 if metric.unit == 'ratio' else None,
        denominator=696 if metric.unit == 'ratio' else None) if total is None else total
    columns = [*([Column(name='month', type='date')] if grain == 'month' else []),
               *[Column(name=d, type='text') for d in dimensions], Column(name='value', type='decimal', unit=metric.unit)]
    if metric.unit == 'ratio':
        columns.extend(Column(name=n, type='decimal', unit='count') for n in ('numerator', 'denominator'))
    if rows is None:
        rows = [[*(['2026-09-01'] if grain == 'month' else []), *['A' for _ in dimensions],
                 aggregate.value, *([aggregate.numerator, aggregate.denominator] if metric.unit == 'ratio' else [])]]
    period = None if request.period == 'all_stored' else resolve_period(request.period, now=NOW, business_timezone='Europe/London')
    p = MetricProvenance(query_reference='a'*64, compiler_version='1.0.0', catalogue_version=compiler.catalogue.version,
        catalogue_sha256=compiler.catalogue_hash, metric_id=metric.id, metric_version=metric.version,
        metric_label=metric.label, unit=metric.unit, source_population=metric.population,
        source_relation='analyst_query.'+metric.relation, source_grain='Fixture records', request=request,
        resolved_period=period, business_timezone='Europe/London', columns=columns, total=aggregate,
        total_scope='complete_filtered_population', freshness=Freshness(queried_at=NOW), coverage={}, limitations=[],
        evaluation_only=True, certification_basis='local_evaluation', truncated=truncated,
        truncation_reasons=['row_limit'] if truncated else [], returned_rows=len(rows), matched_groups=len(rows)+int(truncated),
        dataset_scope='limited' if truncated else 'complete')
    return MetricResult(id=uuid4(), run_id=uuid4(), permissions_version=1, columns=[c.name for c in columns],
        rows=rows, provenance=p, created_at=NOW)


@pytest.mark.parametrize('metric_id', list(Compiler().metrics))
def test_every_catalogue_metric_produces_a_conversational_summary(metric_id):
    result = metric_result(metric_id)
    answer = ground(result, select_presentation(result), {})
    assert answer.summary and answer.summary != answer.text
    assert answer.summary_version == SUMMARY_VERSION
    assert 'end exclusive' not in answer.summary and 'GBP ' not in answer.summary
    assert any(c.id == 'summary_total' for c in answer.claims)
    assert ground(type(result).model_validate_json(result.model_dump_json()), select_presentation(result), {}).summary == answer.summary


def test_screenshot_enquiry_answers_with_a_readable_month_and_exact_evidence():
    result = metric_result('enquiry_monthly_actual', total=Aggregate(value='6055247.06', source_rows=1, known_values=1))
    answer = ground(result, select_presentation(result), {})
    assert answer.summary == 'New enquiry value totalled £6,055,247.06 in September 2026.'
    claim = next(c for c in answer.claims if c.id == 'summary_total')
    assert claim.value == Decimal('6055247.06')
    assert claim.sources == ['provenance.total.value']
    assert claim.period == '2026-09-01 to 2026-10-01 (end exclusive)'
    # The old evidence text remains available under Supporting figures.
    assert answer.text == 'Monthly New Enquiry Value: GBP 6055247.06.'


@pytest.mark.parametrize('start,end,label,scope', [
    ('2026-09-01', '2026-10-01', 'September 2026', 'in September 2026'),
    ('2025-10-01', '2026-10-01', 'October 2025 to September 2026', 'from October 2025 to September 2026'),
    ('2024-02-01', '2024-03-01', 'February 2024', 'in February 2024'),
    ('2024-02-15', '2024-03-01', '15 February 2024 to 29 February 2024', 'from 15 February 2024 to 29 February 2024'),
    ('2026-09-30', '2026-10-01', '30 September 2026', 'on 30 September 2026'),
    ('2024-10-22', None, 'projects created on or after 22 October 2024', 'for projects created on or after 22 October 2024'),
])
def test_readable_periods_preserve_saved_calendar_boundaries(start, end, label, scope):
    period = SimpleNamespace(start_date=date.fromisoformat(start), end_date_exclusive=date.fromisoformat(end) if end else None)
    assert period_text(period) == label
    assert period_scope(period) == scope
    assert period_scope(None) == 'across all recorded history'


def test_comparison_names_the_saved_month_and_not_a_relative_current_period():
    result = metric_result('enquiry_monthly_actual', comparison={'period':'previous_month'})
    result.provenance.comparison_period = resolve_period('previous_month', now=NOW, business_timezone='Europe/London')
    result.provenance.comparison = compare(result.provenance.total, Aggregate(value='50', source_rows=1, known_values=1), unit='source_currency')
    assert summarize_metric(result)[0] == ('New enquiry value totalled £100.50 in September 2026. '
        'That was £50.50 higher than the comparison value of £50.00 in August 2026.')


def test_single_month_does_not_repeat_the_same_value_as_first_and_latest():
    answer = summarize_metric(metric_result('enquiry_monthly_actual', grain='month'))[0]
    assert answer == 'New enquiry value totalled £100.50 in September 2026.'


def test_screenshot_closed_only_conversion_explains_counts_scope_and_exclusions():
    result = metric_result('conversion_closed_two_year')
    answer = ground(result, select_presentation(result), {})
    assert '405 of the 696 closed projects won and invoiced' in answer.summary
    assert answer.summary.startswith('The closed-only conversion rate was 58.2%')
    assert 'projects created on or after 22 October 2024' in answer.summary
    assert any('Open projects are excluded' in n for n in answer.summary_notices)
    assert any('less time' in n for n in answer.summary_notices)
    claims = {c.id:c for c in answer.claims}
    assert claims['summary_numerator'].sources == ['provenance.total.numerator']
    assert claims['summary_denominator'].value == 696


def test_inclusive_conversion_does_not_claim_open_projects_are_excluded():
    result = metric_result('conversion_two_year')
    summary, _, notices = summarize_metric(result)
    assert '696 eligible projects' in summary and 'inclusive' in summary
    assert not any('Open projects are excluded' in n for n in notices)


@pytest.mark.parametrize('metric_id,word', [('gestation_five_year','average'), ('gestation_median_five_year','median')])
def test_gestation_uses_the_correct_statistic(metric_id, word):
    assert f'The {word} observed gestation' in summarize_metric(metric_result(metric_id))[0]


@pytest.mark.parametrize('value,known,source,expected', [(None,0,0,'No matching records'),
    (None,0,2,'unknown'), ('0',2,2,'£0.00'), ('-15.25',2,2,'£-15.25'), ('10',1,2,'known subtotal')])
def test_empty_unknown_zero_signed_and_partial_results_have_distinct_summaries(value, known, source, expected):
    result = metric_result(total=Aggregate(value=value, source_rows=source, known_values=known))
    answer = ground(result, select_presentation(result), {})
    assert expected in answer.summary
    if known < source:
        assert any('unknown values' in n for n in answer.summary_notices)


def test_zero_rate_denominator_is_undefined_not_zero_conversion():
    result = metric_result('conversion_closed_two_year', total=Aggregate(value=None, source_rows=3, known_values=3,
        numerator=0, denominator=0))
    assert 'no closed projects' in summarize_metric(result)[0]
    assert 'cannot be calculated' in summarize_metric(result)[0]


def test_monthly_conversion_uses_all_counts_not_the_average_of_rates():
    result = metric_result('conversion_two_year', grain='month', rows=[['2026-01-01','0.5',1,2],
        ['2026-02-01','0.5',1,2], ['2026-03-01','0.09',9,100], ['2026-04-01','0.09',9,100]])
    answer = ground(result, select_presentation(result), {})
    claims = {c.id:c for c in answer.claims}
    assert claims['summary_first_half'].value == Decimal('.5') and claims['summary_second_half'].value == Decimal('.09')
    assert '41.0 percentage points lower' in answer.summary
    assert claims['summary_overall'].metric_id == 'conversion_two_year'


def test_limited_months_keep_the_full_total_and_only_describe_returned_months():
    result = metric_result(grain='month', rows=[['2026-01-01','10'], ['2026-02-01',None]], truncated=True)
    summary, claims, notices = summarize_metric(result)
    assert '£100.50' in summary and 'Among the returned months' in summary
    assert not any(c.id == 'summary_change' for c in claims)
    assert any('complete filtered population' in n for n in notices)
    assert not any('summary covers only' in n for n in notices)


@pytest.mark.parametrize('dimensions', [('category',), ('category','type')])
def test_category_and_multiple_breakdowns_name_the_leading_returned_group(dimensions):
    result = metric_result(dimensions=dimensions, rows=[[*['A' for _ in dimensions],'10'],[*['B' for _ in dimensions],'90']], truncated=True)
    summary, claims, _ = summarize_metric(result)
    assert 'Among the returned groups' in summary and '£90.00' in summary
    assert next(c for c in claims if c.id == 'summary_leader').sources == [f'rows.1.{len(dimensions)}']


def test_monthly_category_breakdown_names_the_month_as_well_as_the_category():
    result = metric_result(grain='month',dimensions=['category'], rows=[['2026-01-01','A','90'],['2026-02-01','A','10']])
    summary, _, _ = summarize_metric(result)
    assert 'January 2026 / A' in summary and 'highest value at £90.00' in summary


def test_a_result_without_a_requested_total_describes_only_the_displayed_groups():
    result = metric_result(dimensions=['category'],rows=[['A','90']])
    result.provenance.total = None
    result.provenance.total_scope = 'not_requested'
    summary, claims, _ = summarize_metric(result)
    assert 'an overall figure was not requested' in summary and 'highest value' in summary
    assert not any(c.id == 'summary_total' for c in claims)


def test_unknown_groups_are_excluded_explicitly_when_naming_a_leader():
    result = metric_result(dimensions=['category'],rows=[['A',None],['B','90']])
    assert 'Among the groups with known values, B' in summarize_metric(result)[0]


@pytest.mark.parametrize('unit', ['money','rate'])
def test_comparison_uses_stored_absolute_or_percentage_point_change(unit):
    result = metric_result('conversion_two_year' if unit == 'rate' else 'order_parent_value', comparison={'period':'all_stored','filters':[]})
    baseline = Aggregate(value='.5' if unit == 'rate' else '50',source_rows=10,known_values=10)
    result.provenance.comparison = compare(result.provenance.total, baseline, unit=result.provenance.unit)
    summary, _, _ = summarize_metric(result)
    assert ('8.2 percentage points higher' if unit == 'rate' else '£50.50 higher') in summary


def test_zero_unknown_and_partial_comparison_baselines_have_visible_caveats():
    result = metric_result(comparison={'period':'all_stored','filters':[{'dimension':'category','values':['Other']}]})
    for value, known, expected in [(0,3,'undefined'), (None,0,'unknown')]:
        baseline = Aggregate(value=value,source_rows=3,known_values=known)
        result.provenance.comparison = compare(result.provenance.total, baseline, unit=result.provenance.unit)
        _, _, notices = summarize_metric(result)
        assert any(expected in n for n in notices)
        assert any('different filters' in n for n in notices)


@pytest.mark.parametrize('reason', list(LEADS))
def test_every_explanation_path_has_visible_plain_text_without_result_figures(reason):
    answer = explain(reason)
    assert answer.text.startswith(LEADS[reason]) and answer.kind == 'explanation'


@pytest.mark.parametrize('exploratory', [False, True])
def test_legacy_answers_are_enriched_from_owned_saved_results_without_requery_or_write(exploratory):
    result = monthly() if exploratory else metric_result('conversion_closed_two_year')
    answer = ground_exploration(result) if exploratory else ground(result, select_presentation(result), {})
    old = answer.model_dump(mode='json')
    for key in ('summary','summary_version','summary_notices','exploratory_chart'):
        old.pop(key)
    old['claims'] = [c for c in old['claims'] if not c['id'].startswith('summary_')]
    service = object.__new__(WorkflowService)
    service.store = SimpleNamespace(result=AsyncMock(return_value=result.model_dump(mode='json')))
    presented = asyncio.run(service.present_answer('actor', old, result.run_id))
    assert presented['summary'] == answer.summary and presented['text'] == old['text']
    assert presented['summary_version'] == SUMMARY_VERSION
    assert presented['summary_notices']
    assert 'summary' not in old
    service.store.result.assert_awaited_once_with('actor', result.id)


def test_legacy_summary_reads_cannot_cross_result_references_or_permissions():
    result = metric_result()
    answer = ground(result, select_presentation(result), {}).model_dump(mode='json')
    answer['summary'] = None
    service = object.__new__(WorkflowService)
    service.store = SimpleNamespace(result=AsyncMock(return_value=result.model_dump(mode='json')))
    with pytest.raises(HTTPException) as denied:
        asyncio.run(service.present_answer('actor', answer, uuid4()))
    assert denied.value.status_code == 409
    service.store.result.side_effect = HTTPException(403, 'permissions_changed')
    with pytest.raises(HTTPException) as revoked:
        asyncio.run(service.present_answer('actor', answer, result.run_id))
    assert revoked.value.status_code == 403


@pytest.mark.parametrize('exploratory', [False, True])
@pytest.mark.parametrize('old_version', [None, '1', '2', '3'])
def test_existing_outdated_summaries_are_refreshed_without_mutating_history(exploratory, old_version):
    result = monthly() if exploratory else metric_result('enquiry_monthly_actual')
    answer = ground_exploration(result) if exploratory else ground(result, select_presentation(result), {}, asks_for_cause=True)
    old = answer.model_dump(mode='json')
    old.update(summary='Old mechanical summary.', summary_version=old_version)
    old['summary_notices'].append('Old presentation caveat.')
    old['notices'].append('Old presentation caveat.')
    service = object.__new__(WorkflowService)
    service.store = SimpleNamespace(result=AsyncMock(return_value=result.model_dump(mode='json')))
    presented = asyncio.run(service.present_answer('actor', old, result.run_id))
    assert presented['summary'] == answer.summary and presented['summary_version'] == SUMMARY_VERSION
    assert presented['claims'] == answer.model_dump(mode='json')['claims']
    assert presented['text'] == old['text'] and presented['scope_changes'] == old['scope_changes']
    assert 'Old presentation caveat.' not in presented['notices']
    assert presented['summary_notices'] == answer.summary_notices
    assert old['summary'] == 'Old mechanical summary.' and old['summary_version'] == old_version
    service.store.result.assert_awaited_once_with('actor', result.id)
    assert asyncio.run(service.present_answer('actor', presented, result.run_id)) == presented
    service.store.result.assert_awaited_once()  # Current summaries do not need regeneration.


def test_public_reads_refresh_existing_unversioned_summaries():
    result = metric_result('enquiry_monthly_actual')
    answer = ground(result, select_presentation(result), {})
    old = answer.model_copy(update={'summary': 'Old summary', 'summary_version': None})
    service = object.__new__(WorkflowService)
    service.store = SimpleNamespace(result=AsyncMock(return_value=result.model_dump(mode='json')))
    service.jobs = SimpleNamespace(public=lambda row: SimpleNamespace(answer=old, run_id=result.run_id))
    refreshed = asyncio.run(service.public('actor', {}))
    assert refreshed.answer.summary == answer.summary
    assert refreshed.answer.summary_version == SUMMARY_VERSION


@pytest.mark.parametrize('mismatch', ['run_id', 'query_reference', 'catalogue_sha256', 'permissions'])
def test_outdated_summary_refresh_rechecks_owned_evidence(mismatch):
    result = metric_result()
    old = ground(result, select_presentation(result), {}).model_dump(mode='json')
    old.update(summary='Already has text', summary_version='1')
    stored = result.model_dump(mode='json')
    if mismatch == 'run_id':
        stored['run_id'] = str(uuid4())
    elif mismatch != 'permissions':
        stored['provenance'][mismatch] = 'b' * 64
    service = object.__new__(WorkflowService)
    service.store = SimpleNamespace(result=AsyncMock(return_value=stored))
    if mismatch == 'permissions':
        service.store.result.side_effect = HTTPException(403, 'permissions_changed')
    with pytest.raises(HTTPException) as error:
        asyncio.run(service.present_answer('actor', old, result.run_id))
    assert error.value.status_code == (403 if mismatch == 'permissions' else 409)
