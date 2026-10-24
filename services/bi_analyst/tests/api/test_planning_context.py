"""Planner business context, permission boundaries and agreement with executable capabilities."""
from datetime import datetime, timezone
import json

import pytest

from bi_analyst.metrics.compiler import Compiler, InvalidMetricRequest
from bi_analyst.metrics.contracts import MetricRequest
from bi_analyst.workflow.planning_context import build_planning_context


COMPILER = Compiler()
NOW = datetime(2026, 10, 24, 12, tzinfo=timezone.utc)


def context(*ids):
    metrics = [COMPILER.metrics[name] for name in ids] if ids else list(COMPILER.metrics.values())
    return build_planning_context(COMPILER, metrics, now=NOW, business_timezone='Europe/London', max_rows=1000)


def test_planner_can_distinguish_stored_totals_monthly_actuals_and_populations():
    ctx = context()
    entries = {entry['id']:entry for entry in ctx['catalogue']}
    assert len(entries) == 16
    parent, child, hidden = (entries[name] for name in
                            ('invoice_project_value', 'invoice_stored_child_value', 'invoice_hidden_value'))
    assert len({entry['source']['grain'] for entry in (parent, child, hidden)}) == 3
    assert 'Signed values retained.' == parent['calculation']['negatives']
    assert 'all_stored' in parent['periods'] and parent['date_basis'] is None
    assert any('Do not apply business-status filters' in line for line in parent['meaning'])
    revenue = entries['invoice_monthly_actual']
    assert revenue['date_basis'] == 'invoice_date' and revenue['calculation']['negatives'] == 'Excluded.'
    assert "not the parent's first invoice date" in revenue['column_meanings']['invoice_date']['meaning']
    assert 'all business stages' in revenue['source']['description']
    assert revenue['capabilities']['filter_dimensions'] == ['category', 'type']
    bookings = entries['bookings_monthly_actual']
    assert bookings['date_basis'] == 'booking_month'
    assert 'date_order_received' in bookings['column_meanings']['booking_month']['meaning']
    assert 'date_created' in entries['enquiry_monthly_actual']['column_meanings']['enquiry_month']['meaning']
    assert 'Won - Open (Order Received)' in ' '.join(bookings['status_filters'])
    assert bookings['capabilities']['filter_dimensions'] == []
    populations = {p['id']:p['description'] for p in ctx['populations']}
    assert 'Genuine archived projects remain included' in populations['reportable']
    assert 'no project allocation' in populations['hidden_inventory']
    subtotal = entries['order_hidden_complete_subtotal']
    assert any('Only both-numeric rows' in line for line in subtotal['meaning'])
    assert subtotal['result_fields'] == ['value', 'source_rows', 'known_values']
    assert any('weighted actual_pipeline_value' in notice for notice in ctx['notices'])
    assert not any('pending certification' in notice for notice in ctx['notices'])


def test_planner_sees_exact_conversion_denominators_and_actual_gestation_rules():
    entries = {entry['id']:entry for entry in context()['catalogue']}
    for years in ('five', 'two'):
        inclusive = entries[f'conversion_{years}_year']
        closed = entries[f'conversion_closed_{years}_year']
        assert inclusive['supporting_counts']['denominator'] == 'sum(eligible_count)'
        assert closed['supporting_counts']['denominator'] == 'sum(closed_count)'
        assert 'zero denominator gives NULL' in closed['calculation']['zeroes']
        assert 'Not applicable to counts.' == closed['calculation']['negatives']
        assert {'numerator', 'denominator'} <= set(closed['result_fields'])
        assert not closed['capabilities']['include_share']
        gestation = entries[f'gestation_{years}_year']
        assert gestation['date_basis'] == 'date_created'
        assert gestation['calculation']['aggregation'] == 'mean'
        assert gestation['calculation']['zeroes'] == gestation['calculation']['negatives'] == 'Excluded.'
        assert 'first design completion to first invoice' in ' '.join(gestation['meaning'])
        assert any('Never substitute expected_gestation_days' in line for line in gestation['meaning'])
    median = entries['gestation_median_five_year']
    assert 'percentile_cont(0.5)' in median['calculation']['expression']
    assert median['capabilities']['grains'] == ['total']


@pytest.mark.parametrize('metric_id', list(COMPILER.metrics))
def test_advertised_grains_filters_ordering_and_shares_agree_with_compiler(metric_id):
    metric = COMPILER.metrics[metric_id]
    entry = context(metric_id)['catalogue'][0]
    caps = entry['capabilities']
    for grain in ('total', 'month'):
        for dimension in (None, 'category', 'type'):
            for shares in (False, True):
                dimensions = [dimension] if dimension else []
                request = MetricRequest(metric_id=metric.id, metric_version=metric.version,
                    population=metric.population, period=metric.periods[0], grain=grain,
                    dimensions=dimensions,
                    filters=[{'dimension':dimension, 'values':['example']}] if dimension else [],
                    include_share=shares)
                advertised = (grain in caps['grains']
                    and (dimension is None or dimension in caps['filter_dimensions'])
                    and (not shares or caps['include_share'] and bool(dimensions or grain == 'month')))
                try:
                    COMPILER.validate(request)
                    accepted = True
                except InvalidMetricRequest:
                    accepted = False
                assert accepted == advertised, (metric_id, grain, dimension, shares)
        if grain in caps['grains']:
            for column in caps['ordering_columns'][grain]:
                COMPILER.validate(MetricRequest(metric_id=metric.id, metric_version=metric.version,
                    population=metric.population, period=metric.periods[0], grain=grain,
                    dimensions=metric.dimensions, ordering=[{'column':column}]))


def test_restricted_catalogue_exposes_only_its_populations_periods_and_dimensions():
    ctx = context('invoice_hidden_value')
    assert [p['id'] for p in ctx['populations']] == ['hidden_inventory']
    assert [p['name'] for p in ctx['periods']] == ['all_stored']
    assert ctx['dimensions'] == []
    assert [m['id'] for m in ctx['catalogue']] == ['invoice_hidden_value']
    assert 'verified_active' not in {p['id'] for p in ctx['populations']}


def test_period_context_resolves_business_dates_without_narrowing_creation_cohorts():
    ctx = context()
    periods = {p['name']:p for p in ctx['periods']}
    assert ctx['today'] == '2026-10-24' and ctx['business_timezone'] == 'Europe/London'
    assert periods['last_month']['start_date'] == '2026-09-01'
    assert periods['last_month']['end_date_exclusive'] == '2026-10-01'
    assert periods['previous_month']['start_date'] == '2026-08-01'
    assert periods['completed_12_months']['start_date'] == '2025-10-01'
    assert periods['five_year_cohort']['start_date'] == '2021-10-24'
    assert periods['two_year_cohort']['start_date'] == '2024-10-24'
    assert periods['five_year_cohort']['end_date_exclusive'] is None


def test_all_definitions_leave_prompt_budget_for_full_ascii_question_and_replies():
    payload = {**context(), 'question':'Q' * 8000, 'replies':['R' * 2000] * 3,
               'previous_plan':{'metric_id':'invoice_monthly_actual', 'period':'completed_12_months',
                                'dimensions':['category', 'type'], 'grain':'month'}}
    assert len(json.dumps({'stage':'plan', 'data':payload}, ensure_ascii=True).encode()) < 64000
    assert COMPILER.catalogue.owner_accepted
