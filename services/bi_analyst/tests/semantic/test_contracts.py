import ast
from datetime import date, datetime, timezone
from decimal import Decimal
import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from bi_analyst.semantic import Catalogue, load_catalogue
from bi_analyst.semantic.periods import resolve_period
from verify_frozen import compare_results, frozen_ctes, statements, view_definitions

ROOT = Path(__file__).resolve().parents[4]


def test_catalogue_complete_and_migration_surface_matches():
    catalogue = load_catalogue()
    assert {m.family for m in catalogue.metrics} == {'enquiry','order','invoice','conversion','gestation'}
    assert view_definitions().keys() == {r.id for r in catalogue.relations if not r.optional}
    assert len(statements(Path(__file__).with_name('parity.sql'))) == len(catalogue.metrics)
    assert catalogue.resolve('raw enquiry value').id == 'new_enquiry_value'


@pytest.mark.parametrize('name', ['order value', 'invoiced value', 'weighted enquiry', 'not a metric'])
def test_ambiguous_and_unknown_metrics_require_clarification(name):
    with pytest.raises(ValueError, match='clarification'):
        load_catalogue().resolve(name)


@pytest.mark.parametrize('change', ['unknown_column','population','join_fanout','duplicate','extra','missing_ratio'])
def test_invalid_catalogue_rejected(change):
    data = json.loads(load_catalogue().model_dump_json())
    if change == 'unknown_column':
        data['metrics'][0]['value_columns'] = ['missing']
    elif change == 'population':
        data['metrics'][0]['population'] = 'verified_active'
    elif change == 'join_fanout':
        data['joins'][0]['right'] = 'children_v1'
    elif change == 'duplicate':
        data['metrics'].append(data['metrics'][0])
    elif change == 'extra':
        data['metrics'][0]['enabled'] = True
    else:
        next(m for m in data['metrics'] if m['family']=='conversion')['calculation']['denominator'] = None
    with pytest.raises(ValidationError):
        Catalogue.model_validate(data)


def test_pending_and_unsupported_populations_are_never_queryable():
    catalogue = load_catalogue()
    for metric in catalogue.metrics:
        with pytest.raises(ValueError, match='certification'):
            catalogue.require_queryable(metric.id, metric.population)
    with pytest.raises(ValueError, match='population'):
        catalogue.require_queryable('new_enquiry_value', 'verified_active')
    assert not next(p for p in catalogue.populations if p.id=='historical_snapshot').available


def test_money_does_not_allow_membership_expansion():
    catalogue = load_catalogue()
    assert all(not {'account','product_type'} & set(m.dimensions) for m in catalogue.metrics)
    assert all(j.cardinality=='one_to_zero_or_one' for j in catalogue.joins if j.purpose=='project_measures')


def test_relative_period_uses_local_business_day_at_month_boundary():
    period = resolve_period('last_month', now=datetime(2026,9,30,23,30,tzinfo=timezone.utc), business_timezone='Europe/London')
    assert period.as_of_date == date(2026,10,1)
    assert period.start_date == date(2026,9,1)
    assert period.end_date_exclusive == date(2026,10,1)
    assert period.start_utc.isoformat() == '2026-08-31T23:00:00+00:00'


def test_dst_and_leap_boundaries_are_retained():
    period = resolve_period('last_month', now=datetime(2026,4,1,tzinfo=timezone.utc), business_timezone='Europe/London')
    assert (period.end_utc_exclusive-period.start_utc).total_seconds() == (31*24-1)*3600
    leap = resolve_period('five_year_cohort', now=datetime(2024,2,29,tzinfo=timezone.utc), business_timezone='Europe/London')
    assert leap.start_date == date(2019,2,28) and leap.end_date_exclusive is None
    assert not leap.completed_months_only


@pytest.mark.parametrize('name', ['month_to_date','fiscal_year','today'])
def test_unagreed_periods_rejected(name):
    with pytest.raises(ValueError, match='Unsupported'):
        resolve_period(name, now=datetime.now(timezone.utc), business_timezone='Europe/London')


def test_naive_time_rejected():
    with pytest.raises(ValueError, match='offset-aware'):
        resolve_period('last_month',now=datetime(2026,10,7),business_timezone='Europe/London')


def test_frozen_expansion_cannot_contact_mutable_sources():
    query = frozen_ctes('bi_eval_20261007_v1')
    assert 'public.' not in query and 'CURRENT_DATE' not in query and 'analytics.' not in query
    with pytest.raises(ValueError):
        frozen_ctes('public; DROP SCHEMA public')


def test_archive_coverage_interface_exactly_reuses_existing_gate():
    tree = ast.parse((ROOT/'src/services/monday_archive.py').read_text(encoding='utf-8'))
    fn = next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='coverage')
    query = next(n.args[0].value for n in ast.walk(fn) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute) and n.func.attr=='execute')
    for value in ['verified_archive_v1','1825117125','1825117144','1825138260']:
        query=query.replace('%s',f"'{value}'",1)
    sql = (ROOT/'src/database/migrations/20261007_002_analytics_archive_coverage.sql').read_text(encoding='utf-8')
    assert query.strip() in sql


def test_parity_preserves_types_nulls_multiplicity_and_exact_money():
    def result(values,oid=1700):
        return {'columns':[{'name':'amount','type_oid':oid}], 'rows':[{'amount':v} for v in values]}
    assert compare_results(result([Decimal('100.01'),None]),result([None,'100.01']))['matches']
    for actual in [result([Decimal('100.00'),None]),result([Decimal('100.01'),0]),
                   result([Decimal('100.01'),None,None]),result([Decimal('100.01'),None],701)]:
        assert not compare_results(actual,result(['100.01',None]))['matches']
