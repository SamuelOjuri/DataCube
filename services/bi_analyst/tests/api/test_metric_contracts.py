from datetime import datetime, timezone
from decimal import Decimal

import pytest
from pydantic import ValidationError

from bi_analyst.metrics.calculations import compare, share
from bi_analyst.metrics.compiler import Compiler, InvalidMetricRequest
from bi_analyst.metrics.contracts import Aggregate, MetricRequest
from bi_analyst.settings import Settings

NOW = datetime(2026, 10, 8, tzinfo=timezone.utc)


def request(**updates):
    return MetricRequest.model_validate(dict(metric_id="order_parent_value", metric_version="1.0.0",
                                              population="reportable", period="all_stored", **updates))


@pytest.mark.parametrize("update", [
    {"sql": "SELECT pg_sleep(20)"}, {"limit": 1001}, {"limit": True},
    {"dimensions": ["account"]}, {"population": "verified_active"},
    {"filters": [{"dimension": "category", "operator": "in", "values": []}]},
    {"filters": [{"dimension": "category", "operator": "is_null", "values": ["A"]}]},
])
def test_rejects_untyped_or_unbounded_inputs(update):
    with pytest.raises(ValidationError):
        MetricRequest.model_validate({**request().model_dump(), **update})


@pytest.mark.parametrize("update", [
    {"metric_id": "unknown"}, {"metric_version": "2.0.0"}, {"population": "hidden_inventory"},
    {"period": "last_month"}, {"grain": "month"}, {"dimensions": ["category", "category"]},
    {"ordering": [{"column": "pipeline_stage"}]}, {"include_share": True},
    {"include_share": True, "include_total": False, "dimensions": ["category"]},
])
def test_catalogue_rejects_unsupported_plans(update):
    with pytest.raises(InvalidMetricRequest):
        Compiler().compile(MetricRequest.model_validate({**request().model_dump(), **update}),
                           now=NOW, business_timezone="Europe/London")


def test_filters_are_parameters_and_query_references_include_values():
    compiler = Compiler()
    attack = "A'); SELECT public.fixture_invoker_write(); --"
    body = request(filters=[{"dimension": "category", "values": [attack]}])
    query = compiler.compile(body, now=NOW, business_timezone="Europe/London")
    assert attack not in query.rows.sql and [attack] in query.rows.params
    assert 'analyst_query."projects_v1"' in query.rows.sql
    other = compiler.compile(request(), now=NOW, business_timezone="Europe/London")
    assert query.reference != other.reference


def test_all_catalogue_variants_compile_with_only_gateway_sources():
    compiler = Compiler()
    for metric in compiler.catalogue.metrics:
        for period in metric.periods:
            body = MetricRequest(metric_id=metric.id, metric_version=metric.version,
                                 population=metric.population, period=period)
            query = compiler.compile(body, now=NOW, business_timezone="Europe/London")
            assert "public." not in query.rows.sql and "current_" not in query.rows.sql
            if metric.family == "conversion":
                assert "cohort_years = %s" in query.total.sql


def aggregate(value):
    return Aggregate(value=value, source_rows=1, known_values=1)


def test_decimal_changes_points_null_and_zero_denominators():
    change = compare(aggregate(Decimal("0.3")), aggregate(Decimal("0.2")), unit="ratio")
    assert change.absolute_change == Decimal("0.1")
    assert change.percentage_change == 50
    assert change.percentage_point_change == 10
    assert compare(aggregate(1), aggregate(0), unit="days").percentage_change is None
    assert compare(aggregate(None), aggregate(2), unit="days").absolute_change is None
    assert compare(aggregate(-5), aggregate(-10), unit="source_currency").percentage_change == 50
    assert share(Decimal("0.1"), Decimal("0.3")) > Decimal("0.3333333333333333333333333333")
    assert share(Decimal(1), Decimal(0)) is None


def test_evaluation_cannot_enable_remote_or_production_metrics():
    config = dict(read_dsn="host=127.0.0.1 dbname=test user=bi_analyst_reader",
                  state_dsn="host=127.0.0.1 dbname=test user=bi_analyst_state",
                  environment="test", business_timezone="Europe/London", metric_evaluation_enabled=True)
    assert Settings(**config).metric_evaluation_enabled
    for update in ({"business_timezone": None}, {"business_timezone": "invalid/zone"},
                   {"environment": "staging"}, {"environment": "production"}):
        with pytest.raises(ValidationError):
            Settings(**{**config, **update})
    remote = {key: value.replace("127.0.0.1", "database.example.test") + " sslmode=verify-full"
              for key, value in config.items() if key.endswith("dsn")}
    with pytest.raises(ValidationError):
        Settings(**{**config, **remote})
