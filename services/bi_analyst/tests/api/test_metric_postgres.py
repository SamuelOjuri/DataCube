"""Golden metrics and permission/resource boundaries on disposable loopback PG."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from pathlib import Path
import time
import threading
from uuid import uuid4

from fastapi.testclient import TestClient
import psycopg
import httpx
import json
import pytest

from bi_analyst.api import create_app
from bi_analyst.identity import Identity, current_identity
from bi_analyst.metrics.compiler import Compiler
from bi_analyst.metrics.service import wire_size
from bi_analyst.monday_source import BOARDS, EVIDENCE_QUERY
from pydantic import SecretStr

pytestmark = pytest.mark.postgres
ROOT = Path(__file__).resolve().parents[4]


@pytest.fixture(scope="module")
def metric_database(database):
    admin, dsn = database
    with admin.transaction():
        admin.execute((ROOT / "src/database/migrations/20261008_004_analyst_reportable_population.sql").read_text())
    return admin, dsn


@pytest.fixture
def client(settings, metric_database):
    settings = settings.model_copy(update={"metric_evaluation_enabled": True,
                                           "business_timezone": "Europe/London", "requests_per_minute": 600})
    admin, _ = metric_database
    subject = uuid4()
    admin.execute("INSERT INTO analyst_state.principals(subject,enabled,company_wide) VALUES (%s,true,true)", (subject,))
    app = create_app(settings)
    async def identity():
        return Identity(subject)
    app.dependency_overrides[current_identity] = identity
    with TestClient(app) as http:
        http.subject = subject
        http.analyst_app = app
        yield http


def new_run(client):
    conversation = client.post("/v1/conversations", json={"title": "Metric evaluation"})
    assert conversation.status_code == 201, conversation.text
    response = client.post(f"/v1/conversations/{conversation.json()['id']}/runs", json={"question": "Evaluate metric"})
    assert response.status_code == 201, response.text
    return response.json()["id"]


def payload(metric_id="order_parent_value", **updates):
    metric = Compiler().metrics[metric_id]
    return {"metric_id": metric.id, "metric_version": metric.version, "population": metric.population,
            "period": metric.periods[0], **updates}


def query(client, metric_id="order_parent_value", **updates):
    response = client.post(f"/v1/runs/{new_run(client)}/metric", json=payload(metric_id, **updates))
    assert response.status_code == 201, response.text
    return response.json()


@pytest.mark.parametrize("metric_id,expected", [
    ("new_enquiry_value", "350.00"), ("order_parent_value", "350.00"),
    ("order_hidden_complete_subtotal", "107.50"), ("invoice_project_value", "95.00"),
    ("invoice_hidden_value", "80.00"), ("invoice_stored_child_value", "95.00"),
    ("enquiry_monthly_actual", "300.00"), ("bookings_monthly_actual", "150.00"),
    ("invoice_monthly_actual", "100.00"), ("conversion_five_year", "0.200"),
    ("conversion_two_year", "0.200"), ("conversion_closed_five_year", "0.500"),
    ("conversion_closed_two_year", "0.500"), ("gestation_five_year", "20.000000"),
    ("gestation_two_year", "20.000000"), ("gestation_median_five_year", "20.000000"),
])
def test_all_sixteen_variants_match_independent_golden_values(client, metric_id, expected):
    result = query(client, metric_id)
    assert result["rows"][0][0] == expected
    provenance = result["provenance"]
    assert provenance["total"]["value"] == expected
    assert provenance["dataset_scope"] == "complete"
    assert provenance["evaluation_only"]
    assert provenance["freshness"]["status"] == "unknown"
    assert provenance["freshness"]["data_version"] is None
    assert provenance["coverage"]["incomplete_order_inputs"] == 1
    stored = client.get(f"/v1/results/{result['id']}")
    assert stored.status_code == 200 and stored.json() == result
    assert client.get(f"/v1/runs/{result['run_id']}").json()["status"] == "completed"


def test_top_contributors_preserve_complete_total_and_shares(client):
    result = query(client, dimensions=["category"], ordering=[{"column": "value", "direction": "desc"}],
                   include_share=True, limit=1)
    p = result["provenance"]
    assert result["rows"][0][:2] == ["B", "200.00"]
    assert abs(Decimal(result["rows"][0][-1]) - Decimal(200) / Decimal(350)) < Decimal("1e-27")
    assert p["total"]["value"] == "350.00" and p["matched_groups"] == 3
    assert p["truncated"] and p["truncation_reasons"] == ["row_limit"]
    assert p["dataset_scope"] == "limited" and p["total_scope"] == "complete_filtered_population"


def test_conversion_total_uses_counts_and_compares_percentage_points(client):
    result = query(client, "conversion_five_year", dimensions=["category"], limit=1,
                   comparison={"period": "five_year_cohort", "filters": [{"dimension": "category", "values": ["A"]}]})
    p = result["provenance"]
    assert p["total"]["value"] == "0.200"  # Not the mean of .5, 0, 0.
    assert p["total"]["numerator"] == "1" and p["total"]["denominator"] == "5"
    assert Decimal(p["comparison"]["percentage_point_change"]) == -30
    assert Decimal(p["comparison"]["percentage_change"]) == -60


def test_month_spines_comparisons_and_dimension_bins(client):
    for metric in ("enquiry_monthly_actual", "bookings_monthly_actual", "invoice_monthly_actual"):
        result = query(client, metric, period="completed_12_months", grain="month",
                       comparison={"period": "previous_month"})
        assert len(result["rows"]) == 12
        assert sum(Decimal(r[1]) for r in result["rows"]) == Decimal(result["provenance"]["total"]["value"])
        assert result["provenance"]["comparison"]["zero_denominator"]
        assert result["provenance"]["comparison"]["percentage_change"] is None
    result = query(client, "invoice_monthly_actual", period="completed_12_months", grain="month", dimensions=["category"])
    assert len(result["rows"]) == 12 and all(r[1] == "A" for r in result["rows"])
    assert sum(Decimal(r[2]) for r in result["rows"]) == 100


def test_empty_null_zero_and_signed_values(client):
    result = query(client, "invoice_project_value", filters=[{"dimension": "category", "operator": "is_null"}])
    assert result["provenance"]["total"]["value"] == "-5.00"
    result = query(client, filters=[{"dimension": "category", "operator": "is_null"}])
    assert result["provenance"]["total"]["value"] is None
    assert result["provenance"]["total"]["known_values"] == 0
    for metric in ("order_parent_value", "conversion_closed_five_year", "gestation_five_year"):
        result = query(client, metric, filters=[{"dimension": "category", "values": ["unknown"]}])
        assert result["provenance"]["total"]["value"] is None
        assert result["provenance"]["total"]["source_rows"] == 0
    result = query(client, "invoice_monthly_actual", filters=[{"dimension": "category", "values": ["unknown"]}], grain="month")
    assert result["rows"][0][1] == "0" and result["provenance"]["total"]["value"] == "0.00"


def test_revenue_includes_retained_archived_and_open_parents_without_stage_gate(client, metric_database):
    admin, _ = metric_database
    admin.execute("""INSERT INTO public.subitems(monday_id,parent_monday_id,amount_invoiced,invoice_date)
        VALUES ('open_invoice','E3',12.34,(date_trunc('month',current_date)-interval '1 month')::date)""")
    try:
        result = query(client, "invoice_monthly_actual")
        assert result["provenance"]["total"]["value"] == "112.34"
    finally:
        admin.execute("DELETE FROM public.subitems WHERE monday_id='open_invoice'")


def test_alias_ambiguity_entity_clarification_and_literal_injection(client, metric_database):
    assert client.get("/v1/metrics/resolve", params={"name": "order value"}).json()["status"] == "clarification_required"
    assert client.get("/v1/metrics/resolve", params={"name": "raw enquiry value"}).json()["candidates"][0]["metric_id"] == "new_enquiry_value"
    body = {key: value for key, value in payload().items() if key != "period"}
    body.update(dimension="category", query="a")
    result = client.post("/v1/entities/resolve", json=body).json()
    assert result["status"] == "resolved" and result["candidates"] == ["A"]
    admin, _ = metric_database
    admin.execute("INSERT INTO public.projects(monday_id,category) VALUES ('ambiguity1','Roofing'),('ambiguity2','Roofing Supplies')")
    try:
        result = client.post("/v1/entities/resolve", json={**body, "query": "roof"}).json()
        assert result["status"] == "clarification_required" and len(result["candidates"]) == 2
    finally:
        admin.execute("DELETE FROM public.projects WHERE monday_id IN ('ambiguity1','ambiguity2')")
    attack = "A'); SELECT public.fixture_invoker_write(); --"
    result = query(client, filters=[{"dimension": "category", "values": [attack]}])
    assert result["provenance"]["total"]["source_rows"] == 0
    assert client.post("/v1/entities/resolve", json={**body, "query": "%"}).json()["status"] == "not_found"
    assert admin.execute("SELECT count(*) AS n FROM public.projects").fetchone()["n"] == 6


def test_permissions_cancellation_duplicates_and_certification_gate(client, metric_database):
    admin, _ = metric_database
    run = new_run(client)
    assert client.post(f"/v1/runs/{run}/cancel").status_code == 200
    assert client.post(f"/v1/runs/{run}/metric", json=payload()).status_code == 409
    result = query(client)
    assert client.post(f"/v1/runs/{result['run_id']}/metric", json=payload()).status_code == 409
    other = uuid4()
    admin.execute("INSERT INTO analyst_state.principals(subject,enabled,company_wide) VALUES (%s,true,true)", (other,))
    async def other_identity():
        return Identity(other)
    client.analyst_app.dependency_overrides[current_identity] = other_identity
    assert client.get(f"/v1/results/{result['id']}").status_code == 404
    assert client.get(f"/v1/results/{result['id']}/export").status_code == 404
    assert client.post(f"/v1/runs/{result['run_id']}/metric", json=payload()).status_code == 404
    client.analyst_app.state.metrics.settings.metric_evaluation_enabled = False
    accepted = query(client)
    assert accepted['provenance']['certification_basis'] == 'owner_accepted'
    assert not accepted['provenance']['evaluation_only']
    assert not any('owner review pending' in item for item in accepted['provenance']['limitations'])
    discovery = client.get('/v1/metrics').json()
    assert all(metric['certification'] == 'owner_accepted' for metric in discovery['metrics'])
    assert not any('pending certification' in item for item in discovery['notices'])
    compiler = client.analyst_app.state.metrics.compiler
    compiler.catalogue = compiler.catalogue.model_copy(update={'version': '9.0.0'})
    denied = client.post(f"/v1/runs/{new_run(client)}/metric", json=payload())
    assert denied.json()['detail'] == 'metric_not_certified'
    assert denied.json()['source_check']['path'].endswith('/source-check')


def test_byte_budget_and_export_scope(client, metric_database):
    admin, _ = metric_database
    admin.execute("INSERT INTO public.projects(monday_id,category,total_order_value) VALUES ('large_label',%s,9999)", ("x" * 20000,))
    client.analyst_app.state.metrics.settings.metric_max_bytes = 16384
    try:
        result = query(client, dimensions=["category"], ordering=[{"column": "value"}])
        assert result["rows"] == []
        assert result["provenance"]["truncation_reasons"] == ["byte_limit"]
        assert result["provenance"]["total"]["value"] == "10349.00"
        assert client.get(f"/v1/results/{result['id']}/export").headers["X-Export-Scope"] == "stored-result-rows"
    finally:
        admin.execute("DELETE FROM public.projects WHERE monday_id='large_label'")


def test_source_check_available_for_uncertified_metric_with_owned_run(client, metric_database):
    admin, _ = metric_database
    app = client.analyst_app
    app.state.settings.monday_read_token = SecretStr('source-token')
    app.state.settings.monday_account_id = '123'
    app.state.metrics.settings.metric_evaluation_enabled = False
    compiler = app.state.metrics.compiler
    compiler.catalogue = compiler.catalogue.model_copy(update={'version': '9.0.0'})
    requests = []
    def provider(request):
        query = json.loads(request.content)['query']
        requests.append(query)
        result = {'me': {'account': {'id': '123'}}, 'items': [
            {'id': '456', 'board': {'id': BOARDS['hidden_items']}}]}
        if query == EVIDENCE_QUERY:
            result['boards'] = [{'id': BOARDS['hidden_items'], 'columns': [
                {'id': 'numbers3__1', 'title': 'Additional charges', 'type': 'numbers', 'settings': {}}]}]
            result['items'][0].update(name='Source', state='active', updated_at='2026-10-08T12:00:00Z',
                column_values=[{'id': 'numbers3__1', 'type': 'numbers', '__typename': 'NumbersValue',
                                'value': '{"value":"19.99"}', 'text': '19.99'}])
        return httpx.Response(200, json={'data': result}, headers={'API-Version': '2026-07'})
    provider_client = httpx.AsyncClient(transport=httpx.MockTransport(provider))
    app.state.monday_source.client = provider_client
    try:
        run_id = new_run(client)
        blocked = client.post(f'/v1/runs/{run_id}/metric', json=payload())
        assert blocked.status_code == 503
        assert blocked.json()['source_check']['available']
        source = {key: value for key, value in payload().items() if key != 'period'}
        source.update(reason='metric_not_certified', board='hidden_items', item_ids=['456'], column_ids=['numbers3__1'])
        checked = client.post(blocked.json()['source_check']['path'], json=source)
        assert checked.status_code == 200, checked.text
        assert checked.json()['metric_acceptance'] == 'not_accepted'
        assert checked.json()['dataset_scope'] == 'requested_live_items'
        assert client.get(f'/v1/runs/{run_id}').json()['status'] == 'registered'
        assert len(requests) == 2
        assert 'source-token' not in checked.text
        source['query'] = 'mutation { delete_item(item_id:456) { id } }'
        assert client.post(f'/v1/runs/{run_id}/source-check', json=source).status_code == 422
        del source['query']
        other = uuid4()
        admin.execute('INSERT INTO analyst_state.principals(subject,enabled,company_wide) VALUES (%s,true,true)', (other,))
        async def other_identity():
            return Identity(other)
        app.dependency_overrides[current_identity] = other_identity
        assert client.post(f'/v1/runs/{run_id}/source-check', json=source).status_code == 404
        assert len(requests) == 2
    finally:
        asyncio.run(provider_client.aclose())


def test_related_queries_use_one_repeatable_read_snapshot(client, metric_database, monkeypatch):
    admin, _ = metric_database
    compiler = client.analyst_app.state.metrics.compiler
    original = compiler.compile
    calls = 0
    def mutate_between_queries(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            admin.execute("UPDATE public.projects SET total_order_value=999 WHERE monday_id='E1'")
        return original(*args, **kwargs)
    monkeypatch.setattr(compiler, "compile", mutate_between_queries)
    try:
        result = query(client, comparison={"period": "all_stored"})
        assert result["provenance"]["comparison"]["current"]["value"] == "350.00"
        assert result["provenance"]["comparison"]["baseline"]["value"] == "350.00"
    finally:
        admin.execute("UPDATE public.projects SET total_order_value=100 WHERE monday_id='E1'")


@pytest.mark.parametrize("statement_timeout", [100, 5000])
def test_lock_and_statement_timeouts_return_safe_errors_and_pool_recovers(client, metric_database, statement_timeout):
    _, dsn = metric_database
    client.analyst_app.state.metrics.settings.statement_timeout_ms = statement_timeout
    run = new_run(client)
    with psycopg.connect(dsn) as blocker:
        blocker.execute("LOCK TABLE public.projects IN ACCESS EXCLUSIVE MODE")
        response = client.post(f"/v1/runs/{run}/metric", json=payload())
        assert response.status_code == 504 and response.json()["detail"] == "metric_query_timeout"
        entity = {key: value for key, value in payload().items() if key != "period"}
        response = client.post("/v1/entities/resolve", json={**entity, "dimension": "category", "query": "A"})
        assert response.status_code == 504 and response.json()["detail"] == "metric_query_timeout"
    assert client.get(f"/v1/runs/{run}").json()["status"] == "failed"
    assert query(client)["provenance"]["total"]["value"] == "350.00"


def test_durable_cancellation_stops_inflight_sql_and_releases_capacity(client, metric_database):
    admin, dsn = metric_database
    run = new_run(client)
    client.analyst_app.state.metrics.settings.metric_concurrency = 1
    other = new_run(client)
    with psycopg.connect(dsn) as blocker, ThreadPoolExecutor(max_workers=1) as workers:
        blocker.execute("LOCK TABLE public.projects IN ACCESS EXCLUSIVE MODE")
        future = workers.submit(client.post, f"/v1/runs/{run}/metric", json=payload())
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            if admin.execute("""SELECT 1 FROM pg_stat_activity WHERE datname=current_database()
                AND usename='bi_analyst_reader' AND wait_event_type='Lock'""").fetchone():
                break
            time.sleep(.02)
        else:
            pytest.fail("Metric query did not reach the database")
        response = client.post(f"/v1/runs/{other}/metric", json=payload())
        assert response.status_code == 429
        # Simulate cancellation handled on another API replica, through durable state.
        admin.execute("UPDATE analyst_state.runs SET status='cancelled' WHERE id=%s", (run,))
        response = future.result(timeout=3)
        assert response.status_code == 409
    assert client.get(f"/v1/runs/{run}").json()["status"] == "cancelled"
    assert admin.execute("SELECT count(*) AS n FROM analyst_state.results WHERE run_id=%s", (run,)).fetchone()["n"] == 0
    assert query(client)["provenance"]["total"]["value"] == "350.00"


def test_permissions_changed_before_commit_never_releases_result(client, metric_database, monkeypatch):
    admin, _ = metric_database
    service = client.analyst_app.state.metrics
    original = service.query
    async def revoke(*args):
        result = await original(*args)
        admin.execute("UPDATE analyst_state.principals SET permissions_version=permissions_version+1 WHERE subject=%s", (client.subject,))
        return result
    monkeypatch.setattr(service, "query", revoke)
    run = new_run(client)
    response = client.post(f"/v1/runs/{run}/metric", json=payload())
    assert response.status_code == 403
    assert admin.execute("SELECT count(*) AS n FROM analyst_state.results WHERE run_id=%s", (run,)).fetchone()["n"] == 0


def test_byte_budget_includes_metadata_and_escaped_text(client, metric_database):
    admin, _ = metric_database
    for i in range(10):
        admin.execute("INSERT INTO public.projects(monday_id,category,total_order_value) VALUES (%s,%s,1000)",
                      (f"metadata_budget_{i}", f"{i}" + "\\\"" * 450))
    client.analyst_app.state.metrics.settings.metric_max_bytes = 16384
    try:
        result = query(client, dimensions=["category"], ordering=[{"column": "value"}])
        assert result["provenance"]["truncated"]
        assert "byte_limit" in result["provenance"]["truncation_reasons"]
        assert result["provenance"]["returned_rows"] == len(result["rows"])
        assert wire_size(result) <= 16384
        assert result["provenance"]["total"]["value"] == "10350.00"
        assert client.get(f"/v1/results/{result['id']}").json() == result
    finally:
        admin.execute("DELETE FROM public.projects WHERE monday_id LIKE 'metadata_budget_%%'")


def test_workflow_deadline_cancels_child_task(client, monkeypatch):
    service = client.analyst_app.state.metrics
    service.settings.metric_timeout_seconds = 1
    cancelled = []
    async def blocked(*args):
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.append(True)
    monkeypatch.setattr(service, "query", blocked)
    run = new_run(client)
    response = client.post(f"/v1/runs/{run}/metric", json=payload())
    assert response.status_code == 504 and cancelled == [True]
    assert service.active == 0 and service.runs == set()
    assert client.get(f"/v1/runs/{run}").json()["status"] == "failed"


def test_two_replicas_commit_only_one_result(client, metric_database, monkeypatch):
    admin, _ = metric_database
    other_app = create_app(client.analyst_app.state.settings)
    other_app.dependency_overrides[current_identity] = client.analyst_app.dependency_overrides[current_identity]
    run = new_run(client)
    barrier = threading.Barrier(2, timeout=5)
    def coordinate(service):
        original = service.query
        async def query_then_wait(*args):
            result = await original(*args)
            await asyncio.to_thread(barrier.wait)
            return result
        monkeypatch.setattr(service, "query", query_then_wait)
    with TestClient(other_app) as other:
        coordinate(client.analyst_app.state.metrics)
        coordinate(other_app.state.metrics)
        with ThreadPoolExecutor(max_workers=2) as workers:
            first = workers.submit(client.post, f"/v1/runs/{run}/metric", json=payload())
            second = workers.submit(other.post, f"/v1/runs/{run}/metric", json=payload())
            assert sorted([first.result().status_code, second.result().status_code]) == [201, 409]
    assert admin.execute("SELECT count(*) AS n FROM analyst_state.results WHERE run_id=%s", (run,)).fetchone()["n"] == 1
