"""Read-only source boundary; no test contacts Monday or loads repository secrets."""
import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
import json
from types import SimpleNamespace
from uuid import uuid4

from fastapi import HTTPException
import httpx
import pytest
from pydantic import ValidationError

from bi_analyst.metrics.compiler import Compiler
from bi_analyst.monday_auth import API_URL
from bi_analyst.monday_source import (BOARDS, EVIDENCE_QUERY, SCOPE_QUERY, MondaySourceReader,
                                     SourceCheckRequest, SourceCheckService)
from bi_analyst.settings import Settings


def settings(**changes):
    return Settings(read_dsn="host=localhost dbname=test user=bi_analyst_reader",
                    state_dsn="host=localhost dbname=test user=bi_analyst_state", environment="test",
                    monday_read_token="private-read-token", monday_account_id="123", **changes)


def body(**changes):
    return SourceCheckRequest(**{ "metric_id": "order_parent_value", "metric_version": "1.0.0",
        "population": "reportable", "reason": "source_discrepancy", "board": "hidden_items",
        "item_ids": ["456"], "column_ids": ["formula_mkncjq9", "numbers98__1", "numbers3__1"], **changes})


def data(evidence=False):
    result = {"me": {"account": {"id": "123"}},
              "items": [{"id": "456", "board": {"id": BOARDS["hidden_items"]}}]}
    if evidence:
        result["boards"] = [{"id": BOARDS["hidden_items"], "columns": [
            {"id": col, "title": col, "type": "formula" if i == 0 else "numbers", "settings": {}}
            for i, col in enumerate(body().column_ids)]}]
        result["items"][0].update(name="Untrusted CRM text", state="archived", updated_at="2026-10-08T12:00:00Z",
            column_values=[
                {"id": "formula_mkncjq9", "type": "formula", "__typename": "FormulaValue",
                 "value": None, "text": "", "display_value": "123456789012345.67"},
                {"id": "numbers98__1", "type": "numbers", "__typename": "NumbersValue",
                 "text": "0", "value": '{"value":"0"}'},
                {"id": "numbers3__1", "type": "numbers", "__typename": "NumbersValue",
                 "text": "", "value": None}])
    return result


def response(payload=None, **kwargs):
    return httpx.Response(200, json={"data": payload or data()}, headers={"API-Version": "2026-07"}, **kwargs)


def lookup(handler, request=None, config=None):
    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler), follow_redirects=True) as client:
            reader = MondaySourceReader(config or settings(), client)
            result = await reader.lookup(request or body())
            assert reader.active == 0
            return result
    return asyncio.run(run())


def test_exact_ids_fixed_queries_preserve_decimal_blank_zero_and_archived():
    requests = []
    def handler(request):
        payload = json.loads(request.content)
        requests.append(payload)
        assert str(request.url) == API_URL
        assert request.headers["Authorization"] == "private-read-token"
        assert request.headers["API-Version"] == "2026-07"
        assert payload["query"] in {SCOPE_QUERY, EVIDENCE_QUERY}
        return response(data(evidence=payload["query"] == EVIDENCE_QUERY))
    columns, items, missing, missing_columns, limitations = lookup(handler)
    assert len(requests) == 2
    assert requests[0]["variables"] == {"ids": ["456"]}
    assert requests[1]["variables"] == {"ids": ["456"], "boards": [BOARDS["hidden_items"]], "columns": body().column_ids}
    assert "mutation" not in " ".join(p["query"] for p in requests)
    assert items[0]["state"] == "archived"
    values = items[0]["column_values"]
    assert values[0]["display_value"] == "123456789012345.67"
    assert values[1]["value"] == '{"value":"0"}'
    assert values[2]["value"] is None
    assert not missing and not missing_columns and not limitations


@pytest.mark.parametrize("changes", [
    {"query": "mutation { delete_item(item_id: 456) { id } }"}, {"board": "other"},
    {"item_ids": []}, {"item_ids": ["456", "456"]}, {"item_ids": [str(n) for n in range(1, 22)]},
    {"item_ids": ["456) { delete_item"]}, {"column_ids": ["password"]},
    {"column_ids": ["formula_mkncjq9", "formula_mkncjq9"]}, {"column_ids": []},
    {"endpoint": "https://other.example"}, {"api_token": "caller-token"},
])
def test_invalid_queries_ids_and_scope_cannot_enter_reader(changes):
    with pytest.raises(ValidationError):
        body(**changes)


@pytest.mark.parametrize("where,field,value,code", [
    ("scope", "account", "999", "monday_source_account_denied"),
    ("scope", "board", "999", "monday_source_board_denied"),
    ("evidence", "board", "999", "monday_source_board_denied"),
    ("scope", "item", "999", "monday_source_unavailable"),
])
def test_provider_account_board_and_id_scope_checked_on_both_calls(where, field, value, code):
    calls = []
    def handler(request):
        evidence = json.loads(request.content)["query"] == EVIDENCE_QUERY
        calls.append(evidence)
        payload = data(evidence)
        if evidence == (where == "evidence"):
            if field == "account":
                payload["me"]["account"]["id"] = value
            elif field == "board":
                payload["items"][0]["board"]["id"] = value
            else:
                payload["items"][0]["id"] = value
        return response(payload)
    with pytest.raises(HTTPException) as error:
        lookup(handler)
    assert error.value.detail == code
    assert len(calls) == (1 if where == "scope" else 2)


def test_missing_items_never_trigger_an_unfiltered_second_query():
    calls = []
    def handler(request):
        calls.append(request)
        payload = data()
        payload["items"] = []
        return response(payload)
    assert lookup(handler)[2:4] == (["456"], body().column_ids)
    assert len(calls) == 1


@pytest.mark.parametrize("kind,expected", [
    ("partial_graphql", "monday_source_unavailable"), ("redirect", "monday_source_unavailable"),
    ("invalid_json", "monday_source_unavailable"), ("version", "monday_source_api_version_mismatch"),
    ("oversized", "monday_source_response_too_large"), ("rate", "monday_source_rate_limited"),
])
def test_provider_failures_are_bounded_sanitized_and_never_retried(kind, expected):
    calls = []
    def handler(request):
        calls.append(request)
        if kind == "partial_graphql":
            return httpx.Response(200, json={"data": data(), "errors": [{"message": "private-read-token private-record"}]}, headers={"API-Version": "2026-07"})
        if kind == "redirect":
            return httpx.Response(302, headers={"Location": "https://other.example/private-read-token"})
        if kind == "invalid_json":
            return httpx.Response(200, text="private-read-token", headers={"API-Version": "2026-07"})
        if kind == "version":
            return httpx.Response(200, json={"data": data()}, headers={"API-Version": "2026-10"})
        if kind == "oversized":
            return httpx.Response(200, content=b"x" * 5000, headers={"API-Version": "2026-07"})
        return httpx.Response(429, text="private-read-token")
    with pytest.raises(HTTPException) as error:
        lookup(handler, config=settings(monday_read_max_bytes=4096))
    assert error.value.detail == expected
    assert len(calls) == 1
    assert "private" not in str(error.value)


def test_missing_columns_and_unavailable_formula_are_explicit():
    def handler(request):
        evidence = json.loads(request.content)["query"] == EVIDENCE_QUERY
        payload = data(evidence)
        if evidence:
            payload["boards"][0]["columns"].pop()
            payload["items"][0]["column_values"].pop()
            payload["items"][0]["column_values"][0]["display_value"] = ""
        return response(payload)
    _, items, _, missing_columns, limitations = lookup(handler)
    assert missing_columns == ["numbers3__1"]
    assert items[0]["missing_column_ids"] == ["numbers3__1"]
    assert any("not numeric zero" in item for item in limitations)


def test_mirror_links_preserve_multiplicity_but_omit_out_of_scope_boards():
    def handler(request):
        evidence = json.loads(request.content)["query"] == EVIDENCE_QUERY
        payload = data(evidence)
        if evidence:
            value = payload["items"][0]["column_values"][0]
            link = {"linked_board_id": BOARDS["subitems"], "linked_item": {"id": "789"}}
            value.update(__typename="MirrorValue", display_value="10, 10", mirrored_items=[link, deepcopy(link),
                {"linked_board_id": "999", "linked_item": {"id": "999"}}])
        return response(payload)
    _, items, _, _, limitations = lookup(handler)
    value = items[0]["column_values"][0]
    assert value["display_value"] == "10, 10"
    assert len(value["mirrored_items"]) == 2
    assert any("outside approved boards" in item for item in limitations)


def test_concurrency_and_cancellation_release_source_capacity():
    async def run():
        entered, release = asyncio.Event(), asyncio.Event()
        async def handler(request):
            entered.set()
            await release.wait()
            return response()
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            reader = MondaySourceReader(settings(monday_read_concurrency=1), client)
            task = asyncio.create_task(reader.lookup(body()))
            await entered.wait()
            with pytest.raises(HTTPException, match="monday_source_capacity_reached"):
                await reader.lookup(body())
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert reader.active == 0
    asyncio.run(run())


def test_total_timeout_releases_source_capacity():
    async def run():
        async def handler(request):
            await asyncio.sleep(5)
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            reader = MondaySourceReader(settings(monday_read_timeout_seconds=1), client)
            with pytest.raises(HTTPException, match="monday_source_timeout"):
                await reader.lookup(body())
            assert reader.active == 0
    asyncio.run(run())


class FakeStore:
    def __init__(self, *, deny=False, revoked=False, status="registered"):
        self.deny, self.revoked, self.status = deny, revoked, status

    async def run(self, actor, run_id):
        if self.deny:
            raise HTTPException(404, "not_found")
        return {"status": self.status}

    @asynccontextmanager
    async def scoped(self, actor):
        if self.revoked:
            raise HTTPException(403, "permissions_changed")
        yield


@pytest.mark.parametrize("case", ["denied", "cancelled", "revoked", "accepted", "uncertified"])
def test_source_service_ownership_permissions_and_certification_are_separate(case):
    async def run():
        calls = []
        def handler(request):
            calls.append(request)
            return response(data(json.loads(request.content)["query"] == EVIDENCE_QUERY))
        compiler = Compiler()
        if case == "uncertified":
            compiler.catalogue = compiler.catalogue.model_copy(update={"version": "9.0.0"})
        store = FakeStore(deny=case == "denied", revoked=case == "revoked",
                          status="cancelled" if case == "cancelled" else "registered")
        actor = SimpleNamespace(permissions_version=2)
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            service = SourceCheckService(store, compiler, MondaySourceReader(settings(), client))
            if case in {"denied", "cancelled", "revoked"}:
                with pytest.raises(HTTPException):
                    await service.check(actor, uuid4(), body())
                if case != "revoked":
                    assert calls == []
            else:
                evidence = await service.check(actor, uuid4(), body(reason="metric_not_certified"))
                assert evidence.metric_acceptance == ("not_accepted" if case == "uncertified" else "owner_accepted")
                assert evidence.read_only and evidence.source_text_is_untrusted
                assert evidence.dataset_scope == "requested_live_items"
                assert evidence.permissions_version == 2
                assert "private-read-token" not in evidence.model_dump_json()
    asyncio.run(run())


def test_read_token_requires_account_and_is_never_loaded_from_etl_environment(monkeypatch):
    config = settings().model_dump()
    config["monday_account_id"] = None
    with pytest.raises(ValidationError):
        Settings(**config)
    monkeypatch.setenv("MONDAY_API_KEY", "etl-secret")
    monkeypatch.delenv("BI_ANALYST_MONDAY_READ_TOKEN", raising=False)
    monkeypatch.setenv("BI_ANALYST_READ_DSN", "host=localhost dbname=test user=bi_analyst_reader")
    monkeypatch.setenv("BI_ANALYST_STATE_DSN", "host=localhost dbname=test user=bi_analyst_state")
    monkeypatch.setenv("BI_ANALYST_ENVIRONMENT", "test")
    assert Settings.from_env().monday_read_token is None
