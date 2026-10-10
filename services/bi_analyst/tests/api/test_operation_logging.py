"""Run correlation and redaction under real async retries and failure handling."""
import asyncio
import json
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

from fastapi import HTTPException
import httpx
from pydantic import SecretStr
import psycopg
import pytest

from bi_analyst.metrics.service import MetricService
from bi_analyst.operations.telemetry import Telemetry, operation_context
from bi_analyst.workflow.contracts import Interpretation
from bi_analyst.workflow.evidence import EvidenceError
from bi_analyst.workflow.graph import ConversationGraph
from bi_analyst.workflow.provider import GeminiProvider, ProviderFailure
from bi_analyst.workflow.service import WorkflowService


@pytest.fixture(autouse=True)
def capture_operation_logs(caplog, monkeypatch):
    caplog.set_level(logging.INFO, logger="bi_analyst")
    monkeypatch.setattr(logging.getLogger("bi_analyst"), "propagate", True)


def events(caplog):
    return [json.loads(record.message) for record in caplog.records
            if record.name == "bi_analyst.operations"]


def assert_pair(rows, *, run_id, operation, attempt=1, request_id=None, outcome="success", error_code=None):
    start, end = rows
    assert [row["phase"] for row in rows] == ["start", "end"]
    assert start["operation_id"] == end["operation_id"]
    assert str(UUID(start["operation_id"])) == start["operation_id"]
    for row in rows:
        assert row["run_id"] == (str(run_id) if run_id is not None else None)
        assert row["request_id"] == (str(request_id) if request_id is not None else None)
        assert row["operation"] == operation and row["attempt"] == attempt
    assert end["outcome"] == outcome and end["error_code"] == error_code
    assert end["duration_ms"] >= 0


def test_concurrent_runs_retries_and_child_tasks_do_not_mix_context(caplog):
    telemetry = Telemetry()
    run_ids, request_ids = [uuid4(), uuid4()], [uuid4(), uuid4()]
    private_questions = ["private question A", "private question B"]
    calls = {question: 0 for question in private_questions}

    async def scenario():
        both_started = asyncio.Event()

        async def handler(request):
            question = json.loads(json.loads(request.content)["contents"][0]["parts"][0]["text"])["data"]["question"]
            calls[question] += 1
            if all(calls.values()):
                both_started.set()
            await asyncio.wait_for(both_started.wait(), timeout=1)
            if question == private_questions[1] and calls[question] == 1:
                return httpx.Response(429, text="private provider response")
            return httpx.Response(200, json={"candidates": [{"finishReason": "STOP", "content": {
                "parts": [{"text": json.dumps({"families": ["order"], "supported": True})}]}}]})

        settings = SimpleNamespace(gemini_api_key=SecretStr("private-api-key"), model_timeout_seconds=2)
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            provider = GeminiProvider(settings, client, telemetry)

            async def run(index):
                graph = object.__new__(ConversationGraph)
                graph.jobs = SimpleNamespace(budget=AsyncMock(), usage=AsyncMock())
                graph.service = SimpleNamespace(provider=provider)
                graph.actor, graph.run_id, graph.token = object(), run_ids[index], uuid4()
                with operation_context(request_id=request_ids[index]):
                    with telemetry.operation("workflow", "run_workflow", run_id=run_ids[index]):
                        # The production driver also executes its graph in a child task.
                        result = await asyncio.create_task(graph.model(
                            "interpret", {"question": private_questions[index]}, Interpretation))
                        assert result.families == ["order"]

            await asyncio.gather(run(0), run(1))
        # Verify reset in the caller after both child tasks and the retry have ended.
        with telemetry.operation("model", "presentation"):
            pass

    asyncio.run(scenario())
    rows = events(caplog)
    for index in range(2):
        own = [row for row in rows if row["run_id"] == str(run_ids[index])]
        assert_pair([row for row in own if row["stage"] == "workflow"], run_id=run_ids[index],
                    request_id=request_ids[index], operation="run_workflow")
        for attempt in range(1, index + 2):
            retry = index == 1 and attempt == 1
            assert_pair([row for row in own if row["stage"] == "model" and row["attempt"] == attempt],
                        run_id=run_ids[index], request_id=request_ids[index], operation="interpret", attempt=attempt,
                        outcome="failed" if retry else "success", error_code="model_unavailable" if retry else None)
    assert_pair(rows[-2:], run_id=None, operation="presentation")
    assert len(rows) == 12
    assert telemetry.calls["model", "success"] == 3 and telemetry.calls["model", "failed"] == 1
    assert telemetry.calls["workflow", "success"] == 2
    pool = SimpleNamespace(get_stats=lambda: {})
    settings = SimpleNamespace(model_input_usd_per_million=None)
    metrics = telemetry.render(SimpleNamespace(read=pool, state=pool, settings=settings),
                               SimpleNamespace(active=0), SimpleNamespace(tasks={}))
    assert 'bi_analyst_operations_total{stage="model",outcome="success"} 3' in metrics
    for identifier in [*run_ids, *request_ids, *(row["operation_id"] for row in rows)]:
        assert str(identifier) not in metrics
    for secret in [*private_questions, "private-api-key", "private provider response", "SELECT"]:
        assert secret not in caplog.text


@pytest.mark.parametrize("error,outcome,code", [
    (RuntimeError("private SQL and password"), "failed", "query_failed"),
    (HTTPException(403, "permissions_changed"), "rejected", "permissions_changed"),
    (HTTPException(400, {"private": "payload"}), "rejected", "query_failed"),
    (psycopg.errors.QueryCanceled("private SQL"), "timeout", "metric_query_timeout"),
    (psycopg.OperationalError("password=private"), "failed", "storage_unavailable"),
    (TimeoutError("private timeout"), "timeout", "metric_query_timeout"),
    (asyncio.CancelledError("private cancellation"), "cancelled", "execution_cancelled"),
])
def test_query_failures_have_fixed_codes_and_restore_context(caplog, error, outcome, code):
    telemetry, run_id = Telemetry(), uuid4()
    with pytest.raises(type(error)):
        with telemetry.operation("query", "metric_query", run_id=run_id):
            raise error
    assert_pair(events(caplog), run_id=run_id, operation="metric_query", outcome=outcome, error_code=code)
    assert "private" not in caplog.text
    with telemetry.operation("query", "metric_query"):
        pass
    assert_pair(events(caplog)[-2:], run_id=None, operation="metric_query")


def test_direct_metric_query_gets_run_id_from_keyword_argument(caplog):
    service = object.__new__(MetricService)
    service.db = SimpleNamespace(telemetry=Telemetry())
    run_id = uuid4()

    def denied(*args):
        raise HTTPException(503, "metric_disabled")

    service.require_access = denied
    body = SimpleNamespace(metric_id="private", metric_version="private", population="private")
    with pytest.raises(HTTPException):
        asyncio.run(service.query(principal=object(), run_id=run_id, body=body))
    assert_pair(events(caplog), run_id=run_id, operation="metric_query",
                outcome="failed", error_code="metric_disabled")
    assert "private" not in caplog.text


def test_model_transport_timeout_has_specific_fixed_code(caplog):
    async def scenario():
        def handler(request):
            raise httpx.ReadTimeout("private provider URL and key")

        settings = SimpleNamespace(gemini_api_key=SecretStr("private-key"), model_timeout_seconds=1)
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            with pytest.raises(ProviderFailure) as caught:
                await GeminiProvider(settings, client).generate("interpret", {}, Interpretation)
            assert caught.value.code == "model_timeout" and caught.value.retryable

    asyncio.run(scenario())
    assert_pair(events(caplog), run_id=None, operation="interpret", outcome="timeout", error_code="model_timeout")
    assert "private" not in caplog.text


@pytest.mark.parametrize("error,outcome,code", [
    (ProviderFailure("model_authentication_failed"), "failed", "model_authentication_failed"),
    (ProviderFailure("private provider detail"), "failed", "workflow_failed"),
    (ProviderFailure("model_timeout"), "timeout", "model_timeout"),
    (EvidenceError("private evidence"), "failed", "invalid_evidence"),
    (TimeoutError("private timeout"), "timeout", "run_timeout"),
    (HTTPException(403, "permissions_changed"), "failed", "permissions_changed"),
    (HTTPException(400, {"private": "payload"}), "failed", "run_unavailable"),
    (RuntimeError("private internal error"), "failed", "workflow_failed"),
])
def test_handled_workflow_failures_are_not_logged_as_success(caplog, error, outcome, code):
    service = object.__new__(WorkflowService)
    service.store = SimpleNamespace(db=SimpleNamespace(telemetry=Telemetry()))
    service.jobs = SimpleNamespace(guard=AsyncMock(side_effect=error), finish=AsyncMock())
    service.cancellations = set()
    run_id = uuid4()
    asyncio.run(service._drive(object(), run_id, uuid4(), None))
    assert_pair(events(caplog), run_id=run_id, operation="run_workflow", outcome=outcome, error_code=code)
    assert "private" not in caplog.text


@pytest.mark.parametrize("user_cancelled", [False, True])
def test_workflow_cancellation_keeps_explicit_outcome(caplog, user_cancelled):
    service = object.__new__(WorkflowService)
    service.store = SimpleNamespace(db=SimpleNamespace(telemetry=Telemetry()))
    service.cancellations = set()
    run_id = uuid4()

    async def scenario():
        started = asyncio.Event()

        async def guard(*args):
            started.set()
            await asyncio.Event().wait()

        service.jobs = SimpleNamespace(guard=guard, finish=AsyncMock())
        task = asyncio.create_task(service._drive(object(), run_id, uuid4(), None))
        await asyncio.wait_for(started.wait(), timeout=1)
        if user_cancelled:
            service.cancellations.add(run_id)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())
    assert_pair(events(caplog), run_id=run_id, operation="run_workflow",
                outcome="cancelled" if user_cancelled else "interrupted",
                error_code="execution_cancelled" if user_cancelled else "execution_interrupted")
    assert run_id not in service.cancellations


def test_log_fields_are_allowlisted_and_invalid_ids_cannot_inject_content(caplog):
    telemetry = Telemetry()
    with operation_context(run_id="private-question", request_id="private-token"):
        with telemetry.operation("model", "interpret") as operation:
            operation.set_outcome("failed", "private error detail")
    assert_pair(events(caplog), run_id=None, operation="interpret", outcome="failed", error_code="model_failed")
    assert "private" not in caplog.text
    caplog.clear()
    for attempt in (0, 9, True, "private"):
        with pytest.raises(ValueError, match="Unbounded"):
            with operation_context(attempt=attempt):
                pass
    with pytest.raises(ValueError, match="Unbounded"):
        with telemetry.operation("model", "private prompt"):
            pass
    assert not events(caplog)
