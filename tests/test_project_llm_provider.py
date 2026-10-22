"""Offline contract tests through the real Google SDK and a mock HTTP transport."""
import copy
import json
from types import SimpleNamespace
from unittest.mock import Mock

import httpx
import pytest

from src.core import llm_analyzer as module
from scripts.check_project_analysis_llm import check_model, synthetic_cases


VALID = {
    "summary": "Based on the supplied segment statistics.",
    "reasoning": {"gestation": "The median is 120 days.", "conversion": "The baseline is 25%.",
                  "rating": "The baseline rating is retained."},
    "adjustments": {"rating_adjustment": 0},
    "confidence_notes": "Confidence depends on segment support.",
}


def response(payload=None, *, finish="STOP", thoughts=7):
    return httpx.Response(200, json={
        "candidates": [{"content": {"role": "model", "parts": [
            {"text": json.dumps(VALID if payload is None else payload)}]}, "finishReason": finish}],
        "usageMetadata": {"promptTokenCount": 100, "candidatesTokenCount": 30,
                          "thoughtsTokenCount": thoughts, "totalTokenCount": 130 + thoughts},
    })


@pytest.fixture
def provider(monkeypatch):
    real_client = module.genai.Client
    clients, requests = [], []
    monkeypatch.setattr(module, "GEMINI_THINKING_LEVEL", "low")
    monkeypatch.setattr(module, "GEMINI_MAX_OUTPUT_TOKENS", 4096)
    monkeypatch.setattr(module, "GEMINI_TIMEOUT_SECONDS", 90)
    monkeypatch.setattr(module, "GEMINI_INPUT_USD_PER_MILLION", "0.75")
    monkeypatch.setattr(module, "GEMINI_OUTPUT_USD_PER_MILLION", "3.75")
    monkeypatch.setattr(module, "GEMINI_API_KEY", "synthetic-unit-test-key")
    monkeypatch.setattr(module, "GEMINI_MODEL", "gemini-3.8-flash")
    monkeypatch.setattr(module.time, "sleep", Mock())

    def factory(replies=None, *, model="gemini-3.8-flash"):
        replies = iter(replies if replies is not None else [response()])

        def handler(request):
            requests.append(request)
            reply = next(replies)
            if isinstance(reply, Exception):
                raise reply
            return reply

        def create_client(**kwargs):
            kwargs["http_options"].httpx_client = httpx.Client(transport=httpx.MockTransport(handler))
            client = real_client(**kwargs)
            clients.append(client)
            return client

        monkeypatch.setattr(module.genai, "Client", create_client)
        return module.LLMAnalyzer(model=model)

    yield factory, requests
    for client in clients:
        client.close()


def analyze(analyzer):
    project, baseline, stats = next(synthetic_cases())
    return analyzer.analyze_project(project, baseline, stats)


def test_wire_contract_and_actual_usage(provider):
    factory, requests = provider
    analyzer = factory()
    output, metadata = analyze(analyzer)
    assert "error" not in metadata
    assert output.summary == VALID["summary"]
    assert requests[0].url.path.endswith("/models/gemini-3.8-flash:generateContent")
    body = json.loads(requests[0].content)
    config = body["generationConfig"]
    assert "temperature" not in config and "topP" not in config and "topK" not in config
    # ProtoJSON accepts proto field names as well as lowerCamelCase names.
    # SDK 1.56 serializes nested ThinkingConfig fields using proto names.
    thinking = config["thinkingConfig"]
    assert thinking == {"include_thoughts": False, "thinking_level": "LOW"}
    assert config["maxOutputTokens"] == 4096
    assert config["responseMimeType"] == "application/json"
    assert "responseSchema" in config
    assert requests[0].extensions["timeout"]["read"] == 90
    assert metadata["tokens"] == {"input": 100, "output": 30, "thinking": 7}
    assert metadata["tokens_used"] == 137
    assert analyzer.get_usage_statistics()["estimated_cost"] == pytest.approx(0.00021375)


@pytest.mark.parametrize("status", [408, 429, 500, 502, 503, 504])
def test_transient_sdk_errors_retry_then_succeed(provider, status):
    factory, requests = provider
    analyzer = factory([httpx.Response(status, json={"error": {"message": "unavailable"}}), response()])
    _, metadata = analyze(analyzer)
    assert "error" not in metadata
    assert metadata["attempts"] == metadata["api_calls"] == len(requests) == 2
    module.time.sleep.assert_called_once_with(1)


@pytest.mark.parametrize("status,code", [(400, "model_request_rejected"), (401, "model_authentication_failed"),
                                       (403, "model_authentication_failed"), (404, "model_not_found")])
def test_permanent_errors_do_not_retry_or_leak_provider_message(provider, caplog, status, code):
    factory, requests = provider
    analyzer = factory([httpx.Response(status, json={"error": {"message": "sensitive-provider-message"}})])
    output, metadata = analyze(analyzer)
    assert metadata["error"] == code
    assert len(requests) == 1
    assert output.adjustments == {"rating_adjustment": 0}
    assert "sensitive-provider-message" not in str(metadata) + output.confidence_notes + caplog.text


@pytest.mark.parametrize("reply,code", [
    (lambda: httpx.ReadTimeout("timeout"), "model_timeout"),
    (lambda: httpx.ConnectError("connection"), "model_unavailable"),
    (lambda: httpx.Response(503, json={"error": {"message": "unavailable"}}), "model_unavailable"),
])
def test_retry_exhaustion_is_bounded(provider, reply, code):
    factory, requests = provider
    analyzer = factory([reply(), reply(), reply()])
    _, metadata = analyze(analyzer)
    assert metadata["error"] == code
    assert metadata["attempts"] == len(requests) == 3


def test_truncated_attempt_usage_includes_discarded_output(provider):
    factory, _ = provider
    analyzer = factory([response(finish="MAX_TOKENS"), response()])
    _, metadata = analyze(analyzer)
    assert "error" not in metadata
    assert metadata["tokens_used"] == analyzer.total_tokens == 274
    assert metadata["tokens"] == {"input": 200, "output": 60, "thinking": 14}
    assert analyzer.get_usage_statistics()["total_api_calls"] == 2


def test_truncation_never_accepts_even_valid_looking_json(provider):
    factory, _ = provider
    analyzer = factory([response(finish="MAX_TOKENS") for _ in range(3)])
    _, metadata = analyze(analyzer)
    assert metadata["error"] == "model_output_budget"
    assert metadata["tokens_used"] == 411


@pytest.mark.parametrize("payload", [{}, [], {**VALID, "reasoning": {"rating": "only"}},
                                    {**VALID, "summary": " "}, {**VALID, "adjustments": []}])
def test_invalid_structured_output_is_explicit_failure(provider, payload):
    factory, _ = provider
    _, metadata = analyze(factory([response(payload)]))
    assert metadata["error"] == "model_invalid_output"


@pytest.mark.parametrize("text", ["", "not JSON", '{"summary":'])
def test_empty_or_malformed_text_is_failure(provider, text):
    factory, _ = provider
    reply = response()
    body = reply.json()
    body["candidates"][0]["content"]["parts"] = [{"text": text}]
    _, metadata = analyze(factory([httpx.Response(200, json=body)]))
    assert metadata["error"] == "model_invalid_output"


@pytest.mark.parametrize("finish", ["SAFETY", "RECITATION", "OTHER"])
def test_non_stop_finish_reason_is_failure(provider, finish):
    factory, _ = provider
    _, metadata = analyze(factory([response(finish=finish)]))
    assert metadata["error"] == "model_incomplete_output"


@pytest.mark.parametrize("adjustment,expected", [(-5, -5), (5, 5), (6, 0), (-6, 0), (True, 0), ("5", 0)])
def test_rating_adjustments_are_bounded(provider, adjustment, expected):
    factory, _ = provider
    payload = copy.deepcopy(VALID)
    payload["adjustments"]["rating_adjustment"] = adjustment
    analyzer = factory([response(payload)])
    output, metadata = analyze(analyzer)
    assert output.adjustments["rating_adjustment"] == expected
    for project, baseline, stats in synthetic_cases():
        result = analyzer.create_final_analysis(project, baseline, output, metadata)
        assert result.predictions.expected_gestation_days == baseline.expected_gestation_days
        assert result.predictions.expected_conversion_rate == baseline.expected_conversion_rate
        assert result.predictions.rating_score == max(1, min(100, baseline.rating_score + expected))


def test_comparison_override_omits_unsupported_thinking_level(provider):
    factory, requests = provider
    analyze(factory(model="gemini-2.5-flash"))
    assert "thinkingConfig" not in json.loads(requests[0].content)["generationConfig"]


def test_unknown_prices_remain_unavailable(provider, monkeypatch):
    factory, _ = provider
    monkeypatch.setattr(module, "GEMINI_INPUT_USD_PER_MILLION", None)
    analyze_result = factory()
    analyze(analyze_result)
    assert analyze_result.get_usage_statistics()["estimated_cost"] is None
    assert not analyze_result.get_usage_statistics()["cost_configured"]


def test_response_parts_exclude_thought_summaries(provider):
    factory, _ = provider
    analyzer = factory()
    part = SimpleNamespace(text="private reasoning", thought=True)
    assert analyzer._normalize_part(part) is None


def test_synthetic_smoke_check_uses_same_provider_and_reports_failures(provider):
    factory, _ = provider
    factory([response(), response({}), response()])
    result = check_model("gemini-3.8-flash")
    assert not result["passed"]
    assert [case["passed"] for case in result["cases"]] == [True, False, True]


@pytest.fixture
def service(monkeypatch):
    from src.services import analysis_service
    from src.services import monday_archive

    project, baseline, stats = next(synthetic_cases())
    db = Mock()
    db.is_project_reporting_excluded.return_value = False
    db.client.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value.data = {"monday_id": project.project_id}
    monkeypatch.setattr(monday_archive, "enabled", lambda: False)
    svc = analysis_service.AnalysisService(db_client=db)
    svc.llm_enabled = True
    svc.analyze_project = Mock(return_value={**baseline.model_dump(), "llm_model": "numeric-baseline"})
    svc._to_project_features = Mock(return_value=project)
    svc._build_segment_stats = Mock(return_value=stats)
    svc._to_numeric_predictions = Mock(return_value=baseline)
    return svc, db


def test_failed_llm_does_not_overwrite_stored_analysis(provider, service):
    factory, _ = provider
    svc, db = service
    svc.llm_analyzer = factory([response({})])
    result = svc.analyze_and_store("synthetic", with_llm=True)
    assert not result["success"]
    assert result["error"] == "model_invalid_output"
    assert result["llm_metadata"]["llm_model"] == "gemini-3.8-flash"
    db.store_analysis_result.assert_not_called()


def test_direct_batch_callers_cannot_finalize_fallback_output(provider):
    factory, _ = provider
    analyzer = factory([response({})])
    output, metadata = analyze(analyzer)
    project, baseline, _ = next(synthetic_cases())
    with pytest.raises(ValueError, match="Cannot finalize failed"):
        analyzer.create_final_analysis(project, baseline, output, metadata)


@pytest.mark.parametrize("setting,value", [
    ("GEMINI_THINKING_LEVEL", "minimal"), ("GEMINI_TIMEOUT_SECONDS", 0),
    ("GEMINI_TIMEOUT_SECONDS", float("nan")), ("GEMINI_MAX_OUTPUT_TOKENS", 0),
    ("GEMINI_MAX_OUTPUT_TOKENS", 65537), ("GEMINI_INPUT_USD_PER_MILLION", "-1"),
])
def test_invalid_configuration_is_rejected_before_requests(provider, monkeypatch, setting, value):
    factory, requests = provider
    monkeypatch.setattr(module, setting, value)
    with pytest.raises(ValueError):
        factory()
    assert not requests


def test_successful_llm_stores_new_version_and_returns_usage(provider, service):
    factory, _ = provider
    svc, db = service
    svc.llm_analyzer = factory()
    result = svc.analyze_and_store("synthetic", with_llm=True)
    assert result["success"]
    stored = db.store_analysis_result.call_args.args[1]
    assert stored["llm_model"] == "gemini-3.8-flash"
    assert stored["analysis_version"] == "v1.1-llm"
    assert result["llm_metadata"]["tokens"]["thinking"] == 7


def test_explicit_llm_without_key_is_failure_but_numeric_mode_still_stores(service):
    svc, db = service
    svc.llm_enabled = False
    result = svc.analyze_and_store("synthetic", with_llm=True)
    assert result == {"success": False, "error": "model_not_configured"}
    db.store_analysis_result.assert_not_called()
    assert svc.analyze_and_store("synthetic", with_llm=False)["success"]
    db.store_analysis_result.assert_called_once()


def test_configuration_failure_keeps_previous_result(service, monkeypatch):
    from src.services import analysis_service
    svc, db = service
    monkeypatch.setattr(analysis_service, "LLMAnalyzer", Mock(side_effect=ValueError("bad setting")))
    assert svc.analyze_and_store("synthetic", with_llm=True)["error"] == "model_configuration_error"
    db.store_analysis_result.assert_not_called()
