import importlib
import json
import re
import threading
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

import pytest
from fastapi import FastAPI, Response
from fastapi.testclient import TestClient

from src.api.routes import health

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def fresh_monday_probe(monkeypatch):
    monkeypatch.setattr(health, "_monday_probe", {"result": None, "checked": 0.0})


@pytest.fixture
def clock(monkeypatch):
    now = [1000.0]
    monkeypatch.setattr(health.time, "monotonic", lambda: now[0])
    return now


def test_monday_probe_is_reused_within_its_ttl(clock):
    # Each probe is a Monday API call; repeated diagnostics must not spend the daily allowance.
    with patch.object(health, "MondayClient") as monday:
        monday.return_value.test_connection.return_value = True
        assert all(health._check_monday() for _ in range(5))
        clock[0] += health.MONDAY_PROBE_TTL_SECONDS - 1
        assert health._check_monday() is True
        assert monday.return_value.test_connection.call_count == 1
        clock[0] += 2
        assert health._check_monday() is True
        assert monday.return_value.test_connection.call_count == 2


def test_failed_monday_probe_is_rechecked_sooner(clock):
    with patch.object(health, "MondayClient") as monday:
        monday.return_value.test_connection.side_effect = [False, True]
        assert health._check_monday() is False
        clock[0] += health.MONDAY_PROBE_FAILURE_TTL_SECONDS - 1
        assert health._check_monday() is False
        clock[0] += 2
        assert health._check_monday() is True
        assert monday.return_value.test_connection.call_count == 2


def test_monday_client_errors_are_not_cached(clock):
    with patch.object(health, "MondayClient", side_effect=[ValueError("no key"), Mock(
            test_connection=Mock(return_value=True))]):
        with pytest.raises(ValueError):
            health._check_monday()
        assert health._check_monday() is True


def test_repeated_detailed_health_requests_probe_monday_once(clock):
    app = FastAPI()
    app.include_router(health.router)
    client = TestClient(app)
    with patch.object(health, "_check_supabase"), patch.object(health, "MondayClient") as monday:
        monday.return_value.test_connection.return_value = True
        for _ in range(3):
            assert client.get("/health/").json()["services"]["monday_api"]["status"] == "healthy"
    assert monday.return_value.test_connection.call_count == 1


@pytest.mark.parametrize("relative_path", [
    ("src", "api", "app.py"),
    ("src", "api", "routes", "health.py"),
    ("docs", "health-probe-integration.md"),
    ("tests", "test_health_routes.py"),
])
def test_resolved_files_have_no_conflict_markers(relative_path):
    source = ROOT.joinpath(*relative_path).read_text(encoding="utf-8")

    assert not re.search(r"^(?:<{7}|={7}|>{7}|\|{7})(?:\s|$)", source, re.MULTILINE)


def test_app_registers_each_router_once():
    source = ROOT.joinpath("src", "api", "app.py").read_text(encoding="utf-8")

    for router_name in ("analysis_router", "forecast_router", "health_router"):
        assert source.count(f"app.include_router({router_name})") == 1


def test_router_exposes_health_endpoints_without_duplicates():
    paths = [route.path for route in health.router.routes]

    assert set(paths) == {
        "/health", "/health/", "/health/ready", "/health/dependencies",
        "/health/live", "/health/workers",
    }
    assert len(paths) == len(set(paths))


@pytest.mark.asyncio
@pytest.mark.parametrize("ready", [True, False])
async def test_readiness_uses_cached_state_without_downstream_calls(ready):
    database = "connected" if ready else "unavailable"
    with patch.object(health.monitor, "snapshot", return_value={
        "ready": ready, "database": database,
    }), patch.object(health, "SupabaseClient") as supabase, patch.object(
        health, "MondayClient",
    ) as monday, patch.object(health, "run_in_threadpool") as threadpool:
        response = await health.readiness_check()

    assert response.status_code == (200 if ready else 503)
    assert json.loads(response.body) == {
        "status": "ready" if ready else "not_ready", "database": database,
    }
    supabase.assert_not_called()
    monday.assert_not_called()
    threadpool.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("live", [True, False])
async def test_liveness_uses_cached_state_without_downstream_calls(live):
    with patch.object(health.monitor, "snapshot", return_value={"live": live}), patch.object(
        health, "SupabaseClient",
    ) as supabase, patch.object(health, "MondayClient") as monday, patch.object(
        health, "run_in_threadpool",
    ) as threadpool:
        response = await health.liveness_check()

    assert response.status_code == (200 if live else 503)
    result = json.loads(response.body)
    assert result["status"] == ("alive" if live else "unhealthy")
    assert datetime.fromisoformat(result["timestamp"]).tzinfo == timezone.utc
    supabase.assert_not_called()
    monday.assert_not_called()
    threadpool.assert_not_called()


@pytest.mark.asyncio
async def test_dependency_probe_reports_supabase_failure_with_503(caplog):
    response = Response()

    with patch.object(
        health, "_check_supabase", side_effect=RuntimeError("database unavailable"),
    ):
        result = await health.dependency_readiness_check(response)

    assert response.status_code == 503
    assert result == {
        "status": "not ready",
        "services": {"supabase": "unhealthy"},
    }
    assert "Supabase dependency probe failed" in caplog.text


@pytest.mark.asyncio
async def test_dependency_probe_reports_success_through_threadpool():
    response = Response()

    with patch.object(health, "_check_supabase") as check_supabase, patch.object(
        health, "run_in_threadpool",
    ) as threadpool:
        result = await health.dependency_readiness_check(response)

    threadpool.assert_awaited_once_with(check_supabase)
    assert response.status_code == 200
    assert result == {
        "status": "ready",
        "services": {"supabase": "healthy"},
    }


@pytest.mark.asyncio
async def test_detailed_health_constructs_and_calls_clients_in_threadpool():
    event_loop_thread = threading.get_ident()
    downstream_threads = []
    supabase = Mock()
    monday = Mock()

    def create_supabase():
        downstream_threads.append(threading.get_ident())
        return supabase

    def create_monday():
        downstream_threads.append(threading.get_ident())
        return monday

    monday.test_connection.return_value = True
    with patch.object(health, "SupabaseClient", side_effect=create_supabase), patch.object(
        health, "MondayClient", side_effect=create_monday,
    ), patch.object(health, "run_in_threadpool", wraps=health.run_in_threadpool) as threadpool:
        result = await health.health_check()

    assert [call.args[0] for call in threadpool.await_args_list] == [
        health._check_supabase, health._check_monday,
    ]
    assert len(downstream_threads) == 2
    assert all(thread != event_loop_thread for thread in downstream_threads)
    supabase.client.table.assert_called_once_with("projects")
    supabase.client.table.return_value.select.assert_called_once_with("count")
    supabase.client.table.return_value.select.return_value.limit.assert_called_once_with(1)
    supabase.client.table.return_value.select.return_value.limit.return_value.execute.assert_called_once()
    monday.test_connection.assert_called_once_with()
    assert result["status"] == "healthy"
    assert result["services"]["supabase"]["status"] == "healthy"
    assert isinstance(result["services"]["supabase"]["response_time_ms"], int)
    assert result["services"]["supabase"]["response_time_ms"] >= 0
    assert result["services"]["monday_api"] == {"status": "healthy", "authenticated": True}
    assert datetime.fromisoformat(result["timestamp"]).tzinfo == timezone.utc


@pytest.mark.asyncio
@pytest.mark.parametrize("failing_service", ["supabase", "monday"])
async def test_detailed_health_reports_and_logs_dependency_errors(failing_service, caplog):
    with patch.object(health, "_check_supabase") as supabase, patch.object(
        health, "_check_monday", return_value=True,
    ) as monday:
        failing_check = supabase if failing_service == "supabase" else monday
        failing_check.side_effect = RuntimeError("dependency unavailable")
        result = await health.health_check()

    service_key = "supabase" if failing_service == "supabase" else "monday_api"
    assert result["status"] == "degraded"
    assert result["services"][service_key] == {
        "status": "unhealthy", "error": "dependency unavailable",
    }
    supabase.assert_called_once_with()
    monday.assert_called_once_with()
    assert "diagnostic check failed" in caplog.text


@pytest.mark.asyncio
async def test_detailed_health_reports_failed_monday_authentication():
    with patch.object(health, "_check_supabase"), patch.object(
        health, "_check_monday", return_value=False,
    ):
        result = await health.health_check()

    assert result["status"] == "degraded"
    assert result["services"]["monday_api"] == {"status": "unhealthy", "authenticated": False}


def test_readiness_alias_and_detailed_endpoint_are_distinct():
    app = FastAPI()
    app.include_router(health.router)
    client = TestClient(app)
    with patch.object(health.monitor, "snapshot", return_value={
        "ready": True, "database": "connected",
    }), patch.object(health, "_check_supabase") as supabase, patch.object(
        health, "_check_monday", return_value=True,
    ) as monday:
        assert client.get("/health").json() == {"status": "ready", "database": "connected"}
        supabase.assert_not_called()
        monday.assert_not_called()
        assert client.get("/health/").json()["status"] == "healthy"
        supabase.assert_called_once_with()
        monday.assert_called_once_with()


@pytest.mark.asyncio
@pytest.mark.parametrize("failing_stop", [None, "scheduled", "queue", "lifecycle"])
async def test_shutdown_stops_all_workers_and_monitor_even_on_failure(monkeypatch, failing_stop):
    api = importlib.import_module("src.api.app")
    workers = {name: Mock(stop=AsyncMock()) for name in ("scheduled", "queue", "lifecycle")}
    monitor = Mock(stop=AsyncMock())
    monkeypatch.setattr(api, "_scheduled_workers", workers["scheduled"])
    monkeypatch.setattr(api, "get_task_queue", lambda: workers["queue"])
    monkeypatch.setattr(api, "lifecycle_worker", workers["lifecycle"])
    monkeypatch.setattr(api, "monitor", monitor)
    if failing_stop is None:
        await api._shutdown()
    else:
        workers[failing_stop].stop.side_effect = RuntimeError("stop failed")
        with pytest.raises(RuntimeError, match="stop failed"):
            await api._shutdown()

    for worker in workers.values():
        worker.stop.assert_awaited_once_with()
    monitor.stop.assert_awaited_once_with()
