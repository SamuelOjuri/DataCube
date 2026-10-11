"""Local worker probes and explicit thread-pooled dependency diagnostics."""
import hmac
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict

from fastapi import APIRouter, Header, HTTPException, Response, status
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse

from ...core.monday_client import MondayClient
from ...database.supabase_client import SupabaseClient
from ...services.worker_monitor import monitor

router = APIRouter(prefix="/health", tags=["health"])
logger = logging.getLogger(__name__)


def _check_supabase() -> None:
    """Run the blocking Supabase probe outside the application's event loop."""
    supabase = SupabaseClient()
    supabase.client.table("projects").select("count").limit(1).execute()


def _check_monday() -> bool:
    """Construct and call the blocking Monday client in a worker thread."""
    return MondayClient().test_connection()


@router.get("")
@router.get("/ready")
async def readiness_check() -> JSONResponse:
    """Read cached worker and database readiness without downstream calls."""
    state = monitor.snapshot()
    return JSONResponse(
        {"status": "ready" if state["ready"] else "not_ready", "database": state["database"]},
        status_code=200 if state["ready"] else 503,
    )


@router.get("/dependencies")
async def dependency_readiness_check(response: Response) -> Dict[str, Any]:
    """Check Supabase explicitly for operators and dependency alerting."""
    try:
        await run_in_threadpool(_check_supabase)
    except Exception as exc:
        logger.warning("Supabase dependency probe failed: %s", exc)
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {"status": "not ready", "services": {"supabase": "unhealthy"}}
    return {"status": "ready", "services": {"supabase": "healthy"}}


@router.get("/")
async def health_check() -> Dict[str, Any]:
    """Check downstream services without blocking unrelated HTTP requests."""
    health_status: Dict[str, Any] = {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "services": {},
    }

    try:
        started = time.perf_counter()
        await run_in_threadpool(_check_supabase)
        health_status["services"]["supabase"] = {
            "status": "healthy",
            "response_time_ms": round((time.perf_counter() - started) * 1000),
        }
    except Exception as exc:
        logger.warning("Supabase diagnostic check failed: %s", exc)
        health_status["services"]["supabase"] = {
            "status": "unhealthy",
            "error": str(exc),
        }
        health_status["status"] = "degraded"

    try:
        if await run_in_threadpool(_check_monday):
            health_status["services"]["monday_api"] = {
                "status": "healthy",
                "authenticated": True,
            }
        else:
            health_status["services"]["monday_api"] = {
                "status": "unhealthy",
                "authenticated": False,
            }
            health_status["status"] = "degraded"
    except Exception as exc:
        logger.warning("Monday diagnostic check failed: %s", exc)
        health_status["services"]["monday_api"] = {
            "status": "unhealthy",
            "error": str(exc),
        }
        health_status["status"] = "degraded"

    return health_status


@router.get("/live")
async def liveness_check() -> JSONResponse:
    """Read local worker liveness without querying downstream dependencies."""
    state = monitor.snapshot()
    return JSONResponse(
        {
            "status": "alive" if state["live"] else "unhealthy",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
        status_code=200 if state["live"] else 503,
    )


@router.get("/workers")
async def worker_status(authorization: str | None = Header(default=None)) -> JSONResponse:
    token = os.getenv("WORKER_MONITOR_TOKEN")
    if not token or not hmac.compare_digest(authorization or "", "Bearer " + token):
        raise HTTPException(status_code=403, detail="Worker monitoring access denied")
    state = monitor.snapshot()
    return JSONResponse(state, status_code=200 if state["ready"] else 503)
