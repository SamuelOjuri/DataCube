# Health probe integration

The API exposes two different kinds of health signal:

- `GET /health/ready` is the platform readiness probe. It reads cached worker and
  database status from `worker_monitor`, returning HTTP 503 when not ready. It
  must not call Supabase, Monday.com, or another network service.
- `GET /health` (without a trailing slash) remains an alias for readiness.
- `GET /health/live` reads local worker liveness, returning HTTP 503 for dead,
  stale, or overdue workers without contacting dependencies. A database outage
  alone does not fail liveness.
- `GET /health/dependencies` is the deep operational probe. It checks Supabase
  and returns HTTP 503 when that dependency is unavailable.
- `GET /health/` (with a trailing slash) is the detailed diagnostic endpoint.
  Supabase and Monday client construction and SDK calls run in FastAPI's thread
  pool so they do not block the event loop. Dependency failures are logged and
  reported as `degraded` in the response.
- `GET /health/workers` retains the protected worker snapshot endpoint; configure
  `WORKER_MONITOR_TOKEN` and send its value as a bearer token. Missing or invalid
  authorization returns HTTP 403.

Use only the local probes for platform health checks. Diagnostic and liveness
timestamps are timezone-aware UTC.

## Resolving an overlapping local change

If a cherry-pick or patch conflicts in `src/api/app.py`, retain the local router
registrations and add the health router exactly once:

```python
from .routes.health import router as health_router

# Keep the application's other include_router calls.
app.include_router(health_router)
```

In particular, do **not** resolve the conflict by accepting the complete
incoming version of `app.py`. The application may have gained worker lifecycle
management since the health-probe branch diverged. Keep these imports when they
exist on the target branch:

```python
from ..services.worker_monitor import monitor
from ..services.scheduled_workers import ScheduledWorkers
from ..services.monday_lifecycle import worker as lifecycle_worker
```

The corresponding application setup should contain one copy of each router and
should keep the target branch's scheduler wrapper:

```python
app = FastAPI()
app.include_router(analysis_router)
app.include_router(forecast_router)
app.include_router(health_router)

_scheduler = AsyncIOScheduler(timezone="UTC")
_scheduled_workers = ScheduledWorkers(_scheduler)
```

Likewise, retain the newer worker-aware startup and shutdown implementation.
For the conflict shown in the integration example, the shutdown handler should
be resolved to:

```python
@app.on_event("shutdown")
async def _shutdown() -> None:
    try:
        await asyncio.gather(
            _scheduled_workers.stop(),
            get_task_queue().stop(),
            lifecycle_worker.stop(),
        )
    finally:
        await monitor.stop()
```

This preserves scheduler, queue, lifecycle-worker, and monitor cleanup. The
older alternative that only calls `_scheduler.shutdown()` and stops the task
queue belongs to the branch predating `ScheduledWorkers`; it must not replace
the newer target-branch lifecycle code.

If it conflicts in `src/api/routes/health.py`, preserve any unrelated local
routes, but keep these behavioral boundaries:

1. `/ready` and `/live` read `monitor.snapshot()` without constructing either
   downstream client. Preserve the cached readiness/liveness status codes;
   do not replace these probes with unconditional `ready`/`alive` responses.
2. `/dependencies` runs the Supabase check through `run_in_threadpool` and
   returns HTTP 503 on failure.
3. Blocking checks in the detailed `/health/` handler use
   `run_in_threadpool`.

For the conflict between the original synchronous health route and the
non-blocking probe implementation, resolve the imports and helpers to:

```python
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
    supabase = SupabaseClient()
    supabase.client.table("projects").select("count").limit(1).execute()


def _check_monday() -> bool:
    return MondayClient().test_connection()
```

Keep the readiness alias, `/ready`, `/live`, and protected `/workers` routes from
the worker-monitor implementation. Add `/` for detailed diagnostics and
`/dependencies` for the explicit dependency check, with one handler per path.
In particular, use this implementation for the readiness and dependency routes:

```python
@router.get("")
@router.get("/ready")
async def readiness_check() -> JSONResponse:
    state = monitor.snapshot()
    return JSONResponse(
        {"status": "ready" if state["ready"] else "not_ready", "database": state["database"]},
        status_code=200 if state["ready"] else 503,
    )


@router.get("/dependencies")
async def dependency_readiness_check(response: Response) -> Dict[str, Any]:
    try:
        await run_in_threadpool(_check_supabase)
    except Exception as exc:
        logger.warning("Supabase dependency probe failed: %s", exc)
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {"status": "not ready", "services": {"supabase": "unhealthy"}}
    return {"status": "ready", "services": {"supabase": "healthy"}}
```

Delete the original `/ready` implementation that constructs a
`SupabaseClient` directly. It recreates the blocking readiness behavior this
change is intended to remove. Also delete every Git conflict marker rather than
leaving either marker section around the resolved functions.

After resolving the files, verify that no merge markers remain and run the
focused tests:

```bash
rg -n '^(<<<<<<<|=======|>>>>>>>)' src tests docs
python -m pytest tests/test_health_routes.py tests/test_worker_monitoring.py tests/test_recent_rehydrate.py -q
python -m compileall -q src tests/test_health_routes.py
git diff --check
```

The probe change only addresses request starvation caused by slow health
checks. It does not diagnose an analyst run failure by itself; diagnosing a run
requires logs containing its run ID and terminal event.
