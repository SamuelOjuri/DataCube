"""Standalone ASGI application. No ETL, model, scheduler or Monday imports."""
from contextlib import asynccontextmanager
import csv
import io
import json
import logging
import time
from uuid import UUID, uuid4

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
import psycopg
import httpx
from psycopg_pool import PoolTimeout, TooManyRequests
from starlette.middleware import Middleware

from .contracts import Conversation, ConversationInput, Result, Run, RunInput, csv_cell
from .database import Database
from .identity import Identity, current_identity
from .settings import Settings
from .store import Principal, Store
from .auth import routes as auth_routes
from .auth_store import AuthStore
from .monday_auth import MondayOAuth

log = logging.getLogger("bi_analyst.audit")


class BodyLimit:
    """Bound both declared and chunked request bodies before parsing or storing."""
    def __init__(self, app, limit: int):
        self.app, self.limit = app, limit

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        body = bytearray()
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            body.extend(message.get("body", b""))
            if len(body) > self.limit:
                response = JSONResponse({"detail": "request_too_large"}, status_code=413)
                return await response(scope, receive, send)
            if not message.get("more_body", False):
                break
        delivered = False

        async def replay():
            nonlocal delivered
            if not delivered:
                delivered = True
                return {"type": "http.request", "body": bytes(body), "more_body": False}
            return await receive()

        await self.app(scope, replay, send)


def create_app(settings: Settings) -> FastAPI:
    # Environment is loaded at lifespan startup, not on import.
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        config = settings
        database = Database(config)
        app.state.settings = config
        app.state.database = database
        app.state.store = Store(database)
        app.state.auth_store = AuthStore(database)
        await database.open()
        try:
            async with httpx.AsyncClient(timeout=10, follow_redirects=False, trust_env=False,
                                         limits=httpx.Limits(max_connections=8, max_keepalive_connections=4)) as client:
                app.state.monday = MondayOAuth(config, client)
                yield
        finally:
            await database.close()

    app = FastAPI(title="DataCube BI Analyst", version="0.3.1", lifespan=lifespan,
                  docs_url=None, redoc_url=None, openapi_url=None)

    @app.middleware("http")
    async def request_context(request: Request, call_next):
        request_id = uuid4()  # Never trust an externally supplied ID for the audit key.
        request.state.request_id = request_id
        started = time.monotonic()
        try:
            response = await call_next(request)
        except Exception:
            response = JSONResponse({"detail": "internal_error"}, status_code=500)
        route = getattr(request.scope.get("route"), "path", "unmatched")
        subject = getattr(request.state, "subject", None)
        if subject is not None:
            try:
                await request.app.state.store.audit(subject, request_id, route, request.method,
                                                     getattr(request.state, "audit_status", response.status_code))
            except Exception:
                # Do not release an unaudited data response or expose DB messages.
                response = JSONResponse({"detail": "audit_unavailable"}, status_code=503)
        response.headers["X-Request-ID"] = str(request_id)
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "no-referrer"
        log.info(json.dumps({"event": "http_request", "request_id": str(request_id),
                             "subject": str(subject) if subject else None,
                             "route": route, "method": request.method, "status": response.status_code,
                             "auth_outcome": getattr(request.state, "auth_outcome", None),
                             "duration_ms": round((time.monotonic()-started)*1000)}))
        return response

    @app.exception_handler(RequestValidationError)
    async def invalid_request(request, exc):
        # FastAPI's default includes rejected input; questions can contain sensitive text.
        return JSONResponse({"detail": "invalid_request"}, status_code=422)

    async def unavailable(request, exc):
        return JSONResponse({"detail": "storage_unavailable"}, status_code=503)

    for error in (psycopg.Error, PoolTimeout, TooManyRequests):
        app.add_exception_handler(error, unavailable)

    async def principal(request: Request, identity: Identity = Depends(current_identity)) -> Principal:
        request.state.subject = identity.subject
        actor = await request.app.state.store.authorize(identity.subject)
        if identity.permissions_version is not None and identity.permissions_version != actor.permissions_version:
            raise HTTPException(403, "permissions_changed")
        return actor

    app.include_router(auth_routes(principal))

    @app.get("/health/live")
    async def live():
        return {"status": "alive"}

    @app.get("/health/ready")
    async def ready(request: Request):
        try:
            await request.app.state.database.ready()
        except Exception:
            return JSONResponse({"status": "not_ready"}, status_code=503)
        # Infrastructure readiness is deliberately distinct from feature availability.
        return {"status": "ready", "identity": "monday" if settings.auth_provider == "monday" else "deferred",
                "metric_execution": "unavailable"}

    @app.post("/v1/conversations", response_model=Conversation, status_code=201)
    async def create_conversation(body: ConversationInput, request: Request, actor: Principal = Depends(principal)):
        return await request.app.state.store.conversations(actor, title=body.title)

    @app.get("/v1/conversations", response_model=list[Conversation])
    async def list_conversations(request: Request, limit: int = Query(50,ge=1,le=100),
                                 offset: int = Query(0,ge=0,le=10000), actor: Principal = Depends(principal)):
        return await request.app.state.store.conversations(actor, limit=limit, offset=offset)

    @app.get("/v1/conversations/{conversation_id}", response_model=Conversation)
    async def get_conversation(conversation_id: UUID, request: Request, actor: Principal = Depends(principal)):
        return await request.app.state.store.conversation(actor, conversation_id)

    @app.post("/v1/conversations/{conversation_id}/runs", response_model=Run, status_code=201)
    async def submit_run(conversation_id: UUID, body: RunInput, request: Request, actor: Principal = Depends(principal)):
        # Registration only: no task is scheduled and no metric query is executed.
        return await request.app.state.store.create_run(actor, conversation_id, body.question)

    @app.get("/v1/conversations/{conversation_id}/runs", response_model=list[Run])
    async def list_runs(conversation_id: UUID, request: Request, limit: int = Query(50,ge=1,le=100),
                        offset: int = Query(0,ge=0,le=10000), actor: Principal = Depends(principal)):
        return await request.app.state.store.runs(actor, conversation_id, limit, offset)

    @app.get("/v1/runs/{run_id}", response_model=Run)
    async def get_run(run_id: UUID, request: Request, actor: Principal = Depends(principal)):
        return await request.app.state.store.run(actor, run_id)

    @app.post("/v1/runs/{run_id}/cancel", response_model=Run)
    async def cancel_run(run_id: UUID, request: Request, actor: Principal = Depends(principal)):
        return await request.app.state.store.run(actor, run_id, cancel=True)

    @app.post("/v1/runs/{run_id}/resume")
    async def resume_run(run_id: UUID, request: Request, actor: Principal = Depends(principal)):
        await request.app.state.store.run(actor, run_id)
        raise HTTPException(501, "workflow_not_implemented")

    @app.get("/v1/results/{result_id}", response_model=Result)
    async def get_result(result_id: UUID, request: Request, actor: Principal = Depends(principal)):
        return await request.app.state.store.result(actor, result_id)

    @app.get("/v1/results/{result_id}/export")
    async def export_result(result_id: UUID, request: Request, actor: Principal = Depends(principal)):
        result = Result.model_validate(await request.app.state.store.result(actor, result_id))
        buffer = io.StringIO(newline="")
        writer = csv.writer(buffer)
        writer.writerow([csv_cell(c) for c in result.columns])
        writer.writerows([csv_cell(cell) for cell in row] for row in result.rows)
        return Response(buffer.getvalue().encode("utf-8-sig"), media_type="text/csv",
                        headers={"Content-Disposition": f'attachment; filename="result-{result_id}.csv"',
                                 "X-Export-Scope": "stored-result-rows"})

    # Keep request IDs/audit outside the body guard. CORS wraps error responses too.
    app.user_middleware.append(Middleware(BodyLimit, limit=settings.max_body_bytes))
    app.add_middleware(CORSMiddleware, allow_origins=settings.cors_origins,
                       allow_methods=["GET", "POST"], allow_headers=["Authorization", "Content-Type"],
                       expose_headers=["X-Request-ID", "X-Export-Scope"], allow_credentials=False,
                       max_age=600)
    return app


def application():
    """Uvicorn factory, including configured CORS/body limits without import I/O."""
    settings = Settings.from_env()
    return create_app(settings)
