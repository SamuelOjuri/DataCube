"""Standalone ASGI application; no ETL, model or scheduler startup."""
from contextlib import asynccontextmanager
import csv
import io
import json
import logging
import time
import asyncio
import hmac
from uuid import UUID, uuid4

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response, StreamingResponse
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
from .monday_source import MondaySourceReader, SourceCheckRequest, SourceCheckService, SourceEvidence
from .metrics.compiler import InvalidMetricRequest
from .metrics.contracts import EntityRequest, EntityResolution, MetricRequest, MetricResult
from .metrics.service import MetricService
from .workflow.contracts import Reply, Submission, WorkflowRun
from .workflow.provider import GeminiProvider
from .workflow.service import WorkflowService
from .workflow.store import TERMINAL
from .operations.telemetry import correlation_id, operation_context
from .presentation import Feedback, projects, save_feedback
from . import __version__

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
        app.state.metrics = MetricService(app.state.store)
        app.state.auth_store = AuthStore(database)
        await database.open()
        try:
            async with httpx.AsyncClient(timeout=10, follow_redirects=False, trust_env=False,
                                         limits=httpx.Limits(max_connections=8, max_keepalive_connections=4)) as client:
                app.state.monday = MondayOAuth(config, client)
                app.state.monday_source = MondaySourceReader(config, client)
                app.state.source_checks = SourceCheckService(app.state.store, app.state.metrics.compiler,
                                                           app.state.monday_source)
                app.state.workflow = WorkflowService(app.state.store, app.state.metrics, GeminiProvider(config,client,database.telemetry))
                try:
                    yield
                finally:
                    await app.state.workflow.close()
        finally:
            await database.close()

    app = FastAPI(title="DataCube BI Analyst", version=__version__, lifespan=lifespan,
                  docs_url=None, redoc_url=None, openapi_url=None)

    @app.middleware("http")
    async def request_context(request: Request, call_next):
        request_id = uuid4()  # Never trust an externally supplied ID for the audit key.
        request.state.request_id = request_id
        with operation_context(request_id=request_id):
            return await record_request(request,call_next,request_id)

    async def record_request(request: Request, call_next, request_id):
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
        run_id = correlation_id(getattr(request.state, "run_id", request.path_params.get("run_id")))
        with operation_context(run_id=run_id):
            request.app.state.database.telemetry.observe("http", "failed" if response.status_code >= 500 else
                "rejected" if response.status_code >= 400 else "success", time.monotonic()-started)
        log.info(json.dumps({"event": "http_request", "request_id": str(request_id),
                             "run_id": run_id,
                             "route": route, "method": request.method, "status": response.status_code,
                             "auth_outcome": getattr(request.state, "auth_outcome", None),
                             "duration_ms": round((time.monotonic()-started)*1000)}))
        return response

    @app.exception_handler(RequestValidationError)
    async def invalid_request(request, exc):
        # FastAPI's default includes rejected input; questions can contain sensitive text.
        return JSONResponse({"detail": "invalid_request"}, status_code=422)

    @app.exception_handler(InvalidMetricRequest)
    async def invalid_metric(request, exc):
        return JSONResponse({"detail": "invalid_metric_request"}, status_code=422)

    @app.exception_handler(TimeoutError)
    async def metric_timeout(request, exc):
        return JSONResponse({"detail": "metric_query_timeout"}, status_code=504)

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

    @app.get("/ops/metrics", include_in_schema=False)
    async def operational_metrics(request: Request):
        token = settings.telemetry_token
        supplied = request.headers.get("Authorization", "")
        if not token or not hmac.compare_digest(supplied.encode(), ("Bearer " + token.get_secret_value()).encode()):
            raise HTTPException(404, "not_found")
        return Response(request.app.state.database.telemetry.render(request.app.state.database,
            request.app.state.metrics, request.app.state.workflow), media_type="text/plain; version=0.0.4")

    @app.get("/health/ready")
    async def ready(request: Request):
        try:
            await request.app.state.database.ready()
        except Exception:
            return JSONResponse({"status": "not_ready"}, status_code=503)
        # Infrastructure readiness is deliberately distinct from feature availability.
        accepted = request.app.state.metrics.compiler.catalogue.owner_accepted
        execution = "evaluation_only" if settings.metric_evaluation_enabled else "owner_accepted" if accepted else "unavailable"
        if settings.business_timezone is None:
            execution = "unavailable"
        if not settings.analyst_enabled:
            execution = "disabled"
        return {"status": "ready", "identity": "monday" if settings.auth_provider == "monday" else "deferred",
                "metric_execution": execution, "monday_source_reads": request.app.state.monday_source.configured,
                "workflow": "enabled" if settings.workflow_enabled and settings.analyst_enabled else "disabled",
                "api_version": __version__, "contract_version": "v1", "schema_version": request.app.state.database.schema_version,
                "catalogue_version": request.app.state.metrics.compiler.catalogue.version,
                "catalogue_sha256": request.app.state.metrics.compiler.catalogue_hash,
                "environment": settings.environment, "pilot_only": settings.pilot_only, "disabled_metrics": settings.disabled_metrics}

    @app.get("/v1/metrics")
    async def metric_catalogue(request: Request, actor: Principal = Depends(principal)):
        compiler = request.app.state.metrics.compiler
        acceptance = "owner_accepted" if compiler.catalogue.owner_accepted else "pending_phase1"
        return {"catalogue_version": compiler.catalogue.version, "catalogue_sha256": compiler.catalogue_hash,
                "metrics": [{**m.model_dump(mode="json"), "recorded_certification": m.certification,
                             "limitations": compiler.catalogue.runtime_limitations(m),
                             "certification": acceptance, "enabled": m.id not in settings.disabled_metrics}
                            for m in compiler.catalogue.metrics],
                "source_checks": request.app.state.monday_source.capability(),
                "notices": compiler.catalogue.runtime_notices}

    @app.get("/v1/metrics/resolve")
    async def resolve_metric(request: Request, name: str = Query(min_length=1, max_length=128),
                             actor: Principal = Depends(principal)):
        catalogue = request.app.state.metrics.compiler.catalogue
        normalized = name.strip().casefold()
        matches = [m for m in catalogue.metrics if normalized in
                   {m.id.casefold(), m.label.casefold(), *(a.casefold() for a in m.aliases)}]
        return {"status": "resolved" if len(matches) == 1 else "clarification_required" if matches else "not_found",
                "candidates": [{"metric_id": m.id, "metric_version": m.version, "label": m.label,
                                "population": m.population} for m in matches]}

    @app.post("/v1/entities/resolve", response_model=EntityResolution)
    async def resolve_entity(body: EntityRequest, request: Request, actor: Principal = Depends(principal)):
        try:
            return await request.app.state.metrics.resolve_entity(actor, body)
        except (psycopg.errors.QueryCanceled, psycopg.errors.LockNotAvailable):
            raise HTTPException(504, "metric_query_timeout") from None

    @app.post("/v1/runs/{run_id}/metric", response_model=MetricResult, status_code=201)
    async def execute_metric(run_id: UUID, body: MetricRequest, request: Request, actor: Principal = Depends(principal)):
        try:
            return await request.app.state.metrics.execute(actor, run_id, body, request)
        except HTTPException as exc:
            if exc.detail != "metric_not_certified":
                raise
            return JSONResponse(status_code=503, content={"detail": "metric_not_certified",
                "source_check": {**request.app.state.monday_source.capability(),
                                 "path": f"/v1/runs/{run_id}/source-check",
                                 "reason": "metric_not_certified"}})

    @app.post("/v1/runs/{run_id}/source-check", response_model=SourceEvidence)
    async def check_source(run_id: UUID, body: SourceCheckRequest, request: Request, actor: Principal = Depends(principal)):
        return await request.app.state.source_checks.check(actor, run_id, body, request)

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
        if request.app.state.database.schema_version in (6, 7) and await request.app.state.workflow.cancel(actor,run_id):
            return await request.app.state.store.run(actor,run_id)
        return await request.app.state.store.run(actor, run_id, cancel=True)

    @app.post("/v1/runs/{run_id}/resume")
    async def resume_run(run_id: UUID, request: Request, body: Reply | None = None, actor: Principal = Depends(principal)):
        await request.app.state.store.run(actor,run_id)
        request.app.state.workflow.require_enabled()
        if body is None:
            raise HTTPException(422,'clarification_reply_required')
        result = await request.app.state.workflow.resume(actor,run_id,body)
        request.state.run_id = result.run_id
        return result

    @app.post("/v1/conversations/{conversation_id}/messages", response_model=WorkflowRun, status_code=202)
    async def message(conversation_id: UUID, body: Submission, request: Request, actor: Principal = Depends(principal)):
        result = await request.app.state.workflow.submit(actor,conversation_id,body)
        request.state.run_id = result.run_id
        return result

    @app.get("/v1/runs/{run_id}/workflow", response_model=WorkflowRun)
    async def workflow_run(run_id: UUID, request: Request, actor: Principal = Depends(principal)):
        workflow = request.app.state.workflow
        workflow.require_enabled()
        return workflow.jobs.public(await workflow.jobs.get(actor,run_id,expire=True))

    @app.get("/v1/runs/{run_id}/events")
    async def events(run_id: UUID, request: Request, after: int = Query(0,ge=0,le=128),
                     actor: Principal = Depends(principal), identity: Identity = Depends(current_identity)):
        workflow = request.app.state.workflow
        workflow.require_enabled()
        await workflow.jobs.get(actor,run_id,expire=True)
        async def stream():
            cursor, heartbeat = after, time.monotonic()
            while not await request.is_disconnected():
                try:
                    # Revalidate the same live session throughout a long-lived stream.
                    if settings.auth_provider == 'monday':
                        live_identity = await current_identity(request)
                        if live_identity.subject != actor.subject or live_identity.permissions_version != actor.permissions_version:
                            raise HTTPException(403,'permissions_changed')
                    state, rows = await workflow.jobs.snapshot(actor,run_id,cursor)
                except HTTPException:
                    yield 'event: terminal\ndata: {"status":"access_changed"}\n\n'
                    return
                except Exception:
                    yield 'event: terminal\ndata: {"status":"stream_unavailable"}\n\n'
                    return
                for event in rows:
                    cursor = event['sequence']
                    yield f"id: {cursor}\nevent: {event['kind']}\ndata: {json.dumps(event['payload'],ensure_ascii=True)}\n\n"
                if state['status'] in TERMINAL or state['status'] == 'awaiting_clarification':
                    return
                if time.monotonic()-heartbeat >= 10:
                    yield ': heartbeat\n\n'
                    heartbeat = time.monotonic()
                await asyncio.sleep(0.5)
        return StreamingResponse(stream(),media_type='text/event-stream',
                                 headers={'X-Accel-Buffering':'no','Cache-Control':'no-store'})

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

    @app.get("/v1/results/{result_id}/projects")
    async def result_projects(result_id: UUID, request: Request, limit: int = Query(25, ge=1, le=100),
                              offset: int = Query(0, ge=0, le=10000), actor: Principal = Depends(principal)):
        return await projects(request.app.state.metrics, actor, result_id, limit, offset, request)

    @app.post("/v1/results/{result_id}/feedback")
    async def result_feedback(result_id: UUID, body: Feedback, request: Request, actor: Principal = Depends(principal)):
        return await save_feedback(request.app.state.store, actor, result_id, body)

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
    logger = logging.getLogger("bi_analyst")
    if not logger.handlers:
        logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.INFO)
    logger.propagate = False
    for name in ("httpx", "httpcore"):
        logging.getLogger(name).setLevel(logging.WARNING)
    return create_app(settings)
