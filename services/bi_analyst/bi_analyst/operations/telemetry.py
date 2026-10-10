"""Bounded Prometheus exposition and allowlisted JSON events; no request payloads.

Counters are process-local. Scrape each instance; use rate/increase across restarts.
No question, SQL, URL, exception text, identity, result, or model output is accepted.
"""
import asyncio
from collections import Counter
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, replace
from functools import wraps
from inspect import signature
import json
import logging
import time
from uuid import UUID, uuid4

from fastapi import HTTPException
import psycopg
from psycopg_pool import PoolTimeout, TooManyRequests

BOUNDS = (0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300)
STAGES = {"http", "query", "model", "workflow"}
OUTCOMES = {"success", "failed", "timeout", "cancelled", "interrupted", "clarification", "rejected"}
OPERATIONS = {
    "http": {"http_request"},
    "query": {"metric_query"},
    "model": {"model_call", "interpret", "plan", "presentation"},
    "workflow": {"run_workflow"},
}
DEFAULT_OPERATIONS = {"http": "http_request", "query": "metric_query",
                      "model": "model_call", "workflow": "run_workflow"}
ERROR_CODES = {
    "model_not_configured", "model_input_budget", "model_not_found",
    "model_authentication_failed", "model_request_rejected", "model_unavailable",
    "model_output_budget", "model_invalid_output", "model_invalid_plan", "model_timeout",
    "unsupported_question", "clarification_limit", "run_budget_exceeded",
    "permissions_changed", "access_denied", "metric_not_certified", "metric_disabled",
    "analyst_disabled", "workflow_disabled", "workflow_capacity_reached",
    "result_expired", "result_evidence_changed", "workflow_version_changed",
    "run_not_executable", "run_unavailable", "no_evidence", "invalid_evidence",
    "run_timeout", "metric_query_timeout", "execution_cancelled", "execution_interrupted",
    "storage_unavailable", "pool_timeout", "pool_capacity_reached", "request_timeout",
    "http_failed", "query_failed", "model_failed", "workflow_failed",
}
log = logging.getLogger("bi_analyst.operations")


def correlation_id(value):
    """Only canonical UUIDs can enter logs, never arbitrary URL/header values."""
    if not isinstance(value, (str, UUID)):
        return None
    try:
        return str(UUID(str(value)))
    except ValueError:
        return None


@dataclass(frozen=True)
class OperationContext:
    run_id: str | None = None
    request_id: str | None = None
    attempt: int = 1


_context = ContextVar("analyst_operation_context", default=OperationContext())


@contextmanager
def operation_context(*, run_id=None, request_id=None, attempt=None):
    """Task-local correlation, inherited by child tasks and restored on every exit."""
    updates = {}
    if run_id is not None:
        updates["run_id"] = correlation_id(run_id)
    if request_id is not None:
        updates["request_id"] = correlation_id(request_id)
    if attempt is not None:
        if type(attempt) is not int or not 1 <= attempt <= 8:
            raise ValueError("Unbounded operation attempt")
        updates["attempt"] = attempt
    token = _context.set(replace(_context.get(), **updates))
    try:
        yield
    finally:
        _context.reset(token)


def fixed_error_code(code, fallback):
    return code if type(code) is str and code in ERROR_CODES else fallback


def operation_failure(stage, error):
    fallback = stage + "_failed"
    if isinstance(error, asyncio.CancelledError):
        return "cancelled", "execution_cancelled"
    if isinstance(error, PoolTimeout):
        return "timeout", "pool_timeout"
    if isinstance(error, TooManyRequests):
        return "rejected", "pool_capacity_reached"
    if isinstance(error, (TimeoutError, psycopg.errors.QueryCanceled, psycopg.errors.LockNotAvailable)):
        return "timeout", {"model": "model_timeout", "query": "metric_query_timeout",
                           "workflow": "run_timeout", "http": "request_timeout"}[stage]
    if isinstance(error, psycopg.Error):
        return "failed", "storage_unavailable"
    if isinstance(error, HTTPException):
        return ("rejected" if error.status_code < 500 else "failed",
                fixed_error_code(error.detail, fallback))
    code = fixed_error_code(getattr(error, "code", None), fallback)
    return ("timeout" if code == "model_timeout" else "failed"), code


@dataclass
class Operation:
    outcome: str | None = None
    error_code: str | None = None

    def set_outcome(self, outcome, error_code=None):
        # Workflow failures are handled internally; retain their explicit outcome.
        if outcome not in OUTCOMES:
            raise ValueError("Unbounded telemetry label")
        self.outcome, self.error_code = outcome, error_code


class Telemetry:
    def __init__(self):
        self.calls, self.buckets, self.seconds, self.tokens = Counter(), Counter(), Counter(), Counter()
        self.coverage_values = {}
        self.coverage_observed_at = 0

    def observe(self, stage, outcome, seconds, *, operation=None, operation_id=None, error_code=None):
        if stage not in STAGES or outcome not in OUTCOMES:
            raise ValueError("Unbounded telemetry label")
        operation = operation or DEFAULT_OPERATIONS[stage]
        if operation not in OPERATIONS[stage]:
            raise ValueError("Unbounded telemetry operation")
        self.calls[stage, outcome] += 1
        self.seconds[stage] += seconds
        for bound in BOUNDS:
            if seconds <= bound:
                self.buckets[stage, bound] += 1
        context = _context.get()
        log.info(json.dumps({"event": "analyst_operation", "phase": "end", "stage": stage,
                             "operation": operation, "operation_id": correlation_id(operation_id),
                             "run_id": context.run_id, "request_id": context.request_id,
                             "attempt": context.attempt, "outcome": outcome,
                             "error_code": fixed_error_code(error_code, stage + "_failed")
                             if error_code is not None else None,
                             "duration_ms": round(seconds * 1000)}))

    @contextmanager
    def operation(self, stage, operation, *, run_id=None, attempt=None):
        if stage not in STAGES or operation not in OPERATIONS[stage]:
            raise ValueError("Unbounded telemetry operation")
        with operation_context(run_id=run_id, attempt=attempt):
            context, operation_id = _context.get(), uuid4()
            started, result = time.monotonic(), Operation()
            log.info(json.dumps({"event": "analyst_operation", "phase": "start", "stage": stage,
                                 "operation": operation, "operation_id": str(operation_id),
                                 "run_id": context.run_id, "request_id": context.request_id,
                                 "attempt": context.attempt}))
            try:
                yield result
            except BaseException as error:
                if result.outcome is None:
                    result.set_outcome(*operation_failure(stage, error))
                raise
            finally:
                self.observe(stage, result.outcome or "success", time.monotonic() - started,
                             operation=operation, operation_id=operation_id, error_code=result.error_code)

    def usage(self, usage):
        for kind, field in (("input", "promptTokenCount"), ("output", "candidatesTokenCount"), ("thinking", "thoughtsTokenCount")):
            value = usage.get(field, 0)
            if isinstance(value, int) and 0 <= value <= 10_000_000:
                self.tokens[kind] += value

    def coverage(self, coverage):
        # Keys come only from the fixed gateway, never model/client labels.
        from ..semantic import load_catalogue
        if not hasattr(self, '_coverage_columns'):
            self._coverage_columns = next(r.columns for r in load_catalogue().relations if r.id == "coverage_v1")
        self.coverage_values = {k: int(v) for k, v in coverage.items()
                                if k in self._coverage_columns and isinstance(v, (int, float)) and v >= 0}
        self.coverage_observed_at = time.time()

    def render(self, database, metrics, workflow):
        families = {}
        types = {'operations_total':'counter','duration_seconds':'histogram',
                 'model_tokens_total':'counter','model_estimated_usd_total':'counter'}
        def sample(name, value, **labels):
            suffix = "{" + ",".join(f'{k}="{v}"' for k, v in labels.items()) + "}" if labels else ""
            family = 'duration_seconds' if name.startswith('duration_seconds_') else name
            families.setdefault(family,[]).append(f"bi_analyst_{name}{suffix} {value}")
        for stage in sorted(STAGES):
            for outcome in sorted(OUTCOMES):
                sample("operations_total", self.calls[stage, outcome], stage=stage, outcome=outcome)
            for bound in BOUNDS:
                sample("duration_seconds_bucket", self.buckets[stage, bound], stage=stage, le=bound)
            count = sum(self.calls[stage, o] for o in OUTCOMES)
            sample("duration_seconds_bucket", count, stage=stage, le="+Inf")
            sample("duration_seconds_count", count, stage=stage)
            sample("duration_seconds_sum", self.seconds[stage], stage=stage)
        for name, pool in (("read", database.read), ("state", database.state)):
            stats = pool.get_stats()
            for field in ("pool_size", "pool_available", "requests_waiting", "requests_errors", "requests_wait_ms"):
                sample("pool_" + field, stats.get(field, 0), pool=name)
        sample("active_queries", metrics.active)
        sample("active_workflows", len(workflow.tasks))
        for kind in ("input", "output", "thinking"):
            sample("model_tokens_total", self.tokens[kind], kind=kind)
        config = database.settings
        priced = config.model_input_usd_per_million is not None
        sample("model_cost_configured", int(priced))
        if priced:
            sample("model_estimated_usd_total", (self.tokens['input'] * config.model_input_usd_per_million +
                (self.tokens['output'] + self.tokens['thinking']) * config.model_output_usd_per_million) / 1_000_000)
        # Source lineage is not available yet. Unknown must alert, never report query time as freshness.
        for source in ("ingestion", "rollup", "refresh", "snapshot"):
            sample("source_freshness_known", 0, source=source)
        sample("coverage_observed_timestamp_seconds", self.coverage_observed_at)
        for name, value in sorted(self.coverage_values.items()):
            sample("coverage", value, counter=name)
        lines = []
        for family, samples in families.items():
            lines.append(f"# TYPE bi_analyst_{family} {types.get(family,'gauge')}")
            lines.extend(samples)
        return "\n".join(lines) + "\n"


def measured(stage, *, operation=None, operation_parameter=None, run_parameter=None):
    def decorate(method):
        parameters = signature(method)
        @wraps(method)
        async def invoke(self, *args, **kwargs):
            arguments = parameters.bind(self, *args, **kwargs).arguments
            name = arguments[operation_parameter] if operation_parameter else operation or DEFAULT_OPERATIONS[stage]
            run_id = arguments[run_parameter] if run_parameter else None
            telemetry = getattr(self, "telemetry", None) or self.db.telemetry
            with telemetry.operation(stage, name, run_id=run_id,
                                     attempt=1 if stage == "query" else None):
                return await method(self, *args, **kwargs)
        return invoke
    return decorate
