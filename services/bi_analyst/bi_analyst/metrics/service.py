"""Bounded read-only execution, consistent evidence and owner-scoped persistence."""
import asyncio
from contextlib import asynccontextmanager
import json
from uuid import UUID, uuid4

from fastapi import HTTPException, Request
import psycopg

from ..store import Principal, Store
from ..operations.telemetry import measured
from .calculations import compare, share
from .compiler import COMPILER_VERSION, Compiler, InvalidMetricRequest
from .contracts import (Aggregate, EntityRequest, EntityResolution, Freshness, MetricProvenance,
                        MetricRequest, MetricResult)


def wire_size(value) -> int:
    # This representation is conservative for PostgreSQL jsonb storage as well.
    return len(json.dumps(value, ensure_ascii=True, default=str).encode("utf-8"))


class MetricService:
    def __init__(self, store: Store):
        self.store, self.db = store, store.db
        self.settings = store.db.settings
        self.compiler = Compiler()
        self.active = 0
        self.runs: set[UUID] = set()

    def require_access(self, metric_id: str, version: str, population: str):
        if not self.settings.analyst_enabled:
            raise HTTPException(503, "analyst_disabled")
        if metric_id in self.settings.disabled_metrics:
            raise HTTPException(503, "metric_disabled")
        metric = self.compiler.metric(metric_id, version, population)
        if not self.settings.metric_evaluation_enabled:
            # Reuse the owner's recorded closure, bound to this catalogue fingerprint.
            try:
                self.compiler.catalogue.require_queryable(metric.id, population)
            except ValueError:
                raise HTTPException(503, "metric_not_certified") from None
        if self.settings.business_timezone is None:
            raise HTTPException(503, "business_timezone_not_configured")
        return metric

    @asynccontextmanager
    async def admitted(self):
        # No await between checking and incrementing: atomic on this event loop.
        if self.active >= self.settings.metric_concurrency:
            raise HTTPException(429, "metric_capacity_reached", headers={"Retry-After": "1"})
        self.active += 1
        try:
            yield
        finally:
            self.active -= 1

    async def execute(self, principal: Principal, run_id: UUID, body: MetricRequest,
                      http_request: Request | None = None) -> MetricResult:
        run = await self.store.run(principal, run_id)
        if run["status"] != "registered" or run_id in self.runs:
            raise HTTPException(409, "run_not_executable")
        self.compiler.validate(body)
        self.require_access(body.metric_id, body.metric_version, body.population)
        if body.limit > self.settings.metric_max_rows:
            raise InvalidMetricRequest("row_limit_exceeds_server_budget")
        async with self.admitted():
            self.runs.add(run_id)
            work = None
            try:
                async with asyncio.timeout(self.settings.metric_timeout_seconds):
                    work = asyncio.create_task(self.query(principal, run_id, body))
                    while not work.done():
                        await asyncio.wait({work}, timeout=0.2)
                        # Durable status permits cancellation on a different replica.
                        state = await self.store.run(principal, run_id)
                        if state["status"] != "registered":
                            raise HTTPException(409, "run_not_executable")
                        if http_request is not None and await http_request.is_disconnected():
                            await self.store.run(principal, run_id, cancel=True)
                            raise HTTPException(409, "request_disconnected")
                    result = await work
                    # The read connection is released before application-state writes.
                    await self.store.finish_metric(principal, result)
                    return result
            except (TimeoutError, psycopg.errors.QueryCanceled, psycopg.errors.LockNotAvailable):
                await self.store.fail_metric(principal, run_id)
                raise HTTPException(504, "metric_query_timeout") from None
            except asyncio.CancelledError:
                await self.store.run(principal, run_id, cancel=True)
                raise
            except Exception:
                await self.store.fail_metric(principal, run_id)
                raise
            finally:
                if work is not None:
                    if not work.done():
                        work.cancel()  # Psycopg cancels the SQL and rolls back on unwind.
                    await asyncio.gather(work, return_exceptions=True)
                self.runs.discard(run_id)

    @measured("query", operation="metric_query", run_parameter="run_id")
    async def query(self, principal: Principal, run_id: UUID, body: MetricRequest) -> MetricResult:
        self.require_access(body.metric_id, body.metric_version, body.population)
        timezone = self.settings.business_timezone
        async with self.db.transaction(analytical=True) as conn:
            await conn.execute("SELECT set_config('TimeZone',%s,true)", (timezone,))
            now = (await (await conn.execute("SELECT transaction_timestamp() AS now")).fetchone())["now"]
            plan = self.compiler.compile(body, now=now, business_timezone=timezone,
                                         max_cell_bytes=self.settings.metric_max_bytes)
            total = Aggregate.model_validate(await (await conn.execute(plan.total.sql, plan.total.params)).fetchone())
            change, comparison_period = None, None
            if body.comparison:
                comparator = body.model_copy(update={
                    "period": body.comparison.period, "comparison": None,
                    "filters": body.filters if body.comparison.filters is None else body.comparison.filters,
                })
                comparison_plan = self.compiler.compile(comparator, now=now, business_timezone=timezone)
                comparison_period = comparison_plan.period
                baseline = Aggregate.model_validate(await (await conn.execute(
                    comparison_plan.total.sql, comparison_plan.total.params)).fetchone())
                change = compare(total, baseline, unit=plan.metric.unit)
            coverage = await (await conn.execute("SELECT * FROM analyst_query.coverage_v1")).fetchone()
            self.db.telemetry.coverage(coverage)
            rows, reasons, matched_groups = [], [], 0
            names = [column.name for column in plan.columns]
            row_bytes = 0
            # Server cursor prevents buffering all limited rows before byte checks.
            async with conn.cursor(name="metric_rows") as cursor:
                await cursor.execute(plan.rows.sql, plan.rows.params)
                while row := await cursor.fetchone():
                    matched_groups = row.pop("matched_groups")
                    if len(rows) >= body.limit:
                        reasons.append("row_limit")
                        break
                    if row.pop("oversized_cell"):
                        reasons.append("byte_limit")
                        break
                    if body.include_share:
                        row["share"] = share(row["value"], total.value)
                    cells = [row[name] for name in names]
                    size = wire_size(cells)
                    if row_bytes + size > self.settings.metric_max_bytes:
                        reasons.append("byte_limit")
                        break
                    rows.append(cells)
                    row_bytes += size
        # No database connection remains held while serializing or presenting.
        relation = self.compiler.relations[plan.metric.relation]
        provenance = MetricProvenance(
            query_reference=plan.reference, compiler_version=COMPILER_VERSION,
            catalogue_version=self.compiler.catalogue.version, catalogue_sha256=self.compiler.catalogue_hash,
            metric_id=plan.metric.id, metric_version=plan.metric.version, metric_label=plan.metric.label,
            unit=plan.metric.unit, source_population=plan.metric.population, source_relation="analyst_query." + relation.id,
            source_grain=relation.grain, request=body, resolved_period=plan.period,
            comparison_period=comparison_period, business_timezone=timezone, columns=plan.columns,
            total=total if body.include_total else None,
            total_scope="complete_filtered_population" if body.include_total else "not_requested",
            comparison=change, freshness=Freshness(queried_at=now), coverage=coverage,
            limitations=self.compiler.catalogue.runtime_limitations(plan.metric) + [
                "Coverage counters describe the unfiltered gateway population; row-level known-value counts describe this query.",
                "Query time is not an ingestion, refresh, or historical business snapshot date.",
            ], evaluation_only=self.settings.metric_evaluation_enabled,
            certification_basis="local_evaluation" if self.settings.metric_evaluation_enabled else "owner_accepted",
            truncated=bool(reasons), truncation_reasons=reasons, returned_rows=len(rows),
            matched_groups=matched_groups, dataset_scope="limited" if reasons else "complete")
        result = MetricResult(id=uuid4(), run_id=run_id, permissions_version=principal.permissions_version,
                              columns=names, rows=rows, provenance=provenance, created_at=now)
        payload = result.model_dump(mode="json")
        if wire_size(payload) > self.settings.metric_max_bytes:
            if "byte_limit" not in provenance.truncation_reasons:
                provenance.truncation_reasons.append("byte_limit")
            provenance.truncated = True
            provenance.dataset_scope = "limited"
            # Account for metadata once, then retain a prefix in linear time.
            envelope = result.model_dump(mode="json")
            envelope["rows"] = []
            remaining = self.settings.metric_max_bytes - wire_size(envelope)
            if remaining < 0:
                raise HTTPException(413, "metric_metadata_exceeds_byte_budget")
            kept = 0
            for row in payload["rows"]:
                size = wire_size(row) + (2 if kept else 0)
                if size > remaining:
                    break
                remaining -= size
                kept += 1
            result.rows = result.rows[:kept]
            provenance.returned_rows = kept
        return result

    async def resolve_entity(self, principal: Principal, body: EntityRequest) -> EntityResolution:
        metric = self.require_access(body.metric_id, body.metric_version, body.population)
        if body.dimension not in metric.dimensions:
            raise InvalidMetricRequest("unsupported_dimension")
        column = self.compiler.dimensions[body.dimension]
        async with self.admitted(), asyncio.timeout(self.settings.metric_timeout_seconds):
            async with self.db.transaction(analytical=True) as conn:
                await conn.execute("SELECT set_config('TimeZone',%s,true)", (self.settings.business_timezone,))
                # position() treats %, _, quotes and backslashes as literal text.
                # Exact case-insensitive matches precede substring suggestions.
                rows = await (await conn.execute(
                    f'SELECT DISTINCT CASE WHEN length("{column}")<=256 THEN "{column}" END AS value, '
                    f'length("{column}")>256 AS oversized, lower("{column}")=lower(%s) AS exact '
                    f'FROM analyst_query."{metric.relation}" WHERE position(lower(%s) in lower("{column}"))>0 '
                    f'ORDER BY exact DESC,value NULLS LAST LIMIT 21',
                    (body.query, body.query))).fetchall()
            async with self.store.scoped(principal):
                pass  # Recheck permission after releasing the reader.
        exact = [r["value"] for r in rows if r["exact"] and not r["oversized"]]
        candidates = exact if exact else [r["value"] for r in rows[:20] if not r["oversized"]]
        truncated = ((len(rows) > 20 or any(r["oversized"] for r in rows))
                     and not (exact and not rows[-1]["exact"]))
        status = "resolved" if len(exact) == 1 and not truncated else "clarification_required" if candidates or truncated else "not_found"
        return EntityResolution(dimension=body.dimension, query=body.query, status=status,
                                candidates=candidates[:20], truncated=truncated)
