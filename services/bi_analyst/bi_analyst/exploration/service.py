"""Bounded read-only execution of validated exploratory SQL."""
import asyncio
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from hashlib import sha256
import json
import math
import re
from uuid import UUID, uuid4

from fastapi import HTTPException
import psycopg
from psycopg.rows import tuple_row

from ..metrics.contracts import Freshness
from ..metrics.service import wire_size
from ..operations.telemetry import measured
from ..workflow.explanation import narrative
from .context import approved_relations, build_context
from .contracts import ExplorationDraft, ExploratoryColumn, ExploratoryProvenance, ExploratoryResult
from .policy import POLICY_VERSION, UnsafeQuery, ValidatedQuery, validate

INTEGER_TYPES = {20, 21, 23}  # int8, int2, int4
DECIMAL_TYPES = {700, 701, 1700}  # float4, float8, numeric
DATE_TYPES = {1082}
# SQLSTATE classes caused by the generated statement rather than the platform.
REPAIRABLE = {"0A", "21", "22", "25", "42", "53", "54"}
TITLE = re.compile(r"[^\w ,.()/&:'%\-]")
TIMEOUT = "The query exceeded the time limit. Filter earlier, aggregate before joining and avoid cross joins."
LIMITATIONS = [
    "Exploratory analysis: generated SQL over governed views, not a certified metric.",
    "Query time is not an ingestion, refresh, or historical business snapshot date.",
]


class ExplorationQueryError(RuntimeError):
    code = "exploration_query_failed"

    def __init__(self, feedback):
        super().__init__(feedback)
        self.feedback = feedback


def describe(error: psycopg.Error) -> str:
    """Repair guidance that never echoes source data values."""
    state = error.sqlstate or ""
    if isinstance(error, psycopg.errors.QueryCanceled):
        return TIMEOUT
    if state[:2] in {"42", "0A", "21"}:
        # These messages name identifiers and syntax from the generated SQL, not row values.
        diag = error.diag
        hint = f" Hint: {diag.message_hint[:200]}" if diag.message_hint else ""
        return f"PostgreSQL rejected the query ({state}): {(diag.message_primary or '')[:300]}.{hint}"
    if state[:2] in {"53", "54"}:
        return "The query exceeded database resource limits. Aggregate earlier and return fewer rows."
    if state[:2] == "25":
        return "The database rejected a write or session change. Use one read-only SELECT."
    return (f"PostgreSQL rejected a value while running the query ({type(error).__name__}, {state}). "
            "Guard casts, dates and divisors, for example with NULLIF.")


def cell(value):
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None or isinstance(value, (str, int)):
        return value
    if isinstance(value, Decimal):
        return value if value.is_finite() else str(value)
    if isinstance(value, float):
        return Decimal(repr(value)) if math.isfinite(value) else str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value
    if isinstance(value, (time, timedelta, UUID)):
        return str(value)
    return json.dumps(value, ensure_ascii=True, default=str)


def column_type(oid: int) -> str:
    return ("integer" if oid in INTEGER_TYPES else "decimal" if oid in DECIMAL_TYPES
            else "date" if oid in DATE_TYPES else "text")


class ExplorationService:
    def __init__(self, metrics):
        self.metrics, self.db, self.settings = metrics, metrics.db, metrics.settings
        self.catalogue = metrics.compiler.catalogue
        self.catalogue_hash = metrics.compiler.catalogue_hash
        self.relations = approved_relations(self.catalogue)

    def require_access(self):
        if not self.settings.analyst_enabled:
            raise HTTPException(503, "analyst_disabled")
        if not self.settings.exploratory_enabled:
            raise HTTPException(503, "exploration_disabled")
        if self.settings.business_timezone is None:
            raise HTTPException(503, "business_timezone_not_configured")

    def context(self, today: date) -> dict:
        return build_context(self.catalogue, self.relations, today=today, timezone=self.settings.business_timezone)

    def validate(self, draft: ExplorationDraft) -> ValidatedQuery:
        validated = validate(draft.sql, self.relations)
        if tuple(c.name for c in draft.columns) != validated.columns:
            raise UnsafeQuery("Declare exactly these output columns, in order: " + ", ".join(validated.columns) + ".")
        return validated

    @measured("query", operation="exploratory_query", run_parameter="run_id")
    async def execute(self, principal, run_id: UUID, draft: ExplorationDraft, validated: ValidatedQuery) -> ExploratoryResult:
        self.require_access()
        timezone = self.settings.business_timezone
        rows, reasons, size = [], [], 0
        try:
            async with self.metrics.admitted(), asyncio.timeout(self.settings.metric_timeout_seconds):
                async with self.db.transaction(analytical=True) as conn:
                    await conn.execute("SELECT set_config('TimeZone',%s,true)", (timezone,))
                    now = (await (await conn.execute("SELECT transaction_timestamp() AS now")).fetchone())["now"]
                    # A server cursor bounds client memory; no parameters, so % is literal SQL.
                    async with conn.cursor(name="exploration_rows", row_factory=tuple_row) as cursor:
                        await cursor.execute(validated.sql)
                        names = tuple(c.name for c in cursor.description)
                        types = [column_type(c.type_code) for c in cursor.description]
                        while (row := await cursor.fetchone()) is not None:
                            if len(rows) >= self.settings.exploratory_max_rows:
                                reasons.append("row_limit")
                                break
                            cells = [cell(value) for value in row]
                            row_size = wire_size(cells)
                            # Half the stored-result budget remains for SQL and provenance.
                            if size + row_size > self.settings.metric_max_bytes // 2:
                                reasons.append("byte_limit")
                                break
                            rows.append(cells)
                            size += row_size
        except (TimeoutError, psycopg.errors.QueryCanceled):
            raise ExplorationQueryError(TIMEOUT) from None
        except psycopg.Error as error:
            if (error.sqlstate or "")[:2] in REPAIRABLE:
                raise ExplorationQueryError(describe(error)) from None
            raise
        if names != validated.columns:
            raise ExplorationQueryError("Output columns differ from the declared columns: " + ", ".join(names) + ".")
        declared = {c.name: c for c in draft.columns}
        columns = [ExploratoryColumn(name=name, label=name.replace("_", " ").capitalize(), role=declared[name].role,
                                     unit=declared[name].unit if kind in {"integer", "decimal"} else "none", type=kind)
                   for name, kind in zip(names, types)]
        title = " ".join(TITLE.sub("", draft.title).split())[:120] or "Exploratory analysis"
        reference = sha256(json.dumps([POLICY_VERSION, self.catalogue_hash, validated.sql]).encode()).hexdigest()
        provenance = ExploratoryProvenance(
            query_reference=reference, policy_version=POLICY_VERSION, catalogue_version=self.catalogue.version,
            catalogue_sha256=self.catalogue_hash, title=title, sql=validated.sql, relations=list(validated.relations),
            columns=columns, business_timezone=timezone, freshness=Freshness(queried_at=now),
            limitations=list(LIMITATIONS), caveats=[c for c in (narrative(c, 240) for c in draft.caveats) if c],
            truncated=bool(reasons), truncation_reasons=reasons,
            returned_rows=len(rows), dataset_scope="limited" if reasons else "complete")
        return ExploratoryResult(id=uuid4(), run_id=run_id, permissions_version=principal.permissions_version,
                                 columns=list(names), rows=rows, provenance=provenance, created_at=now)
