"""Model and public contracts. No model-controlled SQL, expressions or chart URLs."""
from datetime import datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID

from pydantic import Field

from ..contracts import Contract
from ..metrics.contracts import Comparison, Dimension, Identifier, MetricFilter, Ordering, PeriodName


class Submission(Contract):
    question: str = Field(min_length=1, max_length=8000)
    idempotency_key: UUID
    follow_up_to: UUID | None = None


class Reply(Contract):
    clarification_id: UUID
    idempotency_key: UUID
    answer: str = Field(min_length=1, max_length=2000)


class Interpretation(Contract):
    families: list[Literal["enquiry", "order", "invoice", "conversion", "gestation"]] = Field(max_length=5)
    supported: bool = Field(description="True when any catalogue metric family is identifiable, even when variant or period needs clarification. Planning checks exact support.")
    asks_for_cause: bool = False


class PlanPatch(Contract):
    """Null means inherit; explicit clear flags remove prior filters/comparison."""
    metric_id: Identifier | None = None
    period: PeriodName | None = None
    grain: Literal["total", "month"] | None = None
    dimensions: list[Dimension] | None = Field(default=None, max_length=2)
    filters: list[MetricFilter] | None = Field(default=None, max_length=2)
    comparison: Comparison | None = None
    clear_comparison: bool = False
    ordering: list[Ordering] | None = Field(default=None, max_length=3)
    limit: int | None = Field(default=None, ge=1, le=1000)
    include_share: bool | None = None


class PlanDraft(Contract):
    action: Literal["plan", "clarify", "unsupported"]
    patch: PlanPatch = Field(default_factory=PlanPatch)
    reason: Literal["metric", "period", "scope", "unsupported"] = "scope"
    candidate_metrics: list[Identifier] = Field(default_factory=list, max_length=16)


class Clarification(Contract):
    id: UUID
    reason: Literal["metric", "period", "scope", "entity"]
    question: str
    options: list[str] = Field(default_factory=list, max_length=20)
    dimension: Dimension | None = None
    value: str | None = None
    comparison: bool = False


class ResultReference(Contract):
    result_id: UUID
    query_reference: str
    catalogue_sha256: str


class EvidenceClaim(Contract):
    id: str
    result_id: UUID
    location: str
    metric_id: str
    unit: str
    period: str
    value: Decimal | None
    denominator: Decimal | None = None
    label: str
    kind: Literal["total", "contributor", "absolute_change", "percentage_change", "percentage_point_change"]


class ChartIntent(Contract):
    kind: Literal["table", "bar", "line", "number"] = "table"
    x: Literal["month", "category", "type"] | None = None
    y: Literal["value"] = "value"


class Presentation(Contract):
    claim_ids: list[str] = Field(min_length=1, max_length=12)
    chart: ChartIntent = Field(default_factory=ChartIntent)


class GroundedAnswer(Contract):
    result: ResultReference
    claims: list[EvidenceClaim]
    text: str
    chart: ChartIntent
    notices: list[str]
    scope_changes: dict


class WorkflowRun(Contract):
    run_id: UUID
    conversation_id: UUID
    status: Literal["registered", "running", "awaiting_clarification", "completed", "failed", "cancelled", "interrupted"]
    clarification: Clarification | None = None
    answer: GroundedAnswer | None = None
    error_code: str | None = None
    created_at: datetime
    model_calls: int
    tool_calls: int
    versions: dict


class PublicEvent(Contract):
    sequence: int
    kind: Literal["progress", "clarification", "answer", "terminal"]
    payload: dict
