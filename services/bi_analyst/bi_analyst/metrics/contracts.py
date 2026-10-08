"""Public Phase 4 contracts. Decimal values are JSON strings, never floats."""
from datetime import date, datetime
from decimal import Decimal
from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field, model_validator

from ..contracts import Contract
from ..semantic.periods import ResolvedPeriod

Identifier = Annotated[str, Field(pattern=r"^[a-z][a-z0-9_]*$", max_length=63)]
PeriodName = Literal["all_stored", "last_month", "previous_month", "completed_12_months",
                     "five_year_cohort", "two_year_cohort"]
Dimension = Literal["category", "type"]
Cell = str | int | Decimal | date | None


class MetricFilter(Contract):
    dimension: Dimension
    operator: Literal["in", "is_null"] = "in"
    values: list[Annotated[str, Field(min_length=1, max_length=256)]] = Field(default_factory=list, max_length=50)

    @model_validator(mode="after")
    def valid_values(self):
        if (self.operator == "in") != bool(self.values):
            raise ValueError("in requires values; is_null does not accept values")
        return self


class Comparison(Contract):
    period: PeriodName
    # None inherits current filters; [] explicitly compares with the unfiltered scope.
    filters: list[MetricFilter] | None = Field(default=None, max_length=2)


class Ordering(Contract):
    column: Identifier = "value"
    direction: Literal["asc", "desc"] = "desc"


class MetricRequest(Contract):
    metric_id: Identifier
    metric_version: str = Field(pattern=r"^[1-9][0-9]*\.[0-9]+\.[0-9]+$")
    population: Literal["reportable", "hidden_inventory"]
    period: PeriodName
    grain: Literal["total", "month"] = "total"
    dimensions: list[Dimension] = Field(default_factory=list, max_length=2)
    filters: list[MetricFilter] = Field(default_factory=list, max_length=2)
    comparison: Comparison | None = None
    ordering: list[Ordering] = Field(default_factory=list, max_length=3)
    limit: int = Field(default=100, ge=1, le=1000, strict=True)
    include_total: bool = True
    include_share: bool = False


class Column(Contract):
    name: str
    type: Literal["text", "date", "integer", "decimal"]
    unit: Literal["source_currency", "ratio", "days", "count"] | None = None
    precision: int | None = None


class Aggregate(Contract):
    value: Decimal | None
    source_rows: int
    known_values: int
    numerator: Decimal | None = None
    denominator: Decimal | None = None


class Change(Contract):
    current: Aggregate
    baseline: Aggregate
    absolute_change: Decimal | None
    percentage_change: Decimal | None
    percentage_point_change: Decimal | None
    zero_denominator: bool
    scope: Literal["complete_filtered_totals"] = "complete_filtered_totals"


class Freshness(Contract):
    queried_at: datetime
    status: Literal["unknown"] = "unknown"
    ingestion_at: datetime | None = None
    rollup_at: datetime | None = None
    refreshed_at: datetime | None = None
    data_version: str | None = None
    historical_snapshot_date: date | None = None
    limitation: str = "Successful ingestion, rollup and refresh evidence is not published by the query gateways."


class MetricProvenance(Contract):
    contract_version: Literal["1.0.0"] = "1.0.0"
    query_reference: str
    compiler_version: str
    catalogue_version: str
    catalogue_sha256: str
    metric_id: str
    metric_version: str
    metric_label: str
    unit: str
    source_population: str
    source_relation: str
    source_grain: str
    authorization_scope: Literal["company_wide"] = "company_wide"
    request: MetricRequest
    resolved_period: ResolvedPeriod | None
    comparison_period: ResolvedPeriod | None = None
    business_timezone: str
    columns: list[Column]
    total: Aggregate | None
    total_scope: Literal["complete_filtered_population", "not_requested"]
    comparison: Change | None = None
    freshness: Freshness
    coverage: dict[str, int]
    coverage_scope: Literal["gateway_population_unfiltered"] = "gateway_population_unfiltered"
    limitations: list[str]
    evaluation_only: bool
    certification_basis: Literal["owner_accepted", "local_evaluation"]
    truncated: bool
    truncation_reasons: list[Literal["row_limit", "byte_limit"]]
    returned_rows: int
    matched_groups: int
    dataset_scope: Literal["complete", "limited"]


class MetricResult(Contract):
    id: UUID
    run_id: UUID
    permissions_version: int
    columns: list[str]
    rows: list[list[Cell]]
    provenance: MetricProvenance
    created_at: datetime


class EntityRequest(Contract):
    metric_id: Identifier
    metric_version: str = Field(pattern=r"^[1-9][0-9]*\.[0-9]+\.[0-9]+$")
    population: Literal["reportable", "hidden_inventory"]
    dimension: Dimension
    query: str = Field(min_length=1, max_length=128)


class EntityResolution(Contract):
    dimension: Dimension
    query: str
    status: Literal["resolved", "clarification_required", "not_found"]
    candidates: list[str]
    truncated: bool
