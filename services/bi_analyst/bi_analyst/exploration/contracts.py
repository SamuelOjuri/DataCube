"""The model proposes SQL and column roles; every value comes from PostgreSQL."""
from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import Field

from ..contracts import Contract
from ..metrics.contracts import Cell, Freshness

Unit = Literal["source_currency", "count", "ratio", "percent", "days", "none"]


class DraftColumn(Contract):
    # Names are checked against the validated SQL, so a mismatch is repairable.
    name: str = Field(min_length=1, max_length=63)
    role: Literal["dimension", "measure"]
    unit: Unit


class ExplorationDraft(Contract):
    action: Literal["query", "decline"]
    reason: Literal["none", "not_analytical", "data_unavailable", "forecast", "unsafe"] = "none"
    title: str = Field(default="", max_length=160)
    sql: str = Field(default="", max_length=8000)
    columns: list[DraftColumn] = Field(default_factory=list, max_length=20)
    asks_for_cause: bool = False


class ExploratoryColumn(Contract):
    name: str
    label: str
    role: Literal["dimension", "measure"]
    unit: Unit
    type: Literal["text", "date", "integer", "decimal"]


class ExploratoryProvenance(Contract):
    kind: Literal["exploratory"] = "exploratory"
    contract_version: Literal["1.0.0"] = "1.0.0"
    query_reference: str
    policy_version: str
    catalogue_version: str
    catalogue_sha256: str
    title: str
    sql: str
    relations: list[str]
    columns: list[ExploratoryColumn]
    business_timezone: str
    authorization_scope: Literal["company_wide"] = "company_wide"
    freshness: Freshness
    limitations: list[str]
    certification_basis: Literal["exploratory_uncertified"] = "exploratory_uncertified"
    truncated: bool
    truncation_reasons: list[Literal["row_limit", "byte_limit"]]
    returned_rows: int
    dataset_scope: Literal["complete", "limited"]


class ExploratoryResult(Contract):
    id: UUID
    run_id: UUID
    permissions_version: int
    columns: list[str]
    rows: list[list[Cell]]
    provenance: ExploratoryProvenance
    created_at: datetime
