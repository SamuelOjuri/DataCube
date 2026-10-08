"""Small Phase 3 storage contracts; metric plans and graph state come later."""
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class ConversationInput(Contract):
    title: str = Field(min_length=1, max_length=160)


class RunInput(Contract):
    question: str = Field(min_length=1, max_length=8000)


class Conversation(Contract):
    id: UUID
    title: str
    created_at: datetime


class Run(Contract):
    id: UUID
    conversation_id: UUID
    question: str
    status: Literal["registered", "cancelled", "completed", "failed"]
    permissions_version: int
    created_at: datetime


class Result(Contract):
    id: UUID
    run_id: UUID
    permissions_version: int
    columns: list[str]
    rows: list[list[Any]]
    provenance: dict[str, Any]
    created_at: datetime


def csv_cell(value: Any) -> Any:
    """Prevent spreadsheet formula interpretation of untrusted string values."""
    if isinstance(value, str) and (value.lstrip().startswith(("=", "+", "-", "@"))
                                   or value.startswith(("\t", "\r", "\n"))):
        return "'" + value
    return "" if value is None else value
