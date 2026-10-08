"""Trusted identity seam. Authentication is deliberately deferred by the owner.

No header, query parameter, shared default user or JWT claim is accepted here.
Tests inject a server-side identity dependency; deployed data routes fail closed.
"""
from dataclasses import dataclass
from uuid import UUID

from fastapi import HTTPException


@dataclass(frozen=True)
class Identity:
    subject: UUID


async def current_identity() -> Identity:
    raise HTTPException(503, detail="identity_not_configured")
