"""Provider-independent identity from a live, server-issued opaque session."""
from dataclasses import dataclass
from datetime import datetime
import re
from uuid import UUID

from fastapi import HTTPException, Request


@dataclass(frozen=True)
class Identity:
    subject: UUID
    permissions_version: int | None = None
    expires_at: datetime | None = None


def bearer(request: Request) -> str:
    values = request.headers.getlist("authorization")
    match = re.fullmatch(r"Bearer ([A-Za-z0-9_-]{43})", values[0], re.IGNORECASE) if len(values) == 1 else None
    if not match:
        raise HTTPException(401, "authentication_required", headers={"WWW-Authenticate": "Bearer"})
    return match[1]


async def current_identity(request: Request) -> Identity:
    if request.app.state.settings.auth_provider == "disabled":
        raise HTTPException(503, detail="identity_not_configured")
    row = await request.app.state.auth_store.session(bearer(request))
    request.state.subject = row["owner_id"]
    return Identity(row["owner_id"], row["permissions_version"], row["expires_at"])
