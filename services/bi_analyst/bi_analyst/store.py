"""User-owned application state. Direct connections get explicit trusted scope."""
from contextlib import asynccontextmanager
from dataclasses import dataclass
from uuid import UUID, uuid4
from psycopg.types.json import Jsonb

from fastapi import HTTPException

from .database import Database


@dataclass(frozen=True)
class Principal:
    subject: UUID
    permissions_version: int


class Store:
    def __init__(self, database: Database):
        self.db = database

    async def authorize(self, subject: UUID) -> Principal:
        async with self.db.transaction(subject=subject) as conn:
            grant = await (await conn.execute("""SELECT enabled, company_wide, permissions_version
                FROM analyst_state.principals WHERE subject=%s""", (subject,))).fetchone()
            if not grant or not grant["enabled"] or not grant["company_wide"]:
                raise HTTPException(403, "access_denied")
            # One bounded counter per provisioned identity, shared across all replicas.
            rate = await (await conn.execute("""
                INSERT INTO analyst_state.rate_limits(owner_id,window_start,requests)
                VALUES (%s,date_trunc('minute',clock_timestamp()),1)
                ON CONFLICT(owner_id) DO UPDATE SET
                  window_start=EXCLUDED.window_start,
                  requests=CASE WHEN rate_limits.window_start=EXCLUDED.window_start
                                THEN least(rate_limits.requests+1,1000000) ELSE 1 END
                RETURNING requests
            """, (subject,))).fetchone()
        # Commit a rejected request's counter too; rollback would allow bypass.
        if rate["requests"] > self.db.settings.requests_per_minute:
            raise HTTPException(429, "rate_limited", headers={"Retry-After": "60"})
        return Principal(subject, grant["permissions_version"])

    @asynccontextmanager
    async def scoped(self, principal: Principal):
        async with self.db.transaction(subject=principal.subject) as conn:
            grant = await (await conn.execute("""SELECT enabled,company_wide,permissions_version
                FROM analyst_state.principals WHERE subject=%s""", (principal.subject,))).fetchone()
            if not grant or not grant["enabled"] or not grant["company_wide"]:
                raise HTTPException(403, "access_denied")
            if grant["permissions_version"] != principal.permissions_version:
                raise HTTPException(403, "permissions_changed")
            yield conn

    async def conversations(self, principal: Principal, *, title=None, limit=50, offset=0):
        async with self.scoped(principal) as conn:
            if title is not None:
                return await (await conn.execute("""INSERT INTO analyst_state.conversations(id,owner_id,title)
                    VALUES (%s,%s,%s) RETURNING id,title,created_at""",
                    (uuid4(), principal.subject, title))).fetchone()
            return await (await conn.execute("""SELECT id,title,created_at FROM analyst_state.conversations
                WHERE owner_id=%s ORDER BY created_at DESC,id DESC LIMIT %s OFFSET %s""",
                (principal.subject, limit, offset))).fetchall()

    async def conversation(self, principal: Principal, conversation_id: UUID):
        async with self.scoped(principal) as conn:
            row = await (await conn.execute("""SELECT id,title,created_at FROM analyst_state.conversations
                WHERE id=%s AND owner_id=%s""", (conversation_id, principal.subject))).fetchone()
        if row is None:
            raise HTTPException(404, "not_found")
        return row

    async def create_run(self, principal: Principal, conversation_id: UUID, question: str):
        async with self.scoped(principal) as conn:
            row = await (await conn.execute("""INSERT INTO analyst_state.runs
                (id,conversation_id,owner_id,question,permissions_version)
                SELECT %s,id,owner_id,%s,%s FROM analyst_state.conversations WHERE id=%s AND owner_id=%s
                RETURNING id,conversation_id,question,status,permissions_version,created_at""",
                (uuid4(),question,principal.permissions_version,conversation_id,principal.subject))).fetchone()
        if row is None:
            raise HTTPException(404, "not_found")
        return row

    async def runs(self, principal: Principal, conversation_id: UUID, limit: int, offset: int):
        async with self.scoped(principal) as conn:
            exists = await (await conn.execute("SELECT 1 FROM analyst_state.conversations WHERE id=%s AND owner_id=%s",
                                              (conversation_id,principal.subject))).fetchone()
            if not exists:
                raise HTTPException(404, "not_found")
            rows = await (await conn.execute("""SELECT id,conversation_id,question,status,permissions_version,created_at
                FROM analyst_state.runs WHERE conversation_id=%s AND owner_id=%s
                ORDER BY created_at DESC,id DESC LIMIT %s OFFSET %s""",
                (conversation_id,principal.subject,limit,offset))).fetchall()
        if any(row['permissions_version'] != principal.permissions_version for row in rows):
            raise HTTPException(403, "permissions_changed")
        return rows

    async def run(self, principal: Principal, run_id: UUID, *, cancel=False):
        async with self.scoped(principal) as conn:
            row = await (await conn.execute("""SELECT id,conversation_id,question,status,permissions_version,created_at
                FROM analyst_state.runs WHERE id=%s AND owner_id=%s FOR UPDATE""",
                (run_id,principal.subject))).fetchone()
            if row is None:
                raise HTTPException(404, "not_found")
            if row["permissions_version"] != principal.permissions_version:
                raise HTTPException(403, "permissions_changed")
            if cancel:
                if row["status"] not in {"registered", "cancelled"}:
                    raise HTTPException(409, "run_already_terminal")
                await conn.execute("UPDATE analyst_state.runs SET status='cancelled' WHERE id=%s AND owner_id=%s",
                                   (run_id,principal.subject))
                row["status"] = "cancelled"
        return row

    async def result(self, principal: Principal, result_id: UUID):
        async with self.scoped(principal) as conn:
            row = await (await conn.execute("""SELECT id,run_id,permissions_version,columns,rows,provenance,created_at
                FROM analyst_state.results WHERE id=%s AND owner_id=%s""", (result_id,principal.subject))).fetchone()
        if row is None:
            raise HTTPException(404, "not_found")
        if row["permissions_version"] != principal.permissions_version:
            raise HTTPException(403, "permissions_changed")
        return row

    async def finish_metric(self, principal: Principal, result):
        """Commit one result atomically; cancellation and concurrent completions win safely."""
        payload = result.model_dump(mode="json")
        async with self.scoped(principal) as conn:
            row = await (await conn.execute("""UPDATE analyst_state.runs SET status='completed'
                WHERE id=%s AND owner_id=%s AND permissions_version=%s AND status='registered'
                RETURNING id""", (result.run_id, principal.subject, principal.permissions_version))).fetchone()
            if row is None:
                raise HTTPException(409, "run_not_executable")
            await conn.execute("""INSERT INTO analyst_state.results
                (id,run_id,owner_id,permissions_version,columns,rows,provenance,created_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                (result.id, result.run_id, principal.subject, principal.permissions_version,
                 Jsonb(payload['columns']), Jsonb(payload['rows']), Jsonb(payload['provenance']), result.created_at))

    async def fail_metric(self, principal: Principal, run_id: UUID):
        async with self.scoped(principal) as conn:
            await conn.execute("""UPDATE analyst_state.runs SET status='failed'
                WHERE id=%s AND owner_id=%s AND permissions_version=%s AND status='registered'""",
                (run_id, principal.subject, principal.permissions_version))

    async def audit(self, subject: UUID, request_id: UUID, route: str, method: str, status: int):
        async with self.db.transaction(subject=subject) as conn:
            await conn.execute("""INSERT INTO analyst_state.audit_events(id,request_id,owner_id,route,method,status)
                VALUES (%s,%s,%s,%s,%s,%s)""", (uuid4(),request_id,subject,route,method,status))
