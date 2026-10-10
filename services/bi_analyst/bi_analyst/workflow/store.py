"""Durable admission, run fencing, budget accounting and public events."""
from hashlib import sha256
import json
from uuid import uuid4

from fastapi import HTTPException
from psycopg.types.json import Jsonb

from .contracts import WorkflowRun

ACTIVE = {"registered", "running", "awaiting_clarification"}
TERMINAL = {"completed", "failed", "cancelled", "interrupted"}


def fingerprint(value):
    return sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()


class WorkflowStore:
    def __init__(self, store):
        self.store, self.db = store, store.db

    async def _event(self, conn, actor, run_id, kind, payload):
        # All callers hold the job lock, so sequence allocation works across replicas.
        await conn.execute("""INSERT INTO analyst_state.workflow_events(run_id,owner_id,sequence,kind,payload)
            SELECT %s,%s,coalesce(max(sequence),0)+1,%s,%s FROM analyst_state.workflow_events WHERE run_id=%s""",
            (run_id, actor.subject, kind, Jsonb(payload), run_id))

    async def _status(self, conn, actor, run_id, status):
        await conn.execute("UPDATE analyst_state.workflow_jobs SET status=%s WHERE run_id=%s AND owner_id=%s",
                           (status, run_id, actor.subject))
        await conn.execute("UPDATE analyst_state.runs SET status=%s WHERE id=%s AND owner_id=%s",
                           (status, run_id, actor.subject))

    async def expire(self, actor):
        async with self.store.scoped(actor) as conn:
            rows = await (await conn.execute("""SELECT run_id FROM analyst_state.workflow_jobs
                WHERE owner_id=%s AND (status IN ('registered','running') AND deadline<clock_timestamp()
                  OR status IN ('registered','running','awaiting_clarification') AND permissions_version<>%s)
                ORDER BY run_id FOR UPDATE""", (actor.subject,actor.permissions_version))).fetchall()
            for row in rows:
                await self._status(conn, actor, row['run_id'], "interrupted")
                await conn.execute("UPDATE analyst_state.workflow_jobs SET error_code='execution_interrupted' WHERE run_id=%s", (row['run_id'],))
                await self._event(conn, actor, row['run_id'], "terminal", {"status": "interrupted", "error_code": "execution_interrupted"})

    async def get(self, actor, run_id):
        async with self.store.scoped(actor) as conn:
            return await self._get(conn, actor, run_id)

    async def _get(self, conn, actor, run_id):
        row = await (await conn.execute("""SELECT job.*, run.permissions_version AS run_permissions_version
            FROM analyst_state.workflow_jobs job JOIN analyst_state.runs run
              ON run.id=job.run_id AND run.owner_id=job.owner_id
            WHERE job.run_id=%s AND job.owner_id=%s""", (run_id, actor.subject))).fetchone()
        if not row:
            raise HTTPException(404, "not_found")
        if row.pop('run_permissions_version') != actor.permissions_version or row['permissions_version'] != actor.permissions_version:
            raise HTTPException(403, 'permissions_changed')
        return row

    @staticmethod
    def public(row):
        return WorkflowRun.model_validate({key: row[key] for key in WorkflowRun.model_fields})

    async def submit(self, actor, conversation_id, body, versions):
        await self.expire(actor)
        digest = fingerprint(body.model_dump(mode="json"))
        async with self.store.scoped(actor) as conn:
            conversation = await (await conn.execute("""SELECT id FROM analyst_state.conversations
                WHERE id=%s AND owner_id=%s""", (conversation_id, actor.subject))).fetchone()
            if not conversation:
                raise HTTPException(404, "not_found")
            # Serialize submissions without granting UPDATE on immutable conversations.
            await conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", ('workflow:'+str(conversation_id),))
            existing = await (await conn.execute("""SELECT * FROM analyst_state.workflow_jobs
                WHERE conversation_id=%s AND idempotency_key=%s""", (conversation_id, body.idempotency_key))).fetchone()
            if existing:
                if existing['input_hash'] != digest:
                    raise HTTPException(409, "idempotency_conflict")
                if existing['permissions_version'] != actor.permissions_version:
                    raise HTTPException(403, "permissions_changed")
                return existing, False
            if await (await conn.execute("""SELECT 1 FROM analyst_state.workflow_jobs WHERE conversation_id=%s
                AND status IN ('registered','running','awaiting_clarification')""", (conversation_id,))).fetchone():
                raise HTTPException(409, "thread_has_active_run")
            previous = None
            if body.follow_up_to:
                previous = await (await conn.execute("""SELECT plan,versions FROM analyst_state.workflow_jobs
                    WHERE run_id=%s AND owner_id=%s AND conversation_id=%s AND permissions_version=%s AND status='completed'""",
                    (body.follow_up_to, actor.subject, conversation_id, actor.permissions_version))).fetchone()
                if not previous or not previous['plan']:
                    raise HTTPException(409, "follow_up_unavailable")
                if previous['versions'] != versions:
                    raise HTTPException(409, "workflow_version_changed")
            run_id = uuid4()
            await conn.execute("""INSERT INTO analyst_state.runs(id,conversation_id,owner_id,question,permissions_version,status)
                VALUES (%s,%s,%s,%s,%s,'running')""", (run_id,conversation_id,actor.subject,body.question,actor.permissions_version))
            row = await (await conn.execute("""INSERT INTO analyst_state.workflow_jobs
                (run_id,conversation_id,owner_id,permissions_version,idempotency_key,input_hash,follow_up_to,
                 remaining_seconds,deadline,versions,plan)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,clock_timestamp()+%s*interval '1 second',%s,%s) RETURNING *""",
                (run_id,conversation_id,actor.subject,actor.permissions_version,body.idempotency_key,digest,body.follow_up_to,
                 self.db.settings.workflow_timeout_seconds,self.db.settings.workflow_timeout_seconds,Jsonb(versions),
                 Jsonb(previous['plan']) if previous else None))).fetchone()
            await self._event(conn, actor, run_id, "progress", {"stage": "registered"})
            return row, True

    async def claim(self, actor, run_id, versions, reply=None):
        async with self.store.scoped(actor) as conn:
            row = await (await conn.execute("""SELECT * FROM analyst_state.workflow_jobs
                WHERE run_id=%s AND owner_id=%s FOR UPDATE""", (run_id,actor.subject))).fetchone()
            if not row:
                raise HTTPException(404, "not_found")
            if row['permissions_version'] != actor.permissions_version:
                raise HTTPException(403, "permissions_changed")
            if row['versions'] != versions:
                raise HTTPException(409, "workflow_version_changed")
            if reply and str(reply.idempotency_key) in row['resume_receipts']:
                if row['resume_receipts'][str(reply.idempotency_key)] != fingerprint(reply.model_dump(mode="json")):
                    raise HTTPException(409, "idempotency_conflict")
                return None
            if row['status'] != ('awaiting_clarification' if reply else 'registered'):
                raise HTTPException(409, "run_not_resumable" if reply else "run_not_executable")
            if reply and str(reply.clarification_id) != row['clarification']['id']:
                raise HTTPException(409, "clarification_changed")
            token = uuid4()
            receipts = dict(row['resume_receipts'])
            if reply:
                if len(receipts) >= 3:
                    raise HTTPException(409,'clarification_limit')
                receipts[str(reply.idempotency_key)] = fingerprint(reply.model_dump(mode='json'))
            await self._status(conn, actor, run_id, "running")
            await conn.execute("""UPDATE analyst_state.workflow_jobs SET execution_token=%s,
                segment_started_at=clock_timestamp(), deadline=clock_timestamp()+remaining_seconds*interval '1 second',
                resume_key=%s,resume_hash=%s,resume_receipts=%s WHERE run_id=%s""",
                (token, reply.idempotency_key if reply else None,
                 fingerprint(reply.model_dump(mode="json")) if reply else None,Jsonb(receipts),run_id))
            return token

    async def guard(self, actor, run_id, token):
        row = await self.get(actor, run_id)
        if row['status'] != 'running' or row['execution_token'] != token:
            raise HTTPException(409, "run_not_executable")
        return row

    async def progress(self, actor, run_id, token, stage):
        async with self.store.scoped(actor) as conn:
            await self._locked(conn, actor, run_id, token)
            await self._event(conn, actor, run_id, "progress", {"stage": stage})

    async def _locked(self, conn, actor, run_id, token):
        row = await (await conn.execute("""SELECT * FROM analyst_state.workflow_jobs
            WHERE run_id=%s AND owner_id=%s FOR UPDATE""", (run_id,actor.subject))).fetchone()
        if not row or row['status'] != 'running' or row['execution_token'] != token or row['permissions_version'] != actor.permissions_version:
            raise HTTPException(409, "run_not_executable")
        return row

    async def budget(self, actor, run_id, token, kind):
        column, limit = ('model_calls', self.db.settings.workflow_max_model_calls) if kind == 'model' else ('tool_calls', 24)
        async with self.store.scoped(actor) as conn:
            row = await self._locked(conn, actor, run_id, token)
            if row[column] >= limit:
                raise HTTPException(429, "run_budget_exceeded")
            await conn.execute(f"UPDATE analyst_state.workflow_jobs SET {column}={column}+1 WHERE run_id=%s", (run_id,))

    async def usage(self, actor, run_id, token, usage):
        async with self.store.scoped(actor) as conn:
            row = await self._locked(conn, actor, run_id, token)
            total = {key: row['usage'].get(key,0)+usage.get(key,0) for key in
                     ('promptTokenCount','candidatesTokenCount','thoughtsTokenCount')}
            await conn.execute("UPDATE analyst_state.workflow_jobs SET usage=%s WHERE run_id=%s", (Jsonb(total),run_id))

    async def save_result(self, actor, run_id, token, result):
        payload = result.model_dump(mode='json')
        async with self.store.scoped(actor) as conn:
            await self._locked(conn, actor, run_id, token)
            await conn.execute("""INSERT INTO analyst_state.results
                (id,run_id,owner_id,permissions_version,columns,rows,provenance,created_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""", (result.id,run_id,actor.subject,actor.permissions_version,
                Jsonb(payload['columns']),Jsonb(payload['rows']),Jsonb(payload['provenance']),result.created_at))

    async def finish(self, actor, run_id, token, status, *, plan=None, clarification=None, answer=None, error=None):
        async with self.store.scoped(actor) as conn:
            await self._locked(conn, actor, run_id, token)
            await self._status(conn, actor, run_id, status)
            await conn.execute("""UPDATE analyst_state.workflow_jobs SET plan=coalesce(%s,plan),clarification=%s,
                answer=%s,error_code=%s,remaining_seconds=greatest(0,remaining_seconds-
                  extract(epoch FROM clock_timestamp()-segment_started_at)),deadline=NULL WHERE run_id=%s""",
                (Jsonb(plan) if plan else None,Jsonb(clarification) if clarification else None,
                 Jsonb(answer) if answer else None,error,run_id))
            if clarification:
                await self._event(conn,actor,run_id,'clarification',clarification)
            if answer:
                await self._event(conn,actor,run_id,'answer',answer)
            if status in TERMINAL:
                await self._event(conn,actor,run_id,'terminal',{'status':status,'error_code':error})

    async def cancel(self, actor, run_id):
        await self.store.run(actor, run_id)
        async with self.store.scoped(actor) as conn:
            row = await (await conn.execute("SELECT * FROM analyst_state.workflow_jobs WHERE run_id=%s AND owner_id=%s FOR UPDATE",
                                           (run_id,actor.subject))).fetchone()
            if not row:
                return False
            if row['status'] == 'cancelled':
                return True
            if row['status'] in TERMINAL:
                raise HTTPException(409,'run_already_terminal')
            await self._status(conn,actor,run_id,'cancelled')
            await self._event(conn,actor,run_id,'terminal',{'status':'cancelled','error_code':None})
            return True

    async def events(self, actor, run_id, after):
        _, rows = await self.snapshot(actor,run_id,after)
        return rows

    async def snapshot(self, actor, run_id, after):
        # Stream status and events share a short, freshly authorised transaction.
        async with self.store.scoped(actor) as conn:
            state = await self._get(conn,actor,run_id)
            rows = await (await conn.execute("""SELECT sequence,kind,payload FROM analyst_state.workflow_events
                WHERE run_id=%s AND owner_id=%s AND sequence>%s ORDER BY sequence LIMIT 128""",
                (run_id,actor.subject,after))).fetchall()
            return state, rows
