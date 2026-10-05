"""Durable jobs and scheduler ownership. Requires a direct/session-mode PostgreSQL DSN.

Session advisory locks are held throughout external work. An expired lease alone
cannot launch a second copy while the first connection still owns the lock.
Jobs are at-least-once: handlers must use idempotent writes and durable dedupe.
"""
from __future__ import annotations

from contextvars import ContextVar
from datetime import datetime, timezone
import hashlib
import json
import os
from uuid import uuid4, uuid5, NAMESPACE_URL

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

current_job: ContextVar[str | None] = ContextVar('datacube_job', default=None)
MAX_ATTEMPTS = 5


def connect():
    dsn = os.getenv('SUPABASE_DB_URL')
    if not dsn:
        raise RuntimeError('Worker operations require SUPABASE_DB_URL')
    return psycopg.connect(dsn, autocommit=True, row_factory=dict_row, connect_timeout=5,
                          options='-c statement_timeout=10000 -c lock_timeout=2000',
                          application_name='datacube-workers')


def lock_key(value):
    return int.from_bytes(hashlib.sha256(value.encode()).digest()[:8], 'big', signed=True)


def enqueue(connection, name, project_id, payload=None, *, queue='general', key=None):
    key = key or str(uuid4())
    job_id = uuid5(NAMESPACE_URL, 'datacube:' + key)
    connection.execute('''INSERT INTO public.job_queue
        (id,job_type,project_id,status,attempts,payload,queue_name,dedupe_key)
        VALUES (%s,%s,%s,'queued',0,%s,%s,%s) ON CONFLICT(dedupe_key) DO NOTHING''',
        (job_id, name, str(project_id), Jsonb(payload or {}), queue, key))
    return str(job_id)


def claim(connection, queue, owner):
    with connection.transaction():
        candidates = connection.execute('''SELECT j.* FROM public.job_queue j
            WHERE j.queue_name=%s AND ((j.status IN ('queued','retry') AND j.available_at<=now())
                OR (j.status='running' AND j.lease_until<now()))
            AND NOT EXISTS (SELECT 1 FROM public.job_queue older
                WHERE older.queue_name=j.queue_name AND older.project_id=j.project_id
                AND older.status IN ('queued','retry','running')
                AND (older.created_at,older.id)<(j.created_at,j.id))
            ORDER BY j.available_at,j.created_at FOR UPDATE OF j SKIP LOCKED LIMIT 20''', (queue,)).fetchall()
        for job in candidates:
            # Serialise writes for each target within a queue, including retries.
            key = lock_key('queue:' + queue + ':' + str(job['project_id']))
            if not connection.execute('SELECT pg_try_advisory_lock(%s) AS acquired', (key,)).fetchone()['acquired']:
                continue
            if (job['attempts'] or 0) >= MAX_ATTEMPTS:
                connection.execute("UPDATE public.job_queue SET status='failed',detail='AttemptsExhausted',"
                                   "lease_until=NULL,lease_token=NULL,updated_at=now() WHERE id=%s", (job['id'],))
                connection.execute('SELECT pg_advisory_unlock(%s)', (key,))
                continue
            row = connection.execute('''UPDATE public.job_queue SET status='running',
                attempts=coalesce(attempts,0)+1,lease_token=gen_random_uuid(),
                lease_until=now()+interval '30 minutes',owner_id=%s,updated_at=now()
                WHERE id=%s RETURNING *''', (owner, job['id'])).fetchone()
            row['_lock_key'] = key
            return row
    return None


def finish(connection, job, *, error=None, followups=()):
    status = 'completed' if error is None else ('failed' if job['attempts'] >= MAX_ATTEMPTS else 'retry')
    with connection.transaction():
        row = connection.execute('''UPDATE public.job_queue SET status=%s,detail=%s,
            lease_until=NULL,lease_token=NULL,updated_at=now(),
            available_at=now()+(%s * interval '1 second')
            WHERE id=%s AND status='running' AND lease_token=%s RETURNING id''',
            (status, type(error).__name__ if error else None, min(300, 2 ** job['attempts']),
             job['id'], job['lease_token'])).fetchone()
        if not row:
            raise RuntimeError('Queue lease replaced; stale worker cannot acknowledge')
        for followup in followups:
            enqueue(connection, **followup)
        log_id = (job['payload'] or {}).get('webhook_log_id')
        if job['queue_name'] == 'webhook' and log_id:
            connection.execute('''UPDATE public.webhook_events SET status=%s,
                processed_at=CASE WHEN %s='completed' THEN now() ELSE NULL END,
                retry_count=%s,error_message=%s,
                processing_time_ms=extract(epoch FROM (clock_timestamp()-%s::timestamptz))*1000 WHERE id=%s''',
                ('processed' if status == 'completed' else status, status, job['attempts'] - 1,
                 type(error).__name__ if error else None, job['updated_at'], log_id))
    return status


def accept_webhook(event_type, board_id, item_id, payload, client_ip=None):
    """Atomically persist receipt and job before acknowledging HTTP delivery."""
    event = payload.get('event') or {}
    token = event.get('triggerUuid') or event.get('originalTriggerUuid') or event.get('id')
    identity = token if token is not None else json.dumps(event, sort_keys=True, separators=(',', ':'))
    digest = hashlib.sha256(f'{board_id}:{item_id}:{event_type}:{identity}'.encode()).hexdigest()
    key = 'webhook:' + digest
    log_id = uuid5(NAMESPACE_URL, key)
    with connect() as connection, connection.transaction():
        # Use the scoped durable identity; raw Monday IDs can collide between boards.
        connection.execute('''INSERT INTO public.webhook_events
            (id,event_id,board_id,item_id,event_type,webhook_payload,status,client_ip)
            VALUES (%s,%s,%s,%s,%s,%s,'pending',%s) ON CONFLICT DO NOTHING''',
            (log_id, key, board_id, item_id, event_type, Jsonb(payload), client_ip))
        return enqueue(connection, 'webhook', item_id,
                       dict(event_type=event_type, board_id=board_id, item_id=item_id,
                            data=payload, webhook_log_id=str(log_id)), queue='webhook', key=key)


def run_scheduled(job_id, scheduled_for, owner, handler, *, next_due=None):
    """One owner per job, one attempt per scheduled occurrence across replicas."""
    with connect() as connection:
        key = lock_key('schedule:' + job_id)
        if not connection.execute('SELECT pg_try_advisory_lock(%s) AS acquired', (key,)).fetchone()['acquired']:
            return {'outcome': 'owned_elsewhere'}
        # A connection can disappear while its external call still runs. Never
        # start a later occurrence over an unresolved execution from that session.
        previous = connection.execute("SELECT id FROM public.worker_job_runs WHERE job_id=%s "
                                      "AND outcome='running' LIMIT 1", (job_id,)).fetchone()
        if previous:
            return {'outcome': 'previous_run_unresolved'}
        run_id = uuid4()
        with connection.transaction():
            row = connection.execute('''INSERT INTO public.worker_job_runs
                (id,job_id,scheduled_for,instance_id,outcome) VALUES (%s,%s,%s,%s,'running')
                ON CONFLICT(job_id,scheduled_for) DO NOTHING RETURNING id''',
                (run_id, job_id, scheduled_for, owner)).fetchone()
            if not row:
                return {'outcome': 'already_recorded'}
            if next_due is not None:
                connection.execute('''INSERT INTO public.worker_schedules(job_id,next_due_at) VALUES (%s,%s)
                    ON CONFLICT(job_id) DO UPDATE SET next_due_at=greatest(worker_schedules.next_due_at,excluded.next_due_at),updated_at=now()''',
                    (job_id, next_due))
        try:
            result = handler() or {}
            counts = {k: v for k, v in result.items() if isinstance(v, (int, float))}
            outcome = result.get('outcome') or ('partial' if counts.get('errors', 0) or counts.get('missing', 0) else 'succeeded')
        except Exception as exc:
            connection.execute("UPDATE public.worker_job_runs SET outcome='failed',finished_at=now(),"
                               'error_type=%s WHERE id=%s', (type(exc).__name__, run_id))
            raise
        connection.execute('''UPDATE public.worker_job_runs SET outcome=%s,counts=%s,finished_at=now()
            WHERE id=%s''', (outcome, Jsonb(counts), run_id))
        return {'outcome': outcome, **counts}


def utcnow():
    return datetime.now(timezone.utc)
