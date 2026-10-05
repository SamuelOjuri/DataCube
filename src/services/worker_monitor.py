"""Worker observation and durable summaries. Health reads never perform I/O."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import time
from uuid import uuid4
from psycopg.types.json import Jsonb
from . import worker_store as store

LOG = logging.getLogger(__name__)


class WorkerMonitor:
    def __init__(self, clock=time.monotonic):
        self.clock = clock
        self.instance_id = str(uuid4())
        self.service = 'uninitialised'
        self.started_at = store.utcnow()
        self.workers = {}
        self.task = None
        self.stopping = False
        self.last_persisted = None
        self.database_error = None
        self.lock = threading.RLock()
        self.probes = {}

    def register(self, name, *, enabled=True, budget=1800):
        budget = int(os.getenv('WORKER_' + name.upper().replace(':', '_') + '_DEADLINE_SECONDS', str(budget)))
        if budget < 30:
            raise ValueError('Worker deadline must be at least 30 seconds')
        with self.lock:
            self.workers[name] = dict(state='starting' if enabled else 'disabled', enabled=enabled,
                budget=budget, progress=self.clock(), started=None, last_success=None,
                current_job=None, error_type=None, task=None)

    def bind(self, name, task):
        with self.lock:
            self.workers[name]['task'] = task

    def update(self, name, state, *, job=None, error=None, success=False):
        with self.lock:
            if name not in self.workers:
                return
            worker = self.workers[name]
            now = self.clock()
            if state == 'busy' and (worker['state'] != 'busy' or worker['current_job'] != job):
                worker['started'] = now
            worker.update(state=state, progress=now, current_job=job,
                          error_type=type(error).__name__ if error else None)
            if success:
                worker['last_success'] = store.utcnow().isoformat()

    def snapshot(self):
        now = self.clock()
        with self.lock:
            workers = {}
            for name, worker in self.workers.items():
                state, reason, task = worker['state'], None, worker['task']
                if worker['enabled']:
                    if task is not None and task.done() and not self.stopping:
                        reason = 'task_stopped'
                    elif state == 'busy' and worker['started'] is not None and now - worker['started'] > worker['budget']:
                        reason = 'work_overdue'
                    elif state != 'busy' and now - worker['progress'] > 75:
                        reason = 'poll_stale'
                    elif state in {'starting', 'stopped'}:
                        reason = state
                workers[name] = dict(state=state, healthy=reason is None and state != 'degraded',
                    failure=reason, error_type=worker['error_type'], last_success=worker['last_success'],
                    progress_age_seconds=round(now - worker['progress'], 1), current_job=worker['current_job'],
                    work_age_seconds=round(now - worker['started'], 1) if state == 'busy' and worker['started'] is not None else None)
        live = not self.stopping and bool(workers) and all(w['failure'] is None for w in workers.values())
        database_ok = self.last_persisted is not None and now - self.last_persisted < 75 and not self.database_error
        ready = live and database_ok and all(w['healthy'] for w in workers.values())
        return dict(instance_id=self.instance_id, service=self.service, live=live, ready=ready,
                    database='connected' if database_ok else 'unavailable', workers=workers)

    def start(self, service):
        self.service, self.stopping = service, False
        if self.task is None or self.task.done():
            # Generate after process startup, including prefork/preloaded servers.
            self.instance_id = str(uuid4())
            self.started_at = store.utcnow()
            self.register('monitor', budget=60)
            self.task = asyncio.create_task(self.run(), name='worker-monitor')
            self.bind('monitor', self.task)

    def persist(self, *, stopped=False):
        with store.connect() as connection:
            connection.execute('''INSERT INTO public.worker_heartbeats
                (instance_id,service,revision,started_at,heartbeat_at,stopped_at,status)
                VALUES (%s,%s,%s,%s,now(),CASE WHEN %s THEN now() ELSE NULL END,%s)
                ON CONFLICT(instance_id) DO UPDATE SET heartbeat_at=now(),
                stopped_at=excluded.stopped_at,status=excluded.status''',
                (self.instance_id, self.service, os.getenv('RENDER_GIT_COMMIT'), self.started_at, stopped, Jsonb(self.snapshot())))
        self.last_persisted, self.database_error = self.clock(), None

    async def run(self):
        while not self.stopping:
            try:
                for probe in tuple(self.probes.values()):
                    probe()
                self.update('monitor', 'idle')
                self.persist_task = asyncio.create_task(asyncio.to_thread(self.persist))
                await asyncio.shield(self.persist_task)
            except Exception as exc:
                self.database_error = type(exc).__name__
                self.update('monitor', 'degraded', error=exc)
                LOG.error('worker_monitor_persist_failed error_type=%s', type(exc).__name__)
            await asyncio.sleep(15)

    async def stop(self):
        self.stopping = True
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        pending = getattr(self, 'persist_task', None)
        if pending is not None and not pending.done():
            await asyncio.wait([pending], timeout=15)
            if not pending.done():
                # Do not race a late heartbeat against a graceful-stop record.
                return
        try:
            await asyncio.to_thread(self.persist, stopped=True)
        except Exception:
            LOG.warning('Could not record graceful worker shutdown')


monitor = WorkerMonitor()


def operational_status(connection, *, expected_services=(), queue_age=900):
    """Read-only audit; exclude payloads, DSNs and raw errors from reports."""
    now = store.utcnow()
    instances = connection.execute('''SELECT instance_id,service,heartbeat_at,stopped_at,status
        FROM public.worker_heartbeats WHERE stopped_at IS NULL ORDER BY heartbeat_at DESC''').fetchall()
    alerts = []
    for row in instances:
        if (now - row['heartbeat_at']).total_seconds() > 90:
            alerts.append(dict(key='heartbeat:' + row['instance_id'], issue='heartbeat_missing', service=row['service']))
        else:
            for name, worker in row['status'].get('workers', {}).items():
                if not worker.get('healthy', False):
                    alerts.append(dict(key=f"worker:{row['instance_id']}:{name}", issue=worker.get('failure') or 'worker_degraded', worker=name))
    active_services = {r['service'] for r in instances if (now - r['heartbeat_at']).total_seconds() <= 90}
    for service in expected_services:
        if service not in active_services:
            alerts.append(dict(key='service:' + service, issue='service_missing', service=service))
    queues = connection.execute('''SELECT coalesce(queue_name,'legacy') AS queue,status,count(*) AS count,
        max(extract(epoch FROM now()-created_at)) FILTER (WHERE status IN ('queued','retry') AND available_at<=now()) AS waiting_seconds,
        count(*) FILTER (WHERE status='running' AND lease_until<now()) AS expired_leases
        FROM public.job_queue WHERE status<>'completed' GROUP BY queue_name,status''').fetchall()
    for row in queues:
        if row['status'] == 'failed' or row['queue'] == 'legacy' or row['expired_leases'] or (row['waiting_seconds'] or 0) > queue_age:
            alerts.append(dict(key=f"queue:{row['queue']}:{row['status']}", issue='queue_needs_attention', **row))
    if connection.execute("SELECT to_regclass('public.monday_lifecycle_events') AS relation").fetchone()['relation']:
        lifecycle = connection.execute('''SELECT status,count(*) AS count,
            max(extract(epoch FROM now()-received_at)) FILTER (WHERE status IN ('pending','retry') AND next_attempt_at<=now()) AS waiting_seconds,
            count(*) FILTER (WHERE status='processing' AND lease_until<now()) AS expired_leases
            FROM public.monday_lifecycle_events WHERE status<>'processed' GROUP BY status''').fetchall()
        for row in lifecycle:
            if row['status'] == 'review' or row['expired_leases'] or (row['waiting_seconds'] or 0) > queue_age:
                alerts.append(dict(key='lifecycle:' + row['status'], issue='lifecycle_needs_attention', **row))
        queues.extend(dict(queue='lifecycle', **row) for row in lifecycle)
    schedules = connection.execute('''SELECT s.job_id,s.next_due_at,r.outcome,r.started_at,r.finished_at
        FROM public.worker_schedules s LEFT JOIN LATERAL
        (SELECT * FROM public.worker_job_runs WHERE job_id=s.job_id ORDER BY scheduled_for DESC LIMIT 1) r ON true''').fetchall()
    for row in schedules:
        overdue = (now - row['next_due_at']).total_seconds() > 900
        hung = row['outcome'] == 'running' and (now - row['started_at']).total_seconds() > 7200
        if overdue or hung or row['outcome'] in {'failed', 'partial', 'missed', 'max_instances', 'skipped'}:
            alerts.append(dict(key='schedule:' + row['job_id'], issue='schedule_needs_attention', job_id=row['job_id'], outcome=row['outcome'], overdue=overdue))
    pending = connection.execute('''SELECT count(*) AS count FROM public.webhook_events w
        WHERE w.status IN ('pending','retry') AND w.received_at<now()-interval '15 minutes'
        AND NOT EXISTS (SELECT 1 FROM public.job_queue j WHERE j.queue_name='webhook'
                       AND j.payload->>'webhook_log_id'=w.id::text)''').fetchone()['count']
    if pending:
        alerts.append(dict(key='webhook:legacy', issue='unmanaged_pending_webhooks', count=pending))
    return dict(healthy=not alerts, alerts=alerts, queues=queues, schedules=schedules,
                active_services=sorted(active_services), instances=instances)


def record_alerts(connection, report):
    keys = [a['key'] for a in report['alerts']]
    with connection.transaction():
        connection.execute('UPDATE public.worker_alerts SET resolved_at=now() WHERE resolved_at IS NULL '
                           'AND NOT (alert_key=ANY(%s))', (keys,))
        for alert in report['alerts']:
            safe = json.loads(json.dumps(alert, default=str))
            connection.execute('''INSERT INTO public.worker_alerts(alert_key,summary) VALUES (%s,%s)
                ON CONFLICT(alert_key) DO UPDATE SET last_seen_at=now(),resolved_at=NULL,summary=excluded.summary''',
                (alert['key'], Jsonb(safe)))
