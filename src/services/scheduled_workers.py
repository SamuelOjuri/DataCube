"""UTC schedules with database ownership, occurrence dedupe and run accounting."""
import asyncio
from datetime import datetime, timedelta, timezone
import logging
from uuid import uuid4

from apscheduler.events import EVENT_JOB_MISSED, EVENT_JOB_MAX_INSTANCES
from . import worker_store as store
from .worker_monitor import monitor

LOG = logging.getLogger(__name__)
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


class ScheduledWorkers:
    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.specs = {}
        self.tasks = set()

    def add(self, name, handler, *, seconds=None, hour=None, minute=None, grace=300):
        self.specs[name] = dict(handler=handler, seconds=seconds, hour=hour, minute=minute)
        options = dict(id=name, coalesce=True, max_instances=1, misfire_grace_time=grace,
                       kwargs={'name': name}, replace_existing=True)
        if seconds:
            self.scheduler.add_job(self.execute, 'interval', seconds=seconds, start_date=EPOCH, **options)
        else:
            self.scheduler.add_job(self.execute, 'cron', hour=hour, minute=minute, **options)
        monitor.register('schedule:' + name, budget=7200)
        monitor.update('schedule:' + name, 'idle')

    def occurrence(self, name, now):
        spec = self.specs[name]
        if spec['seconds']:
            return EPOCH + timedelta(seconds=int((now - EPOCH).total_seconds() // spec['seconds']) * spec['seconds'])
        due = now.replace(hour=spec['hour'], minute=spec['minute'], second=0, microsecond=0)
        return due if due <= now else due - timedelta(days=1)

    def next_due(self, name, due):
        return due + timedelta(seconds=self.specs[name]['seconds'] or 86400)

    def seed(self):
        with store.connect() as connection:
            for name in self.specs:
                next_due = self.scheduler.get_job(name).next_run_time
                connection.execute('''INSERT INTO public.worker_schedules(job_id,next_due_at)
                    VALUES (%s,%s) ON CONFLICT DO NOTHING''', (name, next_due))

    async def execute(self, name):
        due = self.occurrence(name, store.utcnow())
        worker_name = 'schedule:' + name
        monitor.update(worker_name, 'busy', job=due.isoformat())
        def run():
            return store.run_scheduled(name, due, monitor.instance_id,
                                       lambda: asyncio.run(self.specs[name]['handler']()),
                                       next_due=self.next_due(name, due))
        # Shield the actual thread from scheduler shutdown cancellation. Monitor
        # and session lock remain tied to real completion, never to cancellation.
        async def attempt():
            try:
                result = await asyncio.to_thread(run)
                if result['outcome'] in {'partial', 'failed', 'skipped', 'previous_run_unresolved'}:
                    monitor.update(worker_name, 'degraded', error=RuntimeError())
                else:
                    monitor.update(worker_name, 'idle', success=result['outcome'] == 'succeeded')
                return result
            except Exception as exc:
                monitor.update(worker_name, 'degraded', error=exc)
                raise
        task = asyncio.create_task(attempt())
        self.tasks.add(task)
        task.add_done_callback(self.completed)
        return await asyncio.shield(task)

    def event(self, event):
        async def record():
            def write():
                with store.connect() as connection:
                    for due in getattr(event, 'scheduled_run_times', [getattr(event, 'scheduled_run_time', None)]):
                        if due is None:
                            continue
                        connection.execute('''INSERT INTO public.worker_job_runs
                            (id,job_id,scheduled_for,instance_id,outcome,finished_at)
                            VALUES (%s,%s,%s,%s,%s,now()) ON CONFLICT(job_id,scheduled_for) DO NOTHING''',
                            (uuid4(), event.job_id, due, monitor.instance_id,
                             'missed' if event.code == EVENT_JOB_MISSED else 'max_instances'))
            try:
                await asyncio.to_thread(write)
            except Exception as exc:
                monitor.update('scheduler', 'degraded', error=exc)
                LOG.error('scheduler_event_persist_failed error_type=%s', type(exc).__name__)
        task = asyncio.create_task(record())
        self.tasks.add(task)
        task.add_done_callback(self.completed)

    def completed(self, task):
        self.tasks.discard(task)
        # Scheduler shutdown can detach its waiter; always retrieve exceptions.
        if not task.cancelled():
            task.exception()

    def probe(self):
        monitor.update('scheduler', 'idle' if self.scheduler.running else 'stopped')
        for name in self.specs:
            worker_name = 'schedule:' + name
            # Idle/failed jobs wait until the next schedule; freshness is checked
            # using the durable ledger, not a fictitious processing heartbeat.
            with monitor.lock:
                worker = monitor.workers[worker_name]
                if worker['state'] != 'busy':
                    worker['progress'] = monitor.clock()

    async def start(self):
        self.scheduler.add_listener(self.event, EVENT_JOB_MISSED | EVENT_JOB_MAX_INSTANCES)
        self.scheduler.start()
        monitor.register('scheduler')
        monitor.probes['scheduler'] = self.probe
        self.probe()
        await asyncio.to_thread(self.seed)

    async def stop(self):
        monitor.probes.pop('scheduler', None)
        if self.scheduler.running:
            self.scheduler.shutdown(wait=False)
        if self.tasks:
            await asyncio.wait(self.tasks, timeout=10)
        monitor.update('scheduler', 'stopped')
