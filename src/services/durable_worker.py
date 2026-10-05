"""A database consumer whose in-flight thread is never replaced by a timeout."""
import asyncio
import logging
from . import worker_store as store
from .worker_monitor import monitor

LOG = logging.getLogger(__name__)


class DurableWorker:
    def __init__(self, queue, handler, on_outcome=None):
        self.queue, self.handler = queue, handler
        self.on_outcome = on_outcome
        self.task = None
        self.inflight = None
        self.stopping = False
        self.wake = None

    def start(self):
        if self.task is not None and not self.task.done():
            return
        if self.inflight is not None and not self.inflight.done():
            raise RuntimeError('Previous worker thread is still running')
        self.stopping = False
        self.wake = asyncio.Event()
        monitor.register(self.queue)
        self.task = asyncio.create_task(self.run(), name='datacube-' + self.queue)
        monitor.bind(self.queue, self.task)

    def run_once(self):
        with store.connect() as connection:
            job = store.claim(connection, self.queue, monitor.instance_id)
            if not job:
                monitor.update(self.queue, 'idle')
                return False
            monitor.update(self.queue, 'busy', job=str(job['id']))
            token = store.current_job.set(str(job['id']))
            try:
                try:
                    followups = self.handler(job) or ()
                except Exception as exc:
                    outcome = store.finish(connection, job, error=exc)
                    monitor.update(self.queue, 'degraded', error=exc)
                    LOG.warning('worker_job_failed queue=%s error_type=%s', self.queue, type(exc).__name__)
                else:
                    outcome = store.finish(connection, job, followups=followups)
                    monitor.update(self.queue, 'idle', success=True)
                if self.on_outcome:
                    self.on_outcome(outcome)
            finally:
                store.current_job.reset(token)
            return True

    async def run(self):
        while not self.stopping:
            try:
                self.inflight = asyncio.create_task(asyncio.to_thread(self.run_once))
                worked = await asyncio.shield(self.inflight)
            except Exception as exc:
                monitor.update(self.queue, 'degraded', error=exc)
                LOG.error('worker_poll_failed queue=%s error_type=%s', self.queue, type(exc).__name__)
                worked = False
            if not worked and not self.stopping:
                try:
                    await asyncio.wait_for(self.wake.wait(), timeout=5)
                except asyncio.TimeoutError:
                    pass
        monitor.update(self.queue, 'stopped')

    async def stop(self):
        self.stopping = True
        if self.wake:
            self.wake.set()
        if self.task:
            # Await without cancelling the task/thread on timeout. The host may
            # terminate the process; the durable claim is then recoverable.
            done, _ = await asyncio.wait([self.task], timeout=10)
            if done:
                await self.task

