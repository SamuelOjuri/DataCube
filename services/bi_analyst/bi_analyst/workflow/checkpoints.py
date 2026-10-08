"""Pinned PostgreSQL saver with short owner-scoped transactions on the state pool."""
from contextlib import asynccontextmanager

from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from psycopg.rows import dict_row


class ScopedPostgresSaver(AsyncPostgresSaver):
    def __init__(self, store, actor, run_id):
        super().__init__(store.db.state)
        self.store, self.actor, self.run_id = store, actor, run_id

    async def setup(self):
        raise RuntimeError("Apply migration 006 offline; runtime DDL is prohibited")

    @asynccontextmanager
    async def _cursor(self, *, pipeline=False):
        # Override the pinned driver's acquisition hook, preserving its serialization
        # and pending-write semantics. No connection survives a checkpoint operation.
        async with self.lock, self.store.scoped(self.actor) as conn:
            await conn.execute("SELECT set_config('search_path','pg_catalog,analyst_state',true), "
                               "set_config('bi_analyst.workflow_run_id',%s,true)", (str(self.run_id),))
            async with conn.cursor(binary=True, row_factory=dict_row) as cursor:
                yield cursor
