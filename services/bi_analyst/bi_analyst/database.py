"""Bounded independent pools and fail-closed privilege checks."""
from contextlib import asynccontextmanager
from importlib.resources import files

from psycopg import sql
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from .settings import Settings


GATEWAYS = (
    "projects_v1", "children_v1", "child_totals_v1", "hidden_values_v1",
    "latest_analysis_v1", "enquiry_monthly_v1", "bookings_monthly_v1",
    "invoice_reporting_facts_v1", "conversion_cohorts_v1", "coverage_v1",
)
STATE_TABLES = ("schema_version", "principals", "conversations", "runs", "results", "rate_limits", "audit_events")
AUTH_TABLES = ("external_identities", "oauth_attempts", "sessions", "auth_rate_limit")


class Database:
    def __init__(self, settings: Settings):
        self.settings = settings
        common = dict(min_size=1, open=False, timeout=settings.pool_timeout_seconds,
                      max_waiting=16, max_lifetime=1800, reconnect_timeout=10,
                      check=AsyncConnectionPool.check_connection)
        kwargs = dict(autocommit=True, row_factory=dict_row, connect_timeout=5,
                      application_name="datacube-bi-analyst")
        self.read = AsyncConnectionPool(settings.read_dsn.get_secret_value(),
                                       max_size=settings.read_pool_size, kwargs=kwargs, **common)
        self.state = AsyncConnectionPool(settings.state_dsn.get_secret_value(),
                                        max_size=settings.state_pool_size, kwargs=kwargs, **common)

    async def open(self):
        try:
            await self.read.open(wait=True, timeout=10)
            await self.state.open(wait=True, timeout=10)
            await self.verify_permissions()
        except BaseException:
            await self.close()
            raise

    async def close(self):
        await self.read.close()
        await self.state.close()

    @asynccontextmanager
    async def transaction(self, *, subject=None, analytical=False):
        pool = self.read if analytical else self.state
        async with pool.connection() as conn:
            async with conn.transaction():
                if analytical:
                    await conn.execute("SET TRANSACTION READ ONLY")
                await conn.execute("SELECT set_config('statement_timeout', %s, true), "
                                   "set_config('lock_timeout', '1000', true), "
                                   "set_config('search_path', 'pg_catalog', true)",
                                   (str(self.settings.statement_timeout_ms),))
                if subject is not None:
                    await conn.execute("SELECT set_config('bi_analyst.subject', %s, true)", (str(subject),))
                yield conn

    async def ready(self):
        async with self.transaction(analytical=True) as conn:
            for relation in GATEWAYS:
                await conn.execute(sql.SQL("SELECT * FROM analyst_query.{} LIMIT 0").format(sql.Identifier(relation)))
        async with self.transaction() as conn:
            row = await (await conn.execute("SELECT version FROM analyst_state.schema_version")).fetchone()
            if row not in ({"version": 3}, {"version": 5}) or (self.settings.auth_provider == "monday" and row != {"version": 5}):
                raise RuntimeError("Unexpected analyst schema version")
            if row == {"version": 5}:
                for relation in AUTH_TABLES:
                    await conn.execute(sql.SQL("SELECT * FROM analyst_state.{} LIMIT 0").format(sql.Identifier(relation)))

    async def verify_permissions(self):
        async with self.transaction() as conn:
            version = await (await conn.execute("SELECT version FROM analyst_state.schema_version")).fetchone()
        auth_installed = version == {"version": 5}
        for analytical, expected in (
            (True, "bi_analyst_reader"),
            (False, "bi_analyst_state"),
        ):
            async with self.transaction(analytical=analytical) as conn:
                row = await (await conn.execute("SELECT current_user AS role")).fetchone()
                if row["role"] != expected:
                    raise RuntimeError("Unexpected analyst connection role")
                violations = await (await conn.execute(
                    files("bi_analyst").joinpath("permissions_auth.sql" if auth_installed else "permissions.sql").read_text(encoding="utf-8")
                )).fetchall()
                if violations:
                    # Object names/issue codes only; never include DSNs or data.
                    details = "; ".join(f"{v['role_name']}: {v['issue']} [{v['object_name']}]" for v in violations[:20])
                    raise RuntimeError("Unsafe analyst effective privileges: " + details)
                if not analytical:
                    state_drift = await (await conn.execute("""
                        SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                        WHERE n.nspname='analyst_state' AND c.relname=ANY(%s)
                        AND (NOT c.relrowsecurity OR NOT c.relforcerowsecurity
                          OR has_table_privilege(c.oid,'DELETE,TRUNCATE,TRIGGER,REFERENCES')) LIMIT 1
                    """, (list(STATE_TABLES[1:] + (AUTH_TABLES if auth_installed else ())),))).fetchone()
                    permission_drift = await (await conn.execute("""
                        SELECT has_table_privilege('analyst_state.principals','INSERT,UPDATE')
                          OR has_any_column_privilege('analyst_state.principals','INSERT,UPDATE')
                          OR has_table_privilege('analyst_state.audit_events','SELECT,UPDATE')
                          OR has_any_column_privilege('analyst_state.audit_events','SELECT,UPDATE')
                          OR has_any_column_privilege('analyst_state.conversations','UPDATE')
                          OR has_any_column_privilege('analyst_state.results','UPDATE')
                          OR has_column_privilege('analyst_state.runs','owner_id','UPDATE') AS unsafe
                    """)).fetchone()
                    if state_drift or permission_drift['unsafe']:
                        raise RuntimeError("Unsafe state permissions or row security")
                    if auth_installed:
                        drift = await (await conn.execute("""
                            SELECT has_any_column_privilege('analyst_state.external_identities','INSERT,UPDATE')
                              OR has_table_privilege('analyst_state.sessions','UPDATE')
                              OR has_column_privilege('analyst_state.sessions','owner_id','UPDATE')
                              OR has_column_privilege('analyst_state.sessions','token_hash','UPDATE')
                              OR has_column_privilege('analyst_state.sessions','permissions_version','UPDATE')
                              OR has_column_privilege('analyst_state.sessions','expires_at','UPDATE')
                              OR has_column_privilege('analyst_state.sessions','created_at','UPDATE')
                              OR has_column_privilege('analyst_state.oauth_attempts','nonce_hash','UPDATE')
                              OR has_column_privilege('analyst_state.oauth_attempts','client_challenge','UPDATE')
                              OR has_column_privilege('analyst_state.oauth_attempts','state_hash','UPDATE') AS unsafe
                        """)).fetchone()
                        if drift['unsafe']:
                            raise RuntimeError("Unsafe authentication permissions")
        async with self.transaction(analytical=True) as conn:
            owners = await (await conn.execute("""
                SELECT c.relname, r.rolname, r.rolcanlogin, r.rolsuper, r.rolbypassrls, c.reloptions
                FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                JOIN pg_roles r ON r.oid=c.relowner
                WHERE n.nspname='analyst_query' AND c.relname=ANY(%s) AND c.relkind='v'
            """, (list(GATEWAYS),))).fetchall()
            if len(owners) != len(GATEWAYS) or any(
                r["rolname"] != "bi_analyst_view_owner" or r["rolcanlogin"] or r["rolsuper"] or r["rolbypassrls"]
                or not {"security_barrier=true", "security_invoker=false"} <= set(r["reloptions"] or [])
                for r in owners
            ):
                raise RuntimeError("Unexpected gateway security or ownership")
        await self.ready()
