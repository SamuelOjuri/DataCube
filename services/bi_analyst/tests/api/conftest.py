import asyncio
import os
from pathlib import Path
import re
import sys
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.rows import dict_row
import pytest

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "services/bi_analyst"))
from bi_analyst.settings import Settings

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

ROLES = ("bi_analyst_reader", "bi_analyst_state", "bi_analyst_view_owner", "bi_analyst_migrator")
CONSUMERS = ("bi_fixture_etl", "bi_fixture_powerbi")


@pytest.fixture(scope="module")
def database():
    dsn = os.environ.get("BI_ANALYST_TEST_DSN")
    if not dsn:
        pytest.skip("Set BI_ANALYST_TEST_DSN to an isolated loopback PostgreSQL admin database")
    info = conninfo_to_dict(dsn)
    if (info.get("host") not in {"127.0.0.1", "localhost", "::1"} or info.get("dbname") != "postgres"
        or info.get("hostaddr", info["host"]) not in {"127.0.0.1", "localhost", "::1"} or info.get("service")):
        pytest.fail("Only an explicit loopback postgres connection is allowed")
    name = "bi_api_test_" + uuid4().hex
    installed = False
    consumers_created = False
    with psycopg.connect(dsn, autocommit=True) as admin:
        if admin.execute("SELECT 1 FROM pg_roles WHERE rolname=ANY(%s)", (list(ROLES + CONSUMERS),)).fetchone():
            pytest.fail("Analyst roles already exist: use a dedicated disposable test cluster")
        for role in CONSUMERS:
            admin.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(role)))
        consumers_created = True
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        try:
            test_dsn = make_conninfo(dsn, dbname=name)
            with psycopg.connect(test_dsn, autocommit=True, row_factory=dict_row) as conn:
                conn.execute((ROOT/"services/bi_analyst/tests/semantic/source_fixture.sql").read_text(encoding="utf-8"))
                source = (ROOT/"src/database/schema/schema.sql").read_text(encoding="utf-8")
                for view in ["vw_actual_enquiry_monthly_v1", "vw_actual_bookings_monthly_v1", "vw_actual_revenue_monthly_v1"]:
                    conn.execute(re.search(r"CREATE OR REPLACE VIEW "+view+r" AS\n.*?;", source, re.S).group())
                with conn.transaction():
                    conn.execute((ROOT/"src/database/migrations/20261007_001_analytics_contracts.sql").read_text(encoding="utf-8"))
                migration = (ROOT/"src/database/migrations/20261008_003_analyst_permissions.sql").read_text(encoding="utf-8")
                # Preserve PostgreSQL defaults: PUBLIC schema USAGE and database TEMP.
                # These consumer grants predate the migration and must remain effective.
                conn.execute("GRANT SELECT,INSERT,UPDATE,DELETE ON public.projects TO bi_fixture_etl")
                conn.execute("GRANT SELECT ON public.projects,public.reportable_projects TO bi_fixture_powerbi")
                conn.execute("CREATE POLICY fixture_etl ON public.projects TO bi_fixture_etl USING(true) WITH CHECK(true)")
                conn.execute("CREATE POLICY fixture_powerbi ON public.projects FOR SELECT TO bi_fixture_powerbi USING(true)")
                for table in ["projects", "subitems", "hidden_items", "analysis_results"]:
                    conn.execute(sql.SQL("ALTER TABLE public.{} ENABLE ROW LEVEL SECURITY").format(sql.Identifier(table)))
                # Source function ACL already protects existing consumers. The migration
                # must reject the bootstrap if this function is exposed through PUBLIC.
                conn.execute("CREATE FUNCTION public.forbidden_write() RETURNS integer LANGUAGE sql SECURITY DEFINER AS 'DELETE FROM public.projects RETURNING 1'")
                with pytest.raises(psycopg.errors.RaiseException, match="unreviewed_function"):
                    with conn.transaction():
                        conn.execute(migration)
                # This is synthetic fixture setup, not migration hardening.
                conn.execute("REVOKE EXECUTE ON FUNCTION public.forbidden_write() FROM PUBLIC")
                conn.execute("GRANT EXECUTE ON FUNCTION public.forbidden_write() TO bi_fixture_etl")
                conn.execute("CREATE TABLE public.project_reporting_classifications(monday_id text,classification text)")
                function_source = (ROOT/"src/database/schema/project_reporting.sql").read_text(encoding="utf-8")
                for function in ["project_placeholder_is_empty", "excluded_project_ids"]:
                    definition = re.search(r"CREATE OR REPLACE FUNCTION public\."+function+r"\(.*?\$\$;", function_source, re.S).group()
                    conn.execute(definition)
                # Snapshot effective consumer/PUBLIC ACL entries before analyst grants.
                conn.execute("""CREATE TEMP TABLE before_acl AS
                    SELECT c.oid,a.grantee,a.privilege_type,a.is_grantable
                    FROM pg_class c CROSS JOIN LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a
                    WHERE c.relnamespace='public'::regnamespace
                      AND a.grantee IN (0,'bi_fixture_etl'::regrole,'bi_fixture_powerbi'::regrole)""")
                with conn.transaction():
                    conn.execute(migration)
                installed = True
                for role in ROLES[:2]:
                    conn.execute(sql.SQL("ALTER ROLE {} LOGIN").format(sql.Identifier(role)))
                conn.execute("CREATE TABLE analyst_query.future_data(secret text)")
                yield conn, test_dsn
        finally:
            admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
            if installed:
                for role in ROLES:
                    admin.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))
            if consumers_created:
                for role in CONSUMERS:
                    admin.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))


@pytest.fixture
def settings(database):
    _, dsn = database
    return Settings(environment="test", read_dsn=make_conninfo(dsn,user=ROLES[0]),
                    state_dsn=make_conninfo(dsn,user=ROLES[1]),
                    cors_origins=["https://analyst.example.test"])
