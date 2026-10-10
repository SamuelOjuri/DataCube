from uuid import uuid4
from pathlib import Path

from fastapi.testclient import TestClient
import psycopg
from psycopg.types.json import Jsonb
import pytest

from bi_analyst.api import create_app
from bi_analyst.identity import Identity, current_identity
from bi_analyst.database import Database
from bi_analyst.store import Principal, Store
from importlib.resources import files
import asyncio

pytestmark = pytest.mark.postgres


@pytest.fixture
def users(database):
    conn, _ = database
    a, b, disabled, partial = (uuid4() for _ in range(4))
    for subject, enabled, company in [(a,True,True),(b,True,True),(disabled,False,True),(partial,True,False)]:
        conn.execute("INSERT INTO analyst_state.principals(subject,enabled,company_wide) VALUES (%s,%s,%s)", (subject,enabled,company))
    return a, b, disabled, partial


def identity(app, subject):
    async def trusted_test_identity():
        return Identity(subject)
    app.dependency_overrides[current_identity] = trusted_test_identity


def test_default_no_identity_and_health(settings):
    app = create_app(settings)
    with TestClient(app) as client:
        assert client.get("/health/live").status_code == 200
        assert client.get("/health/ready").json()["identity"] == "deferred"
        for headers in [{}, {"Authorization":"Bearer pretend-token"}, {"X-User-ID":str(uuid4())}]:
            response = client.get("/v1/conversations",headers=headers)
            assert response.status_code == 503 and response.json()["detail"] == "identity_not_configured"
            assert response.headers["Cache-Control"] == "no-store"
            assert response.headers["X-Request-ID"]


def test_cross_user_access_revocation_export_and_audit(settings, database, users):
    admin, _ = database
    a, b, disabled, partial = users
    app = create_app(settings)
    with TestClient(app) as client:
        identity(app,a)
        conversation = client.post("/v1/conversations",json={"title":"Private analysis"}).json()
        cid = conversation["id"]
        response = client.post(f"/v1/conversations/{cid}/runs",json={"question":"private prompt do not log"})
        assert response.status_code == 201, response.text
        run = response.json()
        rid = run["id"]
        assert run["status"] == "registered"
        assert client.get(f"/v1/conversations/{cid}/runs").json()[0]['id'] == rid
        result_id = uuid4()
        admin.execute("""INSERT INTO analyst_state.results(id,run_id,owner_id,permissions_version,columns,rows,provenance)
            VALUES (%s,%s,%s,1,%s,%s,%s)""", (result_id,rid,a,Jsonb(["label","amount"]),Jsonb([["=HYPERLINK('bad')",-3]]),Jsonb({"population":"synthetic","truncated":False})))
        assert client.get(f"/v1/results/{result_id}").status_code == 200
        export = client.get(f"/v1/results/{result_id}/export")
        assert export.status_code == 200 and "'=HYPERLINK" in export.text
        assert export.headers["X-Export-Scope"] == "stored-result-rows"
        assert client.post(f"/v1/runs/{rid}/resume").status_code == 503
        identity(app,b)
        assert client.get("/v1/conversations").json() == []
        for method,path,kwargs in [
            ("get",f"/v1/conversations/{cid}",{}), ("get",f"/v1/runs/{rid}",{}),
            ("get",f"/v1/conversations/{cid}/runs",{}),
            ("get",f"/v1/results/{result_id}",{}),("get",f"/v1/results/{result_id}/export",{}),
            ("post",f"/v1/runs/{rid}/resume",{}),("post",f"/v1/runs/{rid}/cancel",{}),
            ("post",f"/v1/conversations/{cid}/runs",{"json":{"question":"follow-up"}}),
        ]:
            assert getattr(client,method)(path,**kwargs).status_code == 404
        for user in [disabled,partial,uuid4()]:
            identity(app,user)
            assert client.get("/v1/conversations").status_code == 403
        identity(app,a)
        admin.execute("UPDATE analyst_state.principals SET permissions_version=2 WHERE subject=%s",(a,))
        for path in [f"/v1/results/{result_id}",f"/v1/results/{result_id}/export",f"/v1/runs/{rid}"]:
            assert client.get(path).status_code == 403
        assert client.post(f"/v1/runs/{rid}/resume").status_code == 403
        admin.execute("UPDATE analyst_state.principals SET enabled=false WHERE subject=%s",(a,))
        assert client.get(f"/v1/conversations/{cid}").status_code == 403
        events = admin.execute("SELECT * FROM analyst_state.audit_events WHERE owner_id=%s",(b,)).fetchall()
        assert len(events) == 9 and sum(e["status"] == 404 for e in events) == 8
        assert all(cid not in e["route"] and rid not in e["route"] for e in events)


def test_limits_cors_validation_cancellation_and_persistence(settings, users):
    app = create_app(settings)
    identity(app,users[0])
    with TestClient(app) as client:
        invalid = client.post("/v1/conversations",json={"title":"hello","owner_id":"forged"})
        assert invalid.status_code == 422 and "forged" not in invalid.text
        response = client.post("/v1/conversations",content=b"x"*40000)
        assert response.status_code == 413 and "x"*10 not in response.text
        assert response.headers["X-Request-ID"]
        good = client.get("/health/live",headers={"Origin":"https://analyst.example.test"})
        assert good.headers["Access-Control-Allow-Origin"] == "https://analyst.example.test"
        bad = client.get("/health/live",headers={"Origin":"https://evil.example.test"})
        assert "Access-Control-Allow-Origin" not in bad.headers
        cid = client.post("/v1/conversations",json={"title":"Persist me"}).json()["id"]
        rid = client.post(f"/v1/conversations/{cid}/runs",json={"question":"test"}).json()["id"]
        assert client.post(f"/v1/runs/{rid}/cancel").json()["status"] == "cancelled"
        assert client.post(f"/v1/runs/{rid}/cancel").status_code == 200
    restarted = create_app(settings)
    identity(restarted,users[0])
    with TestClient(restarted) as client:
        assert client.get(f"/v1/conversations/{cid}").status_code == 200


def test_rate_limits_shared_across_instances(settings, users):
    config = settings.model_copy(update={"requests_per_minute":2})
    first, second = create_app(config), create_app(config)
    identity(first,users[0])
    identity(second,users[0])
    with TestClient(first) as a, TestClient(second) as b:
        assert a.get("/v1/conversations").status_code == 200
        assert b.get("/v1/conversations").status_code == 200
        limited = a.get("/v1/conversations")
        assert limited.status_code == 429 and limited.headers["Retry-After"] == "60"


def test_real_database_permission_boundary(settings, database, users):
    admin, dsn = database
    with psycopg.connect(settings.read_dsn.get_secret_value(),autocommit=True) as reader:
        assert reader.execute("SHOW default_transaction_read_only").fetchone()[0] == "on"
        assert reader.execute("SELECT sum(total_order_value) FROM analyst_query.projects_v1").fetchone()[0] == 350
        assert reader.execute("SELECT sum(actual_enquiry_value) FROM analyst_query.enquiry_monthly_v1").fetchone()
        assert reader.execute("SELECT sum(total_order_value) FROM public.projects").fetchone()[0] == 1349
        assert reader.execute("SELECT count(*) FROM public.subitems").fetchone()[0] == 7
        assert reader.execute("SELECT sum(total_order_value) FROM analytics.projects_v1").fetchone()[0] == 350
        assert reader.execute("SELECT * FROM public.excluded_project_ids()").fetchall() == []
        for forbidden in ["SELECT public.forbidden_write()", "SELECT * FROM public.project_reporting_classifications",
                          "SELECT * FROM analyst_state.conversations",
                          "SELECT * FROM analyst_query.future_data", "SET ROLE bi_analyst_view_owner", "SET ROLE bi_analyst_migrator"]:
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                reader.execute(forbidden)
        # A client can change default_transaction_read_only; ACLs still deny all writes.
        reader.execute("SET default_transaction_read_only=off")
        # TEMP is permitted under Option A; permanent objects and operational DML are not.
        reader.execute("CREATE TEMP TABLE scratch(x int)")
        reader.execute("INSERT INTO scratch VALUES (1)")
        for forbidden in ["CREATE TABLE analyst_query.bad(x int)", "CREATE TABLE public.bad(x int)",
                          "CREATE SCHEMA bad", "SELECT public.forbidden_write()",
                          "SELECT public.fixture_invoker_write()",
                          "INSERT INTO public.projects(monday_id) VALUES ('bad')",
                          "DELETE FROM public.projects", "TRUNCATE public.projects",
                          "UPDATE public.projects SET total_order_value=0",
                          "UPDATE analyst_query.projects_v1 SET total_order_value=0"]:
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                reader.execute(forbidden)
    with psycopg.connect(settings.state_dsn.get_secret_value(),autocommit=True) as state:
        assert state.execute("SELECT * FROM analyst_state.principals").fetchall() == []
        for forbidden in ["SELECT * FROM analyst_query.projects_v1", "SELECT * FROM public.projects",
                          "UPDATE analyst_state.principals SET enabled=true", "DELETE FROM analyst_state.audit_events",
                          "SET ROLE bi_analyst_view_owner", "SET ROLE bi_analyst_migrator"]:
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                state.execute(forbidden)
        a,b,*_ = users
        with state.transaction():
            state.execute("SELECT set_config('bi_analyst.subject',%s,true)",(str(a),))
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                with state.transaction():
                    state.execute("INSERT INTO analyst_state.conversations(id,owner_id,title) VALUES (%s,%s,'bad')",(uuid4(),b))
        assert state.execute("SELECT * FROM analyst_state.principals").fetchall() == []


def test_migrator_is_scoped_to_analyst_administration(database, users):
    admin, dsn = database
    flags = admin.execute("""SELECT rolcanlogin,rolsuper,rolcreatedb,rolcreaterole,rolinherit,
        rolreplication,rolbypassrls FROM pg_roles WHERE rolname='bi_analyst_migrator'""").fetchone()
    assert flags is not None and not any(flags.values())
    assert admin.execute("""SELECT nspname FROM pg_namespace
        WHERE nspowner='bi_analyst_migrator'::regrole ORDER BY nspname""").fetchall() == [
            {'nspname': 'analyst_query'}, {'nspname': 'analyst_state'}]
    assert admin.execute("""SELECT count(*) AS n FROM pg_class
        WHERE relnamespace='analyst_state'::regnamespace AND relkind='r'
          AND relowner='bi_analyst_migrator'::regrole""").fetchone()['n'] == 7
    # Unlike SET ROLE from an administrator session, this also restricts SET ROLE
    # to the migrator's own memberships. No LOGIN or password is granted to it.
    with psycopg.connect(dsn, autocommit=True) as migrator:
        migrator.execute("SET SESSION AUTHORIZATION bi_analyst_migrator")
        with migrator.transaction(force_rollback=True):
            subject = uuid4()
            migrator.execute("CREATE TABLE analyst_state.migration_probe(id integer)")
            migrator.execute("ALTER TABLE analyst_state.migration_probe ADD COLUMN note text")
            migrator.execute("ALTER TABLE analyst_state.principals ADD COLUMN migration_probe text")
            migrator.execute("INSERT INTO analyst_state.principals(subject) VALUES (%s)", (subject,))
            migrator.execute("""UPDATE analyst_state.principals
                SET enabled=true,company_wide=true,permissions_version=permissions_version+1
                WHERE subject=%s""", (subject,))
            assert migrator.execute("""SELECT enabled,company_wide,permissions_version
                FROM analyst_state.principals WHERE subject=%s""", (subject,)).fetchone() == (True, True, 2)
            # FORCE RLS remains active on histories; the provisioning policy applies only to principals.
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                with migrator.transaction():
                    migrator.execute("""INSERT INTO analyst_state.conversations(id,owner_id,title)
                        VALUES (%s,%s,'not a runtime user')""", (uuid4(), users[0]))
            for forbidden in [
                "SELECT * FROM public.projects",
                "SELECT * FROM analyst_query.projects_v1",
                "UPDATE public.projects SET total_order_value=0",
                "CREATE TABLE public.migrator_probe(id integer)",
                "CREATE SCHEMA migrator_probe",
                "CREATE ROLE bi_migrator_probe",
                "ALTER TABLE public.projects ADD COLUMN migrator_probe text",
                "ALTER POLICY analyst_gateway_read ON public.projects USING (true)",
                "SELECT public.forbidden_write()",
                "SET ROLE bi_analyst_reader",
                "SET ROLE bi_analyst_view_owner",
            ]:
                with pytest.raises(psycopg.errors.InsufficientPrivilege):
                    with migrator.transaction():
                        migrator.execute(forbidden)
    assert admin.execute(files("bi_analyst").joinpath("permissions.sql").read_text(encoding="utf-8")).fetchall() == []


@pytest.mark.parametrize("grant,cleanup,issue", [
    ("GRANT UPDATE ON public.projects TO PUBLIC", "REVOKE UPDATE ON public.projects FROM PUBLIC", "persistent_write"),
    ("GRANT UPDATE(total_order_value) ON public.projects TO PUBLIC", "REVOKE UPDATE(total_order_value) ON public.projects FROM PUBLIC", "persistent_write"),
    ("GRANT CREATE ON SCHEMA public TO PUBLIC", "REVOKE CREATE ON SCHEMA public FROM PUBLIC", "schema_create"),
    ("GRANT EXECUTE ON FUNCTION public.forbidden_write() TO PUBLIC", "REVOKE EXECUTE ON FUNCTION public.forbidden_write() FROM PUBLIC", "unreviewed_function"),
    ("GRANT SELECT ON analyst_query.future_data TO PUBLIC", "REVOKE SELECT ON analyst_query.future_data FROM PUBLIC", "unapproved_read"),
    ("GRANT bi_analyst_view_owner TO bi_analyst_reader", "REVOKE bi_analyst_view_owner FROM bi_analyst_reader", "unsafe_role"),
    ("GRANT bi_analyst_migrator TO bi_analyst_reader", "REVOKE bi_analyst_migrator FROM bi_analyst_reader", "unsafe_role"),
    ("GRANT bi_analyst_migrator TO bi_analyst_state", "REVOKE bi_analyst_migrator FROM bi_analyst_state", "unsafe_role"),
    ("GRANT bi_analyst_reader TO bi_analyst_migrator", "REVOKE bi_analyst_reader FROM bi_analyst_migrator", "unsafe_role"),
    ("GRANT SELECT ON public.projects TO bi_analyst_migrator", "REVOKE SELECT ON public.projects FROM bi_analyst_migrator", "unapproved_read"),
    ("GRANT UPDATE ON public.projects TO bi_analyst_migrator", "REVOKE UPDATE ON public.projects FROM bi_analyst_migrator", "persistent_write"),
    ("GRANT CREATE ON SCHEMA public TO bi_analyst_migrator", "REVOKE CREATE ON SCHEMA public FROM bi_analyst_migrator", "schema_create"),
    ("ALTER ROLE bi_analyst_migrator LOGIN", "ALTER ROLE bi_analyst_migrator NOLOGIN", "unsafe_role"),
])
def test_startup_rejects_unsafe_effective_privileges(settings, database, grant, cleanup, issue):
    admin,_ = database
    admin.execute(grant)
    try:
        with pytest.raises(RuntimeError,match=issue):
            with TestClient(create_app(settings)):
                pass
    finally:
        admin.execute(cleanup)


def test_existing_consumer_access_and_public_defaults_are_preserved(database):
    admin,_ = database
    assert admin.execute("SELECT has_schema_privilege('bi_analyst_state','public','USAGE') AS ok").fetchone()['ok']
    assert admin.execute("SELECT has_database_privilege('bi_analyst_reader',current_database(),'TEMP') AS ok").fetchone()['ok']
    assert admin.execute("""WITH after_acl AS (
        SELECT c.oid,a.grantee,a.privilege_type,a.is_grantable
        FROM pg_class c CROSS JOIN LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a
        WHERE c.relnamespace='public'::regnamespace
          AND a.grantee IN (0,'bi_fixture_etl'::regrole,'bi_fixture_powerbi'::regrole))
        SELECT * FROM ((SELECT * FROM before_acl EXCEPT SELECT * FROM after_acl)
        UNION ALL (SELECT * FROM after_acl EXCEPT SELECT * FROM before_acl)) changes""").fetchall() == []
    with admin.transaction(force_rollback=True):
        admin.execute("SET LOCAL ROLE bi_fixture_etl")
        admin.execute("UPDATE public.projects SET total_order_value=120 WHERE monday_id='E1'")
        assert admin.execute("SELECT total_order_value FROM public.projects WHERE monday_id='E1'").fetchone()['total_order_value'] == 120
    with admin.transaction():
        admin.execute("SET LOCAL ROLE bi_fixture_powerbi")
        assert admin.execute("SELECT sum(total_order_value) AS total FROM public.reportable_projects").fetchone()['total'] == 350
        assert admin.execute("SELECT count(*) AS n FROM public.projects").fetchone()['n'] == 6


def test_reviewed_function_change_fails_audit(database):
    admin,_ = database
    with admin.transaction(force_rollback=True):
        # Even a benign replacement cannot inherit the old function's approval.
        admin.execute("CREATE OR REPLACE FUNCTION public.excluded_project_ids() RETURNS TABLE(monday_id text) LANGUAGE sql STABLE SECURITY DEFINER SET search_path='' AS 'SELECT monday_id FROM public.projects'")
        rows=admin.execute(files("bi_analyst").joinpath("permissions.sql").read_text(encoding="utf-8")).fetchall()
        assert any(r['issue']=='unreviewed_function' and 'excluded_project_ids' in r['object_name'] for r in rows)


def test_shared_invoker_functions_and_extension_statistics_are_compatible(database):
    admin, _ = database
    audit = files("bi_analyst").joinpath("permissions.sql").read_text(encoding="utf-8")
    assert admin.execute(audit.replace("\n", "\r\n")).fetchall() == []
    assert admin.execute("""SELECT has_function_privilege('bi_analyst_reader',
        'public.fixture_invoker_write()','EXECUTE') AS allowed""").fetchone()['allowed']
    for view in ("pg_stat_statements", "pg_stat_statements_info"):
        assert admin.execute("SELECT has_table_privilege('bi_analyst_reader',%s,'SELECT') AS allowed",
                             ("extensions." + view,)).fetchone()['allowed']
    with admin.transaction(force_rollback=True):
        admin.execute("ALTER FUNCTION public.fixture_invoker_write() SECURITY DEFINER")
        rows = admin.execute(audit).fetchall()
        assert sum(r['issue'] == 'unreviewed_function' and 'fixture_invoker_write' in r['object_name'] for r in rows) == 4
    with admin.transaction(force_rollback=True):
        admin.execute("REVOKE EXECUTE ON FUNCTION public.fixture_invoker_write() FROM PUBLIC")
        admin.execute("GRANT EXECUTE ON FUNCTION public.fixture_invoker_write() TO bi_analyst_reader")
        assert any(r['issue'] == 'unreviewed_function' and 'fixture_invoker_write' in r['object_name']
                   for r in admin.execute(audit).fetchall())
    with admin.transaction(force_rollback=True):
        admin.execute("CREATE VIEW public.pg_stat_statements AS SELECT 1 AS value")
        admin.execute("GRANT SELECT ON public.pg_stat_statements TO PUBLIC")
        assert any(r['issue'] == 'unapproved_read' and r['object_name'] == 'public.pg_stat_statements'
                   for r in admin.execute(audit).fetchall())


def test_readonly_permission_diagnostics_preserve_grants(database):
    admin, _ = database
    diagnostic = (Path(__file__).resolve().parents[2] / "diagnose_permissions.sql").read_text(encoding="utf-8")
    acl_snapshot = """
        SELECT 'relation' AS kind,oid,relacl::text AS acl FROM pg_class
        UNION ALL SELECT 'function',oid,proacl::text FROM pg_proc
        UNION ALL SELECT 'schema',oid,nspacl::text FROM pg_namespace
        UNION ALL SELECT 'role',oid,row_to_json(r)::text FROM pg_roles r
        UNION ALL SELECT 'column',attrelid,jsonb_build_array(attnum,attacl)::text
          FROM pg_attribute WHERE attacl IS NOT NULL
        ORDER BY 1,2,3"""
    admin.execute("CREATE SCHEMA bi_diagnostic_fixture")
    try:
        admin.execute("CREATE TABLE bi_diagnostic_fixture.example(value integer)")
        admin.execute("GRANT SELECT ON bi_diagnostic_fixture.example TO PUBLIC,bi_fixture_powerbi")
        admin.execute("GRANT UPDATE(value) ON bi_diagnostic_fixture.example TO PUBLIC")
        admin.execute("GRANT SELECT,INSERT,UPDATE,DELETE ON bi_diagnostic_fixture.example TO bi_fixture_etl")
        admin.execute("""CREATE FUNCTION bi_diagnostic_fixture.never_execute() RETURNS integer
            LANGUAGE plpgsql SECURITY DEFINER AS $$
            BEGIN RAISE EXCEPTION 'Diagnostic executed an inspected function'; END $$""")
        before = admin.execute(acl_snapshot).fetchall()
        with admin.transaction():
            admin.execute("SET TRANSACTION READ ONLY")
            rows = admin.execute(diagnostic).fetchall()
            assert admin.execute("SHOW transaction_read_only").fetchone()['transaction_read_only'] == 'on'
        assert admin.execute(acl_snapshot).fetchall() == before
        assert all(set(row) == {'section','object_name','details'} for row in rows)
        roles = [r for r in rows if r['section'] == 'bootstrap_role']
        assert len(roles) == 4 and all(r['details']['present'] for r in roles)
        schemas = [r for r in rows if r['section'] == 'bootstrap_schema']
        assert len(schemas) == 2 and all(r['details']['present'] for r in schemas)
        grants = [r['details'] for r in rows if r['object_name'] == 'bi_diagnostic_fixture.example']
        assert any(g['scope'] == 'relation' and g['privilege'] == 'SELECT' for g in grants)
        assert any(g['scope'] == 'column' and g['column'] == 'value' and g['privilege'] == 'UPDATE' for g in grants)
        function = next(r['details'] for r in rows if r['object_name'] == 'bi_diagnostic_fixture.never_execute()')
        assert function['security_definer'] and function['acl_source'] == 'postgres_default'
        assert not function['public_schema_usage'] and 'definition' not in function
        assert not any(r['section'] == 'public_function_execute' and r['object_name'] == 'public.forbidden_write()' for r in rows)
        helper = next(r['details'] for r in rows if r['section'] == 'reporting_helper' and r['object_name'] == 'public.excluded_project_ids()')
        assert helper['present'] and helper['security_definer']
        assert 'SELECT p.monday_id' in helper['stored_source'] and 'CREATE OR REPLACE FUNCTION' in helper['definition']
        assert helper['source_md5'] and helper['settings'] == ['search_path=""']
    finally:
        admin.execute("DROP SCHEMA bi_diagnostic_fixture CASCADE")


def test_analytical_transactions_enforce_read_only(settings):
    async def check():
        db=Database(settings)
        await db.open()
        try:
            async with db.transaction(analytical=True) as conn:
                assert (await (await conn.execute("SHOW transaction_read_only")).fetchone())['transaction_read_only'] == 'on'
                assert (await (await conn.execute("SHOW statement_timeout")).fetchone())['statement_timeout'] == '5s'
                with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
                    await conn.execute("UPDATE public.projects SET total_order_value=0")
            async with db.transaction(analytical=True) as conn:
                assert (await (await conn.execute("SHOW transaction_read_only")).fetchone())['transaction_read_only'] == 'on'
                # Native large-object creation is another reason READ ONLY is essential.
                with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
                    await conn.execute("SELECT pg_catalog.lo_create(0)")
        finally:
            await db.close()
    asyncio.run(check())


@pytest.mark.parametrize('failure,exception', [
    ('statement', psycopg.errors.DivisionByZero),
    ('application', RuntimeError),
    ('cancel', asyncio.CancelledError),
])
def test_state_pipeline_rolls_back_and_pool_scope_is_reset(settings, database, users, failure, exception):
    admin, _ = database
    owner, other = users[:2]
    conversation_id = uuid4()

    async def check():
        db = Database(settings.model_copy(update={'state_pool_size': 1}))
        await db.open()
        store = Store(db)
        actor = Principal(owner, 1)
        try:
            with pytest.raises(exception):
                async with store.scoped(actor) as conn:
                    await conn.execute("""INSERT INTO analyst_state.conversations(id,owner_id,title)
                        VALUES (%s,%s,'Must roll back')""", (conversation_id, owner))
                    if failure == 'statement':
                        await conn.execute('SELECT 1/0')
                    else:
                        raise exception()
            assert admin.execute('SELECT 1 FROM analyst_state.conversations WHERE id=%s',
                                 (conversation_id,)).fetchone() is None
            async with db.state.connection() as conn:
                assert conn.info.transaction_status == psycopg.pq.TransactionStatus.IDLE
                assert conn.info.pipeline_status == psycopg.pq.PipelineStatus.OFF
                scope = await (await conn.execute(
                    "SELECT current_setting('bi_analyst.subject',true) AS subject")).fetchone()
                assert not scope['subject']
            saved = await store.conversations(actor, title='Committed after rollback')
            assert admin.execute('SELECT owner_id FROM analyst_state.conversations WHERE id=%s',
                                 (saved['id'],)).fetchone()['owner_id'] == owner
            assert await store.conversations(Principal(other, 1)) == []
        finally:
            await db.close()

    asyncio.run(check())
