"""The Power BI grant repair preserves the analyst's effective privileges."""
from pathlib import Path
import re

import psycopg


ROOT = Path(__file__).resolve().parents[4]
ANALYST_ROLES = ('bi_analyst_reader', 'bi_analyst_state', 'bi_analyst_view_owner',
                 'bi_analyst_migrator')


def analyst_privileges(conn):
    """Snapshot effective privileges, including inherited and PUBLIC grants."""
    return conn.execute('''
        SELECT r.rolname, n.nspname, c.relname,
               has_table_privilege(r.oid,c.oid,'SELECT') AS can_select,
               has_table_privilege(r.oid,c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER') AS can_write
        FROM pg_roles r CROSS JOIN pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE r.rolname=ANY(%s) AND c.relkind IN ('r','v','m','p','f')
          AND n.nspname IN ('public','analytics','analyst_query','analyst_state')
        ORDER BY r.rolname,n.nspname,c.relname
    ''', (list(ANALYST_ROLES),)).fetchall(), conn.execute('''
        SELECT r.rolname,p.oid::regprocedure::text AS signature,
               has_function_privilege(r.oid,p.oid,'EXECUTE') AS can_execute
        FROM pg_roles r CROSS JOIN pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
        WHERE r.rolname=ANY(%s) AND n.nspname IN ('public','analytics','analyst_query','analyst_state')
        ORDER BY r.rolname,signature
    ''', (list(ANALYST_ROLES),)).fetchall()


def probe_as_reader(conn, statement):
    with conn.transaction(force_rollback=True):
        conn.execute('SET LOCAL ROLE powerbi_reader')
        return conn.execute(statement).fetchall()


def expect_denied(conn, statement, message):
    try:
        probe_as_reader(conn, statement)
    except psycopg.errors.InsufficientPrivilege as exc:
        assert message in str(exc)
    else:
        raise AssertionError('Expected access denial')


def test_powerbi_fix_preserves_analyst_boundary(database):
    conn, _ = database
    with conn.transaction(force_rollback=True):
        # All setup, grants and probes are confined to a disposable local fixture
        # and rolled back. No source environment credentials are loaded.
        conn.execute('CREATE ROLE powerbi_reader NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS')
        conn.execute('ALTER TABLE public.projects ADD COLUMN item_name text')
        conn.execute("INSERT INTO public.projects(monday_id,item_name,pipeline_stage) VALUES ('placeholder','New project','Open Enquiry')")
        conn.execute("INSERT INTO public.project_reporting_classifications VALUES ('placeholder','redundant_placeholder')")
        conn.execute('''CREATE OR REPLACE VIEW public.reportable_projects WITH(security_invoker=true) AS
            SELECT p.* FROM public.projects p WHERE p.monday_id NOT IN
                (SELECT monday_id FROM public.excluded_project_ids())''')
        conn.execute('REVOKE EXECUTE ON FUNCTION public.excluded_project_ids(), public.project_placeholder_is_empty(jsonb) FROM PUBLIC')
        conn.execute('CREATE POLICY powerbi_fixture_public_read ON public.projects FOR SELECT TO PUBLIC USING(true)')
        schema = (ROOT / 'src/database/schema/schema.sql').read_text(encoding='utf-8')
        conn.execute(re.search(r'CREATE OR REPLACE VIEW vw_weighted_enquiry_value_monthly_v1 AS\n.*?;', schema, re.S).group())
        conn.execute('GRANT SELECT ON public.vw_weighted_enquiry_value_monthly_v1 TO powerbi_reader')

        expect_denied(conn, 'SELECT * FROM public.vw_weighted_enquiry_value_monthly_v1',
                      'permission denied for table projects')
        # Demonstrate why a table-only repair still leaves the next dependency blocked.
        with conn.transaction(force_rollback=True):
            conn.execute('GRANT SELECT ON public.projects TO powerbi_reader')
            expect_denied(conn, 'SELECT * FROM public.vw_weighted_enquiry_value_monthly_v1',
                          'permission denied for function excluded_project_ids')

        before = analyst_privileges(conn)
        audit_sql = (ROOT / 'services/bi_analyst/bi_analyst/permissions.sql').read_text(encoding='utf-8')
        assert conn.execute(audit_sql).fetchall() == []
        expected = conn.execute('SELECT * FROM public.vw_weighted_enquiry_value_monthly_v1').fetchall()
        conn.execute((ROOT / 'src/database/schema/powerbi_reader_reporting_permissions.sql').read_text(encoding='utf-8'))

        assert probe_as_reader(conn, 'SELECT * FROM public.vw_weighted_enquiry_value_monthly_v1') == expected
        assert probe_as_reader(conn, 'SELECT * FROM public.excluded_project_ids()') == [{'monday_id': 'placeholder'}]
        assert probe_as_reader(conn, "SELECT monday_id FROM public.reportable_projects WHERE monday_id='placeholder'") == []
        assert probe_as_reader(conn, "SELECT monday_id FROM public.projects WHERE monday_id='placeholder'") == [{'monday_id': 'placeholder'}]
        assert analyst_privileges(conn) == before
        assert conn.execute(audit_sql).fetchall() == []
        assert conn.execute("SELECT reloptions FROM pg_class WHERE oid='public.reportable_projects'::regclass").fetchone()['reloptions'] == ['security_invoker=true']
        assert conn.execute("SELECT relrowsecurity FROM pg_class WHERE oid='public.projects'::regclass").fetchone()['relrowsecurity']
        assert conn.execute("SELECT 1 FROM pg_auth_members WHERE member='powerbi_reader'::regrole").fetchall() == []
        expect_denied(conn, 'SELECT * FROM analyst_state.principals', 'permission denied for schema analyst_state')
        expect_denied(conn, 'SELECT * FROM analyst_query.projects_v1', 'permission denied for schema analyst_query')
        expect_denied(conn, 'UPDATE public.projects SET new_enquiry_value=0', 'permission denied for table projects')
        expect_denied(conn, 'SELECT * FROM public.project_reporting_classifications',
                      'permission denied for table project_reporting_classifications')
        # Repeating the targeted script leaves effective privileges stable.
        conn.execute((ROOT / 'src/database/schema/powerbi_reader_reporting_permissions.sql').read_text(encoding='utf-8'))
        assert analyst_privileges(conn) == before
