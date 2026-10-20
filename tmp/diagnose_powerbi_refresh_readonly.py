"""Diagnose Power BI view permissions without changing any database objects."""
from datetime import datetime, timezone
import json
from pathlib import Path
import re

from dotenv import dotenv_values
import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'outputs/powerbi_refresh_diagnosis_20261013'
SOURCES = [
    'projects', 'reportable_projects', 'analysis_results',
    'vw_weighted_enquiry_value_monthly_v1',
    'vw_pipeline_forecast_monthly_12m_v1',
    'vw_pipeline_forecast_project_v1',
    'vw_pipeline_budget_combo_chart_v1',
    'vw_pipeline_budget_comparison_monthly_v1',
    'vw_pipeline_smoothed_revenue_monthly_12m_v1',
    'vw_pipeline_smoothing_score_v1',
]


def main():
    values = dotenv_values(ROOT / '.env')
    required = ('PG_ROLES', 'PG_USER', 'PG_USER_PASSWORD', 'PG_HOST', 'PG_PORT', 'PG_DATABASE')
    if any(not values.get(name) for name in required):
        raise ValueError('Required Power BI reader settings are missing')
    if values['PG_ROLES'] != 'powerbi_reader' or not values['PG_USER'].startswith('powerbi_reader.'):
        raise ValueError('Expected the configured Power BI reader')
    if not re.fullmatch(r'[a-z0-9-]+\.pooler\.supabase\.com', values['PG_HOST']):
        raise ValueError('Expected the configured Supabase pooler')
    dsn = make_conninfo(host=values['PG_HOST'], port=values['PG_PORT'],
                       dbname=values['PG_DATABASE'], user=values['PG_USER'],
                       password=values['PG_USER_PASSWORD'])
    result = {'captured_at_utc': datetime.now(timezone.utc).isoformat(),
              'scope': 'Live configured Power BI reader; all database operations read only'}
    with psycopg.connect(dsn, sslmode='require', connect_timeout=15,
                         application_name='datacube_powerbi_refresh_diagnosis_readonly',
                         row_factory=dict_row) as conn:
        conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        conn.execute("SET LOCAL statement_timeout='20s'")
        conn.execute("SET LOCAL lock_timeout='3s'")
        result['identity'] = conn.execute('SELECT current_user, session_user, current_database(), '
            "current_setting('transaction_read_only') AS transaction_read_only").fetchone()
        result['role'] = conn.execute('SELECT rolname,rolsuper,rolbypassrls,rolcreaterole,rolcreatedb '
            'FROM pg_roles WHERE rolname=current_user').fetchone()
        if any(result['role'][key] for key in ('rolsuper', 'rolbypassrls', 'rolcreaterole', 'rolcreatedb')):
            raise ValueError('Reader has privileged attributes; diagnostic stopped')
        result['relations'] = conn.execute('''
            SELECT c.relname, c.relkind, pg_get_userbyid(c.relowner) AS owner,
                   c.reloptions, c.relacl::text AS acl, c.relrowsecurity,
                   has_table_privilege(current_user,c.oid,'SELECT') AS reader_select,
                   CASE WHEN c.relkind='v' THEN pg_get_viewdef(c.oid,true) END AS definition,
                   has_table_privilege(c.relowner,'public.projects','SELECT') AS owner_projects_select,
                   has_table_privilege(c.relowner,'public.analysis_results','SELECT') AS owner_analysis_select
            FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE n.nspname='public' AND c.relname=ANY(%s)
            ORDER BY c.relname
        ''', (SOURCES,)).fetchall()
        result['functions'] = conn.execute('''
            SELECT p.proname, pg_get_userbyid(p.proowner) AS owner, p.prosecdef,
                   p.proacl::text AS acl, has_function_privilege(current_user,p.oid,'EXECUTE') AS reader_execute
            FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
            WHERE n.nspname='public' AND p.proname IN ('excluded_project_ids','project_placeholder_is_empty')
            ORDER BY p.proname
        ''').fetchall()
        result['role_memberships'] = conn.execute('''
            SELECT parent.rolname AS parent_role, member.rolname AS member_role
            FROM pg_auth_members m JOIN pg_roles parent ON parent.oid=m.roleid
            JOIN pg_roles member ON member.oid=m.member
            WHERE member.rolname=current_user
        ''').fetchall()
        result['projects_rls_policies'] = conn.execute('''
            SELECT policyname, permissive, roles, cmd, qual, with_check
            FROM pg_policies WHERE schemaname='public' AND tablename='projects'
            ORDER BY policyname
        ''').fetchall()
        result['dependency_edges'] = conn.execute('''
            SELECT DISTINCT child.relname AS view, parent.relname AS dependency,
                   pg_get_userbyid(child.relowner) AS view_owner, child.reloptions
            FROM pg_rewrite r JOIN pg_class child ON child.oid=r.ev_class
            JOIN pg_namespace n ON n.oid=child.relnamespace
            JOIN pg_depend d ON d.classid='pg_rewrite'::regclass AND d.objid=r.oid
            JOIN pg_class parent ON d.refclassid='pg_class'::regclass AND parent.oid=d.refobjid
            WHERE n.nspname='public' AND child.relname=ANY(%s) AND child.oid<>parent.oid
            ORDER BY child.relname,parent.relname
        ''', (SOURCES,)).fetchall()
        result['referenced_function_permissions'] = conn.execute('''
            SELECT DISTINCT child.relname AS view, p.oid::regprocedure::text AS signature,
                   p.prosecdef, has_function_privilege(current_user,p.oid,'EXECUTE') AS reader_execute
            FROM pg_rewrite r JOIN pg_class child ON child.oid=r.ev_class
            JOIN pg_namespace n ON n.oid=child.relnamespace
            JOIN pg_depend d ON d.classid='pg_rewrite'::regclass AND d.objid=r.oid
            JOIN pg_proc p ON d.refclassid='pg_proc'::regclass AND p.oid=d.refobjid
            WHERE n.nspname='public' AND child.relname=ANY(%s)
            ORDER BY child.relname,signature
        ''', (SOURCES,)).fetchall()
        installed = conn.execute('''WITH state_relations AS (
            SELECT c.oid,c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE n.nspname='analyst_state'
        ) SELECT
            EXISTS(SELECT FROM pg_attribute a JOIN state_relations c ON c.oid=a.attrelid
                WHERE c.relname='sessions' AND a.attname='idle_timeout_seconds' AND NOT a.attisdropped) AS sessions,
            EXISTS(SELECT FROM state_relations WHERE relname='result_feedback') AS presentation,
            EXISTS(SELECT FROM state_relations WHERE relname='workflow_jobs') AS workflow,
            EXISTS(SELECT FROM state_relations WHERE relname='external_identities') AS auth
        ''').fetchone()
        audit_file = next(('permissions_' + version + '.sql' for version in
                           ('sessions', 'presentation', 'workflow', 'auth') if installed[version]),
                          'permissions.sql')
        try:
            with conn.transaction():
                violations = conn.execute((ROOT / 'services/bi_analyst/bi_analyst' / audit_file)
                                          .read_text(encoding='utf-8')).fetchall()
            result['analyst_boundary_audit'] = {'query': audit_file, 'violations': violations}
        except psycopg.Error as exc:
            result['analyst_boundary_audit'] = {'query': audit_file, 'sqlstate': exc.sqlstate,
                                              'message': exc.diag.message_primary}
        result['probes'] = {}
        for source in SOURCES:
            try:
                with conn.transaction():
                    conn.execute(sql.SQL('SELECT * FROM public.{} LIMIT 1')
                                 .format(sql.Identifier(source))).fetchone()
                result['probes'][source] = {'status': 'readable'}
            except psycopg.Error as exc:
                result['probes'][source] = {'status': 'failed', 'sqlstate': exc.sqlstate,
                                           'message': exc.diag.message_primary}
        try:
            with conn.transaction():
                conn.execute('SELECT * FROM public.excluded_project_ids() LIMIT 1').fetchone()
            result['probes']['excluded_project_ids()'] = {'status': 'readable'}
        except psycopg.Error as exc:
            result['probes']['excluded_project_ids()'] = {'status': 'failed', 'sqlstate': exc.sqlstate,
                                                       'message': exc.diag.message_primary}
        conn.rollback()
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / 'reader_diagnosis.json').write_text(json.dumps(result, indent=2, default=str) + '\n', encoding='utf-8')
    summary = {k: v for k, v in result.items() if k not in ('relations', 'dependency_edges')}
    summary['relations'] = [{k: v for k, v in r.items() if k not in ('definition', 'acl')}
                            for r in result['relations']]
    print(json.dumps(summary, indent=2, default=str))


if __name__ == '__main__':
    main()
