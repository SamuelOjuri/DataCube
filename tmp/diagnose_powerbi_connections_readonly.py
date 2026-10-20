"""Inspect connection use and smoothing sources; never terminate sessions or change settings."""
from datetime import datetime, timezone
import json
from pathlib import Path
from urllib.parse import urlparse

from dotenv import dotenv_values
import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.rows import dict_row

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'outputs/powerbi_refresh_diagnosis_20261013'
SOURCES = ('vw_pipeline_forecast_monthly_12m_v1',
           'vw_pipeline_smoothed_revenue_monthly_12m_v1',
           'vw_pipeline_smoothing_score_v1')


def error_record(exc):
    return {'sqlstate': exc.sqlstate,
            'message': exc.diag.message_primary or 'Connection failed',
            'session_pool_limit_observed': 'max clients' in str(exc).lower()}


def main():
    values = dotenv_values(ROOT / '.env')
    reader_user = values['PG_USER']
    reader_role, ref = reader_user.split('.', 1)
    if reader_role != 'powerbi_reader':
        raise ValueError('Expected the configured Power BI reader')
    admin_dsn = values['SUPABASE_DB_URL']
    admin_params = conninfo_to_dict(admin_dsn)
    admin_host, admin_user = admin_params.get('host', ''), admin_params.get('user', '')
    if not (admin_host == f'db.{ref}.supabase.co' or
            (admin_host.endswith('.pooler.supabase.com') and admin_user.endswith('.' + ref))):
        raise ValueError('Owner diagnostic must target the same project as Power BI')
    if urlparse(values['SUPABASE_URL']).hostname != ref + '.supabase.co':
        raise ValueError('Power BI reader and configured production project differ')
    result = {'captured_at_utc': datetime.now(timezone.utc).isoformat(),
              'scope': 'Read-only live activity snapshot plus one direct reader connection',
              'reader_endpoint_mode': 'session pooler' if values['PG_PORT']=='5432' else 'other'}
    try:
        with psycopg.connect(admin_dsn, sslmode='require', connect_timeout=15,
                             application_name='datacube_powerbi_connections_readonly',
                             row_factory=dict_row) as conn:
            conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
            conn.execute("SET LOCAL statement_timeout='15s'")
            result['observer'] = conn.execute('''SELECT current_user,
                current_setting('transaction_read_only') AS transaction_read_only,
                current_setting('max_connections') AS database_max_connections,
                pg_has_role(current_user,'pg_read_all_stats','MEMBER') AS full_activity_visibility
            ''').fetchone()
            result['connections_by_role_application_state'] = conn.execute('''
                SELECT usename,application_name,coalesce(state,'not_visible') AS state,count(*) AS sessions
                FROM pg_stat_activity WHERE backend_type='client backend'
                GROUP BY usename,application_name,state ORDER BY usename,application_name,state
            ''').fetchall()
            result['reader_sessions_before_probe'] = conn.execute('''
                SELECT pid,application_name,state,wait_event_type,wait_event,
                    round(extract(epoch FROM clock_timestamp()-backend_start)) AS connection_age_seconds,
                    round(extract(epoch FROM clock_timestamp()-state_change)) AS state_age_seconds,
                    round(extract(epoch FROM clock_timestamp()-xact_start)) AS transaction_age_seconds
                FROM pg_stat_activity WHERE usename=%s AND backend_type='client backend'
                ORDER BY backend_start,pid
            ''', (reader_role,)).fetchall()
            installed = conn.execute('''WITH state_relations AS (
                SELECT c.oid,c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                WHERE n.nspname='analyst_state'
            ) SELECT EXISTS(SELECT FROM pg_attribute a JOIN state_relations c ON c.oid=a.attrelid
                WHERE c.relname='sessions' AND a.attname='idle_timeout_seconds' AND NOT a.attisdropped) AS sessions,
                EXISTS(SELECT FROM state_relations WHERE relname='result_feedback') AS presentation,
                EXISTS(SELECT FROM state_relations WHERE relname='workflow_jobs') AS workflow,
                EXISTS(SELECT FROM state_relations WHERE relname='external_identities') AS auth
            ''').fetchone()
            audit_file = next(('permissions_' + version + '.sql' for version in
                               ('sessions','presentation','workflow','auth') if installed[version]),
                              'permissions.sql')
            with conn.transaction():
                result['analyst_audit'] = {'query': audit_file, 'violations': conn.execute(
                    (ROOT / 'services/bi_analyst/bi_analyst' / audit_file).read_text(encoding='utf-8')).fetchall()}
            conn.rollback()
    except psycopg.Error as exc:
        result['observer_error'] = error_record(exc)

    reader_dsn = make_conninfo(host=values['PG_HOST'],port=values['PG_PORT'],
                              dbname=values['PG_DATABASE'],user=reader_user,
                              password=values['PG_USER_PASSWORD'])
    try:
        with psycopg.connect(reader_dsn,sslmode='require',connect_timeout=15,
                             application_name='datacube_smoothing_sources_readonly',row_factory=dict_row) as conn:
            conn.execute('SET TRANSACTION READ ONLY')
            conn.execute("SET LOCAL statement_timeout='20s'")
            result['reader_connection'] = conn.execute('''SELECT current_user,
                current_setting('transaction_read_only') AS transaction_read_only''').fetchone()
            result['smoothing_sources'] = {}
            for source in SOURCES:
                try:
                    with conn.transaction():
                        conn.execute(sql.SQL('SELECT 1 FROM public.{} LIMIT 1').format(sql.Identifier(source))).fetchone()
                    result['smoothing_sources'][source] = 'readable'
                except psycopg.Error as exc:
                    result['smoothing_sources'][source] = error_record(exc)
            conn.rollback()
    except psycopg.Error as exc:
        result['reader_connection_error'] = error_record(exc)
    OUT.mkdir(parents=True,exist_ok=True)
    (OUT / 'smoothing_connection_diagnosis.json').write_text(
        json.dumps(result,indent=2,default=str)+'\n',encoding='utf-8')
    print(json.dumps(result,indent=2,default=str))


if __name__ == '__main__':
    main()
