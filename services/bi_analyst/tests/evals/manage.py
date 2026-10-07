"""Standalone Phase 1 evaluation tooling. Never imports DataCube's ETL package."""
from __future__ import annotations

import argparse
from datetime import date, datetime
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
import sys
from urllib.parse import urlparse
from uuid import UUID

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict
from psycopg.rows import dict_row
from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[4]
HERE = Path(__file__).resolve().parent


def encode(value):
    if isinstance(value, (date, datetime, Decimal, UUID)):
        return str(value)
    raise TypeError(type(value).__name__)


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, default=encode, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')


def target_config(env_path=None):
    values = dotenv_values(env_path or ROOT / '.env')
    missing = [key for key in ('TEST_SUPABASE_NAME', 'TEST_SUPABASE_URL', 'TEST_SUPABASE_DB_URL') if not values.get(key)]
    if missing:
        raise ValueError('Missing test configuration: ' + ', '.join(missing))
    project = urlparse(values['TEST_SUPABASE_URL'])
    if project.scheme != 'https' or not re.fullmatch(r'[a-z0-9]+\.supabase\.co', project.hostname or ''):
        raise ValueError('TEST_SUPABASE_URL must identify a hosted Supabase test project')
    ref = project.hostname.split('.')[0]
    params = conninfo_to_dict(values['TEST_SUPABASE_DB_URL'])
    host, user = params.get('host', ''), params.get('user', '')
    direct = host == f'db.{ref}.supabase.co'
    pooler = host.endswith('.pooler.supabase.com') and user.endswith('.' + ref)
    if not (direct or pooler):
        raise ValueError('Test database connection does not match TEST_SUPABASE_URL')
    production_host = urlparse(values.get('SUPABASE_URL') or '').hostname
    if production_host == project.hostname:
        raise ValueError('Refusing the production Supabase project')
    if values.get('SUPABASE_DB_URL'):
        production = conninfo_to_dict(values['SUPABASE_DB_URL'])
        if (host, user, params.get('dbname', 'postgres')) == (production.get('host'), production.get('user'), production.get('dbname', 'postgres')):
            raise ValueError('Test and production database endpoints are identical')
    if 'test' not in values['TEST_SUPABASE_NAME'].lower():
        raise ValueError('Test project name must explicitly identify TEST')
    return values['TEST_SUPABASE_DB_URL'], {'name': values['TEST_SUPABASE_NAME'], 'project_ref': ref}


def connect():
    dsn, identity = target_config()
    conn = psycopg.connect(dsn, sslmode='require', connect_timeout=15,
                           application_name='datacube_bi_phase1_evaluation', row_factory=dict_row)
    return conn, identity


def inventory(conn, *, include_cron=True):
    result = {}
    queries = {
        'server': "SELECT current_database() AS database, current_user AS role, version(), current_timestamp AS captured_at, current_setting('TimeZone') AS timezone, current_setting('max_connections') AS max_connections",
        'relations': "SELECT n.nspname AS schema, c.relname AS name, c.relkind AS kind, c.reltuples::bigint AS estimated_rows, c.relrowsecurity AS rls, c.reloptions, pg_get_userbyid(c.relowner) AS owner FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind IN ('r','v','m','p') ORDER BY c.relname",
        'columns': "SELECT table_name,column_name,data_type,udt_name,is_nullable FROM information_schema.columns WHERE table_schema='public' ORDER BY table_name,ordinal_position",
        'views': "SELECT c.relname AS name, c.relkind AS kind, pg_get_viewdef(c.oid,true) AS definition FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind IN ('v','m') ORDER BY c.relname",
        'functions': "SELECT p.proname AS name, pg_get_function_identity_arguments(p.oid) AS arguments, p.prosecdef AS security_definer, pg_get_functiondef(p.oid) AS definition FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public' AND p.prokind='f' AND (p.proname LIKE '%report%' OR p.proname LIKE '%coverage%' OR p.proname LIKE '%refresh%' OR p.proname LIKE '%snapshot%' OR p.proname LIKE '%placeholder%') ORDER BY p.proname",
        'extensions': "SELECT extname,extversion FROM pg_extension ORDER BY extname",
        'triggers': "SELECT c.relname AS table_name,t.tgname,pg_get_triggerdef(t.oid) AS definition FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND NOT t.tgisinternal ORDER BY c.relname,t.tgname",
        'grants': "SELECT table_name,grantee,privilege_type FROM information_schema.table_privileges WHERE table_schema='public' ORDER BY table_name,grantee,privilege_type",
        'indexes': "SELECT tablename,indexname,indexdef FROM pg_indexes WHERE schemaname='public' ORDER BY tablename,indexname",
    }
    for name, query in queries.items():
        result[name] = conn.execute(query).fetchall()
    result['cron_job_count'] = None
    if include_cron and conn.execute("SELECT to_regclass('cron.job') AS relation").fetchone()['relation']:
        result['cron_job_count'] = conn.execute('SELECT count(*) AS n FROM cron.job').fetchone()['n']
    return result


def inspect(args):
    conn, identity = connect()
    with conn:
        conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        conn.execute("SET LOCAL statement_timeout='60s'")
        result = inventory(conn)
        result['target'] = identity
    write_json(args.output / 'inventory.json', result)
    print(json.dumps({'target': identity, 'relations': len(result['relations']),
                      'extensions': [r['extname'] for r in result['extensions']],
                      'cron_job_count': result['cron_job_count'], 'inventory': str(args.output / 'inventory.json')}, ensure_ascii=False))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['inspect','freeze','verify','report','probe',
                                          'phase1-capture','phase1-powerbi','phase1-review',
                                          'phase1-pbix','phase1-reader'])
    parser.add_argument('--output', type=Path, default=ROOT / 'outputs/bi_analyst_evals/preflight')
    parser.add_argument('--dataset', default='bi_eval_20261007_v1')
    parser.add_argument('--as-of', type=date.fromisoformat)
    parser.add_argument('--sample-size', type=int, default=3)
    parser.add_argument('--input', type=Path, help='Power BI export JSON for phase1-powerbi')
    parser.add_argument('--evidence', type=Path, help='Evidence packet for phase1-review')
    parser.add_argument('--review', type=Path, help='Owner review JSON for phase1-review')
    parser.add_argument('--power-bi', type=Path, help='Optional comparison packet for phase1-review')
    parser.add_argument('--pbix', type=Path, help='Optional PBIX inventory packet to bind into capture')
    parser.add_argument('--reader-audit', type=Path, help='Optional PG_* reader inventory packet to bind into capture')
    args = parser.parse_args()
    if args.command in ('phase1-powerbi', 'phase1-pbix') and not args.input:
        parser.error(args.command + ' requires --input')
    if args.command == 'phase1-review' and (not args.evidence or not args.review):
        parser.error('phase1-review requires --evidence and --review')
    if args.command.startswith('phase1-') and args.output == ROOT / 'outputs/bi_analyst_evals/preflight':
        parser.error('Phase 1 commands require an explicit --output inside outputs/')
    try:
        if args.command.startswith('phase1-'):
            from phase1 import capture, power_bi, review_command
            from powerbi import inspect_pbix, audit_reader
            return {'phase1-capture':capture, 'phase1-powerbi':power_bi,
                    'phase1-review':review_command, 'phase1-pbix':inspect_pbix,
                    'phase1-reader':audit_reader}[args.command](args) or 0
        elif args.command == 'inspect':
            inspect(args)
        else:
            from dataset import freeze,verify,report,probe
            if args.output == ROOT / 'outputs/bi_analyst_evals/preflight':
                args.output = ROOT / 'outputs/bi_analyst_evals' / args.dataset
            {'freeze':freeze,'verify':verify,'report':report,'probe':probe}[args.command](args)
    except psycopg.Error as exc:
        # Database errors can contain connection details and business values.
        message = str(exc).lower()
        diagnosis = [label for pattern, label in (
            ('password authentication failed', 'password_authentication_failed'),
            ('could not translate host name', 'dns_resolution_failed'),
            ('failed to resolve host', 'dns_resolution_failed'),
            ('network is unreachable', 'network_unreachable'),
            ('connection refused', 'connection_refused'),
            ('timeout', 'connection_timeout'),
            ('tenant or user not found', 'pooler_tenant_or_user_not_found'),
            ('ssl', 'ssl_mentioned'),
            ('permission denied', 'permission_denied'),
            ('no such host', 'dns_resolution_failed'),
        ) if pattern in message]
        safe_message = str(exc)
        if isinstance(exc, psycopg.OperationalError):
            for key in ('TEST_SUPABASE_DB_URL', 'SUPABASE_DB_URL'):
                raw = dotenv_values(ROOT / '.env').get(key)
                if raw:
                    for value in sorted([raw, *conninfo_to_dict(raw).values()], key=len, reverse=True):
                        if len(value) > 2:
                            safe_message = safe_message.replace(value, '[redacted]')
        else:
            safe_message = 'Server message suppressed'
        print(json.dumps({'error': type(exc).__name__, 'sqlstate': exc.sqlstate,
                          'diagnosis': diagnosis,
                          'connection_detail': safe_message[:700],
                          'detail': 'Database operation failed; credentials and server messages suppressed.'}), file=sys.stderr)
        return 1
    except (ValueError, RuntimeError, OSError) as exc:
        print(json.dumps({'error': type(exc).__name__, 'detail': str(exc)}), file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
