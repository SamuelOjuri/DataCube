"""Local PBIX metadata inspection and explicitly selected Power BI reader audit.

Reading a PBIX never executes its M, DAX or connection strings. The reader audit
uses only the endpoint and reader credential selected by PG_* in the local .env.
"""
import hashlib
import json
from pathlib import Path
import re
import zipfile

from dotenv import dotenv_values
import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row

from manage import ROOT, inventory
from phase1 import VERSION, REPORT_SOURCES, catalog_supplement, save_new, seal_packet, utc_now

PBIX_SOURCES = ('vw_pipeline_forecast_monthly_12m_v1', 'vw_pipeline_forecast_project_v1',
                'vw_pipeline_budget_combo_chart_v1', 'vw_pipeline_budget_comparison_monthly_v1',
                'vw_weighted_enquiry_value_monthly_v1', 'vw_pipeline_smoothed_revenue_monthly_12m_v1',
                'vw_pipeline_smoothing_score_v1')


def pbix_metadata(path):
    try:
        from pbixray import PBIXRay
    except ImportError:
        raise RuntimeError('Install requirements-pbix.txt to inspect PBIX files') from None
    from importlib.metadata import version

    model = PBIXRay(str(path))
    result = {'file': path.name, 'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
              'reader_version': version('pbixray'), 'dax_executed': False,
              'source_contacted': False}
    for name in ('power_query', 'dax_measures', 'dax_columns', 'dax_tables',
                 'relationships', 'schema', 'tmschema_partitions', 'tmschema_tables',
                 'tmschema_roles', 'tmschema_table_permissions'):
        try:
            frame = getattr(model, name)
        except AttributeError:
            result[name] = {'status': 'not_supported_by_reader'}
        else:
            result[name] = json.loads(frame.to_json(orient='records', date_format='iso'))
    with zipfile.ZipFile(path) as archive:
        layout = json.loads(archive.read('Report/Layout').decode('utf-16-le'))
    result['layout'] = layout
    result['pages'] = [{'name': page.get('displayName'),
                        'visual_count': len(page.get('visualContainers', []))}
                       for page in layout.get('sections', [])]
    return result


def inspect_pbix(args):
    paths = sorted(args.input.glob('*.pbix')) if args.input.is_dir() else [args.input]
    if not paths or any(path.suffix.lower() != '.pbix' for path in paths):
        raise ValueError('Select a PBIX file or a directory containing PBIX reports')
    reports = [pbix_metadata(path) for path in paths]
    packet = seal_packet({'version': VERSION, 'captured_at': utc_now(), 'reports': reports,
        'scope': 'Offline model/visual definitions; cached data is not a same-snapshot reference answer'})
    save_new(args.output / 'pbix_inventory.json', packet)
    print(json.dumps({'reports': [{'file': r['file'], 'pages': r['pages'],
                                   'measures': len(r['dax_measures'])} for r in reports]}))


def reader_config(env_path=None):
    values = dotenv_values(env_path or ROOT / '.env')
    required = ('PG_ROLES', 'PG_USER', 'PG_USER_PASSWORD', 'PG_HOST', 'PG_PORT', 'PG_DATABASE')
    if any(not values.get(name) for name in required):
        raise ValueError('Power BI reader audit requires explicit PG_* settings including PG_USER_PASSWORD')
    role = values['PG_ROLES']
    if role != 'powerbi_reader':
        raise ValueError('This audit is restricted to the explicitly requested powerbi_reader role')
    pg_user = values['PG_USER'].split('.')
    if len(pg_user) != 2 or pg_user[0] != role or not re.fullmatch('[a-z0-9]+', pg_user[1]):
        raise ValueError('PG_USER must identify the reader and Supabase project')
    if not re.fullmatch(r'[a-z0-9-]+\.pooler\.supabase\.com', values['PG_HOST']):
        raise ValueError('Expected the supplied Supabase pooler endpoint')
    if values['PG_PORT'] != '5432' or values['PG_DATABASE'] != 'postgres':
        raise ValueError('Expected the supplied session pooler port and database')
    if values.get('PG_SERVER', values['PG_HOST'] + ':5432') != values['PG_HOST'] + ':5432':
        raise ValueError('PG_SERVER and PG_HOST/PG_PORT disagree')
    return make_conninfo(host=values['PG_HOST'], port=values['PG_PORT'],
        dbname=values['PG_DATABASE'], user=values['PG_USER'], password=values['PG_USER_PASSWORD']), role


def audit_reader(args):
    dsn, role = reader_config()
    with psycopg.connect(dsn, sslmode='require', connect_timeout=15,
                         application_name='datacube_phase1_powerbi_readonly', row_factory=dict_row) as conn:
        conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        conn.execute("SET LOCAL statement_timeout='60s'")
        conn.execute("SET LOCAL lock_timeout='5s'")
        conn.execute(sql.SQL('SET LOCAL ROLE {}').format(sql.Identifier(role)))
        attributes = conn.execute('SELECT rolname,rolsuper,rolbypassrls,rolcreaterole,rolcreatedb FROM pg_roles WHERE rolname=current_user').fetchone()
        if any(attributes[key] for key in ('rolsuper', 'rolbypassrls', 'rolcreaterole', 'rolcreatedb')):
            raise ValueError('Power BI role has privileged attributes; reader audit stopped')
        source_inventory = inventory(conn, include_cron=False)
        source_inventory['cron_status'] = 'not_inspected_under_reporting_role'
        supplement = catalog_supplement(conn)
        sources = {}
        for relation in sorted({r for relations in REPORT_SOURCES.values() for r in relations} | set(PBIX_SOURCES)):
            try:
                with conn.transaction():
                    count = conn.execute(sql.SQL('SELECT count(*) AS rows FROM public.{}')
                                         .format(sql.Identifier(relation))).fetchone()['rows']
                    plan = conn.execute(sql.SQL('EXPLAIN (FORMAT JSON) SELECT * FROM public.{} LIMIT 100')
                                        .format(sql.Identifier(relation))).fetchall()
                sources[relation] = {'status': 'readable', 'rows': count, 'plan': plan}
            except psycopg.Error as exc:
                sources[relation] = {'status': 'unavailable', 'sqlstate': exc.sqlstate}
    packet = seal_packet({'version': VERSION, 'captured_at': utc_now(),
        'scope': 'Live configured PG_* source under powerbi_reader; distinct from frozen TEST dataset',
        'transaction_read_only': True, 'role': attributes, 'inventory': source_inventory,
        'catalog_supplement': supplement, 'sources': sources})
    save_new(args.output / 'powerbi_reader_inventory.json', packet)
    unavailable = [name for name, source in sources.items() if source['status'] != 'readable']
    print(json.dumps({'role': role, 'readable_sources': len(sources) - len(unavailable),
                      'unavailable_sources': unavailable, 'status': 'partial' if unavailable else 'passed'}))
    return 2 if unavailable else 0
