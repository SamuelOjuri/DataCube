"""Local PBIX metadata inspection and explicitly selected Power BI reader audit.

Reading a PBIX never executes its M, DAX or connection strings. The reader audit
uses only the endpoint and reader credential selected by PG_* in the local .env.
"""
import hashlib
import csv
import json
from pathlib import Path
import re
import zipfile

from dotenv import dotenv_values
import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.rows import dict_row

from manage import ROOT, connect, inventory, target_config
from dataset import fingerprint, names, reference_version, verify_snapshot
from phase1 import (VERSION, POWER_BI_REFERENCES, REPORT_SOURCES, catalog_supplement,
                    load_packet, output_path, save_new, seal_packet, utc_now)

PBIX_SOURCES = ('vw_pipeline_forecast_monthly_12m_v1', 'vw_pipeline_forecast_project_v1',
                'vw_pipeline_budget_combo_chart_v1', 'vw_pipeline_budget_comparison_monthly_v1',
                'vw_weighted_enquiry_value_monthly_v1', 'vw_pipeline_smoothed_revenue_monthly_12m_v1',
                'vw_pipeline_smoothing_score_v1')


DAX_CONTEXT = """DEFINE
VAR __AsOf = IF(
    COUNTROWS(ALL('DC Context')) = 1,
    MAXX(ALL('DC Context'), 'DC Context'[as_of_date]),
    ERROR("Load exactly one frozen DC Context row"))
VAR __Month = DATE(YEAR(__AsOf), MONTH(__AsOf), 1)
"""


def dax_query(reference):
    if reference not in POWER_BI_REFERENCES:
        raise ValueError('Unsupported Power BI evaluation reference')
    if reference.endswith('_monthly'):
        if reference == 'invoice_monthly':
            rows = """VAR __Parents = SELECTCOLUMNS(ALL('DC Projects'), "id", 'DC Projects'[monday_id])
        VAR __Rows = FILTER(ALL('DC Subitems'),
            NOT ISBLANK('DC Subitems'[invoice_date]) &&
            'DC Subitems'[invoice_date] >= __Start && 'DC Subitems'[invoice_date] < __End &&
            'DC Subitems'[amount_invoiced] > 0 &&
            'DC Subitems'[parent_monday_id] IN __Parents)"""
            cells = """"invoice_rows", COALESCE(COUNTROWS(__Rows), 0),
            "projects", COALESCE(COUNTROWS(DISTINCT(SELECTCOLUMNS(__Rows, "id", 'DC Subitems'[parent_monday_id]))), 0),
            "amount", FORMAT(COALESCE(SUMX(__Rows, 'DC Subitems'[amount_invoiced]), 0), "0.00", "en-US")"""
            columns = '"month", [month], "invoice_rows", [invoice_rows], "projects", [projects], "amount", [amount]'
        else:
            enquiry = reference == 'enquiry_monthly'
            date_field = 'date_created' if enquiry else 'date_order_received'
            amount_field = 'new_enquiry_value' if enquiry else 'total_order_value'
            stages = '' if enquiry else """ &&
            'DC Projects'[pipeline_stage] IN {"Won - Open (Order Received)", "Won - Closed (Invoiced)", "Won Via Other Ref"}"""
            rows = f"""VAR __Rows = FILTER(ALL('DC Projects'),
            NOT ISBLANK('DC Projects'[{date_field}]) &&
            'DC Projects'[{date_field}] >= __Start && 'DC Projects'[{date_field}] < __End &&
            'DC Projects'[{amount_field}] > 0{stages})"""
            cells = f'''"projects", COALESCE(COUNTROWS(__Rows), 0),
            "amount", FORMAT(COALESCE(SUMX(__Rows, 'DC Projects'[{amount_field}]), 0), "0.00", "en-US")'''
            columns = '"month", [month], "projects", [projects], "amount", [amount]'
        return DAX_CONTEXT + f"""EVALUATE
SELECTCOLUMNS(
    GENERATE(GENERATESERIES(1, 12, 1),
        VAR __Start = EDATE(__Month, -[Value])
        VAR __End = EDATE(__Start, 1)
        {rows}
        RETURN ROW("month", FORMAT(__Start, "yyyy-MM-dd", "en-US"),
            {cells})),
    {columns})
ORDER BY [month]
"""
    years = 5 if reference.endswith('five_year') else 2
    positive = " && 'DC Projects'[gestation_period] > 0" if reference.startswith('gestation') else ''
    cohort = f"""VAR __Rows = FILTER(ALL('DC Projects'),
    NOT ISBLANK('DC Projects'[date_created]) &&
    'DC Projects'[date_created] >= EDATE(__AsOf, -{years * 12}){positive})
"""
    if reference.startswith('conversion'):
        result = """VAR __Eligible = COALESCE(COUNTROWS(__Rows), 0)
VAR __Wins = COALESCE(COUNTROWS(FILTER(__Rows, 'DC Projects'[pipeline_stage] = "Won - Closed (Invoiced)")), 0)
VAR __Rate = DIVIDE(__Wins, __Eligible)
EVALUATE ROW("eligible", __Eligible, "wins", __Wins,
    "rate", IF(ISBLANK(__Rate), BLANK(), FORMAT(ROUND(__Rate, 3), "0.000", "en-US")))
"""
    else:
        result = """VAR __Days = AVERAGEX(__Rows, 'DC Projects'[gestation_period])
EVALUATE ROW("projects", COALESCE(COUNTROWS(__Rows), 0),
    "days", IF(ISBLANK(__Days), BLANK(), FORMAT(__Days, "0.0000000000000000", "en-US")))
"""
    return DAX_CONTEXT + cohort + result


def m_query(server, database, query, types):
    quote = lambda text: '"' + text.replace('"', '""') + '"'
    columns = ', '.join('{' + quote(name) + ', ' + kind + '}' for name, kind in types.items())
    return f"""let
    Source = PostgreSQL.Database({quote(server)}, {quote(database)},
        [Query = {quote(query)}, CreateNavigationProperties = false]),
    Typed = Table.TransformColumnTypes(Source, {{{columns}}}, "en-US")
in
    Typed
"""


def package_files(artifacts, server, database):
    manifest = artifacts['manifest']
    schema, _, _ = names(manifest['dataset'])
    if reference_version(manifest) != '1.1.0':
        raise ValueError('The aligned export package requires reference contract 1.1.0')
    if not set(POWER_BI_REFERENCES) <= artifacts['expected'].keys():
        raise ValueError('Reference pack is missing required Power BI calculations')
    files = {
        'DC Projects.m': m_query(server, database,
            f'SELECT monday_id,pipeline_stage,date_created,date_order_received,new_enquiry_value,total_order_value,gestation_period FROM {schema}.reportable_projects',
            {'monday_id': 'type text', 'pipeline_stage': 'type text', 'date_created': 'type date',
             'date_order_received': 'type date', 'new_enquiry_value': 'Currency.Type',
             'total_order_value': 'Currency.Type', 'gestation_period': 'Int64.Type'}),
        'DC Subitems.m': m_query(server, database,
            f'SELECT monday_id,parent_monday_id,invoice_date,amount_invoiced FROM {schema}.subitems',
            {'monday_id': 'type text', 'parent_monday_id': 'type text',
             'invoice_date': 'type date', 'amount_invoiced': 'Currency.Type'}),
        'DC Context.m': m_query(server, database,
            f"""SELECT dataset,as_of_date,business_timezone,reference_contract_version,to_char(captured_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS snapshot_captured_at FROM {schema}.context""",
            {'dataset': 'type text', 'as_of_date': 'type date', 'business_timezone': 'type text',
             'reference_contract_version': 'type text', 'snapshot_captured_at': 'type text'}),
        'context.dax': """EVALUATE
SELECTCOLUMNS(ALL('DC Context'),
    "dataset", 'DC Context'[dataset],
    "as_of_date", FORMAT('DC Context'[as_of_date], "yyyy-MM-dd", "en-US"),
    "business_timezone", 'DC Context'[business_timezone],
    "reference_contract_version", 'DC Context'[reference_contract_version],
    "snapshot_captured_at", 'DC Context'[snapshot_captured_at])
""",
    }
    entries = []
    for reference in POWER_BI_REFERENCES:
        dax = dax_query(reference)
        files[reference + '.dax'] = dax
        entries.append({'reference': reference, 'report_name': '', 'report_version': '',
            'dax': dax, 'filters': 'Unfiltered evaluation query; fixed frozen context and reference-specific periods',
            'population': 'reportable', 'as_of_date': str(manifest['as_of_date']),
            'columns': artifacts['expected'][reference]['columns']})
    metadata = {'dataset': schema, 'manifest_sha256': fingerprint(manifest),
        'execution': {'engine': 'Power BI', 'model_refreshed_at': '', 'exported_at': ''},
        'comparisons': entries}
    files['export.json'] = json.dumps(metadata, indent=2) + '\n'
    files['instructions.txt'] = f"""DataCube reference contract 1.1.0 / {schema}

This is a separate TEST evaluation report, not a refresh of production reports.
No expected answer rows or credentials are included.

1. Use a dedicated TEST login provisioned by the database administrator with
   membership in {manifest['reader_role']}. The dataset reader is NOLOGIN.
   Do not use the answer-key owner/admin credential in Power BI and do not grant
   browser roles access. No reader login/password is created by this package.
2. In a separate Power BI Desktop file, create three blank Power Query queries
   named exactly DC Projects, DC Subitems and DC Context. Paste the corresponding
   .m files into Advanced Editor. Use Import mode and refresh all three queries.
   Disable automatic relationship detection; no relationships are required.
3. Execute each .dax query against that actual Power BI model, using DAX Studio
   or Power BI DAX Query View. Export each result as UTF-8 comma-delimited CSV
   with headers, named after the query: context.csv and the seven reference CSVs.
   DAX Studio bracketed headers are supported. Keep blank cells blank; do not
   replace them with zero. Do not edit results or round values in Excel.
4. Fill the actual report_name/report_version in each export.json comparison.
   Supply actual model_refreshed_at and exported_at as offset-aware ISO times.
   The loader reads alignment from context.csv, not hand-written metadata.
5. Run from the repository root, replacing PACKAGE_DIRECTORY and NEW_OUTPUT:
   .\\report.venv\\Scripts\\python.exe services\\bi_analyst\\tests\\evals\\manage.py phase1-powerbi --dataset {schema} --input PACKAGE_DIRECTORY --output NEW_OUTPUT

All seven results and context.csv are required for this package. Keep original
queries unchanged; changed report definitions require a separately reviewed
export. Matching evaluation results do not certify the existing production
Budget/Smoothing reports, source mirror accuracy, or a business-owner approval.
"""
    immutable = {name: hashlib.sha256(text.encode()).hexdigest() for name, text in files.items() if name != 'export.json'}
    files['package.json'] = json.dumps(seal_packet({'version': VERSION, 'dataset': schema, 'manifest_sha256': fingerprint(manifest),
        'status': 'awaiting_independent_Power_BI_execution', 'files_sha256': immutable}), indent=2) + '\n'
    return files


def prepare_package(args):
    destination = output_path(args.output)
    if destination.exists():
        raise ValueError('Use a new directory for the Power BI package')
    connection, identity = connect()
    with connection:
        connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        connection.execute("SET LOCAL statement_timeout='120s'")
        artifacts, _ = verify_snapshot(connection, identity, args.dataset)
    dsn, _ = target_config()
    params = conninfo_to_dict(dsn)
    server = params['host'] + ':' + params.get('port', '5432')
    files = package_files(artifacts, server, params.get('dbname', 'postgres'))
    destination.mkdir(parents=True, exist_ok=False)
    for name, text in files.items():
        with (destination / name).open('x', encoding='utf-8', newline='\n') as stream:
            stream.write(text)
    print(json.dumps({'dataset': args.dataset, 'files': len(files), 'output': str(destination),
                      'status': 'awaiting_independent_Power_BI_execution', 'answer_rows_exported': False}))


def read_csv(path, columns):
    try:
        with path.open(encoding='utf-8-sig', newline='') as stream:
            reader = csv.reader(stream, strict=True)
            header = next(reader, [])
            header = [name[1:-1] if name.startswith('[') and name.endswith(']') else name for name in header]
            if len(header) != len(set(header)) or set(header) != set(columns):
                raise ValueError('Power BI CSV headers differ: ' + path.name)
            rows = []
            for line, values in enumerate(reader, 2):
                if len(values) != len(header):
                    raise ValueError(f'Power BI CSV column count differs: {path.name}, line {line}')
                rows.append({name: value if value != '' else None for name, value in zip(header, values)})
    except csv.Error:
        raise ValueError('Invalid Power BI CSV syntax: ' + path.name) from None
    return rows


def load_exports(path):
    if path.is_file():
        return json.loads(path.read_text(encoding='utf-8'))
    package = load_packet(path / 'package.json')['payload']
    for name, expected_hash in package['files_sha256'].items():
        source = (path / name).resolve()
        if not source.is_relative_to(path.resolve()) or hashlib.sha256(source.read_bytes()).hexdigest() != expected_hash:
            raise ValueError('Power BI package source changed: ' + name)
    exports = json.loads((path / 'export.json').read_text(encoding='utf-8'))
    if any(exports.get(key) != package[key] for key in ('dataset', 'manifest_sha256')):
        raise ValueError('Export metadata does not belong to this Power BI package')
    context = read_csv(path / 'context.csv', ['dataset', 'as_of_date', 'business_timezone',
        'reference_contract_version', 'snapshot_captured_at'])
    if len(context) != 1:
        raise ValueError('Export exactly one actual Power BI context row')
    exports['alignment'] = {**context[0], 'source_kind': 'frozen_TEST', 'population': 'reportable'}
    entries = exports.get('comparisons')
    if (not isinstance(entries, list) or len(entries) != len(POWER_BI_REFERENCES)
            or any(not isinstance(entry, dict) for entry in entries)
            or {entry.get('reference') for entry in entries} != set(POWER_BI_REFERENCES)):
        raise ValueError('Power BI package requires all seven distinct reference exports')
    for entry in entries:
        reference = entry['reference']
        if entry.get('dax') != (path / (reference + '.dax')).read_text(encoding='utf-8'):
            raise ValueError('Power BI export DAX differs from the reviewed package')
        entry['rows'] = read_csv(path / (reference + '.csv'), [column['name'] for column in entry['columns']])
    return exports


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
