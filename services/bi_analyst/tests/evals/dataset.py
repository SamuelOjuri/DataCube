"""Create and verify immutable evaluation snapshots in TEST Supabase only."""
from __future__ import annotations

from collections import Counter
from datetime import date
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
import sys

from psycopg import sql, Error
from psycopg.types.json import Jsonb

from cases import cases
from manage import HERE, ROOT, connect, encode, inventory, write_json

TABLES = {
    'projects': 'SELECT * FROM public.projects',
    'reportable_projects': 'SELECT * FROM public.reportable_projects',
    'subitems': 'SELECT * FROM public.subitems',
    'hidden_items': 'SELECT * FROM public.hidden_items',
    'current_projects': 'SELECT * FROM public.current_projects',
    'current_subitems': 'SELECT * FROM public.current_subitems',
    'current_hidden_items': 'SELECT * FROM public.current_hidden_items',
    'project_reporting_classifications': 'SELECT * FROM public.project_reporting_classifications',
    'analysis_results': 'SELECT id,project_id,analysis_timestamp,expected_conversion_rate,expected_gestation_days,analysis_version,llm_model FROM public.analysis_results',
    'baseline_enquiry': 'SELECT * FROM public.vw_actual_enquiry_monthly_v1',
    'baseline_bookings': 'SELECT * FROM public.vw_actual_bookings_monthly_v1',
    'baseline_revenue': 'SELECT * FROM public.vw_actual_revenue_monthly_v1',
    'baseline_conversion': 'SELECT * FROM public.conversion_metrics',
    'baseline_conversion_recent': 'SELECT * FROM public.conversion_metrics_recent',
    'baseline_enquiry_chart': 'SELECT * FROM public.vw_enquiry_value_forecast_chart_v1',
    'lifecycle': """SELECT table_name,monday_id,blocked,monday_state,state_verified_at,
        state_evidence->>'parent_monday_id' AS observed_parent_id,
        state_evidence->>'transaction_values_verified'='true' AS transaction_values_verified,
        jsonb_typeof(state_evidence->'item'->'subitems')='array' AS membership_array_present,
        ARRAY(SELECT member->>'id' FROM jsonb_array_elements(CASE
          WHEN jsonb_typeof(state_evidence->'item'->'subitems')='array'
          THEN state_evidence->'item'->'subitems' ELSE '[]'::jsonb END) member
          WHERE coalesce(member->>'state','active')='active') AS observed_active_child_ids
        FROM public.monday_item_lifecycle""",
    'lifecycle_event_status': """SELECT board_id,item_id,status,payload->>'archive_policy' AS archive_policy,
        payload->>'cause' AS cause FROM public.monday_lifecycle_events""",
}


def names(dataset):
    if not re.fullmatch(r'bi_eval_[0-9]{8}_v[1-9][0-9]*', dataset) or len(dataset) > 43:
        raise ValueError('Dataset must be named bi_eval_YYYYMMDD_vN')
    return dataset, dataset + '_key', dataset + '_reader'


def json_value(value):
    return json.loads(json.dumps(value, default=encode))


def references(filename):
    content = (HERE / filename).read_text(encoding='utf-8')
    pieces = re.split(r'^-- name: ([a-z_]+)\s*$', content, flags=re.M)
    queries = dict(zip(pieces[1::2], (query.strip() for query in pieces[2::2])))
    if len(queries) != (len(pieces) - 1) // 2:
        raise ValueError('Duplicate reference query name')
    return queries


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, default=encode, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def source_hashes():
    return {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(HERE.iterdir())
            if p.suffix in ('.py', '.sql') and p.name not in ('test_dataset.py',)}


def table_signature(conn, schema, table):
    return conn.execute(sql.SQL("""SELECT count(*) AS rows,
      encode(sha256(convert_to(coalesce(string_agg(h,'' ORDER BY h),''),'UTF8')),'hex') AS sha256
      FROM (SELECT encode(sha256(convert_to(to_jsonb(t)::text,'UTF8')),'hex') AS h FROM {}.{} t) hashed""").format(
          sql.Identifier(schema), sql.Identifier(table))).fetchone()


def equivalent(actual, expected):
    if isinstance(expected, list):
        return isinstance(actual, list) and len(actual) == len(expected) and all(equivalent(a, e) for a, e in zip(actual, expected))
    if isinstance(expected, dict):
        return isinstance(actual, dict) and actual.keys() == expected.keys() and all(equivalent(actual[k], expected[k]) for k in expected)
    if actual is None or expected is None:
        return actual is expected
    if isinstance(actual,bool) or isinstance(expected,bool):
        return type(actual) is type(expected) and actual == expected
    if isinstance(actual,date):
        actual = str(actual)
    if isinstance(expected,date):
        expected = str(expected)
    if isinstance(actual, (Decimal, float, int)) or isinstance(expected, (Decimal, float, int)):
        try:
            return abs(Decimal(str(actual)) - Decimal(str(expected))) <= Decimal('0.000000001')
        except Exception:
            return actual == expected
    return actual == expected


def restore_result_types(result):
    """Decode NUMERIC answer cells without interpreting numeric-looking text IDs."""
    numeric_columns = {column['name'] for column in result['columns'] if column['type_oid']==1700}
    return {**result,'rows':[{key:Decimal(value) if key in numeric_columns and value is not None else value
                              for key,value in row.items()} for row in result['rows']]}


def set_path(conn, schema):
    conn.execute(sql.SQL('SET LOCAL search_path TO {}, pg_catalog').format(sql.Identifier(schema)))


def run_reference(conn,name,query):
    try:
        return conn.execute(query)
    except Error as exc:
        raise RuntimeError(f'Reference query {name} failed with SQLSTATE {exc.sqlstate}; server details suppressed') from None


def revoke_defaults(conn, schema):
    # Only objects in the newly created dataset schema are affected.
    roles = [r['rolname'] for r in conn.execute("SELECT rolname FROM pg_roles WHERE rolname IN ('anon','authenticated','service_role')")]
    for grantee in [None, *roles]:
        grantee_sql = sql.SQL('PUBLIC') if grantee is None else sql.Identifier(grantee)
        for obj in ('SCHEMA', 'ALL TABLES IN SCHEMA', 'ALL FUNCTIONS IN SCHEMA'):
            conn.execute(sql.SQL('REVOKE ALL ON {} {} FROM {}').format(sql.SQL(obj), sql.Identifier(schema), grantee_sql))


def seal(conn, schema, tables, reader=None):
    conn.execute(sql.SQL("""CREATE FUNCTION {}.reject_changes() RETURNS trigger LANGUAGE plpgsql SET search_path='' AS $$
    BEGIN RAISE EXCEPTION 'Frozen BI evaluation data: create a new dataset version instead' USING ERRCODE='55000'; END; $$""").format(sql.Identifier(schema)))
    for table in tables:
        relation = sql.SQL('{}.{}').format(sql.Identifier(schema), sql.Identifier(table))
        conn.execute(sql.SQL('ALTER TABLE {} ENABLE ROW LEVEL SECURITY').format(relation))
        conn.execute(sql.SQL('CREATE TRIGGER reject_changes BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON {} FOR EACH STATEMENT EXECUTE FUNCTION {}.reject_changes()').format(relation, sql.Identifier(schema)))
        if reader:
            conn.execute(sql.SQL('CREATE POLICY evaluation_read ON {} FOR SELECT TO {} USING (true)').format(relation, sql.Identifier(reader)))
            conn.execute(sql.SQL('GRANT SELECT ON {} TO {}').format(relation, sql.Identifier(reader)))
    revoke_defaults(conn, schema)
    if reader:
        conn.execute(sql.SQL('GRANT USAGE ON SCHEMA {} TO {}').format(sql.Identifier(schema), sql.Identifier(reader)))


def check_access(conn, data_schema, key_schema, reader, tables):
    checks = {}
    attrs = conn.execute('SELECT rolsuper,rolcreaterole,rolcreatedb,rolcanlogin,rolbypassrls FROM pg_roles WHERE rolname=%s', (reader,)).fetchone()
    checks['restricted_nologin_role'] = not any(attrs.values())
    checks['reader_can_read_dataset'] = all(conn.execute('SELECT has_table_privilege(%s,%s,\'SELECT\') AS allowed', (reader,f'{data_schema}.{t}')).fetchone()['allowed'] for t in tables)
    checks['reader_cannot_write_dataset'] = all(not conn.execute("SELECT has_table_privilege(%s,%s,'INSERT,UPDATE,DELETE,TRUNCATE,TRIGGER') AS allowed", (reader,f'{data_schema}.{t}')).fetchone()['allowed'] for t in tables)
    checks['reader_cannot_read_answer_key'] = not conn.execute("SELECT has_schema_privilege(%s,%s,'USAGE') AS allowed", (reader,key_schema)).fetchone()['allowed']
    checks['reader_cannot_read_source_projects'] = not conn.execute("SELECT has_table_privilege(%s,'public.projects','SELECT') AS allowed", (reader,)).fetchone()['allowed']
    for role in ('anon','authenticated','service_role'):
        if conn.execute('SELECT 1 FROM pg_roles WHERE rolname=%s', (role,)).fetchone():
            checks[f'{role}_cannot_access_dataset'] = not conn.execute("SELECT has_schema_privilege(%s,%s,'USAGE') AS allowed", (role,data_schema)).fetchone()['allowed']
            checks[f'{role}_cannot_access_key'] = not conn.execute("SELECT has_schema_privilege(%s,%s,'USAGE') AS allowed", (role,key_schema)).fetchone()['allowed']
    if not all(checks.values()):
        raise RuntimeError('Dataset access checks failed: ' + ', '.join(k for k,v in checks.items() if not v))
    return checks


def independent_checks(conn, expected):
    """Decimal/count checks in Python, independently of the reference SQL text."""
    projects = conn.execute('SELECT new_enquiry_value,total_order_value,total_amount_invoiced,date_created,pipeline_stage,gestation_period FROM reportable_projects').fetchall()
    as_of = conn.execute('SELECT as_of_date FROM context').fetchone()['as_of_date']
    checks = {}
    for reference, field in [('enquiry_total','new_enquiry_value'),('order_parent_total','total_order_value'),('invoice_parent_total','total_amount_invoiced')]:
        known = [p[field] for p in projects if p[field] is not None]
        checks[reference] = equivalent(expected[reference]['rows'], [{'projects':len(projects),'known_values':len(known),'amount':sum(known) if known else None}])
    for years, reference in [(5,'conversion_five_year'),(2,'conversion_two_year')]:
        try:
            cutoff = as_of.replace(year=as_of.year-years)
        except ValueError:
            cutoff = as_of.replace(year=as_of.year-years,day=28)
        cohort = [p for p in projects if p['date_created'] is not None and p['date_created'] >= cutoff]
        wins = sum(p['pipeline_stage']=='Won - Closed (Invoiced)' for p in cohort)
        from decimal import ROUND_HALF_UP
        ratio = (Decimal(wins)/Decimal(len(cohort))).quantize(Decimal('.001'),rounding=ROUND_HALF_UP) if cohort else None
        checks[reference] = equivalent(expected[reference]['rows'], [{'eligible':len(cohort),'wins':wins,'rate':ratio}])
        positive = [p['gestation_period'] for p in cohort if p['gestation_period'] is not None and p['gestation_period']>0]
        gestation_ref = 'gestation_five_year' if years==5 else 'gestation_two_year'
        checks[gestation_ref] = equivalent(expected[gestation_ref]['rows'], [{'projects':len(positive),'days':Decimal(sum(positive))/len(positive) if positive else None}])
    for case in cases():
        if case['kind']=='synthetic_edge':
            checks[case['reference']] = equivalent(expected[case['reference']]['rows'], case['manual_expected'])
    if not all(checks.values()):
        raise RuntimeError('Independent reference validation failed: ' + ', '.join(k for k,v in checks.items() if not v))
    return checks


def freeze(args):
    data_schema,key_schema,reader = names(args.dataset)
    conn,identity = connect()
    refs = {**references('reference.sql'),**references('fixture_reference.sql')}
    question_set = cases()
    if len(question_set)!=70 or len(refs)!=50:
        raise RuntimeError('Expected 70 scenarios and 50 independent reference queries')
    with conn:
        conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
        conn.execute("SET LOCAL statement_timeout='120s'")
        conn.execute("SET LOCAL lock_timeout='5s'")
        conn.execute("SET LOCAL timezone='Europe/London'")
        conn.execute('SELECT pg_advisory_xact_lock(hashtext(%s))', (data_schema,))
        for identifier in (data_schema,key_schema):
            if conn.execute('SELECT 1 FROM pg_namespace WHERE nspname=%s',(identifier,)).fetchone():
                raise ValueError('Dataset already exists; verify it or choose a new version. Nothing is overwritten.')
        as_of = args.as_of or conn.execute('SELECT current_date AS day').fetchone()['day']
        server_day = conn.execute('SELECT current_date AS day').fetchone()['day']
        if as_of != server_day:
            raise ValueError('Initial capture date must equal the database day in Europe/London, so captured CURRENT_DATE views share the reference date')
        source_inventory = inventory(conn)
        source_inventory['target'] = identity
        manifest = {'dataset':data_schema,'target':identity,'as_of_date':as_of,
                    'business_timezone':'Europe/London','timezone_status':'evaluation_assumption_pending_business_confirmation',
                    'captured_at':source_inventory['server'][0]['captured_at'],
                    'transaction_snapshot':conn.execute('SELECT txid_current_snapshot()::text AS snapshot').fetchone()['snapshot'],
                    'source_kind':'user_created_TEST_database_clone','production_was_contacted':False,
                    'backup_timestamp':'not_known_from_database; owner_to_record',
                    'certification':'reference_baseline_created; business/source/Power_BI_review_pending',
                    'population':'frozen deployed reportable_projects; current_* retained separately for coverage evidence',
                    'currency_tax_fiscal_calendar':'not certified; values retain source units and no fiscal questions are answered',
                    'schemas':{'data':data_schema,'answer_key':key_schema},'reader_role':reader,
                    'question_counts':dict(Counter(c['kind'] for c in question_set)),
                    'source_hashes':source_hashes(),'questions_sha256':fingerprint(question_set)}
        for schema in (data_schema,key_schema):
            conn.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
            revoke_defaults(conn,schema)
        if conn.execute('SELECT 1 FROM pg_roles WHERE rolname=%s',(reader,)).fetchone():
            raise ValueError('Version-specific reader role already exists; choose a fresh dataset version')
        conn.execute(sql.SQL('CREATE ROLE {} NOLOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS').format(sql.Identifier(reader)))
        conn.execute(sql.SQL('GRANT {} TO CURRENT_USER').format(sql.Identifier(reader)))
        set_path(conn,data_schema)
        conn.execute('CREATE TABLE context (dataset text PRIMARY KEY,as_of_date date NOT NULL,business_timezone text NOT NULL,captured_at timestamptz NOT NULL)')
        conn.execute('INSERT INTO context VALUES (%s,%s,%s,%s)',(data_schema,as_of,'Europe/London',manifest['captured_at']))
        for name,query in TABLES.items():
            conn.execute(sql.SQL('CREATE TABLE {}.{} AS {}').format(sql.Identifier(data_schema),sql.Identifier(name),sql.SQL(query)))
            if name in ('projects','reportable_projects','subitems','hidden_items','current_projects','current_subitems','current_hidden_items'):
                conn.execute(sql.SQL('ALTER TABLE {} ADD PRIMARY KEY (monday_id)').format(sql.Identifier(name)))
        conn.execute('CREATE INDEX ON subitems (parent_monday_id)')
        conn.execute('CREATE INDEX ON subitems (hidden_item_id)')
        conn.execute('CREATE INDEX ON lifecycle (table_name,monday_id)')
        conn.execute((HERE/'fixtures.sql').read_text(encoding='utf-8'),prepare=False)
        tables = ['context',*TABLES,'fixture_projects','fixture_subitems','fixture_hidden_items']
        manifest['table_signatures'] = {table:table_signature(conn,data_schema,table) for table in tables}
        print('Captured and fingerprinted evaluation tables.',flush=True)
        expected = {}
        for name,query in refs.items():
            cur = run_reference(conn,name,query)
            expected[name] = {'sql':query,'sql_sha256':hashlib.sha256(query.encode()).hexdigest(),
                              'columns':[{'name':d.name,'type_oid':d.type_code} for d in cur.description],
                              'rows':cur.fetchall()}
        diagnostics = {name:run_reference(conn,name,query).fetchall() for name,query in references('diagnostics.sql').items()}
        independent = independent_checks(conn,expected)
        print('Reference results and independent checks passed.',flush=True)
        freshness = {
            'ingestion':conn.execute("SELECT board_name,max(completed_at) FILTER(WHERE status='completed') AS latest_completed_at FROM public.sync_log GROUP BY board_name ORDER BY board_name").fetchall(),
            'scheduled_jobs':conn.execute("SELECT job_id,max(finished_at) FILTER(WHERE outcome='succeeded') AS latest_succeeded_at,max(finished_at) AS latest_finished_at FROM public.worker_job_runs GROUP BY job_id ORDER BY job_id").fetchall(),
            'forecast_snapshots':conn.execute('SELECT min(snapshot_date) AS first_date,max(snapshot_date) AS latest_date,count(*) AS rows FROM public.pipeline_forecast_snapshot').fetchall(),
            'caveat':'Copied operational evidence is historical. Per-writer rollup/refresh certification and live Render flags still require review.'}
        # Capture query plans without timing-dependent EXPLAIN ANALYZE results.
        plans = {name:conn.execute('EXPLAIN (FORMAT JSON) '+refs[name]).fetchall()
                 for name in ('enquiry_monthly','invoice_monthly','conversion_category')}
        seal(conn,data_schema,tables,reader)
        access = check_access(conn,data_schema,key_schema,reader,tables)
        conn.execute(sql.SQL('CREATE TABLE {}.artifacts (name text PRIMARY KEY,payload jsonb NOT NULL)').format(sql.Identifier(key_schema)))
        artifacts = {'manifest':manifest,'inventory':source_inventory,'questions':question_set,'expected':expected,
                     'diagnostics':diagnostics,'freshness':freshness,'query_plans':plans,
                     'validation':{'independent_checks':independent,'access_checks':access}}
        for name,payload in artifacts.items():
            conn.execute(sql.SQL('INSERT INTO {}.artifacts VALUES (%s,%s)').format(sql.Identifier(key_schema)),(name,Jsonb(json_value(payload))))
        seal(conn,key_schema,['artifacts'])
        # Execute all reference queries through the restricted reader, too.
        conn.execute(sql.SQL('SET LOCAL ROLE {}').format(sql.Identifier(reader)))
        for name,query in refs.items():
            if not equivalent(conn.execute(query).fetchall(),expected[name]['rows']):
                raise RuntimeError('Restricted-reader reference results differ: '+name)
        conn.execute('RESET ROLE')
    # Financial results remain inside the restricted answer-key schema.
    write_json(args.output/'manifest.json',manifest)
    write_json(args.output/'validation.json',artifacts['validation'])
    write_json(args.output/'questions.json',question_set)
    print(json.dumps({'created':data_schema,'answer_key':key_schema,'scenarios':len(question_set),
                      'reference_queries':len(refs),'snapshot_tables':len(tables),
                      'independent_checks':len(independent),'access_checks':len(access),
                      'revenue_definition_differing_months':len(diagnostics['revenue_definition_difference']),
                      'certification':'pending_business_source_and_Power_BI_review'}))


def verify(args):
    data_schema,key_schema,reader = names(args.dataset)
    conn,identity = connect()
    with conn:
        conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        conn.execute("SET LOCAL statement_timeout='120s'")
        conn.execute("SET LOCAL timezone='Europe/London'")
        artifacts = {r['name']:r['payload'] for r in conn.execute(sql.SQL('SELECT name,payload FROM {}.artifacts').format(sql.Identifier(key_schema)))}
        manifest = artifacts['manifest']
        if manifest['target']['project_ref']!=identity['project_ref']:
            raise RuntimeError('Manifest belongs to a different test project')
        signatures = {table:table_signature(conn,data_schema,table) for table in manifest['table_signatures']}
        if signatures!=manifest['table_signatures']:
            raise RuntimeError('Frozen dataset content changed')
        access = check_access(conn,data_schema,key_schema,reader,list(signatures))
        set_path(conn,data_schema)
        expected_results = {name:restore_result_types(result) for name,result in artifacts['expected'].items()}
        independent = independent_checks(conn,expected_results)
        conn.execute(sql.SQL('SET LOCAL ROLE {}').format(sql.Identifier(reader)))
        refs = {**references('reference.sql'),**references('fixture_reference.sql')}
        if fingerprint(cases())!=manifest['questions_sha256']:
            raise RuntimeError('Question definitions changed; create a reviewed new dataset version')
        for name,query in refs.items():
            expected = expected_results[name]
            if hashlib.sha256(query.encode()).hexdigest()!=expected['sql_sha256']:
                raise RuntimeError('Reference SQL changed: '+name)
            cur = conn.execute(query)
            if [{'name':d.name,'type_oid':d.type_code} for d in cur.description]!=expected['columns']:
                raise RuntimeError('Reference result columns changed: '+name)
            if not equivalent(cur.fetchall(),expected['rows']):
                raise RuntimeError('Reference result mismatch: '+name)
    result = {'dataset':data_schema,'verified_tables':len(signatures),'reference_queries_passed':len(refs),
              'independent_checks_passed':len(independent),'access_checks_passed':len(access),
              'application_conversations_executed':False,'certification':manifest['certification']}
    write_json(args.output/'verification.json',result)
    print(json.dumps(result))


def report(args):
    """Export aggregate review findings, without financial result rows or identifiers."""
    _,key_schema,_ = names(args.dataset)
    conn,_ = connect()
    with conn:
        conn.execute('SET TRANSACTION READ ONLY')
        artifacts = {r['name']:r['payload'] for r in conn.execute(sql.SQL('SELECT name,payload FROM {}.artifacts').format(sql.Identifier(key_schema)))}
    diagnostics = artifacts['diagnostics'].copy()
    diagnostics['revenue_definition_difference'] = {
        'differing_months':len(diagnostics['revenue_definition_difference']),
        'detail_location':f'{key_schema}.artifacts / diagnostics'}
    summary = {'manifest':artifacts['manifest'],'diagnostics':diagnostics,
               'freshness':artifacts['freshness'],'validation':artifacts['validation']}
    write_json(args.output/'review_summary.json',summary)
    print(json.dumps(summary,default=encode,ensure_ascii=True))


def probe(args):
    """Prove denied access and mutation guards with savepoints and zero-row UPDATEs."""
    data_schema,key_schema,reader = names(args.dataset)
    conn,identity = connect()
    checks = {}
    with conn:
        conn.execute("SET LOCAL statement_timeout='15s'")
        manifest = conn.execute(sql.SQL("SELECT payload FROM {}.artifacts WHERE name='manifest'").format(sql.Identifier(key_schema))).fetchone()['payload']
        if manifest['target']['project_ref'] != identity['project_ref']:
            raise RuntimeError('Manifest target mismatch')
        def denied(name,statement,state):
            try:
                with conn.transaction():
                    conn.execute(statement)
                    raise RuntimeError('Expected protection was absent: '+name)
            except Error as exc:
                if exc.sqlstate != state:
                    raise RuntimeError(f'Unexpected protection error for {name}: {exc.sqlstate}') from None
                checks[name] = True
        conn.execute(sql.SQL('SET LOCAL ROLE {}').format(sql.Identifier(reader)))
        denied('reader_denied_source_projects',sql.SQL('SELECT monday_id FROM public.projects LIMIT 0'),'42501')
        denied('reader_denied_answer_key',sql.SQL('SELECT name FROM {}.artifacts LIMIT 0').format(sql.Identifier(key_schema)),'42501')
        denied('reader_denied_update',sql.SQL('UPDATE {}.context SET dataset=dataset WHERE false').format(sql.Identifier(data_schema)),'42501')
        conn.execute('RESET ROLE')
        denied('owner_mutation_guard_data',sql.SQL('UPDATE {}.context SET dataset=dataset WHERE false').format(sql.Identifier(data_schema)),'55000')
        denied('owner_mutation_guard_key',sql.SQL('UPDATE {}.artifacts SET name=name WHERE false').format(sql.Identifier(key_schema)),'55000')
    write_json(args.output/'protection_probes.json',checks)
    print(json.dumps({'dataset':data_schema,'protection_probes':checks}))
