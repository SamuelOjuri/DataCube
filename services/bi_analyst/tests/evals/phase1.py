"""Read-only Phase 1 evidence and explicit owner review, isolated from ETL startup.

Evidence is about a TEST clone. Review records are attestations, never inferred
production observations. No command repairs data or rewrites the sealed key.
"""
from __future__ import annotations

import ast
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path

from psycopg import sql

from dataset import (equivalent, fingerprint, json_value, references, reference_version,
                     set_path, verify_snapshot)
from manage import ROOT, connect, encode, inventory

VERSION = 1
REPORT_POPULATION = 'reportable'
METRICS = ('enquiry', 'order', 'invoice', 'conversion', 'gestation')
POWER_BI_REFERENCES = ('enquiry_monthly', 'order_monthly', 'invoice_monthly',
                     'conversion_five_year', 'conversion_two_year',
                     'gestation_five_year', 'gestation_two_year')
DECISIONS = {
    'backup_timestamp': 'Platform owner',
    'access_scope': 'Platform/business owner',
    'currency': 'Finance owner',
    'tax_basis': 'Finance owner',
    'business_timezone': 'Business owner',
    'fiscal_calendar': 'Finance owner',
    'current_period_behaviour': 'BI/business owner',
    'provider_data_handling': 'Platform/business owner',
    'retention': 'Platform/business owner',
    'historical_classifications': 'BI/business owner',
    'report_population': 'BI/business owner',
    'writer_source_precedence': 'BI/platform owner',
    'question_and_answer_review': 'Independent BI reviewer',
    'performance_targets': 'Platform/business owner',
    'connection_budget': 'Platform owner',
}
ISSUES = {
    'revenue_definition': 'Reconcile restored revenue SQL, intended filters and Power BI DAX.',
    'invoice_rollup': 'Reconcile stored parent invoices against verified current child membership.',
    'enquiry_rollup': 'Review exact-reason child formula and Open-only current-membership sums.',
    'gestation_fallback': 'Verify stored actual and source/fallback rules for date differences.',
    'archive_coverage': 'Review lifecycle/source findings without excluding retained archived projects or making active-only rollout a reportable-population prerequisite.',
    'relationships': 'Resolve or label missing links and verify repeated source contributions.',
    'classification': 'Verify effective reviewed placeholder exclusions, retained archived projects, held/unreviewed inclusion and automatic re-entry after meaningful changes.',
    'writer_conflict': 'Prove deployed writers share an agreed source-precedence contract.',
    'report_reader_access': 'Resolve or explicitly scope the observed Power BI reader permission failures.',
}
WRITERS = {
    'ordinary_sync': ('src/database/sync_service.py',
        ['_refresh_project_order_invoice_rollups', '_rollup_order_values_from_subitems',
         '_rollup_invoice_totals_from_subitems', '_transform_for_projects_table']),
    'comparison': ('scripts/order_value_monday_compare.py', ['project_projection']),
    'archive': ('src/services/monday_archive.py', ['enabled', 'refresh_current_values', 'refresh_parents']),
    'maintenance': ('src/tasks/postgres_maintenance.py',
        ['refresh_materialized_views', 'create_pipeline_forecast_snapshot',
         'create_pipeline_smoothing_forecast_snapshot']),
    'webhook_entrypoint': ('src/webhooks/webhook_server.py', []),
    'scheduler_entrypoint': ('src/api/app.py', []),
}
FLAGS = ('MONDAY_ARCHIVE_ENABLED', 'MONDAY_ARCHIVE_REPORTING_ENABLED',
         'MONDAY_LIFECYCLE_ENABLED', 'SCHEDULER_ENABLED')
REPORT_SOURCES = {
    'enquiry_monthly': ['vw_actual_enquiry_monthly_v1', 'reportable_projects'],
    'order_monthly': ['vw_actual_bookings_monthly_v1', 'reportable_projects'],
    'invoice_monthly': ['vw_actual_revenue_monthly_v1', 'reportable_projects'],
    'conversion_five_year': ['conversion_metrics', 'reportable_projects'],
    'conversion_two_year': ['conversion_metrics_recent', 'reportable_projects'],
    'gestation_five_year': ['reportable_projects'],
    'gestation_two_year': ['reportable_projects'],
}
CONTRACTS = {
    'enquiry': ['Exact New Enquiry child reason; unweighted quote value otherwise zero.',
        'Current API-active child membership for Open parent refresh; retain Won/Lost.',
        'Monthly actuals: creation month, positive values, completed months.'],
    'order': ['Hidden Total Customer Order Value = material plus additional charges.',
        'Parent Order Value follows the configured typed Monday mirror, including verified multiplicity.',
        'Keep missing/unreadable evidence distinct from typed blanks; do not force scopes equal.',
        'Bookings retain order date, positive amounts, won-stage rules and completed months.'],
    'invoice': ['Hidden Amount Invoiced retains signed values.',
        'Parent mirrors sum complete current children across business statuses; all-blank NULL, zero zero.',
        'Monthly revenue: positive dated invoices for retained reportable parents in completed months, without a business-stage or API lifecycle filter.'],
    'conversion': ['Inclusive closed-invoiced wins / all eligible; closed-only variant separately named.',
        'Aggregate counts before division; preserve five/two-year lower-bound cohorts and three-decimal ratios.',
        'Expected conversion is a prediction, not this observed metric.'],
    'gestation': ['Stored actual gestation retains source/fallback semantics from first design to first invoice.',
        'Historical means/percentiles exclude nonpositive values; expected gestation is separately labelled.'],
}


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def output_path(path):
    path = Path(path).resolve()
    if not path.is_relative_to((ROOT / 'outputs').resolve()):
        raise ValueError('Sensitive Phase 1 artifacts must stay inside the git-ignored outputs directory')
    return path


def save_new(path, value):
    path = output_path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('x', encoding='utf-8') as stream:
        stream.write(json.dumps(value, default=encode, indent=2, ensure_ascii=False) + '\n')


def seal_packet(payload):
    return {'payload': json_value(payload), 'sha256': fingerprint(payload)}


def load_packet(path):
    packet = json.loads(Path(path).read_text(encoding='utf-8'))
    if packet.get('sha256') != fingerprint(packet.get('payload')):
        raise ValueError('Evidence fingerprint mismatch')
    if not isinstance(packet.get('payload'), dict) or packet['payload'].get('version') != VERSION:
        raise ValueError('Unsupported evidence packet version')
    return packet


def writer_inventory():
    """Inspect source text, without importing mutation-capable application code."""
    result = {}
    for name, (path, functions) in WRITERS.items():
        data = (ROOT / path).read_bytes()
        nodes = ast.walk(ast.parse(data.decode('utf-8-sig')))
        result[name] = {'path': path, 'sha256': hashlib.sha256(data).hexdigest(),
            'functions': {node.name: {'line': node.lineno, 'end_line': node.end_lineno}
                          for node in nodes if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                          and node.name in functions},
            'deployment_status': 'not_verified_from_local_source'}
        if set(result[name]['functions']) != set(functions):
            raise ValueError('Writer trace functions changed; update the reviewed source map: ' + name)
    return result


def catalog_supplement(conn):
    queries = {
        'policies': "SELECT schemaname,tablename,policyname,permissive,roles,cmd,qual,with_check FROM pg_policies WHERE schemaname='public' ORDER BY tablename,policyname",
        'schema_permissions': "SELECT nspname,pg_get_userbyid(nspowner) AS owner,nspacl::text FROM pg_namespace WHERE nspname='public'",
        'role_attributes': "SELECT rolname,rolsuper,rolinherit,rolcreaterole,rolcreatedb,rolcanlogin,rolbypassrls,rolconnlimit FROM pg_roles ORDER BY rolname",
        'role_memberships': "SELECT pg_get_userbyid(roleid) AS role,pg_get_userbyid(member) AS member,admin_option FROM pg_auth_members ORDER BY 1,2",
        'relation_acl': "SELECT c.relname,c.relacl::text,c.relforcerowsecurity,c.relispopulated FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind IN ('r','v','m','p') ORDER BY c.relname",
        'function_permissions': "SELECT p.proname,pg_get_function_identity_arguments(p.oid) AS signature,pg_get_userbyid(p.proowner) AS owner,p.proacl::text,p.proconfig FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public' AND p.prokind='f' ORDER BY p.proname,signature",
        'connection_settings': "SELECT name,setting,unit FROM pg_settings WHERE name IN ('max_connections','superuser_reserved_connections','reserved_connections') ORDER BY name",
        'connections_observed': "SELECT datname,backend_type,state,count(*) AS connections FROM pg_stat_activity GROUP BY datname,backend_type,state ORDER BY datname,backend_type,state",
    }
    return {name: conn.execute(query).fetchall() for name, query in queries.items()}


def capture_samples(conn, sample_size):
    selections = {}
    ids = {'project_id': set(), 'child_id': set(), 'hidden_id': set()}
    for name, query in references('reconciliation.sql').items():
        # Count candidates without exporting all IDs. Inner ORDER BY fixes selection.
        rows = conn.execute(sql.SQL('SELECT q.*,count(*) OVER () AS candidate_count FROM ({}) q LIMIT %s')
                            .format(sql.SQL(query.rstrip(';'))), (sample_size,)).fetchall()
        total = rows[0]['candidate_count'] if rows else 0
        for row in rows:
            row.pop('candidate_count')
            for field in ids:
                if row.get(field):
                    ids[field].add(row[field])
        selections[name] = {'candidate_count': total, 'sample': rows,
                            'unit': 'candidate rows; linked hidden items can occur more than once',
                            'source_review': 'pending_exact_id_Monday_evidence'}
    # Preserve actual source multiplicity; no inferred joins or monetary deduplication.
    children = conn.execute('''SELECT monday_id,parent_monday_id,hidden_item_id,
        reason_for_change,quote_amount,new_enquiry_value,cust_order_value_material,
        cust_additional_charges,amount_invoiced,date_order_received,invoice_date,
        date_design_completed,count(*) OVER () AS detail_count FROM subitems
        WHERE monday_id=ANY(%s) OR parent_monday_id=ANY(%s) OR hidden_item_id=ANY(%s)
        ORDER BY monday_id LIMIT 2001''',
        (sorted(ids['child_id']), sorted(ids['project_id']), sorted(ids['hidden_id']))).fetchall()
    total = children[0]['detail_count'] if children else 0
    children = children[:2000]
    for child in children:
        child.pop('detail_count')
        ids['project_id'].add(child['parent_monday_id'])
        if child['hidden_item_id']:
            ids['hidden_id'].add(child['hidden_item_id'])
    projects = conn.execute('''SELECT monday_id,pipeline_stage,status_category,date_created,
        new_enquiry_value,total_order_value,total_amount_invoiced,gestation_period,
        first_date_designed,first_date_invoiced FROM projects WHERE monday_id=ANY(%s)
        ORDER BY monday_id''', (sorted(x for x in ids['project_id'] if x),)).fetchall()
    hidden = conn.execute('''SELECT monday_id,cust_order_value_material,cust_additional_charges,
        amount_invoiced,invoice_date FROM hidden_items WHERE monday_id=ANY(%s)
        ORDER BY monday_id''', (sorted(ids['hidden_id']),)).fetchall()
    return {'cases': selections, 'projects': projects, 'children': children, 'hidden_items': hidden,
            'child_detail_candidate_count': total, 'child_detail_truncated': total > len(children),
            'null_semantics': 'Stored NULL alone cannot distinguish typed blank, missing and unreadable source evidence.',
            'scope': 'Representative frozen records only; not certification of full company totals.'}


def live_clone_evidence(conn):
    """Success evidence and source counts, labelled separately from frozen capture."""
    counts = {}
    for table in ('projects', 'reportable_projects', 'subitems', 'hidden_items',
                  'current_projects', 'current_subitems', 'current_hidden_items'):
        counts[table] = conn.execute(sql.SQL('SELECT count(*) AS n FROM public.{}')
                                    .format(sql.Identifier(table))).fetchone()['n']
    ingestion = conn.execute('''SELECT board_name,
        max(completed_at) FILTER(WHERE status='completed') AS latest_success,
        max(completed_at) AS latest_completion FROM public.sync_log
        GROUP BY board_name ORDER BY board_name''').fetchall()
    jobs = conn.execute('''SELECT job_id,
        max(finished_at) FILTER(WHERE outcome='succeeded') AS latest_success,
        max(finished_at) AS latest_completion FROM public.worker_job_runs
        GROUP BY job_id ORDER BY job_id''').fetchall()
    snapshots = {}
    for table in ('pipeline_forecast_snapshot', 'pipeline_smoothing_forecast_snapshot'):
        exists = conn.execute('SELECT to_regclass(%s) AS relation', ('public.' + table,)).fetchone()['relation']
        snapshots[table] = conn.execute(sql.SQL('SELECT count(*) AS rows,min(snapshot_date) AS first_date,max(snapshot_date) AS latest_date FROM public.{}')
                                       .format(sql.Identifier(table))).fetchone() if exists else {'status': 'relation_missing'}
    return {'scope': 'TEST clone at evidence capture; operational logs may predate its restore',
            'row_counts': counts, 'ingestion_successes': ingestion,
            'worker_successes': jobs, 'snapshot_dates': snapshots,
            'rollup_success': {'status': 'pending', 'reason': 'No separately certified per-writer rollup completion evidence'},
            'materialized_refresh': {'status': 'pending_deployed_code_mapping',
                'reason': 'A job success must be tied to its deployed implementation and refreshed relations'},
            'production_freshness': 'not_observed',
            'connection_budget': 'Observed clone connections are not production capacity or a pooler allowance'}


def pending(owner, **extra):
    return {'status': 'pending', 'owner': owner, 'value': None,
            'reviewed_by': None, 'reviewed_at': None, 'evidence': [], **extra}


def review_template(packet):
    payload = packet['payload']
    return {'version': VERSION, 'dataset': payload['dataset'], 'evidence_sha256': packet['sha256'],
        'decisions': {name: pending(owner) for name, owner in DECISIONS.items()},
        'metrics': {name: pending('BI/business metric owner', population=None,
                                 source_contract=None, limitations=[]) for name in METRICS},
        'issues': {name: pending('BI/data owner', requirement=description,
                                affected_scope=None) for name, description in ISSUES.items()},
        'source_samples': {name: pending('BI/data owner') for name in payload['reconciliation']['cases']},
        'freshness': {name: pending('Platform/data owner', succeeded_at=None,
                                   maximum_age_hours=None) for name in ('ingestion', 'rollup', 'materialized_refresh', 'snapshot')},
        'deployment': pending('Platform owner', environment=None, services=[]),
        'reports': {name: pending('BI report owner', report_name=None, report_version=None,
                                 definition_sha256=None) for name in POWER_BI_REFERENCES},
        'power_bi_exceptions': {},
        'notes': 'Use reportable for the report_population decision value and each metric population. Retain genuine archived projects; exclude only effective reviewed redundant placeholders. Bind source/release versions explicitly. Sample success never certifies an entire dataset.'}


def capture(args):
    if not 1 <= args.sample_size <= 20:
        raise ValueError('sample-size must be between 1 and 20')
    destination = output_path(args.output)
    if destination.exists():
        raise ValueError('Use a new output directory for each evidence capture')
    attachments = {name: load_packet(path) for name, path in
                   (('pbix', args.pbix), ('reader_audit', args.reader_audit)) if path}
    conn, identity = connect()
    with conn:
        conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        conn.execute("SET LOCAL statement_timeout='120s'")
        conn.execute("SET LOCAL lock_timeout='5s'")
        artifacts, verification = verify_snapshot(conn, identity, args.dataset)
        set_path(conn, args.dataset)
        samples = capture_samples(conn, args.sample_size)
        frozen_plans = {name: conn.execute('EXPLAIN (FORMAT JSON) ' + artifacts['expected'][name]['sql']).fetchall()
                        for name in POWER_BI_REFERENCES}
        set_path(conn, 'public')
        current_inventory = inventory(conn)
        supplement = catalog_supplement(conn)
        live = live_clone_evidence(conn)
    payload = {'version': VERSION, 'dataset': args.dataset, 'target': identity, 'captured_at': utc_now(),
        'scope': 'TEST clone plus sealed evaluation data; no production or Monday API contacted',
        'manifest': artifacts['manifest'], 'manifest_sha256': fingerprint(artifacts['manifest']),
        'verification': verification, 'frozen_inventory': artifacts['inventory'],
        'frozen_freshness': artifacts['freshness'], 'diagnostics': artifacts['diagnostics'],
        'clone_inventory': current_inventory, 'catalog_supplement': supplement,
        'clone_operations': live, 'reconciliation': samples, 'writer_paths': writer_inventory(),
        'attachments': attachments,
        'representative_workload': {'references': list(POWER_BI_REFERENCES),
            'plans': frozen_plans, 'measured_load': False,
            'remaining_workload': ['ambiguity clarification', 'follow-up', 'authorised drill-down/export'],
            'note': 'Query plans are EXPLAIN only; latency/concurrency/cost targets require owner approval.'}}
    from reference_review import reference_evidence
    payload['reference_evidence_sha256'] = reference_evidence(artifacts)['sha256']
    views = {view['name']: view for view in artifacts['inventory']['views']}
    payload['report_source_candidates'] = {
        name: {'status': 'pending_Power_BI_mapping', 'definitions': {
            relation: {'definition_sha256': fingerprint(views[relation]['definition']),
                       'kind': views[relation]['kind']} if relation in views else {'status': 'missing'}
            for relation in relations}} for name, relations in REPORT_SOURCES.items()}
    payload['metric_contract_requirements'] = CONTRACTS
    payload['tool_source_hashes'] = {name: hashlib.sha256((Path(__file__).parent / name).read_bytes()).hexdigest()
                                    for name in ('phase1.py', 'powerbi.py', 'reconciliation.sql', 'dataset.py', 'manage.py')}
    if 'reader_audit' in attachments:
        deployed = {view['name']: view for view in attachments['reader_audit']['payload']['inventory']['views']}
        payload['clone_vs_reader_definitions'] = {
            name: {'matches': views[name]['definition'] == deployed[name]['definition'],
                   'frozen_sha256': fingerprint(views[name]['definition']),
                   'reader_sha256': fingerprint(deployed[name]['definition'])}
            for name in sorted(views.keys() & deployed.keys())}
    packet = seal_packet(payload)
    save_new(destination / 'evidence.json', packet)
    save_new(destination / 'review.json', review_template(packet))
    print(json.dumps({'dataset': args.dataset, 'evidence': str(destination / 'evidence.json'),
        'sample_categories': len(samples['cases']), 'verification': verification,
        'certification': 'pending_owner_and_source_review'}))


def typed_rows(result):
    """Validate export types before comparing; reject rounded floats for NUMERIC."""
    numeric = {20, 21, 23, 700, 701, 1700}
    rows = []
    for row in result['rows']:
        normalized = {}
        for column in result['columns']:
            key, oid = column['name'], column['type_oid']
            value = row[key]
            if value is not None and oid in numeric:
                if isinstance(value, bool) or (oid == 1700 and isinstance(value, float)):
                    raise ValueError('Export NUMERIC values as decimal strings; booleans are not numbers')
                try:
                    value = Decimal(str(value))
                except InvalidOperation:
                    raise ValueError('Invalid exported number') from None
                if not value.is_finite() or (oid in {20, 21, 23} and value != value.to_integral_value()):
                    raise ValueError('Exported number must be finite and match its PostgreSQL type')
            elif value is not None and oid == 1082:
                value = date.fromisoformat(str(value)).isoformat()
            elif value is not None and oid in {25, 1042, 1043} and not isinstance(value, str):
                raise ValueError('Exported text must remain text')
            normalized[key] = value
        rows.append(normalized)
    order = lambda row: tuple((0, '') if row[c['name']] is None else (1, row[c['name']]) for c in result['columns'])
    return sorted(rows, key=order)


def validate_power_bi_alignment(exports, manifest, now=None):
    if reference_version(manifest) == '1.0.0':
        return
    alignment = exports.get('alignment')
    required = {
        'dataset': manifest['dataset'],
        'as_of_date': str(manifest['as_of_date']),
        'business_timezone': manifest['business_timezone'],
        'reference_contract_version': reference_version(manifest),
        'source_kind': 'frozen_TEST',
        'population': 'reportable',
    }
    if not isinstance(alignment, dict) or any(alignment.get(key) != value for key, value in required.items()):
        raise ValueError('Power BI alignment must match the frozen TEST dataset, contract, timezone and population')
    captured = timestamp(manifest['captured_at'])
    if timestamp(alignment.get('snapshot_captured_at')) != captured:
        raise ValueError('Power BI model snapshot capture differs from the frozen reference')
    execution = exports.get('execution')
    if not isinstance(execution, dict) or execution.get('engine') != 'Power BI':
        raise ValueError('Independent Power BI execution evidence is required')
    refreshed = timestamp(execution.get('model_refreshed_at'))
    exported = timestamp(execution.get('exported_at'))
    if not captured <= refreshed <= exported <= (now or datetime.now(timezone.utc)):
        raise ValueError('Power BI refresh/export times must follow the frozen capture and not be in the future')


def compare_power_bi(exports, expected, manifest):
    """Compare typed results; preserve duplicate rows, NULL/zero and numeric precision."""
    if exports.get('dataset') != manifest['schemas']['data'] or exports.get('manifest_sha256') != fingerprint(manifest):
        raise ValueError('Power BI export must identify this exact frozen manifest')
    validate_power_bi_alignment(exports, manifest)
    entries = exports.get('comparisons')
    if not isinstance(entries, list) or not entries:
        raise ValueError('Power BI comparisons must be a nonempty list')
    results = {}
    for entry in entries:
        if not isinstance(entry, dict):
            raise ValueError('Power BI comparison entry must be an object')
        name = entry.get('reference')
        if name not in POWER_BI_REFERENCES or name in results:
            raise ValueError('Unknown or duplicate Power BI reference')
        for field in ('report_name', 'report_version', 'dax', 'filters', 'population', 'as_of_date'):
            if not isinstance(entry.get(field), str) or not entry[field].strip():
                raise ValueError('Power BI comparison metadata missing: ' + field)
        if entry['as_of_date'] != str(manifest['as_of_date']):
            raise ValueError('Power BI export reporting date differs from frozen date')
        if reference_version(manifest) == '1.1.0' and entry['population'] != REPORT_POPULATION:
            raise ValueError('Power BI reference population must be reportable')
        columns = expected[name]['columns']
        if entry.get('columns') != columns:
            raise ValueError('Power BI export columns/types differ: ' + name)
        rows = entry.get('rows')
        if not isinstance(rows, list) or any(not isinstance(row, dict) or row.keys() != {c['name'] for c in columns} for row in rows):
            raise ValueError('Power BI export rows have invalid columns: ' + name)
        actual = typed_rows(entry)
        wanted = typed_rows(expected[name])
        results[name] = {'matches': equivalent(actual, wanted),
            'actual_rows': len(actual), 'expected_rows': len(wanted),
            'report_name': entry['report_name'], 'report_version': entry['report_version'],
            'definition_sha256': fingerprint({key: entry[key] for key in ('dax', 'filters', 'population', 'as_of_date')}),
            'definition': {key: entry[key] for key in ('dax', 'filters', 'population', 'as_of_date')}}
    return results


def power_bi(args):
    from powerbi import load_exports
    exports = load_exports(args.input)
    conn, identity = connect()
    with conn:
        conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        conn.execute("SET LOCAL statement_timeout='120s'")
        artifacts, _ = verify_snapshot(conn, identity, args.dataset)
        comparisons = compare_power_bi(exports, artifacts['expected'], artifacts['manifest'])
    packet = seal_packet({'version': VERSION, 'dataset': args.dataset,
        'manifest_sha256': fingerprint(artifacts['manifest']), 'captured_at': utc_now(),
        'reference_contract_version': reference_version(artifacts['manifest']),
        'alignment': exports.get('alignment'), 'execution': exports.get('execution'),
        'export_sha256': fingerprint(exports), 'comparisons': comparisons})
    save_new(args.output / 'power_bi_comparison.json', packet)
    print(json.dumps({'compared': len(comparisons), 'matched': sum(r['matches'] for r in comparisons.values()),
                      'missing': sorted(set(POWER_BI_REFERENCES) - comparisons.keys())}))
    return 0 if len(comparisons) == len(POWER_BI_REFERENCES) and all(r['matches'] for r in comparisons.values()) else 2


def reviewed(record, statuses=('approved',)):
    if not isinstance(record, dict) or record.get('status') not in statuses:
        return False
    if any(not isinstance(record.get(key), str) or not record[key].strip()
           for key in ('owner', 'reviewed_by', 'reviewed_at')):
        return False
    try:
        timestamp(record['reviewed_at'])
    except (ValueError, TypeError):
        return False
    return record.get('value') not in (None, '', [], {}) and isinstance(record.get('evidence'), list) and bool(record['evidence']) and all(
        isinstance(item, str) and item.strip() for item in record['evidence'])


def timestamp(value):
    if not isinstance(value, str):
        raise ValueError('Timestamp requires an ISO-8601 string')
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        raise ValueError('Timestamp requires a timezone offset')
    return parsed


def positive_number(value):
    return isinstance(value, (int, float, Decimal)) and not isinstance(value, bool) and value > 0 and Decimal(str(value)).is_finite()


def deployment_problems(deployment):
    problems = []
    services = deployment.get('services', [])
    if deployment.get('environment') != 'production' or not isinstance(services, list) or not services:
        return ['deployment: production services and observed flags required']
    ids, modes, reporting = set(), set(), set()
    for service in services:
        if not isinstance(service, dict):
            problems.append('deployment: invalid service entry')
            continue
        name = service.get('name')
        if not isinstance(name, str) or not name or name in ids:
            problems.append('deployment: missing or duplicate service name')
        ids.add(name)
        if not service.get('start_command') or not service.get('deployed_commit'):
            problems.append('deployment: start command and deployed commit required')
        flags = service.get('flags', {})
        if any(type(flags.get(flag)) is not bool for flag in FLAGS):
            problems.append('deployment: all four observed flags must be booleans')
            continue
        modes.add(flags['MONDAY_ARCHIVE_ENABLED'])
        reporting.add(flags['MONDAY_ARCHIVE_REPORTING_ENABLED'])
        if flags['MONDAY_ARCHIVE_REPORTING_ENABLED'] and not flags['MONDAY_ARCHIVE_ENABLED']:
            problems.append('deployment: archive reporting requires archive processing')
        if flags['MONDAY_ARCHIVE_ENABLED'] and not flags['MONDAY_LIFECYCLE_ENABLED']:
            problems.append('deployment: archive writer requires lifecycle processing')
    if len(modes) > 1:
        problems.append('deployment: mixed archive and ordinary writers can alternate source definitions')
    if len(reporting) > 1:
        problems.append('deployment: inconsistent reporting populations across services')
    return problems


def evaluate_gate(packet, review, power_bi_packet=None, now=None, reference_packet=None):
    evidence = packet['payload']
    now = now or datetime.now(timezone.utc)
    blockers = []
    if review.get('version') != VERSION or review.get('dataset') != evidence['dataset'] or review.get('evidence_sha256') != packet['sha256']:
        raise ValueError('Review is not bound to this evidence capture')
    for group in ('decisions', 'metrics', 'issues', 'source_samples', 'freshness', 'reports', 'power_bi_exceptions'):
        if not isinstance(review.get(group), dict) or any(not isinstance(v, dict) for v in review[group].values()):
            raise ValueError('Review section must contain named review objects: ' + group)
        for record in review[group].values():
            if record.get('reviewed_at'):
                try:
                    if timestamp(record['reviewed_at']) > now:
                        blockers.append('review timestamp is in the future: ' + group)
                except (ValueError, TypeError):
                    blockers.append('invalid review timestamp: ' + group)
    if not isinstance(review.get('deployment'), dict):
        raise ValueError('Deployment review must be an object')
    for name in DECISIONS:
        if not reviewed(review.get('decisions', {}).get(name)):
            blockers.append('decision: ' + name)
    if review.get('decisions', {}).get('report_population', {}).get('value') != REPORT_POPULATION:
        blockers.append('report_population: require reportable, retaining genuine archived projects')
    for name in METRICS:
        metric = review.get('metrics', {}).get(name, {})
        if not reviewed(metric) or metric.get('population') != REPORT_POPULATION or not metric.get('source_contract'):
            blockers.append('metric/source/population sign-off: ' + name)
    for name in ISSUES:
        issue = review.get('issues', {}).get(name, {})
        if not reviewed(issue, ('resolved', 'accepted_limitation')) or not issue.get('affected_scope'):
            blockers.append('unresolved finding: ' + name)
        elif issue['status'] == 'accepted_limitation':
            affected = issue.get('metrics', [])
            if not affected or any(m not in METRICS or name not in review.get('metrics', {}).get(m, {}).get('limitations', []) for m in affected):
                blockers.append('limitation absent from affected metric contracts: ' + name)
    for name in evidence['reconciliation']['cases']:
        if not reviewed(review.get('source_samples', {}).get(name)):
            blockers.append('exact-ID source review: ' + name)
    for name in ('ingestion', 'rollup', 'materialized_refresh', 'snapshot'):
        record = review.get('freshness', {}).get(name, {})
        valid = reviewed(record) and positive_number(record.get('maximum_age_hours'))
        try:
            age = (now - timestamp(record.get('succeeded_at', ''))).total_seconds() / 3600
            valid = valid and 0 <= age <= record['maximum_age_hours']
        except (TypeError, ValueError):
            valid = False
        if not valid:
            blockers.append('missing or stale successful completion evidence: ' + name)
    deployment = review.get('deployment', {})
    if not reviewed(deployment):
        blockers.append('deployed Render configuration not attested')
    blockers.extend(deployment_problems(deployment))
    decisions = review.get('decisions', {})
    targets = decisions.get('performance_targets', {}).get('value')
    target_fields = ('p95_response_ms', 'concurrent_users', 'concurrent_queries',
                     'run_timeout_seconds', 'max_queries_per_run', 'max_rows_per_run',
                     'cost_per_successful_answer')
    if not isinstance(targets, dict) or any(not positive_number(targets.get(f)) for f in target_fields) or not targets.get('cost_currency') or not targets.get('workload'):
        blockers.append('performance targets: require numerical limits, cost currency and representative workload')
    budget = decisions.get('connection_budget', {}).get('value')
    fields = ('database_limit', 'reserved', 'etl_peak', 'other_peak', 'analyst_replicas',
              'read_pool_per_replica', 'state_pool_per_replica', 'headroom')
    if not isinstance(budget, dict) or any(type(budget.get(f)) is not int or budget[f] < 0 for f in fields):
        blockers.append('connection budget: explicit integer allocations required')
    elif min(budget['database_limit'], budget['analyst_replicas'], budget['read_pool_per_replica'], budget['state_pool_per_replica']) <= 0 or (
        budget['reserved'] + budget['etl_peak'] + budget['other_peak'] + budget['headroom']
        + budget['analyst_replicas'] * (budget['read_pool_per_replica'] + budget['state_pool_per_replica']) > budget['database_limit']):
        blockers.append('connection budget: allocated pools exceed capacity or are empty')
    comparisons = {}
    if reference_version(evidence.get('manifest', {})) == '1.1.0':
        approval = reference_packet['payload'] if reference_packet else {}
        if (approval.get('status') != 'approved_with_owner_attestation'
                or approval.get('scope') != 'reference_answers_only'
                or approval.get('dataset') != evidence['dataset']
                or approval.get('manifest_sha256') != evidence['manifest_sha256']
                or not evidence.get('reference_evidence_sha256')
                or approval.get('reference_evidence_sha256') != evidence['reference_evidence_sha256']):
            blockers.append('revised reference answers: matching explicit owner approval required')
    if power_bi_packet:
        payload = power_bi_packet['payload']
        if payload.get('dataset') != evidence['dataset'] or payload.get('manifest_sha256') != evidence['manifest_sha256']:
            raise ValueError('Power BI comparison belongs to a different frozen dataset')
        if reference_version(evidence.get('manifest', {})) == '1.1.0':
            try:
                validate_power_bi_alignment(payload, evidence['manifest'], now=now)
            except ValueError as exc:
                blockers.append('Power BI alignment: ' + str(exc))
        comparisons = payload['comparisons']
    for name in POWER_BI_REFERENCES:
        result = comparisons.get(name)
        report = review.get('reports', {}).get(name, {})
        if not result:
            blockers.append('Power BI comparison missing: ' + name)
        elif not reviewed(report) or any(report.get(key) != result.get(key) for key in ('report_name', 'report_version', 'definition_sha256')):
            blockers.append('BI report definition/version review missing: ' + name)
        elif not result['matches']:
            exception = review.get('power_bi_exceptions', {}).get(name, {})
            if not reviewed(exception) or exception.get('comparison_sha256') != power_bi_packet['sha256']:
                blockers.append('unexplained Power BI difference: ' + name)
    return {'version': VERSION, 'dataset': evidence['dataset'], 'evaluated_at': now.isoformat(),
        'evidence_sha256': packet['sha256'], 'review_sha256': fingerprint(review),
        'power_bi_sha256': power_bi_packet['sha256'] if power_bi_packet else None,
        'reference_approval_sha256': reference_packet['sha256'] if reference_packet else None,
        'status': 'blocked' if blockers else 'passed_with_owner_attestations',
        'blockers': blockers,
        'basis': 'TEST evidence plus supplied owner attestations; no automatic production certification',
        'enabled_metrics': list(METRICS) if not blockers else []}


def review_command(args):
    packet = load_packet(args.evidence)
    review = json.loads(args.review.read_text(encoding='utf-8'))
    comparison = load_packet(args.power_bi) if args.power_bi else None
    reference_approval = load_packet(args.reference_review) if args.reference_review else None
    result = evaluate_gate(packet, review, comparison, reference_packet=reference_approval)
    save_new(args.output / 'phase1_gate.json', result)
    lines = ['# Phase 1 certification review', '', 'Status: ' + result['status'], '',
             result['basis'], '', 'Dataset: `' + result['dataset'] + '`', '',
             '## Outstanding evidence', '']
    lines.extend('- ' + issue for issue in result['blockers'])
    if not result['blockers']:
        lines.append('All required owner attestations and comparison reviews are present.')
    path = output_path(args.output / 'phase1_gate.md')
    with path.open('x', encoding='utf-8') as stream:
        stream.write('\n'.join(lines) + '\n')
    print(json.dumps({'status': result['status'], 'blockers': len(result['blockers']), 'report': str(path)}))
    return 2 if result['blockers'] else 0
