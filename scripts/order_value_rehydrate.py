"""Stage, apply and verify missing-row rehydration from explicit Monday links.

Uses the production DataSyncService transformations through transform_exact_rows.
Only hidden_items and subitems may be inserted; existing project totals are
refreshed from their complete child set. No queue jobs or Monday mutations.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import logging
import os
from pathlib import Path
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from scripts import backfill_order_values as backfill
from scripts import order_value_blocked_review as review
from scripts import order_value_scope_reads as reads
from scripts import order_value_scopes as scopes
from scripts import order_value_scopes_refresh as refresh
from scripts import order_value_scopes_targeted as targeted
from scripts import reconcile_order_values as reconcile

LOG = logging.getLogger(__name__)
WORKFLOW = 'exact-link-rehydration-v1'


def code_fingerprint():
    return hashlib.sha256(targeted.code_fingerprint().encode() + b''.join(
        Path(module.__file__).read_bytes() for module in (refresh, review))
        + Path(__file__).read_bytes()).hexdigest()


def insert_contract(connection):
    """Capture defaults/nullability as well as the usual transformation contract."""
    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute('''SELECT table_name, column_name, is_nullable, column_default,
            is_identity, identity_generation, is_generated, generation_expression
            FROM information_schema.columns WHERE table_schema='public'
            AND table_name=ANY(%s) ORDER BY table_name, ordinal_position''', (list(scopes.TABLES),))
        rows = cursor.fetchall()
        cursor.execute('''SELECT c.relname FROM pg_index i
            JOIN pg_class c ON c.oid=i.indrelid
            JOIN pg_namespace n ON n.oid=c.relnamespace
            JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum=i.indkey[0]
            WHERE n.nspname='public' AND c.relname=ANY(%s) AND i.indisunique
            AND i.indisvalid AND i.indimmediate AND i.indnkeyatts=1
            AND i.indpred IS NULL AND a.attname='monday_id' ''', (list(scopes.TABLES),))
        if {row['relname'] for row in cursor.fetchall()} != set(scopes.TABLES):
            raise ValueError('Every table requires an immediate unique Monday ID key')
    return rows


def split_writes(before, updates):
    inserts, existing = {}, {}
    for table in scopes.TABLES:
        ids = set(backfill.indexed(before[table]))
        inserts[table] = [r for r in updates[table] if r['monday_id'] not in ids]
        existing[table] = [r for r in updates[table] if r['monday_id'] in ids]
    if inserts['projects']:
        raise scopes.ScopeConflict('Missing parents require a separate project-creation review')
    return inserts, existing


def expected_state(before, updates):
    inserts, existing = split_writes(before, updates)
    after = scopes.expected_state(before, existing)
    return {t: scopes.stable_rows(after[t] + inserts[t]) for t in scopes.TABLES}


def validate_insert_fields(inserts, contract):
    for column in contract:
        table, field = column['table_name'], column['column_name']
        for row in inserts[table]:
            if field in row and row[field] is None and column['is_nullable'] == 'NO':
                raise scopes.ScopeConflict(f'Insert requires non-null {table}.{field}')
            if (field not in row and column['is_nullable'] == 'NO'
                    and column['column_default'] is None and column['is_identity'] == 'NO'
                    and column['is_generated'] == 'NEVER'):
                raise scopes.ScopeConflict(f'Insert lacks required field {table}.{field}')


def make_record(baseline, source, raw, contract, parents):
    inventory = scopes.index_source(source)
    children = [r for pid in parents for r in inventory['parents'][pid]]
    if (not 1 <= len(parents) <= scopes.MAX_PROJECTS or not children
            or any(r['link_error'] or len(r['hidden_ids']) != 1 for r in children)):
        raise scopes.ScopeConflict('Invalid or oversized parent/child group')
    scope = {'projects': sorted(parents), 'subitems': sorted(r['monday_id'] for r in children),
             'hidden_items': sorted({r['hidden_ids'][0] for r in children})}
    if len(scope['hidden_items']) != len(children):
        raise scopes.ScopeConflict('A Monday source has more than one selected owner')
    evidence = scopes.source_evidence(source, scope, inventory)
    if evidence['owners'] != evidence['subitems']:
        raise scopes.ScopeConflict('Monday source has an owner outside the selected group')
    boundary = scopes.boundary_from_baseline(baseline, scope)
    before = scopes.select_boundary(scopes.index_baseline(baseline), boundary)
    # Existing child parents cannot move. Missing children are allowed; stale
    # stored children still block the entire affected group.
    existing = set(backfill.indexed(before['subitems']))
    membership = {**evidence, 'subitems': [r for r in evidence['subitems'] if r['monday_id'] in existing]}
    membership_scope = {**scope, 'subitems': sorted(set(scope['subitems']) & existing)}
    scopes.check_parent_membership(before, membership_scope, membership)
    inputs = {t: [raw[t][i] for i in scope[t]] for t in ('hidden_items', 'subitems')}
    for child in inputs['subitems']:
        row = reads.normalized_link(child)
        if row != inventory['children'][row['monday_id']]:
            raise scopes.ScopeConflict('Full child evidence differs from its reviewed relationship')
    for item in inputs['hidden_items']:
        row = backfill.normalize_hidden(item)
        wanted = inventory['hidden'][row['monday_id']]
        if row['issues'] or any(row[f] != wanted[f] for f in (*backfill.ORDER_FIELDS, 'monday_total')):
            raise scopes.ScopeConflict('Full hidden evidence differs from its reviewed order inputs')
    updates = reconcile.normalize_updates(reconcile.transform_exact_rows(
        inputs['hidden_items'], inputs['subitems'], set(parents)), contract)
    for table in scopes.TABLES:
        if set(backfill.indexed(updates[table])) != set(scope[table]):
            raise scopes.ScopeConflict('Transformation does not cover the exact selected IDs')
    transformed = {t: backfill.indexed(rows) for t, rows in updates.items()}
    expected_totals = defaultdict(Decimal)
    for child in children:
        cid, hid, pid = child['monday_id'], child['hidden_ids'][0], child['parent_monday_id']
        row = transformed['subitems'][cid]
        if row['hidden_item_id'] != hid or row['parent_monday_id'] != pid:
            raise scopes.ScopeConflict('Transformation changed an explicit Monday relationship')
        for field in backfill.ORDER_FIELDS:
            wanted = inventory['hidden'][hid][field]
            if (backfill.money(row[field]) != wanted
                    or backfill.money(transformed['hidden_items'][hid][field]) != wanted):
                raise scopes.ScopeConflict('Transformed order fields differ from Monday inputs')
            expected_totals[pid] += Decimal(wanted)
    for pid, total in expected_totals.items():
        if backfill.money(transformed['projects'][pid]['total_order_value']) != backfill.money(total):
            raise scopes.ScopeConflict('Project order total differs from its complete Monday source set')
    after = expected_state(before, updates)
    if sum(map(len, after.values())) > scopes.MAX_SCOPE_ROWS:
        raise scopes.ScopeConflict('Scope exceeds the 500-row transaction limit after insertion')
    scopes.check_ownership(after, scope)
    return {'scope_id': 'rehydrate-' + backfill.fingerprint(sorted(parents))[:16],
            'scope': scope, 'boundary': boundary, 'source': evidence, 'raw': inputs,
            'before': before, 'after': after, 'updates': updates}


def build_records(baseline, source, raw, contract):
    records, deferred = [], []
    for parents in review.dependency_groups(source['project_ids'], baseline, source):
        try:
            record = make_record(baseline, source, raw, contract, parents)
        except scopes.ScopeConflict as exc:
            deferred.append({'project_ids': parents, 'reason': str(exc)})
            continue
        if records:
            try:
                merged = make_record(baseline, source, raw, contract,
                                     records[-1]['scope']['projects'] + parents)
            except scopes.ScopeConflict:
                pass
            else:
                records[-1] = merged
                continue
        records.append(record)
    return records, deferred


def stage_run(connection, monday, output_dir, project_ids):
    if output_dir.exists():
        raise ValueError('Use a new directory to preserve previous evidence')
    if (not project_ids or len(project_ids) != len(set(project_ids))
            or any(not isinstance(i, str) or not i.isascii() or not i.isdigit() for i in project_ids)):
        raise ValueError('Select unique numeric Monday project IDs')
    source, capture = refresh.capture_selected(monday, sorted(project_ids))
    combined = {'projects': sorted(project_ids), 'subitems': sorted(r['monday_id'] for r in source['subitems']),
                'hidden_items': sorted(r['monday_id'] for r in source['hidden_items'])}
    raw = reconcile.capture_targeted(monday, source, combined)
    # Recheck parent metadata too, without a global board traversal.
    temporary = {'scope': combined, 'source': scopes.source_evidence(source, combined)}
    if scopes.targeted_orders(monday, temporary) != temporary['source']:
        raise scopes.ScopeConflict('Monday evidence changed during staging')
    with connection.transaction():
        connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
        safety = scopes.schema_safety(connection)
        contract, defaults = reconcile.read_contract(connection), insert_contract(connection)
        initial = scopes.read_boundary(connection, combined, full=True)
        boundary = scopes.boundary_from_baseline(initial, combined)
        baseline = scopes.read_boundary(connection, boundary, full=True)
    raw_index = {t: backfill.indexed(rows, 'id') for t, rows in raw.items()}
    records, deferred = build_records(baseline, source, raw_index, contract)
    for record in records:
        validate_insert_fields(split_writes(record['before'], record['updates'])[0], defaults)
    staged = {'mode': 'repair', 'contract': contract, 'insert_contract': defaults,
              'selected_project_ids': sorted(project_ids), 'scopes': records, 'deferred': deferred,
              'source_of_truth': 'Monday CRM', 'global_ownership_validated_at_stage': False,
              'capture_started_at': capture['started_at']}
    return save_run(output_dir, staged, backfill.target_fingerprint(connection), safety)


def save_run(output_dir, staged, target, safety):
    output_dir.mkdir(parents=True, exist_ok=False)
    backfill.write_json(output_dir / 'scopes.json', staged)
    changes, insert_counts = [], Counter()
    for record in staged['scopes']:
        inserts, _ = split_writes(record['before'], record['updates'])
        insert_counts.update({t: len(rows) for t, rows in inserts.items()})
        for table, rows in record['updates'].items():
            previous = backfill.indexed(record['before'][table])
            for row in rows:
                missing = row['monday_id'] not in previous
                for field, value in row.items():
                    old = previous.get(row['monday_id'], {}).get(field)
                    if missing or old != value:
                        changes.append({'scope_id': record['scope_id'], 'project_ids': record['scope']['projects'],
                            'operation': 'insert' if missing else 'update', 'table': table,
                            'monday_id': row['monday_id'], 'field': field, 'before': old, 'after': value})
    reconcile.write_csv(output_dir / 'changes.csv', changes,
        ['scope_id', 'project_ids', 'operation', 'table', 'monday_id', 'field', 'before', 'after'])
    manifest = {'version': 1, 'workflow': WORKFLOW, 'mode': 'repair', 'run_id': str(uuid4()),
                'prepared_at': datetime.now(timezone.utc).isoformat(), 'target': target,
                'code': code_fingerprint(), 'source_contract': backfill.source_contract(),
                'sha256': backfill.fingerprint(staged),
                'review_sha256': hashlib.sha256((output_dir / 'changes.csv').read_bytes()).hexdigest(),
                'projects': sum(len(r['scope']['projects']) for r in staged['scopes']),
                'selected_projects': len(staged['selected_project_ids']), 'scopes': len(staged['scopes']),
                'deferred_scopes': len(staged['deferred']), 'insert_rows': dict(insert_counts),
                'review_entries': len(changes), 'safety': safety,
                'write_lock': 'SHARE ROW EXCLUSIVE on projects, hidden_items and subitems per transaction'}
    backfill.write_json(output_dir / 'manifest.json', manifest)
    load_run(output_dir)
    return manifest


def load_run(run_dir):
    manifest = json.loads((run_dir / 'manifest.json').read_text(encoding='utf-8'))
    staged = json.loads((run_dir / 'scopes.json').read_text(encoding='utf-8'))
    if (manifest.get('workflow') != WORKFLOW or manifest['version'] != 1
            or manifest['code'] != code_fingerprint() or manifest['source_contract'] != backfill.source_contract()
            or manifest['sha256'] != backfill.fingerprint(staged)
            or manifest['review_sha256'] != hashlib.sha256((run_dir / 'changes.csv').read_bytes()).hexdigest()
            or staged['mode'] != 'repair' or manifest['mode'] != 'repair'):
        raise ValueError('Workflow, code, mappings or reviewed artifacts changed; stage a new run')
    seen = {t: set() for t in scopes.TABLES}
    for record in staged['scopes']:
        for table in scopes.TABLES:
            ids = record['boundary'][table]
            if len(ids) != len(set(ids)) or seen[table].intersection(ids):
                raise ValueError('Scopes overlap; related dependencies must commit together')
            seen[table].update(ids)
        evidence = record['source']
        source = {'project_ids': evidence['projects'], 'subitems': evidence['owners'],
                  'hidden_items': evidence['hidden_items'],
                  'exclusion_evidence': {'parent_details': {'items': evidence['parents']}}}
        raw = {t: backfill.indexed(rows, 'id') for t, rows in record['raw'].items()}
        if make_record(record['before'], source, raw, staged['contract'], record['scope']['projects']) != record:
            raise ValueError('Staged changes differ from the reviewed exact-ID evidence')
        validate_insert_fields(split_writes(record['before'], record['updates'])[0], staged['insert_contract'])
    deferred_ids = [pid for r in staged['deferred'] for pid in r['project_ids']]
    covered = list(seen['projects']) + deferred_ids
    if len(covered) != len(set(covered)) or sorted(covered) != staged['selected_project_ids']:
        raise ValueError('Every selected project must be staged or explicitly deferred')
    return manifest, staged


def validate_actual(actual, record):
    """Existing rows compare fully; new rows may acquire DB-generated defaults.

    The full actual after-state, including those defaults, is hashed into the
    transaction journal. Verification subsequently checks that exact hash.
    """
    for table in scopes.TABLES:
        current, expected = backfill.indexed(actual[table]), backfill.indexed(record['after'][table])
        previous = backfill.indexed(record['before'][table])
        if current.keys() != expected.keys():
            raise scopes.ScopeConflict('Post-write row membership differs from review')
        for item_id, row in expected.items():
            if ((item_id in previous and current[item_id] != row)
                    or any(current[item_id].get(field) != value for field, value in row.items())):
                raise scopes.ScopeConflict('Post-write values differ from review')
    scopes.check_ownership(actual, record['scope'])


def write_inserts(connection, inserts):
    counts = {t: 0 for t in scopes.TABLES}
    for table in ('hidden_items', 'subitems'):
        groups = defaultdict(list)
        for row in inserts[table]:
            groups[tuple(sorted(row))].append(row)
        for fields, rows in groups.items():
            names = sql.SQL(', ').join(map(sql.Identifier, fields))
            result = connection.execute(sql.SQL('INSERT INTO public.{table} ({fields}) '
                'SELECT {fields} FROM jsonb_populate_recordset(NULL::public.{table}, %s) '
                'RETURNING monday_id').format(table=sql.Identifier(table), fields=names), (Jsonb(rows),))
            actual = [r[0] for r in result.fetchall()]
            if len(actual) != len(rows) or set(actual) != {r['monday_id'] for r in rows}:
                raise scopes.ScopeConflict('Insert row coverage differs from review')
            counts[table] += len(rows)
    return counts


def commit_scope(connection, manifest, staged, record):
    with connection.transaction():
        connection.execute('SET TRANSACTION ISOLATION LEVEL READ COMMITTED')
        connection.execute("SET LOCAL lock_timeout='750ms'")
        connection.execute("SET LOCAL statement_timeout='4s'")
        connection.execute("SET LOCAL transaction_timeout='10s'")
        connection.execute("SET LOCAL idle_in_transaction_session_timeout='5s'")
        connection.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))',
                           (manifest['run_id'] + ':' + record['scope_id'],))
        if record['scope_id'] in scopes.committed_scopes(connection, manifest):
            return {'scope_id': record['scope_id'], 'status': 'already_committed'}
        # Missing keys cannot be row locked. Brief table write locks protect
        # absence, concurrent inserts and incoming FK ownership until commit.
        # No HTTP calls occur inside this transaction; ordinary SELECTs continue.
        connection.execute('LOCK TABLE public.projects, public.hidden_items, public.subitems IN SHARE ROW EXCLUSIVE MODE')
        scopes.schema_safety(connection)
        if reconcile.read_contract(connection) != staged['contract'] or insert_contract(connection) != staged['insert_contract']:
            raise scopes.ScopeConflict('Database schema/defaults changed since review')
        current = scopes.read_boundary(connection, record['boundary'], lock=True, full=True)
        if current != record['before']:
            raise scopes.ScopeConflict('Rows, values or ownership changed since staging; restage this scope')
        inserts, existing = split_writes(current, record['updates'])
        inserted = write_inserts(connection, inserts)
        updated = scopes.write_updates(connection, existing)
        actual = scopes.read_boundary(connection, record['boundary'], full=True)
        validate_actual(actual, record)
        counts = {'inserted': inserted, 'updated': updated}
        connection.execute('INSERT INTO public.order_value_scope_commits '
            '(run_id, scope_id, plan_sha256, mode, project_ids, before_sha256, after_sha256, updated_rows) '
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s)',
            (manifest['run_id'], record['scope_id'], manifest['sha256'], 'repair', record['scope']['projects'],
             backfill.fingerprint(current), backfill.fingerprint(actual), Jsonb(counts)))
    return {'scope_id': record['scope_id'], 'status': 'committed_pending_source_verification',
            'inserted_rows': inserted, 'updated_rows': updated, 'after_sha256': backfill.fingerprint(actual)}


def execute_run(connection, monday, run_dir, *, apply=False, confirm_run_id=None, allow_rehydration=False):
    manifest, staged = load_run(run_dir)
    if manifest['target'] != backfill.target_fingerprint(connection):
        raise ValueError('Database target differs')
    if apply and (confirm_run_id != manifest['run_id'] or not allow_rehydration):
        raise ValueError('Apply requires the reviewed run ID and --allow-rehydration acknowledgement')
    with connection.transaction():
        connection.execute('SET TRANSACTION READ ONLY')
        scopes.schema_safety(connection)
        committed = scopes.committed_scopes(connection, manifest)
    known = {r['scope_id'] for r in staged['scopes']}
    if committed - known:
        raise ValueError('Journal contains an unknown scope')
    selected = [r for r in staged['scopes'] if r['scope_id'] not in committed] if apply else staged['scopes']
    ownership_file, owners = None, {}
    if selected and (apply or committed):
        ownership = reads.capture_ownership(connection, monday)
        ownership_file = f'ownership-{"apply" if apply else "verify"}-{uuid4()}.json'
        backfill.write_json(run_dir / ownership_file, ownership)
        owners = reads.owner_index(ownership)
    results = []
    for position, record in enumerate(selected, 1):
        LOG.info('%s scope %d/%d: %s', 'Apply' if apply else 'Verify', position, len(selected), record['scope_id'])
        result = {'scope_id': record['scope_id'], 'project_ids': record['scope']['projects']}
        try:
            if not apply and record['scope_id'] not in committed:
                result['status'] = 'not_committed'
            else:
                reads.check_scope(monday, record, owners, mode='repair')
                if apply:
                    result.update(commit_scope(connection, manifest, staged, record))
                else:
                    with connection.transaction():
                        connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
                        current = scopes.read_boundary(connection, record['boundary'], full=True)
                        journal = connection.execute('SELECT after_sha256 FROM public.order_value_scope_commits '
                            'WHERE run_id=%s AND scope_id=%s AND plan_sha256=%s',
                            (manifest['run_id'], record['scope_id'], manifest['sha256'])).fetchone()
                    validate_actual(current, record)
                    if not journal or journal[0] != backfill.fingerprint(current):
                        raise scopes.ScopeConflict('Database state changed after the journaled commit')
                    result['status'] = 'verified'
        except ValueError as exc:
            result.update(status='deferred' if apply else 'changed_requires_reassessment', reason=str(exc))
        except (psycopg.errors.LockNotAvailable, psycopg.errors.DeadlockDetected,
                psycopg.errors.SerializationFailure, psycopg.errors.QueryCanceled, psycopg.IntegrityError) as exc:
            result.update(status='deferred' if apply else 'changed_requires_reassessment', reason=type(exc).__name__)
        # Connection failures stop here; the journal resolves uncertain commits on
        # retry. Receipts never substitute for the database's atomic commit record.
        results.append(result)
        backfill.write_json(run_dir / f'scope-{uuid4()}.json', result)
    remaining = len(known - scopes.committed_scopes(connection, manifest))
    summary = {'run_id': manifest['run_id'], 'checked_at': datetime.now(timezone.utc).isoformat(),
               'action': 'apply' if apply else 'verify', 'counts': dict(Counter(r['status'] for r in results)),
               'results': results, 'remaining_uncommitted': remaining, 'ownership_evidence': ownership_file,
               'deferred_at_staging': staged['deferred'], 'certifies_entire_dataset': False,
               'complete': not apply and bool(results) and not staged['deferred']
                           and all(r['status'] == 'verified' for r in results)}
    backfill.write_json(run_dir / f'{summary["action"]}-{uuid4()}.json', summary)
    return summary


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    stage = commands.add_parser('stage', help='Read-only current Monday/SQL staging')
    stage.add_argument('--projects-file', type=Path, required=True)
    stage.add_argument('--run-dir', type=Path, required=True)
    apply = commands.add_parser('apply', help='Apply every pending bounded scope')
    apply.add_argument('--run-dir', type=Path, required=True)
    apply.add_argument('--confirm-run-id', required=True)
    apply.add_argument('--allow-rehydration', action='store_true', help='Acknowledge inserts, repair fields, partial commits and brief table write locks')
    verify = commands.add_parser('verify', help='Read-only fresh source and database verification')
    verify.add_argument('--run-dir', type=Path, required=True)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('src.database.sync_service').setLevel(logging.WARNING)
    backfill.load_dotenv()
    try:
        project_ids = refresh.project_ids_from_file(args.projects_file) if args.command == 'stage' else None
        dsn = os.environ.get('SUPABASE_DB_URL')
        if not dsn:
            raise ValueError('SUPABASE_DB_URL is required; never pass credentials in command arguments')
        with psycopg.connect(dsn, autocommit=True, connect_timeout=15) as connection:
            monday = backfill.MondayClient()
            if args.command == 'stage':
                result = stage_run(connection, monday, args.run_dir, project_ids)
                success = bool(result['scopes']) and not result['deferred_scopes']
            else:
                result = execute_run(connection, monday, args.run_dir, apply=args.command == 'apply',
                    confirm_run_id=getattr(args, 'confirm_run_id', None),
                    allow_rehydration=getattr(args, 'allow_rehydration', False))
                success = result['complete'] if args.command == 'verify' else (
                    result['remaining_uncommitted'] == 0 and not result['deferred_at_staging'])
        print(json.dumps(result, indent=2))
        return 0 if success else 2
    except ValueError as exc:
        LOG.error('Rehydration stopped: %s', exc)
    except Exception as exc:
        LOG.error('Rehydration stopped (%s). Apply may have committed earlier scopes; preserve the run and verify it.', type(exc).__name__)
    return 1


if __name__ == '__main__':
    raise SystemExit(main())
