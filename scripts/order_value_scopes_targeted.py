"""Scoped financial reads and one complete relationship inventory per invocation.

This separate entry point preserves the original workflow's fingerprints while
an existing apply/verify is running. New runs must be staged with this module.
The reviewed write transaction is shared unchanged with order_value_scopes.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import logging
import os
from pathlib import Path
from uuid import uuid4

import psycopg

from scripts import backfill_order_values as backfill
from scripts import reconcile_order_values as reconcile
from scripts import order_value_scopes as legacy
from scripts import order_value_scope_reads as reads
from scripts.order_value_scopes import (
    ScopeConflict, MAX_PROJECTS, TABLES, boundary_from_baseline, check_ownership,
    check_parent_membership, choose_groups, committed_scopes, commit_scope,
    expected_state, index_baseline, index_source, order_updates, read_boundary,
    require_existing_scope, schema_safety, select_boundary, source_evidence,
)

LOG = logging.getLogger(__name__)
VERSION = 1
WORKFLOW = 'scoped-reads-v1'


def code_fingerprint():
    return hashlib.sha256(legacy.code_fingerprint().encode() + Path(__file__).read_bytes()
                          + Path(reads.__file__).read_bytes()).hexdigest()


def stage_run(connection, monday, capture_dir, output_dir, *, mode, project_ids, all_verified=False):
    origin, baseline, source, plan = backfill.load_run(capture_dir)
    if not origin['approve_reviewed_parentless_duplicates']:
        raise ValueError('Use the metadata-complete reviewed inventory capture')
    if origin['target'] != backfill.target_fingerprint(connection):
        raise ValueError('Database target differs from the capture')
    groups = choose_groups(baseline, source, plan, mode, project_ids, all_verified)
    inventory = index_source(source)
    captured_children = backfill.indexed(baseline['subitems'])
    selected_parents = sorted({pid for group in groups for pid in group})
    selected_children = [row for pid in selected_parents for row in inventory['parents'][pid]]
    combined_scope = {'projects': selected_parents,
                      'subitems': sorted(row['monday_id'] for row in selected_children),
                      'hidden_items': sorted({hid for row in selected_children for hid in row['hidden_ids']})}
    combined_boundary = boundary_from_baseline(baseline, combined_scope, captured_children)
    output_dir.mkdir(parents=True, exist_ok=False)
    records, deferred, changes = [], [], []
    with connection.transaction():
        connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
        safety = schema_safety(connection, require_journal=False)
        contract = reconcile.read_contract(connection)
        # Three targeted queries, in one snapshot, even for an all-verified run.
        # The subitem predicate includes every incoming owner of old/new sources.
        current_index = index_baseline(read_boundary(connection, combined_boundary, full=mode == 'repair'))
    for parents in groups:
        try:
            if len(parents) > MAX_PROJECTS:
                raise ScopeConflict('Dependency group is too large')
            children = [r for pid in parents for r in inventory['parents'][pid]]
            scope = {'projects': sorted(parents), 'subitems': sorted(r['monday_id'] for r in children),
                     'hidden_items': sorted({r['hidden_ids'][0] for r in children})}
            if not children:
                raise ScopeConflict('Empty parents need separate explicit review; online correction cannot clear them')
            if mode == 'repair':
                reconcile.select_scope(baseline, source, plan, set(parents))
            boundary = boundary_from_baseline(baseline, scope, captured_children)
            evidence = source_evidence(source, scope, inventory)
            before = select_boundary(current_index, boundary)
            require_existing_scope(before, scope)
            check_parent_membership(before, scope, evidence)
            # A relink since capture may reference a key outside the planned locks.
            observed = boundary_from_baseline(before, scope)
            if observed != boundary:
                raise ScopeConflict('Stored source links changed since capture; reassess this group')
            raw = None
            if mode == 'orders':
                updates = order_updates(before, evidence, scope)
            else:
                raw = reconcile.capture_targeted(monday, source, scope)
                updates = reconcile.normalize_updates(reconcile.transform_exact_rows(raw['hidden_items'], raw['subitems'], set(parents)), contract)
            after = expected_state(before, updates)
            check_ownership(after, scope)
            record = {'scope_id': 'scope-' + backfill.fingerprint(parents)[:16], 'scope': scope,
                      'boundary': boundary, 'before': before, 'after': after, 'updates': updates,
                      'source': evidence, 'raw': raw}
            records.append(record)
            for table, rows in updates.items():
                previous = backfill.indexed(before[table])
                for row in rows:
                    for field, value in row.items():
                        old = previous[row['monday_id']].get(field)
                        if old != value:
                            changes.append({'scope_id': record['scope_id'], 'project_ids': parents, 'table': table,
                                            'monday_id': row['monday_id'], 'field': field, 'before': old, 'after': value})
        except ScopeConflict as exc:
            deferred.append({'project_ids': parents, 'reason': str(exc)})
    staged = {'mode': mode, 'contract': contract, 'scopes': records, 'deferred': deferred,
              'origin_run_id': origin['run_id'], 'origin_summary': origin['summary'],
              'approved_empty': origin['approved_empty']}
    backfill.write_json(output_dir / 'scopes.json', staged)
    reconcile.write_csv(output_dir / 'changes.csv', changes,
                        ['scope_id', 'project_ids', 'table', 'monday_id', 'field', 'before', 'after'])
    manifest = {'version': VERSION, 'workflow': WORKFLOW, 'run_id': str(uuid4()), 'prepared_at': datetime.now(timezone.utc).isoformat(),
                'code': code_fingerprint(), 'target': origin['target'], 'source_contract': backfill.source_contract(),
                'sha256': backfill.fingerprint(staged), 'review_sha256': hashlib.sha256((output_dir / 'changes.csv').read_bytes()).hexdigest(),
                'mode': mode, 'scopes': len(records), 'deferred_scopes': len(deferred), 'changes': len(changes), 'safety': safety}
    backfill.write_json(output_dir / 'manifest.json', manifest)
    return manifest


def load_run(run_dir):
    manifest = json.loads((run_dir / 'manifest.json').read_text(encoding='utf-8'))
    staged = json.loads((run_dir / 'scopes.json').read_text(encoding='utf-8'))
    if manifest.get('workflow') != WORKFLOW:
        raise ValueError('This is not a targeted-workflow run; use its original entry point or stage a new run')
    if (manifest['version'] != VERSION or manifest['code'] != code_fingerprint()
            or manifest['source_contract'] != backfill.source_contract()
            or manifest['sha256'] != backfill.fingerprint(staged)
            or manifest['review_sha256'] != hashlib.sha256((run_dir / 'changes.csv').read_bytes()).hexdigest()):
        raise ValueError('Code, mappings or reviewed artifacts changed; stage a new run')
    if staged['mode'] not in ('orders', 'repair') or staged['mode'] != manifest['mode']:
        raise ValueError('Invalid staged mode')
    seen = {table: set() for table in TABLES}
    seen_scopes = set()
    for record in staged['scopes']:
        if record['scope_id'] in seen_scopes:
            raise ValueError('Duplicate staged scope ID')
        seen_scopes.add(record['scope_id'])
        for table in TABLES:
            ids = set(record['scope'][table])
            if len(ids) != len(record['scope'][table]) or ids & seen[table]:
                raise ValueError('Staged scopes contain duplicate or overlapping dependencies')
            seen[table].update(ids)
        if not 1 <= len(record['scope']['projects']) <= MAX_PROJECTS:
            raise ValueError('Invalid project count in scope')
        if boundary_from_baseline(record['before'], record['scope']) != record['boundary']:
            raise ValueError('Scope boundary does not cover reviewed source dependencies')
        require_existing_scope(record['before'], record['scope'])
        check_parent_membership(record['before'], record['scope'], record['source'])
        if expected_state(record['before'], record['updates']) != record['after']:
            raise ValueError('Scope expected state does not match its updates')
        check_ownership(record['after'], record['scope'])
        if staged['mode'] == 'orders':
            if order_updates(record['before'], record['source'], record['scope']) != record['updates']:
                raise ValueError('Order updates differ from reviewed source evidence')
            for table, rows in record['updates'].items():
                allowed = {'monday_id', 'total_order_value'} if table == 'projects' else {'monday_id', *backfill.ORDER_FIELDS}
                if any(set(row) - allowed for row in rows):
                    raise ValueError('Order-only plan contains other fields')
        else:
            rebuilt = reconcile.normalize_updates(reconcile.transform_exact_rows(record['raw']['hidden_items'],
                record['raw']['subitems'], set(record['scope']['projects'])), staged['contract'])
            if rebuilt != record['updates']:
                raise ValueError('Repair updates differ from reviewed exact-ID evidence')
    return manifest, staged


def apply_run(connection, monday, run_dir, *, confirm_run_id, allow_partial=False, allow_repair=False,
              limit=None, all_pending=False, scope_ids=None):
    manifest, staged = load_run(run_dir)
    if manifest['run_id'] != confirm_run_id or manifest['target'] != backfill.target_fingerprint(connection):
        raise ValueError('Run confirmation or target differs')
    if not allow_partial:
        raise ValueError('Online batches require explicit --allow-partial acknowledgement')
    if staged['mode'] == 'repair' and not allow_repair:
        raise ValueError('Repair changes relationships, invoice/enquiry values and dates; use --allow-repair-fields after review')
    if all_pending and limit is not None:
        raise ValueError('Select either --all-pending or --limit')
    limit = 10 if limit is None else limit
    if not all_pending and not 1 <= limit <= 100:
        raise ValueError('Select a batch limit between 1 and 100 scopes')
    with connection.transaction():
        connection.execute('SET TRANSACTION READ ONLY')
        schema_safety(connection)
        committed = committed_scopes(connection, manifest)
    known = {r['scope_id'] for r in staged['scopes']}
    if committed - known:
        raise ValueError('Journal contains an unknown scope')
    if scope_ids and not set(scope_ids) <= known:
        raise ValueError('Unknown selected scope ID')
    eligible = [r for r in staged['scopes'] if r['scope_id'] not in committed and (not scope_ids or r['scope_id'] in scope_ids)]
    pending = eligible if all_pending else eligible[:limit]
    results = []
    ownership_file = None
    if pending:
        # One narrow relationship inventory for the whole invocation. No whole
        # parent board, hidden financial board, or Supabase baseline traversal.
        ownership = reads.capture_ownership(connection, monday)
        ownership_file = f'ownership-apply-{uuid4()}.json'
        backfill.write_json(run_dir / ownership_file, ownership)
        owners = reads.owner_index(ownership)
        for position, record in enumerate(pending, 1):
            LOG.info('Apply scope %d/%d: %s (%d projects)', position, len(pending),
                     record['scope_id'], len(record['scope']['projects']))
            try:
                try:
                    reads.check_scope(monday, record, owners, mode=staged['mode'])
                except ValueError as exc:
                    raise ScopeConflict(str(exc)) from exc
                result = commit_scope(connection, manifest, staged, record)
            except ScopeConflict as exc:
                result = {'scope_id': record['scope_id'], 'status': 'deferred', 'reason': str(exc)}
            except (psycopg.errors.LockNotAvailable, psycopg.errors.DeadlockDetected,
                    psycopg.errors.SerializationFailure, psycopg.errors.QueryCanceled) as exc:
                result = {'scope_id': record['scope_id'], 'status': 'deferred', 'reason': type(exc).__name__}
            # Connection failures stop the batch. The database journal resolves an
            # uncertain commit on the next invocation, even without a local receipt.
            results.append(result)
            result['ownership_evidence'] = ownership_file
            backfill.write_json(run_dir / f"scope-{uuid4()}.json", result)
    newly_committed = sum(r['status'] in ('committed_pending_source_verification', 'already_committed') for r in results)
    summary = {'run_id': manifest['run_id'], 'results': results, 'previously_committed': len(committed),
               'remaining_unattempted': max(0, len(staged['scopes']) - len(committed) - len(pending)),
               'remaining_uncommitted': len(staged['scopes']) - len(committed) - newly_committed,
               'counts': dict(Counter(r['status'] for r in results)),
               'all_pending_selected': all_pending, 'ownership_evidence': ownership_file,
               'requires_fresh_source_verification': True}
    backfill.write_json(run_dir / f'apply-{uuid4()}.json', summary)
    return summary


def verify_run(connection, monday, run_dir):
    manifest, staged = load_run(run_dir)
    if manifest['target'] != backfill.target_fingerprint(connection):
        raise ValueError('Database target differs')
    with connection.transaction():
        connection.execute('SET TRANSACTION READ ONLY')
        schema_safety(connection)
        committed = committed_scopes(connection, manifest)
    # This second relationship inventory checks GLOBAL ownership after commit.
    # Cross-system atomicity is impossible: subsequent changes remain normal sync's
    # responsibility. A failed capture leaves all commits explicitly unverified.
    known = {r['scope_id'] for r in staged['scopes']}
    if committed - known:
        raise ValueError('Journal contains an unknown scope')
    ownership_file, owners = None, {}
    if committed:
        ownership = reads.capture_ownership(connection, monday)
        ownership_file = f'ownership-verify-{uuid4()}.json'
        backfill.write_json(run_dir / ownership_file, ownership)
        owners = reads.owner_index(ownership)
    results = []
    for record in staged['scopes']:
        result = {'scope_id': record['scope_id'], 'project_ids': record['scope']['projects']}
        if record['scope_id'] not in committed:
            result['status'] = 'not_committed'
        else:
            try:
                reads.check_scope(monday, record, owners, mode=staged['mode'])
                with connection.transaction():
                    connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
                    current = read_boundary(connection, record['boundary'], full=staged['mode'] == 'repair')
                result['status'] = 'verified' if current == record['after'] else 'changed_requires_reassessment'
            except ValueError as exc:
                result['status'] = 'changed_requires_reassessment'
                result['reason'] = str(exc)
        results.append(result)
    summary = {'run_id': manifest['run_id'], 'checked_at': datetime.now(timezone.utc).isoformat(),
               'complete': bool(results) and all(r['status'] == 'verified' for r in results) and not staged['deferred'],
               'completion_scope': 'selected_scopes_only', 'certifies_entire_dataset': False,
               'counts': dict(Counter(r['status'] for r in results)), 'results': results,
               'ownership_evidence': ownership_file,
               'deferred_at_staging': staged['deferred'], 'original_capture_summary': staged['origin_summary']}
    backfill.write_json(run_dir / f'verify-{uuid4()}.json', summary)
    return summary


def argument_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    commands.add_parser('preflight', help='Read-only schema and journal readiness check')
    report = commands.add_parser('reassess', help='Offline comparison of two validated captures')
    report.add_argument('--previous-run', type=Path, required=True)
    report.add_argument('--capture-dir', type=Path, required=True)
    report.add_argument('--output-dir', type=Path, required=True)
    stage = commands.add_parser('stage', help='Read-only scoped staging; never applies database changes')
    stage.add_argument('--capture-dir', type=Path, required=True)
    stage.add_argument('--run-dir', type=Path, required=True)
    stage.add_argument('--mode', choices=['orders', 'repair'], required=True)
    selection = stage.add_mutually_exclusive_group(required=True)
    selection.add_argument('--project-id', action='append', default=[])
    selection.add_argument('--all-verified', action='store_true')
    apply = commands.add_parser('apply', help='Apply reviewed scopes with one relationship inventory per invocation')
    apply.add_argument('--run-dir', type=Path, required=True)
    apply.add_argument('--confirm-run-id', required=True)
    apply.add_argument('--allow-partial', action='store_true')
    apply.add_argument('--allow-repair-fields', action='store_true')
    batch = apply.add_mutually_exclusive_group()
    batch.add_argument('--limit', type=int, help='Maximum scopes to attempt (default 10, maximum 100)')
    batch.add_argument('--all-pending', action='store_true', help='Attempt every selected uncommitted scope; retain bounded transactions')
    apply.add_argument('--scope-id', action='append', default=[], help='Select reviewed scopes explicitly, including to pass deferred scopes')
    verify = commands.add_parser('verify', help='Read-only scope verification and one fresh relationship inventory')
    verify.add_argument('--run-dir', type=Path, required=True)
    probe = commands.add_parser('check-owner-filter', help='Read-only comparison of filtered owners with a complete link scan; does not enable filtered apply')
    probe.add_argument('--run-dir', type=Path, required=True)
    probe.add_argument('--output', type=Path, required=True)
    return parser


def main(argv=None):
    args = argument_parser().parse_args(argv)
    backfill.load_dotenv()
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('src.database.sync_service').setLevel(logging.WARNING)
    try:
        if args.command == 'reassess':
            result = legacy.reassess(args.previous_run, args.capture_dir, args.output_dir)
        elif args.command == 'check-owner-filter':
            _, staged = load_run(args.run_dir)
            if args.output.exists():
                raise ValueError('Use a new diagnostic output filename')
            monday = backfill.MondayClient()
            scan = reads.scan_links(monday)
            hidden_ids = {hid for record in staged['scopes'] for hid in record['scope']['hidden_items']}
            result = reads.compare_owner_filter(monday, scan, hidden_ids)
            backfill.write_json(args.output, {'scan': scan, 'comparison': result})
        else:
            dsn = os.environ.get('SUPABASE_DB_URL')
            if not dsn:
                raise ValueError('SUPABASE_DB_URL is required; do not place credentials in command arguments')
            with psycopg.connect(dsn, autocommit=True, connect_timeout=15) as connection:
                if args.command == 'preflight':
                    with connection.transaction():
                        connection.execute('SET TRANSACTION READ ONLY')
                        result = schema_safety(connection, require_journal=False)
                elif args.command == 'stage':
                    result = stage_run(connection, backfill.MondayClient() if args.mode == 'repair' else None,
                        args.capture_dir, args.run_dir, mode=args.mode, project_ids=set(args.project_id), all_verified=args.all_verified)
                elif args.command == 'apply':
                    result = apply_run(connection, backfill.MondayClient(), args.run_dir, confirm_run_id=args.confirm_run_id,
                        allow_partial=args.allow_partial, allow_repair=args.allow_repair_fields,
                        limit=args.limit, all_pending=args.all_pending, scope_ids=args.scope_id)
                else:
                    result = verify_run(connection, backfill.MondayClient(), args.run_dir)
        print(json.dumps(result, indent=2))
        if (result.get('complete') is False or result.get('matches') is False or result.get('deferred_scopes', 0)
                or any(r.get('status') == 'deferred' for r in result.get('results', []))):
            return 2
        return 0
    except ValueError as exc:
        LOG.error('Order scopes stopped: %s', exc)
    except Exception as exc:
        LOG.error('Order scopes stopped (%s). No completion is assumed. The database journal resolves uncertain commits; '
                  'rerun apply with the same reviewed run, then verify.', type(exc).__name__)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
