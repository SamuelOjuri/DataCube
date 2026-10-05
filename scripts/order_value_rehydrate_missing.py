"""Insert explicitly selected missing subitems without updating any existing row.

Exact-ID Monday reads and production row transformations only. No board scans,
parent rollups, hidden-item inserts, upserts, or lifecycle/name inference.
"""
from __future__ import annotations

import argparse
from collections import Counter
from copy import deepcopy
import hashlib
import json
import logging
import os
from pathlib import Path
from uuid import uuid4

import psycopg
from psycopg.types.json import Jsonb

from scripts import backfill_order_values as backfill
from scripts import order_value_monday_compare as compare
from scripts import order_value_rehydrate as legacy
from scripts import order_value_scopes as scopes
from scripts import reconcile_order_values as reconcile

LOG = logging.getLogger(__name__)
WORKFLOW = 'missing-subitems-insert-only-v1'
DEFAULT_TARGETS = Path(__file__).with_name('order_value_missing_subitems_2.json')
TARGET_FIELDS = {'project_id', 'subitem_id', 'hidden_item_id'}


def code_fingerprint():
    return hashlib.sha256((compare.code_fingerprint() + legacy.code_fingerprint()).encode()
                          + Path(__file__).read_bytes()).hexdigest()


def validate_targets(targets):
    if not isinstance(targets, list) or not 1 <= len(targets) <= scopes.MAX_PROJECTS:
        raise ValueError('Select 1-25 explicit subitem/parent/source triples')
    for target in targets:
        if not isinstance(target, dict) or set(target) != TARGET_FIELDS:
            raise ValueError('Each target requires project_id, subitem_id and hidden_item_id only')
        for value in target.values():
            compare.validate_ids([value])
    compare.validate_ids([t['subitem_id'] for t in targets])
    return sorted(targets, key=lambda t: t['subitem_id'])


def boundary_for(targets):
    return {table: sorted({t[field] for t in targets}) for table, field in (
        ('projects', 'project_id'), ('subitems', 'subitem_id'), ('hidden_items', 'hidden_item_id'))}


def required_columns():
    return {
        'projects': sorted(compare.PARENT_COLUMNS[f] for f in ('project_name', 'pipeline_stage')),
        'subitems': sorted(set(reconcile.get_subitems_extraction_columns()) - {'name'}),
        'hidden_items': sorted(set(reconcile.get_hidden_items_extraction_columns()) - {'name'}),
    }


def validate_source(targets, source):
    boundary = boundary_for(targets)
    required = required_columns()
    for table in scopes.TABLES:
        if set(source[table]) != set(boundary[table]):
            raise ValueError(f'Incomplete exact-ID Monday evidence for {table}')
        if not set(required[table]) <= set(source['read_columns'][table]):
            raise ValueError(f'Incomplete production extraction columns for {table}')
        for item_id, item in source[table].items():
            if (item.get('id') != item_id or item.get('state') != 'active'
                    or (item.get('board') or {}).get('id') != compare.BOARDS[table]
                    or not isinstance(item.get('name'), str) or not item.get('updated_at')):
                raise ValueError(f'Inactive, misplaced or incomplete Monday {table} item')
            columns = backfill.indexed(item['column_values'], 'id')
            if set(columns) != set(source['read_columns'][table]) or any(
                    not {'value', 'text', 'type'} <= value.keys() for value in columns.values()):
                raise ValueError(f'Missing requested Monday columns for {table}')
    for target in targets:
        pid, cid, hid = (target[f] for f in ('project_id', 'subitem_id', 'hidden_item_id'))
        parent, child = source['projects'][pid], source['subitems'][cid]
        if parent.get('parent_item') is not None or (child.get('parent_item') or {}).get('id') != pid:
            raise ValueError('Monday parent relationship differs from selected target')
        if not isinstance(parent.get('subitems'), list):
            raise ValueError('Monday parent membership is incomplete')
        children = backfill.indexed(parent['subitems'], 'id')
        if cid not in children or (children[cid].get('parent_item') or {}).get('id') != pid:
            raise ValueError('Selected subitem is absent from current Monday parent membership')
        if compare.links(child) != [hid]:
            raise ValueError('Monday hidden-source link differs from selected target; no inferred relinks')


def capture(monday, targets):
    targets = validate_targets(targets)
    boundary, columns = boundary_for(targets), required_columns()
    parents = compare.fetch_items(monday, boundary['projects'], columns['projects'], parents=True, mirror_depth=0)
    children = compare.fetch_items(monday, boundary['subitems'], columns['subitems'])
    # Follow settings for the financial/date/status mirrors we verify. Other
    # production metadata uses its ordinary direct/display-value transformation.
    checked_fields = ('quote_amount', 'amount_invoiced', *compare.DATE_FIELDS, 'order_status')
    checked_ids = {compare.SUBITEM_COLUMNS[f] for f in checked_fields}
    mirrors = [{**item, 'column_values': [c for c in item['column_values'] if c['id'] in checked_ids]}
               for item in children.values()]
    sources, extra = compare.mirror_dependencies(mirrors, backfill.HIDDEN_ITEMS_BOARD_ID)
    if sources - set(boundary['hidden_items']):
        raise ValueError('A checked mirror points outside the explicitly selected hidden sources')
    columns['hidden_items'] = sorted(set(columns['hidden_items']) | extra)
    hidden = compare.fetch_items(monday, boundary['hidden_items'], columns['hidden_items'], mirror_depth=0)
    source = {'projects': parents, 'subitems': children, 'hidden_items': hidden, 'read_columns': columns}
    validate_source(targets, source)
    return source


def transform_children(targets, source, contract):
    """Reuse production extraction, but never call its parent rollup methods."""
    validate_source(targets, source)
    service = backfill.DataSyncService.__new__(backfill.DataSyncService)
    service.label_normalizer = reconcile.LabelNormalizer()
    service.mirror_resolver = reconcile.EnhancedMirrorResolver()
    for name in ('_hidden_lookup_by_id', '_hidden_lookup_by_name',
                 '_hidden_lookup_by_normalized_name', '_hidden_lookup_by_prefix'):
        setattr(service, name, {})
    hidden = service._transform_for_hidden_table(deepcopy(list(source['hidden_items'].values())))
    if set(backfill.indexed(hidden)) != set(source['hidden_items']):
        raise ValueError('Production transformation omitted a hidden source')
    # The selected explicit ID is sufficient; prevent metadata name fallbacks.
    service._hidden_lookup_by_name = {}
    service._hidden_lookup_by_normalized_name = {}
    service._hidden_lookup_by_prefix = {}
    rows = service._transform_for_subitems_table(deepcopy(list(source['subitems'].values())))
    indexed = backfill.indexed(rows)
    if set(indexed) != {t['subitem_id'] for t in targets}:
        raise ValueError('Production transformation omitted a selected child')
    for target in targets:
        cid, pid, hid = (target[f] for f in ('subitem_id', 'project_id', 'hidden_item_id'))
        row, child, hidden_item = indexed[cid], source['subitems'][cid], source['hidden_items'][hid]
        if row.get('parent_monday_id') != pid or row.get('hidden_item_id') != hid:
            raise ValueError('Production transformation changed an explicit relationship')
        row.pop('last_synced_at', None)
        row['item_name'] = child['name']
        # Preserve authoritative blanks and exact decimals instead of fallback
        # amounts or formatted mirror summaries from production normalization.
        for field in backfill.ORDER_FIELDS:
            row[field] = compare.numeric(compare.col(hidden_item, compare.HIDDEN_ITEMS_COLUMNS[field]))
        for field in ('quote_amount', 'amount_invoiced', 'new_enquiry_value'):
            row[field] = compare.numeric(compare.resolved_col(source, child, compare.SUBITEM_COLUMNS[field]))
        for field in compare.DATE_FIELDS:
            row[field] = compare.scalar(compare.resolved_col(source, child, compare.SUBITEM_COLUMNS[field]), 'date')
        row['order_status'] = compare.scalar(
            compare.resolved_col(source, child, compare.SUBITEM_COLUMNS['order_status']), 'status')
    return reconcile.normalize_updates({'subitems': [indexed[t['subitem_id']] for t in targets]}, contract)['subitems']


def build_record(targets, source, before, contract, defaults):
    targets = validate_targets(targets)
    boundary = boundary_for(targets)
    for table in ('projects', 'hidden_items'):
        if set(backfill.indexed(before[table])) != set(boundary[table]):
            raise scopes.ScopeConflict(f'Existing {table} rows are required; this workflow only inserts subitems')
    proposed = transform_children(targets, source, contract)
    existing = backfill.indexed(before['subitems'])
    inserts, present = [], []
    for row in proposed:
        if row['monday_id'] in existing:
            if any(existing[row['monday_id']].get(f) != v for f, v in row.items()):
                raise scopes.ScopeConflict('An existing target differs from Monday; use comparison, not insertion')
            present.append(row['monday_id'])
        else:
            inserts.append(row)
    writes = {'projects': [], 'hidden_items': [], 'subitems': inserts}
    legacy.validate_insert_fields(writes, defaults)
    after = deepcopy(before)
    after['subitems'] = scopes.stable_rows(after['subitems'] + inserts)
    if sum(map(len, after.values())) > scopes.MAX_SCOPE_ROWS:
        raise scopes.ScopeConflict('Insertion boundary exceeds the 500-row transaction limit')
    return {'scope_id': 'missing-' + backfill.fingerprint(targets)[:16], 'targets': targets,
            'boundary': boundary, 'source': source, 'before': before, 'after': after,
            'inserts': writes, 'already_present': present}


def stage_run(connection, monday, run_dir, targets):
    if run_dir.exists():
        raise ValueError('Use a new run directory; preserve earlier receipts')
    targets = validate_targets(targets)
    source = capture(monday, targets)
    with connection.transaction():
        connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
        safety = scopes.schema_safety(connection)
        contract, defaults = reconcile.read_contract(connection), legacy.insert_contract(connection)
        before = scopes.read_boundary(connection, boundary_for(targets), full=True)
    record = build_record(targets, source, before, contract, defaults)
    return save_run(run_dir, {'targets': targets, 'contract': contract, 'insert_contract': defaults,
                             'scopes': [record]}, backfill.target_fingerprint(connection), safety)


def save_run(run_dir, staged, target, safety):
    run_dir.mkdir(parents=True, exist_ok=False)
    backfill.write_json(run_dir / 'inserts.json', staged)
    changes = [{'scope_id': r['scope_id'], 'operation': 'insert', 'table': 'subitems',
                'monday_id': row['monday_id'], 'field': field, 'after': value}
               for r in staged['scopes'] for row in r['inserts']['subitems'] for field, value in row.items()]
    reconcile.write_csv(run_dir / 'changes.csv', changes,
                        ['scope_id', 'operation', 'table', 'monday_id', 'field', 'after'])
    manifest = {'workflow': WORKFLOW, 'run_id': str(uuid4()), 'prepared_at': compare.now(),
                'target': target, 'code': code_fingerprint(), 'sha256': backfill.fingerprint(staged),
                'review_sha256': hashlib.sha256((run_dir / 'changes.csv').read_bytes()).hexdigest(),
                'selected_subitems': len(staged['targets']),
                'selected_projects': len(boundary_for(staged['targets'])['projects']),
                'scopes': len(staged['scopes']),
                'insert_rows': {'projects': 0, 'hidden_items': 0,
                                'subitems': sum(len(r['inserts']['subitems']) for r in staged['scopes'])},
                'already_present': sum(len(r['already_present']) for r in staged['scopes']),
                'update_rows': 0, 'review_entries': len(changes), 'safety': safety}
    backfill.write_json(run_dir / 'manifest.json', manifest)
    load_run(run_dir)
    return manifest


def load_run(run_dir):
    manifest = json.loads((run_dir / 'manifest.json').read_text(encoding='utf-8'))
    staged = json.loads((run_dir / 'inserts.json').read_text(encoding='utf-8'))
    if (manifest['workflow'] != WORKFLOW or manifest['code'] != code_fingerprint()
            or manifest['sha256'] != backfill.fingerprint(staged)
            or manifest['review_sha256'] != hashlib.sha256((run_dir / 'changes.csv').read_bytes()).hexdigest()):
        raise ValueError('Code or reviewed artifacts changed; stage a new run')
    targets = validate_targets(staged['targets'])
    if len(staged['scopes']) != 1 or staged['scopes'][0]['targets'] != targets:
        raise ValueError('Every selected target must be in the single bounded transaction')
    record = staged['scopes'][0]
    if build_record(targets, record['source'], record['before'], staged['contract'], staged['insert_contract']) != record:
        raise ValueError('Insert plan differs from reviewed Monday evidence; existing updates are forbidden')
    if (manifest['scopes'] != 1 or manifest['selected_subitems'] != len(targets)
            or manifest['selected_projects'] != len(record['boundary']['projects'])
            or manifest['already_present'] != len(record['already_present']) or manifest['update_rows'] != 0
            or manifest['insert_rows'] != {'projects': 0, 'hidden_items': 0, 'subitems': len(record['inserts']['subitems'])}
            or manifest['review_entries'] != sum(len(row) for row in record['inserts']['subitems'])):
        raise ValueError('Manifest summary differs from reviewed inserts')
    return manifest, staged


def validate_actual(actual, record):
    for table in scopes.TABLES:
        current, wanted = backfill.indexed(actual[table]), backfill.indexed(record['after'][table])
        previous = backfill.indexed(record['before'][table])
        if current.keys() != wanted.keys():
            raise scopes.ScopeConflict('Post-insert row membership differs from review')
        for item_id, expected in wanted.items():
            if (item_id in previous and current[item_id] != expected) or any(
                    current[item_id].get(f) != v for f, v in expected.items()):
                raise scopes.ScopeConflict(f'Post-insert values differ for {table} {item_id}')


def check_source(monday, record):
    if capture(monday, record['targets']) != record['source']:
        raise scopes.ScopeConflict('Monday changed since staging; restage without overwriting Finance edits')


def commit_scope(connection, manifest, staged, record):
    if record['inserts']['projects'] or record['inserts']['hidden_items']:
        raise ValueError('This workflow can insert only subitems')
    with connection.transaction():
        connection.execute('SET TRANSACTION ISOLATION LEVEL READ COMMITTED')
        connection.execute("SET LOCAL lock_timeout='750ms'")
        connection.execute("SET LOCAL statement_timeout='4s'")
        connection.execute("SET LOCAL transaction_timeout='10s'")
        connection.execute("SET LOCAL idle_in_transaction_session_timeout='5s'")
        connection.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))',
                           (manifest['run_id'] + ':' + record['scope_id'],))
        if record['scope_id'] in scopes.committed_scopes(connection, manifest):
            return 'already_committed'
        if not record['inserts']['subitems']:
            raise ValueError('No-op scopes must not be committed')
        connection.execute('LOCK TABLE public.projects, public.hidden_items, public.subitems IN SHARE ROW EXCLUSIVE MODE')
        scopes.schema_safety(connection)
        if (reconcile.read_contract(connection) != staged['contract']
                or legacy.insert_contract(connection) != staged['insert_contract']):
            raise scopes.ScopeConflict('Database schema or defaults changed since staging')
        current = scopes.read_boundary(connection, record['boundary'], full=True)
        if current != record['before']:
            raise scopes.ScopeConflict('Database values, membership or target absence changed since staging')
        # INSERT only, no ON CONFLICT and no UPDATE of existing rows.
        counts = legacy.write_inserts(connection, record['inserts'])
        actual = scopes.read_boundary(connection, record['boundary'], full=True)
        validate_actual(actual, record)
        connection.execute('INSERT INTO public.order_value_scope_commits '
            '(run_id,scope_id,plan_sha256,mode,project_ids,before_sha256,after_sha256,updated_rows) '
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s)',
            (manifest['run_id'], record['scope_id'], manifest['sha256'], 'repair', record['boundary']['projects'],
             backfill.fingerprint(current), backfill.fingerprint(actual),
             Jsonb({'inserted': counts, 'updated': {t: 0 for t in scopes.TABLES}})))
    return 'committed_pending_verification'


def execute_run(connection, monday, run_dir, *, apply=False, confirm_run_id=None, allow_rehydration=False):
    manifest, staged = load_run(run_dir)
    if manifest['target'] != backfill.target_fingerprint(connection):
        raise ValueError('Database target differs from staging')
    if apply and (confirm_run_id != manifest['run_id'] or not allow_rehydration):
        raise ValueError('Apply requires the reviewed run ID and --allow-rehydration')
    with connection.transaction():
        connection.execute('SET TRANSACTION READ ONLY')
        scopes.schema_safety(connection)
        committed = scopes.committed_scopes(connection, manifest)
    changed = {r['scope_id'] for r in staged['scopes'] if r['inserts']['subitems']}
    if committed - changed:
        raise ValueError('Unexpected journal scopes')
    results = []
    for record in staged['scopes']:
        sid = record['scope_id']
        result = {'scope_id': sid, 'subitem_ids': record['boundary']['subitems']}
        try:
            if apply and sid in committed:
                result['status'] = 'already_committed'
            elif not apply and sid in changed and sid not in committed:
                result['status'] = 'not_committed'
            else:
                check_source(monday, record)
                if apply and sid in changed:
                    commit_scope(connection, manifest, staged, record)
                    check_source(monday, record)
                    result['status'] = 'committed_source_rechecked'
                else:
                    with connection.transaction():
                        connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
                        actual = scopes.read_boundary(connection, record['boundary'], full=True)
                        validate_actual(actual, record)
                        if sid in changed:
                            journal = connection.execute('SELECT after_sha256 FROM public.order_value_scope_commits '
                                'WHERE run_id=%s AND scope_id=%s AND plan_sha256=%s',
                                (manifest['run_id'], sid, manifest['sha256'])).fetchone()
                            if not journal or journal[0] != backfill.fingerprint(actual):
                                raise scopes.ScopeConflict('Database differs from the committed journal')
                    check_source(monday, record)
                    result['status'] = 'verified' if sid in changed else 'verified_no_changes'
        except ValueError as exc:
            result.update(status='requires_reassessment', reason=str(exc))
        except (psycopg.errors.LockNotAvailable, psycopg.errors.DeadlockDetected,
                psycopg.errors.SerializationFailure, psycopg.errors.QueryCanceled, psycopg.IntegrityError) as exc:
            result.update(status='requires_reassessment', reason=type(exc).__name__)
        results.append(result)
        backfill.write_json(run_dir / f'receipt-{uuid4()}.json', result)
    remaining = changed - scopes.committed_scopes(connection, manifest)
    success = {'already_committed', 'committed_source_rechecked', 'verified_no_changes'} if apply else {'verified', 'verified_no_changes'}
    summary = {'run_id': manifest['run_id'], 'action': 'apply' if apply else 'verify', 'checked_at': compare.now(),
               'results': results, 'counts': dict(Counter(r['status'] for r in results)),
               'remaining_uncommitted': len(remaining),
               'staged_changes_successful': not remaining and all(r['status'] in success for r in results),
               'certifies_entire_dataset': False}
    backfill.write_json(run_dir / f'{summary["action"]}-{uuid4()}.json', summary)
    return summary


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    stage = commands.add_parser('stage', help='Read-only evidence capture and insertion plan')
    stage.add_argument('--targets-file', type=Path, default=DEFAULT_TARGETS)
    stage.add_argument('--run-dir', type=Path, required=True)
    for name in ('apply', 'verify'):
        command = commands.add_parser(name)
        command.add_argument('--run-dir', type=Path, required=True)
        if name == 'apply':
            command.add_argument('--confirm-run-id', required=True)
            command.add_argument('--allow-rehydration', action='store_true')
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('src.database.sync_service').setLevel(logging.WARNING)
    try:
        targets = validate_targets(json.loads(args.targets_file.read_text(encoding='utf-8-sig'))) if args.command == 'stage' else None
        backfill.load_dotenv()
        dsn = os.environ.get('SUPABASE_DB_URL')
        if not dsn:
            raise ValueError('SUPABASE_DB_URL is required; never pass credentials in command arguments')
        with psycopg.connect(dsn, autocommit=True, connect_timeout=15) as connection:
            monday = backfill.MondayClient()
            if args.command == 'stage':
                result = stage_run(connection, monday, args.run_dir, targets)
                success = True
            else:
                result = execute_run(connection, monday, args.run_dir, apply=args.command == 'apply',
                    confirm_run_id=getattr(args, 'confirm_run_id', None),
                    allow_rehydration=getattr(args, 'allow_rehydration', False))
                success = result['staged_changes_successful']
        print(json.dumps(result, indent=2))
        return 0 if success else 2
    except Exception as exc:
        LOG.error('%s', compare.failure_message(args.command, exc))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
