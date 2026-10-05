"""Stage, apply and verify bounded order corrections while other projects remain writable.

Legacy backfill/repair commands retain their maintenance-window contracts. This
workflow requires validated, immediate foreign keys and PostgreSQL 17+; it never
installs its journal or weakens schema safeguards automatically.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
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
from scripts import reconcile_order_values as reconcile

LOG = logging.getLogger(__name__)
VERSION = 1
MAX_PROJECTS = 25
MAX_SCOPE_ROWS = 500
TRANSACTION_TIMEOUT = '10s'
TABLES = ('projects', 'hidden_items', 'subitems')
VOLATILE_FIELDS = {'updated_at', 'last_synced_at'}
SCHEMA_PATH = Path(__file__).resolve().parents[1] / 'src/database/schema/order_value_scope_commits.sql'


class ScopeConflict(ValueError):
    """A reviewed scope needs new evidence; never silently rebuild its updates."""


def code_fingerprint():
    root = Path(__file__).resolve().parents[1]
    files = [Path(__file__), root / 'scripts/reconcile_order_values.py',
             root / 'src/core/data_processor.py', root / 'src/core/monday_client.py',
             root / 'src/config.py', SCHEMA_PATH]
    return hashlib.sha256(backfill.code_fingerprint().encode() + b''.join(p.read_bytes() for p in files)).hexdigest()


def stable_rows(rows):
    return [{k: v for k, v in row.items() if k not in VOLATILE_FIELDS}
            for row in sorted(reconcile.json_value(rows), key=lambda row: row['monday_id'])]


def schema_safety(connection, *, require_journal=True):
    """The row-lock protocol depends on FK checks, full visibility and PG17 timeouts."""
    if connection.info.server_version < 170000:
        raise ValueError('PostgreSQL 17+ is required for the transaction duration limit')
    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute("""
            SELECT r.rolsuper OR r.rolbypassrls AS full_visibility,
                   current_setting('session_replication_role') AS replication_role,
                   to_regclass('public.order_value_scope_commits') IS NOT NULL AS journal
            FROM pg_roles r WHERE r.rolname = current_user
        """)
        access = cursor.fetchone()
        if not access['full_visibility'] or access['replication_role'] != 'origin':
            raise ValueError('Use a role with BYPASSRLS/full visibility and normal FK enforcement')
        if require_journal and not access['journal']:
            raise ValueError('Commit journal is absent; review and install order_value_scope_commits.sql first')
        cursor.execute("""
            SELECT c.oid, c.convalidated, c.condeferrable,
                   a.attname AS child_column, p.relname AS parent_table, b.attname AS parent_column,
                   NOT EXISTS (SELECT 1 FROM pg_trigger t WHERE t.tgconstraint = c.oid
                               AND t.tgenabled NOT IN ('O', 'A')) AS triggers_enabled
            FROM pg_constraint c
            JOIN pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=c.conkey[1]
            JOIN pg_class p ON p.oid=c.confrelid
            JOIN pg_namespace n ON n.oid=p.relnamespace
            JOIN pg_attribute b ON b.attrelid=c.confrelid AND b.attnum=c.confkey[1]
            WHERE c.conrelid='public.subitems'::regclass AND c.contype='f'
              AND cardinality(c.conkey)=1 AND cardinality(c.confkey)=1 AND n.nspname='public'
        """)
        keys = {(r['child_column'], r['parent_table'], r['parent_column']) for r in cursor.fetchall()
                if r['convalidated'] and not r['condeferrable'] and r['triggers_enabled']}
        required = {('parent_monday_id', 'projects', 'monday_id'), ('hidden_item_id', 'hidden_items', 'monday_id')}
        if not required <= keys:
            raise ValueError('Validated, nondeferrable, enabled parent and hidden-source foreign keys are required')
    return {'server_version': connection.info.server_version, 'foreign_keys_verified': True,
            'journal_installed': access['journal'], 'transaction_timeout': TRANSACTION_TIMEOUT}


def index_source(source):
    children = backfill.indexed(source['subitems'])
    parents, owners = defaultdict(list), defaultdict(list)
    for row in children.values():
        parents[row.get('parent_monday_id')].append(row)
        for item_id in row.get('hidden_ids', []):
            owners[item_id].append(row)
    details = source.get('exclusion_evidence', {}).get('parent_details', {}).get('items', [])
    return {'children': children, 'hidden': backfill.indexed(source['hidden_items']),
            'parents': parents, 'owners': owners, 'project_ids': set(source['project_ids']),
            'metadata': backfill.indexed(details, 'id')}


def source_evidence(source, scope, inventory=None):
    """Project membership plus GLOBAL live ownership; unrelated source changes are ignored."""
    inventory = inventory or index_source(source)
    hidden = inventory['hidden']
    parents = set(scope['projects'])
    if not parents <= inventory['project_ids']:
        raise ScopeConflict('A selected parent is missing from Monday')
    selected = [row for pid in parents for row in inventory['parents'][pid]]
    if {row['monday_id'] for row in selected} != set(scope['subitems']):
        raise ScopeConflict('Selected Monday child membership changed')
    fields = ('monday_id', 'parent_monday_id', 'hidden_ids', 'link_error', 'state', 'board_id',
              'parent_state', 'parent_board_id')
    selected = [{k: row.get(k) for k in fields} for row in selected]
    wanted = set(scope['hidden_items'])
    owner_rows = {row['monday_id']: row for hid in wanted for row in inventory['owners'][hid]}
    owners = [{k: row.get(k) for k in fields} for row in owner_rows.values()]
    amounts = []
    for hidden_id in sorted(wanted):
        row = hidden.get(hidden_id)
        if row is None or row.get('issues') or any(row.get(f) is None for f in backfill.ORDER_FIELDS):
            raise ScopeConflict('A selected hidden source is missing or invalid')
        amounts.append({k: row.get(k) for k in ('monday_id', *backfill.ORDER_FIELDS, 'monday_total', 'issues')})
    metadata = [inventory['metadata'][pid] for pid in parents if pid in inventory['metadata']]
    if len(metadata) != len(parents):
        raise ScopeConflict('Metadata-complete parent inventory is required')
    return {'projects': sorted(parents), 'parents': sorted(metadata, key=lambda row: str(row['id'])),
            'subitems': sorted(selected, key=lambda row: row['monday_id']), 'hidden_items': amounts,
            'owners': sorted(owners, key=lambda row: row['monday_id'])}


def boundary_from_baseline(baseline, scope, children=None):
    children = children if children is not None else backfill.indexed(baseline['subitems'])
    source_ids = set(scope['hidden_items'])
    source_ids.update(children[item_id].get('hidden_item_id') for item_id in scope['subitems'] if item_id in children)
    return {'projects': scope['projects'], 'subitems': scope['subitems'],
            'hidden_items': sorted(source_ids - {None, ''})}


def read_boundary(connection, boundary, *, lock=False, full=False):
    """Lock referenced keys BEFORE reading all their children and source owners.

    Immediate FK checks use KEY SHARE on the referenced parent/source. FOR UPDATE
    on these keys blocks inserts and incoming relinks, including from ordinary
    READ COMMITTED writers. Existing owners are then locked and re-read. A move
    observed mid-lock is rejected; it is never treated as a complete population.
    """
    result = {}
    with connection.cursor(row_factory=dict_row) as cursor:
        for table in TABLES:
            fields = sql.SQL('*') if full else sql.SQL(', ').join(map(sql.Identifier, backfill.BASELINE_COLUMNS[table]))
            if table == 'subitems':
                predicate = sql.SQL('monday_id = ANY(%s) OR parent_monday_id = ANY(%s) OR hidden_item_id = ANY(%s)')
                values = (boundary['subitems'], boundary['projects'], boundary['hidden_items'])
            else:
                predicate, values = sql.SQL('monday_id = ANY(%s)'), (boundary[table],)
            cursor.execute(sql.SQL('SELECT {} FROM public.{} WHERE {} ORDER BY monday_id{}').format(
                fields, sql.Identifier(table), predicate, sql.SQL(' FOR UPDATE' if lock else '')), values)
            result[table] = stable_rows(cursor.fetchall())
    return result


def index_baseline(baseline):
    result = {t: backfill.indexed(rows) for t, rows in baseline.items()}
    result['parents'], result['owners'] = defaultdict(set), defaultdict(set)
    for row in baseline['subitems']:
        result['parents'][row.get('parent_monday_id')].add(row['monday_id'])
        result['owners'][row.get('hidden_item_id')].add(row['monday_id'])
    return result


def select_boundary(index, boundary):
    ids = set(boundary['subitems'])
    for pid in boundary['projects']:
        ids.update(index['parents'][pid])
    for hid in boundary['hidden_items']:
        ids.update(index['owners'][hid])
    return {t: stable_rows([index[t][i] for i in (ids if t == 'subitems' else boundary[t]) if i in index[t]]) for t in TABLES}


def require_existing_scope(state, scope):
    # Missing-row insertion is deliberately left to the separately reviewed legacy
    # rehydration workflow: an absent referenced key cannot be row-locked.
    for table in TABLES:
        if set(scope[table]) - set(backfill.indexed(state[table])):
            raise ScopeConflict('Missing scoped rows require separate rehydration before online correction')
    if sum(len(rows) for rows in state.values()) > MAX_SCOPE_ROWS:
        raise ScopeConflict(f'Scope exceeds the {MAX_SCOPE_ROWS}-row transaction limit')


def expected_state(before, updates):
    result = json.loads(json.dumps(before))
    for table, rows in updates.items():
        indexed = backfill.indexed(result[table])
        for update in rows:
            if update['monday_id'] not in indexed:
                raise ScopeConflict('Online scopes cannot insert missing rows')
            indexed[update['monday_id']].update(update)
            if table == 'projects' and 'status_category' in indexed[update['monday_id']]:
                row = indexed[update['monday_id']]
                # Match the GENERATED ALWAYS expression in schema/schema.sql
                # exactly. Predict the read-back value; never add it to updates.
                # Won - Open, alternate labels and NULL all take ELSE 'Open'.
                row['status_category'] = (
                    'Won' if row.get('pipeline_stage') == 'Won - Closed (Invoiced)'
                    else 'Lost' if row.get('pipeline_stage') == 'Lost' else 'Open'
                )
            if table == 'projects' and 'invoicing_spread_days' in indexed[update['monday_id']]:
                row = indexed[update['monday_id']]
                first, last = row.get('first_date_invoiced'), row.get('last_date_invoiced')
                row['invoicing_spread_days'] = max((date.fromisoformat(last)-date.fromisoformat(first)).days, 0) if first and last else None
    return result


def check_ownership(state, scope):
    children = state['subitems']
    if {r['monday_id'] for r in children if r.get('parent_monday_id') in scope['projects']} != set(scope['subitems']):
        raise ScopeConflict('Database child membership differs from the reviewed complete parent scope')
    for hidden_id in scope['hidden_items']:
        users = [r for r in children if r.get('hidden_item_id') == hidden_id]
        if len(users) != 1 or users[0]['monday_id'] not in scope['subitems']:
            raise ScopeConflict('A source is shared outside the reviewed dependency group')


def check_parent_membership(state, scope, evidence):
    actual = {r['monday_id']: r.get('parent_monday_id') for r in state['subitems']
              if r['monday_id'] in scope['subitems'] or r.get('parent_monday_id') in scope['projects']}
    expected = {r['monday_id']: r['parent_monday_id'] for r in evidence['subitems']}
    if actual != expected:
        raise ScopeConflict('Stored child membership or parent changed; reassess all affected parents')


def order_updates(before, evidence, scope):
    # Rebuild from the same scoped evidence during both staging and loading. The
    # owners list is global for the selected sources; no shared source disappears
    # simply because its other parent was not selected.
    children = {r['monday_id']: r for r in evidence['owners'] + evidence['subitems']}
    source = {'project_ids': scope['projects'], 'subitems': list(children.values()),
              'hidden_items': evidence['hidden_items']}
    plan = backfill.build_plan(before, source)
    statuses = {r['project_id']: r['status'] for r in plan['projects']}
    if any(statuses.get(pid) != 'verified' for pid in scope['projects']):
        raise ScopeConflict('Project no longer satisfies order-only validation')
    return {t: [r for r in rows if r['monday_id'] in scope[t]] for t, rows in plan['updates'].items()}


def choose_groups(baseline, source, plan, mode, project_ids, all_verified=False):
    if mode == 'orders':
        candidates = {r['project_id'] for r in plan['projects'] if r['status'] == 'verified'}
        selected = candidates if all_verified else project_ids
        if not selected or selected - candidates:
            raise ValueError('Select verified project IDs, or explicitly select --all-verified')
        counts = Counter(r.get('parent_monday_id') for r in source['subitems'])
        groups, group, rows = [], [], 0
        for item_id in sorted(selected):
            size = 1 + 2 * counts[item_id]
            if group and (len(group) == MAX_PROJECTS or rows + size > MAX_SCOPE_ROWS):
                groups.append(group)
                group, rows = [], 0
            group.append(item_id)
            rows += size
        if group:
            groups.append(group)
        return groups
    if all_verified or not project_ids:
        raise ValueError('Repair mode requires explicit project IDs')
    groups = reconcile.build_report(baseline, source, plan)['repair_groups']
    selected = [g for g in groups if set(g['project_ids']) & project_ids]
    if set().union(*(set(g['project_ids']) for g in selected)) != project_ids:
        raise ValueError('Select each complete repair dependency group explicitly')
    if any(g['action'] != 'prepare_candidate' for g in selected):
        raise ValueError('Selected repair group requires manual resolution')
    return [g['project_ids'] for g in selected]


def stage_run(connection, monday, capture_dir, output_dir, *, mode, project_ids, all_verified=False):
    origin, baseline, source, plan = backfill.load_run(capture_dir)
    if not origin['approve_reviewed_parentless_duplicates']:
        raise ValueError('Use the metadata-complete reviewed inventory capture')
    if origin['target'] != backfill.target_fingerprint(connection):
        raise ValueError('Database target differs from the capture')
    groups = choose_groups(baseline, source, plan, mode, project_ids, all_verified)
    inventory = index_source(source)
    captured_children = backfill.indexed(baseline['subitems'])
    output_dir.mkdir(parents=True, exist_ok=False)
    records, deferred, changes = [], [], []
    with connection.transaction():
        connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
        safety = schema_safety(connection, require_journal=False)
        contract = reconcile.read_contract(connection)
        current_index = index_baseline(backfill.read_baseline(connection)) if mode == 'orders' else None
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
            if mode == 'orders':
                before = select_boundary(current_index, boundary)
            else:
                with connection.transaction():
                    connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
                    before = read_boundary(connection, boundary, full=True)
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
    manifest = {'version': VERSION, 'run_id': str(uuid4()), 'prepared_at': datetime.now(timezone.utc).isoformat(),
                'code': code_fingerprint(), 'target': origin['target'], 'source_contract': backfill.source_contract(),
                'sha256': backfill.fingerprint(staged), 'review_sha256': hashlib.sha256((output_dir / 'changes.csv').read_bytes()).hexdigest(),
                'mode': mode, 'scopes': len(records), 'deferred_scopes': len(deferred), 'changes': len(changes), 'safety': safety}
    backfill.write_json(output_dir / 'manifest.json', manifest)
    return manifest


def load_run(run_dir):
    manifest = json.loads((run_dir / 'manifest.json').read_text(encoding='utf-8'))
    staged = json.loads((run_dir / 'scopes.json').read_text(encoding='utf-8'))
    if (manifest['version'] != VERSION or manifest['code'] != code_fingerprint()
            or manifest['source_contract'] != backfill.source_contract()
            or manifest['sha256'] != backfill.fingerprint(staged)
            or manifest['review_sha256'] != hashlib.sha256((run_dir / 'changes.csv').read_bytes()).hexdigest()):
        raise ValueError('Code, mappings or reviewed artifacts changed; stage a new run')
    for record in staged['scopes']:
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


def targeted_orders(monday, record):
    scope = record['scope']
    parents = backfill.fetch_inventory_details(monday, scope['projects'], include_subitems=True)
    if parents['not_returned_ids']:
        raise ScopeConflict('Selected parent disappeared')
    child_rows = reconcile.fetch_columns(monday, scope['subitems'], [backfill.SUBITEM_COLUMNS['hidden_item_id']], backfill.SUBITEM_BOARD_ID)
    hidden_rows = reconcile.fetch_columns(monday, scope['hidden_items'],
        [backfill.HIDDEN_ITEMS_COLUMNS[f] for f in backfill.ORDER_FIELDS] + [backfill.TOTAL_COLUMN], backfill.HIDDEN_ITEMS_BOARD_ID)
    children = []
    for row in child_rows:
        parent = row.get('parent_item') or {}
        children.append({**backfill.normalize_subitem(row), 'board_id': row['board']['id'], 'state': row['state'],
                         'parent_board_id': (parent.get('board') or {}).get('id'), 'parent_state': parent.get('state')})
    source = {'project_ids': scope['projects'], 'subitems': children,
              'hidden_items': [backfill.normalize_hidden(row) for row in hidden_rows],
              'exclusion_evidence': {'parent_details': {'items': parents['items']}}}
    evidence = source_evidence(source, scope)
    for parent in evidence['parents']:
        parent['subitems'] = sorted(parent['subitems'], key=lambda row: str(row['id']))
    # Global ownership comes from the fresh complete capture, not this targeted read.
    evidence['owners'] = record['source']['owners']
    return evidence


def write_updates(connection, updates):
    """Set-based statements keep the locked interval independent of network RTT per row."""
    counts = {}
    with connection.cursor() as cursor:
        for table in ('hidden_items', 'subitems', 'projects'):
            groups = defaultdict(list)
            for row in updates[table]:
                groups[tuple(sorted(set(row) - {'monday_id'}))].append(row)
            counts[table] = 0
            for fields, rows in groups.items():
                if not fields:
                    continue
                statement = sql.SQL('UPDATE public.{table} AS target SET {assignments} '
                    'FROM jsonb_populate_recordset(NULL::public.{table}, %s) AS incoming '
                    'WHERE target.monday_id = incoming.monday_id RETURNING target.monday_id').format(
                    table=sql.Identifier(table), assignments=sql.SQL(', ').join(
                        sql.SQL('{} = incoming.{}').format(sql.Identifier(f), sql.Identifier(f)) for f in fields))
                cursor.execute(statement, (Jsonb(rows),))
                actual = {r[0] for r in cursor.fetchall()}
                if actual != {r['monday_id'] for r in rows} or len(actual) != len(rows):
                    raise ScopeConflict('Expected row coverage was not updated; rolling back')
                counts[table] += len(rows)
    return counts


def commit_scope(connection, manifest, staged, record):
    """No external API calls inside the bounded database transaction."""
    with connection.transaction():
        connection.execute('SET TRANSACTION ISOLATION LEVEL READ COMMITTED')
        connection.execute("SET LOCAL lock_timeout = '750ms'")
        connection.execute("SET LOCAL statement_timeout = '4s'")
        connection.execute("SET LOCAL transaction_timeout = '10s'")
        connection.execute("SET LOCAL idle_in_transaction_session_timeout = '5s'")
        connection.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))',
                           (manifest['run_id'] + ':' + record['scope_id'],))
        with connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute('SELECT plan_sha256 FROM public.order_value_scope_commits WHERE run_id=%s AND scope_id=%s',
                           (manifest['run_id'], record['scope_id']))
            committed = cursor.fetchone()
        if committed:
            if committed['plan_sha256'] != manifest['sha256']:
                raise ValueError('Journal entry belongs to different reviewed content')
            return {'scope_id': record['scope_id'], 'status': 'already_committed'}
        # Stabilize schema definitions without blocking ordinary data writes.
        connection.execute('LOCK TABLE public.projects, public.hidden_items, public.subitems IN ROW SHARE MODE')
        schema_safety(connection)
        if reconcile.read_contract(connection) != staged['contract']:
            raise ScopeConflict('Database schema changed since review')
        current = read_boundary(connection, record['boundary'], lock=True, full=staged['mode'] == 'repair')
        if current != record['before']:
            raise ScopeConflict('Scoped values, membership or source ownership changed; restage this scope')
        require_existing_scope(current, record['scope'])
        counts = write_updates(connection, record['updates'])
        actual = read_boundary(connection, record['boundary'], full=staged['mode'] == 'repair')
        if actual != record['after']:
            raise ScopeConflict('Post-write reconciliation failed; rolling back this scope')
        check_ownership(actual, record['scope'])
        connection.execute('INSERT INTO public.order_value_scope_commits '
            '(run_id, scope_id, plan_sha256, mode, project_ids, before_sha256, after_sha256, updated_rows) '
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s)',
            (manifest['run_id'], record['scope_id'], manifest['sha256'], staged['mode'], record['scope']['projects'],
             backfill.fingerprint(current), backfill.fingerprint(actual), Jsonb(counts)))
    return {'scope_id': record['scope_id'], 'status': 'committed_pending_source_verification', 'updated_rows': counts}


def committed_scopes(connection, manifest):
    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute('SELECT scope_id, plan_sha256 FROM public.order_value_scope_commits WHERE run_id=%s', (manifest['run_id'],))
        rows = cursor.fetchall()
    if any(r['plan_sha256'] != manifest['sha256'] for r in rows):
        raise ValueError('Commit journal does not match the reviewed plan')
    return {r['scope_id'] for r in rows}


def fresh_capture(connection, monday):
    source = backfill.capture_source_with_reviewed_duplicates(monday)
    with connection.transaction():
        connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
        baseline = backfill.read_baseline(connection)
    backfill.validate_reviewed_duplicates(baseline, source)
    return source


def apply_run(connection, monday, run_dir, *, confirm_run_id, allow_partial=False, allow_repair=False, limit=10, scope_ids=None):
    manifest, staged = load_run(run_dir)
    if manifest['run_id'] != confirm_run_id or manifest['target'] != backfill.target_fingerprint(connection):
        raise ValueError('Run confirmation or target differs')
    if not allow_partial:
        raise ValueError('Online batches require explicit --allow-partial acknowledgement')
    if staged['mode'] == 'repair' and not allow_repair:
        raise ValueError('Repair changes relationships, invoice/enquiry values and dates; use --allow-repair-fields after review')
    if not 1 <= limit <= 100:
        raise ValueError('Select a batch limit between 1 and 100 scopes')
    with connection.transaction():
        connection.execute('SET TRANSACTION READ ONLY')
        schema_safety(connection)
        committed = committed_scopes(connection, manifest)
    known = {r['scope_id'] for r in staged['scopes']}
    if scope_ids and not set(scope_ids) <= known:
        raise ValueError('Unknown selected scope ID')
    eligible = [r for r in staged['scopes'] if r['scope_id'] not in committed and (not scope_ids or r['scope_id'] in scope_ids)]
    pending = eligible[:limit]
    results = []
    if pending:
        fresh = fresh_capture(connection, monday)
        inventory = index_source(fresh)
        for record in pending:
            try:
                if source_evidence(fresh, record['scope'], inventory) != record['source']:
                    raise ScopeConflict('Monday values, membership or global ownership changed; restage this scope')
                try:
                    if targeted_orders(monday, record) != record['source']:
                        raise ScopeConflict('Selected Monday evidence changed after inventory capture')
                    if staged['mode'] == 'repair' and reconcile.capture_targeted(monday, fresh, record['scope']) != record['raw']:
                        raise ScopeConflict('Targeted repair inputs changed since review')
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
            backfill.write_json(run_dir / f"scope-{uuid4()}.json", result)
    summary = {'run_id': manifest['run_id'], 'results': results, 'previously_committed': len(committed),
               'remaining_unattempted': max(0, len(staged['scopes']) - len(committed) - len(pending)),
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
    # This second complete capture checks GLOBAL Monday ownership after commit.
    # Cross-system atomicity is impossible: subsequent changes remain normal sync's
    # responsibility. A failed capture leaves all commits explicitly unverified.
    fresh = fresh_capture(connection, monday)
    inventory = index_source(fresh)
    results = []
    for record in staged['scopes']:
        result = {'scope_id': record['scope_id'], 'project_ids': record['scope']['projects']}
        if record['scope_id'] not in committed:
            result['status'] = 'not_committed'
        else:
            try:
                source_matches = source_evidence(fresh, record['scope'], inventory) == record['source']
                source_matches = source_matches and targeted_orders(monday, record) == record['source']
                if source_matches and staged['mode'] == 'repair':
                    source_matches = reconcile.capture_targeted(monday, fresh, record['scope']) == record['raw']
                with connection.transaction():
                    connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
                    current = read_boundary(connection, record['boundary'], full=staged['mode'] == 'repair')
                result['status'] = 'verified' if source_matches and current == record['after'] else 'changed_requires_reassessment'
            except ValueError:
                result['status'] = 'changed_requires_reassessment'
        results.append(result)
    summary = {'run_id': manifest['run_id'], 'checked_at': datetime.now(timezone.utc).isoformat(),
               'complete': bool(results) and all(r['status'] == 'verified' for r in results) and not staged['deferred'],
               'completion_scope': 'selected_scopes_only', 'certifies_entire_dataset': False,
               'counts': dict(Counter(r['status'] for r in results)), 'results': results,
               'deferred_at_staging': staged['deferred'], 'original_capture_summary': staged['origin_summary']}
    backfill.write_json(run_dir / f'verify-{uuid4()}.json', summary)
    return summary


def reassess(previous_dir, capture_dir, output_dir):
    old_manifest, _, _, old_plan = backfill.load_run(previous_dir)
    new_manifest, baseline, source, new_plan = backfill.load_run(capture_dir)
    summary = reconcile.report_run(capture_dir, output_dir)
    readiness = online_readiness(baseline, source, new_plan)
    backfill.write_json(output_dir / 'online-readiness.json', readiness)
    reconcile.write_csv(output_dir / 'online-readiness.csv', readiness,
                        ['group_id', 'project_ids', 'readiness', 'reason'])
    previous = {r['project_id']: r for r in old_plan['projects']}
    current = {r['project_id']: r for r in new_plan['projects']}
    changes = [{'project_id': pid, 'previous_status': previous.get(pid, {}).get('status', 'absent'),
                'current_status': current.get(pid, {}).get('status', 'absent'),
                'previous_issues': previous.get(pid, {}).get('issues', []),
                'current_issues': current.get(pid, {}).get('issues', [])}
               for pid in sorted(previous.keys() | current.keys())
               if previous.get(pid, {}).get('status') == 'blocked' or current.get(pid, {}).get('status') == 'blocked']
    reconcile.write_csv(output_dir / 'blocked-changes.csv', changes,
                        ['project_id', 'previous_status', 'current_status', 'previous_issues', 'current_issues'])
    result = {'previous_run_id': old_manifest['run_id'], 'capture_run_id': new_manifest['run_id'],
              'captured_at': new_manifest['prepared_at'], 'reconciliation': summary,
              'transitions': dict(Counter(r['previous_status'] + ' -> ' + r['current_status'] for r in changes)),
              'online_repair_project_counts': dict(sum((Counter({r['readiness']: len(r['project_ids'])}) for r in readiness), Counter())),
              'capture_summary': new_manifest['summary']}
    backfill.write_json(output_dir / 'reassessment.json', result)
    return result


def online_readiness(baseline, source, plan):
    """Distinguish legacy rehydration candidates from lockable existing-row repairs."""
    report = reconcile.build_report(baseline, source, plan)
    inventory, stored = index_source(source), index_baseline(baseline)
    result = []
    for group in report['repair_groups']:
        record = {'group_id': group['group_id'], 'project_ids': group['project_ids']}
        if group['action'] != 'prepare_candidate':
            record.update(readiness='manual_review', reason='; '.join(group['blockers']))
        else:
            children = [r for pid in group['project_ids'] for r in inventory['parents'][pid]]
            scope = {'projects': group['project_ids'], 'subitems': sorted(r['monday_id'] for r in children),
                     'hidden_items': sorted({r['hidden_ids'][0] for r in children})}
            boundary = boundary_from_baseline(baseline, scope, stored['subitems'])
            before = select_boundary(stored, boundary)
            try:
                require_existing_scope(before, scope)
                check_parent_membership(before, scope, source_evidence(source, scope, inventory))
                record.update(readiness='stage_candidate', reason='Stage the complete group in repair mode and review all field changes')
            except ScopeConflict as exc:
                record.update(readiness='separate_rehydration_or_review', reason=str(exc))
        result.append(record)
    for project in report['projects']:
        if project['action'] == 'manual_review':
            result.append({'group_id': None, 'project_ids': [project['project_id']], 'readiness': 'manual_review',
                           'reason': '; '.join(project['blockers'])})
    return result


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
    apply = commands.add_parser('apply', help='Commit at most --limit independent reviewed scopes')
    apply.add_argument('--run-dir', type=Path, required=True)
    apply.add_argument('--confirm-run-id', required=True)
    apply.add_argument('--allow-partial', action='store_true')
    apply.add_argument('--allow-repair-fields', action='store_true')
    apply.add_argument('--limit', type=int, default=10)
    apply.add_argument('--scope-id', action='append', default=[], help='Select reviewed scopes explicitly, including to pass deferred scopes')
    verify = commands.add_parser('verify', help='Read-only commit, scope and fresh global Monday verification')
    verify.add_argument('--run-dir', type=Path, required=True)
    return parser


def main(argv=None):
    args = argument_parser().parse_args(argv)
    backfill.load_dotenv()
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('src.database.sync_service').setLevel(logging.WARNING)
    try:
        if args.command == 'reassess':
            result = reassess(args.previous_run, args.capture_dir, args.output_dir)
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
                        allow_partial=args.allow_partial, allow_repair=args.allow_repair_fields, limit=args.limit, scope_ids=args.scope_id)
                else:
                    result = verify_run(connection, backfill.MondayClient(), args.run_dir)
        print(json.dumps(result, indent=2))
        if (result.get('complete') is False or result.get('deferred_scopes', 0)
                or any(r.get('status') == 'deferred' for r in result.get('results', []))):
            return 2
        return 0
    except ValueError as exc:
        LOG.error('Order scopes stopped: %s', exc)
    except Exception as exc:
        LOG.error('Order scopes stopped (%s). No completion is assumed. The database journal resolves uncertain commits; '
                  'rerun apply with the same reviewed run, then verify.', type(exc).__name__)
    return 1


if __name__ == '__main__':
    raise SystemExit(main())
