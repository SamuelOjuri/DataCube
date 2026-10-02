"""Stage selected, previously reviewed projects using fresh Monday order evidence.

This read-only entry point produces the existing targeted workflow's run format.
Apply and verify remain in order_value_scopes_targeted, with their independent
global ownership checks and unchanged transaction/journal implementation.
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
from scripts import order_value_scope_reads as reads
from scripts import order_value_scopes as scopes
from scripts import order_value_scopes_targeted as targeted
from scripts import reconcile_order_values as reconcile

LOG = logging.getLogger(__name__)


def project_ids_from_file(path):
    ids = [line.strip() for line in path.read_text(encoding='utf-8-sig').splitlines()
           if line.strip() and not line.lstrip().startswith('#')]
    if not ids or any(not item_id.isascii() or not item_id.isdigit() for item_id in ids):
        raise ValueError('Project file must contain one numeric Monday project ID per line')
    if len(ids) != len(set(ids)):
        raise ValueError('Project file contains duplicate IDs')
    return sorted(ids)


def capture_selected(monday, project_ids):
    """Read complete selected parents, their current children, and linked sources.

    Expected owners here are ONLY the selected children. This is not proof of
    global Monday ownership: unchanged apply/verify must establish that separately.
    """
    started_at = datetime.now(timezone.utc).isoformat()
    LOG.info('Refreshing Monday evidence for %d selected projects', len(project_ids))
    details = backfill.fetch_inventory_details(monday, project_ids, include_subitems=True)
    parents = backfill.indexed(details['items'], 'id')
    if details['not_returned_ids'] or set(parents) != set(project_ids):
        raise ValueError('Selected Monday parent is missing')
    listed_children = {}
    for pid, parent in parents.items():
        if (parent.get('state') != 'active' or (parent.get('board') or {}).get('id') != backfill.PARENT_BOARD_ID
                or 'parent_item' not in parent or parent['parent_item'] is not None):
            raise ValueError(f'Invalid selected parent metadata: {pid}')
        if not isinstance(parent.get('subitems'), list) or not parent['subitems']:
            raise ValueError(f'Parent {pid} has no readable children; empty totals need separate review')
        for child in parent['subitems']:
            cid = str(child.get('id') or '')
            reported = child.get('parent_item') or {}
            if (not cid.isdigit() or cid in listed_children or child.get('state') != 'active'
                    or (child.get('board') or {}).get('id') != backfill.SUBITEM_BOARD_ID
                    or reported.get('id') != pid or reported.get('state') != 'active'
                    or (reported.get('board') or {}).get('id') != backfill.PARENT_BOARD_ID):
                raise ValueError(f'Invalid or duplicate child metadata under parent {pid}: {cid}')
            listed_children[cid] = pid
        parent['subitems'] = sorted(parent['subitems'], key=lambda row: row['id'])
    LOG.info('Refreshing links for %d selected children', len(listed_children))
    raw_children = reconcile.fetch_columns(monday, sorted(listed_children),
        [backfill.SUBITEM_COLUMNS['hidden_item_id']], backfill.SUBITEM_BOARD_ID)
    if set(backfill.indexed(raw_children, 'id')) != set(listed_children):
        raise ValueError('Incomplete selected child response')
    children, hidden_ids = [], set()
    for item in raw_children:
        row = reads.normalized_link(item)
        if row['parent_monday_id'] != listed_children[row['monday_id']]:
            raise ValueError('Child moved between the selected parent and relationship reads')
        if (row['parent_state'] != 'active' or row['parent_board_id'] != backfill.PARENT_BOARD_ID
                or len(row['hidden_ids']) != 1):
            raise ValueError(f'Invalid parent or ambiguous source link: {row["monday_id"]}')
        if row['hidden_ids'][0] in hidden_ids:
            raise ValueError('A hidden source has multiple owners in the selected Monday projects')
        hidden_ids.update(row['hidden_ids'])
        children.append(row)
    LOG.info('Refreshing order inputs and formulas for %d linked sources', len(hidden_ids))
    raw_hidden = reconcile.fetch_columns(monday, sorted(hidden_ids),
        [backfill.HIDDEN_ITEMS_COLUMNS[f] for f in backfill.ORDER_FIELDS] + [backfill.TOTAL_COLUMN],
        backfill.HIDDEN_ITEMS_BOARD_ID)
    if set(backfill.indexed(raw_hidden, 'id')) != hidden_ids:
        raise ValueError('Incomplete selected source response')
    hidden = [backfill.normalize_hidden(item) for item in raw_hidden]
    for row in hidden:
        if row['issues'] or any(row[f] is None for f in backfill.ORDER_FIELDS):
            raise ValueError(f'Invalid Monday order inputs or formula: {row["monday_id"]}')
    source = {'project_ids': sorted(project_ids), 'subitems': sorted(children, key=lambda row: row['monday_id']),
              'hidden_items': hidden, 'exclusion_evidence': {'parent_details': {'items': details['items']}}}
    raw = {'started_at': started_at, 'finished_at': datetime.now(timezone.utc).isoformat(),
           'project_ids': sorted(project_ids), 'parents': details, 'subitems': raw_children, 'hidden_items': raw_hidden,
           'global_ownership_validated': False, 'ownership_check_required_at_apply': True}
    return source, raw


def group_projects(source):
    counts = Counter(row['parent_monday_id'] for row in source['subitems'])
    groups, group, rows = [], [], 0
    for pid in sorted(source['project_ids']):
        size = 1 + 2 * counts[pid]
        if group and (len(group) == scopes.MAX_PROJECTS or rows + size > scopes.MAX_SCOPE_ROWS):
            groups.append(group)
            group, rows = [], 0
        group.append(pid)
        rows += size
    if group:
        groups.append(group)
    return groups


def stage_run(connection, monday, previous_dir, output_dir, project_ids):
    LOG.info('Validating previous run artifacts locally; no service traversal is involved')
    previous, old_plan = targeted.load_run(previous_dir)
    if old_plan['mode'] != 'orders':
        raise ValueError('Refresh requires a previous order-only targeted run')
    if previous['target'] != backfill.target_fingerprint(connection):
        raise ValueError('Database target differs from the previous run')
    allowed = {pid for record in old_plan['scopes'] for pid in record['scope']['projects']}
    if (not project_ids or len(project_ids) != len(set(project_ids))
            or any(not isinstance(pid, str) or not pid.isascii() or not pid.isdigit() for pid in project_ids)
            or set(project_ids) - allowed):
        raise ValueError('Select unique numeric project IDs previously staged in the reviewed run')
    if output_dir.exists():
        raise ValueError('Use a new recovery run directory; previous evidence must remain intact')
    source, raw = capture_selected(monday, sorted(project_ids))
    inventory = scopes.index_source(source)
    combined = {'projects': sorted(project_ids), 'subitems': sorted(inventory['children']),
                'hidden_items': sorted(inventory['hidden'])}
    LOG.info('Reading current database rows and all stored owners for the selected boundary')
    with connection.transaction():
        connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
        safety = scopes.schema_safety(connection)
        contract = reconcile.read_contract(connection)
        current = scopes.index_baseline(scopes.read_boundary(connection, combined))
    records, deferred, changes = [], [], []
    for parents in group_projects(source):
        try:
            children = [r for pid in parents for r in inventory['parents'][pid]]
            scope = {'projects': parents, 'subitems': sorted(r['monday_id'] for r in children),
                     'hidden_items': sorted(r['hidden_ids'][0] for r in children)}
            evidence = scopes.source_evidence(source, scope, inventory)
            # Order-only recovery must not repair stored links or insert records.
            # Incoming stored owners are included by select_boundary/read_boundary.
            before = scopes.select_boundary(current, scope)
            scopes.require_existing_scope(before, scope)
            scopes.check_parent_membership(before, scope, evidence)
            if scopes.boundary_from_baseline(before, scope) != scope:
                raise scopes.ScopeConflict('Stored source links differ from Monday; separate relationship review is required')
            updates = scopes.order_updates(before, evidence, scope)
            after = scopes.expected_state(before, updates)
            scopes.check_ownership(after, scope)
            record = {'scope_id': 'scope-' + backfill.fingerprint(parents)[:16], 'scope': scope,
                      'boundary': scope, 'before': before, 'after': after, 'updates': updates,
                      'source': evidence, 'raw': None}
            records.append(record)
            for table, rows in updates.items():
                old_rows = backfill.indexed(before[table])
                for row in rows:
                    for field, value in row.items():
                        old = old_rows[row['monday_id']].get(field)
                        if old != value:
                            changes.append({'scope_id': record['scope_id'], 'project_ids': parents,
                                            'table': table, 'monday_id': row['monday_id'], 'field': field,
                                            'before': old, 'after': value})
        except scopes.ScopeConflict as exc:
            deferred.append({'project_ids': parents, 'reason': str(exc)})
    refreshed = {'previous_run_id': previous['run_id'], 'previous_plan_sha256': previous['sha256'],
                 'selected_project_ids': sorted(project_ids), 'source_of_truth': 'Monday CRM',
                 'raw_evidence_file': 'monday-refresh.json', 'raw_evidence_sha256': backfill.fingerprint(raw),
                 'started_at': raw['started_at'], 'finished_at': raw['finished_at'],
                 'stager_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                 'global_ownership_validated_at_stage': False, 'ownership_check_required_at_apply': True}
    staged = {'mode': 'orders', 'contract': contract, 'scopes': records, 'deferred': deferred,
              'origin_run_id': old_plan['origin_run_id'], 'origin_summary': old_plan['origin_summary'],
              'approved_empty': old_plan['approved_empty'], 'refresh': refreshed}
    output_dir.mkdir(parents=True, exist_ok=False)
    backfill.write_json(output_dir / 'monday-refresh.json', raw)
    backfill.write_json(output_dir / 'scopes.json', staged)
    reconcile.write_csv(output_dir / 'changes.csv', changes,
                        ['scope_id', 'project_ids', 'table', 'monday_id', 'field', 'before', 'after'])
    manifest = {'version': targeted.VERSION, 'workflow': targeted.WORKFLOW, 'run_id': str(uuid4()),
                'prepared_at': datetime.now(timezone.utc).isoformat(), 'code': targeted.code_fingerprint(),
                'target': previous['target'], 'source_contract': backfill.source_contract(),
                'sha256': backfill.fingerprint(staged),
                'review_sha256': hashlib.sha256((output_dir / 'changes.csv').read_bytes()).hexdigest(),
                'mode': 'orders', 'scopes': len(records), 'deferred_scopes': len(deferred), 'changes': len(changes),
                'projects': sum(len(r['scope']['projects']) for r in records),
                'selected_projects': len(project_ids), 'monday_source_refreshed': True, 'safety': safety}
    backfill.write_json(output_dir / 'manifest.json', manifest)
    # Validate with the exact unchanged loader that apply/verify will use. The
    # staging helper is provenance only; it is never imported by the write path.
    targeted.load_run(output_dir)
    return manifest


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--previous-run', type=Path, required=True)
    parser.add_argument('--run-dir', type=Path, required=True)
    parser.add_argument('--projects-file', type=Path, required=True)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('src.database.sync_service').setLevel(logging.WARNING)
    backfill.load_dotenv()
    try:
        project_ids = project_ids_from_file(args.projects_file)
        dsn = os.environ.get('SUPABASE_DB_URL')
        if not dsn:
            raise ValueError('SUPABASE_DB_URL is required; do not place credentials in command arguments')
        with psycopg.connect(dsn, autocommit=True, connect_timeout=15) as connection:
            result = stage_run(connection, backfill.MondayClient(), args.previous_run, args.run_dir, project_ids)
        print(json.dumps(result, indent=2))
        return 2 if result['deferred_scopes'] else 0
    except ValueError as exc:
        LOG.error('Recovery staging stopped: %s', exc)
    except Exception as exc:
        LOG.error('Recovery staging stopped (%s); no database writes were performed', type(exc).__name__)
    return 1


if __name__ == '__main__':
    raise SystemExit(main())
