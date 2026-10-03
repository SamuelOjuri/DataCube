"""Read-only reassessment capture for historical blocked order projects.

Reads one complete Monday relationship inventory, then exact-ID metadata and
financial fields for blocked projects and their dependencies. Never writes to
Monday or to database business tables. Capture files are evidence, not apply runs.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path

import psycopg

from scripts import backfill_order_values as backfill
from scripts import order_value_scope_reads as reads
from scripts import order_value_scopes as scopes
from scripts import reconcile_order_values as reconcile
from src.config import get_hidden_items_extraction_columns, get_subitems_extraction_columns

LOG = logging.getLogger(__name__)


def finish_capture(connection, output_dir, context):
    """Finish local evidence after an interrupted final receipt write, without rescanning."""
    if context['complete'] or context['target'] != backfill.target_fingerprint(connection):
        raise ValueError('Only an incomplete capture of this database can be finalized')
    with connection.transaction():
        connection.execute('SET TRANSACTION READ ONLY')
        safety = scopes.schema_safety(connection)
    def read(name):
        return json.loads((output_dir / (name + '.json')).read_text(encoding='utf-8'))
    source = {key: read(key) for key in ('parents', 'children', 'hidden')}
    other = {key: read(key) for key in ('baseline', 'contract', 'ownership', 'exceptions')}
    context = {**context, 'complete': True, 'finished_at': datetime.fromtimestamp(
        (output_dir / 'contract.json').stat().st_mtime, timezone.utc).isoformat(), 'safety': safety,
        'receipt_finalized_at': datetime.now(timezone.utc).isoformat(),
        'boundary': {'projects': context['selected_project_ids'],
                     'subitems': sorted({r['id'] for r in source['children']['items']} | set(source['children']['not_returned_ids'])),
                     'hidden_items': sorted({r['id'] for r in source['hidden']['items']} | set(source['hidden']['not_returned_ids']))}}
    for key, value in {'source': source, **other}.items():
        context[key + '_sha256'] = backfill.fingerprint(value)
    backfill.write_json(output_dir / 'completed-context.json', context)
    return {'directory': str(output_dir), 'complete': True, 'selected_projects': len(context['selected_project_ids']),
            'finalized_saved_capture': True, 'global_links': len(other['ownership']['items']),
            'exceptions': other['exceptions']}


def details(monday, item_ids, columns=None, *, parents=False):
    if columns is None:
        return backfill.fetch_inventory_details(monday, sorted(item_ids), include_subitems=parents)
    query = '''query BlockedReviewItems($ids: [ID!]!, $columns: [String!]!) {
        items(ids: $ids, limit: 100, exclude_nonactive: false) {
            id name state board { id } parent_item { id state board { id } }
            column_values(ids: $columns) {
                id type text value
                ... on FormulaValue { display_value }
                ... on MirrorValue { display_value }
                ... on BoardRelationValue { linked_item_ids }
            }
        }
    }'''
    ids, found = sorted(item_ids), {}
    columns = sorted(set(columns) - {'name'})
    for offset in range(0, len(ids), 100):
        batch = ids[offset:offset + 100]
        response = backfill._inventory_read('blocked-project exact-ID details',
            lambda: monday.execute_query(query, {'ids': batch, 'columns': columns}), monday=monday)
        if response.get('errors') or not isinstance(response.get('data', {}).get('items'), list):
            raise ValueError('Invalid or incomplete blocked-project detail response')
        rows = backfill.indexed(response['data']['items'], 'id')
        if set(rows) - set(batch) or found.keys() & rows.keys():
            raise ValueError('Unexpected or repeated exact-ID results')
        for row in rows.values():
            if not {'state', 'board', 'parent_item', 'column_values', 'name'} <= row.keys():
                raise ValueError('Missing exact-ID metadata fields')
        found.update(rows)
        LOG.info('Blocked-project details: %d/%d IDs checked', offset + len(batch), len(ids))
    return {'items': [found[i] for i in sorted(found)], 'not_returned_ids': sorted(set(ids) - found.keys()),
            'requested_columns': columns}


def capture(connection, monday, previous_dir, output_dir):
    if output_dir.exists():
        required = ['context', 'parents', 'children', 'hidden', 'baseline', 'contract', 'ownership', 'exceptions']
        if not (output_dir / 'completed-context.json').exists() and all((output_dir / (n + '.json')).is_file() for n in required):
            context = json.loads((output_dir / 'context.json').read_text(encoding='utf-8'))
            return finish_capture(connection, output_dir, context)
        raise ValueError('Capture directory exists; use a new directory for fresh evidence')
    LOG.info('Validating historical capture locally')
    original, old_baseline, old_source, old_plan = backfill.load_run(previous_dir)
    if original['target'] != backfill.target_fingerprint(connection):
        raise ValueError('Database target differs from the historical capture')
    project_ids = sorted(r['project_id'] for r in old_plan['projects'] if r['status'] == 'blocked')
    if not project_ids:
        raise ValueError('The historical capture has no blocked projects')
    output_dir.mkdir(parents=True, exist_ok=False)
    context = {'started_at': datetime.now(timezone.utc).isoformat(), 'previous_capture_id': original['run_id'],
               'previous_capture_sha256': original['hashes']['plan'], 'target': original['target'],
               'selected_project_ids': project_ids, 'original_summary': original['summary'],
               'source_of_truth': 'Monday CRM', 'read_only': True, 'complete': False}
    backfill.write_json(output_dir / 'context.json', context)
    with connection.transaction():
        connection.execute('SET TRANSACTION READ ONLY')
        safety = scopes.schema_safety(connection)
        contract = reconcile.read_contract(connection)
    LOG.info('Capturing global source ownership for blocked-project reassessment')
    ownership = reads.scan_links(monday)
    backfill.write_json(output_dir / 'ownership.json', ownership)
    try:
        reads.validate_exceptions(connection, monday, ownership)
        exceptions = {'valid': True, 'reviewed_ids': sorted(backfill.REVIEWED_PARENTLESS_DUPLICATES)}
    except ValueError as exc:
        exceptions = {'valid': False, 'reason': str(exc), 'reviewed_ids': sorted(backfill.REVIEWED_PARENTLESS_DUPLICATES)}
    backfill.write_json(output_dir / 'exceptions.json', exceptions)
    LOG.info('Fetching current metadata for %d formerly blocked parents', len(project_ids))
    parents = details(monday, project_ids, parents=True)
    backfill.write_json(output_dir / 'parents.json', parents)
    selected = set(project_ids)
    links = [r for r in ownership['items'] if r.get('parent_monday_id') in selected]
    child_ids = {r['monday_id'] for r in links}
    child_ids.update(c['id'] for p in parents['items'] for c in p.get('subitems', []))
    child_ids.update(r['monday_id'] for r in old_baseline['subitems'] if r.get('parent_monday_id') in selected)
    child_ids.update(r['monday_id'] for r in old_source['subitems'] if r.get('parent_monday_id') in selected)
    child_ids.update(backfill.REVIEWED_PARENTLESS_DUPLICATES)
    child_ids.update(r['subitem_id'] for r in backfill.REVIEWED_PARENTLESS_DUPLICATES.values())
    hidden_ids = {hid for r in links for hid in r['hidden_ids']}
    hidden_ids.update(r['hidden_item_id'] for r in old_baseline['subitems']
                      if r.get('parent_monday_id') in selected and r.get('hidden_item_id'))
    hidden_ids.update(r['hidden_item_id'] for r in old_plan['diagnostics'] if r['issue'] == 'unlinked_hidden_order')
    hidden_ids.update(r['hidden_id'] for r in backfill.REVIEWED_PARENTLESS_DUPLICATES.values())
    # A preliminary selected SQL read discovers current stored children/sources
    # that were not present in either the historical capture or current Monday.
    boundary = {'projects': project_ids, 'subitems': sorted(child_ids), 'hidden_items': sorted(hidden_ids)}
    with connection.transaction():
        connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
        discovery = scopes.read_boundary(connection, boundary)
    child_ids.update(r['monday_id'] for r in discovery['subitems'])
    hidden_ids.update(r['hidden_item_id'] for r in discovery['subitems'] if r.get('hidden_item_id'))
    children = details(monday, child_ids, get_subitems_extraction_columns())
    backfill.write_json(output_dir / 'children.json', children)
    for item in children['items']:
        hidden_ids.update(backfill.normalize_subitem(item)['hidden_ids'])
    hidden = details(monday, hidden_ids, [*get_hidden_items_extraction_columns(), backfill.TOTAL_COLUMN])
    backfill.write_json(output_dir / 'hidden.json', hidden)
    boundary = {'projects': project_ids, 'subitems': sorted(child_ids), 'hidden_items': sorted(hidden_ids)}
    with connection.transaction():
        connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
        baseline = scopes.read_boundary(connection, boundary, full=True)
    backfill.write_json(output_dir / 'baseline.json', baseline)
    backfill.write_json(output_dir / 'contract.json', contract)
    source = {'parents': parents, 'children': children, 'hidden': hidden}
    context.update(complete=True, finished_at=datetime.now(timezone.utc).isoformat(), safety=safety,
                   boundary=boundary, baseline_sha256=backfill.fingerprint(baseline),
                   contract_sha256=backfill.fingerprint(contract), source_sha256=backfill.fingerprint(source),
                   ownership_sha256=backfill.fingerprint(ownership), exceptions_sha256=backfill.fingerprint(exceptions))
    backfill.write_json(output_dir / 'completed-context.json', context)
    return {'directory': str(output_dir), 'complete': True, 'selected_projects': len(project_ids),
            'global_links': len(ownership['items']), 'returned_parents': len(parents['items']),
            'missing_parent_ids': parents['not_returned_ids'], 'children_checked': len(child_ids),
            'hidden_sources_checked': len(hidden_ids), 'exceptions': exceptions}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--previous-capture', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('src.database.sync_service').setLevel(logging.WARNING)
    backfill.load_dotenv()
    dsn = os.environ.get('SUPABASE_DB_URL')
    if not dsn:
        raise ValueError('SUPABASE_DB_URL is required')
    try:
        with psycopg.connect(dsn, autocommit=True, connect_timeout=15) as connection:
            result = capture(connection, backfill.MondayClient(), args.previous_capture, args.output_dir)
        print(json.dumps(result, indent=2))
        return 0
    except ValueError as exc:
        LOG.error('Read-only reassessment stopped: %s', exc)
    except Exception as exc:
        LOG.error('Read-only reassessment stopped (%s); no business data was changed', type(exc).__name__)
    return 1


if __name__ == '__main__':
    raise SystemExit(main())
