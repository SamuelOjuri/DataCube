"""Read only the financial scope and a complete, narrow Monday owner inventory.

No calls run at import time. The filtered owner query is diagnostic-only until
its completeness has been tested on the actual board and duplicate exceptions.
The production path retains one complete subitem relationship scan per batch.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import json
import logging

from scripts import backfill_order_values as backfill
from scripts import order_value_scopes as scopes
from scripts import reconcile_order_values as reconcile

LOG = logging.getLogger(__name__)
OWNER_FIELDS = ('monday_id', 'parent_monday_id', 'hidden_ids', 'link_error', 'state',
                'board_id', 'parent_state', 'parent_board_id')
ITEM_FIELDS = """
    id state board { id } parent_item { id state board { id } }
    column_values(ids: $columns) {
        id type value ... on BoardRelationValue { linked_item_ids }
    }
"""


def normalized_link(item):
    """An unreadable link anywhere prevents a claim of complete ownership."""
    if (item.get('state') != 'active'
            or (item.get('board') or {}).get('id') != backfill.SUBITEM_BOARD_ID
            or 'parent_item' not in item):
        raise ValueError('Invalid or incomplete subitem ownership metadata')
    columns = backfill.indexed(item.get('column_values', []), 'id')
    link = columns.get(backfill.SUBITEM_COLUMNS['hidden_item_id'])
    if link is None or link.get('type') != 'board_relation':
        raise ValueError('Missing source relationship column in ownership inventory')
    row = backfill.normalize_subitem(item)
    if row['link_error']:
        raise ValueError('Unreadable source relationship in ownership inventory')
    parent = item['parent_item'] or {}
    if item['parent_item'] is not None and not str(parent.get('id') or '').isdigit():
        raise ValueError('Missing parent ID in ownership metadata')
    row.update(state=item['state'], board_id=item['board']['id'],
               parent_state=parent.get('state'), parent_board_id=(parent.get('board') or {}).get('id'))
    return {field: row.get(field) for field in OWNER_FIELDS}


def link_pages(monday, *, hidden_ids=None):
    """Exhaust cursor pages; never interpret missing data as an empty population."""
    variables = {'board_id': backfill.SUBITEM_BOARD_ID,
                 'columns': [backfill.SUBITEM_COLUMNS['hidden_item_id']]}
    declaration, predicate = '', ''
    if hidden_ids is not None:
        if not hidden_ids or any(not str(item_id).isdigit() for item_id in hidden_ids):
            raise ValueError('Select numeric hidden-source IDs for the owner query')
        # Compare values are numbers, not names. ItemsQuery carries the rule's
        # API-specific scalar rather than incorrectly declaring these as Int32.
        declaration = ', $filter: ItemsQuery!'
        predicate = ', query_params: $filter'
        variables['filter'] = {'rules': [{'column_id': backfill.SUBITEM_COLUMNS['hidden_item_id'],
                                         'operator': 'any_of',
                                         'compare_value': [int(i) for i in sorted(set(hidden_ids))]}]}
    query = ('''query ScopeOwnerFirst($board_id: ID!, $columns: [String!]! DECLARATION) {
        boards(ids: [$board_id]) { id items_page(limit: 100 PREDICATE) {
            cursor items { FIELDS }
        } }
    }'''.replace('DECLARATION', declaration).replace('PREDICATE', predicate).replace('FIELDS', ITEM_FIELDS))
    rows, cursors, cursor, pages = {}, set(), None, 0
    while True:
        response = backfill._inventory_read('subitem source-owner page',
            lambda: monday.execute_query(query, variables), monday=monday)
        data = response.get('data')
        if response.get('errors') or not isinstance(data, dict):
            raise ValueError('Invalid Monday ownership response')
        if cursor is None:
            boards = data.get('boards')
            if (not isinstance(boards, list) or len(boards) != 1
                    or str(boards[0].get('id')) != backfill.SUBITEM_BOARD_ID):
                raise ValueError('Missing or unexpected ownership board')
            page = boards[0].get('items_page')
        else:
            page = data.get('next_items_page')
        if not isinstance(page, dict) or not isinstance(page.get('items'), list) or 'cursor' not in page:
            raise ValueError('Incomplete ownership page')
        items = backfill.indexed(page['items'], 'id')
        if rows.keys() & items.keys():
            raise ValueError('Duplicate IDs during ownership pagination')
        rows.update({item_id: normalized_link(item) for item_id, item in items.items()})
        pages += 1
        LOG.info('Source ownership: %d subitem links read', len(rows))
        cursor = page['cursor']
        if cursor is None:
            break
        if not isinstance(cursor, str) or not cursor or not items or cursor in cursors:
            raise ValueError('Ownership pagination did not advance')
        cursors.add(cursor)
        query = '''query ScopeOwnerNext($cursor: String!, $columns: [String!]!) {
            next_items_page(cursor: $cursor, limit: 100) { cursor items { FIELDS } }
        }'''.replace('FIELDS', ITEM_FIELDS)
        variables = {'cursor': cursor, 'columns': variables['columns']}
    return {'items': [rows[i] for i in sorted(rows)], 'pages': pages}


def scan_links(monday):
    started = datetime.now(timezone.utc).isoformat()
    before = backfill._inventory_read('ownership before-count',
        lambda: monday.get_board_info(backfill.SUBITEM_BOARD_ID), monday=monday)['items_count']
    scan = link_pages(monday)
    after = backfill._inventory_read('ownership after-count',
        lambda: monday.get_board_info(backfill.SUBITEM_BOARD_ID), monday=monday)['items_count']
    rows = backfill.indexed(scan['items'])
    excluded = backfill.REVIEWED_PARENTLESS_DUPLICATES
    # Preserve the precisely reviewed four-item count anomaly. A new count
    # discrepancy, disappeared exception or unknown parentless item is a stop.
    if (any(type(n) is not int or n < 0 for n in (before, after))
            or before != after or not excluded.keys() <= rows.keys()
            or len(rows) - len(excluded) != before):
        raise ValueError('Ownership counts do not reconcile with the four reviewed duplicates')
    for item_id, row in rows.items():
        if item_id in excluded:
            expected = excluded[item_id]
            if row['parent_monday_id'] or row['hidden_ids'] != [expected['hidden_id']]:
                raise ValueError('Reviewed parentless duplicate relationship changed')
        elif (not row['parent_monday_id'] or row['parent_state'] != 'active'
                or row['parent_board_id'] != backfill.PARENT_BOARD_ID):
            raise ValueError('Ownership inventory has an unreviewed parent relationship')
    return {**scan, 'board_id': backfill.SUBITEM_BOARD_ID, 'started_at': started,
            'finished_at': datetime.now(timezone.utc).isoformat(),
            'before_count': before, 'after_count': after}


def validate_exceptions(connection, monday, scan):
    """Recheck the exact four exclusions, their sources and ALL stored owners.

    This is intentionally independent of the legacy full-inventory validator:
    no artificial global parent/hidden counts are manufactured for scoped data.
    """
    mappings = backfill.REVIEWED_PARENTLESS_DUPLICATES
    links = backfill.indexed(scan['items'])
    parent_ids = sorted({r['parent_id'] for r in mappings.values()})
    child_ids = sorted({r['subitem_id'] for r in mappings.values()})
    hidden_ids = sorted({r['hidden_id'] for r in mappings.values()})
    parents = backfill.fetch_inventory_details(monday, parent_ids, include_subitems=True)
    if parents['not_returned_ids']:
        raise ValueError('Reviewed duplicate counterpart parent is missing')
    metadata = backfill.indexed(parents['items'], 'id')
    hidden = reconcile.fetch_columns(monday, hidden_ids,
        [backfill.HIDDEN_ITEMS_COLUMNS[f] for f in (*backfill.ORDER_FIELDS, 'amount_invoiced',
                                                  'date_order_received', 'invoice_date')] + [backfill.TOTAL_COLUMN],
        backfill.HIDDEN_ITEMS_BOARD_ID)
    hidden = backfill.indexed(hidden, 'id')
    boundary = {'projects': sorted(set(parent_ids) | mappings.keys()),
                'subitems': sorted(set(child_ids) | mappings.keys()),
                'hidden_items': sorted(set(hidden_ids) | mappings.keys())}
    with connection.transaction():
        connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
        state = scopes.read_boundary(connection, boundary)
    stored = {table: backfill.indexed(state[table]) for table in scopes.TABLES}
    for excluded_id, expected in mappings.items():
        failure = f'Reviewed duplicate {excluded_id} no longer meets exclusion conditions'
        child_id, parent_id, hidden_id = (expected[k] for k in ('subitem_id', 'parent_id', 'hidden_id'))
        if any(excluded_id in table for table in stored.values()):
            raise ValueError(failure + ': excluded ID exists in database')
        duplicate, child = links.get(excluded_id), links.get(child_id)
        if (duplicate is None or duplicate['parent_monday_id'] or duplicate['hidden_ids'] != [hidden_id]
                or child is None or child['parent_monday_id'] != parent_id or child['hidden_ids'] != [hidden_id]
                or child['parent_state'] != 'active' or child['parent_board_id'] != backfill.PARENT_BOARD_ID):
            raise ValueError(failure + ': counterpart relationship changed')
        parent = metadata[parent_id]
        if (parent.get('state') != 'active' or (parent.get('board') or {}).get('id') != backfill.PARENT_BOARD_ID
                or parent.get('parent_item') is not None):
            raise ValueError(failure + ': counterpart parent metadata changed')
        listed = backfill.indexed(parent['subitems'], 'id').get(child_id)
        if (listed is None or listed.get('state') != 'active'
                or (listed.get('board') or {}).get('id') != backfill.SUBITEM_BOARD_ID
                or (listed.get('parent_item') or {}).get('id') != parent_id
                or (listed.get('parent_item') or {}).get('state') != 'active'
                or ((listed.get('parent_item') or {}).get('board') or {}).get('id') != backfill.PARENT_BOARD_ID):
            raise ValueError(failure + ': counterpart missing from parent child list')
        owners = {i for i, row in links.items() if hidden_id in row['hidden_ids']}
        if owners != {excluded_id, child_id}:
            raise ValueError(failure + ': extra Monday source owners')
        source = hidden[hidden_id]
        values = backfill.normalize_hidden(source)
        if values['issues'] or any(values[f] != '0.00' for f in (*backfill.ORDER_FIELDS, 'monday_total')):
            raise ValueError(failure + ': source order values changed')
        columns = backfill.indexed(source['column_values'], 'id')
        for field in ('amount_invoiced', 'date_order_received', 'invoice_date'):
            value = columns[backfill.HIDDEN_ITEMS_COLUMNS[field]]['value']
            value = json.loads(value) if isinstance(value, str) and value else value
            if field == 'amount_invoiced':
                if backfill.money('0' if value in (None, '') else value) != '0.00':
                    raise ValueError(failure + ': source invoice value changed')
            elif value not in (None, ''):
                raise ValueError(failure + ': source date changed')
        stored_child, stored_source = stored['subitems'].get(child_id), stored['hidden_items'].get(hidden_id)
        if (parent_id not in stored['projects'] or stored_child is None or stored_source is None
                or stored_child.get('parent_monday_id') != parent_id or stored_child.get('hidden_item_id') != hidden_id):
            raise ValueError(failure + ': stored counterpart missing or relinked')
        if {r['monday_id'] for r in state['subitems'] if r.get('hidden_item_id') == hidden_id} != {child_id}:
            raise ValueError(failure + ': extra stored source owners')
        for row in (stored_child, stored_source):
            if (any(backfill.money(row.get(f)) not in (None, '0.00')
                    for f in (*backfill.ORDER_FIELDS, 'amount_invoiced'))
                    or any(row.get(f) is not None for f in ('date_order_received', 'invoice_date'))):
                raise ValueError(failure + ': stored amounts or dates changed')


def capture_ownership(connection, monday):
    scan = scan_links(monday)
    validate_exceptions(connection, monday, scan)
    return scan


def owner_index(scan):
    owners = defaultdict(list)
    for row in scan['items']:
        if row['monday_id'] not in backfill.REVIEWED_PARENTLESS_DUPLICATES:
            for hidden_id in row['hidden_ids']:
                owners[hidden_id].append(row)
    return owners


def check_scope(monday, record, owners, *, mode):
    wanted = set(record['scope']['hidden_items'])
    found = {row['monday_id']: row for hid in wanted for row in owners.get(hid, [])}
    actual = [found[i] for i in sorted(found)]
    if actual != record['source']['owners']:
        raise scopes.ScopeConflict('Monday source ownership changed; restage this scope')
    # targeted_orders copies the reviewed owners; never use it as proof of
    # global ownership. The independent complete owner comparison above is vital.
    evidence = scopes.targeted_orders(monday, record)
    if evidence != record['source']:
        raise scopes.ScopeConflict('Selected Monday values or complete child membership changed')
    if mode == 'repair':
        source = {k: record['source'][k] for k in ('subitems', 'hidden_items')}
        if reconcile.capture_targeted(monday, source, record['scope']) != record['raw']:
            raise scopes.ScopeConflict('Targeted repair inputs changed since review')


def compare_owner_filter(monday, scan, hidden_ids):
    """Diagnostic only: a match is an observation, not an authorization to skip scans."""
    requested = sorted(set(hidden_ids) | {r['hidden_id'] for r in backfill.REVIEWED_PARENTLESS_DUPLICATES.values()})
    filtered = {}
    for offset in range(0, len(requested), 100):
        batch = requested[offset:offset + 100]
        for row in link_pages(monday, hidden_ids=batch)['items']:
            if not set(row['hidden_ids']) & set(batch):
                raise ValueError('Filtered owner query returned an unrelated source')
            if row['monday_id'] in filtered and filtered[row['monday_id']] != row:
                raise ValueError('Filtered owner metadata changed across query batches')
            filtered[row['monday_id']] = row
    expected = {row['monday_id']: row for row in scan['items'] if set(row['hidden_ids']) & set(requested)}
    return {'checked_at': datetime.now(timezone.utc).isoformat(), 'diagnostic_only': True,
            'enables_filtered_apply': False, 'hidden_ids': requested,
            'api_version_requested': getattr(monday, 'headers', {}).get('API-Version'),
            'matches': expected == filtered, 'missing_owner_ids': sorted(expected.keys() - filtered.keys()),
            'unexpected_owner_ids': sorted(filtered.keys() - expected.keys()),
            'changed_owner_ids': sorted(i for i in expected.keys() & filtered.keys() if expected[i] != filtered[i])}
