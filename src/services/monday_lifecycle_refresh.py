"""Exact-link rehydration after lifecycle changes; Finance's Monday values win.

Uses the already reviewed flat GraphQL reader/projection used by the comparison
backfill. No name searches, board scans, stored-child financial rollups or Monday
mutations. Unrepresentable fields remain explicit review issues.
"""
from copy import deepcopy
from decimal import Decimal
from datetime import datetime, timezone

from psycopg import sql

from scripts import order_value_monday_compare as compare
from scripts import reconcile_order_values as reconcile
from ..config import PARENT_COLUMNS, SUBITEM_COLUMNS, HIDDEN_ITEMS_COLUMNS
from ..core.data_processor import LabelNormalizer, EnhancedMirrorResolver, HierarchicalSegmentation
from ..database.sync_service import DataSyncService
from . import monday_lifecycle as life
from . import monday_archive as archive


def fetch_project(monday, pid):
    return fetch_projects(monday, [pid])


def fetch_projects(monday, project_ids):
    project_ids = sorted(set(project_ids))
    if not project_ids or len(project_ids) > life.MAX_ROWS:
        raise life.ReviewRequired('Select 1-500 exact projects')
    parents = compare.fetch_items(monday, project_ids, sorted(set(PARENT_COLUMNS.values()) - {'name'}), parents=True)
    membership = {}
    for pid in project_ids:
        parent = life.require_item(parents, pid, 'projects', 'active')
        if parent.get('parent_item') is not None:
            raise life.ReviewRequired('Selected project is now a subitem')
        for child in parent['subitems']:
            if child['id'] in membership or (child.get('parent_item') or {}).get('id') != pid:
                raise life.ReviewRequired('Duplicate or inconsistent current Monday child membership')
            membership[child['id']] = pid
    relevant = {PARENT_COLUMNS[f] for f in compare.PARENT_FIELDS}
    mirror_parents = [{**parent, 'column_values': [c for c in parent['column_values'] if c['id'] in relevant]}
                      for parent in parents.values()]
    dependencies, child_columns = compare.mirror_dependencies(mirror_parents, compare.backfill.SUBITEM_BOARD_ID)
    child_ids = set(membership) | dependencies
    if len(child_ids) + len(project_ids) > life.MAX_ROWS:
        raise life.ReviewRequired('Too many mirror dependencies')
    child_columns |= set(SUBITEM_COLUMNS.values()) - {'name'}
    children = compare.fetch_items(monday, child_ids, sorted(child_columns))
    hidden_ids = set()
    for cid, pid in membership.items():
        child = life.require_item(children, cid, 'subitems', 'active')
        if (child.get('parent_item') or {}).get('id') != pid:
            raise life.ReviewRequired('Child moved during refresh')
        hidden_ids.update(compare.links(child))
    checked = {SUBITEM_COLUMNS[f] for f in (*compare.CHILD_FIELDS, 'new_enquiry_value')}
    mirror_children = [{**c, 'column_values': [v for v in c['column_values'] if v['id'] in checked]}
                       for c in children.values()]
    dependencies, hidden_columns = compare.mirror_dependencies(mirror_children, compare.backfill.HIDDEN_ITEMS_BOARD_ID)
    hidden_ids |= dependencies
    if len(child_ids) + len(hidden_ids) + len(project_ids) > life.MAX_ROWS:
        raise life.ReviewRequired('Refresh exceeds 500 related rows')
    hidden_columns |= set(HIDDEN_ITEMS_COLUMNS.values()) - {'name'}
    hidden = compare.fetch_items(monday, hidden_ids, sorted(hidden_columns), mirror_depth=0)
    return dict(projects=parents, subitems=children, hidden_items=hidden,
                project_ids=project_ids, extra_children=[])


def read_snapshot(connection, pid, source):
    children = life.read_rows(connection, 'subitems', 'parent_monday_id', [pid])
    known = {r['monday_id']: r for r in children}
    known.update({r['monday_id']: r for r in life.read_rows(connection, 'subitems', 'monday_id', list(source['subitems']))})
    known.update({r['monday_id']: r for r in life.read_rows(connection, 'subitems', 'hidden_item_id', list(source['hidden_items']))})
    snapshot = dict(projects=life.read_rows(connection, 'projects', 'monday_id', [pid]),
                    subitems=sorted(known.values(), key=lambda r: r['monday_id']),
                    hidden_items=life.read_rows(connection, 'hidden_items', 'monday_id', list(source['hidden_items'])))
    if sum(map(len, snapshot.values())) > life.MAX_ROWS:
        raise life.ReviewRequired('Stored refresh dependency exceeds 500 rows')
    return snapshot


def capture_refresh_source(connection, monday, pid):
    result = fetch_project(monday, pid)
    if archive.enabled():
        stored = life.read_rows(connection, 'subitems', 'parent_monday_id', [pid])
        extra = {r['monday_id'] for r in stored} - set(result['subitems'])
        result['subitems'].update(life.read_items(monday, extra))
    return result


def transformer():
    service = DataSyncService.__new__(DataSyncService)
    service.label_normalizer = LabelNormalizer()
    service.mirror_resolver = EnhancedMirrorResolver()
    service.segmentation = HierarchicalSegmentation()
    for name in ('_hidden_lookup_by_id', '_hidden_lookup_by_name',
                 '_hidden_lookup_by_normalized_name', '_hidden_lookup_by_prefix'):
        setattr(service, name, {})
    service._product_alias_map = None
    service._category_alias_map = None
    return service


def clean_rows(table, rows, contract):
    return reconcile.normalize_updates({table: [
        {k: v for k, v in row.items() if k in contract[table]
         and contract[table][k]['generated'] == 'NEVER'
         and k not in {'id', 'created_at', 'updated_at', 'last_synced_at'}}
        for row in rows]}, contract)[table]


def same_value(before, after, column):
    if before is None or after is None:
        return before is after
    if column['type'] == 'numeric':
        return Decimal(str(before)) == Decimal(str(after))
    return before == after


def build_values(pid, source, before, contract, *, lifecycle=None, reparent_id=None):
    """Fresh metadata for new rows; explicit comparison fields for existing rows."""
    service = transformer()
    current = {t: {r['monday_id']: r for r in before[t]} for t in life.BOARDS.values()}
    seeds = {t: {} for t in current}
    parent = source['projects'][pid]
    # Normalize with this board's own mapping (column IDs can repeat on boards).
    normalized = {'id': pid, 'name': parent['name']}
    for field, column_id in PARENT_COLUMNS.items():
        if column_id == 'name':
            continue
        column = compare.col(parent, column_id)
        normalized[field] = service.label_normalizer.normalize_column_value(column, column_id)
    if pid not in current['projects']:
        rows = clean_rows('projects', service._transform_for_projects_table([normalized]), contract)
        if len(rows) != 1:
            raise ValueError('Project transformation did not produce one row')
        seeds['projects'][pid] = rows[0]
    hidden = [r for r in source['hidden_items'].values() if r['state'] == 'active']
    transformed_hidden = clean_rows('hidden_items', service._transform_for_hidden_table(deepcopy(hidden)), contract)
    if {r['monday_id'] for r in transformed_hidden} != {r['id'] for r in hidden}:
        raise ValueError('Hidden-source transformation omitted an item')
    for row in transformed_hidden:
        if row['monday_id'] not in current['hidden_items']:
            seeds['hidden_items'][row['monday_id']] = row
    # Prevent all production name/prefix fallbacks, including same-name deleted revisions.
    service._hidden_lookup_by_name = {}
    service._hidden_lookup_by_normalized_name = {}
    service._hidden_lookup_by_prefix = {}
    children = []
    for member in parent['subitems']:
        cid = member['id']
        child = source['subitems'][cid]
        links = compare.links(child)
        if len(links) > 1:
            raise life.ReviewRequired(f'Subitem {cid} has multiple sources; cannot fit hidden_item_id')
        if links:
            life.require_item(source['hidden_items'], links[0], 'hidden_items', 'active')
        old_parent = current['subitems'].get(cid, {}).get('parent_monday_id')
        if old_parent not in (None, pid) and cid != reparent_id:
            raise life.ReviewRequired(f'Subitem {cid} moved from {old_parent}; compare both parents before relinking')
        children.append(child)
    transformed_children = clean_rows('subitems', service._transform_for_subitems_table(deepcopy(children)), contract)
    if {r['monday_id'] for r in transformed_children} != {r['id'] for r in children}:
        raise ValueError('Subitem transformation omitted a current item')
    for row in transformed_children:
        if row['monday_id'] not in current['subitems']:
            seeds['subitems'][row['monday_id']] = row
    provisional = {t: list(current[t].values()) + list(seeds[t].values()) for t in current}
    if reparent_id is not None:
        provisional['subitems'] = [
            {**r, 'parent_monday_id': pid} if r['monday_id'] == reparent_id else r
            for r in provisional['subitems']]
    proposed, issues = compare.project_projection(pid, source, provisional, contract, lifecycle=lifecycle)
    for child in children:
        cid, links = child['id'], compare.links(child)
        row = proposed['subitems'].setdefault(cid, {'monday_id': cid})
        # An intentionally blank exact link is representable, and must clear the
        # former relation and hidden-only amounts rather than guessing by name.
        if not links:
            row.update(hidden_item_id=None, cust_order_value_material=None, cust_additional_charges=None)
            issues = [i for i in issues if not (i['table'] == 'subitems' and i['monday_id'] == cid
                      and i['field'] == 'hidden_item_id' and i['reason'].startswith('Expected one representable source, Monday has 0:'))]
        # Refresh all mirrored metadata after a source deletion, from the actual
        # child columns. Financial/date/status values above override fallbacks.
        metadata = next(r for r in transformed_children if r['monday_id'] == cid)
        proposed['subitems'][cid] = {**metadata, **row}
        if 'new_enquiry_value' in contract['subitems']:
            try:
                proposed['subitems'][cid]['new_enquiry_value'] = compare.numeric(
                    compare.resolved_col(source, child, SUBITEM_COLUMNS['new_enquiry_value']))
            except ValueError as exc:
                issues.append(dict(project_id=pid, table='subitems', monday_id=cid,
                                   field='new_enquiry_value', reason=str(exc)))
    # Never allow a production fallback to fill an unresolved financial field.
    for issue in issues:
        t, item, field = issue['table'], issue['monday_id'], issue['field']
        if field in contract[t] and item in seeds[t]:
            seeds[t][item][field] = None
        if field not in {'*', 'monday_state'} and item in proposed[t]:
            proposed[t][item].pop(field, None)
    values = {t: [] for t in current}
    for table in current:
        for item in sorted(set(proposed[table]) | set(seeds[table])):
            row = {**seeds[table].get(item, {}), **proposed[table].get(item, {})}
            # Compare at the precision actually persisted, not raw formula precision.
            row = reconcile.normalize_updates({table: [row]}, contract)[table][0]
            old = current[table].get(item)
            if old is not None:
                row = {k: v for k, v in row.items() if k == 'monday_id'
                       or not same_value(old.get(k), v, contract[table][k])}
            if old is None or len(row) > 1:
                values[table].append(row)
    return values, issues


def write_values(connection, values, before, contract, *, job=None):
    for table in ('projects', 'hidden_items', 'subitems'):
        old = {r['monday_id'] for r in before[table]}
        for row in values[table]:
            fields = list(row)
            if row['monday_id'] in old:
                fields.remove('monday_id')
                command = sql.SQL('UPDATE public.{} SET {} WHERE monday_id=%s RETURNING monday_id').format(
                    sql.Identifier(table), sql.SQL(',').join(sql.SQL('{}=%s').format(sql.Identifier(f)) for f in fields))
                args = [row[f] for f in fields] + [row['monday_id']]
            else:
                command = sql.SQL('INSERT INTO public.{} ({}) VALUES ({}) RETURNING monday_id').format(
                    sql.Identifier(table), sql.SQL(',').join(map(sql.Identifier, fields)),
                    sql.SQL(',').join(sql.Placeholder() for _ in fields))
                args = [row[f] for f in fields]
            from psycopg.types.json import Jsonb
            args = [Jsonb(v) if isinstance(v, (dict, list)) else v for v in args]
            if connection.execute(command, args).fetchone() is None:
                raise ValueError('Lifecycle guard refused a row; capture current Monday state again')
            actual = life.read_rows(connection, table, 'monday_id', [row['monday_id']])[0]
            for field, wanted in row.items():
                observed = actual.get(field)
                if wanted is not None and contract[table][field]['type'] == 'numeric':
                    equal = observed is not None and Decimal(str(observed)) == Decimal(str(wanted))
                else:
                    equal = observed == wanted
                if not equal:
                    raise ValueError(f'Post-write mismatch for {table}.{field}')
            if table == 'projects' and 'status_category' in actual:
                stage = actual.get('pipeline_stage')
                expected = 'Won' if stage == 'Won - Closed (Invoiced)' else 'Lost' if stage == 'Lost' else 'Open'
                if actual['status_category'] != expected:
                    raise ValueError('Generated status_category differs from defined schema')
            if job is not None:
                baseline = next((r for r in before[table] if r['monday_id'] == row['monday_id']), {})
                life.audit(connection, job, 'refresh_field_changes', table, row['monday_id'], baseline,
                    {'after_row': actual, 'changes': {k: {'before': baseline.get(k), 'after': actual.get(k)}
                        for k in sorted(set(baseline) | set(actual)) if baseline.get(k) != actual.get(k)}})


def refresh_new_enquiry(connection, monday, job, pid):
    """Targeted recovery: API-active children summed only for Open parents."""
    source = compare.capture_new_enquiry(monday, pid)
    parent = source['projects'][pid]
    amount = compare.project_new_enquiry_total(source, parent)
    before = {'projects': life.read_rows(connection, 'projects', 'monday_id', [pid]),
              'hidden_items': [], 'subitems': []}
    if len(before['projects']) != 1:
        raise life.ReviewRequired('Enquiry-only refresh requires one existing parent')
    category = compare.require_stored_enquiry_category(parent, before['projects'][0])
    eligible = category == 'Open'
    contract = reconcile.read_contract(connection)
    column = contract['projects'].get('new_enquiry_value')
    if not column or column['type'] != 'numeric' or column['generated'] != 'NEVER':
        raise life.ReviewRequired('New enquiry value is not a writable numeric column')
    normalized = reconcile.normalize_updates({'projects': [
        {'monday_id': pid, 'new_enquiry_value': amount if eligible else
         before['projects'][0].get('new_enquiry_value')}]}, contract)['projects'][0]
    changed = eligible and not same_value(before['projects'][0].get('new_enquiry_value'), normalized['new_enquiry_value'], column)
    if compare.capture_new_enquiry(monday, pid) != source:
        raise ValueError('Monday changed during enquiry refresh; retry with fresh evidence')
    with life.write_transaction(connection, job):
        if (life.read_rows(connection, 'projects', 'monday_id', [pid]) != before['projects']
                or reconcile.read_contract(connection) != contract):
            raise ValueError('Stored parent/schema changed during enquiry refresh')
        if changed:
            life.audit(connection, job, 'refresh_new_enquiry', 'projects', pid, before['projects'][0],
                       {'rule': compare.ENQUIRY_RULE, 'source': source})
            write_values(connection, {'projects': [normalized], 'hidden_items': [], 'subitems': []}, before, contract, job=job)
        round_number = int(job['payload'].get('verification_round', 0))
        if changed and round_number < 3:
            life.enqueue(connection, 'refresh', life.PARENT_BOARD_ID, pid,
                key=f"{job['event_key']}:verify_refresh", payload={'cause': job['event_key'],
                    'verification_round': round_number + 1, 'refresh_mode': 'new_enquiry_sum'})
        issues = [{'reason': 'Enquiry value keeps changing; inspect concurrent writers'}] if changed and round_number >= 3 else []
        life.finish(connection, job, 'review' if issues else 'processed',
            {'project_id': pid, 'rows_written': int(changed), 'issues': issues,
             'new_enquiry_value': normalized['new_enquiry_value'], 'refresh_mode': 'new_enquiry_sum',
             'eligible': eligible, 'status_category': category,
             'skipped_reason': '' if eligible else 'Parent status_category is not Open; value preserved',
             'verification_queued': bool(changed and round_number < 3)})


def refresh_project(connection, monday, job, *, pid=None):
    pid = pid or job['item_id']
    if job['payload'].get('cleanup_policy') is not None and (
            job['payload']['cleanup_policy'] != life.CLEANUP_POLICY
            or job['payload'].get('refresh_mode') != 'new_enquiry_sum'):
        raise life.ReviewRequired('Scoped cleanup cannot run a full refresh')
    if job['payload'].get('refresh_mode') == 'new_enquiry_sum':
        return refresh_new_enquiry(connection, monday, job, pid)
    if archive.enabled():
        root = life.read_items(monday, [pid])
        if life.require_item(root, pid, 'projects')['state'] == 'archived':
            if job['board_id'] != life.PARENT_BOARD_ID or job['item_id'] != pid:
                raise life.ReviewRequired('Cannot restore a child under an archived parent')
            return archive.archive_item(connection, monday, job, root)
    source = capture_refresh_source(connection, monday, pid)
    before = read_snapshot(connection, pid, source)
    contract = reconcile.read_contract(connection)
    reparent_id, former_parent, former_source = None, None, None
    if archive.enabled() and job['board_id'] == life.SUBITEM_BOARD_ID:
        target = next((r for r in before['subitems'] if r['monday_id'] == job['item_id']), None)
        former_parent = target.get('parent_monday_id') if target else None
        if former_parent and former_parent != pid:
            former_source = life.read_items(monday, [former_parent], parents=True)
            old_parent = life.require_item(former_source, former_parent, 'projects')
            if old_parent['state'] not in {'active', 'archived'} or any(
                    r['id'] == job['item_id'] for r in old_parent.get('subitems', [])):
                raise life.ReviewRequired('Former parent does not confirm the restored child moved')
            reparent_id = job['item_id']
    lifecycle = archive.read_states(connection, {t: list(source[t]) for t in life.BOARDS.values()}) if archive.enabled() else None
    values, issues = build_values(pid, source, before, contract, lifecycle=lifecycle, reparent_id=reparent_id)
    if archive.enabled():
        restoring = any(r.get('blocked') or r.get('monday_state') in {'archived', 'deleted'}
                        for rows in lifecycle.values() for r in rows.values())
        if restoring and any(i['monday_id'] in source.get(i['table'], {})
                             and source[i['table']][i['monday_id']]['state'] == 'active'
                             and not i['reason'].startswith('Source also has stored owner outside selection') for i in issues):
            raise life.ReviewRequired(f'Restoration requires complete current data: {issues}')
    # Re-read the same small scope before any database transaction.
    verified_at = datetime.now(timezone.utc)
    if capture_refresh_source(connection, monday, pid) != source:
        raise ValueError('Monday changed during refresh; retry with fresh evidence')
    if former_source is not None and life.read_items(monday, [former_parent], parents=True) != former_source:
        raise life.ReviewRequired('Former parent changed during restoration')
    with life.write_transaction(connection, job):
        if read_snapshot(connection, pid, source) != before or reconcile.read_contract(connection) != contract:
            raise ValueError('Stored rows/schema changed during lifecycle refresh')
        if archive.enabled():
            archive.observe_source(connection, job, source, verified_at, restore=True)
            for table in life.BOARDS.values():
                for item, evidence in source[table].items():
                    if evidence['state'] == 'active':
                        life.marker(connection, table, item, False, job)
        for table, rows in values.items():
            old = {r['monday_id']: r for r in before[table]}
            for row in rows:
                item = row['monday_id']
                # Only a fresh active item on the exact expected board can clear a marker.
                evidence = life.require_item(source[table], item, table, 'active')
                life.marker(connection, table, item, False, job)
                life.audit(connection, job, 'refresh_or_restore', table, item, old.get(item), evidence)
        write_values(connection, values, before, contract, job=job)
        if archive.enabled() and not any(i['table'] == 'projects' and i['field'] in archive.FINANCIAL_FIELDS for i in issues):
            archive.verify_parent_values(connection, job, pid)
        changed_hidden = {r['monday_id'] for r in values['hidden_items']}
        other_parents = {r.get('parent_monday_id') for r in before['subitems']
                         if r.get('hidden_item_id') in changed_hidden} - {None, '', pid}
        if reparent_id is not None and former_parent not in other_parents:
            archive.enqueue_refresh(connection, former_parent, f"{job['event_key']}:former_parent:{former_parent}")
        for parent_id in sorted(other_parents):
            life.enqueue(connection, 'refresh', life.PARENT_BOARD_ID, parent_id,
                         key=f"{job['event_key']}:shared_source:{parent_id}", payload={'cause': job['event_key']})
        # Shared exact links are valid. Any changed shared source gets durable
        # refreshes for its other stored owners instead of a permanent warning.
        issues = [i for i in issues if not i['reason'].startswith('Source also has stored owner outside selection')]
        changed = sum(map(len, values.values()))
        round_number = int(job['payload'].get('verification_round', 0))
        if changed and round_number < 3:
            life.enqueue(connection, 'refresh', life.PARENT_BOARD_ID, pid,
                         key=f"{job['event_key']}:verify_refresh",
                         payload={'verification_round': round_number + 1, 'cause': job['event_key']})
        if changed and round_number >= 3:
            issues.append({'reason': 'Source/SQL values keep changing; inspect concurrent writers'})
        life.finish(connection, job, 'review' if issues else 'processed',
                    {'project_id': pid, 'rows_written': changed, 'issues': issues,
                     'shared_owner_refresh_projects': sorted(other_parents),
                     'verification_queued': bool(changed and round_number < 3)})


def restore_item(connection, monday, job, observed):
    table = life.BOARDS[job['board_id']]
    if table == 'projects':
        refresh_project(connection, monday, job)
    elif table == 'subitems':
        parent = (observed.get('parent_item') or {}).get('id')
        if not parent:
            raise life.ReviewRequired('Restored subitem has no current Monday parent')
        refresh_project(connection, monday, job, pid=parent)
    else:
        # A hidden item can legitimately exist without any current owner. Restore
        # that exact item and refresh former owners using audit IDs, never names.
        item = job['item_id']
        columns = sorted(set(HIDDEN_ITEMS_COLUMNS.values()) - {'name'})
        source = compare.fetch_items(monday, [item], columns, mirror_depth=0)
        evidence = life.require_item(source, item, 'hidden_items', 'active')
        before = life.deletion_snapshot(connection, 'hidden_items', item)
        contract = reconcile.read_contract(connection)
        values = clean_rows('hidden_items', transformer()._transform_for_hidden_table([evidence]), contract)
        if len(values) != 1:
            raise ValueError('Hidden restoration transform failed')
        for field in compare.MONEY_FIELDS:
            values[0][field] = compare.numeric(compare.col(evidence, HIDDEN_ITEMS_COLUMNS[field]))
        values = reconcile.normalize_updates({'hidden_items': values}, contract)['hidden_items']
        verified_at = datetime.now(timezone.utc)
        if compare.fetch_items(monday, [item], columns, mirror_depth=0) != source:
            raise ValueError('Hidden source changed during restoration')
        with life.write_transaction(connection, job):
            if life.deletion_snapshot(connection, 'hidden_items', item) != before:
                raise ValueError('Hidden restoration baseline changed')
            if archive.enabled():
                archive.observe(connection, job, 'hidden_items', item, source, verified_at, restore=True)
            life.marker(connection, 'hidden_items', item, False, job)
            life.audit(connection, job, 'restore', 'hidden_items', item,
                       before['hidden_items'][0] if before['hidden_items'] else None, evidence)
            write_values(connection, {'projects': [], 'hidden_items': values, 'subitems': []}, before, contract, job=job)
            parents = {r['parent_id'] for r in connection.execute('''
                SELECT DISTINCT a.before_row->>'parent_monday_id' AS parent_id
                FROM public.monday_lifecycle_audit a
                WHERE a.action='unlink_deleted_hidden_source' AND a.before_row->>'hidden_item_id'=%s
                LIMIT 501''', (item,)).fetchall()} - {None, ''}
            parents.update(r['parent_monday_id'] for r in before['subitems'] if r.get('parent_monday_id'))
            if len(parents) > life.MAX_ROWS:
                raise life.ReviewRequired('Too many former hidden source owners')
            for parent in sorted(parents):
                life.enqueue(connection, 'refresh', life.PARENT_BOARD_ID, parent,
                             key=f"{job['event_key']}:refresh:{parent}")
            life.finish(connection, job, 'processed', {'restored_hidden_item': item, 'refresh_projects': sorted(parents)})
