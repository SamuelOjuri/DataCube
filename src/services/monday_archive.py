"""Verified API lifecycle observations; never infer state from business labels."""
from __future__ import annotations

from datetime import datetime, timezone
import logging
import os
from uuid import uuid4

from psycopg.types.json import Jsonb

from . import monday_lifecycle as life

LOG = logging.getLogger(__name__)
POLICY = 'verified_archive_v1'
FINANCIAL_FIELDS = {'total_order_value', 'total_amount_invoiced', 'new_enquiry_value'}


def enabled() -> bool:
    return os.getenv('MONDAY_ARCHIVE_ENABLED', 'false').lower() in {'true', '1', 'yes'}


def current_relation(name: str) -> str:
    reporting = os.getenv('MONDAY_ARCHIVE_REPORTING_ENABLED', 'false').lower() in {'true', '1', 'yes'}
    if not reporting:
        return name
    if not enabled():
        raise RuntimeError('Archive reporting requires MONDAY_ARCHIVE_ENABLED')
    with life.connect() as connection:
        with connection.transaction():
            connection.execute('SET TRANSACTION READ ONLY')
            missing = coverage(connection)
            if any(missing.values()):
                raise life.ReviewRequired(f'Archive reporting coverage is incomplete: {missing}')
    return {
        'reportable_projects': 'current_projects',
        'vw_pipeline_forecast_project_v1': 'current_pipeline_forecast_project',
        'vw_pipeline_smoothing_score_v1': 'current_pipeline_smoothing_score',
        'mv_pipeline_forecast_monthly_12m_v1': 'current_pipeline_forecast_monthly',
        'mv_pipeline_smoothed_revenue_monthly_12m_v1': 'current_pipeline_smoothed_monthly',
        'create_pipeline_forecast_snapshot': 'create_current_pipeline_forecast_snapshot',
        'create_pipeline_smoothing_forecast_snapshot': 'create_current_pipeline_smoothing_forecast_snapshot',
    }[name]


def coverage(connection):
    """Unknown identities or unverifiable current links must not disappear from totals."""
    return connection.execute('''
        SELECT
        (SELECT count(*) FROM public.reportable_projects p
         LEFT JOIN public.monday_item_lifecycle l ON l.table_name='projects' AND l.monday_id=p.monday_id
         WHERE l.monday_state IS NULL AND NOT COALESCE(l.blocked,false)) AS unverified_projects,
        (SELECT count(*) FROM public.subitems s JOIN public.reportable_projects p ON p.monday_id=s.parent_monday_id
         JOIN public.monday_item_lifecycle pl ON pl.table_name='projects' AND pl.monday_id=p.monday_id
         LEFT JOIN public.monday_item_lifecycle l ON l.table_name='subitems' AND l.monday_id=s.monday_id
         WHERE pl.monday_state='active' AND NOT pl.blocked AND NOT COALESCE(l.blocked,false)
           AND (l.monday_state IS NULL OR (l.monday_state='active'
                AND l.state_evidence->>'parent_monday_id' IS DISTINCT FROM s.parent_monday_id))) AS unverified_subitems,
        (SELECT count(DISTINCT s.hidden_item_id) FROM public.current_subitems s
         LEFT JOIN public.monday_item_lifecycle l ON l.table_name='hidden_items' AND l.monday_id=s.hidden_item_id
         WHERE s.hidden_item_id IS NOT NULL AND l.monday_state IS NULL
           AND NOT COALESCE(l.blocked,false)) AS unverified_sources,
        (SELECT count(*) FROM public.current_projects p
         JOIN public.monday_item_lifecycle l ON l.table_name='projects' AND l.monday_id=p.monday_id
         WHERE l.state_evidence->>'transaction_values_verified' IS DISTINCT FROM 'true'
            OR jsonb_typeof(l.state_evidence->'item'->'subitems') IS DISTINCT FROM 'array'
            OR EXISTS (
                SELECT FROM jsonb_array_elements(CASE
                    WHEN jsonb_typeof(l.state_evidence->'item'->'subitems')='array'
                    THEN l.state_evidence->'item'->'subitems' ELSE '[]'::jsonb END) member
                WHERE COALESCE(member->>'state','active')='active' AND NOT EXISTS (
                    SELECT FROM public.current_subitems s WHERE s.monday_id=member->>'id'
                      AND s.parent_monday_id=p.monday_id))) AS unverified_current_values,
        (SELECT count(*) FROM public.monday_lifecycle_events e
         WHERE e.payload->>'archive_policy'=%s AND (e.status IN ('retry','review')
           OR (e.status IN ('pending','processing')
               AND e.payload->>'cause' IS DISTINCT FROM 'periodic_lifecycle_check'))
           AND ((e.board_id=%s AND EXISTS (SELECT FROM public.reportable_projects p WHERE p.monday_id=e.item_id))
             OR (e.board_id=%s AND EXISTS (SELECT FROM public.subitems s JOIN public.reportable_projects p
                 ON p.monday_id=s.parent_monday_id WHERE s.monday_id=e.item_id))
             OR (e.board_id=%s AND EXISTS (SELECT FROM public.subitems s JOIN public.reportable_projects p
                 ON p.monday_id=s.parent_monday_id WHERE s.hidden_item_id=e.item_id)))) AS unresolved_archive_jobs
    ''', (POLICY, life.PARENT_BOARD_ID, life.SUBITEM_BOARD_ID, life.HIDDEN_ITEMS_BOARD_ID)).fetchone()


def require_runtime(connection):
    row = connection.execute("SELECT to_regprocedure('public.guard_monday_archived_item()') IS NOT NULL AS ready").fetchone()
    if not row['ready']:
        raise RuntimeError('Apply monday_lifecycle_archive_runtime.sql before enabling archive handling')


def state_row(connection, table, item):
    return connection.execute('SELECT * FROM public.monday_item_lifecycle WHERE table_name=%s AND monday_id=%s',
                              (table, item)).fetchone()


def read_states(connection, boundary):
    return {table: {r['row']['monday_id']: r['row'] for r in connection.execute(
        'SELECT to_jsonb(l) AS row FROM public.monday_item_lifecycle l '
        'WHERE table_name=%s AND monday_id=ANY(%s)', (table, list(ids))).fetchall()}
        for table, ids in boundary.items() if table in life.BOARDS.values()}


def verify_parent_values(connection, job, pid):
    connection.execute('UPDATE public.monday_item_lifecycle '
        "SET state_evidence=state_evidence || '{\"transaction_values_verified\":true}'::jsonb "
        "WHERE table_name='projects' AND monday_id=%s AND monday_state='active'", (pid,))
    life.audit(connection, job, 'verify_current_parent_values', 'projects', pid, None,
               {'basis': 'fresh_monday_projection_and_checked_sql_after_values',
                'fields': sorted(FINANCIAL_FIELDS)})


def observe(connection, job, table, item, evidence, verified_at, *, restore=False):
    """Called inside the writer transaction, after a second matching API capture."""
    source = life.require_item(evidence, item, table)
    state = source.get('state')
    if state not in {'active', 'archived', 'deleted'}:
        raise life.ReviewRequired(f'Unknown API lifecycle state for {item}')
    previous = state_row(connection, table, item)
    if previous and previous['state_verified_at'] and previous['state_verified_at'] > verified_at:
        raise life.ReviewRequired('A newer lifecycle observation already exists')
    if state == 'active' and previous and (
            previous['blocked'] or previous['monday_state'] in {'archived', 'deleted'}) and not restore:
        raise life.ReviewRequired(f'{item} requires full verified restoration, not an ordinary upsert')
    parent = (source.get('parent_item') or {}).get('id')
    if table == 'subitems' and state == 'active' and not parent:
        raise life.ReviewRequired(f'Active subitem {item} has no verified parent')
    proof = {'basis': 'exact_id_monday_api', 'parent_monday_id': parent, 'item': source}
    connection.execute('''
        INSERT INTO public.monday_item_lifecycle
            (table_name,monday_id,monday_state,state_verified_at,state_event_key,state_evidence)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT(table_name,monday_id) DO UPDATE SET
            monday_state=EXCLUDED.monday_state,state_verified_at=EXCLUDED.state_verified_at,
            state_event_key=EXCLUDED.state_event_key,state_evidence=EXCLUDED.state_evidence,
            changed_at=CASE WHEN monday_item_lifecycle.monday_state IS DISTINCT FROM EXCLUDED.monday_state
                THEN now() ELSE monday_item_lifecycle.changed_at END,
            recheck_after=now()+interval '1 day'
    ''', (table, item, state, verified_at, job['event_key'], Jsonb(proof)))
    life.audit(connection, job, 'observe_monday_state', table, item,
               {k: str(v) if isinstance(v, datetime) else v for k, v in previous.items()} if previous else None,
               {**proof, 'verified_at': verified_at.isoformat(), 'state': state})


def observe_source(connection, job, source, verified_at, *, restore=False):
    for table in life.BOARDS.values():
        for item, evidence in sorted(source.get(table, {}).items()):
            if evidence.get('state') == 'active':
                observe(connection, job, table, item, {item: evidence}, verified_at, restore=restore)


def enqueue_refresh(connection, parent, key):
    return life.enqueue(connection, 'refresh', life.PARENT_BOARD_ID, parent,
        key=key, payload={'archive_policy': POLICY, 'refresh_mode': 'archive_current_values'})


def archive_item(connection, monday, job, evidence):
    table, item = life.BOARDS[job['board_id']], job['item_id']
    before = life.deletion_snapshot(connection, table, item)
    if len(before[table]) != 1:
        raise life.ReviewRequired('Soft archive requires the retained business row; do not invent history')
    if job['payload'].get('reviewed_before') is not None and job['payload']['reviewed_before'] != before:
        raise life.ReviewRequired('Archive rows changed since the reviewed preview; stage again')
    observed = life.require_item(evidence, item, table, 'archived')
    verified_at = datetime.now(timezone.utc)
    if life.read_items(monday, [item]).get(item) != observed:
        raise life.ReviewRequired('Monday changed during archive verification')
    parents = {r.get('parent_monday_id') for r in before['subitems']} - {None, ''}
    with life.write_transaction(connection, job):
        if life.deletion_snapshot(connection, table, item) != before:
            raise life.ReviewRequired('Stored archive scope changed; retry')
        observe(connection, job, table, item, {item: observed}, verified_at)
        life.audit(connection, job, 'retain_archived_record', table, item, before[table][0], observed)
        if table != 'projects' and not job['payload'].get('verification_only'):
            for parent in sorted(parents):
                enqueue_refresh(connection, parent, f"{job['event_key']}:refresh:{parent}")
        if not job['payload'].get('verification_only'):
            life.enqueue(connection, 'reconcile', job['board_id'], item,
                         key=f"{job['event_key']}:verify_archive", payload={'verification_only': True})
        life.finish(connection, job, 'processed',
            {'archived': item, 'table': table, 'retained': True, 'business_rows_changed': 0,
             'refresh_projects': sorted(parents) if table != 'projects' else []})


def refresh_current_values(connection, monday, job):
    """Fresh parent mirror values and active-child enquiry/invoices, never stored-child sums."""
    from scripts import order_value_monday_compare as compare
    from scripts import reconcile_order_values as reconcile
    from . import monday_lifecycle_refresh as refresh

    pid = job['item_id']
    before_children = life.read_rows(connection, 'subitems', 'parent_monday_id', [pid])
    source = compare.capture(monday, [pid], [r['monday_id'] for r in before_children])
    parent = life.require_item(source['projects'], pid, 'projects')
    if parent['state'] == 'archived':
        return archive_item(connection, monday, job, life.read_items(monday, [pid]))
    life.require_item(source['projects'], pid, 'projects', 'active')
    before = refresh.read_snapshot(connection, pid, source)
    if not before['projects']:
        raise life.ReviewRequired('Current-value refresh requires an existing project')
    contract = reconcile.read_contract(connection)
    lifecycle = read_states(connection, {t: list(source[t]) for t in life.BOARDS.values()})
    parent_state = lifecycle['projects'].get(pid, {})
    if parent_state.get('blocked') or parent_state.get('monday_state') in {'archived', 'deleted'}:
        with life.write_transaction(connection, job):
            life.enqueue(connection, 'restore', life.PARENT_BOARD_ID, pid,
                         key=f"{job['event_key']}:restore")
            life.finish(connection, job, 'processed', {'deferred_to_restoration': pid, 'rows_written': 0})
        LOG.warning('Deferring current values for %s until full restoration', pid)
        return
    proposed, issues = compare.project_projection(pid, source, before, contract, lifecycle=lifecycle)
    values = {t: [] for t in life.BOARDS.values()}
    row = {k: v for k, v in proposed['projects'].get(pid, {}).items()
           if k == 'monday_id' or k in FINANCIAL_FIELDS}
    if len(row) > 1:
        values['projects'] = [row]
    if 'new_enquiry_value' in row:
        compare.require_stored_enquiry_category(parent, before['projects'][0])
    verified_at = datetime.now(timezone.utc)
    if compare.capture(monday, [pid], [r['monday_id'] for r in before_children]) != source:
        raise life.ReviewRequired('Monday changed during current-value refresh')
    with life.write_transaction(connection, job):
        if refresh.read_snapshot(connection, pid, source) != before or reconcile.read_contract(connection) != contract:
            raise life.ReviewRequired('Current-value baseline changed')
        if read_states(connection, {t: list(source[t]) for t in life.BOARDS.values()}) != lifecycle:
            raise life.ReviewRequired('Lifecycle states changed during current-value refresh')
        # This path is not a restoration path. Never reactivate an archived row here.
        observe_source(connection, job, source, verified_at)
        refresh.write_values(connection, values, before, contract, job=job)
        financial_issues = [i for i in issues if i['table'] == 'projects' and i['field'] in FINANCIAL_FIELDS]
        if not financial_issues:
            verify_parent_values(connection, job, pid)
        life.finish(connection, job, 'review' if financial_issues else 'processed',
            {'project_id': pid, 'rows_written': len(values['projects']), 'issues': issues,
             'financial_issues': financial_issues, 'refresh_mode': 'archive_current_values'})


def refresh_parents(parent_ids):
    """Synchronous, leased refresh; errors also remain in the durable queue."""
    from scripts import order_value_monday_compare as compare

    count = 0
    with life.connect() as connection:
        require_runtime(connection)
        monday = compare.ComparisonMondayClient()
        for parent in sorted(set(parent_ids)):
            prefix = f'recovery:{uuid4()}:'
            with connection.transaction():
                key = enqueue_refresh(connection, parent, f'{prefix}current-values')
                job = life.claim(connection, event_prefixes=[prefix])
            if job is None:
                raise RuntimeError('Could not claim current-value refresh')
            try:
                refresh_current_values(connection, monday, job)
            except (ValueError, RuntimeError) as exc:
                with life.write_transaction(connection, job):
                    life.finish(connection, job, 'review', {}, error=str(exc))
                raise
            result = connection.execute('SELECT status,result FROM public.monday_lifecycle_events WHERE event_key=%s',
                                        (key,)).fetchone()
            if result['status'] != 'processed':
                raise life.ReviewRequired(f"Current values for {parent} require review: {result['result']}")
            count += result['result'].get('rows_written', 0)
    return count


def prepare_sync_rows(table, rows):
    """Fresh exact-ID gate for every normal upsert, independent of extraction labels."""
    if not enabled() or not rows:
        return rows
    from scripts import order_value_monday_compare as compare

    ids = {str(r['monday_id']) for r in rows}
    if len(ids) > life.MAX_ROWS:
        raise life.ReviewRequired('Archive-aware sync batch exceeds 500 IDs')
    monday = compare.ComparisonMondayClient()
    with life.connect() as connection:
        require_runtime(connection)
        columns = [compare.SUBITEM_COLUMNS['hidden_item_id']] if table == 'subitems' else []
        source = compare.fetch_items(monday, ids, columns, mirror_depth=0)
        for item in ids:
            life.require_item(source, item, table)
        parent_ids = {(v.get('parent_item') or {}).get('id') for v in source.values()} - {None, ''}
        parents = life.read_items(monday, parent_ids) if table == 'subitems' else {}
        for pid in parent_ids:
            life.require_item(parents, pid, 'projects')
        hidden_ids = {str(r['hidden_item_id']) for r in rows if r.get('hidden_item_id')} if table == 'subitems' else set()
        hidden = life.read_items(monday, hidden_ids)
        for hid in hidden_ids:
            life.require_item(hidden, hid, 'hidden_items')
        verified_at = datetime.now(timezone.utc)
        if compare.fetch_items(monday, ids, columns, mirror_depth=0) != source or (
                parents and life.read_items(monday, parent_ids) != parents) or (
                hidden and life.read_items(monday, hidden_ids) != hidden):
            raise life.ReviewRequired('Monday changed while gating normal sync')
        accepted = []
        operation = {'event_key': f'sync-observation:{uuid4()}'}
        with connection.transaction():
            connection.execute("SET LOCAL lock_timeout='750ms'")
            connection.execute("SET LOCAL statement_timeout='4s'")
            connection.execute('LOCK TABLE public.projects,public.hidden_items,public.subitems IN SHARE ROW EXCLUSIVE MODE')
            for row in rows:
                item = str(row['monday_id'])
                previous = state_row(connection, table, item)
                state = source[item]['state']
                stored = life.read_rows(connection, table, 'monday_id', [item])
                moved = (table == 'subitems' and stored
                         and stored[0].get('parent_monday_id') != (source[item].get('parent_item') or {}).get('id'))
                if state == 'active' and (moved or (previous and (
                        previous['blocked'] or previous['monday_state'] in {'archived', 'deleted'}))):
                    life.enqueue(connection, 'restore', next(b for b, t in life.BOARDS.items() if t == table),
                        item, payload={'archive_policy': POLICY}, key=f"{operation['event_key']}:restore:{item}")
                    LOG.warning('Deferring %s %s to verified restoration', table, item)
                    continue
                observe(connection, operation, table, item, source, verified_at)
                if state != 'active':
                    if state == 'archived':
                        stored = life.deletion_snapshot(connection, table, item)
                        life.audit(connection, operation, 'retain_archived_record', table, item,
                                   stored[table][0] if stored[table] else None, source[item])
                        life.enqueue(connection, 'reconcile', next(b for b, t in life.BOARDS.items() if t == table),
                                     item, key=f"{operation['event_key']}:verify:{item}",
                                     payload={'verification_only': True})
                        for pid in sorted({r.get('parent_monday_id') for r in stored['subitems']} - {None, ''}):
                            enqueue_refresh(connection, pid, f"{operation['event_key']}:refresh:{pid}")
                    LOG.info('Retaining inactive %s %s without a business-row upsert', table, item)
                    continue
                if table == 'subitems':
                    pid = (source[item].get('parent_item') or {}).get('id')
                    if row.get('parent_monday_id') != pid:
                        raise life.ReviewRequired(f'Stale parent membership in incoming subitem {item}')
                    if parents[pid]['state'] != 'active':
                        LOG.info('Skipping subitem %s under inactive parent %s', item, pid)
                        continue
                    links = compare.links(source[item])
                    expected = [str(row['hidden_item_id'])] if row.get('hidden_item_id') else []
                    if links != expected:
                        raise life.ReviewRequired(f'Stale or ambiguous hidden-source link for {item}')
                    dependencies = [('projects', pid, parents[pid])]
                    dependencies.extend(('hidden_items', hid, hidden[hid]) for hid in expected)
                    deferred = False
                    for dep_table, dep_id, dep_source in dependencies:
                        previous_dep = state_row(connection, dep_table, dep_id)
                        if previous_dep and (previous_dep['blocked'] or previous_dep['monday_state'] in {'archived', 'deleted'}):
                            if dep_source['state'] == 'active':
                                life.enqueue(connection, 'restore', next(b for b, t in life.BOARDS.items() if t == dep_table),
                                    dep_id, key=f"{operation['event_key']}:restore:{dep_table}:{dep_id}")
                            deferred = True
                        elif dep_source['state'] != 'active':
                            observe(connection, operation, dep_table, dep_id, {dep_id: dep_source}, verified_at)
                            deferred = True
                    if deferred:
                        LOG.warning('Deferring subitem %s until parent/source lifecycle is eligible', item)
                        continue
                clean = dict(row)
                if table == 'projects':
                    existing = life.read_rows(connection, table, 'monday_id', [item])
                    for field in FINANCIAL_FIELDS:
                        if existing:
                            clean.pop(field, None)
                        else:
                            clean[field] = None
                accepted.append(clean)
        return accepted


def preview_values(connection, monday, selected):
    """Read-only hypothetical financial effects, with selected archives represented."""
    from scripts import order_value_monday_compare as compare
    from scripts import reconcile_order_values as reconcile
    from . import monday_lifecycle_refresh as refresh

    parents = {r.get('parent_monday_id') for entry in selected for r in entry['before']['subitems']} - {None, ''}
    archived_parents = {entry['item_id'] for entry in selected if life.BOARDS[entry['board_id']] == 'projects'}
    results = []
    for pid in sorted(parents - archived_parents):
        with connection.transaction():
            connection.execute('SET TRANSACTION READ ONLY')
            children = life.read_rows(connection, 'subitems', 'parent_monday_id', [pid])
        source = compare.capture(monday, [pid], [r['monday_id'] for r in children])
        with connection.transaction():
            connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
            before = refresh.read_snapshot(connection, pid, source)
            contract = reconcile.read_contract(connection)
            states = read_states(connection, {t: list(source[t]) for t in life.BOARDS.values()})
        for entry in selected:
            table, item = life.BOARDS[entry['board_id']], entry['item_id']
            if source[table].get(item, {}).get('state') == 'archived':
                states[table][item] = {'monday_state': 'archived', 'blocked': False}
        proposed, issues = compare.project_projection(pid, source, before, contract, lifecycle=states)
        values = {k: v for k, v in proposed['projects'].get(pid, {}).items() if k in FINANCIAL_FIELDS}
        if compare.capture(monday, [pid], [r['monday_id'] for r in children]) != source:
            raise life.ReviewRequired('Monday changed during archive preview; stage again')
        results.append({'project_id': pid, 'before': {k: before['projects'][0].get(k) for k in FINANCIAL_FIELDS},
                        'proposed': values, 'issues': issues})
    return {'active_parent_values': results, 'archived_parents_preserved': sorted(archived_parents),
            'historical_business_rows_deleted': 0, 'hypothetical_only': True}
