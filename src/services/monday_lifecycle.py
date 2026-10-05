"""Durable, exact-ID Monday deletions and restoration, with no board traversal."""
from __future__ import annotations

import asyncio
from contextlib import contextmanager
import hashlib
import json
import logging
import os
import time
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from ..config import PARENT_BOARD_ID, SUBITEM_BOARD_ID, HIDDEN_ITEMS_BOARD_ID

LOG = logging.getLogger(__name__)
BOARDS = {str(PARENT_BOARD_ID): 'projects', str(SUBITEM_BOARD_ID): 'subitems',
          str(HIDDEN_ITEMS_BOARD_ID): 'hidden_items'}
DELETE_EVENTS = {'delete_pulse', 'delete_item', 'item_deleted', 'delete_subitem', 'subitem_deleted'}
RESTORE_EVENTS = {'restore_pulse', 'restore_item', 'item_restored', 'restore_subitem', 'subitem_restored'}
MAX_ROWS = 500
MAX_ATTEMPTS = 12


class ReviewRequired(ValueError):
    """Evidence is inconclusive or the operation exceeds the bounded workflow."""


def enabled():
    return os.getenv('MONDAY_LIFECYCLE_ENABLED', 'false').lower() in {'true', '1', 'yes'}


def event_from_payload(payload):
    event = payload.get('event') or {}
    event_type = event.get('type')
    if event_type not in DELETE_EVENTS | RESTORE_EVENTS:
        return None
    board = str(event.get('boardId') or '')
    item = str(event.get('pulseId') or event.get('itemId') or '')
    if board not in BOARDS or not item.isdecimal():
        raise ValueError('Lifecycle event requires a configured item board and numeric Monday ID')
    if event.get('pulseId') and event.get('itemId') and str(event['pulseId']) != str(event['itemId']):
        raise ValueError('Conflicting item IDs in lifecycle event')
    # The subscription lives on the parent board; boardId in a subitem payload
    # is the actual subitem board. Never guess a table from a name or parent ID.
    parent = event.get('parentItemId') or event.get('parent_item_id')
    if ('subitem' in event_type or parent) and board != str(SUBITEM_BOARD_ID):
        raise ValueError('Subitem lifecycle event must identify its actual subitem board')
    parent_board = event.get('parentItemBoardId') or event.get('parent_item_board_id')
    if parent_board and str(parent_board) != str(PARENT_BOARD_ID):
        raise ValueError('Unexpected parent board in lifecycle event')
    if parent is not None and not str(parent).isdecimal():
        raise ValueError('Invalid parent ID')
    kind = 'delete' if event_type in DELETE_EVENTS else 'restore'
    token = event.get('triggerUuid') or event.get('originalTriggerUuid') or event.get('id')
    if token is None:
        token = json.dumps(event, sort_keys=True, separators=(',', ':'))
    key = hashlib.sha256(f'{board}:{item}:{kind}:{token}'.encode()).hexdigest()
    return dict(event_key=key, board_id=board, item_id=item, kind=kind,
                parent_id=str(parent) if parent else None, payload=payload)


def persist_event(client, event):
    # Only immutable inputs are submitted. A duplicate cannot reset status/lease.
    client.table('monday_lifecycle_events').upsert(
        event, on_conflict='event_key', ignore_duplicates=True).execute()
    return event['event_key']


def connect():
    dsn = os.getenv('SUPABASE_DB_URL')
    if not dsn:
        raise RuntimeError('SUPABASE_DB_URL is required for the lifecycle worker')
    return psycopg.connect(dsn, autocommit=True, row_factory=dict_row,
                           connect_timeout=10, application_name='datacube-monday-lifecycle')


def enqueue(connection, kind, board, item, *, parent=None, payload=None, key=None):
    if str(board) not in BOARDS or not str(item).isdecimal():
        raise ValueError('Invalid lifecycle target')
    key = key or str(uuid4())
    connection.execute('INSERT INTO public.monday_lifecycle_events '
        '(event_key,board_id,item_id,kind,parent_id,payload) VALUES (%s,%s,%s,%s,%s,%s) '
        'ON CONFLICT(event_key) DO NOTHING', (key, str(board), str(item), kind, parent, Jsonb(payload or {})))
    return key


def claim(connection):
    return connection.execute('''
        WITH candidate AS (
            SELECT event_key FROM public.monday_lifecycle_events
            WHERE ((status IN ('pending','retry') AND next_attempt_at<=now())
                OR (status='processing' AND lease_until<now()))
            ORDER BY next_attempt_at,received_at FOR UPDATE SKIP LOCKED LIMIT 1
        ) UPDATE public.monday_lifecycle_events e SET status='processing',
            attempts=e.attempts+1,lease_token=gen_random_uuid(),lease_until=now()+interval '20 minutes'
        FROM candidate c WHERE e.event_key=c.event_key RETURNING e.*
    ''').fetchone()


def schedule_rechecks(connection):
    """Revisit at most ten known tombstones per idle pass, once per day each.

    Recovers missed restoration notifications, including subitem restorations.
    This queries the indexed marker queue, never all business tables or boards.
    """
    with connection.transaction():
        rows = connection.execute('SELECT table_name,monday_id,former_parent_id '
            'FROM public.monday_item_lifecycle WHERE blocked AND recheck_after<=now() '
            'ORDER BY recheck_after FOR UPDATE SKIP LOCKED LIMIT 10').fetchall()
        for row in rows:
            board = next(b for b, t in BOARDS.items() if t == row['table_name'])
            enqueue(connection, 'reconcile', board, row['monday_id'], parent=row['former_parent_id'],
                    payload={'verification_only': True, 'cause': 'periodic_tombstone_check'})
            connection.execute("UPDATE public.monday_item_lifecycle SET recheck_after=now()+interval '1 day' "
                'WHERE table_name=%s AND monday_id=%s', (row['table_name'], row['monday_id']))
        return len(rows)


def check_lease(connection, job):
    row = connection.execute('SELECT status,lease_token FROM public.monday_lifecycle_events '
        'WHERE event_key=%s FOR UPDATE', (job['event_key'],)).fetchone()
    if not row or row['status'] != 'processing' or row['lease_token'] != job['lease_token']:
        raise RuntimeError('Lifecycle lease replaced; stale worker cannot commit')


def finish(connection, job, status, result, *, error=None):
    connection.execute('UPDATE public.monday_lifecycle_events SET status=%s,result=%s,last_error=%s, '
        'processed_at=CASE WHEN %s IN (\'processed\',\'ignored\',\'review\') THEN now() ELSE NULL END, '
        'next_attempt_at=now()+(%s * interval \'1 second\'),lease_until=NULL '
        'WHERE event_key=%s AND lease_token=%s AND status=\'processing\'',
        (status, Jsonb(result), error, status, min(3600, 2 ** min(job['attempts'], 12)),
         job['event_key'], job['lease_token']))


@contextmanager
def write_transaction(connection, job):
    with connection.transaction():
        connection.execute('SET TRANSACTION ISOLATION LEVEL READ COMMITTED')
        connection.execute("SET LOCAL lock_timeout='750ms'")
        connection.execute("SET LOCAL statement_timeout='4s'")
        connection.execute("SET LOCAL transaction_timeout='10s'")
        # Protect absent IDs, incoming links and cascades. No HTTP while locked.
        connection.execute('LOCK TABLE public.projects,public.hidden_items,public.subitems '
                           'IN SHARE ROW EXCLUSIVE MODE')
        check_lease(connection, job)
        yield


def read_rows(connection, table, field, ids):
    if table not in BOARDS.values() or field not in {'monday_id','parent_monday_id','hidden_item_id'}:
        raise ValueError('Invalid row selector')
    if not ids:
        return []
    rows = connection.execute(sql.SQL('SELECT to_jsonb(t) AS row FROM public.{} t '
        'WHERE {}=ANY(%s) ORDER BY monday_id LIMIT %s').format(sql.Identifier(table), sql.Identifier(field)),
        (list(ids), MAX_ROWS + 1)).fetchall()
    if len(rows) > MAX_ROWS:
        raise ReviewRequired('Lifecycle dependency exceeds 500 rows; use a reviewed maintenance run')
    return [r['row'] for r in rows]


def deletion_snapshot(connection, table, item):
    result = {t: [] for t in BOARDS.values()}
    result[table] = read_rows(connection, table, 'monday_id', [item])
    if table in {'projects', 'hidden_items'}:
        field = 'parent_monday_id' if table == 'projects' else 'hidden_item_id'
        result['subitems'] = read_rows(connection, 'subitems', field, [item])
    if sum(map(len, result.values())) > MAX_ROWS:
        raise ReviewRequired('Lifecycle dependency exceeds 500 rows')
    return result


def read_items(monday, ids, *, parents=False):
    from scripts.order_value_monday_compare import fetch_items
    if len(ids) > MAX_ROWS:
        raise ReviewRequired('Too many lifecycle evidence IDs')
    return fetch_items(monday, list(ids), [], parents=parents, mirror_depth=0)


def require_item(items, item, table, state=None):
    row = items.get(item)
    if not row:
        raise ReviewRequired(f'Monday did not return {item}; absence is not deletion evidence')
    expected = next(board for board, name in BOARDS.items() if name == table)
    if str((row.get('board') or {}).get('id')) != expected:
        raise ReviewRequired(f'Monday item {item} belongs to a different board')
    if state and row.get('state') != state:
        raise ReviewRequired(f'Monday item {item} is not {state}')
    return row


def marker(connection, table, item, blocked, job, *, parent=None):
    connection.execute('INSERT INTO public.monday_item_lifecycle '
        '(table_name,monday_id,blocked,last_event_key,former_parent_id) VALUES (%s,%s,%s,%s,%s) '
        'ON CONFLICT(table_name,monday_id) DO UPDATE SET blocked=EXCLUDED.blocked, '
        'last_event_key=EXCLUDED.last_event_key,changed_at=now(), '
        'former_parent_id=COALESCE(EXCLUDED.former_parent_id,monday_item_lifecycle.former_parent_id)',
        (table, item, blocked, job['event_key'], parent))


def audit(connection, job, action, table, item, before, evidence):
    connection.execute('INSERT INTO public.monday_lifecycle_audit '
        '(event_key,action,table_name,monday_id,before_row,evidence) VALUES (%s,%s,%s,%s,%s,%s)',
        (job['event_key'], action, table, item, Jsonb(before), Jsonb(evidence)))


def apply_deletion(connection, job, before, evidence):
    table, item = BOARDS[job['board_id']], job['item_id']
    require_item(evidence, item, table, 'deleted')
    if table == 'projects':
        # A stored child might have moved in Monday. Never cascade-delete it on
        # the assumption that old Supabase membership is still authoritative.
        for child in before['subitems']:
            require_item(evidence, child['monday_id'], 'subitems', 'deleted')
    parents = {r.get('parent_monday_id') for r in before['subitems']} - {None, ''}
    if table == 'subitems' and job.get('parent_id'):
        parents.add(job['parent_id'])
    with write_transaction(connection, job):
        if deletion_snapshot(connection, table, item) != before:
            raise RuntimeError('Stored deletion dependencies changed; capture again')
        targets = [(table, row) for row in before[table]]
        if table == 'projects':
            targets += [('subitems', row) for row in before['subitems']]
        # Also block an absent root so an old create/upsert cannot resurrect it.
        marker(connection, table, item, True, job)
        for name, row in targets:
            marker(connection, name, row['monday_id'], True, job, parent=row.get('parent_monday_id'))
            proof = {'deleted_root': evidence[item], 'deleted_item': evidence[row['monday_id']]}
            audit(connection, job, 'delete', name, row['monday_id'], row, proof)
        if not targets:
            audit(connection, job, 'delete_already_absent', table, item, None, evidence)
        if table == 'hidden_items':
            for child in before['subitems']:
                audit(connection, job, 'unlink_deleted_hidden_source', 'subitems', child['monday_id'], child, evidence)
        connection.execute(sql.SQL('DELETE FROM public.{} WHERE monday_id=%s').format(sql.Identifier(table)), (item,))
        if read_rows(connection, table, 'monday_id', [item]):
            raise RuntimeError('Deleted row still exists; rolling back')
        if table == 'projects' and read_rows(connection, 'subitems', 'parent_monday_id', [item]):
            raise RuntimeError('Parent cascade failed; rolling back')
        if table == 'hidden_items' and read_rows(connection, 'subitems', 'hidden_item_id', [item]):
            raise RuntimeError('Hidden-source unlink failed; rolling back')
        if table == 'hidden_items':
            surviving = {r['monday_id']: r for r in read_rows(connection, 'subitems', 'monday_id',
                          [r['monday_id'] for r in before['subitems']])}
            for child in before['subitems']:
                actual = surviving.get(child['monday_id'])
                if actual is None or any(actual.get(k) != v for k, v in child.items()
                                         if k not in {'hidden_item_id', 'updated_at', 'last_synced_at'}):
                    raise RuntimeError('Hidden deletion unexpectedly altered a surviving child; rolling back')
        if table != 'projects':
            for parent in sorted(parents):
                enqueue(connection, 'refresh', PARENT_BOARD_ID, parent,
                        key=f"{job['event_key']}:refresh:{parent}", payload={'cause': job['event_key']})
        # Source verification is itself durable, so a restart after COMMIT does
        # not lose the check for a restoration concurrent with the deletion.
        enqueue(connection, 'reconcile', job['board_id'], item,
                parent=job.get('parent_id'), key=f"{job['event_key']}:verify",
                payload={'verification_only': True, 'cause': job['event_key']})
        finish(connection, job, 'processed', {'deleted': item, 'table': table, 'refresh_projects': sorted(parents)})


def process_job(connection, monday, job):
    if job['attempts'] > MAX_ATTEMPTS:
        raise ReviewRequired('Retry limit reached; inspect evidence and requeue this event')
    if job['kind'] == 'refresh':
        from .monday_lifecycle_refresh import refresh_project
        refresh_project(connection, monday, job)
        return
    table, item = BOARDS[job['board_id']], job['item_id']
    before = deletion_snapshot(connection, table, item)
    ids = {item}
    if table == 'projects':
        ids.update(r['monday_id'] for r in before['subitems'])
    evidence = read_items(monday, ids)
    observed = require_item(evidence, item, table)
    state = observed['state']
    blocked = connection.execute('SELECT blocked FROM public.monday_item_lifecycle '
        'WHERE table_name=%s AND monday_id=%s', (table, item)).fetchone()
    blocked = bool(blocked and blocked['blocked'])
    if state == 'deleted':
        if (job['payload'].get('verification_only') and not before[table] and blocked):
            with write_transaction(connection, job):
                current_marker = connection.execute('SELECT blocked FROM public.monday_item_lifecycle '
                    'WHERE table_name=%s AND monday_id=%s', (table, item)).fetchone()
                if read_rows(connection, table, 'monday_id', [item]) or not current_marker or not current_marker['blocked']:
                    raise ValueError('Lifecycle state changed during deletion verification')
                finish(connection, job, 'processed', {'deletion_verified': item})
            return
        apply_deletion(connection, job, before, evidence)
    elif state == 'active':
        if job['kind'] == 'delete' and not blocked:
            with write_transaction(connection, job):
                finish(connection, job, 'ignored', {'reason': 'Current Monday item is active; no deletion'})
            return
        if job['kind'] == 'reconcile' and not blocked:
            with write_transaction(connection, job):
                finish(connection, job, 'ignored', {'reason': 'Active item has no deletion marker'})
            return
        # Active evidence is required even for a purported restoration webhook.
        # Refresh clears markers and inserts fresh data atomically; no unchecked
        # interval during which stale import jobs can recreate an empty record.
        from .monday_lifecycle_refresh import restore_item
        restore_item(connection, monday, job, observed)
    else:
        raise ReviewRequired(f'Monday API state {state!r}; archive is not a deletion or restoration')


def run_once(connection=None, monday=None):
    own = connection is None
    own_monday = monday is None
    connection = connection or connect()
    try:
        job = claim(connection)
        if not job:
            schedule_rechecks(connection)
            job = claim(connection)
            if not job:
                return False
        try:
            if monday is None:
                from scripts.order_value_monday_compare import ComparisonMondayClient
                class BoundedMonday(ComparisonMondayClient):
                    def __init__(self):
                        super().__init__()
                        self.deadline = time.monotonic() + 600
                        self.remaining = 200

                    def execute_query(self, query, variables=None):
                        self.remaining -= 1
                        if self.remaining < 0 or time.monotonic() > self.deadline:
                            raise ValueError('Lifecycle Monday request/time budget exhausted; retry later')
                        return super().execute_query(query, variables)
                monday = BoundedMonday()
            process_job(connection, monday, job)
        except Exception as exc:
            # Exception class/message only for evidence/logic failures; transport
            # exceptions may contain credential-bearing DSNs or URLs.
            message = str(exc)[:1500] if isinstance(exc, (ReviewRequired, ValueError)) else type(exc).__name__
            status = 'review' if isinstance(exc, ReviewRequired) or job['attempts'] >= MAX_ATTEMPTS else 'retry'
            finish(connection, job, status, {'error_type': type(exc).__name__}, error=message)
            LOG.warning('Lifecycle %s for %s: %s', status, job['item_id'], message)
        return True
    finally:
        if own_monday and monday is not None:
            monday.session.close()
        if own:
            connection.close()


class LifecycleWorker:
    def __init__(self):
        self.task = None
        self.stopping = False

    def start(self):
        if enabled() and (self.task is None or self.task.done()):
            if not os.getenv('SUPABASE_DB_URL'):
                raise RuntimeError('Enabled lifecycle worker requires SUPABASE_DB_URL')
            self.stopping = False
            self.task = asyncio.create_task(self.run(), name='monday-lifecycle')

    async def run(self):
        while not self.stopping:
            try:
                worked = await asyncio.to_thread(run_once)
            except Exception as exc:
                LOG.error('Lifecycle worker unavailable: %s', type(exc).__name__)
                worked = False
            if not worked:
                await asyncio.sleep(5)

    async def stop(self):
        self.stopping = True
        if self.task:
            # Cancelling the waiter does not cancel an SQL transaction in its
            # thread. Lease fencing and atomic commits make restart safe.
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass


worker = LifecycleWorker()
