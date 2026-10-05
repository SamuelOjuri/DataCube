"""Operate the durable lifecycle worker and stage historical exact-ID deletions."""
import argparse
import asyncio
from collections import Counter
import csv
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import time
from uuid import uuid4

from dotenv import load_dotenv

from src.services import monday_lifecycle as life


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()


def code_digest():
    root = Path(__file__).resolve().parents[1]
    names = ['scripts/monday_lifecycle.py', 'src/services/monday_lifecycle.py',
             'src/services/monday_lifecycle_activity.py',
             'src/services/monday_lifecycle_refresh.py', 'src/database/schema/monday_lifecycle.sql',
             'src/database/schema/monday_lifecycle_scoped_cleanup.sql']
    from scripts.order_value_monday_compare import code_fingerprint
    return digest({'files': {n: hashlib.sha256((root / n).read_bytes()).hexdigest() for n in names},
                   'shared_comparison_code': code_fingerprint()})


def target_digest(connection):
    row = connection.execute('SELECT current_database() AS db, inet_server_addr() AS host, '
                             'inet_server_port() AS port').fetchone()
    return digest(row)


def selected_targets(args):
    targets = set()
    if args.review_csv:
        if args.board or args.item_id:
            raise ValueError('Choose a review CSV or explicit IDs, not both')
        with args.review_csv.open(encoding='utf-8-sig', newline='') as stream:
            for row in csv.DictReader(stream):
                reason = row.get('review_reason', row.get('reason', ''))
                if 'state=deleted' not in reason:
                    continue
                table = row.get('table')
                board = next((b for b, t in life.BOARDS.items() if t == table), None)
                item = row.get('affected_monday_id', row.get('monday_id', ''))
                if board and item.isdecimal():
                    targets.add((board, item))
    else:
        if not args.board or not args.item_id:
            raise ValueError('Provide --board and --item-id, or --review-csv')
        targets = {(args.board, item) for item in args.item_id}
    if not targets or len(targets) > 100 or any(b not in life.BOARDS or not i.isdecimal() for b, i in targets):
        raise ValueError('Select between 1 and 100 exact lifecycle targets')
    return sorted(targets)


def activity_request(args, targets):
    from src.services import monday_lifecycle_activity as activity
    log_id = getattr(args, 'activity_log_id', None)
    since = getattr(args, 'activity_log_from', None)
    parent = getattr(args, 'parent_id', None)
    creation = getattr(args, 'activity_creation_log_id', None)
    survivor = getattr(args, 'preserve_item_id', None)
    if not any((log_id, since, parent, creation, survivor)):
        return None
    if (not all((log_id, since, parent)) or args.review_csv or len(targets) != 1
            or targets[0][0] != life.SUBITEM_BOARD_ID):
        raise ValueError('Activity recovery requires one explicit subitem, --parent-id, '
                         '--activity-log-id and --activity-log-from; no review CSV')
    request = dict(mode=activity.MODE, board_id=targets[0][0], item_id=targets[0][1],
                   parent_id=parent, log_id=log_id, **{'from': since})
    if creation or survivor:
        request.update(mode=activity.LINKED_MODE, creation_log_id=creation, preserve_item_id=survivor)
    activity.validate_request(request)
    return request


def stage(connection, monday, args):
    if args.run_dir.exists():
        raise ValueError('Use a new run directory')
    targets = selected_targets(args)
    recovery = activity_request(args, targets)
    evidence = life.read_items(monday, {i for _, i in targets})
    selected, deferred = [], []
    # No transaction spans the Monday network read; this SQL transaction is read-only.
    with connection.transaction():
        connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
        target = target_digest(connection)
        for board, item in targets:
            try:
                if not recovery:
                    life.require_item(evidence, item, life.BOARDS[board], 'deleted')
                before = life.deletion_snapshot(connection, life.BOARDS[board], item)
                selected.append(dict(board_id=board, item_id=item, before=before))
            except life.ReviewRequired as exc:
                deferred.append(dict(board_id=board, item_id=item, reason=str(exc)))
    # Activity network reads never run inside the database transaction.
    if recovery and selected:
        from src.services import monday_lifecycle_activity as activity
        entry = selected.pop()
        try:
            activity.require_snapshot(recovery, entry['before'])
            proof = activity.capture(monday, recovery)
            entry.update(activity_recovery=proof['request'], activity_evidence=proof)
            selected.append(entry)
        except life.ReviewRequired as exc:
            deferred.append(dict(board_id=recovery['board_id'], item_id=recovery['item_id'], reason=str(exc)))
    plan = dict(selected=selected, deferred=deferred, evidence=evidence)
    args.run_dir.mkdir(parents=True)
    (args.run_dir / 'plan.json').write_text(json.dumps(plan, indent=2, default=str), encoding='utf-8')
    with (args.run_dir / 'review.csv').open('w', encoding='utf-8-sig', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=['board_id','item_id','table','stored_rows','dependent_subitems',
            'evidence_basis','parent_id','activity_log_id','deleted_at_utc'])
        writer.writeheader()
        for entry in selected:
            table = life.BOARDS[entry['board_id']]
            writer.writerow(dict(board_id=entry['board_id'], item_id=entry['item_id'], table=table,
                stored_rows=len(entry['before'][table]),
                dependent_subitems=len(entry['before']['subitems']) if table != 'subitems' else 0,
                evidence_basis='activity_log' if entry.get('activity_recovery') else 'current_state',
                parent_id=entry.get('activity_recovery', {}).get('parent_id', ''),
                activity_log_id=entry.get('activity_recovery', {}).get('log_id', ''),
                deleted_at_utc=activity.event_time(entry['activity_evidence']['deletion_event']).isoformat()
                    if entry.get('activity_recovery') else ''))
    manifest = dict(run_id=str(uuid4()), prepared_at=datetime.now(timezone.utc).isoformat(),
                    target=target, code=code_digest(), sha256=digest(plan),
                    review_sha256=hashlib.sha256((args.run_dir / 'review.csv').read_bytes()).hexdigest(),
                    selected=len(selected), deferred=len(deferred), read_only=True)
    (args.run_dir / 'manifest.json').write_text(json.dumps(manifest, indent=2), encoding='utf-8')
    return manifest


def queue_run(connection, args):
    manifest = json.loads((args.run_dir / 'manifest.json').read_text())
    plan = json.loads((args.run_dir / 'plan.json').read_text())
    if not plan['selected']:
        raise ValueError('No confirmed deletions were staged')
    if (args.confirm_run_id != manifest['run_id'] or code_digest() != manifest['code']
            or target_digest(connection) != manifest['target'] or digest(plan) != manifest['sha256']
            or hashlib.sha256((args.run_dir / 'review.csv').read_bytes()).hexdigest() != manifest['review_sha256']):
        raise ValueError('Confirmation, code, target or reviewed artifacts differ; stage again')
    keys = []
    with connection.transaction():
        for entry in plan['selected']:
            board, item = entry['board_id'], entry['item_id']
            if life.deletion_snapshot(connection, life.BOARDS[board], item) != entry['before']:
                raise ValueError(f'Supabase target {item} changed since staging; stage again')
            key = f"recovery:{manifest['run_id']}:{board}:{item}"
            payload = {'recovery_run_id': manifest['run_id'], 'review_sha256': manifest['sha256']}
            recovery = entry.get('activity_recovery')
            if recovery:
                from src.services import monday_lifecycle_activity as activity
                activity.require_snapshot(recovery, entry['before'])
                if (board != recovery['board_id'] or item != recovery['item_id']
                        or entry['activity_evidence']['request'] != recovery
                        or activity.fingerprint(entry['activity_evidence']['deletion_event']) != recovery['deletion_sha256']):
                    raise ValueError('Activity recovery differs from the staged selection')
                payload.update(activity_recovery=recovery, reviewed_before=entry['before'])
            life.enqueue(connection, 'delete', board, item, key=key,
                         parent=recovery['parent_id'] if recovery else None, payload=payload)
            keys.append(key)
    return dict(queued=len(keys), event_keys=keys,
                note='Worker rechecks current Monday state and related rows before deleting')


def main(argv=None):
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__)
    subs = parser.add_subparsers(dest='command', required=True)
    p = subs.add_parser('stage')
    p.add_argument('--board', choices=list(life.BOARDS))
    p.add_argument('--item-id', action='append')
    p.add_argument('--review-csv', type=Path)
    p.add_argument('--run-dir', type=Path, required=True)
    p.add_argument('--activity-log-id', help='Opt in to recovery of one missing subitem using this deletion event')
    p.add_argument('--activity-log-from', help='ISO timestamp with timezone before the deletion; history is read through now')
    p.add_argument('--parent-id', help='Exact reviewed parent for activity-log recovery')
    p.add_argument('--activity-creation-log-id', help='Exact creation event linking the old subitem to its parent')
    p.add_argument('--preserve-item-id', help='Reviewed surviving child ID required by linked creation/deletion recovery')
    p = subs.add_parser('queue')
    p.add_argument('--run-dir', type=Path, required=True)
    p.add_argument('--confirm-run-id', required=True)
    p = subs.add_parser('worker')
    p.add_argument('--max-jobs', type=int, default=10)
    p.add_argument('--loop', action='store_true')
    p = subs.add_parser('status')
    p.add_argument('--run-id')
    p.add_argument('--item-id')
    p = subs.add_parser('requeue')
    p.add_argument('--event-key', required=True)
    p = subs.add_parser('reconcile')
    p.add_argument('--board', choices=list(life.BOARDS), required=True)
    p.add_argument('--item-id', required=True)
    args = parser.parse_args(argv)
    if args.command == 'worker' and args.loop:
        # Reconnect each pass and use the same observation/recovery loop as the
        # applications. Explicit CLI invocation does not need the ingress flag.
        async def serve():
            from src.services.worker_monitor import monitor
            monitor.start('lifecycle-cli')
            monitor.register('lifecycle', budget=1200)
            worker = life.LifecycleWorker()
            worker.task = asyncio.create_task(worker.run())
            monitor.bind('lifecycle', worker.task)
            try:
                await asyncio.shield(worker.task)
            finally:
                await worker.stop()
                await monitor.stop()
        asyncio.run(serve())
        return
    with life.connect() as connection:
        if args.command == 'stage':
            from src.services.monday_lifecycle_activity import LifecycleMondayClient
            monday = LifecycleMondayClient()
            try:
                result = stage(connection, monday, args)
            finally:
                monday.session.close()
        elif args.command == 'queue':
            result = queue_run(connection, args)
        elif args.command == 'worker':
            if not 1 <= args.max_jobs <= 100:
                raise ValueError('--max-jobs must be between 1 and 100')
            processed = 0
            while args.loop or processed < args.max_jobs:
                if life.run_once(connection):
                    processed += 1
                elif args.loop:
                    time.sleep(5)
                else:
                    break
            result = {'jobs_attempted': processed}
        elif args.command == 'requeue':
            row = connection.execute("UPDATE public.monday_lifecycle_events SET status='pending',attempts=0,"
                "next_attempt_at=now(),lease_token=NULL,lease_until=NULL,last_error=NULL,processed_at=NULL "
                "WHERE event_key=%s AND status IN ('retry','review') RETURNING event_key", (args.event_key,)).fetchone()
            if not row:
                raise ValueError('Only retry/review events can be requeued')
            result = row
        elif args.command == 'reconcile':
            result = {'event_key': life.enqueue(connection, 'reconcile', args.board, args.item_id)}
        else:
            query, params = 'SELECT event_key,kind,item_id,status,attempts,last_error,result FROM public.monday_lifecycle_events', []
            if args.run_id:
                query += ' WHERE event_key LIKE %s'
                params.append(f'recovery:{args.run_id}:%')
            elif args.item_id:
                query += ' WHERE item_id=%s'
                params.append(args.item_id)
            query += ' ORDER BY received_at DESC LIMIT 500'
            rows = connection.execute(query, params).fetchall()
            result = {'counts': dict(Counter(r['status'] for r in rows)), 'events': rows, 'limit': 500}
        print(json.dumps(result, indent=2, default=str))


if __name__ == '__main__':
    main()
