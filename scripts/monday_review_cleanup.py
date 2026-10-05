"""Stage, queue, process and verify the 31 explicitly reviewed subitem deletions.

Stage is read-only in Monday and PostgreSQL. Queue/process write PostgreSQL only,
using the existing audited lifecycle worker. The reviewed target file excludes
the two incomplete deletion events and five unconfirmed missing items.
"""
import argparse
from collections import Counter
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import time
from types import SimpleNamespace
from uuid import UUID, uuid4

from dotenv import load_dotenv
import requests

from scripts import monday_lifecycle as lifecycle_cli
from scripts import order_value_monday_compare as compare
from src.services import monday_lifecycle as life
from src.services import monday_lifecycle_activity as activity

TARGETS = Path(__file__).with_name('monday_review_cleanup_31_targets.json')
WORKFLOW = 'reviewed-31-subitem-cleanup-v1'


class CleanupMondayClient(activity.LifecycleMondayClient):
    def execute_query(self, query, variables=None):
        if query != activity.ACTIVITY_QUERY:
            return super().execute_query(query, variables)
        for attempt in range(3):
            try:
                return super().execute_query(query, variables)
            except requests.exceptions.SSLError:
                raise
            except (requests.ConnectionError, requests.Timeout):
                if attempt == 2:
                    raise
                time.sleep((2, 5)[attempt])
            except compare.MondayReadError as exc:
                if not exc.retryable or exc.retry_after > 30 or attempt == 2:
                    raise
                time.sleep(max((2, 5)[attempt], exc.retry_after))


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, default=str) + '\n', encoding='utf-8')


def file_hash(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def code_digest():
    return lifecycle_cli.digest({'lifecycle': lifecycle_cli.code_digest(),
        'script': file_hash(Path(__file__)), 'targets': file_hash(TARGETS)})


def targets():
    document = json.loads(TARGETS.read_text(encoding='utf-8'))
    rows = document['selected']
    if (document['version'] != 1 or len(rows) != 31
            or len({r['item_id'] for r in rows}) != 31
            or len({r['parent_id'] for r in rows}) != 27):
        raise ValueError('Expected the reviewed 31 distinct subitems across 27 parents')
    for row in rows:
        activity.validate_request(recovery_request(row))
    return rows


def recovery_request(row):
    # Start before the exact deletion, avoiding irrelevant years of parent edits.
    since = activity.utc_date(row['deleted_at_utc']) - timedelta(days=1)
    return dict(mode=activity.MODE, board_id=life.SUBITEM_BOARD_ID,
                item_id=row['item_id'], parent_id=row['parent_id'], log_id=row['log_id'],
                **{'from': since.isoformat()})


def child_args(run_dir, row):
    request = recovery_request(row)
    return SimpleNamespace(run_dir=run_dir / 'deletions' / row['item_id'],
        review_csv=None, board=life.SUBITEM_BOARD_ID, item_id=[row['item_id']],
        parent_id=row['parent_id'], activity_log_id=row['log_id'],
        activity_log_from=request['from'])


def capture_enquiry(monday, pid):
    return compare.capture_new_enquiry(monday, pid)


def enquiry_preview(connection, monday, pid):
    source = capture_enquiry(monday, pid)
    total = compare.project_new_enquiry_total(source, source['projects'][pid])
    with connection.transaction():
        connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
        before = life.read_rows(connection, 'projects', 'monday_id', [pid])
        contract = compare.reconcile.read_contract(connection)
    if len(before) != 1:
        raise life.ReviewRequired('Expected one stored parent for enquiry refresh')
    category = compare.require_stored_enquiry_category(source['projects'][pid], before[0])
    column = contract['projects'].get('new_enquiry_value')
    if not column or column['type'] != 'numeric' or column['generated'] != 'NEVER':
        raise life.ReviewRequired('New enquiry value is not a writable numeric column')
    old = before[0].get('new_enquiry_value')
    eligible = category == 'Open'
    normalized = (compare.reconcile.normalize_updates({'projects': [
        {'monday_id': pid, 'new_enquiry_value': total}]}, contract)['projects'][0]['new_enquiry_value']
        if eligible else old)
    return dict(project_id=pid, item_name=source['projects'][pid]['name'], source=source,
                before=old, after=normalized, contract=column,
                status_category=category, eligible=eligible,
                skipped_reason='' if eligible else 'Parent status_category is not Open; value preserved',
                changed=eligible and (old is None or Decimal(str(old)) != Decimal(normalized)),
                eligible_subitems=sum(c['state'] == 'active' for c in source['subitems'].values()) if eligible else 0,
                current_subitems=len(source['projects'][pid]['subitems']))


def check_prerequisites(connection):
    with connection.transaction():
        connection.execute('SET TRANSACTION READ ONLY')
        compare.scopes.schema_safety(connection, require_journal=False)
        for table in ('monday_lifecycle_events', 'monday_item_lifecycle', 'monday_lifecycle_audit'):
            if not connection.execute('SELECT to_regclass(%s) AS name', ('public.' + table,)).fetchone()['name']:
                raise ValueError('Install the existing monday_lifecycle.sql migration before staging')
        guards = connection.execute("SELECT c.relname FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid "
            "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' "
            "AND t.tgname='guard_monday_deleted_item' AND t.tgenabled IN ('O','A')").fetchall()
        if {r['relname'] for r in guards} != {'projects', 'hidden_items', 'subitems'}:
            raise ValueError('Enabled lifecycle stale-write guards are required on all three core tables')


def stage(connection, monday, run_dir):
    if run_dir.exists():
        raise ValueError('Use a new run directory; reviewed artifacts are never overwritten')
    selected = targets()
    check_prerequisites(connection)
    run_dir.mkdir(parents=True)
    index, previews, failures = [], [], []
    for number, row in enumerate(selected, 1):
        print(f'Staging deletion {number}/{len(selected)}: {row["item_name"]} / {row["item_id"]}', flush=True)
        args = child_args(run_dir, row)
        manifest = lifecycle_cli.stage(connection, monday, args)
        index.append(dict(target=row, run_id=manifest['run_id'], selected=manifest['selected'],
                          deferred=manifest['deferred']))
    for pid in sorted({r['parent_id'] for r in selected}):
        print(f'Previewing active-child enquiry SUM for Open parent: {pid}', flush=True)
        try:
            previews.append(enquiry_preview(connection, monday, pid))
        except (ValueError, ArithmeticError) as exc:
            failures.append(dict(project_id=pid, reason=str(exc)))
    plan = dict(deletions=index, enquiry_previews=previews, enquiry_failures=failures,
                enquiry_rule=compare.ENQUIRY_RULE,
                enquiry_rule_confirmed='User clarification: API lifecycle active and parent category Open, 2026-10-05')
    write_json(run_dir / 'plan.json', plan)
    rows = []
    for entry in index:
        row = entry['target']
        child_plan = json.loads((child_args(run_dir, row).run_dir / 'plan.json').read_text(encoding='utf-8'))
        preview = next((p for p in previews if p['project_id'] == row['parent_id']), {})
        rows.append(dict(project=row['item_name'], project_id=row['parent_id'],
            subitem=row['subitem_name'], subitem_id=row['item_id'], deletion_event_id=row['log_id'],
            deletion_ready=entry['selected'] == 1 and entry['deferred'] == 0,
            deletion_review=' | '.join(r['reason'] for r in child_plan['deferred']),
            enquiry_before=preview.get('before'), enquiry_after=preview.get('after'),
            status_category=preview.get('status_category'), enquiry_eligible=preview.get('eligible'),
            eligible_subitems=preview.get('eligible_subitems'), enquiry_skipped=preview.get('skipped_reason'),
            enquiry_review=' | '.join(r['reason'] for r in failures if r['project_id'] == row['parent_id'])))
    compare.reconcile.write_csv(run_dir / 'review.csv', rows, list(rows[0]))
    artifacts = {'plan.json': file_hash(run_dir / 'plan.json'), 'review.csv': file_hash(run_dir / 'review.csv')}
    for row in selected:
        for name in ('plan.json', 'manifest.json', 'review.csv'):
            relative = f'deletions/{row["item_id"]}/{name}'
            artifacts[relative] = file_hash(run_dir / relative)
    count = sum(r['selected'] for r in index)
    manifest = dict(workflow=WORKFLOW, run_id=str(uuid4()), prepared_at=datetime.now(timezone.utc).isoformat(),
        code=code_digest(), target=lifecycle_cli.target_digest(connection), artifacts=artifacts,
        selected=count, deferred=sum(r['deferred'] for r in index), projects=27,
        enquiry_ready=len(previews), enquiry_failures=len(failures),
        enquiry_eligible=sum(p['eligible'] for p in previews),
        enquiry_skipped=sum(not p['eligible'] for p in previews),
        enquiry_changes=sum(p['changed'] for p in previews), read_only=True,
        ready=count == 31 and not any(r['deferred'] for r in index) and len(previews) == 27 and not failures)
    write_json(run_dir / 'manifest.json', manifest)
    return manifest


def load_run(run_dir, *, check_code=True):
    manifest = json.loads((run_dir / 'manifest.json').read_text(encoding='utf-8'))
    plan = json.loads((run_dir / 'plan.json').read_text(encoding='utf-8'))
    UUID(manifest['run_id'])
    expected = {'plan.json', 'review.csv'} | {
        f'deletions/{r["item_id"]}/{name}' for r in targets()
        for name in ('plan.json', 'manifest.json', 'review.csv')}
    if (manifest['workflow'] != WORKFLOW or set(manifest['artifacts']) != expected
            or (check_code and manifest['code'] != code_digest())
            or any(file_hash(run_dir / n) != h for n, h in manifest['artifacts'].items())
            or [r['target'] for r in plan['deletions']] != targets()):
        raise ValueError('Code, selection or reviewed artifacts changed; stage a new run')
    for entry in plan['deletions']:
        UUID(entry['run_id'])
    return manifest, plan


def prefixes(plan):
    return [f'recovery:{entry["run_id"]}:' for entry in plan['deletions']]


def status(connection, run_dir):
    manifest, plan = load_run(run_dir, check_code=False)
    if lifecycle_cli.target_digest(connection) != manifest['target']:
        raise ValueError('Database target differs from the staged target')
    with connection.transaction():
        connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
        events = connection.execute('SELECT event_key,kind,item_id,status,attempts,last_error,result '
            'FROM public.monday_lifecycle_events WHERE event_key LIKE ANY(%s) ORDER BY received_at,event_key',
            ([p + '%' for p in prefixes(plan)],)).fetchall()
        remaining = life.read_rows(connection, 'subitems', 'monday_id', [r['item_id'] for r in targets()])
        parents = life.read_rows(connection, 'projects', 'monday_id', sorted({r['parent_id'] for r in targets()}))
    roots = {f'recovery:{r["run_id"]}:{life.SUBITEM_BOARD_ID}:{r["target"]["item_id"]}' for r in plan['deletions']}
    successful_roots = {r['event_key'] for r in events if r['event_key'] in roots
                        and r['status'] == 'processed' and (r['result'] or {}).get('deleted') == r['item_id']}
    verified = {r['item_id'] for r in events if r['kind'] == 'reconcile' and r['status'] == 'processed'
                and (r['result'] or {}).get('deletion_verified') == r['item_id']}
    parent_ids = {r['parent_id'] for r in targets()}
    refreshed = {r['item_id'] for r in events if r['kind'] == 'refresh' and r['status'] == 'processed'
                 and not (r['result'] or {}).get('issues')}
    skipped = {r['item_id'] for r in events if r['kind'] == 'refresh' and r['status'] == 'processed'
               and (r['result'] or {}).get('eligible') is False}
    all_processed = bool(events) and all(r['status'] == 'processed' for r in events)
    complete = (not remaining and len(successful_roots) == 31 and len(verified) == 31
                and parent_ids <= refreshed and all_processed)
    result = dict(run_id=manifest['run_id'], complete=complete, event_counts=dict(Counter(r['status'] for r in events)),
        deletions_processed=len(successful_roots), deletions_verified=len(verified),
        parent_refreshes_completed=len(parent_ids & refreshed),
        parent_refreshes_skipped_nonopen=len(parent_ids & skipped),
        remaining_subitem_ids=[r['monday_id'] for r in remaining], events=events,
        current_enquiry_values=[{'project_id':r['monday_id'], 'new_enquiry_value':r.get('new_enquiry_value')}
                                for r in parents])
    write_json(run_dir / 'status.json', result)
    return result


def queue(connection, monday, run_dir, confirm_run_id):
    manifest, plan = load_run(run_dir)
    if confirm_run_id != manifest['run_id']:
        raise ValueError('Queue requires --confirm-run-id from the reviewed manifest')
    if not manifest['ready'] or manifest['selected'] != 31 or plan['enquiry_failures']:
        raise ValueError('All 31 deletions and 27 enquiry previews must be ready; resolve deferrals and restage')
    if lifecycle_cli.target_digest(connection) != manifest['target']:
        raise ValueError('Database target differs from the staged target')
    existing = connection.execute('SELECT event_key FROM public.monday_lifecycle_events '
        'WHERE event_key LIKE ANY(%s)', ([p + '%' for p in prefixes(plan)],)).fetchall()
    if existing:
        # Never queue a second copy or overwrite a completed job on resume.
        expected_roots = {f'recovery:{r["run_id"]}:{life.SUBITEM_BOARD_ID}:{r["target"]["item_id"]}'
                          for r in plan['deletions']}
        if not expected_roots <= {r['event_key'] for r in existing}:
            raise ValueError('Partial or inconsistent existing queue; inspect status before continuing')
        return dict(already_queued=31, note='Use process/status to resume this same run')
    # No HTTP under database locks. Recheck all 27 reviewed totals before queueing.
    for reviewed in plan['enquiry_previews']:
        fresh = enquiry_preview(connection, monday, reviewed['project_id'])
        if fresh != reviewed:
            raise ValueError(f'Enquiry source, schema or stored value changed for {reviewed["project_id"]}; restage')
    keys = []
    # Either every root job is queued or none is; workers revalidate each deletion.
    with connection.transaction():
        for entry in plan['deletions']:
            args = child_args(run_dir, entry['target'])
            args.confirm_run_id = entry['run_id']
            keys.extend(lifecycle_cli.queue_run(connection, args)['event_keys'])
        # Same transaction: an enabled worker cannot see an unscoped refresh job.
        connection.execute("UPDATE public.monday_lifecycle_events SET payload=payload || "
            "'{\"refresh_mode\":\"new_enquiry_sum\"}'::jsonb WHERE event_key=ANY(%s)", (keys,))
    result = dict(queued=len(keys), event_keys=keys, note='Queued only; process and verify before reporting completion')
    write_json(run_dir / 'queue.json', result)
    return result


def process(connection, run_dir, max_jobs):
    manifest, plan = load_run(run_dir)
    if lifecycle_cli.target_digest(connection) != manifest['target']:
        raise ValueError('Database target differs from the staged target')
    if not 1 <= max_jobs <= 1000:
        raise ValueError('--max-jobs must be between 1 and 1000')
    for _ in range(max_jobs):
        if not life.run_once(connection, event_prefixes=prefixes(plan)):
            break
    return status(connection, run_dir)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    subs = parser.add_subparsers(dest='command', required=True)
    for name in ('stage', 'queue', 'process', 'status'):
        sub = subs.add_parser(name)
        sub.add_argument('--run-dir', type=Path, required=True)
        if name == 'queue':
            sub.add_argument('--confirm-run-id', required=True)
        if name == 'process':
            sub.add_argument('--max-jobs', type=int, default=500)
    args = parser.parse_args(argv)
    load_dotenv()
    with life.connect() as connection:
        if args.command in ('stage', 'queue'):
            monday = CleanupMondayClient()
            try:
                result = (stage(connection, monday, args.run_dir) if args.command == 'stage'
                          else queue(connection, monday, args.run_dir, args.confirm_run_id))
            finally:
                monday.session.close()
        elif args.command == 'process':
            result = process(connection, args.run_dir, args.max_jobs)
        else:
            result = status(connection, args.run_dir)
    print(json.dumps({k: v for k, v in result.items() if k not in ('artifacts', 'events', 'current_enquiry_values')},
                     indent=2, default=str))
    if args.command == 'stage':
        return 0 if result['ready'] else 2
    if args.command in ('process', 'status'):
        return 0 if result['complete'] else 2
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except Exception as exc:
        # Transport errors can include credential-bearing DSNs; never print them.
        print(json.dumps({'error_type': type(exc).__name__,
                          'message': str(exc) if isinstance(exc, ValueError) else 'Operation failed; no success claimed'}))
        raise SystemExit(1)
