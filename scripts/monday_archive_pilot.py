"""Stage and apply the pinned, exact-ID archive coverage pilot; Monday is read-only."""
import argparse
import csv
from datetime import datetime, timedelta, timezone
import hashlib
import json
import logging
import os
from pathlib import Path
import re
from uuid import UUID, uuid4

from dotenv import load_dotenv
import psycopg
from psycopg.types.json import Jsonb
import requests

from scripts import monday_lifecycle as cli
from scripts import order_value_monday_compare as compare
from scripts import order_value_scopes as scopes
from scripts import reconcile_order_values as reconcile
from src.services import monday_archive as archive
from src.services import monday_lifecycle as life
from src.services import monday_lifecycle_refresh as refresh

LOG = logging.getLogger(__name__)
POLICY = 'archive_coverage_pilot_v1'
TARGETS = Path(__file__).with_name('monday_archive_pilot_targets.json')
MAX_STAGE_AGE = timedelta(hours=24)
MAX_SOURCE_AGE = timedelta(minutes=5)


def now():
    return datetime.now(timezone.utc)


def code_digest():
    return cli.digest({'shared': cli.code_digest(),
                       'runner': hashlib.sha256(Path(__file__).read_bytes()).hexdigest()})


def load_targets():
    targets = json.loads(TARGETS.read_text(encoding='utf-8'))
    projects = targets['projects']
    if targets['version'] != 1 or not 1 <= len(projects) <= 10:
        raise ValueError('Invalid bounded pilot approval')
    seen = set()
    for target in projects:
        ids = [target['monday_id'], *target['subitems'], *target['subitems'].values()]
        if (len(ids) != len(set(ids)) or seen.intersection(ids)
                or len(ids) > 25 or any(not re.fullmatch(r'[0-9]+', i) for i in ids)):
            raise ValueError('Pilot requires distinct exact IDs and isolated source ownership')
        seen.update(ids)
    return targets


def require_environment(connection):
    if not archive.enabled():
        raise ValueError('Enable MONDAY_ARCHIVE_ENABLED on all replacement writers first')
    if os.getenv('MONDAY_ARCHIVE_REPORTING_ENABLED', 'false').lower() in {'true', '1', 'yes'}:
        raise ValueError('Keep MONDAY_ARCHIVE_REPORTING_ENABLED=false during the pilot')
    archive.require_runtime(connection)
    scopes.schema_safety(connection, require_journal=False)


def boundary(target):
    return {'projects': [target['monday_id']], 'subitems': sorted(target['subitems']),
            'hidden_items': sorted(set(target['subitems'].values()))}


def require_real_project(*names):
    for name in names:
        text = str(name or '').strip().lower()
        if text == 'new project' or re.fullmatch(r'free+(?:\s+(?:number|to\s+use))?', text):
            raise life.ReviewRequired('New project/FREE records are excluded from this pilot')


def require_source(target, source):
    pid, members = target['monday_id'], target['subitems']
    for table, ids in boundary(target).items():
        if set(source[table]) != set(ids):
            raise life.ReviewRequired(f'{pid}: Monday {table} exceeds or differs from approved IDs')
        for ident in ids:
            life.require_item(source[table], ident, table, 'active')
    parent = source['projects'][pid]
    if parent.get('parent_item') is not None:
        raise life.ReviewRequired(f'{pid}: selected project is now a subitem')
    if (len(parent['subitems']) != len(members)
            or {r['id'] for r in parent['subitems']} != set(members)):
        raise life.ReviewRequired(f'{pid}: Monday membership differs from approval')
    require_real_project(parent['name'], compare.scalar(
        compare.col(parent, compare.PARENT_COLUMNS['project_name']), 'text'))
    for child in parent['subitems']:
        cid = child['id']
        if ((child.get('parent_item') or {}).get('id') != pid
                or (source['subitems'][cid].get('parent_item') or {}).get('id') != pid
                or child.get('state') not in (None, 'active')
                or compare.links(source['subitems'][cid]) != [members[cid]]):
            raise life.ReviewRequired(f'{cid}: current parent/source link differs from approval')


def checked_snapshot(connection, target, source):
    pid = target['monday_id']
    before = refresh.read_snapshot(connection, pid, source)
    for table, ids in boundary(target).items():
        if {r['monday_id'] for r in before[table]} != set(ids):
            raise life.ReviewRequired(f'{pid}: stored {table} missing or has out-of-scope owners')
    for child in before['subitems']:
        if (child['parent_monday_id'] != pid
                or child['hidden_item_id'] != target['subitems'][child['monday_id']]):
            raise life.ReviewRequired(f'{pid}: stored parent/source link differs from approval')
    parent = before['projects'][0]
    require_real_project(parent.get('item_name'), parent.get('project_name'))
    if not connection.execute('SELECT 1 FROM public.reportable_projects WHERE monday_id=%s', (pid,)).fetchone():
        raise life.ReviewRequired(f'{pid}: project is excluded from reporting')
    states = archive.read_states(connection, boundary(target))
    for rows in states.values():
        for row in rows.values():
            if row['blocked'] or row['monday_state'] not in (None, 'active'):
                raise life.ReviewRequired(f'{pid}: lifecycle restoration requires separate review')
    return before, states


def read_case(connection, monday, target):
    source = refresh.fetch_project(monday, target['monday_id'])
    require_source(target, source)
    with connection.transaction():
        connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
        before, states = checked_snapshot(connection, target, source)
        contract = reconcile.read_contract(connection)
    values, issues = refresh.build_values(target['monday_id'], source, before, contract, lifecycle=states)
    if issues:
        raise life.ReviewRequired(f"{target['monday_id']}: source projection requires review: {issues}")
    for table, rows in values.items():
        if not {r['monday_id'] for r in rows}.issubset(boundary(target)[table]):
            raise life.ReviewRequired('Projected write exceeds approved IDs')
    second = refresh.fetch_project(monday, target['monday_id'])
    if cli.digest(second) != cli.digest(source):
        raise life.ReviewRequired('Monday changed during capture; restage with fresh evidence')
    return dict(target=target, source=source, before=before, states=states,
                contract=contract, values=values, captured_at=now().isoformat())


def require_fresh(timestamp, age, label):
    elapsed = now() - datetime.fromisoformat(timestamp)
    if not timedelta(0) <= elapsed <= age:
        raise life.ReviewRequired(f'{label} is expired or future-dated; capture fresh evidence')


def require_same(left, right, label):
    if cli.digest(left) != cli.digest(right):
        raise life.ReviewRequired(f'{label} changed; do not apply this stale plan')


def write_json(path, value):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, indent=2, default=str), encoding='utf-8')
    temporary.replace(path)


def stage(connection, monday, run_dir):
    require_environment(connection)
    if run_dir.exists():
        raise ValueError('Use a new run directory')
    targets = load_targets()
    cases = [read_case(connection, monday, t) for t in targets['projects']]
    plan = dict(targets=targets, cases=cases)
    run_dir.mkdir(parents=True)
    write_json(run_dir / 'plan.json', plan)
    with (run_dir / 'review.csv').open('w', encoding='utf-8-sig', newline='') as stream:
        writer = csv.writer(stream)
        writer.writerow(['project_id', 'table', 'monday_id', 'field', 'before', 'after'])
        for case in cases:
            for table, rows in case['values'].items():
                old = {r['monday_id']: r for r in case['before'][table]}
                for row in rows:
                    for field, value in row.items():
                        if field != 'monday_id':
                            writer.writerow([case['target']['monday_id'], table, row['monday_id'], field,
                                             json.dumps(old[row['monday_id']].get(field), default=str),
                                             json.dumps(value, default=str)])
    manifest = dict(policy=POLICY, run_id=str(uuid4()), prepared_at=now().isoformat(),
                    target=cli.target_digest(connection), code=code_digest(), sha256=cli.digest(plan),
                    review_sha256=hashlib.sha256((run_dir / 'review.csv').read_bytes()).hexdigest(),
                    projects=len(cases), subitems=sum(len(t['subitems']) for t in targets['projects']),
                    hidden_sources=sum(len(set(t['subitems'].values())) for t in targets['projects']),
                    business_rows_to_update=sum(len(rows) for case in cases for rows in case['values'].values()),
                    read_only=True)
    write_json(run_dir / 'manifest.json', manifest)
    return manifest


def load_run(connection, run_dir, confirmation, *, applying):
    require_environment(connection)
    manifest = json.loads((run_dir / 'manifest.json').read_text(encoding='utf-8'))
    plan = json.loads((run_dir / 'plan.json').read_text(encoding='utf-8'))
    if confirmation != manifest['run_id'] or str(UUID(confirmation)) != confirmation:
        raise ValueError('Explicit --confirm-run-id must match the staged run')
    if manifest['policy'] != POLICY:
        raise ValueError('Unsupported pilot policy')
    require_same(cli.digest(plan), manifest['sha256'], 'Plan checksum')
    require_same(hashlib.sha256((run_dir / 'review.csv').read_bytes()).hexdigest(),
                 manifest['review_sha256'], 'Review checksum')
    require_same(load_targets(), plan['targets'], 'Pinned approval')
    require_same([c['target'] for c in plan['cases']], plan['targets']['projects'], 'Plan scope')
    require_same(code_digest(), manifest['code'], 'Deployed code')
    require_same(cli.target_digest(connection), manifest['target'], 'Database target')
    if applying:
        require_fresh(manifest['prepared_at'], MAX_STAGE_AGE, 'Stage')
    return manifest, plan


def event_key(manifest, target):
    return f"archive-pilot:{manifest['run_id']}:{target['monday_id']}"


def receipt(connection, manifest, target, *, locked=False):
    row = connection.execute('SELECT * FROM public.monday_lifecycle_events WHERE event_key=%s'
                             + (' FOR UPDATE' if locked else ''),
                             (event_key(manifest, target),)).fetchone()
    if row:
        expected = dict(operator_policy=POLICY, archive_policy=archive.POLICY,
                        run_id=manifest['run_id'], plan_sha256=manifest['sha256'])
        require_same(row['payload'], expected, 'Operator receipt identity')
        if (row['status'] not in {'review', 'processed'} or row['kind'] != 'refresh'
                or row['board_id'] != life.PARENT_BOARD_ID or row['item_id'] != target['monday_id']
                or row['result']['phase'] not in {'prepared', 'applied_pending_verification', 'verified'}
                or (row['status'] == 'processed') != (row['result']['phase'] == 'verified')):
            raise life.ReviewRequired('Invalid operator receipt; never requeue it to a worker')
    return row


def prepare_receipt(connection, manifest, target):
    payload = dict(operator_policy=POLICY, archive_policy=archive.POLICY,
                   run_id=manifest['run_id'], plan_sha256=manifest['sha256'])
    # Only review/processed states are ever committed: even legacy workers cannot claim this.
    connection.execute('INSERT INTO public.monday_lifecycle_events '
        '(event_key,board_id,item_id,kind,payload,status,result,last_error) '
        "VALUES (%s,%s,%s,'refresh',%s,'review',%s,'Operator pilot requires completion') "
        'ON CONFLICT(event_key) DO NOTHING',
        (event_key(manifest, target), life.PARENT_BOARD_ID, target['monday_id'], Jsonb(payload),
         Jsonb({'phase': 'prepared'})))


def check_locked(connection, case):
    require_fresh(case['captured_at'], MAX_SOURCE_AGE, 'Monday capture')
    scopes.schema_safety(connection, require_journal=False)
    before, states = checked_snapshot(connection, case['target'], case['source'])
    require_same(before, case['before'], 'Stored rows/owners')
    require_same(states, case['states'], 'Lifecycle evidence')
    require_same(reconcile.read_contract(connection), case['contract'], 'Schema contract')


def apply_case(connection, manifest, staged, fresh):
    target = staged['target']
    for name in ('source', 'before', 'states', 'contract', 'values'):
        require_same(fresh[name], staged[name], name)
    prepare_receipt(connection, manifest, target)
    with life.locked_write_transaction(connection):
        connection.execute('LOCK TABLE public.monday_item_lifecycle IN SHARE ROW EXCLUSIVE MODE')
        job = receipt(connection, manifest, target, locked=True)
        if job['result']['phase'] != 'prepared':
            raise life.ReviewRequired('Another operator advanced this project; resume without replaying writes')
        check_locked(connection, fresh)
        archive.observe_source(connection, job, fresh['source'], datetime.fromisoformat(fresh['captured_at']))
        refresh.write_values(connection, fresh['values'], fresh['before'], fresh['contract'], job=job)
        after, states = checked_snapshot(connection, target, fresh['source'])
        life.audit(connection, job, 'pilot_apply', 'projects', target['monday_id'], fresh['before'],
                   {'after': after, 'plan_sha256': manifest['sha256']})
        connection.execute("UPDATE public.monday_lifecycle_events SET result=%s,last_error=%s WHERE event_key=%s",
            (Jsonb(dict(phase='applied_pending_verification', after=after, states=states)),
             'Fresh post-write verification required; resume with the pilot CLI', job['event_key']))


def verify_case(connection, monday, manifest, staged):
    target = staged['target']
    job = receipt(connection, manifest, target)
    if not job or job['result']['phase'] == 'prepared':
        raise life.ReviewRequired(f"{target['monday_id']}: project has not been applied")
    fresh = read_case(connection, monday, target)
    require_same(fresh['source'], staged['source'], 'Post-write Monday source')
    require_same(fresh['contract'], staged['contract'], 'Post-write schema')
    require_same(fresh['before'], job['result']['after'], 'Post-write stored rows')
    require_same(fresh['states'], job['result']['states'], 'Post-write lifecycle evidence')
    if any(fresh['values'].values()):
        raise life.ReviewRequired('Fresh source projection still differs from SQL; values are not verified')
    with life.locked_write_transaction(connection):
        connection.execute('LOCK TABLE public.monday_item_lifecycle IN SHARE ROW EXCLUSIVE MODE')
        current = receipt(connection, manifest, target, locked=True)
        require_same(current, job, 'Operator receipt')
        check_locked(connection, fresh)
        if job['result']['phase'] == 'verified':
            return
        archive.verify_parent_values(connection, job, target['monday_id'])
        result = {**job['result'], 'phase': 'verified', 'verified_at': now().isoformat(),
                  'states': archive.read_states(connection, boundary(target))}
        connection.execute("UPDATE public.monday_lifecycle_events SET status='processed',result=%s,"
                           'last_error=NULL,processed_at=now() WHERE event_key=%s',
                           (Jsonb(result), job['event_key']))


def run(connection, monday, run_dir, confirmation, *, applying):
    manifest, plan = load_run(connection, run_dir, confirmation, applying=applying)
    write_json(run_dir / 'result.json', dict(run_id=manifest['run_id'], complete=False,
               verified_projects=[], expected_projects=len(plan['cases'])))
    # Check the entire selected population before the first new business write.
    if applying:
        for staged in plan['cases']:
            job = receipt(connection, manifest, staged['target'])
            if not job or job['result']['phase'] == 'prepared':
                fresh = read_case(connection, monday, staged['target'])
                for name in ('source', 'before', 'states', 'contract', 'values'):
                    require_same(fresh[name], staged[name], name)
    verified = []
    for staged in plan['cases']:
        target = staged['target']
        job = receipt(connection, manifest, target)
        if applying and (not job or job['result']['phase'] == 'prepared'):
            apply_case(connection, manifest, staged, read_case(connection, monday, target))
        verify_case(connection, monday, manifest, staged)
        verified.append(target['monday_id'])
        result = dict(run_id=manifest['run_id'], complete=len(verified) == len(plan['cases']),
                      verified_projects=verified, expected_projects=len(plan['cases']))
        write_json(run_dir / 'result.json', result)
        LOG.info('Verified pilot project %s (%s/%s)', target['monday_id'], len(verified), len(plan['cases']))
    return result


def main(argv=None):
    load_dotenv()
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description=__doc__)
    subs = parser.add_subparsers(dest='command', required=True)
    for command in ('stage', 'apply', 'verify'):
        sub = subs.add_parser(command)
        sub.add_argument('--run-dir', type=Path, required=True)
        if command != 'stage':
            sub.add_argument('--confirm-run-id', required=True)
    args = parser.parse_args(argv)
    try:
        with life.connect() as connection:
            if args.command == 'stage':
                connection.execute('SET default_transaction_read_only=on')
            monday = compare.ComparisonMondayClient()
            try:
                result = (stage(connection, monday, args.run_dir) if args.command == 'stage' else
                          run(connection, monday, args.run_dir, args.confirm_run_id,
                              applying=args.command == 'apply'))
            finally:
                monday.session.close()
        print(json.dumps(result, indent=2, default=str))
        return 0
    except (ValueError, RuntimeError) as exc:
        LOG.error('Pilot stopped: %s. Retain the run directory and inspect operator receipts before resuming.', exc)
        return 1
    except (psycopg.Error, requests.RequestException) as exc:
        LOG.error('Pilot stopped (%s). Check connectivity and retained receipts; do not assume rollback '
                  'if the connection was lost during commit.', type(exc).__name__)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
