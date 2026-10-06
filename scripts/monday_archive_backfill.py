"""Once-approved, resumable exact-ID archive coverage batches. Never writes Monday."""
import argparse
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime
import hashlib
import json
import logging
from pathlib import Path
from uuid import UUID, uuid4, uuid5

from dotenv import load_dotenv
import psycopg
from psycopg.types.json import Jsonb
import requests

from scripts import monday_archive_pilot as pilot
from scripts import monday_lifecycle as cli
from scripts import order_value_monday_compare as compare
from scripts import order_value_scopes as scopes
from scripts import reconcile_order_values as reconcile
from src.services import monday_archive as archive
from src.services import monday_lifecycle as life
from src.services import monday_lifecycle_refresh as refresh

LOG = logging.getLogger(__name__)
POLICY = 'archive_coverage_backfill_v1'
SOURCE_MANIFEST_SHA256 = '179f432846af543afa73ee5dea2baf6f936428fe040734e9fce3114e0a5e56d4'
APPROVAL_SHA256 = '9c2be5ccd442a0ab7071d68175de18ad2753d5696a5ddfd5bfe43cc9235e259a'
TARGETS = Path(__file__).with_name('monday_archive_backfill_targets.json')
MAX_BATCH_PROJECTS = 25


def export_approval(source_dir):
    """Reproduce the numeric-only deployment input from the immutable reviewed inventory."""
    raw = (source_dir / 'manifest.json').read_bytes()
    if hashlib.sha256(raw).hexdigest() != SOURCE_MANIFEST_SHA256:
        raise ValueError('Not the reviewed initial approval manifest')
    manifest = json.loads(raw)
    def checked(name):
        content = (source_dir / name).read_bytes()
        if hashlib.sha256(content).hexdigest() != manifest['artifact_hashes'][name]:
            raise ValueError(f'Approval artifact checksum differs: {name}')
        return content
    ids = checked('approved_project_ids.txt').decode().split()
    dependencies = json.loads(checked('approved_dependencies.json'))
    database = json.loads(checked('database_after.json'))
    owners = {r['monday_id']: r for r in json.loads(checked('monday_ownership.json'))['items']}
    projects = {pid: {} for pid in ids}
    for row in database['subitems']:
        pid, cid, hid = row['parent_monday_id'], row['monday_id'], row['hidden_item_id']
        if pid not in projects:
            continue
        source = owners[cid]
        if (source['parent_monday_id'] != pid or source['hidden_ids'] != [hid]
                or source['state'] != 'active' or source['parent_state'] != 'active'):
            raise ValueError(f'Approval ownership evidence conflicts for {cid}')
        projects[pid][cid] = hid
    for pid, children in projects.items():
        if (set(children) != set(dependencies[pid]['subitem_ids'])
                or set(children.values()) != set(dependencies[pid]['hidden_item_ids'])):
            raise ValueError(f'Approved dependency set differs for {pid}')
    result = dict(version=1, approval_manifest_sha256=SOURCE_MANIFEST_SHA256, projects=projects)
    if (len(ids) != len(projects) or len(projects) != 15153
            or sum(map(len, projects.values())) != 37408
            or len({h for children in projects.values() for h in children.values()}) != 37391):
        raise ValueError('Reviewed approval counts differ')
    if TARGETS.exists():
        raise ValueError('Refusing to overwrite the exported approval')
    # One project per line keeps this generated, numeric-only input reviewable.
    lines = [f'  {json.dumps(pid)}: {json.dumps(children, sort_keys=True)}'
             for pid, children in sorted(projects.items())]
    TARGETS.write_text('{"version": 1, "approval_manifest_sha256": '
                      + json.dumps(SOURCE_MANIFEST_SHA256) + ', "projects": {\n'
                      + ',\n'.join(lines) + '\n}}\n', encoding='utf-8', newline='\n')
    return dict(approval_sha256=cli.digest(result), projects=len(projects))


def boundary(group):
    return dict(projects=sorted(group), subitems=sorted(c for children in group.values() for c in children),
                hidden_items=sorted({h for children in group.values() for h in children.values()}))


def components(projects):
    owners = defaultdict(set)
    for pid, children in projects.items():
        for hid in children.values():
            owners[hid].add(pid)
    seen, result = set(), []
    for pid in sorted(projects):
        if pid in seen:
            continue
        pending, members = {pid}, set()
        while pending:
            current = pending.pop()
            if current in members:
                continue
            members.add(current)
            for hid in projects[current].values():
                pending.update(owners[hid] - members)
        seen.update(members)
        group = {p: projects[p] for p in sorted(members)}
        if sum(map(len, boundary(group).values())) > life.MAX_ROWS:
            raise life.ReviewRequired('An approved shared-source group exceeds 500 rows')
        result.append(group)
    return result


def load_approval():
    data = json.loads(TARGETS.read_text(encoding='utf-8'))
    if (cli.digest(data) != APPROVAL_SHA256 or data['version'] != 1
            or data['approval_manifest_sha256'] != SOURCE_MANIFEST_SHA256):
        raise ValueError('Pinned initial approval checksum differs')
    projects = data['projects']
    ids = boundary(projects)
    if (not projects or len(ids['subitems']) != len(set(ids['subitems']))
            or sum(map(len, ids.values())) != len(set().union(*map(set, ids.values())))
            or any(not isinstance(i, str) or not i.isascii() or not i.isdecimal()
                   for values in ids.values() for i in values)):
        raise ValueError('Invalid exact project/dependency identities')
    return projects


def campaign_manifest(connection, pilot_run_id, projects):
    if str(UUID(pilot_run_id)) != pilot_run_id:
        raise ValueError('Use the canonical completed pilot UUID')
    selected = pilot.load_targets()['projects']
    plan_hashes = set()
    for target in selected:
        pid = target['monday_id']
        if projects.get(pid) != target['subitems']:
            raise ValueError('Pilot scope differs from full approval')
        key = f'archive-pilot:{pilot_run_id}:{pid}'
        row = connection.execute('SELECT * FROM public.monday_lifecycle_events WHERE event_key=%s', (key,)).fetchone()
        if (not row or row['status'] != 'processed' or row['kind'] != 'refresh'
                or row['board_id'] != life.PARENT_BOARD_ID or row['item_id'] != pid
                or row['payload'].get('operator_policy') != pilot.POLICY
                or row['payload'].get('archive_policy') != archive.POLICY
                or row['payload'].get('run_id') != pilot_run_id
                or (row['result'] or {}).get('phase') != 'verified'
                or not row['payload'].get('plan_sha256')):
            raise life.ReviewRequired(f'Completed verified pilot receipt is missing for {pid}')
        plan_hashes.add(row['payload']['plan_sha256'])
    if len(plan_hashes) != 1:
        raise ValueError('Pilot receipts refer to different plans')
    excluded = {t['monday_id'] for t in selected}
    groups = components(projects)
    if any(excluded.intersection(g) and not set(g) <= excluded for g in groups):
        raise ValueError('Cannot split a pilot/shared-source group')
    groups = [g for g in groups if not set(g) <= excluded]
    remaining = {pid: children for g in groups for pid, children in g.items()}
    identity = dict(policy=POLICY, run_id=str(uuid5(UUID(pilot_run_id), APPROVAL_SHA256)),
        pilot_run_id=pilot_run_id, pilot_plan_sha256=next(iter(plan_hashes)),
        approval_sha256=APPROVAL_SHA256, target=cli.target_digest(connection),
        code=cli.digest({'pilot': pilot.code_digest(),
                         'runner': hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}),
        expected_projects=len(remaining), expected_groups=len(groups),
        expected_subitems=len(boundary(remaining)['subitems']),
        expected_hidden_sources=len(boundary(remaining)['hidden_items']),
        excluded_pilot_projects=sorted(excluded),
        approval_mode='once_for_fresh_source_values_and_metadata_not_just_lifecycle_flags')
    return identity, groups


def prepare(connection, run_dir, pilot_run_id):
    pilot.require_environment(connection)
    manifest, _ = campaign_manifest(connection, pilot_run_id, load_approval())
    if run_dir.exists():
        raise ValueError('Use a new directory, or resume the existing campaign')
    run_dir.mkdir(parents=True)
    pilot.write_json(run_dir / 'manifest.json', manifest)
    return manifest


def load_campaign(connection, run_dir, confirmation=None):
    pilot.require_environment(connection)
    manifest = json.loads((run_dir / 'manifest.json').read_text(encoding='utf-8'))
    expected, groups = campaign_manifest(connection, manifest['pilot_run_id'], load_approval())
    pilot.require_same(manifest, expected, 'Campaign approval/code/database/pilot')
    if confirmation is not None and confirmation != manifest['run_id']:
        raise ValueError('--confirm-run-id must match the prepared campaign')
    return manifest, groups


def event_key(manifest, group):
    return f"archive-backfill:{manifest['run_id']}:{min(group)}"


def payload(manifest, group):
    return dict(operator_policy=POLICY, archive_policy=archive.POLICY,
                run_id=manifest['run_id'], campaign_sha256=cli.digest(manifest), scope_sha256=cli.digest(group))


def check_receipt(row, manifest, group):
    if row is None:
        return
    pilot.require_same(row['payload'], payload(manifest, group), 'Backfill receipt identity')
    phase = (row['result'] or {}).get('phase')
    if (row['event_key'] != event_key(manifest, group) or row['item_id'] != min(group)
            or row['board_id'] != life.PARENT_BOARD_ID or row['kind'] != 'refresh'
            or row['status'] not in {'review', 'processed'}
            or phase not in {'prepared', 'applied_pending_verification', 'verified'}
            or (row['status'] == 'processed') != (phase == 'verified')):
        raise life.ReviewRequired('Invalid backfill receipt; do not requeue it to a worker')


def receipt(connection, manifest, group, *, locked=False):
    row = connection.execute('SELECT * FROM public.monday_lifecycle_events WHERE event_key=%s'
        + (' FOR UPDATE' if locked else ''), (event_key(manifest, group),)).fetchone()
    check_receipt(row, manifest, group)
    return row


def receipts(connection, manifest, groups):
    wanted = {event_key(manifest, g): g for g in groups}
    rows = connection.execute('SELECT * FROM public.monday_lifecycle_events WHERE event_key LIKE %s',
                              (f"archive-backfill:{manifest['run_id']}:%",)).fetchall()
    result = {}
    for row in rows:
        if row['event_key'] not in wanted:
            raise life.ReviewRequired('Campaign has an unexpected receipt outside approval')
        check_receipt(row, manifest, wanted[row['event_key']])
        result[row['event_key']] = row
    return result


def summary(manifest, groups, known):
    verified = sum(len(g) for g in groups
                   if (known.get(event_key(manifest, g), {}).get('result') or {}).get('phase') == 'verified')
    return dict(run_id=manifest['run_id'], complete=verified == manifest['expected_projects'],
                verified_projects=verified, remaining_projects=manifest['expected_projects'] - verified,
                expected_projects=manifest['expected_projects'])


@contextmanager
def campaign_lock(connection):
    acquired = connection.execute('SELECT pg_try_advisory_lock(hashtextextended(%s,0)) AS acquired',
                                  (POLICY,)).fetchone()['acquired']
    if not acquired:
        raise life.ReviewRequired('Another archive backfill process is running; do not run batches in parallel')
    try:
        yield
    finally:
        if not connection.closed:
            connection.execute('SELECT pg_advisory_unlock(hashtextextended(%s,0))', (POLICY,))


def batches(groups, project_limit):
    if not 1 <= project_limit <= MAX_BATCH_PROJECTS:
        raise ValueError('--batch-size must be between 1 and 25')
    batch, combined = [], {}
    for group in groups:
        candidate = {**combined, **group}
        if batch and (len(candidate) > project_limit
                      or sum(map(len, boundary(candidate).values())) > life.MAX_ROWS):
            yield batch
            batch, combined = [], {}
        batch.append(group)
        combined.update(group)
    if batch:
        yield batch


def subset(source, group):
    result = {t: {i: source[t][i] for i in ids} for t, ids in boundary(group).items()}
    return dict(**result, project_ids=sorted(group), extra_children=[])


def narrow_source(source, group):
    """Exclude columns fetched only for other batch members from checkpoint hashes."""
    result = subset(source, group)
    parent_fields = {compare.PARENT_COLUMNS[f] for f in compare.PARENT_FIELDS}
    parents = [{**r, 'column_values': [c for c in r['column_values'] if c['id'] in parent_fields]}
               for r in result['projects'].values()]
    _, child_extra = compare.mirror_dependencies(parents, life.SUBITEM_BOARD_ID)
    child_fields = {compare.SUBITEM_COLUMNS[f] for f in (*compare.CHILD_FIELDS, 'new_enquiry_value')}
    children = [{**r, 'column_values': [c for c in r['column_values'] if c['id'] in child_fields]}
                for r in result['subitems'].values()]
    _, hidden_extra = compare.mirror_dependencies(children, life.HIDDEN_ITEMS_BOARD_ID)
    columns = dict(projects=set(compare.PARENT_COLUMNS.values()) - {'name'},
                   subitems=(set(compare.SUBITEM_COLUMNS.values()) - {'name'}) | child_extra,
                   hidden_items=(set(compare.HIDDEN_ITEMS_COLUMNS.values()) - {'name'}) | hidden_extra)
    for table, ids in boundary(group).items():
        result[table] = {i: {**result[table][i], 'column_values': sorted(
            [v for v in result[table][i]['column_values'] if v['id'] in columns[table]], key=lambda v: v['id'])}
            for i in ids}
    return result


def require_source(group, source):
    for table, ids in boundary(group).items():
        if set(source[table]) != set(ids):
            raise life.ReviewRequired(f'Monday {table} differs from the approved batch boundary')
    for pid, children in group.items():
        target = dict(monday_id=pid, subitems=children)
        pilot.require_source(target, subset(source, {pid: children}))


def checked_snapshot(connection, group):
    ids = boundary(group)
    children = {}
    for field, selected in [('parent_monday_id', ids['projects']), ('monday_id', ids['subitems']),
                            ('hidden_item_id', ids['hidden_items'])]:
        children.update({r['monday_id']: r for r in life.read_rows(connection, 'subitems', field, selected)})
    before = dict(projects=life.read_rows(connection, 'projects', 'monday_id', ids['projects']),
                  subitems=sorted(children.values(), key=lambda r: r['monday_id']),
                  hidden_items=life.read_rows(connection, 'hidden_items', 'monday_id', ids['hidden_items']))
    for table, selected in ids.items():
        if len(before[table]) != len(selected) or {r['monday_id'] for r in before[table]} != set(selected):
            raise life.ReviewRequired(f'Stored {table} missing or contains outside owners')
    for child in before['subitems']:
        pid, cid = child['parent_monday_id'], child['monday_id']
        if pid not in group or group[pid].get(cid) != child['hidden_item_id']:
            raise life.ReviewRequired(f'{cid}: stored parent/source link differs from approval')
    for parent in before['projects']:
        pilot.require_real_project(parent.get('item_name'), parent.get('project_name'))
    reportable = connection.execute('SELECT monday_id FROM public.reportable_projects WHERE monday_id=ANY(%s)',
                                   (ids['projects'],)).fetchall()
    if {r['monday_id'] for r in reportable} != set(group):
        raise life.ReviewRequired('A selected project is no longer reportable')
    states = archive.read_states(connection, ids)
    for rows in states.values():
        if any(r['blocked'] or r['monday_state'] not in (None, 'active') for r in rows.values()):
            raise life.ReviewRequired('Lifecycle restoration requires separate review')
    return before, states


def project_values(group, source, before, states, contract):
    merged = {t: {} for t in life.BOARDS.values()}
    for pid in group:
        values, issues = refresh.build_values(pid, source, before, contract, lifecycle=states)
        if issues:
            raise life.ReviewRequired(f'{pid}: source projection requires review: {issues}')
        for table, rows in values.items():
            for row in rows:
                item = row['monday_id']
                if item not in boundary(group)[table]:
                    raise life.ReviewRequired('Projected write exceeds approval')
                current = merged[table].setdefault(item, {})
                if any(k in current and current[k] != v for k, v in row.items()):
                    raise life.ReviewRequired('Shared-source projections disagree')
                current.update(row)
    return {t: [rows[i] for i in sorted(rows)] for t, rows in merged.items()}


def capture_cases(connection, monday, batch):
    group = {pid: children for component in batch for pid, children in component.items()}
    source = refresh.fetch_projects(monday, list(group))
    require_source(group, source)
    with connection.transaction():
        connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
        before, states = checked_snapshot(connection, group)
        contract = reconcile.read_contract(connection)
    result = []
    for component in batch:
        ids = boundary(component)
        local_source = narrow_source(source, component)
        local_before = {t: [r for r in before[t] if r['monday_id'] in selected] for t, selected in ids.items()}
        local_states = {t: {i: r for i, r in states[t].items() if i in selected} for t, selected in ids.items()}
        values = project_values(component, local_source, local_before, local_states, contract)
        result.append(dict(group=component, source=local_source, before=local_before,
                           states=local_states, contract=contract, values=values))
    second = refresh.fetch_projects(monday, list(group))
    require_source(group, second)
    for case in result:
        pilot.require_same(case['source'], narrow_source(second, case['group']), 'Monday capture')
        case['captured_at'] = pilot.now().isoformat()
    return result


def check_locked(connection, case):
    pilot.require_fresh(case['captured_at'], pilot.MAX_SOURCE_AGE, 'Monday capture')
    scopes.schema_safety(connection, require_journal=False)
    before, states = checked_snapshot(connection, case['group'])
    pilot.require_same(before, case['before'], 'Stored rows/owners')
    pilot.require_same(states, case['states'], 'Lifecycle observations')
    pilot.require_same(reconcile.read_contract(connection), case['contract'], 'Column contract')


def write_values(connection, job, case):
    counts = scopes.write_updates(connection, case['values'])
    after, _ = checked_snapshot(connection, case['group'])
    audits = []
    for table, rows in after.items():
        old = {r['monday_id']: r for r in case['before'][table]}
        changes = {r['monday_id']: r for r in case['values'][table]}
        for row in rows:
            baseline = old[row['monday_id']]
            expected = {**baseline, **changes.get(row['monday_id'], {})}
            for field, value in expected.items():
                column = case['contract'][table][field]
                if field in scopes.VOLATILE_FIELDS or column['generated'] != 'NEVER':
                    continue
                if not refresh.same_value(value, row[field], column):
                    raise life.ReviewRequired(f'Post-write mismatch: {table}/{row["monday_id"]}/{field}')
            if table == 'projects':
                stage = row.get('pipeline_stage')
                category = 'Won' if stage == 'Won - Closed (Invoiced)' else 'Lost' if stage == 'Lost' else 'Open'
                if row.get('status_category') != category:
                    raise life.ReviewRequired('Generated project category does not match the source stage')
            if row['monday_id'] in changes:
                audits.append(('refresh_field_changes', table, row['monday_id'], baseline,
                    {'after_row': row, 'changes': {k: {'before': baseline.get(k), 'after': row.get(k)}
                                                  for k in row if row.get(k) != baseline.get(k)}}))
    life.audit_many(connection, job, audits)
    return after, counts


def apply_case(connection, manifest, case):
    group = case['group']
    connection.execute('INSERT INTO public.monday_lifecycle_events '
        '(event_key,board_id,item_id,kind,payload,status,result,last_error) '
        "VALUES (%s,%s,%s,'refresh',%s,'review',%s,'Backfill operator completion required') "
        'ON CONFLICT(event_key) DO NOTHING',
        (event_key(manifest, group), life.PARENT_BOARD_ID, min(group),
         Jsonb(payload(manifest, group)), Jsonb({'phase': 'prepared'})))
    with life.locked_write_transaction(connection):
        connection.execute('LOCK TABLE public.monday_item_lifecycle IN SHARE ROW EXCLUSIVE MODE')
        job = receipt(connection, manifest, group, locked=True)
        if job['result']['phase'] != 'prepared':
            raise life.ReviewRequired('Project advanced in another execution; do not replay writes')
        check_locked(connection, case)
        archive.observe_active_source_batch(connection, job, case['source'],
                                            datetime.fromisoformat(case['captured_at']))
        after, counts = write_values(connection, job, case)
        result = dict(phase='applied_pending_verification', source_sha256=cli.digest(case['source']),
            after_sha256=cli.digest(after), states_sha256=cli.digest(archive.read_states(connection, boundary(group))),
            contract_sha256=cli.digest(case['contract']), rows_written=counts)
        connection.execute('UPDATE public.monday_lifecycle_events SET result=%s,last_error=%s WHERE event_key=%s',
                           (Jsonb(result), 'Fresh verification required; resume the backfill CLI', job['event_key']))
    return receipt(connection, manifest, group)


def verify_case(connection, manifest, case, job):
    result = job['result']
    if result['phase'] != 'applied_pending_verification':
        raise life.ReviewRequired('Verification requires an applied, unverified scope')
    for key, value in [('source', case['source']), ('after', case['before']),
                       ('states', case['states']), ('contract', case['contract'])]:
        pilot.require_same(cli.digest(value), result[key + '_sha256'], f'Post-write {key}')
    if any(case['values'].values()):
        raise life.ReviewRequired('Fresh source projection still differs from SQL')
    with life.locked_write_transaction(connection):
        connection.execute('LOCK TABLE public.monday_item_lifecycle IN SHARE ROW EXCLUSIVE MODE')
        current = receipt(connection, manifest, case['group'], locked=True)
        pilot.require_same(current, job, 'Verification receipt')
        check_locked(connection, case)
        for pid in case['group']:
            archive.verify_parent_values(connection, job, pid)
        result = {**result, 'phase': 'verified', 'verified_at': pilot.now().isoformat(),
                  'states_sha256': cli.digest(archive.read_states(connection, boundary(case['group'])))}
        connection.execute("UPDATE public.monday_lifecycle_events SET status='processed',result=%s,"
                           'last_error=NULL,processed_at=now() WHERE event_key=%s', (Jsonb(result), job['event_key']))
    return receipt(connection, manifest, case['group'])


def execute(connection, monday, run_dir, *, confirmation=None, batch_size=25, max_batches=None, preview=False):
    manifest, groups = load_campaign(connection, run_dir, confirmation)
    if not preview and confirmation is None:
        raise ValueError('Explicit campaign confirmation is required')
    if max_batches is not None and max_batches < 1:
        raise ValueError('--max-batches must be positive')
    with campaign_lock(connection):
        known = receipts(connection, manifest, groups)
        pending = [g for g in groups if (known.get(event_key(manifest, g), {}).get('result') or {}).get('phase') != 'verified']
        work = list(batches(pending, batch_size))
        attempt_dir = run_dir / ('preview-' if preview else 'attempt-') / uuid4().hex
        attempt_dir.mkdir(parents=True)
        result_path = run_dir / ('preview_result.json' if preview else 'result.json')
        progress = summary(manifest, groups, known)
        pilot.write_json(result_path, {**progress, 'phase': 'checking', 'read_only': preview})
        completed_batches = 0
        try:
            for index, batch in enumerate(work, 1):
                if max_batches is not None and index > max_batches:
                    break
                LOG.info('Capturing batch %s: %s projects', index, sum(map(len, batch)))
                cases = capture_cases(connection, monday, batch)
                pilot.write_json(attempt_dir / f'batch_{index:06d}.json', cases)
                if not preview:
                    for case in cases:
                        key = event_key(manifest, case['group'])
                        job = known.get(key)
                        if not job or job['result']['phase'] == 'prepared':
                            known[key] = apply_case(connection, manifest, case)
                    for case in capture_cases(connection, monday, batch):
                        key = event_key(manifest, case['group'])
                        known[key] = verify_case(connection, manifest, case, known[key])
                        progress['verified_projects'] += len(case['group'])
                        progress['remaining_projects'] -= len(case['group'])
                        progress['complete'] = progress['remaining_projects'] == 0
                        pilot.write_json(result_path, {**progress, 'phase': 'running'})
                        LOG.info('Verified %s; %s/%s projects complete', ','.join(case['group']),
                                 progress['verified_projects'], manifest['expected_projects'])
                completed_batches += 1
            result = {**progress, 'phase': 'previewed' if preview else 'paused',
                      'batches_this_invocation': completed_batches, 'read_only': preview,
                      'evidence_directory': str(attempt_dir)}
            if result['complete'] and not preview:
                result['phase'] = 'complete'
            pilot.write_json(result_path, result)
            return result
        except (ValueError, RuntimeError, psycopg.Error, requests.RequestException, OSError, KeyboardInterrupt) as exc:
            pilot.write_json(result_path, {**progress, 'complete': False, 'phase': 'stopped',
                             'error_type': type(exc).__name__,
                             'error': str(exc) if isinstance(exc, (ValueError, RuntimeError)) else type(exc).__name__,
                             'evidence_directory': str(attempt_dir),
                             'note': 'Counts are last confirmed results; database receipts decide ambiguous commits.'})
            raise


def main(argv=None):
    load_dotenv()
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('src.database.sync_service').setLevel(logging.WARNING)
    parser = argparse.ArgumentParser(description=__doc__)
    subs = parser.add_subparsers(dest='command', required=True)
    p = subs.add_parser('export-approval')
    p.add_argument('--source-dir', type=Path, required=True)
    p = subs.add_parser('prepare')
    p.add_argument('--run-dir', type=Path, required=True)
    p.add_argument('--pilot-run-id', required=True)
    for command in ('run', 'preview', 'status'):
        p = subs.add_parser(command)
        p.add_argument('--run-dir', type=Path, required=True)
        if command != 'status':
            p.add_argument('--batch-size', type=int, default=25)
            p.add_argument('--max-batches', type=int, default=1 if command == 'preview' else None)
        if command == 'run':
            p.add_argument('--confirm-run-id', required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == 'export-approval':
            result = export_approval(args.source_dir)
        else:
            with life.connect() as connection:
                if args.command != 'run':
                    connection.execute('SET default_transaction_read_only=on')
                if args.command == 'prepare':
                    result = prepare(connection, args.run_dir, args.pilot_run_id)
                elif args.command == 'status':
                    manifest, groups = load_campaign(connection, args.run_dir)
                    result = summary(manifest, groups, receipts(connection, manifest, groups))
                else:
                    monday = compare.ComparisonMondayClient()
                    try:
                        result = execute(connection, monday, args.run_dir,
                            confirmation=getattr(args, 'confirm_run_id', None), batch_size=args.batch_size,
                            max_batches=args.max_batches, preview=args.command == 'preview')
                    finally:
                        monday.session.close()
        print(json.dumps(result, indent=2, default=str))
        return 0
    except (ValueError, RuntimeError) as exc:
        LOG.error('Backfill stopped: %s. Inspect the retained evidence and resume the same campaign.', exc)
        return 1
    except (psycopg.Error, requests.RequestException, OSError) as exc:
        LOG.error('Backfill stopped (%s). Database receipts determine progress; do not assume an ambiguous '
                  'commit rolled back. Restore connectivity/files before resuming.', type(exc).__name__)
        return 1
    except KeyboardInterrupt:
        LOG.error('Backfill interrupted. Resume the same campaign; database receipts preserve progress.')
        return 130


if __name__ == '__main__':
    raise SystemExit(main())
