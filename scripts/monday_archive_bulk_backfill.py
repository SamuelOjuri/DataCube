"""Resolve the remaining approved archive-coverage projects with bulk Monday reads.

Replaces the `run` command of monday_archive_backfill.py for the unfinished campaign.
That runner read every 25-project batch from Monday four times, 10 IDs per request
(about 4.5 calls per project), gave up on any cooldown over 30 seconds, and failed a
project after one 750 ms wait for the table lock (245 of its 252 failures). This one:

- reads each chunk of projects twice, 100 IDs per request, about 0.15 calls per project,
  and writes only projects whose two reads match;
- paces itself from Monday's RateLimit headers, waits out throttling, and stops cleanly
  at a daily-limit error or at its own call budget, so production keeps its allowance;
- writes each project in a short locked transaction that retries lock waits, using the
  lifecycle worker's own projection, observations, write checks and value verification;
- isolates problems per project, so one bad project never stops a chunk.

It selects reviewed approval projects that are not yet verified, so a rerun resumes
where the last one stopped. It never writes to Monday, restores lifecycle states or
guesses links: anything needing judgment is reported for review.
"""
import argparse
from collections import Counter
from contextlib import contextmanager, nullcontext
from copy import deepcopy
import csv
from datetime import datetime, timezone
from decimal import InvalidOperation
import json
import logging
import os
from pathlib import Path
import re
import sys
import time
from uuid import uuid4

from dotenv import load_dotenv
import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from scripts import monday_archive_backfill as campaign
from scripts import monday_archive_pilot as pilot
from scripts import monday_lifecycle as cli
from scripts import order_value_monday_compare as compare
from scripts import reconcile_order_values as reconcile
from src.config import HIDDEN_ITEMS_COLUMNS, PARENT_COLUMNS, SUBITEM_COLUMNS
from src.services import monday_archive as archive
from src.services import monday_lifecycle as life
from src.services import monday_lifecycle_refresh as refresh

LOG = logging.getLogger(__name__)
POLICY = 'archive_coverage_bulk_v1'
RECEIPT_PREFIX = 'archive-backfill:'
PARENT_IDS = sorted(set(PARENT_COLUMNS.values()) - {'name'})
CHILD_BASE = set(SUBITEM_COLUMNS.values()) - {'name'}
HIDDEN_BASE = set(HIDDEN_ITEMS_COLUMNS.values()) - {'name'}
PARENT_MIRRORS = {PARENT_COLUMNS[f] for f in compare.PARENT_FIELDS}
CHILD_MIRRORS = {SUBITEM_COLUMNS[f] for f in (*compare.CHILD_FIELDS, 'new_enquiry_value')}
REQUEST_TIMEOUT = (10, 120)
THROTTLE_ATTEMPTS = 8
MAX_COOLDOWN = 900
MIN_INTERVAL = 0.25
COMPLEXITY_RESERVE = 1_500_000
MINUTE_RESERVE = 200
RATE_LIMIT = re.compile(r'"(\w+)";r=(\d+)(?:;t=(\d+))?')
PENDING_SQL = '''
SELECT p.monday_id,
       COALESCE(l.blocked, false) OR COALESCE(l.monday_state, 'active') <> 'active' AS held
FROM public.projects p
JOIN public.reportable_projects r ON r.monday_id = p.monday_id
LEFT JOIN public.monday_item_lifecycle l ON l.table_name = 'projects' AND l.monday_id = p.monday_id
WHERE p.monday_id = ANY(%s)
  AND NOT COALESCE(l.monday_state = 'active' AND NOT l.blocked
           AND l.state_evidence->>'transaction_values_verified' = 'true'
           AND jsonb_typeof(l.state_evidence->'item'->'subitems') = 'array', false)
ORDER BY p.monday_id'''


class StopRun(Exception):
    """End the run cleanly at a chunk boundary; a rerun resumes."""


class DailyLimitReached(StopRun):
    pass


class CallBudgetReached(StopRun):
    pass


class PacedMondayClient(compare.ComparisonMondayClient):
    """The comparison transport, paced from Monday's RateLimit headers.

    Throttling (per-minute requests, complexity, concurrency) is waited out here, so the
    shared read_batch never aborts a long run for a cooldown over 30 seconds. All calls,
    including throttled ones, count toward the account's daily allowance.
    """

    def __init__(self, max_calls, *, api_key=None, sleep=time.sleep, clock=time.monotonic):
        super().__init__(api_key)
        self.max_calls, self.sleep, self.clock = max_calls, sleep, clock
        self.calls, self.throttled, self.waited = 0, 0, 0.0
        self.limits, self.min_complexity = {}, None
        self.last_call = None

    def _pause(self, seconds, reason):
        if seconds > 0:
            LOG.info('Pausing Monday reads %.0fs: %s', seconds, reason)
            self.sleep(seconds)
            self.waited += seconds

    def _pace(self):
        if self.last_call is not None:
            self._pause(MIN_INTERVAL - (self.clock() - self.last_call), 'request spacing')
        for name, reserve in (('complexityMinute', COMPLEXITY_RESERVE), ('minuteRate', MINUTE_RESERVE)):
            remaining, reset = self.limits.get(name, (None, None))
            if remaining is not None and remaining < reserve:
                self._pause(reset or 15, f'{name} budget low ({remaining} left)')
                self.limits.pop(name)

    def _record(self, headers):
        for name, remaining, reset in RATE_LIMIT.findall(headers.get('RateLimit') or ''):
            self.limits[name] = (int(remaining), int(reset) if reset else None)
        complexity = self.limits.get('complexityMinute', (None,))[0]
        if complexity is not None:
            self.min_complexity = complexity if self.min_complexity is None else min(self.min_complexity, complexity)

    def _execute_read(self, query, variables=None):
        for attempt in range(1, THROTTLE_ATTEMPTS + 1):
            if self.calls >= self.max_calls:
                raise CallBudgetReached(f'Monday call budget of {self.max_calls} reached; rerun to continue')
            self._pace()
            self.calls += 1
            self.last_call = self.clock()
            with self.session.post(self.api_url, json={'query': query, 'variables': variables or {}},
                                   headers=self.headers, timeout=REQUEST_TIMEOUT) as response:
                self._record(response.headers)
                try:
                    body = response.json()
                except ValueError:
                    body = {}
                body = body if isinstance(body, dict) else {}
                error = compare.response_error(body, status=response.status_code, headers=response.headers)
            if error is None:
                if not isinstance(body.get('data'), dict):
                    raise ValueError('Monday returned no valid GraphQL data')
                return body
            if 'DAILY_LIMIT_EXCEEDED' in error.codes:
                raise DailyLimitReached('Monday daily call limit reached; rerun after midnight UTC')
            throttled = error.retryable and not error.splittable and (
                set(error.codes) & (compare.THROTTLE_CODES | {'HTTP_429'}))
            if not throttled or attempt == THROTTLE_ATTEMPTS or error.retry_after > MAX_COOLDOWN:
                raise error
            self.throttled += 1
            self._pause(max(error.retry_after, 5 * attempt), f'Monday throttled the read ({error})')
        raise AssertionError('unreachable')


def only(item, columns):
    return {**item, 'column_values': [v for v in item['column_values'] if v['id'] in columns]}


def source_digest(source, contract):
    """Compare numeric parent formulas at their declared database precision.

    Monday can alternate Project Value between e.g. 241502.0100000001 and
    241502.01 without an item update. Keep the captured evidence untouched and
    keep child/source formulas exact: their unrounded values feed parent sums.
    Unmapped, unscaled or unreadable formulas retain strict comparison.
    """
    normalized = deepcopy(source)
    fields = {cid: field for field, cid in PARENT_COLUMNS.items()
              if contract['projects'].get(field, {}).get('type') == 'numeric'
              and isinstance(contract['projects'][field].get('scale'), int)
              and contract['projects'][field].get('generated', 'NEVER') == 'NEVER'}
    for parent in (normalized or {}).get('projects', {}).values():
        for column in parent['column_values']:
            field = fields.get(column['id'])
            if field is None or column.get('__typename') != 'FormulaValue':
                continue
            try:
                amount = compare.numeric(column)
                if amount is not None:
                    column['display_value'] = reconcile.normalize_updates(
                        {'projects': [{field: amount}]}, contract)['projects'][0][field]
            except (ValueError, InvalidOperation):
                pass  # Invalid source values must never become comparable amounts.
    return cli.digest(normalized)


def capture_chunk(monday, pids, batch_size):
    """refresh.fetch_projects() for many projects in a few large reads.

    Returns (sources, problems). Each source equals what fetch_projects() returns for
    that project alone; a problem with one project is recorded against it only.
    """
    problems, plans = {}, {}
    parents = compare.fetch_items(monday, pids, PARENT_IDS, parents=True, batch_size=batch_size)
    for pid in pids:
        try:
            parent = life.require_item(parents, pid, 'projects', 'active')
            if parent.get('parent_item') is not None:
                raise life.ReviewRequired('Selected project is now a subitem')
            members = [c['id'] for c in parent['subitems']]
            if len(members) != len(set(members)) or any(
                    (c.get('parent_item') or {}).get('id') != pid for c in parent['subitems']):
                raise life.ReviewRequired('Duplicate or inconsistent current Monday child membership')
            depends, columns = compare.mirror_dependencies([only(parent, PARENT_MIRRORS)], life.SUBITEM_BOARD_ID)
            plans[pid] = dict(members=members, children=sorted(set(members) | depends),
                              child_columns=columns | CHILD_BASE)
        except ValueError as exc:
            problems[pid] = str(exc)
    child_ids = sorted({c for plan in plans.values() for c in plan['children']})
    child_columns = sorted(set().union(*(plan['child_columns'] for plan in plans.values())))
    children = compare.fetch_items(monday, child_ids, child_columns, batch_size=batch_size) if child_ids else {}
    for pid, plan in list(plans.items()):
        try:
            hidden_ids = set()
            for cid in plan['members']:
                child = life.require_item(children, cid, 'subitems', 'active')
                if (child.get('parent_item') or {}).get('id') != pid:
                    raise life.ReviewRequired('Child moved during refresh')
                hidden_ids.update(compare.links(child))
            depends, columns = compare.mirror_dependencies(
                [only(children[c], CHILD_MIRRORS) for c in plan['children'] if c in children],
                life.HIDDEN_ITEMS_BOARD_ID)
            plan.update(hidden=sorted(hidden_ids | depends), hidden_columns=columns | HIDDEN_BASE)
            if 1 + len(plan['children']) + len(plan['hidden']) > life.MAX_ROWS:
                raise life.ReviewRequired('Refresh exceeds 500 related rows')
        except ValueError as exc:
            problems[pid] = str(exc)
            del plans[pid]
    hidden_ids = sorted({h for plan in plans.values() for h in plan['hidden']})
    hidden_columns = sorted(set().union(*(plan['hidden_columns'] for plan in plans.values())))
    hidden = (compare.fetch_items(monday, hidden_ids, hidden_columns, mirror_depth=0, batch_size=batch_size)
              if hidden_ids else {})
    sources = {pid: dict(
        projects={pid: only(parents[pid], set(PARENT_IDS))},
        subitems={c: only(children[c], plan['child_columns']) for c in plan['children'] if c in children},
        hidden_items={h: only(hidden[h], plan['hidden_columns']) for h in plan['hidden'] if h in hidden},
        project_ids=[pid], extra_children=[]) for pid, plan in plans.items()}
    return sources, problems


def capture(monday, pids, batch_size):
    """capture_chunk(), halving the chunk when a Monday read fails outright."""
    try:
        return capture_chunk(monday, pids, batch_size)
    except ValueError as exc:
        if len(pids) == 1:
            return {}, {pids[0]: f'Monday read failed: {exc}'}
        LOG.warning('Monday read failed for %d projects; retrying in halves: %s', len(pids), exc)
        middle = len(pids) // 2
        left, left_problems = capture(monday, pids[:middle], batch_size)
        right, right_problems = capture(monday, pids[middle:], batch_size)
        return {**left, **right}, {**left_problems, **right_problems}


def decisions(states):
    return {t: {i: (r['monday_state'], r['blocked']) for i, r in rows.items()} for t, rows in states.items()}


def call_count(monday):
    calls = getattr(monday, 'calls', 0)
    return len(calls) if isinstance(calls, list) else calls


def plan_project(connection, pid, source, contract):
    """The full project refresh's projection, with the old campaign's scope guards."""
    ids = {t: sorted(source[t]) for t in life.BOARDS.values()}
    with connection.transaction():
        connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
        before = refresh.read_snapshot(connection, pid, source)
        states = archive.read_states(connection, ids)
        reportable = connection.execute('SELECT 1 FROM public.reportable_projects WHERE monday_id=%s',
                                        (pid,)).fetchone()
    if len(before['projects']) != 1:
        raise life.ReviewRequired('Stored project row is missing')
    if not reportable:
        raise life.ReviewRequired('Project is excluded from reporting')
    stored, observed = before['projects'][0], source['projects'][pid]
    pilot.require_real_project(stored.get('item_name'), stored.get('project_name'), observed['name'],
                               compare.scalar(compare.col(observed, PARENT_COLUMNS['project_name']), 'text'))
    others = sorted({str(r.get('parent_monday_id')) for r in before['subitems']} - {pid})
    if others:
        raise life.ReviewRequired(f'Shares subitems or hidden sources with project(s) {", ".join(others)}; '
                                  'refresh those projects together separately')
    if any(r['blocked'] or r['monday_state'] not in (None, 'active') for rows in states.values() for r in rows.values()):
        raise life.ReviewRequired('Lifecycle restoration requires separate review')
    values, issues = refresh.build_values(pid, source, before, contract, lifecycle=states)
    if issues:
        detail = '; '.join(f"{i['table']}/{i['monday_id']}/{i['field']}: {i['reason']}" for i in issues[:3])
        raise life.ReviewRequired(f'Source projection requires review: {detail}')
    return dict(before=before, states=states, values=values)


def close_receipts(connection, job, pid, scope_sha256):
    """Mark the unfinished campaign receipt for this single-project scope verified."""
    rows = connection.execute(
        "SELECT to_jsonb(e) AS row FROM public.monday_lifecycle_events e WHERE item_id=%s AND event_key LIKE %s "
        "AND status='review' AND payload ? 'operator_policy' FOR UPDATE",
        (pid, RECEIPT_PREFIX + '%')).fetchall()
    closed = []
    for row in (r['row'] for r in rows):
        if row['payload'].get('scope_sha256') != scope_sha256:
            LOG.warning('Leaving receipt %s: its scope is not exactly project %s', row['event_key'], pid)
            continue
        result = {**(row['result'] or {}), 'phase': 'verified', 'verified_at': pilot.now().isoformat(),
                  'verified_by': job['event_key']}
        connection.execute("UPDATE public.monday_lifecycle_events SET status='processed',result=%s,"
                           'last_error=NULL,processed_at=now() WHERE event_key=%s',
                           (Jsonb(result), row['event_key']))
        closed.append(row)
    life.audit_many(connection, job, [
        ('close_backfill_receipt', 'projects', pid, row, {'policy': POLICY, 'receipt': row['event_key'],
         'reason': 'project verified from a fresh matching Monday read'}) for row in closed])
    return [row['event_key'] for row in closed]


def apply_project(connection, job, pid, source, plan, contract, verified_at, scope_sha256):
    ids = {t: sorted(source[t]) for t in life.BOARDS.values()}

    def write():
        with life.locked_write_transaction(connection):
            if (refresh.read_snapshot(connection, pid, source) != plan['before']
                    or reconcile.read_contract(connection) != contract):
                raise ValueError('Stored rows or schema changed since planning; a rerun retries it')
            if decisions(archive.read_states(connection, ids)) != decisions(plan['states']):
                raise ValueError('Lifecycle state changed since planning; a rerun retries it')
            archive.observe_source(connection, job, source, verified_at)
            refresh.write_values(connection, plan['values'], plan['before'], contract, job=job)
            archive.verify_parent_values(connection, job, pid)
            return close_receipts(connection, job, pid, scope_sha256)

    return life.retry_lock_conflicts(connection, write)


@contextmanager
def exclusive(connection):
    """One archive backfill at a time, old runner included."""
    held = []
    try:
        for policy in (POLICY, campaign.POLICY):
            if not connection.execute('SELECT pg_try_advisory_lock(hashtextextended(%s,0)) AS ok',
                                      (policy,)).fetchone()['ok']:
                raise ValueError(f'Another archive backfill ({policy}) is running; run only one at a time')
            held.append(policy)
        yield
    finally:
        for policy in held:
            if not connection.closed:
                connection.execute('SELECT pg_advisory_unlock(hashtextextended(%s,0))', (policy,))


def run(connection, monday, out_dir, *, approved, dry_run, limit=None, chunk_size=100,
        batch_size=100, only_ids=None):
    """Process pending approved projects; write progress to out_dir after every chunk."""
    if not 1 <= chunk_size <= 500:
        raise ValueError('--chunk-size must be 1-500 projects')
    if dry_run:
        # Belt and braces: the database itself refuses writes from a preview.
        connection.execute('SET default_transaction_read_only = on')
    pilot.require_environment(connection)
    selected = set(approved)
    if only_ids is not None:
        outside = set(only_ids) - selected
        if outside:
            raise ValueError(f'Not in the reviewed approval: {", ".join(sorted(outside)[:10])}')
        selected = set(only_ids)
    rows = connection.execute(PENDING_SQL, (sorted(selected),)).fetchall()
    held = [r['monday_id'] for r in rows if r['held']]
    pending = [r['monday_id'] for r in rows if not r['held']]
    work = pending[:limit] if limit else pending
    run_id = str(uuid4())
    out_dir.mkdir(parents=True, exist_ok=True)
    outcomes = []
    result = dict(run_id=run_id, mode='dry_run' if dry_run else 'apply', policy=POLICY,
                  started_at=pilot.now().isoformat(), approved_projects=len(approved),
                  pending_at_start=len(pending), held_lifecycle_projects=len(held),
                  selected_this_run=len(work), chunk_size=chunk_size, ids_per_request=batch_size)

    def record(pid, outcome, reason='', **extra):
        outcomes.append(dict(project_id=pid, outcome=outcome, reason=reason, **extra))

    def save(phase, stop_reason=None):
        counts = Counter(o['outcome'] for o in outcomes)
        reasons = Counter(re.sub(r'\d{6,}', '#', o['reason'])[:160] for o in outcomes if o['outcome'] == 'review')
        calls = call_count(monday)
        remaining = len(pending) - counts['verified']
        per_project = calls / len(outcomes) if calls and outcomes else None
        result.update(phase=phase, stop_reason=stop_reason, updated_at=pilot.now().isoformat(),
                      processed=len(outcomes), outcomes=dict(counts), top_review_reasons=reasons.most_common(15),
                      monday_calls=calls, monday_throttled=getattr(monday, 'throttled', None),
                      monday_waited_seconds=round(getattr(monday, 'waited', 0.0)),
                      min_complexity_remaining=getattr(monday, 'min_complexity', None),
                      calls_per_project=round(per_project, 3) if per_project else None,
                      estimated_calls_for_remaining=round(per_project * remaining) if per_project else None)
        pilot.write_json(out_dir / 'result.json', result)
        with (out_dir / 'outcomes.csv').open('w', encoding='utf-8', newline='') as stream:
            writer = csv.DictWriter(stream, ['project_id', 'outcome', 'reason', 'rows_written', 'receipts_closed'],
                                    extrasaction='ignore')
            writer.writeheader()
            writer.writerows(outcomes)

    LOG.info('%d approved projects pending (%d held for lifecycle review); this run: %d, %s',
             len(pending), len(held), len(work), 'dry run' if dry_run else 'applying')
    chunks = [work[i:i + chunk_size] for i in range(0, len(work), chunk_size)]
    try:
        with (nullcontext() if dry_run else exclusive(connection)):
            for number, chunk in enumerate(chunks, 1):
                calls_before = call_count(monday)
                first, problems = capture(monday, chunk, batch_size)
                read_at = pilot.now()
                second, changed = capture(monday, sorted(first), batch_size) if first else ({}, {})
                contract = reconcile.read_contract(connection)
                for pid in chunk:
                    if pid in problems:
                        record(pid, 'review', problems[pid])
                        continue
                    if (pid in changed
                            or source_digest(first[pid], contract) != source_digest(second.get(pid), contract)):
                        record(pid, 'retry', changed.get(pid, 'Monday changed between the two reads'))
                        continue
                    source = second[pid]
                    try:
                        plan = plan_project(connection, pid, source, contract)
                    except ValueError as exc:
                        record(pid, 'review', str(exc))
                        continue
                    rows_written = sum(map(len, plan['values'].values()))
                    if dry_run:
                        record(pid, 'would_verify', rows_written=rows_written)
                        continue
                    if pilot.now() - read_at > pilot.MAX_SOURCE_AGE:
                        record(pid, 'retry', 'Monday read is over 5 minutes old')
                        continue
                    job = {'event_key': f'archive-bulk:{run_id}:{pid}'}
                    scope = cli.digest({pid: approved[pid]}) if pid in approved else None
                    try:
                        closed = apply_project(connection, job, pid, source, plan, contract, read_at, scope)
                    except (psycopg.errors.LockNotAvailable, psycopg.errors.QueryCanceled) as exc:
                        record(pid, 'retry', f'Database busy through every retry ({type(exc).__name__})')
                        continue
                    except life.ReviewRequired as exc:
                        outcome = 'retry' if str(exc).startswith('A newer lifecycle observation') else 'review'
                        record(pid, outcome, str(exc))
                        continue
                    except ValueError as exc:
                        record(pid, 'retry', str(exc))
                        continue
                    except psycopg.Error as exc:
                        # transaction_timeout ends the whole session; everything committed is kept.
                        if connection.closed:
                            raise StopRun(f'Database session ended ({type(exc).__name__}); rerun to resume') from exc
                        record(pid, 'review', f'{type(exc).__name__}: {exc}')
                        continue
                    record(pid, 'verified', rows_written=rows_written, receipts_closed=';'.join(closed))
                save('running')
                counts = Counter(o['outcome'] for o in outcomes)
                LOG.info('Chunk %d/%d: %d projects, %d Monday calls; so far %s; %d/%d processed',
                         number, len(chunks), len(chunk), call_count(monday) - calls_before,
                         dict(counts), len(outcomes), len(work))
    except StopRun as exc:
        save('stopped', str(exc))
        LOG.warning('Stopped: %s', exc)
        return result
    except BaseException as exc:
        save('failed', f'{type(exc).__name__}: {exc}')
        raise
    save('complete')
    return result


def connect():
    dsn = os.getenv('SUPABASE_DB_URL')
    if not dsn:
        raise RuntimeError('SUPABASE_DB_URL is required')
    return psycopg.connect(dsn, autocommit=True, row_factory=dict_row, connect_timeout=15,
                           application_name='datacube-archive-bulk-backfill')


def main(argv=None):
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
    for noisy in ('src.database.sync_service', 'scripts.order_value_monday_compare', 'numexpr'):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--dry-run', action='store_true', help='Read Monday and Supabase only; write nothing')
    parser.add_argument('--limit', type=int, help='Process at most this many pending projects')
    parser.add_argument('--chunk-size', type=int, default=100, help='Projects read together (default 100)')
    parser.add_argument('--ids-per-request', type=int, default=100, help='Monday IDs per request (1-100)')
    parser.add_argument('--max-monday-calls', type=int, default=2000,
                        help='Stop cleanly after this many Monday calls (default 2000)')
    parser.add_argument('--project-id', action='append', help='Only these approved projects (repeatable)')
    parser.add_argument('--output-dir', type=Path)
    args = parser.parse_args(argv)
    if args.limit is not None and args.limit < 1:
        parser.error('--limit must be positive')
    if args.max_monday_calls < 1:
        parser.error('--max-monday-calls must be positive')
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    out_dir = args.output_dir or Path('outputs/archive_coverage') / f"bulk_{'preview' if args.dry_run else 'run'}_{stamp}"
    approved = campaign.load_approval()
    monday = PacedMondayClient(args.max_monday_calls)
    try:
        with connect() as connection:
            result = run(connection, monday, out_dir, approved=approved, dry_run=args.dry_run, limit=args.limit,
                         chunk_size=args.chunk_size, batch_size=args.ids_per_request, only_ids=args.project_id)
    finally:
        monday.session.close()
    print(json.dumps({k: result.get(k) for k in (
        'phase', 'stop_reason', 'mode', 'pending_at_start', 'selected_this_run', 'processed', 'outcomes',
        'monday_calls', 'calls_per_project', 'estimated_calls_for_remaining', 'monday_waited_seconds')}, indent=2))
    print(f'Details: {out_dir}')
    if result['phase'] == 'stopped':
        return 2
    return 1 if result['outcomes'].get('review') or result['outcomes'].get('retry') else 0


if __name__ == '__main__':
    sys.exit(main())
