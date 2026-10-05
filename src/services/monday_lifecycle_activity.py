"""Explicit recovery of one missing subitem using fresh Monday activity history.

This is not an absence-based deletion rule. Both staging and the worker must
re-read the selected deletion event and all available later activity, bracketed
by exact-ID/parent-membership reads. No project or hidden-source cascade is allowed.
"""
from datetime import datetime, timedelta, timezone
import hashlib
import json
import re

from scripts.order_value_monday_compare import ComparisonMondayClient
from . import monday_lifecycle as life

MODE = 'subitem-activity-deletion-v1'
PAGE_SIZE = 100
MAX_PAGES = 10  # Per board; fail closed rather than accept truncated history.
MAX_PROOF_AGE = timedelta(minutes=5)
# These parent-only edits do not change the child's lifecycle. Other actions,
# including unfamiliar move/restore aliases, need a new review.
PARENT_METADATA_EVENTS = {'update_column_value', 'update_name', 'subscribe', 'unsubscribe'}
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
ACTIVITY_QUERY = '''query LifecycleActivityDeletion(
    $board: ID!, $items: [ID!]!, $from: ISO8601DateTime!,
    $to: ISO8601DateTime!, $page: Int!, $limit: Int!) {
    boards(ids: [$board]) {
        id state
        activity_logs(item_ids: $items, from: $from, to: $to, page: $page, limit: $limit) {
            id event entity data user_id created_at
        }
    }
}'''


class LifecycleMondayClient(ComparisonMondayClient):
    def execute_query(self, query, variables=None):
        if query == ACTIVITY_QUERY:
            return self._execute_read(query, variables)
        return super().execute_query(query, variables)


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()


def utc_date(value):
    try:
        result = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if result.tzinfo is None:
            raise ValueError('Timezone required')
        return result.astimezone(timezone.utc)
    except (AttributeError, TypeError, ValueError) as exc:
        raise life.ReviewRequired('Activity recovery needs an ISO timestamp with timezone') from exc


def validate_request(request):
    required = {'mode', 'board_id', 'item_id', 'parent_id', 'log_id', 'from'}
    if (not isinstance(request, dict) or not required <= request.keys()
            or request.keys() - required - {'deletion_sha256'}
            or request['mode'] != MODE or request['board_id'] != life.SUBITEM_BOARD_ID):
        raise life.ReviewRequired('Activity recovery supports only an explicit subitem deletion')
    for key in ('item_id', 'parent_id'):
        if not isinstance(request[key], str) or not re.fullmatch(r'[0-9]+', request[key]):
            raise life.ReviewRequired('Activity recovery requires exact numeric item and parent IDs')
    if request['item_id'] == request['parent_id']:
        raise life.ReviewRequired('Subitem and parent IDs must differ')
    if not isinstance(request['log_id'], str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', request['log_id']):
        raise life.ReviewRequired('Specify the exact deletion activity log ID')
    if 'deletion_sha256' in request and not re.fullmatch(r'[a-f0-9]{64}', str(request['deletion_sha256'])):
        raise life.ReviewRequired('Invalid reviewed deletion fingerprint')
    if utc_date(request['from']) >= datetime.now(timezone.utc):
        raise life.ReviewRequired('Activity history must start before the deletion')


def require_snapshot(request, before):
    validate_request(request)
    rows = before.get('subitems', [])
    if (before.get('projects') or before.get('hidden_items') or len(rows) != 1
            or rows[0].get('monday_id') != request['item_id']
            or rows[0].get('parent_monday_id') != request['parent_id']):
        raise life.ReviewRequired('Activity recovery requires one unchanged subitem with the reviewed parent')


def current_membership(monday, request):
    rows = life.read_items(monday, {request['item_id'], request['parent_id']}, parents=True)
    if request['item_id'] in rows:
        raise life.ReviewRequired('Activity recovery requires a missing item; current Monday record was returned')
    parent = life.require_item(rows, request['parent_id'], 'projects', 'active')
    if parent.get('parent_item') is not None:
        raise life.ReviewRequired('Reviewed parent is no longer a project')
    members = parent.get('subitems')
    if not isinstance(members, list) or len(members) > life.MAX_ROWS:
        raise life.ReviewRequired('Parent membership is incomplete or exceeds the recovery limit')
    for child in members:
        if (str(child.get('id')) == request['item_id']
                or (child.get('board') or {}).get('id') != life.SUBITEM_BOARD_ID
                or (child.get('parent_item') or {}).get('id') != request['parent_id']
                or child.get('state') != 'active'):
            raise life.ReviewRequired('Current parent membership conflicts with the reviewed deletion')
    return rows


def event_time(event):
    value = event.get('created_at')
    if not isinstance(value, str) or not re.fullmatch(r'[0-9]{17}', value):
        raise life.ReviewRequired('Malformed activity timestamp')
    return EPOCH + timedelta(microseconds=int(value) // 10)


def read_history(monday, request, until):
    histories = {}
    start, end = utc_date(request['from']), utc_date(until)
    for board_id in (life.SUBITEM_BOARD_ID, life.PARENT_BOARD_ID):
        rows, seen, previous = [], set(), None
        for page in range(1, MAX_PAGES + 1):
            response = monday.execute_query(ACTIVITY_QUERY, dict(board=board_id,
                items=[request['item_id'], request['parent_id']],
                **{'from': request['from'], 'to': until}, page=page, limit=PAGE_SIZE))
            data = response.get('data')
            if response.get('errors') or not isinstance(data, dict):
                raise life.ReviewRequired('Activity API failed; partial history cannot authorize deletion')
            boards = data.get('boards')
            if (not isinstance(boards, list) or len(boards) != 1
                    or boards[0].get('id') != board_id or boards[0].get('state') != 'active'):
                raise life.ReviewRequired('Expected activity board is unavailable or inactive')
            batch = boards[0].get('activity_logs')
            if not isinstance(batch, list) or len(batch) > PAGE_SIZE:
                raise life.ReviewRequired('Incomplete activity page')
            for event in batch:
                if (not isinstance(event, dict) or not isinstance(event.get('id'), str)
                        or not event['id'] or event['id'] in seen
                        or not isinstance(event.get('event'), str) or not event['event']
                        or event.get('entity') not in {'pulse', 'board'}):
                    raise life.ReviewRequired('Malformed or repeated activity entry; history is not complete')
                timestamp = event_time(event)
                ticks = int(event['created_at'])
                if not start <= timestamp <= end or (previous is not None and ticks > previous):
                    raise life.ReviewRequired('Activity history window or pagination order is inconsistent')
                try:
                    payload = json.loads(event['data'])
                except (KeyError, TypeError, ValueError) as exc:
                    raise life.ReviewRequired('Unreadable activity data') from exc
                if not isinstance(payload, dict):
                    raise life.ReviewRequired('Unreadable activity data')
                ids = [str(payload[k]) for k in ('pulse_id', 'item_id') if k in payload]
                if (any(not re.fullmatch(r'[0-9]+', i) for i in ids) or len(set(ids)) > 1
                        or not any(references(payload, i) for i in (request['item_id'], request['parent_id']))):
                    raise life.ReviewRequired('Activity entry has an ambiguous or unexpected item identity')
                seen.add(event['id'])
                previous = ticks
                rows.append(event)
            if len(batch) < PAGE_SIZE:
                break
        else:
            raise life.ReviewRequired('Activity history exceeds the bounded page limit; no deletion')
        histories[board_id] = rows
    return histories


def references(value, item_id):
    if isinstance(value, dict):
        return any(references(v, item_id) for v in value.values())
    if isinstance(value, list):
        return any(references(v, item_id) for v in value)
    return str(value) == item_id


def require_deletion(request, histories):
    matches = [e for e in histories[life.SUBITEM_BOARD_ID] if e['id'] == request['log_id']]
    if len(matches) != 1:
        raise life.ReviewRequired('Exact deletion event was not returned in fresh activity history')
    deletion = matches[0]
    payload = json.loads(deletion['data'])
    if (deletion['event'] not in life.DELETE_EVENTS or deletion['entity'] != 'pulse'
            or str(payload.get('pulse_id')) != request['item_id']
            or str(payload.get('board_id')) != request['board_id']
            or str(payload.get('parent_item_id')) != request['parent_id']
            or str(payload.get('parent_board_id')) != life.PARENT_BOARD_ID
            or ('item_id' in payload and str(payload['item_id']) != request['item_id'])):
        raise life.ReviewRequired('Deletion event does not match the exact subitem, board and parent')
    if request.get('deletion_sha256') not in (None, fingerprint(deletion)):
        raise life.ReviewRequired('Deletion activity differs from the reviewed event')
    for board_id, events in histories.items():
        for event in events:
            if board_id == request['board_id'] and event['id'] == deletion['id']:
                continue
            # Any later action on this ID is ambiguous, including unrecognised
            # restore/move aliases. Tied timestamps fail closed as well.
            if (int(event['created_at']) >= int(deletion['created_at'])
                    and references(json.loads(event['data']), request['item_id'])):
                raise life.ReviewRequired('Later or simultaneous activity exists for this subitem; review required')
            if (int(event['created_at']) >= int(deletion['created_at'])
                    and references(json.loads(event['data']), request['parent_id'])
                    and event['event'] not in PARENT_METADATA_EVENTS):
                raise life.ReviewRequired('Later lifecycle or unfamiliar activity on the parent requires review')
    return deletion


def capture(monday, request):
    validate_request(request)
    before_items = current_membership(monday, request)
    until = datetime.now(timezone.utc).isoformat()
    histories = read_history(monday, request, until)
    deletion = require_deletion(request, histories)
    after_items = current_membership(monday, request)
    if before_items != after_items:
        raise life.ReviewRequired('Parent or membership changed during activity recovery checks')
    return dict(request={**request, 'deletion_sha256': fingerprint(deletion)},
                deletion_event=deletion, histories=histories, history_to=until,
                before_items=before_items, after_items=after_items,
                checked_at=datetime.now(timezone.utc).isoformat())


def require_proof(proof, job, before, items, *, verification=False):
    request = job['payload'].get('activity_recovery')
    validate_request(request)
    if (job['board_id'] != request['board_id'] or job['item_id'] != request['item_id']
            or job.get('parent_id') != request['parent_id'] or proof['request'] != request
            or request['item_id'] in items
            or 'deletion_sha256' not in request
            or fingerprint(proof['deletion_event']) != request['deletion_sha256']):
        raise life.ReviewRequired('Activity proof differs from the reviewed recovery')
    now = datetime.now(timezone.utc)
    if not timedelta(0) <= now - utc_date(proof['history_to']) <= MAX_PROOF_AGE:
        raise life.ReviewRequired('Activity proof is stale; capture again')
    if not verification:
        require_snapshot(request, before)
        if job['kind'] != 'delete' or before != job['payload'].get('reviewed_before'):
            raise life.ReviewRequired('Stored row changed since activity recovery staging; stage again')
