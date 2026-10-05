"""Fresh, read-only Monday comparison; guarded application of representable differences.

This is an update-only backfill, not a general Monday replica. Missing rows,
lifecycle changes and multi-source scalar relationships are reported explicitly.
Finance owns Monday values. The enquiry-only rule uses API-active children of
Open projects; other financial values have no business-status exclusion.
No name inference, formula repair or financial deduplication is performed here.
"""
from __future__ import annotations

import argparse
from collections import Counter
from copy import deepcopy
import csv
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from email.utils import parsedate_to_datetime
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import time
from uuid import uuid4

import psycopg
from psycopg.types.json import Jsonb
import requests

from scripts import backfill_order_values as backfill
from scripts import order_value_scopes as scopes
from scripts import reconcile_order_values as reconcile
from src.config import PARENT_COLUMNS, SUBITEM_COLUMNS, HIDDEN_ITEMS_COLUMNS

LOG = logging.getLogger(__name__)
WORKFLOW = 'monday-field-comparison-v1'
DEFAULT_REPORT = Path('outputs/order_value_backfill/blocked_review_20261002/projects.csv')
MONEY_FIELDS = ('cust_order_value_material', 'cust_additional_charges', 'quote_amount', 'amount_invoiced')
DATE_FIELDS = ('invoice_date', 'date_order_received')
PARENT_FIELDS = ('project_name', 'pipeline_stage', 'total_order_value', 'new_enq_value_mirror')
SOURCE_FIELDS = (*MONEY_FIELDS, *DATE_FIELDS, 'status')
CHILD_FIELDS = ('hidden_item_id', 'quote_amount', 'amount_invoiced', *DATE_FIELDS, 'order_status')
ENQUIRY_RULE = ('SUM of current API-active subitem New Enquiry Values for parent '
                "status_category='Open'; preserve Won/Lost parent values")
BOARDS = {'projects': backfill.PARENT_BOARD_ID, 'subitems': backfill.SUBITEM_BOARD_ID,
          'hidden_items': backfill.HIDDEN_ITEMS_BOARD_ID}
READ_BATCH_SIZE = 10
READ_ATTEMPTS = 3
READ_DELAYS = (2, 5)
MAX_RETRY_WAIT = 30
MAX_BATCH_REQUESTS = 48
SERVER_ERROR_CODES = {'INTERNAL_SERVER_ERROR', 'DOWNSTREAM_SERVICE_ERROR'}
THROTTLE_CODES = {'COMPLEXITY_BUDGET_EXHAUSTED', 'IP_RATE_LIMIT_EXCEEDED',
                  'maxConcurrencyExceeded', 'RATE_LIMIT_EXCEEDED'}


class MondayReadError(ValueError):
    """Sanitized structured error; never accept data from a failed response."""

    def __init__(self, codes, *, retryable=False, splittable=False, retry_after=0, request_id=None):
        self.codes = tuple(sorted(set(codes)))
        self.retryable = retryable
        self.splittable = splittable
        self.retry_after = retry_after
        self.request_id = request_id
        safe_id = re.sub(r'[^A-Za-z0-9_.:-]', '', str(request_id))[:128] if request_id else None
        message = ', '.join(self.codes)
        if safe_id:
            message += f'; request_id={safe_id}'
        if retry_after:
            message += f'; retry_after={retry_after:g}s'
        super().__init__(message)


def retry_delay(body, headers):
    """Honor numeric/date Retry-After and structured API retry hints."""
    values = [headers.get('Retry-After'), body.get('retry_in_seconds')]
    containers = [body.get('extensions') or {}]
    containers.extend(e.get('extensions') or {} for e in body.get('errors', []) if isinstance(e, dict))
    for entry in containers:
        values.append(entry.get('retry_in_seconds'))
        nested = entry.get('error_data')
        if isinstance(nested, dict):
            values.append(nested.get('retry_in_seconds'))
    delays = []
    for value in values:
        if value is None:
            continue
        try:
            delay = float(value)
        except (TypeError, ValueError):
            try:
                delay = (parsedate_to_datetime(str(value)) - datetime.now(timezone.utc)).total_seconds()
            except (TypeError, ValueError, OverflowError):
                continue
        if delay >= 0:
            delays.append(delay)
    return max(delays, default=0)


def response_error(body, *, status=200, headers=None):
    """Retry only classified transient errors; permanent or mixed errors stop."""
    headers = headers or {}
    errors = body.get('errors')
    if not errors and 200 <= status < 300:
        return None
    codes, transient, split = [], [], []
    throttled = status == 429
    for error in errors if isinstance(errors, list) else []:
        ext = error.get('extensions') or {} if isinstance(error, dict) else {}
        code = str(ext.get('code') or 'UNCLASSIFIED_GRAPHQL_ERROR')
        code = re.sub(r'[^A-Za-z0-9_]', '', code)[:100]
        codes.append(code)
        try:
            error_status = int(ext.get('status_code', 0))
        except (TypeError, ValueError):
            error_status = 0
        limited = code in THROTTLE_CODES or error_status == 429
        server = code in SERVER_ERROR_CODES or 500 <= error_status < 600
        throttled = throttled or limited
        transient.append(server or limited)
        split.append(server)
    if status >= 400:
        codes.append(f'HTTP_{status}')
        transient.append(status == 429 or 500 <= status < 600)
        split.append(500 <= status < 600)
    if not codes:
        codes = ['MALFORMED_GRAPHQL_ERROR']
    request_id = (body.get('extensions') or {}).get('request_id') or headers.get('X-Request-ID')
    return MondayReadError(codes, retryable=bool(transient) and all(transient),
        splittable=bool(split) and all(split) and not throttled,
        retry_after=retry_delay(body, headers), request_id=request_id)


class ComparisonMondayClient(backfill.MondayClient):
    """Read-only comparison transport that preserves GraphQL error metadata.

    The shared production client raises a generic Exception before callers can
    classify GraphQL failures. Keep this adapter local to the comparison workflow
    so existing production writers and previously staged workflows are unchanged.
    Retries are owned solely by read_batch below, including HTTP-level failures.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        adapter = requests.adapters.HTTPAdapter(max_retries=0)
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)

    def execute_query(self, query, variables=None):
        if not query.lstrip().startswith('query CompareMonday('):
            raise ValueError('Comparison transport only accepts the exact-ID read query')
        return self._execute_read(query, variables)

    def _execute_read(self, query, variables=None):
        """Shared transport; callers validate their narrowly allowed read query."""
        with self.session.post(self.api_url, json={'query': query, 'variables': variables or {}},
                               headers=self.headers, timeout=(10, 45)) as response:
            try:
                body = response.json()
            except ValueError:
                body = {}
            if not isinstance(body, dict):
                body = {}
            error = response_error(body, status=response.status_code, headers=response.headers)
            if error:
                raise error
            if not isinstance(body.get('data'), dict):
                raise ValueError('Monday returned no valid GraphQL data')
            return body


def read_batch(monday, query, batch, columns, label, budget):
    """Bounded retry, then split only server/timeout failures into smaller reads.

    Never skip failed IDs, consume partial GraphQL data, or split a throttled or
    permanent-error response. A failing singleton aborts the entire capture.
    """
    last_error = None
    for attempt in range(1, READ_ATTEMPTS + 1):
        if budget[0] >= MAX_BATCH_REQUESTS:
            raise ValueError(f'Monday {label} request budget exhausted; staging cannot use partial evidence')
        budget[0] += 1
        try:
            response = monday.execute_query(query, {'ids': batch, 'columns': columns})
            if not isinstance(response, dict):
                raise ValueError('Invalid GraphQL response')
            error = response_error(response)
            if error:
                raise error
            if not isinstance(response.get('data'), dict) or not isinstance(response['data'].get('items'), list):
                raise ValueError('Incomplete GraphQL response; no partial data accepted')
            if set(backfill.indexed(response['data']['items'], 'id')) - set(batch):
                raise ValueError('Unexpected Monday IDs in requested batch')
            return response
        except requests.exceptions.SSLError:
            raise  # Certificate failures require investigation, never retries.
        except (requests.Timeout, requests.ConnectionError) as exc:
            last_error = MondayReadError([type(exc).__name__], retryable=True,
                                          splittable=isinstance(exc, requests.Timeout))
        except MondayReadError as exc:
            last_error = exc
        if not last_error.retryable:
            raise ValueError(f'Incomplete GraphQL response for {label} IDs {batch}: {last_error}; no partial data accepted') from None
        if last_error.retry_after > MAX_RETRY_WAIT:
            raise ValueError(f'Monday {label} requests a longer cooldown: {last_error}. '
                             'Wait for that cooldown before retrying Stage; no early retry was sent') from None
        if attempt < READ_ATTEMPTS:
            delay = max(READ_DELAYS[attempt - 1], last_error.retry_after)
            LOG.warning('Monday %s read failed (%s), %d IDs, attempt %d/%d; retrying in %gs',
                        label, last_error, len(batch), attempt, READ_ATTEMPTS, delay)
            time.sleep(delay)
    if last_error.splittable and len(batch) > 1:
        # A server may ask for a delay even on the last failed attempt. Splitting
        # is still a retry and must respect that hint.
        if last_error.retry_after:
            time.sleep(last_error.retry_after)
        middle = len(batch) // 2
        LOG.warning('Monday %s still failing (%s); reducing this batch from %d to %d/%d IDs',
                    label, last_error, len(batch), middle, len(batch) - middle)
        left = read_batch(monday, query, batch[:middle], columns, label, budget)
        right = read_batch(monday, query, batch[middle:], columns, label, budget)
        # Validate both halves independently before combining. An incomplete
        # response remains fatal and cannot be mistaken for an absent item.
        for response in (left, right):
            if not isinstance(response.get('data'), dict) or not isinstance(response['data'].get('items'), list):
                raise ValueError('Incomplete GraphQL response in split batch')
        return {'data': {'items': left['data']['items'] + right['data']['items']}}
    raise ValueError(f'Monday {label} read failed for IDs {batch} after {READ_ATTEMPTS} attempts: '
                     f'{last_error}. No partial data accepted') from None


def now():
    return datetime.now(timezone.utc).isoformat()


def code_fingerprint():
    return hashlib.sha256(scopes.code_fingerprint().encode() + Path(__file__).read_bytes()).hexdigest()


def project_ids_from_report(path):
    """Accept both historical action and newer status report formats, by Monday ID."""
    with path.open(encoding='utf-8-sig', newline='') as stream:
        reader = csv.DictReader(stream)
        fields = set(reader.fieldnames or [])
        status = 'status' if 'status' in fields else 'action'
        key = 'project_id' if 'project_id' in fields else 'monday_id'
        if not {status, key} <= fields:
            raise ValueError('Report requires project_id/monday_id and status/action columns')
        ids = [r[key].strip() for r in reader if r[status] == 'manual_review']
    validate_ids(ids)
    return sorted(ids)


def validate_ids(ids):
    if (not ids or len(ids) != len(set(ids))
            or any(not isinstance(i, str) or not i.isascii() or not i.isdigit() for i in ids)):
        raise ValueError('Select nonempty, unique numeric Monday IDs (not project names)')


def value_selection(depth=3):
    # Mirror display_value is a comma-separated summary, NOT a numeric API value.
    # Read typed sources recursively; a truncated/unavailable value is deferred.
    basic = '''__typename
        ... on NumbersValue { number }
        ... on FormulaValue { display_value }
        ... on DateValue { date }
        ... on StatusValue { label }
        ... on TextValue { text }'''
    if depth:
        basic += ''' ... on MirrorValue { display_value
            column { settings_str }
            mirrored_items { linked_item { id } linked_board_id
                mirrored_value { ''' + value_selection(depth - 1) + ' } } }'
    return basic


def flat_value_selection():
    """Read mirror references; read their actual values separately by exact ID.

    Monday's nested MirrorValue resolver returned INTERNAL_SERVER_ERROR on the
    live parent 1770216092. Direct item reads and mirror linked IDs succeeded.
    Do not request mirrored_value or treat display_value as a single number.
    """
    return value_selection(0) + ''' ... on MirrorValue {
        display_value column { settings_str }
        mirrored_items { linked_item { id } linked_board_id }
    }'''


def fetch_items(monday, ids, columns, *, parents=False, mirror_depth=None):
    result = {}
    # The legacy mirror_depth=0 argument denotes direct hidden-source reads.
    # All other reads retrieve references only, never nested mirrored_value.
    references = mirror_depth != 0
    selection = flat_value_selection() if references else value_selection(0)
    query = '''query CompareMonday($ids: [ID!]!, $columns: [String!]!) {
        items(ids: $ids, limit: 100, exclude_nonactive: false) {
            id name state updated_at board { id } parent_item { id }
            CHILDREN
            column_values(ids: $columns) { id type text value
                ... on BoardRelationValue { linked_item_ids }
                VALUES
            }
        }
    }'''.replace('VALUES', selection).replace('CHILDREN',
        'subitems { id state board { id } parent_item { id } }' if parents else '')
    if not columns:
        # Monday treats ids: [] as ALL columns. Metadata-only lifecycle reads
        # must not include volatile, unrelated formula/mirror payloads.
        query = query.replace('column_values(ids: $columns)',
                              'column_values(ids: $columns) @skip(if: true)')
    ids = sorted(set(ids))
    label = 'parents' if parents else 'subitems' if references else 'hidden sources'
    for offset in range(0, len(ids), READ_BATCH_SIZE):
        batch = ids[offset:offset + READ_BATCH_SIZE]
        LOG.info('Reading Monday %s: IDs %d-%d/%d, %d columns, direct values%s',
                 label, offset + 1, offset + len(batch), len(ids), len(set(columns)),
                 ' and mirror references' if references else '')
        response = read_batch(monday, query, batch, sorted(set(columns)), label, [0])
        data = response.get('data')
        if response.get('errors') or not isinstance(data, dict) or not isinstance(data.get('items'), list):
            raise ValueError('Incomplete GraphQL response; no partial data may be staged')
        rows = backfill.indexed(data['items'], 'id')
        if set(rows) - set(batch):
            raise ValueError('Unexpected Monday item IDs')
        for item in rows.values():
            if not columns:
                item['column_values'] = []  # Deliberately not requested above.
            if not {'name', 'state', 'updated_at', 'board', 'parent_item', 'column_values'} <= item.keys():
                raise ValueError('Incomplete Monday item metadata')
            backfill.indexed(item['column_values'], 'id')
            if parents:
                if not isinstance(item.get('subitems'), list):
                    raise ValueError('Missing complete parent membership')
                backfill.indexed(item['subitems'], 'id')
                item['subitems'].sort(key=lambda r: r['id'])
            item['column_values'].sort(key=lambda r: r['id'])
        result.update({i: canonical_source(r) for i, r in rows.items()})
        LOG.info('Monday exact-ID read: %d/%d', offset + len(batch), len(ids))
    return result


def canonical_source(value):
    if isinstance(value, dict):
        return {k: canonical_source(v) for k, v in value.items()}
    if isinstance(value, list):
        rows = [canonical_source(v) for v in value]
        # GraphQL does not promise relation/mirror contribution order. Values
        # remain attached to their IDs; only presentation order is discarded.
        if rows and all(isinstance(r, dict) and 'linked_item' in r for r in rows):
            return sorted(rows, key=lambda r: (str(r.get('linked_board_id')), str((r.get('linked_item') or {}).get('id'))))
        if rows and all(isinstance(r, str) for r in rows):
            return sorted(rows)
        return rows
    return value


def col(item, column_id):
    columns = backfill.indexed(item['column_values'], 'id')
    if column_id not in columns:
        raise ValueError(f'Column {column_id} not returned')
    return columns[column_id]


def links(item):
    value = col(item, SUBITEM_COLUMNS['hidden_item_id'])
    ids = value.get('linked_item_ids')
    if value.get('type') != 'board_relation' or not isinstance(ids, list):
        raise ValueError('Typed source links not returned')
    if len(ids) != len(set(ids)) or any(not isinstance(i, str) or not i.isdigit() for i in ids):
        raise ValueError('Invalid source link IDs')
    return sorted(ids)


def mirror_columns(value):
    """Use live column settings, never assume the mirror points to a formula."""
    settings = (value.get('column') or {}).get('settings_str')
    if isinstance(settings, str):
        settings = json.loads(settings)
    if not isinstance(settings, dict):
        raise ValueError('Mirror source settings unavailable')
    sources = settings.get('displayed_linked_columns')
    if isinstance(sources, list):
        sources = {str(r['board_id']): r['column_ids'] for r in sources}
    if not sources:
        old = settings.get('displayed_column') or {}
        sources = {str(board): [field] if isinstance(field, str) else field for board, field in old.items()}
    if not isinstance(sources, dict) or not sources:
        raise ValueError('Mirror has no explicit source-column mapping')
    result = {}
    for board, fields in sources.items():
        if not isinstance(fields, list) or len(fields) != 1 or not isinstance(fields[0], str) or not fields[0]:
            raise ValueError('Mirror requires exactly one configured column per source board')
        result[str(board)] = fields[0]
    return result


def mirror_dependencies(items, board_id):
    ids, columns = set(), set()
    for item in items:
        for value in item['column_values']:
            if value.get('__typename') != 'MirrorValue':
                continue
            try:
                mapping = mirror_columns(value)
            except (ValueError, KeyError, TypeError):
                continue  # Resolution reports this field as unreadable later.
            if board_id not in mapping:
                continue
            columns.add(mapping[board_id])
            for link in value.get('mirrored_items') or []:
                item_id = (link.get('linked_item') or {}).get('id')
                if (str(link.get('linked_board_id')) == board_id
                        and isinstance(item_id, str) and item_id.isascii() and item_id.isdigit()):
                    ids.add(item_id)
    return ids, columns


def resolved_col(evidence, item, column_id, trail=(), *, active_sources=False):
    """Join freshly captured exact-ID values; keep the original evidence raw.

    Each hop is proved by mirrored_items IDs, source board and column settings.
    Missing/unsupported references withhold the field, never become zero. Legacy
    inline typed evidence remains readable for offline fixtures and validation.
    """
    board = str((item.get('board') or {}).get('id'))
    key = (board, item['id'], column_id)
    if key in trail or len(trail) >= 6:
        raise ValueError('Cyclic or excessively deep mirror references')
    value = col(item, column_id)
    if value.get('__typename') != 'MirrorValue':
        return value
    contributions = value.get('mirrored_items')
    if not isinstance(contributions, list):
        raise ValueError('Mirror references unavailable')
    if not contributions or all('mirrored_value' in r for r in contributions):
        return value
    try:
        mapping = mirror_columns(value)
    except (KeyError, TypeError) as exc:
        raise ValueError('Invalid mirror source-column mapping') from exc
    tables = {board: table for table, board in BOARDS.items()}
    result = deepcopy(value)
    for reference in result['mirrored_items']:
        source_board = str(reference.get('linked_board_id'))
        source_id = (reference.get('linked_item') or {}).get('id')
        table = tables.get(source_board)
        source_item = evidence.get(table, {}).get(source_id) if table else None
        if (source_item is None or str((source_item.get('board') or {}).get('id')) != source_board
                or source_board not in mapping):
            raise ValueError(f'Mirror source {source_id} on board {source_board} is outside readable evidence')
        if active_sources and source_item.get('state') != 'active':
            raise ValueError(f'Current mirror source {source_id} is not API active; retain historical value pending link review')
        reference['mirrored_value'] = resolved_col(evidence, source_item, mapping[source_board], (*trail, key),
                                                  active_sources=active_sources)
    return result


def capture(monday, project_ids, extra_children=(), *, read_columns=None):
    """Only selected parents, their children, stored extras and explicit sources."""
    parent_columns = sorted({PARENT_COLUMNS[f] for f in PARENT_FIELDS})
    parents = fetch_items(monday, project_ids, parent_columns, parents=True)
    child_ids = set(extra_children)
    membership = {}
    for pid, parent in parents.items():
        for child in parent['subitems']:
            cid = child['id']
            if cid in membership or (child.get('parent_item') or {}).get('id') != pid:
                raise ValueError('Duplicate or inconsistent current parent membership')
            membership[cid] = pid
            child_ids.add(cid)
    mirrored_children, child_columns = mirror_dependencies(parents.values(), backfill.SUBITEM_BOARD_ID)
    child_ids.update(mirrored_children)
    child_columns.update(SUBITEM_COLUMNS[f] for f in CHILD_FIELDS)
    # Explicit enquiry rule uses each eligible child's own formula. This does
    # not claim the parent mirror's undocumented aggregation was verified.
    child_columns.add(SUBITEM_COLUMNS['new_enquiry_value'])
    child_columns.update((read_columns or {}).get('subitems', []))
    children = fetch_items(monday, child_ids, child_columns)
    source_ids = set()
    for cid, child in children.items():
        if cid in membership and (child.get('parent_item') or {}).get('id') != membership[cid]:
            raise ValueError('A child moved during capture; retry with fresh evidence')
        try:
            source_ids.update(links(child))
        except ValueError:
            pass  # Report the affected fields; never infer a link.
    mirrored_sources, source_columns = mirror_dependencies(children.values(), backfill.HIDDEN_ITEMS_BOARD_ID)
    source_ids.update(mirrored_sources)
    source_columns.update(HIDDEN_ITEMS_COLUMNS[f] for f in SOURCE_FIELDS)
    source_columns.update((read_columns or {}).get('hidden_items', []))
    sources = fetch_items(monday, source_ids, source_columns, mirror_depth=0)
    return {'projects': parents, 'subitems': children, 'hidden_items': sources,
            'project_ids': sorted(project_ids), 'extra_children': sorted(extra_children),
            'read_columns': {'projects': parent_columns, 'subitems': sorted(child_columns),
                             'hidden_items': sorted(source_columns)}}


def decimal_value(value):
    if value is None or value == '':
        return None
    # Formula display values must be unambiguous API decimal strings. Never turn
    # "2,104.88, 5,465.76" into one amount or silently accept an error as zero.
    if isinstance(value, bool) or not re.fullmatch(r'[+-]?\d+(?:\.\d+)?', str(value).strip()):
        raise ValueError('Non-numeric or formatted numeric source value')
    try:
        amount = Decimal(str(value).strip())
    except InvalidOperation as exc:
        raise ValueError('Invalid number') from exc
    if not amount.is_finite():
        raise ValueError('Non-finite number')
    return amount


def mirror_sources(value):
    rows = value.get('mirrored_items')
    if not isinstance(rows, list):
        raise ValueError('Typed mirror sources unavailable (or recursion limit reached)')
    ids = [(r.get('linked_item') or {}).get('id') for r in rows]
    if any(i is None for i in ids) or len(ids) != len(set(ids)):
        raise ValueError('Incomplete or duplicate mirror contributions')
    if any(r.get('mirrored_value') is None for r in rows):
        raise ValueError('A mirrored source is unreadable')
    if not rows and value.get('display_value') not in ('', None):
        raise ValueError('Mirror text exists but typed contributions are absent')
    return [r['mirrored_value'] for r in rows]


def numeric(value):
    kind = value.get('__typename')
    if kind == 'NumbersValue' and 'number' in value:
        return decimal_value(value['number'])
    if kind == 'FormulaValue' and 'display_value' in value:
        return decimal_value(value['display_value'])
    if kind == 'MirrorValue':
        settings = (value.get('column') or {}).get('settings_str')
        if isinstance(settings, str):
            settings = json.loads(settings)
        function = (settings or {}).get('function')
        values = [numeric(v) for v in mirror_sources(value)]
        # A single mirrored number is unambiguous; several require explicit SUM
        # configuration. Unknown/changed aggregation is a field-level deferral.
        if function is not None and str(function).lower() != 'sum':
            raise ValueError(f'Unsupported Monday numeric aggregation: {function}')
        if len(values) > 1 and function is None:
            raise ValueError('Multiple mirror values without confirmed SUM configuration')
        present = [v for v in values if v is not None]
        if not present and value.get('display_value') not in ('', None):
            raise ValueError('Mirror displays a value but all typed numeric contributions are blank')
        return sum(present, Decimal(0)) if present else None
    raise ValueError('Unsupported or incomplete numeric column')


def scalar(value, kind):
    if value.get('__typename') == 'MirrorValue':
        values = [scalar(v, kind) for v in mirror_sources(value)]
        unique = set(values)
        if len(unique) > 1:
            raise ValueError('Multiple distinct values cannot fit a scalar column')
        return values[0] if values else None
    field = {'date': 'date', 'text': 'text', 'status': 'label'}[kind]
    expected = {'date': 'DateValue', 'text': 'TextValue', 'status': 'StatusValue'}[kind]
    if value.get('__typename') != expected or field not in value:
        raise ValueError(f'Incomplete {kind} column')
    return value[field] or None


def project_enquiry_category(parent):
    """Match schema.sql's exact parent-project CASE, including NULL -> Open."""
    if ((parent.get('board') or {}).get('id') != BOARDS['projects']
            or parent.get('state') != 'active' or parent.get('parent_item') is not None):
        raise ValueError('New enquiry total requires a current active project')
    stage = scalar(col(parent, PARENT_COLUMNS['pipeline_stage']), 'status')
    return 'Won' if stage == 'Won - Closed (Invoiced)' else 'Lost' if stage == 'Lost' else 'Open'


def require_stored_enquiry_category(parent, stored):
    """The targeted refresh cannot silently repair a stale parent category."""
    category = project_enquiry_category(parent)
    if stored.get('status_category') != category:
        raise ValueError('Stored parent status_category differs from Monday; reconcile the parent stage first')
    return category


def project_new_enquiry_total(evidence, parent):
    """SUM current API-active children for Open parents; None means skip parent.

    User clarified the two filters on 2026-10-05. Business Status labels such
    as Archived do not exclude API-active children. Missing/unreadable evidence
    withholds the total; explicitly nonactive children do not contribute.
    Won/Lost parents retain their stored enquiry value, rather than being zeroed.
    """
    if project_enquiry_category(parent) != 'Open':
        return None
    members = parent.get('subitems')
    if not isinstance(members, list):
        raise ValueError('Current Monday child membership is unavailable')
    seen, total = set(), Decimal(0)
    for member in members:
        cid = member.get('id')
        if (not isinstance(cid, str) or not cid.isascii() or not cid.isdigit()
                or cid in seen or (member.get('parent_item') or {}).get('id') != parent['id']):
            raise ValueError('Duplicate or inconsistent current Monday child membership')
        seen.add(cid)
        child = evidence.get('subitems', {}).get(cid)
        if (not child or child.get('id') != cid
                or (child.get('board') or {}).get('id') != BOARDS['subitems']
                or (child.get('parent_item') or {}).get('id') != parent['id']):
            raise ValueError(f'Incomplete current Monday new enquiry evidence for child {cid}')
        if child.get('state') not in ('active', 'archived', 'deleted'):
            raise ValueError(f'Unknown Monday lifecycle state for child {cid}')
        if 'state' in member and member['state'] != child['state']:
            raise ValueError(f'Child lifecycle state changed during capture: {cid}')
        if child['state'] != 'active':
            continue
        amount = numeric(resolved_col(evidence, child, SUBITEM_COLUMNS['new_enquiry_value']))
        if amount is not None:
            total += amount
    return total


def capture_new_enquiry(monday, pid):
    """Read parent category, current membership and each child's formula."""
    parents = fetch_items(monday, [pid], [PARENT_COLUMNS['pipeline_stage']], parents=True)
    parent = parents.get(pid)
    if parent is None or not isinstance(parent.get('subitems'), list):
        raise ValueError('Current Monday project/membership is unavailable')
    ids = [c['id'] for c in parent['subitems']]
    if len(ids) != len(set(ids)) or len(ids) > scopes.MAX_SCOPE_ROWS:
        raise ValueError('Invalid or oversized current child membership')
    children = fetch_items(monday, ids, [SUBITEM_COLUMNS['new_enquiry_value']])
    evidence = dict(projects=parents, subitems=children, hidden_items={})
    project_new_enquiry_total(evidence, parent)
    return evidence


def project_projection(pid, evidence, before, contract, *, lifecycle=None):
    """Partial, explicit field proposals plus an exhaustive list of limitations."""
    proposed = {t: {} for t in scopes.TABLES}
    issues = []
    stored = {t: backfill.indexed(before[t]) for t in scopes.TABLES}

    def represented_archive(table, item_id):
        state = (lifecycle or {}).get(table, {}).get(item_id, {})
        return (item_id in stored[table] and state.get('monday_state') == 'archived'
                and state.get('blocked') is False)

    def source_col(item, column_id):
        return resolved_col(evidence, item, column_id, active_sources=lifecycle is not None)

    def issue(table, item_id, field, reason):
        issues.append({'project_id': pid, 'table': table, 'monday_id': item_id,
                       'field': field, 'reason': reason})

    def put(table, item_id, field, getter):
        if item_id not in stored[table]:
            issue(table, item_id, field, 'Missing Supabase row: requires rehydration')
            return
        if field not in contract[table]:
            issue(table, item_id, field, 'No corresponding Supabase column')
            return
        try:
            value = getter()
            row = reconcile.normalize_updates({table: [{'monday_id': item_id, field: value}]}, contract)[table][0]
        except (ValueError, InvalidOperation) as exc:
            issue(table, item_id, field, str(exc))
            return
        proposed[table].setdefault(item_id, {'monday_id': item_id})[field] = row[field]

    def valid(table, item_id):
        item = evidence[table].get(item_id)
        if item is None:
            issue(table, item_id, '*', 'Not returned by Monday; absence is not deletion evidence')
            return None
        if (item.get('board') or {}).get('id') != BOARDS[table]:
            issue(table, item_id, '*', 'Item moved to a different board')
            return None
        if item.get('state') not in ('active', 'archived', 'deleted'):
            issue(table, item_id, '*', 'Unknown lifecycle state')
            return None
        if item['state'] != 'active':
            if item['state'] == 'archived' and represented_archive(table, item_id):
                return None
            issue(table, item_id, 'monday_state',
                  f"Monday state={item['state']}; lifecycle storage/retirement requires separate review")
            return None
        return item

    parent = valid('projects', pid)
    if parent is None:
        return proposed, issues
    if parent.get('parent_item') is not None:
        issue('projects', pid, '*', 'Selected item is now a subitem')
        return proposed, issues
    put('projects', pid, 'item_name', lambda: parent['name'])
    for field, kind in [('project_name', 'text'), ('pipeline_stage', 'status')]:
        put('projects', pid, field, lambda f=field, k=kind: scalar(source_col(parent, PARENT_COLUMNS[f]), k))
    put('projects', pid, 'total_order_value', lambda: numeric(source_col(parent, PARENT_COLUMNS['total_order_value'])))
    try:
        enquiry_total = project_new_enquiry_total(evidence, parent)
    except (ValueError, InvalidOperation) as exc:
        issue('projects', pid, 'new_enquiry_value', str(exc))
    else:
        if enquiry_total is not None:
            put('projects', pid, 'new_enquiry_value', lambda: enquiry_total)
    current_ids = {r['id'] for r in parent['subitems']}
    if lifecycle is not None:
        current_ids = {cid for cid in current_ids
                       if not (evidence['subitems'].get(cid, {}).get('state') == 'archived'
                           and (evidence['subitems'][cid].get('board') or {}).get('id') == BOARDS['subitems'])}
    for row in before['subitems']:
        if row.get('parent_monday_id') == pid and row['monday_id'] not in current_ids:
            observed = evidence['subitems'].get(row['monday_id'])
            if (observed and observed.get('state') == 'archived'
                    and (observed.get('board') or {}).get('id') == BOARDS['subitems']
                    and represented_archive('subitems', row['monday_id'])):
                continue
            detail = f"state={observed.get('state')}, parent={(observed.get('parent_item') or {}).get('id')}" if observed else 'not returned'
            issue('subitems', row['monday_id'], 'parent_monday_id',
                  f'Stored child absent from current parent membership ({detail}); retain pending lifecycle review')
    invoices, seen_hidden = [], set()
    invoice_complete = True
    for cid in sorted(current_ids):
        child = valid('subitems', cid)
        if child is None:
            invoice_complete = False
            continue
        put('subitems', cid, 'item_name', lambda: child['name'])
        old_parent = stored['subitems'].get(cid, {}).get('parent_monday_id')
        if old_parent not in (None, pid):
            issue('subitems', cid, 'parent_monday_id', f'Reparenting also requires comparison of old parent {old_parent}')
            # Leave the entire row together until both parents can be reconciled.
            proposed['subitems'].pop(cid, None)
            invoice_complete = False
            continue
        else:
            put('subitems', cid, 'parent_monday_id', lambda: pid)
        for field in ('quote_amount', 'amount_invoiced'):
            put('subitems', cid, field, lambda f=field: numeric(source_col(child, SUBITEM_COLUMNS[f])))
        for field in DATE_FIELDS:
            put('subitems', cid, field, lambda f=field: scalar(source_col(child, SUBITEM_COLUMNS[f]), 'date'))
        put('subitems', cid, 'order_status', lambda: scalar(source_col(child, SUBITEM_COLUMNS['order_status']), 'status'))
        try:
            invoices.append(numeric(source_col(child, SUBITEM_COLUMNS['amount_invoiced'])))
        except ValueError:
            invoice_complete = False
        try:
            source_ids = links(child)
        except ValueError as exc:
            issue('subitems', cid, 'hidden_item_id', str(exc))
            continue
        if len(source_ids) != 1:
            issue('subitems', cid, 'hidden_item_id',
                  f'Expected one representable source, Monday has {len(source_ids)}: {source_ids}; no guessed link or amount')
            continue
        hid = source_ids[0]
        source = valid('hidden_items', hid)
        if source is None:
            continue
        if hid in stored['hidden_items']:
            put('subitems', cid, 'hidden_item_id', lambda: hid)
        else:
            issue('subitems', cid, 'hidden_item_id', 'Missing referenced source row: requires rehydration')
        for field in backfill.ORDER_FIELDS:
            put('subitems', cid, field, lambda f=field: numeric(source_col(source, HIDDEN_ITEMS_COLUMNS[f])))
        if hid in seen_hidden:
            continue  # Write source once; NEVER drop either child or its contribution.
        seen_hidden.add(hid)
        for owner in before['subitems']:
            if owner.get('hidden_item_id') == hid and owner.get('parent_monday_id') not in evidence['project_ids']:
                issue('subitems', owner['monday_id'], 'hidden_item_id',
                      f'Source also has stored owner outside selection (parent={owner.get("parent_monday_id")}); compare that parent separately')
        put('hidden_items', hid, 'item_name', lambda: source['name'])
        for field in MONEY_FIELDS:
            put('hidden_items', hid, field, lambda f=field: numeric(source_col(source, HIDDEN_ITEMS_COLUMNS[f])))
        for field in DATE_FIELDS:
            put('hidden_items', hid, field, lambda f=field: scalar(source_col(source, HIDDEN_ITEMS_COLUMNS[f]), 'date'))
        put('hidden_items', hid, 'status', lambda: scalar(source_col(source, HIDDEN_ITEMS_COLUMNS['status']), 'status'))
    # Supabase's invoice total is derived (no mapped parent invoice column).
    # Derive only from the complete CURRENT Monday child list, never stored extras.
    if invoice_complete:
        present = [v for v in invoices if v is not None]
        put('projects', pid, 'total_amount_invoiced', lambda: sum(present, Decimal(0)) if present else None)
    else:
        issue('projects', pid, 'total_amount_invoiced', 'Incomplete current Monday invoice evidence')
    return proposed, issues


def build_record(project_ids, evidence, before, contract, boundary, *, lifecycle=None):
    proposed = {t: {} for t in scopes.TABLES}
    issues = []
    for pid in project_ids:
        rows, problems = project_projection(pid, evidence, before, contract, lifecycle=lifecycle)
        issues.extend(problems)
        for table in scopes.TABLES:
            for item_id, row in rows[table].items():
                previous = proposed[table].get(item_id)
                if previous is not None and previous != row:
                    raise ValueError('Conflicting projections of a shared row')
                proposed[table][item_id] = row
    updates = {t: [] for t in scopes.TABLES}
    for table in scopes.TABLES:
        old = backfill.indexed(before[table])
        for item_id, row in sorted(proposed[table].items()):
            changed = {f: v for f, v in row.items() if f != 'monday_id' and old[item_id].get(f) != v}
            if changed:
                updates[table].append({'monday_id': item_id, **changed})
    return {**({'lifecycle': lifecycle} if lifecycle is not None else {}),
            'scope_id': 'compare-' + backfill.fingerprint(project_ids)[:16],
            'project_ids': project_ids, 'boundary': boundary, 'source': evidence,
            'before': before, 'after': scopes.expected_state(before, updates),
            'updates': updates, 'issues': issues}


def boundary_for(project_ids, evidence, before):
    children = {c['id'] for pid in project_ids for c in evidence['projects'].get(pid, {}).get('subitems', [])}
    children.update(mirror_dependencies([evidence['projects'][pid] for pid in project_ids
                                         if pid in evidence['projects']], backfill.SUBITEM_BOARD_ID)[0])
    children.update(r['monday_id'] for r in before['subitems'] if r.get('parent_monday_id') in project_ids)
    sources = {r.get('hidden_item_id') for r in before['subitems'] if r['monday_id'] in children}
    for cid in children:
        if cid in evidence['subitems']:
            try:
                sources.update(links(evidence['subitems'][cid]))
            except ValueError:
                pass
    sources.update(mirror_dependencies([evidence['subitems'][cid] for cid in children
                                        if cid in evidence['subitems']], backfill.HIDDEN_ITEMS_BOARD_ID)[0])
    return {'projects': sorted(project_ids), 'subitems': sorted(children), 'hidden_items': sorted(sources - {None})}


def stage_run(connection, monday, run_dir, project_ids, *, selection=None):
    from src.services import monday_archive as archive
    validate_ids(project_ids)
    if run_dir.exists():
        raise ValueError('Use a new run directory to preserve reviewed artifacts')
    started = now()
    empty = {'projects': sorted(project_ids), 'subitems': [], 'hidden_items': []}
    with connection.transaction():
        connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
        safety = scopes.schema_safety(connection)
        discovery = scopes.read_boundary(connection, empty, full=True)
    extra = [r['monday_id'] for r in discovery['subitems']]
    evidence = capture(monday, project_ids, extra)
    boundary = boundary_for(project_ids, evidence, discovery)
    with connection.transaction():
        connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
        contract = reconcile.read_contract(connection)
        baseline = scopes.read_boundary(connection, boundary, full=True)
        lifecycle = archive.read_states(connection, boundary) if archive.enabled() else None
        if scopes.read_boundary(connection, empty, full=True) != discovery:
            raise ValueError('Database membership/values changed during capture; stage again')
    # Join dependencies using the SQL read boundary, including incoming owners.
    # A shared source is allowed, but two transactions must never review/write
    # overlapping SQL rows against different before states.
    groups = []
    for pid in sorted(project_ids):
        b = boundary_for([pid], evidence, baseline)
        state = scopes.select_boundary(scopes.index_baseline(baseline), b)
        keys = {(t, r['monday_id']) for t in scopes.TABLES for r in state[t]}
        keys.update((t, i) for t in scopes.TABLES for i in b[t])
        group = {'ids': [pid], 'keys': keys}
        overlap = [g for g in groups if g['keys'] & keys]
        for previous in overlap:
            group['ids'].extend(previous['ids'])
            group['keys'].update(previous['keys'])
            groups.remove(previous)
        group['ids'].sort()
        groups.append(group)
    # Pack independent dependency groups without changing the reviewed boundary.
    # This amortizes exact-ID API calls while retaining the 25/500 transaction cap.
    packed = []
    index = scopes.index_baseline(baseline)
    for group in sorted(groups, key=lambda g: g['ids']):
        if packed:
            candidate = sorted(packed[-1]['ids'] + group['ids'])
            candidate_boundary = boundary_for(candidate, evidence, baseline)
            candidate_state = scopes.select_boundary(index, candidate_boundary)
            if len(candidate) <= scopes.MAX_PROJECTS and sum(map(len, candidate_state.values())) <= scopes.MAX_SCOPE_ROWS:
                packed[-1] = {'ids': candidate, 'keys': packed[-1]['keys'] | group['keys']}
                continue
        packed.append(group)
    records, deferred = [], []
    for group in packed:
        ids = group['ids']
        b = boundary_for(ids, evidence, baseline)
        state = scopes.select_boundary(scopes.index_baseline(baseline), b)
        if len(ids) > scopes.MAX_PROJECTS or sum(map(len, state.values())) > scopes.MAX_SCOPE_ROWS:
            deferred.append({'project_ids': ids, 'reason': 'Connected dependency exceeds bounded transaction limits'})
            continue
        local = {t: {i: row for i, row in evidence[t].items() if i in b[t]} for t in scopes.TABLES}
        local.update(project_ids=ids, extra_children=sorted(set(b['subitems']) & set(extra)))
        if 'read_columns' in evidence:
            # Stage fetches a union of source columns for all selected projects.
            # Keep that request contract when rereading one scope; otherwise a
            # narrower per-scope column set could falsely look like source drift.
            local['read_columns'] = evidence['read_columns']
        local_states = ({t: {i: r for i, r in lifecycle[t].items() if i in b[t]} for t in scopes.TABLES}
                        if lifecycle is not None else None)
        records.append(build_record(ids, local, state, contract, b, lifecycle=local_states))
    staged = {'selected_project_ids': sorted(project_ids), 'selection': selection, 'contract': contract,
              'started_at': started, 'finished_at': now(), 'scopes': records, 'deferred': deferred}
    run_dir.mkdir(parents=True, exist_ok=False)
    backfill.write_json(run_dir / 'comparison.json', staged)
    changes, projects, issues = [], [], []
    for record in records:
        issues.extend(record['issues'])
        for table, rows in record['updates'].items():
            old = backfill.indexed(record['before'][table])
            for row in rows:
                changes.extend({'scope_id': record['scope_id'], 'table': table, 'monday_id': row['monday_id'],
                    'field': f, 'before': old[row['monday_id']].get(f), 'after': v}
                    for f, v in row.items() if f != 'monday_id')
        for pid in record['project_ids']:
            parent = record['source']['projects'].get(pid, {})
            projects.append({'project_id': pid, 'item_name': parent.get('name'), 'monday_state': parent.get('state'),
                'scope_id': record['scope_id'], 'scope_has_changes': any(record['updates'].values()),
                'unresolved_fields': sum(i['project_id'] == pid for i in record['issues'])})
    reconcile.write_csv(run_dir / 'changes.csv', changes, ['scope_id', 'table', 'monday_id', 'field', 'before', 'after'])
    reconcile.write_csv(run_dir / 'projects.csv', projects,
        ['project_id', 'item_name', 'monday_state', 'scope_id', 'scope_has_changes', 'unresolved_fields'])
    reconcile.write_csv(run_dir / 'unresolved.csv', issues, ['project_id', 'table', 'monday_id', 'field', 'reason'])
    manifest = {'workflow': WORKFLOW, 'run_id': str(uuid4()), 'prepared_at': now(),
        'target': backfill.target_fingerprint(connection), 'code': code_fingerprint(),
        'sha256': backfill.fingerprint(staged), 'safety': safety, 'selected_projects': len(project_ids),
        'scopes_with_changes': sum(any(r['updates'].values()) for r in records),
        'scopes_without_changes': sum(not any(r['updates'].values()) for r in records),
        'changes': len(changes), 'unresolved_fields': len(issues), 'deferred_scopes': len(deferred),
        'review_hashes': {name: hashlib.sha256((run_dir / name).read_bytes()).hexdigest()
                          for name in ('changes.csv', 'projects.csv', 'unresolved.csv')}}
    backfill.write_json(run_dir / 'manifest.json', manifest)
    load_run(run_dir)
    return manifest


def load_run(run_dir):
    manifest = json.loads((run_dir / 'manifest.json').read_text(encoding='utf-8'))
    staged = json.loads((run_dir / 'comparison.json').read_text(encoding='utf-8'))
    if (manifest['workflow'] != WORKFLOW or manifest['code'] != code_fingerprint()
            or manifest['sha256'] != backfill.fingerprint(staged)
            or set(manifest['review_hashes']) != {'changes.csv', 'projects.csv', 'unresolved.csv'}
            or any(hashlib.sha256((run_dir / n).read_bytes()).hexdigest() != h for n, h in manifest['review_hashes'].items())):
        raise ValueError('Code or reviewed evidence changed; stage a new run')
    covered, seen = [], set()
    for record in staged['scopes']:
        covered.extend(record['project_ids'])
        keys = {(t, i) for t in scopes.TABLES for i in record['boundary'][t]}
        keys.update((t, r['monday_id']) for t in scopes.TABLES for r in record['before'][t])
        if seen & keys:
            raise ValueError('Overlapping SQL dependencies in staged scopes')
        seen.update(keys)
        if (len(record['project_ids']) > scopes.MAX_PROJECTS
                or sum(map(len, record['before'].values())) > scopes.MAX_SCOPE_ROWS
                or build_record(record['project_ids'], record['source'], record['before'],
                                staged['contract'], record['boundary'], lifecycle=record.get('lifecycle')) != record):
            raise ValueError('Changes do not match reviewed Monday evidence')
    covered.extend(pid for r in staged['deferred'] for pid in r['project_ids'])
    if sorted(covered) != staged['selected_project_ids'] or len(covered) != len(set(covered)):
        raise ValueError('Incomplete or duplicated project coverage')
    return manifest, staged


def check_source(monday, record):
    options = {'read_columns': record['source']['read_columns']} if 'read_columns' in record['source'] else {}
    live = capture(monday, record['project_ids'], record['source']['extra_children'], **options)
    if live != record['source']:
        raise scopes.ScopeConflict('Monday changed since staging; compare again, do not overwrite Finance edits')


def commit_scope(connection, manifest, staged, record):
    with connection.transaction():
        connection.execute('SET TRANSACTION ISOLATION LEVEL READ COMMITTED')
        connection.execute("SET LOCAL lock_timeout='750ms'")
        connection.execute("SET LOCAL statement_timeout='4s'")
        connection.execute("SET LOCAL transaction_timeout='10s'")
        connection.execute("SET LOCAL idle_in_transaction_session_timeout='5s'")
        connection.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))',
                           (manifest['run_id'] + ':' + record['scope_id'],))
        if record['scope_id'] in scopes.committed_scopes(connection, manifest):
            return 'already_committed'
        if not any(record['updates'].values()):
            raise ValueError('No-op scopes must not be committed')
        # Bounded protection of absent IDs, relinks and external incoming owners.
        # No HTTP calls or table traversal occurs inside the transaction.
        connection.execute('LOCK TABLE public.projects, public.hidden_items, public.subitems IN SHARE ROW EXCLUSIVE MODE')
        scopes.schema_safety(connection)
        if reconcile.read_contract(connection) != staged['contract']:
            raise scopes.ScopeConflict('Database schema changed since staging')
        current = scopes.read_boundary(connection, record['boundary'], full=True)
        if 'lifecycle' in record:
            from src.services import monday_archive as archive
            if archive.read_states(connection, record['boundary']) != record['lifecycle']:
                raise scopes.ScopeConflict('Lifecycle states changed since staging')
            connection.execute("SELECT set_config('datacube.archive_worker_protocol','verified_archive_v1',true)")
        if current != record['before']:
            raise scopes.ScopeConflict('Database rows changed since staging')
        counts = scopes.write_updates(connection, record['updates'])
        actual = scopes.read_boundary(connection, record['boundary'], full=True)
        if actual != record['after']:
            raise scopes.ScopeConflict('Post-write values differ from reviewed result')
        connection.execute('INSERT INTO public.order_value_scope_commits '
            '(run_id,scope_id,plan_sha256,mode,project_ids,before_sha256,after_sha256,updated_rows) '
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s)',
            (manifest['run_id'], record['scope_id'], manifest['sha256'], 'repair', record['project_ids'],
             backfill.fingerprint(current), backfill.fingerprint(actual), Jsonb(counts)))
    return 'committed_pending_verification'


def execute_run(connection, monday, run_dir, *, apply=False, confirm_run_id=None):
    manifest, staged = load_run(run_dir)
    if manifest['target'] != backfill.target_fingerprint(connection):
        raise ValueError('Database target differs from staged target')
    if apply and confirm_run_id != manifest['run_id']:
        raise ValueError('Apply requires --confirm-run-id matching the reviewed manifest')
    with connection.transaction():
        connection.execute('SET TRANSACTION READ ONLY')
        scopes.schema_safety(connection)
        committed = scopes.committed_scopes(connection, manifest)
    changed = {r['scope_id'] for r in staged['scopes'] if any(r['updates'].values())}
    if committed - changed:
        raise ValueError('Unexpected journal scopes')
    results = []
    for n, record in enumerate(staged['scopes'], 1):
        sid = record['scope_id']
        LOG.info('%s %d/%d: %s', 'Apply' if apply else 'Verify', n, len(staged['scopes']), sid)
        result = {'scope_id': sid, 'project_ids': record['project_ids']}
        try:
            if apply and sid in committed:
                result['status'] = 'already_committed'
            elif apply and sid not in changed:
                result['status'] = 'no_changes_staged'
            elif not apply and sid in changed and sid not in committed:
                result['status'] = 'not_committed'
            else:
                check_source(monday, record)
                if apply:
                    result['status'] = commit_scope(connection, manifest, staged, record)
                    # Monday and PostgreSQL cannot share a transaction. Check
                    # again after commit; never report a changed source as success.
                    check_source(monday, record)
                    result['status'] = 'committed_source_rechecked'
                else:
                    with connection.transaction():
                        connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
                        actual = scopes.read_boundary(connection, record['boundary'], full=True)
                        if actual != record['after']:
                            raise scopes.ScopeConflict('Database differs from reviewed after-state')
                        if sid in changed:
                            journal = connection.execute('SELECT after_sha256 FROM public.order_value_scope_commits '
                                'WHERE run_id=%s AND scope_id=%s AND plan_sha256=%s',
                                (manifest['run_id'], sid, manifest['sha256'])).fetchone()
                            if not journal or journal[0] != backfill.fingerprint(actual):
                                raise scopes.ScopeConflict('Database differs from committed journal')
                    check_source(monday, record)
                    result['status'] = 'verified' if sid in changed else 'verified_no_changes'
        except ValueError as exc:
            result.update(status='requires_reassessment', reason=str(exc))
        except (psycopg.errors.LockNotAvailable, psycopg.errors.DeadlockDetected,
                psycopg.errors.SerializationFailure, psycopg.errors.QueryCanceled, psycopg.IntegrityError) as exc:
            result.update(status='requires_reassessment', reason=type(exc).__name__)
        results.append(result)
        backfill.write_json(run_dir / f'receipt-{uuid4()}.json', result)
    remaining = changed - scopes.committed_scopes(connection, manifest)
    success_states = {'already_committed', 'no_changes_staged', 'committed_source_rechecked'} if apply else {'verified', 'verified_no_changes'}
    summary = {'run_id': manifest['run_id'], 'checked_at': now(), 'action': 'apply' if apply else 'verify',
        'results': results, 'counts': dict(Counter(r['status'] for r in results)), 'remaining_uncommitted': len(remaining),
        'unresolved_fields': manifest['unresolved_fields'], 'deferred_scopes': staged['deferred'],
        'staged_changes_successful': not remaining and all(r['status'] in success_states for r in results),
        'all_selected_projects_fully_reconciled': False}
    # Even verified mapped fields do not certify every column in these tables.
    backfill.write_json(run_dir / f'{summary["action"]}-{uuid4()}.json', summary)
    return summary


def failure_message(command, exc):
    detail = str(exc) if isinstance(exc, ValueError) else type(exc).__name__
    if command == 'stage':
        return (f'Staging failed: {detail}. This Stage command was read-only and made no Supabase changes. '
                'Do not apply this incomplete run. Retry Stage in a new run directory.')
    if command == 'verify':
        return (f'Verification failed: {detail}. Verification is read-only. '
                'Preserve the run and inspect its receipts before retrying.')
    return (f'Apply stopped: {detail}. Earlier scopes may have committed; '
            'preserve the run and verify before retrying.')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    stage = commands.add_parser('stage', help='Read-only fresh comparison and staging of actual differences')
    stage.add_argument('--report', type=Path, default=DEFAULT_REPORT)
    stage.add_argument('--run-dir', type=Path, required=True)
    for action in ('apply', 'verify'):
        command = commands.add_parser(action)
        command.add_argument('--run-dir', type=Path, required=True)
        if action == 'apply':
            command.add_argument('--confirm-run-id', required=True)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    backfill.load_dotenv()
    try:
        # Validate local inputs before connecting to either service.
        ids = project_ids_from_report(args.report) if args.command == 'stage' else None
        if args.command != 'stage':
            load_run(args.run_dir)
        dsn = os.environ.get('SUPABASE_DB_URL')
        if not dsn:
            raise ValueError('SUPABASE_DB_URL is required in the environment')
        with psycopg.connect(dsn, autocommit=True, connect_timeout=15) as connection:
            monday = ComparisonMondayClient()
            if args.command == 'stage':
                result = stage_run(connection, monday, args.run_dir, ids, selection={
                    'report': str(args.report), 'sha256': hashlib.sha256(args.report.read_bytes()).hexdigest(),
                    'filter': 'manual_review'})
                success = not result['unresolved_fields'] and not result['deferred_scopes']
            else:
                result = execute_run(connection, monday, args.run_dir, apply=args.command == 'apply',
                                     confirm_run_id=getattr(args, 'confirm_run_id', None))
                success = result['staged_changes_successful'] and not result['unresolved_fields'] and not result['deferred_scopes']
        print(json.dumps(result, indent=2))
        return 0 if success else 2
    except Exception as exc:
        LOG.error('%s', failure_message(args.command, exc))
    return 1


if __name__ == '__main__':
    raise SystemExit(main())
