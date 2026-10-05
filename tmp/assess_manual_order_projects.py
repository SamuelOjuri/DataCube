"""Offline assessment only: no live clients, database writes or apply plans."""
from collections import Counter, defaultdict
from decimal import Decimal, InvalidOperation
from pathlib import Path
import csv
import json
import logging
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import backfill_order_values as backfill
from scripts import order_value_blocked_review as review
from scripts import order_value_scope_reads as reads
from src.config import HIDDEN_ITEMS_COLUMNS as HC

logging.getLogger('src.database.sync_service').setLevel(logging.ERROR)
BASE = ROOT / 'outputs/order_value_backfill'
OUTPUT = BASE / 'manual_review_assessment_20261003'
context, docs = review.load_capture(BASE / 'blocked_current_20261002')
parents = backfill.indexed(docs['source']['parents']['items'], 'id')
children = backfill.indexed(docs['source']['children']['items'], 'id')
hidden = backfill.indexed(docs['source']['hidden']['items'], 'id')
stored = {table: backfill.indexed(rows) for table, rows in docs['baseline'].items()}
owners = reads.owner_index(docs['ownership'])


def read_csv(path):
    with path.open(encoding='utf-8-sig', newline='') as stream:
        return list(csv.DictReader(stream))


def numeric(value):
    if value is None or value == '':
        return None
    try:
        value = Decimal(str(value))
        return value if value.is_finite() else None
    except InvalidOperation:
        return None


def nonzero(value):
    value = numeric(value)
    return value is not None and value != 0


def column_value(item, key):
    column = next((r for r in item.get('column_values', []) if r['id'] == HC[key]), {})
    for field in ('display_value', 'text'):
        if column.get(field) not in (None, ''):
            return column[field]
    value = column.get('value')
    if isinstance(value, str):
        try:
            return json.loads(value)
        except ValueError:
            return value
    return value


latest = [r for r in read_csv(BASE / 'blocked_review_20261002/projects.csv') if r['status'] == 'manual_review']
original = [r for r in read_csv(BASE / 'reconciliation_run4/projects.csv') if r['action'] == 'manual_review']
old_ids = {r['project_id'] for r in original}
recommendations = {
    'empty_active_no_stored_children': 'Exclude from this order correction; retain enquiry/history; reassess if children appear.',
    'archived_parent': 'Retain historical data; exclude from active correction pending lifecycle reconciliation.',
    'parent_not_returned': 'Targeted access/state recheck; not-returned is not proof of deletion; preserve stored data.',
    'stale_sql_children': 'Reconcile current membership; retire confirmed stale children from current rollups with audit history.',
    'shared_source': 'Keep project; resolve duplicate/shared source ownership before rollup; do not discard on Archived label.',
    'invalid_source_link': 'Keep project pending exact source relationship repair in Monday; no name-based substitution.',
}
assessments, stale_rows, archived_sources, duplicate_sources = [], [], {}, {}
for report in latest:
    pid = report['project_id']
    parent = parents.get(pid, {})
    state = parent.get('state', 'not_returned')
    project = stored['projects'][pid]
    live_ids = {r['id'] for r in parent.get('subitems', [])}
    sql_children = [r for r in stored['subitems'].values() if r.get('parent_monday_id') == pid]
    stale = [r for r in sql_children if r['monday_id'] not in live_ids]
    reasons = json.loads(report['reasons'])
    if state == 'archived':
        group = 'archived_parent'
    elif state != 'active':
        group = 'parent_not_returned'
    elif stale:
        group = 'stale_sql_children'
    elif not live_ids:
        group = 'empty_active_no_stored_children'
    elif 'shared_live_hidden_source' in reasons:
        group = 'shared_source'
    else:
        assert 'invalid_live_hidden_link' in reasons, report
        group = 'invalid_source_link'
    wanted, valid_links = set(), bool(live_ids)
    for cid in live_ids:
        if cid not in children:
            valid_links = False
            continue
        link = backfill.normalize_subitem(children[cid])
        valid_links = valid_links and not link['link_error'] and len(link['hidden_ids']) == 1
        wanted.update(link['hidden_ids'])
    complete = valid_links
    total = Decimal(0)
    archived_ids, archived_financial_ids, shared_ids = [], [], []
    for hid in sorted(wanted):
        item = hidden.get(hid)
        if item is None:
            complete = False
            continue
        amount = backfill.normalize_hidden(item)
        usable = (item.get('state') == 'active' and not amount['issues']
                  and all(amount.get(f) is not None for f in backfill.ORDER_FIELDS))
        complete = complete and usable
        if usable:
            total += sum(Decimal(amount[f]) for f in backfill.ORDER_FIELDS)
        if len(owners.get(hid, [])) > 1:
            shared_ids.append(hid)
            duplicate_sources[hid] = {'hidden_id': hid, 'name': item.get('name'),
                'business_status': column_value(item, 'status'),
                'parent_ids': sorted({r['parent_monday_id'] for r in owners[hid]}),
                'child_ids': sorted(r['monday_id'] for r in owners[hid]),
                **{f: amount.get(f) for f in backfill.ORDER_FIELDS},
                'amount_invoiced': column_value(item, 'amount_invoiced'),
                'quote_amount': column_value(item, 'quote_amount')}
        if column_value(item, 'status') == 'Archived':
            archived_ids.append(hid)
            has_finance = any(nonzero(amount.get(f)) for f in backfill.ORDER_FIELDS) or nonzero(column_value(item, 'amount_invoiced'))
            if has_finance:
                archived_financial_ids.append(hid)
            archived_sources[hid] = {'hidden_id': hid, 'parent_id': pid, 'api_state': item.get('state'),
                'business_status': 'Archived', 'name': item.get('name'), 'has_order_or_invoice_value': has_finance,
                **{f: amount.get(f) for f in backfill.ORDER_FIELDS},
                'amount_invoiced': column_value(item, 'amount_invoiced'),
                'quote_amount': column_value(item, 'quote_amount')}
    for row in stale:
        detail = children.get(row['monday_id'], {})
        stale_rows.append({'project_id': pid, 'project_name': project.get('item_name'), 'parent_api_state': state,
            'subitem_id': row['monday_id'], 'subitem_name': row.get('item_name'),
            'subitem_api_state': detail.get('state', 'not_returned'),
            'current_parent_id': (detail.get('parent_item') or {}).get('id'),
            'stored_hidden_id': row.get('hidden_item_id'),
            **{f: row.get(f) for f in (*backfill.ORDER_FIELDS, 'amount_invoiced', 'quote_amount')},
            'has_stored_order_or_invoice': any(nonzero(row.get(f)) for f in (*backfill.ORDER_FIELDS, 'amount_invoiced'))})
    assessments.append({'project_id': pid, 'item_name': project.get('item_name'),
        'project_name': project.get('project_name'), 'in_original_manual_report': pid in old_ids,
        'monday_api_state': state, 'stored_pipeline_stage': project.get('pipeline_stage'),
        'numeric_item_name': str(project.get('item_name') or '').isascii() and str(project.get('item_name') or '').isdigit(),
        'current_child_count': len(live_ids) if parent else None, 'stored_child_count': len(sql_children),
        'stale_child_ids': [r['monday_id'] for r in stale],
        'stored_order_total': project.get('total_order_value'), 'stored_invoice_total': project.get('total_amount_invoiced'),
        'stored_enquiry_value': project.get('new_enquiry_value'),
        'diagnostic_unique_source_order_total_not_apply': format(total, '.2f') if complete else None,
        'archived_label_source_ids': archived_ids, 'archived_label_nonzero_source_ids': archived_financial_ids,
        'shared_hidden_ids': shared_ids, 'original_reasons': reasons,
        'recommended_group': group, 'recommendation': recommendations[group],
        'automatic_write_authorized': False})

assert len(assessments) == 156 and len({r['project_id'] for r in assessments}) == 156
OUTPUT.mkdir(parents=True, exist_ok=False)


def write_csv(name, rows):
    with (OUTPUT / name).open('x', encoding='utf-8-sig', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        for row in rows:
            writer.writerow({k: json.dumps(v) if isinstance(v, (dict, list)) else v for k, v in row.items()})


write_csv('projects-reassessed.csv', assessments)
write_csv('stale-children.csv', stale_rows)
write_csv('archived-status-sources.csv', list(archived_sources.values()))
write_csv('shared-sources.csv', list(duplicate_sources.values()))
summary = {'source_capture_finished_at': context['finished_at'], 'source_capture_sha256': context['source_sha256'],
    'baseline_sha256': context['baseline_sha256'], 'offline_assessment': True, 'fresh_live_reads_performed': False,
    'production_writes_performed': False, 'project_count': len(assessments),
    'groups': dict(Counter(r['recommended_group'] for r in assessments)),
    'non_numeric_names': sum(not r['numeric_item_name'] for r in assessments),
    'stale_child_states': dict(Counter(r['subitem_api_state'] for r in stale_rows)),
    'archived_label_sources': len(archived_sources),
    'archived_label_sources_with_order_or_invoice': sum(r['has_order_or_invoice_value'] for r in archived_sources.values()),
    'shared_source_count': len(duplicate_sources),
    'shared_sources_across_parents': sum(len(r['parent_ids']) > 1 for r in duplicate_sources.values()),
    'old_report_only_ids': sorted(old_ids - {r['project_id'] for r in assessments}),
    'latest_manual_only_ids': sorted({r['project_id'] for r in assessments} - old_ids)}
(OUTPUT / 'summary.json').write_text(json.dumps(summary, indent=2), encoding='utf-8')
print(json.dumps(summary, indent=2))
print('Examples:', json.dumps([{k: r[k] for k in ['project_id', 'item_name', 'recommended_group',
    'diagnostic_unique_source_order_total_not_apply', 'archived_label_nonzero_source_ids']}
    for r in assessments if r['item_name'] in ['16312', '16214', '15802', '15227']], indent=2))
