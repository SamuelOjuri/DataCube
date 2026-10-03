"""Offline reassessment and guarded staging from a current blocked-project capture."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import logging
from pathlib import Path
from uuid import uuid4

from scripts import backfill_order_values as backfill
from scripts import order_value_scope_reads as reads
from scripts import order_value_scopes as scopes
from scripts import order_value_scopes_targeted as targeted
from scripts import reconcile_order_values as reconcile

LOG = logging.getLogger(__name__)


def load_capture(path):
    def read(name):
        return json.loads((path / (name + '.json')).read_text(encoding='utf-8'))
    context = read('completed-context' if (path / 'completed-context.json').is_file() else 'context')
    if not context['complete']:
        raise ValueError('Current evidence capture is incomplete')
    source = {key: read(key) for key in ('parents', 'children', 'hidden')}
    documents = {'source': source, **{key: read(key) for key in ('baseline', 'ownership', 'contract', 'exceptions')}}
    if any(backfill.fingerprint(value) != context[key + '_sha256'] for key, value in documents.items()):
        raise ValueError('Current capture evidence hashes do not match')
    return context, documents


def valid_detail(item, columns, board):
    if (item.get('state') != 'active' or (item.get('board') or {}).get('id') != board
            or not isinstance(item.get('name'), str) or 'parent_item' not in item):
        raise ValueError('Inactive, misplaced or incomplete item metadata')
    values = backfill.indexed(item.get('column_values', []), 'id')
    if set(values) != set(columns) or any('value' not in row for row in values.values()):
        raise ValueError('Missing requested source columns')
    return {**item, 'column_values': [values[c] for c in sorted(values)]}


def source_from_capture(context, documents):
    raw = documents['source']
    selected = set(context['selected_project_ids'])
    links = backfill.indexed(documents['ownership']['items'])
    excluded = set(backfill.REVIEWED_PARENTLESS_DUPLICATES)
    parents = backfill.indexed(raw['parents']['items'], 'id')
    children, hidden, issues = {}, {}, defaultdict(set)
    for item in raw['children']['items']:
        try:
            detail = valid_detail(item, raw['children']['requested_columns'], backfill.SUBITEM_BOARD_ID)
            row = reads.normalized_link(detail)
            if links.get(row['monday_id']) != row:
                raise ValueError('Child relationship changed during current capture')
            children[row['monday_id']] = detail
        except ValueError as exc:
            parent = (item.get('parent_item') or {}).get('id')
            if parent in selected:
                issues[parent].add(str(exc))
    for item in raw['hidden']['items']:
        try:
            detail = valid_detail(item, raw['hidden']['requested_columns'], backfill.HIDDEN_ITEMS_BOARD_ID)
            value = backfill.normalize_hidden(detail)
            if value['issues'] or any(value[f] is None for f in backfill.ORDER_FIELDS):
                raise ValueError('Invalid order inputs or inconsistent Monday formula')
            hidden[item['id']] = detail
        except ValueError:
            pass  # Missing/invalid sources are explicit blockers in the plan below.
    valid_parents = []
    for pid in sorted(selected):
        parent = parents.get(pid)
        if (parent is None or parent.get('state') != 'active'
                or (parent.get('board') or {}).get('id') != backfill.PARENT_BOARD_ID
                or parent.get('parent_item') is not None):
            issues[pid].add('Parent unavailable, inactive or on a different board')
            continue
        check = backfill.compare_parent_inventory(
            {i for i, row in links.items() if row.get('parent_monday_id') == pid},
            {'counts_match': True}, {'items': [parent], 'not_returned_ids': []})
        if not check['consistent']:
            issues[pid].add('Parent membership or metadata changed during capture')
        parent['subitems'] = sorted(parent['subitems'], key=lambda r: r['id'])
        valid_parents.append(pid)
    wanted_sources = set(context['boundary']['hidden_items'])
    normalized = [row for cid, row in links.items() if cid not in excluded
                  and (row['parent_monday_id'] in selected or wanted_sources.intersection(row['hidden_ids']))]
    source = {'project_ids': valid_parents, 'subitems': normalized,
              'hidden_items': [backfill.normalize_hidden(hidden[i]) for i in sorted(hidden)],
              'exclusion_evidence': {'parent_details': {'items': [parents[i] for i in valid_parents]}}}
    return source, {'subitems': children, 'hidden_items': hidden}, issues


def make_record(baseline, source, raw, contract, parents, mode):
    inventory = scopes.index_source(source)
    children = [r for pid in parents for r in inventory['parents'][pid]]
    if not children or any(len(r['hidden_ids']) != 1 or r['link_error'] for r in children):
        raise scopes.ScopeConflict('Empty or ambiguous Monday child/source set')
    scope = {'projects': sorted(parents), 'subitems': sorted(r['monday_id'] for r in children),
             'hidden_items': sorted({r['hidden_ids'][0] for r in children})}
    boundary = scopes.boundary_from_baseline(baseline, scope)
    evidence = scopes.source_evidence(source, scope, inventory)
    # Every source's observed owners must be exactly the selected active children.
    if evidence['owners'] != evidence['subitems']:
        raise scopes.ScopeConflict('Monday source ownership extends outside the selected group')
    state = baseline if mode == 'repair' else {
        table: [{k: row.get(k) for k in backfill.BASELINE_COLUMNS[table]} for row in rows]
        for table, rows in baseline.items()}
    before = scopes.select_boundary(scopes.index_baseline(state), boundary)
    scopes.require_existing_scope(before, scope)
    scopes.check_parent_membership(before, scope, evidence)
    inputs = None
    if mode == 'orders':
        updates = scopes.order_updates(before, evidence, scope)
    else:
        inputs = {t: [raw[t][i] for i in scope[t]] for t in ('hidden_items', 'subitems')}
        updates = reconcile.normalize_updates(reconcile.transform_exact_rows(
            inputs['hidden_items'], inputs['subitems'], set(parents)), contract)
    after = scopes.expected_state(before, updates)
    scopes.check_ownership(after, scope)
    return {'scope_id': 'scope-' + backfill.fingerprint(sorted(parents))[:16], 'scope': scope,
            'boundary': boundary, 'before': before, 'after': after, 'updates': updates,
            'source': evidence, 'raw': inputs}


def field_changes(records):
    changes = []
    for record in records:
        for table, rows in record['updates'].items():
            original = backfill.indexed(record['before'][table])
            for row in rows:
                for field, value in row.items():
                    old = original[row['monday_id']].get(field)
                    if old != value:
                        changes.append({'scope_id': record['scope_id'], 'project_ids': record['scope']['projects'],
                                        'table': table, 'monday_id': row['monday_id'], 'field': field,
                                        'before': old, 'after': value})
    return changes


def dependency_groups(candidate_ids, baseline, source):
    """Keep candidates sharing either old or new source keys in one transaction."""
    selected = set(candidate_ids)
    sources = defaultdict(set)
    for row in baseline['subitems']:
        if row.get('parent_monday_id') in selected and row.get('hidden_item_id'):
            sources[row['hidden_item_id']].add(row['parent_monday_id'])
    for row in source['subitems']:
        if row['parent_monday_id'] in selected:
            for hid in row['hidden_ids']:
                sources[hid].add(row['parent_monday_id'])
    neighbors = {pid: set() for pid in selected}
    for parents in sources.values():
        for pid in parents:
            neighbors[pid].update(parents - {pid})
    groups = []
    while selected:
        pending, group = [min(selected)], set()
        while pending:
            pid = pending.pop()
            if pid not in group:
                group.add(pid)
                pending.extend(neighbors[pid] - group)
        selected -= group
        groups.append(sorted(group))
    return groups


def pack_records(records, baseline, source, raw, contract, mode):
    """Combine independent reviewed groups without exceeding existing bounds."""
    result, current = [], None
    for record in records:
        if current is None:
            current = record
            continue
        parents = sorted(current['scope']['projects'] + record['scope']['projects'])
        if len(parents) <= scopes.MAX_PROJECTS:
            try:
                current = make_record(baseline, source, raw, contract, parents, mode)
                continue
            except scopes.ScopeConflict:
                pass
        result.append(current)
        current = record
    if current:
        result.append(current)
    return result


def save_run(output, context, contract, records, mode):
    output.mkdir(parents=True, exist_ok=False)
    staged = {'mode': mode, 'contract': contract, 'scopes': records, 'deferred': [],
              'origin_run_id': context['previous_capture_id'], 'origin_summary': context['original_summary'],
              'approved_empty': [], 'blocked_reassessment': {
                  'captured_at': context['finished_at'], 'source_sha256': context['source_sha256'],
                  'baseline_sha256': context['baseline_sha256'], 'ownership_sha256': context['ownership_sha256'],
                  'source_of_truth': 'Monday CRM'}}
    changes = field_changes(records)
    backfill.write_json(output / 'scopes.json', staged)
    reconcile.write_csv(output / 'changes.csv', changes,
                        ['scope_id', 'project_ids', 'table', 'monday_id', 'field', 'before', 'after'])
    manifest = {'version': targeted.VERSION, 'workflow': targeted.WORKFLOW, 'run_id': str(uuid4()),
                'prepared_at': datetime.now(timezone.utc).isoformat(), 'code': targeted.code_fingerprint(),
                'target': context['target'], 'source_contract': backfill.source_contract(),
                'sha256': backfill.fingerprint(staged),
                'review_sha256': hashlib.sha256((output / 'changes.csv').read_bytes()).hexdigest(),
                'mode': mode, 'scopes': len(records), 'deferred_scopes': 0, 'changes': len(changes),
                'projects': sum(len(r['scope']['projects']) for r in records), 'safety': context['safety']}
    backfill.write_json(output / 'manifest.json', manifest)
    targeted.load_run(output)
    return manifest


def review(capture_dir, output_dir):
    context, documents = load_capture(capture_dir)
    source, raw, metadata_issues = source_from_capture(context, documents)
    baseline, contract = documents['baseline'], documents['contract']
    minimal = {t: [{k: r.get(k) for k in backfill.BASELINE_COLUMNS[t]} for r in rows] for t, rows in baseline.items()}
    plan = backfill.build_plan(minimal, {k: source[k] for k in ('project_ids', 'subitems', 'hidden_items')})
    report = reconcile.build_report(minimal, source, plan)
    project_reviews = {r['project_id']: r for r in report['projects']}
    plan_projects = backfill.indexed(plan['projects'], 'project_id')
    rows, ready = {}, []
    for pid in context['selected_project_ids']:
        current = plan_projects.get(pid, {})
        previous_review = project_reviews.get(pid, {})
        reasons = set(metadata_issues[pid]) | set(previous_review.get('blockers', []))
        if not documents['exceptions']['valid']:
            reasons.add('Reviewed duplicate exception conditions changed')
        row = {'project_id': pid, 'item_name': current.get('item_name'), 'status': 'manual_review',
               'monday_state': next((r.get('state') for r in documents['source']['parents']['items'] if r['id'] == pid), 'not_returned'),
               'issues': current.get('issues', []), 'reasons': sorted(reasons), 'dependency_field_changes': 0}
        rows[pid] = row
        if not current:
            row['reasons'].append('Project row missing from Supabase')
        elif not reasons:
            ready.append(pid)
    candidates = {'orders': [], 'repair': []}
    for group in dependency_groups(ready, baseline, source):
        mode = 'orders' if all(plan_projects[pid]['status'] == 'verified' for pid in group) else 'repair'
        try:
            if len(group) > scopes.MAX_PROJECTS:
                raise scopes.ScopeConflict('Dependency group exceeds the 25-project transaction limit')
            record = make_record(baseline, source, raw, contract, group, mode)
            changes = field_changes([record])
            if changes:
                candidates[mode].append(record)
            for pid in group:
                rows[pid].update(status=(mode + '_ready') if changes else 'already_matches',
                                 dependency_field_changes=len(changes), dependency_project_ids=group)
        except (ValueError, KeyError) as exc:
            status = 'needs_missing_rows' if 'Missing scoped rows' in str(exc) else 'manual_review'
            for pid in group:
                rows[pid].update(status=status, reasons=[str(exc)])
    output_dir.mkdir(parents=True, exist_ok=False)
    manifests = {}
    for mode, records in candidates.items():
        if records:
            packed = pack_records(records, baseline, source, raw, contract, mode)
            manifests[mode] = save_run(output_dir / (mode + '-run'), context, contract, packed, mode)
    project_rows = [rows[i] for i in sorted(rows)]
    fields = ['project_id', 'item_name', 'monday_state', 'status', 'issues', 'reasons', 'dependency_field_changes', 'dependency_project_ids']
    reconcile.write_csv(output_dir / 'projects.csv', project_rows, fields)
    reconcile.write_csv(output_dir / 'unresolved-projects.csv',
        [r for r in project_rows if r['status'] in ('manual_review', 'needs_missing_rows')], fields)
    backfill.write_json(output_dir / 'report.json', {**report, 'current_project_reviews': project_rows,
                                                   'exception_reassessment': documents['exceptions']})
    # Revisit previously unlinked sources as well; ownership comes from this live scan.
    owner_map = reads.owner_index(documents['ownership'])
    unlinked = [r for r in source['hidden_items'] if not owner_map[r['monday_id']]
                and any(r[f] != '0.00' for f in backfill.ORDER_FIELDS)]
    reconcile.write_csv(output_dir / 'unlinked-nonzero-sources.csv', unlinked,
                        ['monday_id', 'item_name', *backfill.ORDER_FIELDS, 'monday_total'])
    summary = {'source_of_truth': 'Monday CRM', 'captured_at': context['finished_at'],
               'selected_projects': len(project_rows), 'counts': dict(Counter(r['status'] for r in project_rows)),
               'unresolved_reasons': dict(Counter(reason for r in project_rows for reason in r['reasons'])),
               'excluded_subitems': documents['exceptions'], 'unlinked_nonzero_sources_in_review': len(unlinked),
               'runs': manifests, 'business_writes_performed': False}
    backfill.write_json(output_dir / 'summary.json', summary)
    return summary


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--capture-dir', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('src.database.sync_service').setLevel(logging.ERROR)
    result = review(args.capture_dir, args.output_dir)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
