"""Export the remaining review queue from saved evidence; no network or DB access."""
import csv
import hashlib
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / 'outputs/order_value_backfill'
OUT = BASE / 'manual_review_96_20261005'
SPECS = [
    ('monday_compare_20261004_225343', 'comparison.json',
     'verify-2cba2954-97ac-4b85-881d-fc009a6ed475.json'),
    ('monday_compare_retry_20261004_235725', 'comparison.json',
     'verify-b25fca73-9dc9-4116-93c2-eeb0dab99b20.json'),
    ('rehydrate_missing_2_20261005_003553', 'inserts.json',
     'verify-3824d531-df1d-4dad-a3c1-dde19a7de58d.json'),
]
POLICY = {
    'mirror_aggregation': (
        'Unconfirmed mirror aggregation',
        'Confirm the actual Monday aggregation for new_enquiry_value and implement its mapping in sync; do not assume SUM. Finance changes Monday only if the intended configuration or value is wrong.',
        'CRM administrator; integration maintainer'),
    'child_not_returned': (
        'Stored subitem absent; Monday did not return it',
        'Check the exact subitem ID, access and current parent/lifecycle in Monday. Absence alone does not prove deletion. Sync the confirmed result through the lifecycle/backfill workflow.',
        'CRM administrator; integration maintainer'),
    'archived_child': (
        'Stored subitem has Monday API state archived',
        'Reflect the confirmed archived lifecycle and current membership through the sync lifecycle workflow, retaining audit history. A business status label Archived alone must not exclude order values.',
        'Integration maintainer; CRM administrator'),
    'deleted_child': (
        'Stored subitem has Monday API state deleted',
        'Reflect the confirmed deleted lifecycle and removal from current membership through the sync lifecycle workflow, preserving audit history. Finance restores the item in Monday only if deletion was unintended.',
        'Integration maintainer; CRM administrator'),
    'missing_source_link': (
        'Current Monday subitem has no linked hidden source',
        'Finance confirms whether the blank Monday source link is intentional; repair it in Monday if incorrect. Sync must represent the confirmed link or blank faithfully without guessing a source or retaining stale linked values.',
        'Finance; integration maintainer'),
    'archived_project': (
        'Project has Monday API state archived',
        'Reflect the confirmed project archive through the sync lifecycle workflow and retain its history. Finance restores it in Monday only if the archive was unintended; do not zero values solely because of archive status.',
        'Integration maintainer; CRM administrator'),
    'project_not_returned': (
        'Monday did not return the project',
        'Check the exact project ID, board visibility/access and lifecycle in Monday. Do not infer deletion from absence. Sync the confirmed state, or restore access/item in Monday if needed.',
        'CRM administrator; integration maintainer'),
}
EXPECTED = dict(zip(POLICY, [69, 43, 30, 5, 7, 11, 11]))


def read_json(path):
    return json.loads(path.read_text(encoding='utf-8'))


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_saved(spec):
    name, plan_file, verification_file = spec
    directory = BASE / name
    manifest = read_json(directory / 'manifest.json')
    plan = read_json(directory / plan_file)
    canonical = json.dumps(plan, sort_keys=True, indent=2, ensure_ascii=True).encode('utf-8')
    assert hashlib.sha256(canonical).hexdigest() == manifest['sha256'], name
    checks = manifest.get('review_hashes', {'changes.csv': manifest.get('review_sha256')})
    for filename, digest in checks.items():
        assert sha(directory / filename) == digest, (name, filename)
    verification = read_json(directory / verification_file)
    assert verification['run_id'] == manifest['run_id']
    assert verification['action'] == 'verify'
    return {'name': name, 'manifest': manifest, 'plan': plan, 'verify': verification,
            'verification_file': verification_file, 'plan_file': plan_file}


def category(issue):
    reason = issue['reason']
    if reason == 'Multiple mirror values without confirmed SUM configuration':
        return 'mirror_aggregation'
    if reason.startswith('Stored child absent from current parent membership'):
        if '(not returned)' in reason:
            return 'child_not_returned'
        if '(state=archived, parent=None)' in reason:
            return 'archived_child'
        if '(state=deleted, parent=None)' in reason:
            return 'deleted_child'
    if reason == 'Expected one representable source, Monday has 0: []; no guessed link or amount':
        return 'missing_source_link'
    if reason == 'Monday state=archived; lifecycle storage/retirement requires separate review':
        return 'archived_project'
    if reason == 'Not returned by Monday; absence is not deletion evidence':
        return 'project_not_returned'
    raise ValueError(f'Unmapped review reason: {reason}')


def join(values):
    return ' | '.join(dict.fromkeys(str(v) for v in values if v is not None and v != ''))


def write_csv(filename, rows):
    # BOM supports direct opening in Excel. No formulas are written to these CSVs.
    for row in rows:
        for value in row.values():
            if isinstance(value, str):
                assert value == '-' or not value.lstrip().startswith(('=', '+', '-', '@')), value
    path = OUT / filename
    with path.open('w', encoding='utf-8-sig', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    with path.open(encoding='utf-8-sig', newline='') as stream:
        reread = list(csv.DictReader(stream))
    assert reread == [{k: '' if v is None else str(v) for k, v in r.items()} for r in rows]
    return path


def main():
    original, retry, hydrate = sources = [load_saved(spec) for spec in SPECS]
    replaced = set(retry['plan']['selected_project_ids'])
    original_ids = set(original['plan']['selected_project_ids'])
    assert len(original_ids) == 156 and len(replaced) == 50 and replaced <= original_ids
    for source in (retry, hydrate):
        assert source['verify']['staged_changes_successful'] is True
        assert source['verify']['remaining_uncommitted'] == 0
        assert all(r['status'] == 'verified' for r in source['verify']['results'])
    fixed = {(t['project_id'], t['subitem_id']) for t in hydrate['plan']['targets']}
    assert fixed == {('1772110252', '2828149014'), ('2964337986', '3123480558')}
    verified_inserts = {i for r in hydrate['verify']['results'] for i in r['subitem_ids']}
    assert verified_inserts == {sid for _, sid in fixed}
    assert {r['monday_id'] for s in hydrate['plan']['scopes']
            for r in s['inserts']['subitems']} == verified_inserts

    contexts, issues, removed = {}, [], []
    for source in (original, retry):
        verified = {r['scope_id']: r['status'] for r in source['verify']['results']}
        for scope in source['plan']['scopes']:
            selected = set(scope['project_ids'])
            if source is original:
                selected -= replaced
            if not selected:
                continue
            assert verified[scope['scope_id']] == 'verified'
            for pid in selected:
                assert pid not in contexts
                project = next(p for p in scope['after']['projects'] if p['monday_id'] == pid)
                contexts[pid] = (source, scope, project)
            for issue in scope['issues']:
                if issue['project_id'] not in selected:
                    continue
                if (issue['reason'] == 'Missing Supabase row: requires rehydration'
                        and (issue['project_id'], issue['monday_id']) in fixed):
                    removed.append(issue)
                else:
                    issues.append(issue)
    assert set(contexts) == original_ids
    assert len(removed) == 20 and len(issues) == 176
    assert len({tuple(sorted(i.items())) for i in issues}) == len(issues)
    counts = Counter(category(i) for i in issues)
    assert counts == EXPECTED, counts
    by_project = defaultdict(list)
    for issue in issues:
        by_project[issue['project_id']].append(issue)
    assert len(by_project) == 96
    for scope in hydrate['plan']['scopes']:
        for project in scope['after']['projects']:
            pid = project['monday_id']
            if pid in contexts:
                source, old_scope, _ = contexts[pid]
                contexts[pid] = (source, old_scope, project)

    project_rows, detail_rows = [], []
    ordered = sorted(by_project, key=lambda pid: (contexts[pid][2]['date_created'], int(pid)), reverse=True)
    for pid in ordered:
        source, scope, project = contexts[pid]
        parent = scope['source']['projects'].get(pid)
        project_issues = sorted(by_project[pid], key=lambda i: (category(i), i['table'], i['monday_id'], i['field']))
        project_counts = Counter(category(i) for i in project_issues)
        categories = [c for c in POLICY if c in project_counts]
        item_name = parent['name'] if parent else project['item_name']
        common = dict(project_id=pid, item_name=item_name, project_name=project.get('project_name'),
                      date_created=project['date_created'])
        provenance = dict(comparison_run_id=source['manifest']['run_id'],
                          comparison_prepared_at_utc=source['manifest']['prepared_at'],
                          comparison_verified_at_utc=source['verify']['checked_at'],
                          evidence_directory=f"outputs/order_value_backfill/{source['name']}")
        board_id = (parent or {}).get('board', {}).get('id')
        url = (f'https://taperedplus.monday.com/boards/{board_id}/pulses/{pid}' if board_id
               else f'https://taperedplus.monday.com/boards/1825117125/pulses/{pid}')
        # For unavailable parents, URL points to the original project board and may not resolve.
        project_rows.append({**common, 'pipeline_stage': project.get('pipeline_stage'),
            'monday_item_state': parent.get('state') if parent else 'not_returned',
            'review_status': 'manual_review', 'issue_count': len(project_issues),
            **{f'{c}_issue_count': project_counts[c] for c in POLICY},
            'issue_categories': join(POLICY[c][0] for c in categories),
            'affected_subitem_ids': join(sorted({i['monday_id'] for i in project_issues if i['table'] == 'subitems'})),
            'affected_fields': join(sorted({f"{i['table']}.{i['field']}" for i in project_issues})),
            'review_issues': join(f"{i['table']} {i['monday_id']}.{i['field']}: {i['reason']}" for i in project_issues),
            'required_actions': join(POLICY[c][1] for c in categories),
            'suggested_reviewers': join(r for c in categories for r in POLICY[c][2].split('; ')),
            'resolved_rehydration_subitem_ids': join(sorted(sid for project_id, sid in fixed if project_id == pid)),
            **provenance, 'monday_project_url': url})
        for issue in project_issues:
            c = category(issue)
            monday_row = scope['source'][issue['table']].get(issue['monday_id'])
            stored = next((r for r in scope['after'][issue['table']] if r['monday_id'] == issue['monday_id']), {})
            detail_rows.append({**common, 'table': issue['table'], 'affected_monday_id': issue['monday_id'],
                'affected_item_name': monday_row.get('name') if monday_row else stored.get('item_name'),
                'affected_monday_item_state': monday_row.get('state') if monday_row else 'not_returned',
                'field': issue['field'], 'issue_category': POLICY[c][0], 'review_reason': issue['reason'],
                'required_action': POLICY[c][1], 'suggested_reviewers': POLICY[c][2],
                'scope_id': scope['scope_id'], **provenance, 'monday_project_url': url})

    assert len(project_rows) == 96 and len(detail_rows) == 176
    assert sum(r['issue_count'] for r in project_rows) == 176
    assert all(r['date_created'] for r in project_rows)
    assert (project_rows[0]['project_id'], project_rows[0]['date_created']) == ('3240803517', '2026-09-23')
    OUT.mkdir(parents=True, exist_ok=True)
    outputs = [write_csv('projects_96_review.csv', project_rows),
               write_csv('review_issues_176.csv', detail_rows)]
    audit = {
        'generated_at_utc': datetime.now(timezone.utc).isoformat(),
        'basis': 'Saved comparison and successful verification artifacts; no fresh Monday or Supabase reads.',
        'original_selected_projects': 156, 'remaining_review_projects': 96,
        'projects_without_remaining_issues_in_compared_fields': 60,
        'remaining_issue_entries': 176, 'removed_verified_rehydration_issue_entries': len(removed),
        'issue_counts': dict(counts), 'resolved_subitems': hydrate['plan']['targets'],
        'rehydration_verified_at_utc': hydrate['verify']['checked_at'],
        'notes': [
            'Review entries are unresolved comparison questions, not all confirmed bad values or Finance errors.',
            'date_created is the saved Supabase project date_created mapped from Monday Date Created (date9__1), not Monday API created_at.',
            'Project metadata for unavailable Monday items comes from saved Supabase rows; it is not newly verified in Monday.',
            'The retry replaces the original comparison for its 50 selected projects. Verified insertion closes only the 20 exact missing-row issues.',
            'The remaining issues were recorded at comparison staging; verification confirms staged changes, not resolution of these issues.',
            'Monday item state is the API lifecycle state, distinct from the business Status column label Archived.',
            'No projects are omitted merely because their names are nonnumeric or their business status is Archived.',
            'Suggested reviewers are recommendations, not assigned owners. Finance owns any corrections to Monday data.',
            'Multiple values for new_enquiry_value do not establish that SUM is the configured Monday aggregation.',
            'For unavailable projects the Monday URL uses the original parent board and may not resolve.',
        ],
        'sources': [{
            'directory': s['name'], 'run_id': s['manifest']['run_id'],
            'files': {name: sha(BASE / s['name'] / name)
                      for name in ('manifest.json', s['plan_file'], s['verification_file'])}
        } for s in sources],
        'outputs': {p.name: {'sha256': sha(p), 'rows': 96 if n == 0 else 176}
                    for n, p in enumerate(outputs)},
    }
    (OUT / 'provenance.json').write_text(json.dumps(audit, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    print(json.dumps({'directory': str(OUT), 'projects': len(project_rows), 'issues': len(detail_rows),
                      'removed_resolved_entries': len(removed), 'issue_counts': dict(counts),
                      'newest_project': {k: project_rows[0][k] for k in common}}, indent=2))


if __name__ == '__main__':
    main()
