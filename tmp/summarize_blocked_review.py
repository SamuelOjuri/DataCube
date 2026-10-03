"""Local-only annotations of the completed blocked-project review."""
from collections import Counter
import csv
import json
from pathlib import Path

CAPTURE = Path('outputs/order_value_backfill/blocked_current_20261002')
REVIEW = Path('outputs/order_value_backfill/blocked_review_20261002')
baseline = json.loads((CAPTURE / 'baseline.json').read_text())
stored = {t: {r['monday_id'] for r in rows} for t, rows in baseline.items()}
ownership = json.loads((CAPTURE / 'ownership.json').read_text())['items']
report = json.loads((REVIEW / 'report.json').read_text())
selection = {r['project_id'] for r in report['current_project_reviews'] if r['status'] == 'needs_missing_rows'}
missing = []
for child in ownership:
    if child['parent_monday_id'] not in selection:
        continue
    pid = child['parent_monday_id']
    if child['monday_id'] not in stored['subitems']:
        missing.append({'project_id': pid, 'table': 'subitems', 'monday_id': child['monday_id'],
                        'current_hidden_ids': child['hidden_ids']})
    for hid in child['hidden_ids']:
        if hid not in stored['hidden_items']:
            missing.append({'project_id': pid, 'table': 'hidden_items', 'monday_id': hid, 'current_hidden_ids': []})
with (REVIEW / 'missing-rows.csv').open('x', newline='', encoding='utf-8') as stream:
    writer = csv.DictWriter(stream, fieldnames=['project_id', 'table', 'monday_id', 'current_hidden_ids'])
    writer.writeheader()
    writer.writerows(missing)
plan = json.loads((REVIEW / 'repair-run/scopes.json').read_text())
with (REVIEW / 'repair-run/changes.csv').open(newline='', encoding='utf-8') as stream:
    changes = list(csv.DictReader(stream))
field_counts = Counter(r['table'] + '.' + r['field'] for r in changes)
details = {'missing_rows': dict(Counter(r['table'] for r in missing)),
           'missing_row_projects': len(selection),
           'scope_projects': [len(r['scope']['projects']) for r in plan['scopes']],
           'largest_scope_database_rows': max(sum(len(rows) for rows in r['before'].values()) for r in plan['scopes']),
           'changed_rows': dict(Counter(table for table, item in {(r['table'], r['monday_id']) for r in changes})),
           'field_change_counts': dict(field_counts),
           'proposed_row_updates': {t: sum(len(r['updates'][t]) for r in plan['scopes'])
                                    for t in ('projects', 'subitems', 'hidden_items')}}
(REVIEW / 'assessment-details.json').write_text(json.dumps(details, indent=2), encoding='utf-8')
print(json.dumps(details, indent=2))
