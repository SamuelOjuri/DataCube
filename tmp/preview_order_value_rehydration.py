"""Offline validation against saved evidence; never constructs a live client."""
from collections import Counter
import json
import logging
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import order_value_blocked_review as review
from scripts import order_value_rehydrate as hydrate
from scripts import order_value_scopes_refresh as refresh

logging.getLogger('src.database.sync_service').setLevel(logging.ERROR)
root = Path(__file__).resolve().parents[1]
context, documents = review.load_capture(root / 'outputs/order_value_backfill/blocked_current_20261002')
source, raw, issues = review.source_from_capture(context, documents)
source['project_ids'] = refresh.project_ids_from_file(root / 'scripts/order_value_rehydrate_projects_27.txt')
if any(issues.get(pid) for pid in source['project_ids']):
    raise ValueError('Saved source contains issues for selected projects')
records, deferred = hydrate.build_records(documents['baseline'], source, raw, documents['contract'])
inserts, totals = Counter(), Counter()
for record in records:
    missing, existing = hydrate.split_writes(record['before'], record['updates'])
    inserts.update({t: len(rows) for t, rows in missing.items()})
    totals.update({t: len(rows) for t, rows in record['updates'].items()})
report = {'offline_only': True, 'captured_at': context['finished_at'], 'projects': len(source['project_ids']),
          'scopes': len(records), 'deferred': deferred, 'insert_rows': dict(inserts),
          'transformed_rows': dict(totals),
          'scope_sizes_after_insert': [sum(map(len, r['after'].values())) for r in records],
          'does_not_authorize_apply': True}
print(json.dumps(report, indent=2))
(root / 'outputs/order_value_backfill/blocked_review_20261002/rehydration-preview.json').write_text(
    json.dumps(report, indent=2), encoding='utf-8')
