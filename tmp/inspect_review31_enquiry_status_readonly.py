"""Read both meanings of active and parent pipeline stages; no remote writes."""
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
import json
import os
from pathlib import Path
import time

from dotenv import load_dotenv
import requests

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'outputs/order_value_backfill/review31_enquiry_status_20261005'
QUERY = '''query Review31EnquiryStatuses($ids: [ID!]!) {
  items(ids: $ids, limit: 100, exclude_nonactive: false) {
    id name state board { id }
    column_values(ids: ["status4__1"]) {
      id type ... on StatusValue { label }
    }
    subitems {
      id name state board { id } parent_item { id }
      column_values(ids: ["mirror11__1", "formula_mkqa31kh"]) {
        id type
        ... on FormulaValue { display_value }
        ... on MirrorValue {
          display_value
          mirrored_items { linked_item { id state } linked_board_id
            mirrored_value { ... on StatusValue { label } }
          }
        }
      }
    }
  }
}'''


def main():
    load_dotenv(ROOT / '.env')
    token = os.environ['MONDAY_API_KEY']
    selected = json.loads((ROOT / 'scripts/monday_review_cleanup_31_targets.json').read_text())['selected']
    ids = sorted({r['parent_id'] for r in selected})
    assert len(ids) == 27
    OUT.mkdir(parents=True, exist_ok=True)
    parents = []
    for offset in range(0, len(ids), 5):
        batch = ids[offset:offset + 5]
        for attempt in range(3):
            try:
                response = requests.post('https://api.monday.com/v2',
                    headers={'Authorization': token, 'API-Version': '2026-07'},
                    json={'query': QUERY, 'variables': {'ids': batch}}, timeout=(10, 45))
                break
            except requests.exceptions.SSLError:
                raise
            except (requests.ConnectionError, requests.Timeout):
                if attempt == 2:
                    raise
                time.sleep(2)
        with response:
            body = response.json()
            record = dict(captured_at_utc=datetime.now(timezone.utc).isoformat(),
                api_version=response.headers.get('API-Version'), query=QUERY,
                variables={'ids': batch}, response=body)
            (OUT / f'batch_{offset:03}.json').write_text(json.dumps(record, indent=2), encoding='utf-8')
            if response.status_code != 200 or body.get('errors'):
                raise RuntimeError(json.dumps(body.get('errors')))
            found = body['data']['items']
            if {p['id'] for p in found} != set(batch):
                raise ValueError('Incomplete requested parents')
            parents.extend(found)
            print(f'Read {len(parents)}/27 parent projects', flush=True)
    rows = []
    for parent in parents:
        pipeline = next(c for c in parent['column_values'] if c['id'] == 'status4__1')['label']
        category = 'Won' if pipeline == 'Won - Closed (Invoiced)' else 'Lost' if pipeline == 'Lost' else 'Open'
        for child in parent['subitems']:
            if child['parent_item']['id'] != parent['id'] or child['board']['id'] != '1825117144':
                raise ValueError('Unexpected child membership')
            columns = {c['id']: c for c in child['column_values']}
            status = columns['mirror11__1']
            labels = sorted({r['mirrored_value']['label'] for r in status['mirrored_items']
                             if r.get('mirrored_value') and r['mirrored_value'].get('label') is not None})
            amount = columns['formula_mkqa31kh']['display_value']
            rows.append(dict(project_id=parent['id'], project=parent['name'],
                parent_state=parent['state'], pipeline_stage=pipeline, status_category=category,
                subitem_id=child['id'], subitem=child['name'], item_state=child['state'],
                visible_status_labels=labels, visible_status_display=status['display_value'],
                new_enquiry_value=amount))
    (OUT / 'subitems.json').write_text(json.dumps(rows, indent=2), encoding='utf-8')
    summary = dict(parent_categories=dict(Counter(
        'Won' if p['column_values'][0]['label'] == 'Won - Closed (Invoiced)' else
        'Lost' if p['column_values'][0]['label'] == 'Lost' else 'Open' for p in parents)),
        subitems=len(rows), lifecycle_states=dict(Counter(r['item_state'] for r in rows)),
        visible_statuses=dict(Counter(' | '.join(r['visible_status_labels']) for r in rows)),
        project_18687=[r for r in rows if r['project'] == '18687'])
    (OUT / 'summary.json').write_text(json.dumps(summary, indent=2), encoding='utf-8')
    print(json.dumps(summary, indent=2))


if __name__ == '__main__':
    main()
