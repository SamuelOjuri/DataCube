"""Verify exact IDs for the four screenshot projects; Monday queries only."""
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import time

from dotenv import load_dotenv
import requests

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'outputs/order_value_backfill/screenshot_review_20261005'
TARGETS = {
    '18326': ('2915886260', '2916255740', '18326_26.02 - A'),
    '15957': ('1771987887', '2902522882', '15957_24.02 - E'),
    '10306': ('1776390707', '2964050952', '10306_26.01 - A'),
    '18466': ('3002770448', '3002886227', '18466_26.01 - A'),
}
QUERY = '''query ScreenshotExactIdAssessment($ids: [ID!]!) {
  items(ids: $ids, limit: 100, exclude_nonactive: false) {
    id name state board { id } parent_item { id }
    column_values(ids: ["status4__1"]) {
      id type ... on StatusValue { label }
    }
    subitems {
      id name state board { id } parent_item { id }
      column_values(ids: ["mirror11__1"]) {
        id type ... on MirrorValue { display_value }
      }
    }
  }
}'''


def main():
    load_dotenv(ROOT / '.env')
    ids = sorted({i for parent, old, _ in TARGETS.values() for i in (parent, old)})
    for attempt in range(3):
        try:
            response = requests.post('https://api.monday.com/v2',
                headers={'Authorization': os.environ['MONDAY_API_KEY'], 'API-Version': '2026-07'},
                json={'query': QUERY, 'variables': {'ids': ids}}, timeout=(10, 45))
            break
        except requests.exceptions.SSLError:
            raise
        except (requests.ConnectionError, requests.Timeout):
            if attempt == 2:
                raise
            time.sleep(2)
    with response:
        body = response.json()
        OUT.mkdir(parents=True, exist_ok=True)
        (OUT / 'raw.json').write_text(json.dumps(dict(
            captured_at_utc=datetime.now(timezone.utc).isoformat(), query=QUERY, ids=ids,
            api_version=response.headers.get('API-Version'), response=body), indent=2), encoding='utf-8')
        if response.status_code != 200 or body.get('errors'):
            raise RuntimeError('Read failed; see saved error response, no partial evidence accepted')
    index = {item['id']: item for item in body['data']['items']}
    results = []
    for project, (pid, old, old_name) in TARGETS.items():
        parent = index[pid]
        if parent['board']['id'] != '1825117125' or parent['name'] != project:
            raise ValueError('Unexpected parent identity')
        stage = next(c for c in parent['column_values'] if c['id'] == 'status4__1')['label']
        children = []
        for child in parent['subitems']:
            if child['parent_item']['id'] != pid or child['board']['id'] != '1825117144':
                raise ValueError('Unexpected child membership')
            children.append(dict(id=child['id'], name=child['name'], state=child['state'],
                visible_status=next(c for c in child['column_values'] if c['id'] == 'mirror11__1')['display_value']))
        results.append(dict(project=project, parent_id=pid, pipeline_stage=stage,
            status_category='Won' if stage == 'Won - Closed (Invoiced)' else 'Lost' if stage == 'Lost' else 'Open',
            flagged_id=old, flagged_name=old_name, flagged_returned=index.get(old),
            flagged_current_member=old in {c['id'] for c in children}, current_subitems=children,
            current_same_name=[c for c in children if c['name'] == old_name]))
    (OUT / 'assessment.json').write_text(json.dumps(results, indent=2), encoding='utf-8')
    print(json.dumps(results, indent=2))


if __name__ == '__main__':
    main()
