"""Read-only Monday column configuration and current subitem value evidence."""
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import time

from dotenv import load_dotenv
import requests

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'outputs/order_value_backfill/new_enquiry_aggregation_verification_20261005'
CONFIG = '''query VerifyEnquiryConfiguration {
  parent: boards(ids: [1825117125]) {
    id name columns(ids: ["lookup_mkqanpbe", "subitems__1", "mirror5__1"]) {
      id title type settings
    }
  }
  child: boards(ids: [1825117144]) {
    id name columns { id title type settings }
  }
}'''
LEGACY_CONFIG = CONFIG.replace(' settings', ' settings_str')
VALUES = '''query VerifyEnquiryValues($ids: [ID!]!) {
  items(ids: $ids, limit: 100, exclude_nonactive: false) {
    id name state board { id }
    column_values(ids: ["lookup_mkqanpbe"]) {
      id type text value
      ... on MirrorValue {
        display_value
        mirrored_items { linked_item { id name } linked_board_id }
      }
    }
    subitems {
      id name state board { id } parent_item { id }
      column_values(ids: ["formula_mkqa31kh"]) {
        id type text value
        ... on FormulaValue { display_value }
      }
    }
  }
}'''


def query(name, document, version, variables=None):
    if document not in (CONFIG, LEGACY_CONFIG, VALUES):
        raise ValueError('Only the fixed read queries are allowed')
    load_dotenv(ROOT / '.env')
    token = os.environ.get('MONDAY_API_KEY')
    if not token:
        raise RuntimeError('MONDAY_API_KEY is not configured')
    for attempt in range(3):
        try:
            response = requests.post('https://api.monday.com/v2',
                headers={'Authorization': token, 'API-Version': version},
                json={'query': document, 'variables': variables or {}}, timeout=(10, 45))
            break
        except requests.exceptions.SSLError:
            raise
        except (requests.ConnectionError, requests.Timeout):
            if attempt == 2:
                raise
            print(json.dumps({'query': name, 'retry': attempt + 1}), flush=True)
            time.sleep(2)
    with response:
        body = response.json()
        record = dict(captured_at_utc=datetime.now(timezone.utc).isoformat(),
            requested_version=version, returned_version=response.headers.get('API-Version'),
            http_status=response.status_code, query=document, variables=variables, response=body)
        OUT.mkdir(parents=True, exist_ok=True)
        (OUT / (name + '.json')).write_text(json.dumps(record, indent=2), encoding='utf-8')
        print(json.dumps({'query': name, 'http_status': response.status_code,
                          'returned_version': record['returned_version'],
                          'errors': body.get('errors')}), flush=True)
        if response.status_code != 200 or body.get('errors'):
            return None
        return body['data']


def main():
    for name, document, version in [('modern_configuration', CONFIG, '2026-07'),
                                    ('legacy_configuration', LEGACY_CONFIG, '2025-07')]:
        data = query(name, document, version)
        if data:
            print(json.dumps({'parent_settings': data['parent'], 'child_formula': [c
                for b in data['child'] for c in b['columns'] if c['id'] in
                {'formula_mkqa31kh', 'mirror03__1', 'mirror77__1'}]}), flush=True)
    selected = json.loads((ROOT / 'scripts/monday_review_cleanup_31_targets.json').read_text())['selected']
    ids = sorted({r['parent_id'] for r in selected if r['item_name'] in
                  {'18687', '15081', '11726', '17214', '18453'}})
    if len(ids) != 5:
        raise ValueError('Expected five selected projects')
    data = query('current_values_direct', VALUES, '2026-07', {'ids': ids})
    if data:
        print(json.dumps(data), flush=True)


if __name__ == '__main__':
    main()
