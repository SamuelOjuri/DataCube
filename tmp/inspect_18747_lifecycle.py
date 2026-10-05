"""Read-only exact-ID investigation of project 18747; no remote mutations."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path

from dotenv import load_dotenv
import requests

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / '.env')
TARGET = '3201675663'
PARENT = '3199168336'
VISIBLE = '3200638626'
BOARDS = ['1825117125', '1825117144']
OUT = ROOT / 'outputs/order_value_backfill/project_18747_lifecycle_20261005'


def save(name, record):
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / name).write_text(json.dumps(record, indent=2, default=str), encoding='utf-8')


def query(name, document, variables):
    if not document.lstrip().startswith('query '):
        raise ValueError('Only GraphQL queries are allowed')
    token = os.environ.get('MONDAY_API_KEY')
    if not token:
        raise RuntimeError('MONDAY_API_KEY is not configured')
    with requests.post('https://api.monday.com/v2',
                       headers={'Authorization': token, 'API-Version': '2025-07',
                                'Content-Type': 'application/json'},
                       json={'query': document, 'variables': variables},
                       timeout=(10, 45)) as response:
        body = response.json()
        record = {'captured_at_utc': datetime.now(timezone.utc).isoformat(),
                  'http_status': response.status_code,
                  'returned_api_version': response.headers.get('API-Version'),
                  'query': document, 'variables': variables, 'response': body}
        save(name + '.json', record)
        if response.status_code != 200 or body.get('errors'):
            print(json.dumps({'probe': name, 'http_status': response.status_code,
                              'errors': body.get('errors')}, default=str), flush=True)
            raise RuntimeError('Monday query failed; saved diagnostic response')
        return body['data']


def metadata():
    result = query('metadata', '''query Lifecycle18747($ids: [ID!]!) {
      items(ids: $ids, limit: 100, exclude_nonactive: false) {
        id name state created_at updated_at board { id name state }
        parent_item { id name state board { id name } }
        subitems { id name state board { id } parent_item { id } }
      }
    }''', {'ids': [TARGET, PARENT, VISIBLE]})
    print(json.dumps(result, indent=2), flush=True)


def logs():
    document = '''query LifecycleLogs18747($boards: [ID!]!, $items: [ID!]!, $page: Int!, $to: ISO8601DateTime!) {
      boards(ids: $boards) {
        id name state
        activity_logs(item_ids: $items, from: "2026-09-01T00:00:00Z", to: $to, limit: 100, page: $page) {
          id event entity data user_id created_at
        }
      }
    }'''
    until = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    boards = BOARDS.copy()
    for page in range(1, 11):
        result = query(f'activity_page_{page}', document,
                       {'boards': boards, 'items': [TARGET, PARENT, VISIBLE],
                        'page': page, 'to': until})
        next_boards = []
        for board in result['boards']:
            rows = board['activity_logs']
            matches = [r for r in rows if TARGET in str(r.get('data'))]
            print(json.dumps({'board_id': board['id'], 'page': page, 'count': len(rows),
                              'target_events': matches}, indent=2), flush=True)
            if len(rows) == 100:
                next_boards.append(board['id'])
        if not next_boards:
            break
        boards = next_boards


def database():
    import psycopg
    from psycopg.rows import dict_row
    dsn = os.environ.get('SUPABASE_DB_URL')
    if not dsn:
        print('SUPABASE_DB_URL is not configured')
        return
    result = {}
    with psycopg.connect(dsn, connect_timeout=10, row_factory=dict_row,
                        options='-c default_transaction_read_only=on -c statement_timeout=10000',
                        application_name='datacube-18747-read-only-investigation') as connection:
        result['subitems'] = connection.execute(
            'SELECT monday_id,item_name,parent_monday_id,hidden_item_id,created_at '
            'FROM public.subitems WHERE monday_id=ANY(%s)', ([TARGET, VISIBLE],)).fetchall()
        for table, field in [('monday_lifecycle_events', 'item_id'),
                             ('monday_item_lifecycle', 'monday_id'),
                             ('monday_lifecycle_audit', 'monday_id')]:
            exists = connection.execute('SELECT to_regclass(%s) AS table_name',
                                        ('public.' + table,)).fetchone()['table_name']
            if exists:
                from psycopg import sql
                result[table] = connection.execute(sql.SQL(
                    'SELECT * FROM public.{} WHERE {}=ANY(%s) LIMIT 100').format(
                        sql.Identifier(table), sql.Identifier(field)),
                    ([TARGET, PARENT],)).fetchall()
            else:
                result[table] = {'table_present': False}
    result['captured_at_utc'] = datetime.now(timezone.utc).isoformat()
    save('database.json', result)
    print(json.dumps(result, indent=2, default=str), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('probe', choices=['metadata', 'logs', 'database'])
    args = parser.parse_args()
    try:
        {'metadata': metadata, 'logs': logs, 'database': database}[args.probe]()
    except Exception as exc:
        error = {'failed_probe': args.probe, 'error_type': type(exc).__name__}
        if isinstance(exc, requests.RequestException):
            message = str(exc)
            for key in ('MONDAY_API_KEY', 'SUPABASE_DB_URL'):
                if os.environ.get(key):
                    message = message.replace(os.environ[key], '[redacted]')
            error['network_error'] = message[:1000]
        print(json.dumps(error), flush=True)
        raise SystemExit(1)
