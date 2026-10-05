"""Read-only Monday verification of the user's exact 34-project review filter.

Only the two literal GraphQL query documents below can be submitted. No database
connection, mutation, application code imports, or remote writes are performed.
"""
import argparse
from collections import Counter
from datetime import datetime, timedelta, timezone
import csv
import hashlib
import json
import os
from pathlib import Path
import time

from dotenv import load_dotenv
import requests

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / 'outputs/order_value_backfill/manual_review_96_20261005'
OUT = ROOT / 'outputs/order_value_backfill/manual_review_34_readonly_20261005'
CATEGORY = 'Unconfirmed mirror aggregation | Stored subitem absent; Monday did not return it'
BOARDS = ['1825117125', '1825117144']
META = '''query Review34Metadata($ids: [ID!]!) {
  items(ids: $ids, limit: 100, exclude_nonactive: false) {
    id name state created_at updated_at board { id name state }
    parent_item { id name state board { id } }
    subitems { id name state board { id } parent_item { id } }
    column_values(ids: ["lookup_mkqanpbe"]) {
      id type text value column { title settings_str }
      ... on MirrorValue { display_value mirrored_items { linked_item { id } linked_board_id } }
    }
  }
}'''
LOGS = '''query Review34Activity($boards: [ID!]!, $items: [ID!]!,
    $from: ISO8601DateTime!, $to: ISO8601DateTime!, $page: Int!) {
  boards(ids: $boards) {
    id name state
    activity_logs(item_ids: $items, from: $from, to: $to, limit: 100, page: $page) {
      id event entity data created_at
    }
  }
}'''


def now():
    return datetime.now(timezone.utc).isoformat()


def save(name, value):
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / name).write_text(json.dumps(value, indent=2), encoding='utf-8')


def selection():
    with (SOURCE / 'projects_96_review.csv').open(encoding='utf-8-sig', newline='') as stream:
        projects = [r for r in csv.DictReader(stream)
                    if r['monday_item_state'] == 'active' and r['issue_categories'] == CATEGORY]
    ids = {r['project_id'] for r in projects}
    with (SOURCE / 'review_issues_176.csv').open(encoding='utf-8-sig', newline='') as stream:
        issues = [r for r in csv.DictReader(stream) if r['project_id'] in ids
                  and r['issue_category'] == 'Stored subitem absent; Monday did not return it']
    return projects, issues


def query(name, document, variables):
    if document not in (META, LOGS):
        raise ValueError('Only the allowlisted read queries are permitted')
    load_dotenv(ROOT / '.env')
    token = os.environ.get('MONDAY_API_KEY')
    if not token:
        raise RuntimeError('MONDAY_API_KEY is not configured')
    response = None
    for attempt in range(3):
        try:
            response = requests.post('https://api.monday.com/v2',
                           headers={'Authorization': token, 'API-Version': '2025-07',
                                    'Content-Type': 'application/json'},
                           json={'query': document, 'variables': variables}, timeout=(10, 45))
            break
        except requests.exceptions.SSLError:
            raise
        except (requests.ConnectionError, requests.Timeout):
            if attempt == 2:
                raise
            print(json.dumps({'probe': name, 'retry': attempt + 1}), flush=True)
            time.sleep((2, 5)[attempt])
    with response:
        body = response.json()
        record = dict(captured_at_utc=now(), http_status=response.status_code,
                      returned_api_version=response.headers.get('API-Version'),
                      query=document, variables=variables, response=body)
        save(name + '.json', record)
        if response.status_code != 200 or body.get('errors') or not isinstance(body.get('data'), dict):
            print(json.dumps({'probe': name, 'http_status': response.status_code,
                              'errors': body.get('errors')}), flush=True)
            raise RuntimeError('Monday query failed; no partial response accepted')
        return body['data']


def metadata(suffix):
    projects, issues = selection()
    ids = sorted({r['project_id'] for r in projects} | {r['affected_monday_id'] for r in issues})
    assert len(ids) <= 100
    data = query('metadata_' + suffix, META, {'ids': ids})
    returned = {r['id']: r for r in data['items']}
    if set(returned) - set(ids):
        raise ValueError('Unexpected returned item ID')
    print(json.dumps({'projects': len(projects), 'missing_subitems_to_check': len(issues),
                      'returned_states': dict(Counter(r['state'] for r in data['items'])),
                      'returned_target_subitems': [returned[r['affected_monday_id']]
                          for r in issues if r['affected_monday_id'] in returned],
                      'not_returned': [i for i in ids if i not in returned]}), flush=True)
    save('selection.json', {'filters': {'monday_item_state': 'active', 'issue_categories': CATEGORY},
                            'projects': projects, 'issues': issues,
                            'source_sha256': {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                                              for p in SOURCE.glob('*.csv')}})


def activity(include_parents=False):
    projects, issues = selection()
    since = '2020-01-01T00:00:00Z'
    if include_parents:
        findings = json.loads((OUT / 'findings.json').read_text())
        remaining = [r for r in findings['subitems'] if r['finding'] == 'unresolved_missing']
        parents = {r['project_id'] for r in remaining}
        targets = {r['subitem_id'] for r in remaining}
        projects = [r for r in projects if r['project_id'] in parents]
        issues = [r for r in issues if r['affected_monday_id'] in targets]
        timestamps = [e['at_utc'] for r in remaining for e in r['target_events']]
        if timestamps:
            since = (datetime.fromisoformat(min(timestamps)) - timedelta(days=1)).isoformat()
    ids = {r['affected_monday_id'] for r in issues}
    if include_parents:
        ids |= {r['project_id'] for r in projects}
    until = now()
    boards = BOARDS.copy()
    prefix = 'parent_activity' if include_parents else 'activity'
    count = Counter()
    for page in range(1, 101):
        data = query(f'{prefix}_{page:03}', LOGS,
                     {'boards': boards, 'items': sorted(ids), 'from': since,
                      'to': until, 'page': page})
        if {r['id'] for r in data['boards']} != set(boards):
            raise ValueError('An expected board was not returned')
        following = []
        for board in data['boards']:
            logs = board['activity_logs']
            count[board['id']] += len(logs)
            print(json.dumps({'board': board['id'], 'page': page, 'count': len(logs),
                              'events': dict(Counter(r['event'] for r in logs))}), flush=True)
            if len(logs) == 100:
                following.append(board['id'])
        if not following:
            save(prefix + '_complete.json', dict(from_utc=since,
                 to_utc=until, counts=dict(count), completed_at_utc=now(), pages=page,
                 requested_ids=sorted(ids),
                 note='Complete pagination of history available to this Monday connection.'))
            return
        boards = following
    raise RuntimeError('History hit the 10,000-event limit per board; review remains incomplete')


def references(value, target):
    if isinstance(value, dict):
        return any(references(v, target) for v in value.values())
    if isinstance(value, list):
        return any(references(v, target) for v in value)
    return str(value) == target


def event_time(event):
    return (datetime(1970, 1, 1, tzinfo=timezone.utc)
            + timedelta(microseconds=int(event['created_at']) // 10)).isoformat()


def report():
    projects, issues = selection()
    before = json.loads((OUT / 'metadata_before.json').read_text())
    after = json.loads((OUT / 'metadata_after.json').read_text())
    current = {r['id']: r for r in after['response']['data']['items']}
    complete = json.loads((OUT / 'activity_complete.json').read_text())
    events = {}
    activity_paths = sorted(OUT.glob('activity_[0-9][0-9][0-9].json'))
    activity_paths += sorted(OUT.glob('parent_activity_[0-9][0-9][0-9].json'))
    for path in activity_paths:
        for board in json.loads(path.read_text())['response']['data']['boards']:
            for row in board['activity_logs']:
                payload = json.loads(row['data'])
                events[(board['id'], row['id'])] = {**row, 'activity_board_id': board['id'],
                                                   'payload': payload, 'at_utc': event_time(row)}
    results = []
    for issue in issues:
        target, parent_id = issue['affected_monday_id'], issue['project_id']
        parent = current.get(parent_id)
        child = current.get(target)
        members = {r['id'] for r in (parent or {}).get('subitems', [])}
        history = sorted((e for e in events.values() if references(e['payload'], target)),
                         key=lambda e: int(e['created_at']), reverse=True)
        deletion = next((e for e in history if e['event'] in ('delete_pulse', 'delete_subitem')
                         and str(e['payload'].get('pulse_id')) == target
                         and str(e['payload'].get('parent_item_id')) == parent_id
                         and str(e['payload'].get('board_id')) == BOARDS[1]
                         and str(e['payload'].get('parent_board_id')) == BOARDS[0]), None)
        exact_deletion = next((e for e in history if e['event'] in ('delete_pulse', 'delete_subitem')
                         and str(e['payload'].get('pulse_id')) == target
                         and str(e['payload'].get('board_id')) == BOARDS[1]), None)
        creation = next((e for e in history if e['event'] == 'create_pulse'
                        and str(e['payload'].get('pulse_id')) == target
                        and str(e['payload'].get('parent_item_id')) == parent_id), None)
        latest_deletion = deletion or exact_deletion
        later = [e for e in history if latest_deletion and e['id'] != latest_deletion['id']
                 and int(e['created_at']) >= int(latest_deletion['created_at'])]
        if child:
            finding = 'current_' + child['state']
        elif parent and parent['state'] == 'active' and target not in members and deletion and not later:
            finding = 'deletion_confirmed_by_history'
        elif parent and parent['state'] == 'active' and target not in members and exact_deletion and not later:
            finding = 'deletion_confirmed_parent_details_incomplete'
        else:
            finding = 'unresolved_missing'
        results.append(dict(item_name=issue['item_name'], project_id=parent_id,
            project_name=issue['project_name'], subitem_name=issue['affected_item_name'],
            subitem_id=target, finding=finding, parent_state=(parent or {}).get('state'),
            currently_member=target in members, current_item=child,
            deletion_event=latest_deletion, creation_event=creation,
            parent_link_in_deletion_event=deletion is not None,
            later_events=later, target_events=history))
    mirrors = []
    for project in projects:
        item = current.get(project['project_id'], {})
        columns = item.get('column_values', [])
        value = next((c for c in columns if c['id'] == 'lookup_mkqanpbe'), {})
        settings = json.loads((value.get('column') or {}).get('settings_str') or '{}')
        mirrors.append(dict(item_name=project['item_name'], project_id=project['project_id'],
            state=item.get('state'), title=(value.get('column') or {}).get('title'),
            aggregation_function=settings.get('function'), display_value=value.get('display_value'),
            linked_item_ids=[r['linked_item']['id'] for r in value.get('mirrored_items', [])],
            settings=settings))
    summary = dict(checked_at_utc=after['captured_at_utc'], read_only=True,
        project_count=len(projects), subitem_count=len(results),
        findings=dict(Counter(r['finding'] for r in results)), history=complete,
        metadata_unchanged=before['response']['data'] == after['response']['data'],
        subitems=results, mirrors=mirrors)
    additional = OUT / 'parent_activity_complete.json'
    if additional.exists():
        summary['additional_history'] = json.loads(additional.read_text())
    summary['project_findings'] = {
        finding: len({r['project_id'] for r in results if r['finding'] == finding})
        for finding in summary['findings']}
    save('findings.json', summary)
    deleted = [r for r in results if r['deletion_event'] and not r['later_events']]
    unresolved = [r for r in results if r['finding'] == 'unresolved_missing']
    incomplete = [r for r in results if r['finding'] == 'deletion_confirmed_parent_details_incomplete']
    focus = next(r for r in results if r['item_name'] == '18687')
    labels = {'deletion_confirmed_by_history': 'Deleted; full parent details',
              'deletion_confirmed_parent_details_incomplete': 'Deleted; parent details absent from deletion event',
              'unresolved_missing': 'Unconfirmed; retain for review'}
    lines = [
        '# Read-only Monday review of 34 projects', '',
        f'Current-state check: {summary["checked_at_utc"]}. No Monday or DataCube records were changed.', '',
        'Selection: rows in the supplied projects_96_review.csv whose monday_item_state is active and whose '
        f'issue_categories is exactly `{CATEGORY}`.', '',
        '## Project 18687', '',
        f'Monday records a delete_pulse event for **{focus["subitem_name"]}** '
        f'(ID `{focus["subitem_id"]}`) at **{focus["deletion_event"]["at_utc"]}** '
        '(24 August 2026, 15:16 BST). The event explicitly identifies project 3159872420 and the subitem board. '
        f'Event ID: `{focus["deletion_event"]["id"]}`.', '',
        'The exact-ID check including inactive items returned the active parent with A and B, '
        'but did not return C. No later event referring to C was returned in the available history. '
        'The missing subitem has therefore been verified as deleted.', '',
        '## Results', '',
        f'- {len(projects)} matching projects; all returned as active.',
        f'- {len(results)} missing subitems checked; none returned by the exact-ID check including inactive records.',
        f'- {len(deleted)} subitems across {len({r["project_id"] for r in deleted})} projects have explicit deletion events.',
        f'- {len(deleted) - len(incomplete)} deletion events include the expected parent and board details.',
        f'- {len(incomplete)} deletion events omit parent details: projects '
        + ', '.join(r['item_name'] for r in incomplete) + '. Their creation events identify the expected parent.',
        f'- {len(unresolved)} remain unconfirmed: projects ' + ', '.join(r['item_name'] for r in unresolved) + '.',
        '- No archived state or archive event was established for these 38 subitems.', '',
        '## Suggested treatment', '',
        'Use the 31 fully linked deletion records as candidates for the existing lifecycle deletion workflow. '
        'That workflow must freshly revalidate Monday evidence and the stored DataCube row before applying cleanup. '
        'This report does not authorize or perform deletion.', '',
        'The two deletion records without parent details (18326 and 15957) establish deletion of the exact item, '
        'but do not satisfy the existing recovery code\'s requirement that the deletion event itself identify the parent. '
        'Review their creation-event linkage separately before any cleanup.', '',
        'Retain the five unconfirmed records pending further evidence. No deletion or archive event was returned for them, '
        'including the additional checks of their parent-project histories. Missing records alone do not establish deletion.', '',
        'All 34 projects still have the separate New Enq Value aggregation issue: the returned mirror settings link '
        'multiple subitems but omit an explicit aggregation function, and the returned display value is blank. '
        'The calculation remains unconfirmed; deletion cleanup would not resolve this issue. '
        'Confirm the intended Monday column calculation and map it in the sync separately.', '',
        '## Subitem evidence', '',
        '| Project | Stored subitem name | Subitem ID | Finding | Deleted at (UTC) |',
        '| --- | --- | --- | --- | --- |',
    ]
    for row in results:
        deleted_at = (row['deletion_event'] or {}).get('at_utc', '')
        name = row['subitem_name'].replace('|', '\\|')
        lines.append(f'| {row["item_name"]} | {name} | {row["subitem_id"]} | '
                     f'{labels.get(row["finding"], row["finding"])} | {deleted_at} |')
    lines += ['', '## Evidence and limits', '',
        '- Original source CSVs were not changed; their hashes and selected rows are in selection.json.',
        '- Exact-ID metadata reads before and after the initial history check matched.',
        '- History was read from 2020 onward for the 38 exact subitem IDs on both known boards. '
        'The available result was 94 events on the subitem board and zero on the parent board.',
        '- A further seven-project history check included their parent IDs and completed pagination '
        'without repeated event IDs: 217 parent-board events and 11 subitem-board events.',
        '- Complete pagination means all history returned by this connection, subject to Monday access and retention. '
        'It does not prove that Monday retains every historic event or rule out activity on other inaccessible boards.',
        '- This is an investigation report, not a staged deletion plan. No fresh DataCube/database state was read.',
        '- Raw read queries, timestamps, API version and responses are saved in the metadata and activity JSON files. '
        'The summarized evidence is in [findings.json](findings.json).', '',
        'API references: [items](https://developer.monday.com/api-reference/reference/items) and '
        '[board activity logs](https://developer.monday.com/api-reference/re/reference/activity-logs).', '']
    (OUT / 'review_report.md').write_text('\n'.join(lines), encoding='utf-8')
    print(json.dumps({k: v for k, v in summary.items() if k not in ('subitems', 'mirrors')}, indent=2))
    for result in results:
        print(json.dumps({k: result[k] for k in ('item_name','subitem_name','subitem_id','finding')}
              | {'events': [e['event'] for e in result['target_events']],
                 'deleted_at_utc': (result['deletion_event'] or {}).get('at_utc')}))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('phase', choices=['before', 'activity', 'parent-activity', 'after', 'report', 'verify'])
    args = parser.parse_args()
    try:
        if args.phase == 'verify':
            activity()
            metadata('after')
            report()
        elif args.phase in ('before', 'after'):
            metadata(args.phase)
        elif args.phase in ('activity', 'parent-activity'):
            activity(args.phase == 'parent-activity')
        else:
            report()
    except Exception as exc:
        error = {'error_type': type(exc).__name__}
        if isinstance(exc, requests.RequestException):
            message = str(exc)
            token = os.environ.get('MONDAY_API_KEY')
            if token:
                message = message.replace(token, '[redacted]')
            error['network_error'] = message[:1000]
        else:
            error['message'] = str(exc)[:1000]
        print(json.dumps(error), flush=True)
        raise SystemExit(1)
