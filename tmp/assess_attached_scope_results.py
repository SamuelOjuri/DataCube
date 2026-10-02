"""Offline comparison of user-supplied GraphQL/SQL observations and reviewed plans."""
import csv
from collections import Counter, defaultdict
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re

from scripts import backfill_order_values as backfill

BASE = Path(r'C:\Users\SamuelOjuri\.codex\attachments')
RUN = Path('outputs/order_value_backfill/targeted_orders_20261002_092604')
OUT = Path('outputs/order_value_backfill/attached_scope_assessment_20261002')
PAIRS = [
    ('2479af30d0f54b2a','77cfd5c4-0f5e-415e-a222-dae7e40ea512','9e3289d0-e31c-4a74-b010-27a751ad9686'),
    ('3330880044595727','a53fd17d-f709-429d-909b-0deaa3382e83','c34415c1-7a24-43d5-b188-7ee88e64a067'),
    ('4f2bd042e18b114a','82fd4946-659f-4120-90de-61f14a1f7888','8cd66129-8b39-4bd7-b333-664998d3e9c8'),
]


def read_result(attachment, pattern):
    text = (BASE/attachment/'Pasted text.txt').read_text(encoding='utf-8-sig')
    match = re.search(pattern, text)
    assert match is not None, attachment
    value, end = json.JSONDecoder(parse_float=Decimal).raw_decode(text[match.start():])
    assert not text[match.start()+end:].strip(), 'Unexpected content after response'
    return value


def metadata(item, children=False):
    parent = item['parent_item']
    row = {'id':item['id'], 'state':item['state'], 'board':{'id':item['board']['id']},
           'parent_item':None if parent is None else {'id':parent['id'], 'state':parent['state'],
                                                    'board':{'id':parent['board']['id']}}}
    if children:
        row['subitems'] = sorted([metadata(child) for child in item['subitems']], key=lambda c:c['id'])
    return row


def diff_rows(old, new, key):
    before, after = backfill.indexed(old,key), backfill.indexed(new,key)
    added, missing, changed = [], [], []
    for item_id in sorted(before.keys() | after.keys()):
        if item_id not in before: added.append(after[item_id])
        elif item_id not in after: missing.append(before[item_id])
        elif before[item_id] != after[item_id]:
            changed.append({'id':item_id, 'fields':{f:{'reviewed':before[item_id].get(f), 'observed':after[item_id].get(f)}
                            for f in before[item_id].keys() | after[item_id].keys()
                            if before[item_id].get(f) != after[item_id].get(f)}})
    return {'added':added,'missing':missing,'changed':changed}


def normalized_hidden(item):
    normalized = backfill.normalize_hidden(item)
    return {k:normalized[k] for k in ('monday_id', *backfill.ORDER_FIELDS, 'monday_total', 'issues')}


plan = json.loads((RUN/'scopes.json').read_text(encoding='utf-8'))
manifest = json.loads((RUN/'manifest.json').read_text(encoding='utf-8'))
assert manifest['sha256'] == backfill.fingerprint(plan)
records = {r['scope_id']:r for r in plan['scopes']}
OUT.mkdir(parents=True, exist_ok=True)
summaries, proposed, project_rows = [], [], []
for suffix, qid, sid in PAIRS:
    scope_id = 'scope-'+suffix
    gql = read_result(qid,r'\{\s*"data"\s*:')
    report = read_result(sid,r'\[\s*\{\s*"verification_report"\s*:')[0]['verification_report']
    assert not gql.get('errors') and report['scope_id'] == scope_id
    record = records[scope_id]
    data = gql['data']
    projects = backfill.indexed(data['projects'],'id')
    assert set(projects) == set(record['scope']['projects'])
    known_children = backfill.indexed(data['knownSubitems'],'id')
    hidden = {item['id']:normalized_hidden(item) for item in data['knownHiddenSources']}
    hidden_metadata = {item['id']:item for item in data['knownHiddenSources']}
    children, links_by_child = [], {}
    validation = []
    def inspect_child(child, listed_parent=None):
        parent = child['parent_item'] or {}
        if (child['state'] != 'active' or child['board']['id'] != backfill.SUBITEM_BOARD_ID
                or parent.get('state') != 'active' or (parent.get('board') or {}).get('id') != backfill.PARENT_BOARD_ID):
            validation.append({'item_id':child['id'],'issue':'invalid_child_metadata'})
        if listed_parent and parent.get('id') != listed_parent:
            validation.append({'item_id':child['id'],'issue':'parent_disagreement'})
        normalized = backfill.normalize_subitem(child)
        if normalized['link_error'] or len(normalized['hidden_ids']) != 1:
            validation.append({'item_id':child['id'],'issue':'ambiguous_link'})
        column = next(c for c in child['column_values'] if c['id']==backfill.SUBITEM_COLUMNS['hidden_item_id'])
        assert set(column['linked_item_ids']) == {r['id'] for r in column['linked_items']}
        for item in column['linked_items']:
            value = normalized_hidden(item)
            if item['id'] in hidden and hidden[item['id']] != value:
                validation.append({'item_id':item['id'],'issue':'hidden_value_changed_between_aliases'})
            hidden[item['id']] = value
            hidden_metadata[item['id']] = item
        return {'monday_id':normalized['monday_id'], 'parent_monday_id':normalized['parent_monday_id'],
                'hidden_ids':normalized['hidden_ids'],'link_error':normalized['link_error'],
                'state':child['state'], 'board_id':child['board']['id'],
                'parent_state':parent.get('state'), 'parent_board_id':(parent.get('board') or {}).get('id')}
    known_links = {i:inspect_child(child) for i,child in known_children.items()}
    for pid, parent in projects.items():
        if parent['state']!='active' or parent['board']['id']!=backfill.PARENT_BOARD_ID or parent['parent_item'] is not None:
            validation.append({'item_id':pid,'issue':'invalid_parent_metadata'})
        for child in parent['subitems']:
            row = inspect_child(child,pid)
            if row['monday_id'] in known_links and known_links[row['monday_id']] != row:
                validation.append({'item_id':row['monday_id'],'issue':'child_changed_between_aliases'})
            children.append(row)
            links_by_child[row['monday_id']] = row
    assert len(children)==len(links_by_child)
    for hid, item in hidden_metadata.items():
        if item['state']!='active' or item['board']['id']!=backfill.HIDDEN_ITEMS_BOARD_ID:
            validation.append({'item_id':hid,'issue':'invalid_hidden_metadata'})
        if hidden[hid]['issues'] or any(hidden[hid][field] is None for field in backfill.ORDER_FIELDS):
            validation.append({'item_id':hid,'issue':'invalid_hidden_amount_or_formula','detail':hidden[hid]})
    owners = defaultdict(list)
    for row in children:
        for hid in row['hidden_ids']: owners[hid].append(row['monday_id'])
    for hid, ids in owners.items():
        if len(ids)!=1: validation.append({'item_id':hid,'issue':'shared_source_in_supplied_monday_results','owners':ids})
    expected = record['source']
    old_hidden_ids={r['monday_id'] for r in expected['hidden_items']}
    monday_differences={
        'parent_metadata':diff_rows(expected['parents'],[metadata(p,True) for p in data['projects']],'id'),
        'child_relationships':diff_rows(expected['subitems'],children,'monday_id'),
        'hidden_order_inputs':diff_rows(expected['hidden_items'],list(hidden.values()),'monday_id'),
    }
    stored = {t:backfill.indexed(report['current_'+{'projects':'projects','subitems':'subitems','hidden_items':'hidden_sources'}[t]])
              for t in ('projects','subitems','hidden_items')}
    scope_proposals=[]
    def compare(table,item_id,field,desired,pid):
        if item_id not in stored[table]:
            validation.append({'item_id':item_id,'issue':'missing_database_row','table':table})
            return
        before=backfill.money(stored[table][item_id].get(field))
        if before != desired:
            scope_proposals.append({'scope_id':scope_id,'project_id':pid,'table':table,'monday_id':item_id,
                                    'field':field,'before':before,'after':desired})
    for pid, parent in projects.items():
        count_before=len(scope_proposals)
        total=Decimal(0)
        parent_validation=[]
        current_db_child_ids={i for i,r in stored['subitems'].items() if r['parent_monday_id']==pid}
        monday_child_ids={child['id'] for child in parent['subitems']}
        if current_db_child_ids!=monday_child_ids:
            parent_validation.append('database_monday_child_sets_differ')
        for child in parent['subitems']:
            row=links_by_child[child['id']]
            if len(row['hidden_ids'])!=1: continue
            hid=row['hidden_ids'][0]
            if child['id'] in stored['subitems'] and stored['subitems'][child['id']]['hidden_item_id']!=hid:
                parent_validation.append('database_hidden_link_differs')
            if hidden[hid]['issues'] or any(hidden[hid][f] is None for f in backfill.ORDER_FIELDS): continue
            total += sum(Decimal(hidden[hid][f]) for f in backfill.ORDER_FIELDS)
            for f in backfill.ORDER_FIELDS:
                compare('hidden_items',hid,f,hidden[hid][f],pid)
                compare('subitems',child['id'],f,hidden[hid][f],pid)
        compare('projects',pid,'total_order_value',format(total,'.2f'),pid)
        project_rows.append({'scope_id':scope_id,'project_id':pid,'project_name':parent['name'],
                             'monday_child_count':len(parent['subitems']), 'database_child_count':len(current_db_child_ids),
                             'monday_order_total':format(total,'.2f'),
                             'database_order_total':backfill.money(stored['projects'].get(pid,{}).get('total_order_value')),
                             'order_field_difference_count':len(scope_proposals)-count_before,
                             'relationship_issues':parent_validation})
        for issue in parent_validation: validation.append({'item_id':pid,'issue':issue})
    proposed.extend(scope_proposals)
    summary={'scope_id':scope_id, 'sql_checked_at':report['checked_at'], 'journal':report['commit_journal'],
             'counts':{'projects':len(projects),'current_children':len(children),'hidden_sources':len(hidden)},
             'validation_issues':validation,'sql_missing_rows':report['missing_requested_rows'],
             'sql_ownership_issues':[r for r in report['source_owner_checks'] if r['stored_owner_count']!=1
                                     or any(not o['parent_is_selected'] for o in r['owners'])],
             'sql_differences_from_review':report['field_or_membership_differences'],
             'monday_differences_from_review':monday_differences,
             'current_database_vs_monday_order_field_differences':len(scope_proposals),
             'projects_needing_order_changes':len({r['project_id'] for r in scope_proposals}),
             'already_matching_projects':[r['project_id'] for r in project_rows if r['scope_id']==scope_id
                                           and r['order_field_difference_count']==0 and not r['relationship_issues']],
             'field_difference_counts':dict(Counter(r['table']+'.'+r['field'] for r in scope_proposals))}
    summaries.append(summary)
    (OUT/f'{scope_id}_assessment.json').write_text(json.dumps(summary,indent=2,default=str),encoding='utf-8')
    (OUT/f'{scope_id}_monday_response.json').write_text(json.dumps(gql,indent=2,default=str),encoding='utf-8')
    (OUT/f'{scope_id}_database_report.json').write_text(json.dumps(report,indent=2,default=str),encoding='utf-8')
    printable={k:v for k,v in summary.items() if k!='sql_differences_from_review'}
    print(json.dumps(printable,indent=2,default=str))

for name, rows, fields in [('proposed_order_differences.csv',proposed,['scope_id','project_id','table','monday_id','field','before','after']),
                          ('project_assessment.csv',project_rows,['scope_id','project_id','project_name','monday_child_count','database_child_count','monday_order_total','database_order_total','order_field_difference_count','relationship_issues'])]:
    with (OUT/name).open('w',encoding='utf-8',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
(OUT/'summary.json').write_text(json.dumps(summaries,indent=2,default=str),encoding='utf-8')
print(json.dumps({'total_order_field_differences':len(proposed),'projects_requiring_order_changes':len({r['project_id'] for r in proposed}),
                  'assessment_directory':str(OUT)},indent=2))
