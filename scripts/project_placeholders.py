"""Review exact placeholder IDs, classify them locally, and archive verified empty Monday items."""
from __future__ import annotations

import argparse
from collections import Counter
import csv
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import json
import os
from pathlib import Path

from dotenv import load_dotenv
import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from scripts.project_reporting import capture, digest, ordered
from scripts.order_value_monday_compare import ComparisonMondayClient, fetch_items
from src.config import PARENT_BOARD_ID, PARENT_COLUMNS

DEFAULT_SELECTION = Path('scripts/project_placeholder_review_20261005.json')
BLANK_FIELDS = ('project_name','type','category','zip_code','sales_representative',
                'funding','feedback','lost_to_who_or_why','expected_start_date','follow_up_date')
NUMERIC_FIELDS = ('gestation_period','probability_percent','project_value','weighted_pipeline',
                  'overall_project_value','total_order_value','new_enq_value_mirror')
ARCHIVE_MUTATION = 'mutation ArchiveReviewedPlaceholder($id: ID!) { archive_item(item_id: $id) { id state } }'


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def source_rows(monday, ids):
    return fetch_items(monday, ids, sorted(set(PARENT_COLUMNS.values())-{'name'}),parents=True)


def source_objections(item):
    """An unavailable source can never authorize a Monday mutation."""
    if not item:
        return ['Monday did not return item; lifecycle unresolved']
    objections = []
    if str((item.get('board') or {}).get('id')) != str(PARENT_BOARD_ID) or item.get('parent_item'):
        objections.append('Unexpected board or parent')
    if item.get('name','').strip().lower() != 'new project':
        objections.append('Item has a meaningful name')
    if item.get('state') not in {'active','archived'}:
        objections.append('Unexpected lifecycle state')
    if item.get('subitems') or 'subitems' not in item:
        objections.append('Subitems exist or membership was not returned')
    columns = {v['id']:v for v in item.get('column_values',[])}
    for field in (*BLANK_FIELDS,'pipeline_stage'):
        col = columns.get(PARENT_COLUMNS[field])
        if col is None:
            objections.append(f'Missing source column: {field}')
            continue
        text = (col.get('text') or '').strip()
        if field=='pipeline_stage':
            if text != 'Open Enquiry':
                objections.append('Project is not an open empty enquiry')
        elif text or col.get('value') not in (None, '', 'null', '""', '{}'):
            objections.append(f'Source contains {field}')
    # Empty parents must not carry hidden mirrored/source relationships.
    for col in columns.values():
        if col.get('linked_item_ids') or col.get('mirrored_items'):
            objections.append('Source has linked or mirrored items')
    for field, column_id in PARENT_COLUMNS.items():
        if field in {'name','date_created','won_vs_open_lost',*BLANK_FIELDS,'pipeline_stage'}:
            continue
        col = columns.get(column_id)
        if col is None:
            objections.append(f'Missing source column: {field}')
            continue
        value = (col.get('display_value') or col.get('text') or '').strip()
        if field in NUMERIC_FIELDS:
            try:
                # Monday returns the literal string "null" for archived formulas.
                # It is missing evidence, not a nonzero amount or verified zero.
                empty = value in ('', 'null') or Decimal(value)==0
            except InvalidOperation:
                empty = False
        else:
            empty = not value
        if not empty:
            objections.append(f'Source contains {field}')
    return sorted(set(objections))


def read_projects(connection, ids, *, lock=False):
    if lock:
        connection.execute('SELECT monday_id FROM public.projects WHERE monday_id=ANY(%s) ORDER BY monday_id FOR UPDATE',(ids,)).fetchall()
    rows = connection.execute('''SELECT p.*, public.project_placeholder_is_empty(to_jsonb(p)) AS empty_candidate,
        (SELECT count(*) FROM public.subitems s WHERE s.parent_monday_id=p.monday_id) AS child_count,
        (SELECT count(*) FROM public.analysis_results a WHERE a.project_id=p.monday_id) AS analysis_count
        FROM public.projects p WHERE p.monday_id=ANY(%s) ORDER BY p.monday_id''',(ids,)).fetchall()
    if {r['monday_id'] for r in rows} != set(ids):
        raise ValueError('Selected project missing from database; review exact IDs')
    return {r['monday_id']:r for r in rows}


def decisions(selection, stored, source):
    result = []
    for item_id in sorted(selection['project_ids']):
        row, item = stored[item_id], source.get(item_id)
        objections = source_objections(item) if item else []
        if not row['empty_candidate'] or row['child_count']:
            objections.append('Stored record has meaningful data or linked subitems')
        if item_id in selection.get('hold_for_review',{}):
            objections.append(selection['hold_for_review'][item_id])
        classification = 'needs_review' if objections else 'redundant_placeholder'
        state = item['state'] if item else 'not_returned'
        result.append(dict(monday_id=item_id,classification=classification,
            monday_state=state,archive_eligible=classification=='redundant_placeholder' and state=='active',
            reason='; '.join(sorted(set(objections))) if objections else selection['reason'],
            lifecycle_resolved=False if state=='not_returned' else True))
    return result


def write_json(path, value):
    path.write_text(json.dumps(value,default=str,indent=2),encoding='utf-8')


def write_csv(path, rows, fields=None):
    fields = fields or list(rows[0])
    with path.open('w',encoding='utf-8-sig',newline='') as stream:
        writer = csv.DictWriter(stream,fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def stable_row(row):
    return {k:v for k,v in row.items() if k not in {'updated_at','last_synced_at','sync_version','analysis_count'}}


def load_stage(run_dir):
    staged = json.loads((run_dir/'stage.json').read_text(encoding='utf-8'))
    if digest(staged)!=json.loads((run_dir/'manifest.json').read_text())['sha256']:
        raise ValueError('Staged decisions changed; stage again')
    return staged


def refresh(connection):
    catalog = capture(connection)
    kinds = {r['name']:r['kind'] for r in catalog['views']}
    from psycopg import sql
    for name in ordered(kinds,catalog['edges']):
        if kinds[name]=='m':
            connection.execute(sql.SQL('REFRESH MATERIALIZED VIEW public.{}').format(sql.Identifier(name)))


def classify(connection, staged, fresh_source, reviewer):
    ids = staged['selection']['project_ids']
    with connection.transaction():
        connection.execute("SET LOCAL lock_timeout='2s'")
        connection.execute("SET LOCAL statement_timeout='15s'")
        stored = read_projects(connection,ids,lock=True)
        # JSON round-trip makes database dates/decimals comparable to the staged file.
        for item in ids:
            if digest(stable_row(stored[item])) != digest(stable_row(staged['stored'][item])):
                raise ValueError(f'Project {item} changed since review; stage again')
        current = decisions(staged['selection'],stored,fresh_source)
        if current != staged['decisions']:
            raise ValueError('Source eligibility or lifecycle changed; stage again')
        for decision in current:
            existing = connection.execute('SELECT * FROM project_reporting_classifications WHERE monday_id=%s FOR UPDATE',
                                          (decision['monday_id'],)).fetchone()
            evidence = dict(stage_sha256=digest(staged),observed_at=utcnow(),
                source_state=decision['monday_state'],source=fresh_source.get(decision['monday_id']),
                before=stored[decision['monday_id']],lifecycle_resolved=decision['lifecycle_resolved'])
            if existing:
                if existing['evidence'].get('stage_sha256')==digest(staged):
                    continue
                raise ValueError('Existing classification requires an explicit review, not overwrite')
            connection.execute('''INSERT INTO project_reporting_classifications
                (monday_id,classification,reason,reviewed_by,evidence) VALUES (%s,%s,%s,%s,%s)''',
                (decision['monday_id'],decision['classification'],decision['reason'],reviewer,
                 Jsonb(json.loads(json.dumps(evidence,default=str)))))
    refresh(connection)


def archive(connection, monday, staged, run_dir):
    results = []
    for decision in staged['decisions']:
        if not decision['archive_eligible']:
            continue
        item_id = decision['monday_id']
        current = source_rows(monday,[item_id]).get(item_id)
        objections = source_objections(current)
        row = read_projects(connection,[item_id])[item_id]
        effective = connection.execute('SELECT reporting_excluded FROM project_reporting_review WHERE monday_id=%s',(item_id,)).fetchone()
        if objections or not row['empty_candidate'] or row['child_count'] or not effective or not effective['reporting_excluded']:
            connection.execute("UPDATE project_reporting_classifications SET classification='needs_review', "
                "reason='Source or stored data changed before archive', reviewed_by='archive verification', reviewed_at=now() "
                "WHERE monday_id=%s AND classification='redundant_placeholder'",(item_id,))
            raise ValueError(f'Project {item_id} no longer eligible for archive: {objections}')
        if current['state']=='archived':
            results.append(dict(monday_id=item_id,status='already_archived'))
            continue
        # Capture reviewable evidence before the reversible external operation.
        write_json(run_dir/f'archive-before-{item_id}.json',dict(observed_at=utcnow(),source=current,stored=row))
        # One bounded request, with no automatic retry after an ambiguous response.
        response = monday.session.post(monday.api_url,headers=monday.headers,
            json={'query':ARCHIVE_MUTATION,'variables':{'id':item_id}},timeout=(10,45))
        response.raise_for_status()
        body = response.json()
        changed = (body.get('data') or {}).get('archive_item') or {}
        if body.get('errors') or str(changed.get('id'))!=item_id:
            raise ValueError(f'Archive response unresolved for {item_id}; re-read before retrying')
        after = source_rows(monday,[item_id]).get(item_id)
        write_json(run_dir/f'archive-after-{item_id}.json',dict(observed_at=utcnow(),source=after))
        if not after or after['state']!='archived' or source_objections(after):
            connection.execute("UPDATE project_reporting_classifications SET classification='needs_review', "
                "reason='Source changed during archive; verify Monday state', reviewed_by='archive verification', reviewed_at=now() "
                "WHERE monday_id=%s AND classification='redundant_placeholder'",(item_id,))
            raise ValueError(f'Archive verification requires review for {item_id}')
        connection.execute("UPDATE project_reporting_classifications SET evidence=evidence || %s "
            "WHERE monday_id=%s AND classification='redundant_placeholder'",
            (Jsonb(dict(archive_verified_at=utcnow(),source_state_after_archive='archived')),item_id))
        results.append(dict(monday_id=item_id,status='archived_verified'))
        write_json(run_dir/'archive-results.json',results)
    write_json(run_dir/'archive-results.json',results)
    return results


def export_review(connection, staged, directory, review_dir):
    directory.mkdir(exist_ok=True)
    ids = staged['selection']['project_ids']
    rows = connection.execute('SELECT * FROM project_reporting_review WHERE monday_id=ANY(%s) ORDER BY monday_id',(ids,)).fetchall()
    write_csv(directory/'placeholder_review.csv',rows)
    counts = {}
    for source_name, prefix in [('projects_96_review.csv','projects'),('review_issues_176.csv','issues')]:
        with (review_dir/source_name).open(encoding='utf-8-sig',newline='') as stream:
            reader = csv.DictReader(stream)
            fields = reader.fieldnames
            source = list(reader)
        selected = [r for r in source if r['project_id'] in ids]
        other = [r for r in source if r['project_id'] not in ids]
        write_csv(directory/f'{prefix}_placeholder_lifecycle_review.csv',selected,fields)
        write_csv(directory/f'{prefix}_business_review.csv',other,fields)
        counts[prefix] = dict(placeholder=len(selected),business=len(other),original=len(source))
    write_json(directory/'provenance.json',dict(generated_at=utcnow(),stage_sha256=digest(staged),counts=counts,
        notes=['Original issue reasons and lifecycle uncertainties retained. Classification does not resolve lifecycle issues.',
               'FREE remains included in reporting and appears in the placeholder review category.',
               'Historical comparison issues are partitioned, not re-certified as current.']))
    return counts


def verify(connection, monday, staged, run_dir, *, require_archived=False):
    ids = staged['selection']['project_ids']
    stored = read_projects(connection,ids)
    source = source_rows(monday,ids)
    rows = connection.execute('SELECT * FROM project_reporting_review WHERE monday_id=ANY(%s) ORDER BY monday_id',(ids,)).fetchall()
    excluded = {r['monday_id'] for r in rows if r['reporting_excluded']}
    expected = {d['monday_id'] for d in staged['decisions'] if d['classification']=='redundant_placeholder'}
    changed = [i for i in ids if digest(stable_row(stored[i]))!=digest(stable_row(staged['stored'][i]))]
    leaked = connection.execute('SELECT monday_id FROM reportable_projects WHERE monday_id=ANY(%s)',(sorted(excluded),)).fetchall()
    forecast_leaked = connection.execute('SELECT project_id FROM vw_pipeline_forecast_project_v1 WHERE project_id=ANY(%s)',(sorted(excluded),)).fetchall()
    pending_archive = [d['monday_id'] for d in staged['decisions'] if d['archive_eligible'] and
                       source.get(d['monday_id'],{}).get('state')!='archived']
    result = dict(verified_at=utcnow(),retained_projects=len(stored),retained_analysis_rows=sum(r['analysis_count'] for r in stored.values()),
        excluded_projects=len(excluded),held_for_review=[r['monday_id'] for r in rows if r['review_status']=='needs_review'],
        source_states=dict(Counter(source.get(i,{}).get('state','not_returned') for i in ids)),
        changed_business_records=changed,reporting_leaks=leaked,forecast_leaks=forecast_leaked,pending_archive=pending_archive,
        expected_exclusions_match=excluded==expected)
    write_json(run_dir/'verification.json',result)
    print(json.dumps(result,indent=2))
    if changed or leaked or forecast_leaked or (require_archived and pending_archive) or excluded!=expected:
        raise ValueError('Verification needs review; see saved verification.json')
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command',choices=['stage','classify','archive','export','verify'])
    parser.add_argument('--run-dir',type=Path,required=True)
    parser.add_argument('--selection',type=Path,default=DEFAULT_SELECTION)
    parser.add_argument('--reviewed-by',default='Samuel Ojuri / requested treatment 2026-10-05')
    parser.add_argument('--require-archived',action='store_true',help='Fail verification if staged archive actions are incomplete')
    parser.add_argument('--review-dir',type=Path,default=Path('outputs/order_value_backfill/manual_review_96_20261005'))
    args = parser.parse_args()
    load_dotenv()
    with psycopg.connect(os.environ['SUPABASE_DB_URL'],autocommit=True,row_factory=dict_row,connect_timeout=10) as connection:
        if args.command=='stage':
            selection = json.loads(args.selection.read_text())
            ids = selection['project_ids']
            if not 1 <= len(ids) <= 100 or len(set(ids))!=len(ids) or any(not i.isdecimal() for i in ids):
                raise ValueError('Supply 1-100 distinct exact numeric project IDs')
            stored = read_projects(connection,ids)
            source = source_rows(ComparisonMondayClient(),ids)
            staged = dict(staged_at=utcnow(),selection=selection,stored=stored,source=source,
                          decisions=decisions(selection,stored,source))
            args.run_dir.mkdir(parents=True,exist_ok=False)
            write_json(args.run_dir/'stage.json',staged)
            write_json(args.run_dir/'manifest.json',dict(sha256=digest(staged)))
            write_csv(args.run_dir/'decisions.csv',staged['decisions'])
            print(json.dumps(dict(classifications=Counter(d['classification'] for d in staged['decisions']),
                states=Counter(d['monday_state'] for d in staged['decisions']),
                archive_eligible=sum(d['archive_eligible'] for d in staged['decisions'])),indent=2))
        else:
            staged = load_stage(args.run_dir)
            if args.command=='classify':
                fresh = source_rows(ComparisonMondayClient(),staged['selection']['project_ids'])
                classify(connection,staged,fresh,args.reviewed_by)
                print('Classifications committed; current aggregates refreshed; raw records and history retained.')
            elif args.command=='archive':
                print(json.dumps(archive(connection,ComparisonMondayClient(),staged,args.run_dir),indent=2))
            elif args.command=='verify':
                verify(connection,ComparisonMondayClient(),staged,args.run_dir,require_archived=args.require_archived)
            else:
                print(json.dumps(export_review(connection,staged,args.run_dir/'review',args.review_dir),indent=2))


if __name__=='__main__':
    main()
