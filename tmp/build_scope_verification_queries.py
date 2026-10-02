"""Generate read-only diagnostic queries from local artifacts. Never connects."""
import csv
import hashlib
import json
from pathlib import Path


RUN = Path('outputs/order_value_backfill/targeted_orders_20261002_092604')
OUT = Path('outputs/order_value_backfill/verification_queries_20261002_3scopes')
ORDER = ['scope-2479af30d0f54b2a', 'scope-3330880044595727', 'scope-4f2bd042e18b114a']
FIELDS = {
    'projects': ['monday_id', 'item_name', 'total_order_value', 'new_enquiry_value',
                 'total_amount_invoiced', 'date_order_received'],
    'subitems': ['monday_id', 'parent_monday_id', 'hidden_item_id', 'cust_order_value_material',
                'cust_additional_charges', 'amount_invoiced', 'invoice_date', 'date_order_received'],
    'hidden_items': ['monday_id', 'cust_order_value_material', 'cust_additional_charges',
                    'amount_invoiced', 'invoice_date', 'date_order_received'],
}

GRAPHQL = '''# Scope: SCOPE_ID
# Read only. All IDs are filled in; paste the entire file into Monday Playground.
# Current children are discovered under projects; previously known rows are also
# fetched by ID so disappeared/reparented/unlinked records remain inspectable.
query VerifyOrderScope {
  projects: items(ids: PROJECT_IDS, limit: 100, exclude_nonactive: false) {
    id
    name
    state
    updated_at
    board { id name }
    parent_item { id state board { id } }
    column_values(ids: ["text3__1", "status4__1", "mirror5__1"]) {
      id
      type
      text
      value
      ... on MirrorValue { display_value }
    }
    subitems { ...SubitemEvidence }
  }
  knownSubitems: items(ids: SUBITEM_IDS, limit: 100, exclude_nonactive: false) {
    ...SubitemEvidence
  }
  knownHiddenSources: items(ids: HIDDEN_IDS, limit: 100, exclude_nonactive: false) {
    ...HiddenEvidence
  }
}

fragment SubitemEvidence on Item {
  id
  name
  state
  updated_at
  board { id name }
  parent_item { id state board { id } }
  column_values(ids: ["connect_boards8__1"]) {
    id
    type
    value
    ... on BoardRelationValue {
      linked_item_ids
      linked_items { ...HiddenEvidence }
    }
  }
}

fragment HiddenEvidence on Item {
  id
  name
  state
  updated_at
  board { id name }
  column_values(ids: [
    "numbers98__1"     # Customer material order value
    "numbers3__1"      # Customer additional charges
    "formula_mkncjq9"  # Total order value: material + additional charges
    "numbers56__1"     # Amount invoiced (context)
    "date7__1"         # Date order received (context)
    "date42__1"        # Invoice date (context)
  ]) {
    id
    type
    text
    value
    ... on FormulaValue { display_value }
  }
}
'''

SQL = '''-- Scope: SCOPE_ID. Reference: REFERENCE_STATE.
-- READ ONLY: one SELECT, one database snapshot, no writes and no row locks.
-- Run using the privileged SQL Editor/operator role with full visibility.
-- Database consistency is not proof of agreement with Monday; compare the
-- companion GraphQL results. NULL components remain unknown in these checks.
WITH
input AS (
  SELECT $scope_evidence$
PAYLOAD
$scope_evidence$::jsonb AS data
),
project_ids AS (
  SELECT jsonb_array_elements_text(data->'project_ids') AS monday_id FROM input
),
known_subitem_ids AS (
  SELECT jsonb_array_elements_text(data->'known_subitem_ids') AS monday_id FROM input
),
known_hidden_ids AS (
  SELECT jsonb_array_elements_text(data->'known_hidden_ids') AS monday_id FROM input
),
selected_children AS (
  SELECT s.* FROM public.subitems s
  WHERE s.parent_monday_id IN (SELECT monday_id FROM project_ids)
     OR s.monday_id IN (SELECT monday_id FROM known_subitem_ids)
),
source_ids AS (
  SELECT monday_id FROM known_hidden_ids
  UNION
  SELECT hidden_item_id FROM selected_children WHERE hidden_item_id IS NOT NULL
),
current_projects AS (
  SELECT PROJECT_FIELDS FROM public.projects p
  WHERE p.monday_id IN (SELECT monday_id FROM project_ids)
),
current_subitems AS (
  SELECT SUBITEM_FIELDS FROM public.subitems s
  WHERE s.monday_id IN (SELECT monday_id FROM selected_children)
     OR s.hidden_item_id IN (SELECT monday_id FROM source_ids)
),
current_hidden AS (
  SELECT HIDDEN_FIELDS FROM public.hidden_items h
  WHERE h.monday_id IN (SELECT monday_id FROM source_ids)
),
-- Typed conversion avoids false differences between JSON strings and numeric DB values.
expected_projects AS (
  SELECT EXPECTED_PROJECT_FIELDS FROM input i
  CROSS JOIN LATERAL jsonb_populate_recordset(NULL::public.projects, i.data->'reference'->'projects') e
),
expected_subitems AS (
  SELECT EXPECTED_SUBITEM_FIELDS FROM input i
  CROSS JOIN LATERAL jsonb_populate_recordset(NULL::public.subitems, i.data->'reference'->'subitems') e
),
expected_hidden AS (
  SELECT EXPECTED_HIDDEN_FIELDS FROM input i
  CROSS JOIN LATERAL jsonb_populate_recordset(NULL::public.hidden_items, i.data->'reference'->'hidden_items') e
),
actual_rows AS (
  SELECT 'projects'::text AS table_name, p.monday_id, to_jsonb(p) AS values FROM current_projects p
  UNION ALL
  SELECT 'subitems', s.monday_id, to_jsonb(s) FROM current_subitems s
  UNION ALL
  SELECT 'hidden_items', h.monday_id, to_jsonb(h) FROM current_hidden h
),
expected_rows AS (
  SELECT 'projects'::text AS table_name, p.monday_id, to_jsonb(p) AS values FROM expected_projects p
  UNION ALL
  SELECT 'subitems', s.monday_id, to_jsonb(s) FROM expected_subitems s
  UNION ALL
  SELECT 'hidden_items', h.monday_id, to_jsonb(h) FROM expected_hidden h
),
missing_requested_rows AS (
  SELECT 'projects'::text AS table_name, k.monday_id FROM project_ids k
  LEFT JOIN current_projects p USING (monday_id) WHERE p.monday_id IS NULL
  UNION ALL
  SELECT 'subitems', k.monday_id FROM known_subitem_ids k
  LEFT JOIN current_subitems s USING (monday_id) WHERE s.monday_id IS NULL
  UNION ALL
  SELECT 'hidden_items', k.monday_id FROM source_ids k
  LEFT JOIN current_hidden h USING (monday_id) WHERE h.monday_id IS NULL
),
differences AS (
  SELECT COALESCE(e.table_name, a.table_name) AS table_name,
         COALESCE(e.monday_id, a.monday_id) AS monday_id,
         CASE WHEN e.monday_id IS NULL THEN 'additional_row_since_review'
              WHEN a.monday_id IS NULL THEN 'missing_reviewed_row'
              ELSE 'field_values_changed' END AS difference,
         e.values AS reviewed_values, a.values AS current_values,
         (SELECT COALESCE(jsonb_agg(jsonb_build_object(
                    'field', f.key, 'reviewed', f.value, 'current', a.values->f.key)
                    ORDER BY f.key), '[]'::jsonb)
          FROM jsonb_each(COALESCE(e.values, '{}'::jsonb)) f
          WHERE f.value IS DISTINCT FROM a.values->f.key) AS changed_fields
  FROM expected_rows e
  FULL JOIN actual_rows a USING (table_name, monday_id)
  WHERE e.values IS DISTINCT FROM a.values
),
child_checks AS (
  SELECT s.monday_id, s.parent_monday_id, s.hidden_item_id,
         s.parent_monday_id IN (SELECT monday_id FROM project_ids) AS parent_is_selected,
         s.monday_id IN (SELECT monday_id FROM expected_subitems) AS child_was_reviewed,
         h.monday_id IS NOT NULL AS hidden_row_exists,
         s.cust_order_value_material AS subitem_material,
         s.cust_additional_charges AS subitem_additional_charges,
         h.cust_order_value_material AS hidden_material,
         h.cust_additional_charges AS hidden_additional_charges,
         s.cust_order_value_material + s.cust_additional_charges AS subitem_total,
         h.cust_order_value_material + h.cust_additional_charges AS hidden_total,
         CASE WHEN h.monday_id IS NULL THEN 'missing_or_unlinked_hidden_source'
              WHEN s.cust_order_value_material IS NULL OR s.cust_additional_charges IS NULL
                OR h.cust_order_value_material IS NULL OR h.cust_additional_charges IS NULL
                THEN 'unknown_order_component'
              WHEN s.cust_order_value_material IS DISTINCT FROM h.cust_order_value_material
                OR s.cust_additional_charges IS DISTINCT FROM h.cust_additional_charges
                THEN 'subitem_hidden_amount_mismatch'
              ELSE 'stored_amounts_match' END AS amount_status
  FROM current_subitems s LEFT JOIN current_hidden h ON h.monday_id = s.hidden_item_id
),
rollups AS (
  SELECT p.monday_id,
         count(s.monday_id) AS child_count,
         count(s.monday_id) FILTER (WHERE s.cust_order_value_material IS NULL
                                      OR s.cust_additional_charges IS NULL) AS unknown_child_count,
         count(s.monday_id) FILTER (WHERE h.monday_id IS NULL
                                      OR h.cust_order_value_material IS NULL
                                      OR h.cust_additional_charges IS NULL) AS unknown_hidden_count,
         CASE WHEN count(s.monday_id) > 0
                AND count(s.monday_id) FILTER (WHERE s.cust_order_value_material IS NULL
                                                 OR s.cust_additional_charges IS NULL) = 0
              THEN sum(s.cust_order_value_material + s.cust_additional_charges)
              END AS total_from_stored_children,
         CASE WHEN count(s.monday_id) > 0
                AND count(s.monday_id) FILTER (WHERE h.monday_id IS NULL
                                                 OR h.cust_order_value_material IS NULL
                                                 OR h.cust_additional_charges IS NULL) = 0
              THEN sum(h.cust_order_value_material + h.cust_additional_charges)
              END AS total_from_stored_hidden_sources
  FROM project_ids p
  LEFT JOIN current_subitems s ON s.parent_monday_id = p.monday_id
  LEFT JOIN current_hidden h ON h.monday_id = s.hidden_item_id
  GROUP BY p.monday_id
),
project_checks AS (
  SELECT r.*, p.item_name, p.total_order_value,
         p.monday_id IS NOT NULL AS project_row_exists,
         CASE WHEN p.monday_id IS NULL THEN 'missing_project'
              WHEN r.child_count = 0 THEN 'no_stored_children_requires_review'
              WHEN r.unknown_child_count > 0 OR r.unknown_hidden_count > 0
                THEN 'unknown_order_components'
              WHEN p.total_order_value IS DISTINCT FROM r.total_from_stored_children
                OR p.total_order_value IS DISTINCT FROM r.total_from_stored_hidden_sources
                THEN 'stored_rollup_mismatch'
              ELSE 'stored_rollups_match' END AS rollup_status
  FROM rollups r LEFT JOIN current_projects p USING (monday_id)
),
source_owner_checks AS (
  SELECT h.monday_id AS hidden_item_id, count(s.monday_id) AS stored_owner_count,
         COALESCE(jsonb_agg(jsonb_build_object('subitem_id', s.monday_id,
                   'parent_id', s.parent_monday_id,
                   'parent_is_selected', s.parent_monday_id IN (SELECT monday_id FROM project_ids))
                   ORDER BY s.monday_id) FILTER (WHERE s.monday_id IS NOT NULL), '[]'::jsonb) AS owners
  FROM source_ids h LEFT JOIN current_subitems s ON s.hidden_item_id = h.monday_id
  GROUP BY h.monday_id
),
commit_check AS (
  SELECT i.data->>'run_id' AS run_id, i.data->>'scope_id' AS scope_id,
         (i.data->>'expected_committed')::boolean AS previously_observed_committed,
         j.run_id IS NOT NULL AS journal_entry_present,
         j.committed_at, j.updated_rows, j.plan_sha256,
         CASE WHEN j.run_id IS NULL THEN NULL ELSE
           j.plan_sha256 = i.data->>'plan_sha256' END AS plan_hash_matches
  FROM input i
  LEFT JOIN public.order_value_scope_commits j
    ON j.run_id = (i.data->>'run_id')::uuid AND j.scope_id = i.data->>'scope_id'
)
SELECT jsonb_build_object(
  'scope_id', (SELECT data->>'scope_id' FROM input),
  'checked_at', statement_timestamp(),
  'reference_state', (SELECT data->>'reference_state' FROM input),
  'diagnostic_only', true,
  'certifies_monday_agreement', false,
  'commit_journal', (SELECT to_jsonb(c) FROM commit_check c),
  'missing_requested_rows', (SELECT COALESCE(jsonb_agg(to_jsonb(m) ORDER BY table_name, monday_id), '[]'::jsonb) FROM missing_requested_rows m),
  'field_or_membership_differences', (SELECT COALESCE(jsonb_agg(to_jsonb(d) ORDER BY table_name, monday_id), '[]'::jsonb) FROM differences d),
  'project_checks', (SELECT COALESCE(jsonb_agg(to_jsonb(p) ORDER BY monday_id), '[]'::jsonb) FROM project_checks p),
  'child_checks_including_external_owners', (SELECT COALESCE(jsonb_agg(to_jsonb(s) ORDER BY parent_monday_id, monday_id), '[]'::jsonb) FROM child_checks s),
  'source_owner_checks', (SELECT COALESCE(jsonb_agg(to_jsonb(s) ORDER BY hidden_item_id), '[]'::jsonb) FROM source_owner_checks s),
  'current_projects', (SELECT COALESCE(jsonb_agg(to_jsonb(p) ORDER BY monday_id), '[]'::jsonb) FROM current_projects p),
  'current_subitems', (SELECT COALESCE(jsonb_agg(to_jsonb(s) ORDER BY monday_id), '[]'::jsonb) FROM current_subitems s),
  'current_hidden_sources', (SELECT COALESCE(jsonb_agg(to_jsonb(h) ORDER BY monday_id), '[]'::jsonb) FROM current_hidden h)
) AS verification_report;
'''


def canonical_hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, indent=2, ensure_ascii=True).encode()).hexdigest()


manifest = json.loads((RUN/'manifest.json').read_text(encoding='utf-8'))
plan = json.loads((RUN/'scopes.json').read_text(encoding='utf-8'))
verification = json.loads((RUN/'verify-78322ece-eb2d-47c2-aab6-4d5a7e236485.json').read_text(encoding='utf-8'))
assert verification['run_id'] == manifest['run_id'] == '4ad3de2c-601d-4d27-b31e-ee424b8d174e'
assert manifest['sha256'] == canonical_hash(plan)
assert manifest['review_sha256'] == hashlib.sha256((RUN/'changes.csv').read_bytes()).hexdigest()
records = {r['scope_id']: r for r in plan['scopes']}
status = {r['scope_id']: r['status'] for r in verification['results']}
ownership = json.loads((RUN/verification['ownership_evidence']).read_text(encoding='utf-8'))
OUT.mkdir(parents=True, exist_ok=True)
catalog = []
for index, scope_id in enumerate(ORDER, 1):
    r = records[scope_id]
    project_ids = r['scope']['projects']
    observed_children = [child for child in ownership['items'] if child['parent_monday_id'] in project_ids]
    subitem_ids = sorted(set(r['scope']['subitems']) | {child['monday_id'] for child in observed_children})
    hidden_ids = sorted(set(r['boundary']['hidden_items']) | {hid for child in observed_children for hid in child['hidden_ids']})
    assert len(project_ids) == 25 and len(subitem_ids) <= 100 and len(hidden_ids) <= 100
    assert all(i.isdigit() for i in project_ids + subitem_ids + hidden_ids)
    committed = status[scope_id] == 'changed_requires_reassessment'
    reference_state = 'reviewed_after_state' if committed else 'reviewed_before_state'
    payload = {'scope_id':scope_id, 'run_id':manifest['run_id'], 'plan_sha256':manifest['sha256'],
               'expected_committed':committed, 'reference_state':reference_state,
               'project_ids':project_ids, 'known_subitem_ids':subitem_ids,
               'known_hidden_ids':hidden_ids, 'reference':r['after'] if committed else r['before']}
    stem = f'{index:02d}_{scope_id}'
    graphql = GRAPHQL.replace('SCOPE_ID',scope_id)
    for token, ids in [('PROJECT_IDS',project_ids), ('SUBITEM_IDS',subitem_ids), ('HIDDEN_IDS',hidden_ids)]:
        graphql = graphql.replace(token, '[\n    ' + ',\n    '.join(json.dumps(i) for i in ids) + '\n  ]')
    (OUT/f'{stem}.graphql').write_text(graphql,encoding='utf-8')
    owners = '''# OPTIONAL diagnostic: all active owners returned by Monday for these known sources.
# Not yet validated as a replacement for the backfill's complete ownership scan.
# If cursor is non-null, use next_source_owners.graphql until it becomes null.
# Add any further hidden IDs discovered by the primary query before checking owners.
query SourceOwners {
  boards(ids: ["1825117144"]) {
    id
    items_page(limit: 100, query_params: {
      rules: [{column_id: "connect_boards8__1", operator: any_of, compare_value: HIDDEN_NUMBERS}]
    }) {
      cursor
      items {
        id
        state
        board { id }
        parent_item { id state board { id } }
        column_values(ids: ["connect_boards8__1"]) {
          id
          ... on BoardRelationValue { linked_item_ids }
        }
      }
    }
  }
}
'''.replace('HIDDEN_NUMBERS', json.dumps([int(i) for i in hidden_ids]))
    (OUT/f'{stem}_source_owners.graphql').write_text(owners,encoding='utf-8')
    sql = SQL
    replacements = {'SCOPE_ID':scope_id, 'REFERENCE_STATE':reference_state,
                    'PAYLOAD':json.dumps(payload,indent=2,ensure_ascii=False)}
    for token, table, alias in [('PROJECT_FIELDS','projects','p'), ('SUBITEM_FIELDS','subitems','s'),
                               ('HIDDEN_FIELDS','hidden_items','h'), ('EXPECTED_PROJECT_FIELDS','projects','e'),
                               ('EXPECTED_SUBITEM_FIELDS','subitems','e'), ('EXPECTED_HIDDEN_FIELDS','hidden_items','e')]:
        replacements[token] = ', '.join(f'{alias}.{field}' for field in FIELDS[table])
    for token in sorted(replacements,key=len,reverse=True):
        sql = sql.replace(token,replacements[token])
    (OUT/f'{stem}.sql').write_text(sql,encoding='utf-8')
    (OUT/f'{stem}_reviewed_monday_evidence.json').write_text(json.dumps(r['source'],indent=2),encoding='utf-8')
    catalog.append({'scope_id':scope_id, 'status':status[scope_id], 'project_count':len(project_ids),
                    'known_subitems':len(subitem_ids), 'known_hidden_sources':len(hidden_ids),
                    'graphql':f'{stem}.graphql', 'sql':f'{stem}.sql',
                    'optional_owner_query':f'{stem}_source_owners.graphql',
                    'comparison':reference_state})
with (OUT/'scope_projects.csv').open('w',encoding='utf-8',newline='') as stream:
    writer=csv.writer(stream)
    writer.writerow(['scope_id','project_id','verification_status'])
    for scope_id in ORDER:
        writer.writerows((scope_id,pid,status[scope_id]) for pid in records[scope_id]['scope']['projects'])
(OUT/'query_manifest.json').write_text(json.dumps({'run_id':manifest['run_id'],'scopes':catalog},indent=2),encoding='utf-8')
(OUT/'next_source_owners.graphql').write_text('''# Put {"cursor": "the returned cursor"} in the Playground Variables panel.
query NextSourceOwners($cursor: String!) {
  next_items_page(cursor: $cursor, limit: 100) {
    cursor
    items {
      id
      state
      board { id }
      parent_item { id state board { id } }
      column_values(ids: ["connect_boards8__1"]) {
        id
        ... on BoardRelationValue { linked_item_ids }
      }
    }
  }
}
''',encoding='utf-8')
print(json.dumps({'output_dir':str(OUT),'scopes':catalog},indent=2))
