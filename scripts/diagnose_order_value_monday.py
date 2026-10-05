"""Bounded, single-project, read-only isolation of Monday GraphQL failures.

No database connection, board traversal, mutation, retries or skipped failures.
Raw responses (including unusable partial error responses) are diagnostic evidence
only, never inputs to the apply workflow. Credentials/headers are never saved.
"""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

from scripts import order_value_monday_compare as compare


META = 'id name state updated_at board { id } parent_item { id }'
CHILDREN = 'subitems { id state board { id } parent_item { id } }'


def probes():
    fields = [compare.PARENT_COLUMNS[f] for f in compare.PARENT_FIELDS]
    simple = 'id type text value ... on MirrorValue { display_value }'
    typed = 'id type text value ' + compare.value_selection(2)
    result = [('metadata', META, []), ('membership', META + ' ' + CHILDREN, []),
              ('display_values', META + ' column_values(ids: $columns) { ' + simple + ' }', fields),
              ('combined_typed', META + ' ' + CHILDREN + ' column_values(ids: $columns) { ' + typed + ' }', fields)]
    result.extend(('typed_' + field, META + ' column_values(ids: $columns) { ' + typed + ' }', [field])
                  for field in fields)
    order = compare.PARENT_COLUMNS['total_order_value']
    result.extend([
        ('order_settings', META + ' column_values(ids: $columns) { id column { settings_str } }', [order]),
        ('order_mirrored_ids', META + ''' column_values(ids: $columns) { id
            ... on MirrorValue { mirrored_items { linked_item { id } linked_board_id } } }''', [order]),
        ('order_typed_one_level', META + ''' column_values(ids: $columns) { id
            ... on MirrorValue { mirrored_items { linked_item { id } linked_board_id
                mirrored_value { ''' + compare.value_selection(0) + ' } } } }', [order]),
        ('order_typed_two_levels_no_settings', META + ' column_values(ids: $columns) { id ' +
            compare.value_selection(2).replace('column { settings_str }', '') + ' }', [order]),
        ('flat_parent', META + ' ' + CHILDREN + ' column_values(ids: $columns) { id type text value ' +
            compare.flat_value_selection() + ' }', fields),
        ('flat_child', META + ' column_values(ids: $columns) { id type text value ... on BoardRelationValue { linked_item_ids } ' +
            compare.flat_value_selection() + ' }',
            [compare.SUBITEM_COLUMNS[f] for f in (*compare.CHILD_FIELDS, 'cust_order_value_material', 'new_enquiry_value')]),
        ('flat_hidden', META + ' column_values(ids: $columns) { id type text value ' +
            compare.flat_value_selection() + ' }',
            [compare.HIDDEN_ITEMS_COLUMNS[f] for f in compare.SOURCE_FIELDS] + [compare.backfill.TOTAL_COLUMN]),
    ])
    return result


def make_query(selection, columns):
    variables = ', $columns: [String!]!' if columns else ''
    return ('query CompareMonday($ids: [ID!]!' + variables + ') { '
            'items(ids: $ids, limit: 100, exclude_nonactive: false) { ' + selection + ' } }')


def diagnose(client, project_id, output_dir, selected=()):
    compare.validate_ids([project_id])
    if output_dir.exists():
        raise ValueError('Use a new diagnostic output directory')
    output_dir.mkdir(parents=True)
    results = []
    for name, selection, columns in probes():
        if selected and name not in selected:
            continue
        query = make_query(selection, columns)
        variables = {'ids': [project_id]}
        if columns:
            variables['columns'] = columns
        print(f'Checking {name}: project {project_id}', flush=True)
        with client.session.post(client.api_url, json={'query': query, 'variables': variables},
                                 headers=client.headers, timeout=(10, 45)) as response:
            try:
                body = response.json()
            except ValueError:
                body = {'diagnostic_error': 'Response was not JSON'}
            payload = body if isinstance(body, dict) else {}
            error = compare.response_error(payload,
                                           status=response.status_code, headers=response.headers)
            record = {'probe': name, 'project_id': project_id, 'captured_at': datetime.now(timezone.utc).isoformat(),
                      'requested_api_version': client.headers.get('API-Version'),
                      'returned_api_version': response.headers.get('API-Version'),
                      'http_status': response.status_code, 'query': query, 'variables': variables, 'response': body}
            compare.backfill.write_json(output_dir / (name + '.json'), record)
            valid = (not error and isinstance(body, dict) and isinstance(body.get('data'), dict)
                     and isinstance(body['data'].get('items'), list)
                     and len(body['data']['items']) == 1 and body['data']['items'][0].get('id') == project_id)
            result = {'probe': name, 'success': valid, 'error': str(error) if error else None,
                      'request_id': error.request_id if error else (payload.get('extensions') or {}).get('request_id')}
            results.append(result)
            print(json.dumps(result), flush=True)
            # Diagnostics do not retry or disregard rate-limit cooldowns.
            if error and (error.retry_after or any(c in compare.THROTTLE_CODES or c == 'HTTP_429' for c in error.codes)):
                break
    compare.backfill.write_json(output_dir / 'summary.json', {'project_id': project_id, 'results': results})
    return results


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--project-id', required=True)
    parser.add_argument('--output-dir', required=True, type=Path)
    parser.add_argument('--probe', action='append', choices=[p[0] for p in probes()], default=[])
    parser.add_argument('--capture-only', action='store_true',
                        help='Validate the corrected exact-ID capture for this parent and its linked sources; no Supabase access')
    args = parser.parse_args(argv)
    compare.validate_ids([args.project_id])
    client = compare.ComparisonMondayClient()
    try:
        if args.capture_only:
            if args.probe or args.output_dir.exists():
                raise ValueError('Capture-only requires a new directory and no --probe options')
            evidence = compare.capture(client, [args.project_id])
            args.output_dir.mkdir(parents=True)
            compare.backfill.write_json(args.output_dir / 'capture.json', evidence)
            parent = evidence['projects'].get(args.project_id)
            if parent is None:
                raise ValueError('Parent not returned')
            values = {}
            for field in ('total_order_value', 'new_enq_value_mirror'):
                try:
                    amount = compare.numeric(compare.resolved_col(evidence, parent, compare.PARENT_COLUMNS[field]))
                    values[field] = {'value': str(amount) if amount is not None else None}
                except ValueError as exc:
                    values[field] = {'unresolved': str(exc)}
            report = {'project_id': args.project_id, 'capture_completed': True,
                      'rows': {t: len(evidence[t]) for t in compare.scopes.TABLES}, 'values': values}
            compare.backfill.write_json(args.output_dir / 'summary.json', report)
            print(json.dumps(report, indent=2))
            return 0
        results = diagnose(client, args.project_id, args.output_dir, args.probe)
        return 0 if results and all(r['success'] for r in results) else 2
    finally:
        client.session.close()


if __name__ == '__main__':
    raise SystemExit(main())
