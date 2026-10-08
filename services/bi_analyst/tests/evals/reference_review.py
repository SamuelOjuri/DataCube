"""Evidence-bound approval of reference answers, never automatic business certification."""
from datetime import datetime, timezone
import json

from dataset import fingerprint, names, reference_version, verify_snapshot
from manage import connect
from phase1 import VERSION, output_path, reviewed, save_new, seal_packet, timestamp


def reference_evidence(artifacts):
    manifest = artifacts['manifest']
    return seal_packet({
        'version': VERSION,
        'dataset': manifest['dataset'],
        'reference_contract_version': reference_version(manifest),
        'manifest_sha256': fingerprint(manifest),
        'catalogue_sha256': manifest.get('catalogue_sha256'),
        'scope': 'reference_answers_only',
        'references': {
            name: {'sql_sha256': result['sql_sha256'],
                   'result_sha256': fingerprint({'columns': result['columns'], 'rows': result['rows']}),
                   'columns': result['columns'], 'row_count': len(result['rows'])}
            for name, result in artifacts['expected'].items()},
        'changed_references': {
            name: {'same_snapshot_results_differ': change['same_snapshot_results_differ']}
            for name, change in artifacts.get('reference_changes', {}).items()},
    })


def review_template(evidence):
    return {
        'reference_evidence_sha256': evidence['sha256'],
        'scope': 'reference_answers_only',
        'status': 'pending', 'owner': None, 'value': None,
        'reviewed_by': None, 'reviewed_at': None, 'evidence': [],
        'instructions': 'Review the SQL and expected results in the restricted dataset answer-key schema, including same-snapshot reference_changes. Record actual reviewer identity and evidence. This is not approval of source accuracy, Power BI results or production access.',
    }


def evaluate_reference_review(evidence, review, now=None):
    now = now or datetime.now(timezone.utc)
    if (review.get('reference_evidence_sha256') != evidence['sha256']
            or review.get('scope') != 'reference_answers_only'):
        raise ValueError('Reference approval is not bound to this exact reference evidence and scope')
    blockers = []
    if not reviewed(review):
        blockers.append('Reference answers require an explicit owner approval with reviewer, time and evidence')
    elif timestamp(review['reviewed_at']) > now:
        blockers.append('Reference approval timestamp is in the future')
    return {
        'version': VERSION,
        'dataset': evidence['payload']['dataset'],
        'manifest_sha256': evidence['payload']['manifest_sha256'],
        'reference_contract_version': evidence['payload']['reference_contract_version'],
        'reference_evidence_sha256': evidence['sha256'],
        'approval_sha256': fingerprint(review),
        'scope': 'reference_answers_only',
        'status': 'blocked' if blockers else 'approved_with_owner_attestation',
        'blockers': blockers,
        'business_source_certification': 'not_established',
    }


def review_command(args):
    connection, identity = connect()
    with connection:
        connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        connection.execute("SET LOCAL statement_timeout='120s'")
        artifacts, _ = verify_snapshot(connection, identity, args.dataset)
    evidence = reference_evidence(artifacts)
    review = json.loads(args.review.read_text(encoding='utf-8')) if args.review else review_template(evidence)
    result = evaluate_reference_review(evidence, review)
    save_new(args.output / 'reference_evidence.json', evidence)
    save_new(args.output / 'reference_review.json', review)
    save_new(args.output / 'reference_gate.json', seal_packet(result))
    _, key_schema, _ = names(args.dataset)
    review_sql = f"""-- Private evaluator/owner review only. Never import this answer key into Power BI.
BEGIN READ ONLY;
SELECT r.key AS reference, r.value->>'sql' AS reference_sql,
       r.value->'columns' AS columns, r.value->'rows' AS expected_rows
FROM {key_schema}.artifacts a CROSS JOIN LATERAL jsonb_each(a.payload) r
WHERE a.name='expected' ORDER BY r.key;
SELECT r.key AS reference, r.value AS same_snapshot_revision
FROM {key_schema}.artifacts a CROSS JOIN LATERAL jsonb_each(a.payload) r
WHERE a.name='reference_changes' ORDER BY r.key;
ROLLBACK;
"""
    with output_path(args.output / 'reference-review.sql').open('x', encoding='utf-8') as stream:
        stream.write(review_sql)
    print(json.dumps({'dataset': args.dataset, 'references': len(evidence['payload']['references']),
                      'changed_references': sorted(evidence['payload']['changed_references']),
                      'status': result['status'], 'blockers': result['blockers']}))
    return 2 if result['blockers'] else 0
