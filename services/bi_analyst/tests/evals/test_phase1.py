"""Failure-sensitive certification, export parity and connection-isolation checks."""
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
import json
from pathlib import Path
import sys

import pytest
from psycopg.conninfo import conninfo_to_dict

sys.path.insert(0, str(Path(__file__).resolve().parent))
import phase1
from phase1 import (FLAGS, POWER_BI_REFERENCES, compare_power_bi, deployment_problems,
                    evaluate_gate, fingerprint, load_packet, review_template,
                    save_new, seal_packet, writer_inventory)
from powerbi import reader_config

NOW = datetime(2026, 10, 7, 15, tzinfo=timezone.utc)


def approved(**kwargs):
    return dict(status='approved', owner='Test owner', value='Reviewed scope',
                reviewed_by='Test reviewer', reviewed_at=NOW.isoformat(),
                evidence=['review/independent-evidence'], **kwargs)


def service(name='api', archive=True):
    return {'name': name, 'start_command': 'uvicorn src.api.app:app',
            'deployed_commit': 'test-commit', 'flags': dict(zip(FLAGS, [archive, False, True, True]))}


@pytest.fixture
def complete_review():
    packet = seal_packet({'version': 1, 'dataset': 'bi_eval_20261007_v1',
        'manifest_sha256': 'manifest-test', 'reconciliation': {'cases': {'signed_invoices': {}}}})
    review = review_template(packet)
    for group in ('decisions', 'metrics', 'issues', 'source_samples', 'freshness', 'reports'):
        for record in review[group].values():
            record.update(approved())
    for record in review['metrics'].values():
        record.update(population='reportable', source_contract='Reviewed exact-ID scope')
    review['decisions']['report_population']['value'] = 'reportable'
    for record in review['issues'].values():
        record.update(status='resolved', affected_scope='reviewed release population')
    for record in review['freshness'].values():
        record.update(succeeded_at=NOW.isoformat(), maximum_age_hours=24)
    review['deployment'].update(approved(), environment='production', services=[service()])
    review['decisions']['performance_targets']['value'] = dict(p95_response_ms=5000,
        concurrent_users=10, concurrent_queries=3, run_timeout_seconds=60,
        max_queries_per_run=3, max_rows_per_run=1000, cost_per_successful_answer=.01,
        cost_currency='test units', workload=list(POWER_BI_REFERENCES))
    review['decisions']['connection_budget']['value'] = dict(database_limit=30,
        reserved=3, etl_peak=5, other_peak=2, analyst_replicas=2,
        read_pool_per_replica=4, state_pool_per_replica=2, headroom=3)
    comparisons = {}
    for name in POWER_BI_REFERENCES:
        review['reports'][name].update(report_name='Fixture report', report_version='fixture-v1', definition_sha256='definition-test')
        comparisons[name] = {key: review['reports'][name][key] for key in ('report_name', 'report_version', 'definition_sha256')}
        comparisons[name]['matches'] = True
    power_bi = seal_packet({'version': 1, 'dataset': packet['payload']['dataset'],
        'manifest_sha256': 'manifest-test', 'comparisons': comparisons})
    return packet, review, power_bi


def test_pending_template_never_certifies_or_enables_metrics(complete_review):
    packet, _, _ = complete_review
    result = evaluate_gate(packet, review_template(packet), now=NOW)
    assert result['status'] == 'blocked'
    assert result['enabled_metrics'] == []
    assert any('exact-ID' in b for b in result['blockers'])


def test_complete_owner_attestations_pass(complete_review):
    result = evaluate_gate(*complete_review, now=NOW)
    assert result['blockers'] == []
    assert result['status'] == 'passed_with_owner_attestations'


def closed_review(packet):
    review = review_template(packet)
    review['owner_closure'] = approved(
        scope='phase1_progression', dataset=packet['payload']['dataset'],
        evidence_sha256=packet['sha256'], accepted_metric_families=list(phase1.METRICS),
        power_bi_required=False, monday_cleanup_required=False,
        dispositions={'source': 'Owner accepts the implemented app definitions.',
                      'operations': 'Unmeasured operational evidence is outside Phase 1 closure.'})
    return review


def test_explicit_owner_closure_preserves_unverified_history(complete_review):
    packet, _, _ = complete_review
    review = closed_review(packet)
    original = deepcopy(review)
    result = evaluate_gate(packet, review, now=NOW)
    assert result['status'] == 'closed_by_owner'
    assert result['blockers'] == []
    assert result['enabled_metrics'] == list(phase1.METRICS)
    assert 'metric/source/population sign-off: order' in result['superseded_checks']
    assert any('successful completion' in item for item in result['superseded_checks'])
    assert result['power_bi_sha256'] is None
    assert result['power_bi_required'] is False
    assert review == original  # Acceptance must not fabricate detailed source/deployment reviews.


@pytest.mark.parametrize('field,value', [
    ('status', 'pending'), ('evidence', []), ('reviewed_by', ''),
    ('reviewed_at', '2026-10-08T12:00:00+00:00'),
    ('reviewed_at', '2026-10-07T12:00:00'),
    ('scope', 'production_deployment'), ('dataset', 'another-dataset'),
    ('evidence_sha256', 'another-capture'), ('accepted_metric_families', ['order']),
    ('power_bi_required', True), ('monday_cleanup_required', True), ('dispositions', {}),
])
def test_incomplete_or_misbound_closure_cannot_close_gate(complete_review, field, value):
    packet, _, _ = complete_review
    review = closed_review(packet)
    review['owner_closure'][field] = value
    result = evaluate_gate(packet, review, now=NOW)
    assert result['status'] == 'blocked'
    assert any(item.startswith('owner closure:') for item in result['blockers'])
    assert result['enabled_metrics'] == []


def test_power_bi_is_optional_without_owner_closure(complete_review):
    packet, review, _ = complete_review
    review['reports'] = {}
    review['issues']['report_reader_access'] = {'status': 'pending'}
    result = evaluate_gate(packet, review, now=NOW)
    assert result['blockers'] == []
    assert result['status'] == 'passed_with_owner_attestations'
    assert any('not supplied (optional)' in item for item in result['diagnostics'])


def test_revised_references_require_separate_bound_approval(complete_review):
    packet, review, power_bi = complete_review
    packet['payload']['manifest'] = {'reference_contract_version': '1.1.0'}
    packet['payload']['reference_evidence_sha256'] = 'revised-reference-evidence'
    result = evaluate_gate(packet, review, None, now=NOW)
    assert any('revised reference answers' in item for item in result['blockers'])
    approval = seal_packet({'status': 'approved_with_owner_attestation',
        'scope': 'reference_answers_only', 'dataset': packet['payload']['dataset'],
        'manifest_sha256': packet['payload']['manifest_sha256'],
        'reference_evidence_sha256': 'revised-reference-evidence'})
    result = evaluate_gate(packet, review, None, now=NOW, reference_packet=approval)
    assert not any('revised reference answers' in item for item in result['blockers'])
    assert result['reference_approval_sha256'] == approval['sha256']
    assert result['blockers'] == []
    assert any('Power BI comparison not supplied' in item for item in result['diagnostics'])
    approval['payload']['reference_evidence_sha256'] = 'old-reference-evidence'
    assert any('revised reference answers' in item for item in
               evaluate_gate(packet, review, None, now=NOW, reference_packet=approval)['blockers'])


def test_revised_gate_reports_optional_comparison_alignment(complete_review):
    packet, review, power_bi = complete_review
    packet['payload']['manifest'] = {
        'reference_contract_version': '1.1.0', 'dataset': packet['payload']['dataset'],
        'as_of_date': '2026-10-07', 'business_timezone': 'Europe/London'}
    result = evaluate_gate(packet, review, power_bi, now=NOW)
    assert any('Power BI alignment:' in item for item in result['diagnostics'])
    assert not any('Power BI alignment:' in item for item in result['blockers'])


@pytest.mark.parametrize('population', ['verified_active', 'current_projects', 'frozen_reportable_projects'])
def test_superseded_population_cannot_be_certified(complete_review, population):
    packet, review, power_bi = complete_review
    review['decisions']['report_population']['value'] = population
    review['metrics']['invoice']['population'] = population
    blockers = evaluate_gate(packet, review, power_bi, now=NOW)['blockers']
    assert any('report_population:' in blocker for blocker in blockers)
    assert 'metric/source/population sign-off: invoice' in blockers


@pytest.mark.parametrize('group,name,field', [
    ('decisions', 'tax_basis', 'evidence'), ('metrics', 'order', 'population'),
    ('metrics', 'enquiry', 'source_contract'), ('source_samples', 'signed_invoices', 'reviewed_by'),
    ('issues', 'relationships', 'affected_scope'),
])
def test_missing_review_evidence_blocks(complete_review, group, name, field):
    packet, review, power_bi = complete_review
    review[group][name][field] = None
    assert evaluate_gate(packet, review, power_bi, now=NOW)['status'] == 'blocked'


@pytest.mark.parametrize('when', ['2026-10-01T12:00:00+00:00', '2026-10-08T12:00:00+00:00', '2026-10-07T12:00:00'])
def test_stale_future_or_naive_freshness_blocks(complete_review, when):
    packet, review, power_bi = complete_review
    review['freshness']['rollup']['succeeded_at'] = when
    assert any('rollup' in b for b in evaluate_gate(packet, review, power_bi, now=NOW)['blockers'])


def test_limitation_must_appear_in_affected_metric_contract(complete_review):
    packet, review, power_bi = complete_review
    review['issues']['archive_coverage'].update(status='accepted_limitation', metrics=['order'])
    assert evaluate_gate(packet, review, power_bi, now=NOW)['blockers']
    review['metrics']['order']['limitations'].append('archive_coverage')
    assert not evaluate_gate(packet, review, power_bi, now=NOW)['blockers']


def test_connection_budget_includes_replicas_state_etl_and_headroom(complete_review):
    packet, review, power_bi = complete_review
    review['decisions']['connection_budget']['value']['analyst_replicas'] = 4
    assert any('exceed capacity' in b for b in evaluate_gate(packet, review, power_bi, now=NOW)['blockers'])


@pytest.mark.parametrize('value', [0, -1, True, float('inf'), 'soon'])
def test_nonnumeric_or_invalid_performance_targets_block(complete_review, value):
    packet, review, power_bi = complete_review
    review['decisions']['performance_targets']['value']['p95_response_ms'] = value
    assert any('performance targets' in b for b in evaluate_gate(packet, review, power_bi, now=NOW)['blockers'])


def test_mixed_writer_flags_and_reporting_population_block():
    assert any('mixed archive' in p for p in deployment_problems({'environment': 'production', 'services': [service(), service('webhook', False)]}))
    s = service()
    s['flags']['MONDAY_ARCHIVE_REPORTING_ENABLED'] = True
    assert any('inconsistent reporting' in p for p in deployment_problems({'environment': 'production', 'services': [service('webhook'), s]}))
    s['flags']['MONDAY_ARCHIVE_ENABLED'] = 'true'
    assert any('booleans' in p for p in deployment_problems({'environment': 'production', 'services': [s]}))


def test_optional_power_bi_difference_is_visible_and_exception_is_bound(complete_review):
    packet, review, power_bi = complete_review
    power_bi['payload']['comparisons']['invoice_monthly']['matches'] = False
    power_bi = seal_packet(power_bi['payload'])
    result = evaluate_gate(packet, review, power_bi, now=NOW)
    assert result['blockers'] == []
    assert any('unexplained' in b for b in result['diagnostics'])
    review['power_bi_exceptions']['invoice_monthly'] = approved(comparison_sha256='old')
    assert evaluate_gate(packet, review, power_bi, now=NOW)['diagnostics']
    review['power_bi_exceptions']['invoice_monthly']['comparison_sha256'] = power_bi['sha256']
    assert not evaluate_gate(packet, review, power_bi, now=NOW)['diagnostics']


def test_missing_optional_report_metadata_does_not_block(complete_review):
    packet, review, power_bi = complete_review
    review['reports']['invoice_monthly']['report_version'] = None
    result = evaluate_gate(packet, review, power_bi, now=NOW)
    assert result['blockers'] == []
    assert 'BI report definition/version review missing: invoice_monthly' in result['diagnostics']


def test_wrong_capture_or_power_bi_manifest_is_rejected(complete_review):
    packet, review, power_bi = complete_review
    review['evidence_sha256'] = 'wrong'
    with pytest.raises(ValueError):
        evaluate_gate(packet, review, power_bi)
    review['evidence_sha256'] = packet['sha256']
    power_bi['payload']['manifest_sha256'] = 'wrong'
    with pytest.raises(ValueError):
        evaluate_gate(packet, review, power_bi)


@pytest.fixture
def export_case():
    manifest = {'schemas': {'data': 'bi_eval_20261007_v1'}, 'as_of_date': '2026-10-07'}
    columns = [{'name': 'month', 'type_oid': 1082}, {'name': 'amount', 'type_oid': 1700}]
    rows = [{'month': '2026-09-01', 'amount': '9007199254740993.00'},
            {'month': '2026-08-01', 'amount': None}, {'month': '2026-07-01', 'amount': '0.00'}]
    expected = {'enquiry_monthly': {'columns': columns, 'rows': rows}}
    entry = dict(reference='enquiry_monthly', report_name='Fixture', report_version='v1',
        dax='SUM([amount])', filters='completed months', population='frozen reportable',
        as_of_date='2026-10-07', columns=deepcopy(columns), rows=deepcopy(rows))
    exports = {'dataset': manifest['schemas']['data'], 'manifest_sha256': fingerprint(manifest), 'comparisons': [entry]}
    return exports, expected, manifest


def test_power_bi_parity_is_decimal_exact_and_order_independent(export_case):
    exports, expected, manifest = export_case
    exports['comparisons'][0]['rows'].reverse()
    assert compare_power_bi(exports, expected, manifest)['enquiry_monthly']['matches']
    exports['comparisons'][0]['rows'][-1]['amount'] = '9007199254740992.00'
    assert not compare_power_bi(exports, expected, manifest)['enquiry_monthly']['matches']


def test_power_bi_does_not_deduplicate_or_zero_fill(export_case):
    exports, expected, manifest = export_case
    exports['comparisons'][0]['rows'][1]['amount'] = '0.00'
    assert not compare_power_bi(exports, expected, manifest)['enquiry_monthly']['matches']
    exports['comparisons'][0]['rows'] = deepcopy(expected['enquiry_monthly']['rows']) * 2
    assert not compare_power_bi(exports, expected, manifest)['enquiry_monthly']['matches']


@pytest.mark.parametrize('bad', [True, float(1.23), 'NaN', 'Infinity', 'not a number'])
def test_power_bi_rejects_lossy_or_nonfinite_numeric_exports(export_case, bad):
    exports, expected, manifest = export_case
    exports['comparisons'][0]['rows'][0]['amount'] = bad
    with pytest.raises(ValueError):
        compare_power_bi(exports, expected, manifest)


@pytest.mark.parametrize('field,value', [('dax', ''), ('as_of_date', '2026-10-08'),
                                      ('columns', []), ('reference', 'unknown')])
def test_power_bi_requires_matching_definition_context(export_case, field, value):
    exports, expected, manifest = export_case
    exports['comparisons'][0][field] = value
    with pytest.raises(ValueError):
        compare_power_bi(exports, expected, manifest)


def test_packet_integrity_and_no_overwrite(tmp_path, monkeypatch):
    monkeypatch.setattr(phase1, 'ROOT', tmp_path)
    path = tmp_path / 'outputs' / 'evidence.json'
    packet = seal_packet({'version': 1, 'data': 'a'})
    save_new(path, packet)
    assert load_packet(path) == packet
    with pytest.raises(FileExistsError):
        save_new(path, packet)
    path.write_text(path.read_text().replace('"a"', '"b"'))
    with pytest.raises(ValueError, match='fingerprint'):
        load_packet(path)
    with pytest.raises(ValueError, match='git-ignored'):
        save_new(tmp_path / 'tracked.json', packet)


def test_writer_trace_has_functions_without_importing_etl():
    inventory = writer_inventory()
    assert 'refresh_current_values' in inventory['archive']['functions']
    assert 'src.config' not in sys.modules
    assert 'src.api.app' not in sys.modules


def reader_env(tmp_path, **changes):
    values = dict(PG_ROLES='powerbi_reader', PG_USER='powerbi_reader.testref',
                  PG_HOST='aws-1-eu-west-2.pooler.supabase.com', PG_PORT='5432',
                  PG_DATABASE='postgres', PG_USER_PASSWORD='fake password')
    values.update(changes)
    path = tmp_path / 'reader.env'
    path.write_text('\n'.join(key + '=' + value for key, value in values.items()))
    return path


def test_reader_uses_only_explicit_reader_credential(tmp_path):
    dsn, role = reader_config(reader_env(tmp_path))
    params = conninfo_to_dict(dsn)
    assert params['user'] == 'powerbi_reader.testref'
    assert params['password'] == 'fake password'
    assert role == 'powerbi_reader'


@pytest.mark.parametrize('changes', [{'PG_ROLES': 'postgres'}, {'PG_USER_PASSWORD': ''},
    {'PG_USER': 'postgres.testref'}, {'PG_HOST': 'attacker.example'},
    {'PG_PORT': '6543'}, {'PG_SERVER': 'different:5432'}])
def test_reader_never_falls_back_to_admin_or_another_endpoint(tmp_path, changes):
    with pytest.raises(ValueError):
        reader_config(reader_env(tmp_path, **changes))
