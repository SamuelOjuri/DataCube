"""Synthetic reference/version/export checks; never evidence of a real Power BI execution."""
from copy import deepcopy
import csv
from datetime import date, datetime, timezone
from decimal import Decimal
import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from cases import cases
from dataset import invoice_period_results, reference_queries, reference_version
from phase1 import POWER_BI_REFERENCES, compare_power_bi, fingerprint, load_packet, seal_packet, validate_power_bi_alignment
from powerbi import dax_query, load_exports, package_files, read_csv
from reference_review import evaluate_reference_review, reference_evidence, review_template

NOW = datetime(2026, 10, 7, 15, tzinfo=timezone.utc)


@pytest.fixture
def artifacts():
    manifest = {'dataset': 'bi_eval_20261007_v2', 'schemas': {'data': 'bi_eval_20261007_v2'},
        'reader_role': 'bi_eval_20261007_v2_reader', 'as_of_date': '2026-10-07',
        'captured_at': '2026-10-07T12:00:00+00:00', 'business_timezone': 'Europe/London',
        'reference_contract_version': '1.1.0', 'catalogue_sha256': 'catalogue-test'}
    expected = {}
    for reference in POWER_BI_REFERENCES:
        columns = [{'name': 'month', 'type_oid': 1082}, {'name': 'amount', 'type_oid': 1700}]
        expected[reference] = {'sql_sha256': 'sql-test-' + reference, 'columns': columns,
                              'rows': [{'month': '2026-09-01', 'amount': '123.45'}]}
    return {'manifest': manifest, 'expected': expected,
            'reference_changes': {'invoice_monthly': {'same_snapshot_results_differ': True}}}


def write_package(path, artifacts):
    path.mkdir()
    for name, text in package_files(artifacts, 'test.invalid:5432', 'postgres').items():
        (path / name).write_text(text, encoding='utf-8', newline='\n')


def write_csv(path, rows):
    with path.open('w', encoding='utf-8-sig', newline='') as stream:
        writer = csv.writer(stream)
        writer.writerow(['[' + name + ']' for name in rows[0]])
        writer.writerows(row.values() for row in rows)


def synthetic_exports(path, artifacts):
    metadata = json.loads((path / 'export.json').read_text())
    metadata['execution'].update(model_refreshed_at='2026-10-07T13:00:00+00:00',
                                 exported_at='2026-10-07T14:00:00+00:00')
    for entry in metadata['comparisons']:
        entry.update(report_name='Synthetic test model', report_version='fixture')
        write_csv(path / (entry['reference'] + '.csv'), artifacts['expected'][entry['reference']]['rows'])
    (path / 'export.json').write_text(json.dumps(metadata), encoding='utf-8')
    context = {key: artifacts['manifest'][key] for key in ('dataset', 'as_of_date', 'business_timezone', 'reference_contract_version')}
    context['snapshot_captured_at'] = '2026-10-07T12:00:00Z'
    write_csv(path / 'context.csv', [context])


def test_reference_versions_preserve_legacy_sql_and_question_hash_inputs():
    legacy = reference_queries()
    revised = reference_queries('1.1.0')
    assert len(legacy) == len(revised) == 50
    assert {name for name in legacy if legacy[name] != revised[name]} == {
        'invoice_last_month', 'invoice_previous_month', 'invoice_monthly'}
    assert "p.pipeline_stage='Won - Closed (Invoiced)'" in legacy['invoice_monthly']
    assert 'pipeline_stage' not in revised['invoice_monthly']
    assert len(cases()) == len(cases('1.1.0')) == 70
    assert cases() == cases('1.0.0')
    assert fingerprint(cases()) != fingerprint(cases('1.1.0'))
    assert reference_version({}) == '1.0.0'
    with pytest.raises(ValueError):
        reference_queries('2.0.0')


def test_independent_invoice_checks_preserve_dates_signed_values_and_multiplicity():
    invoices = [
        {'parent_monday_id': 'A', 'invoice_date': date(2026, 9, 1), 'amount_invoiced': Decimal('10.10')},
        {'parent_monday_id': 'A', 'invoice_date': date(2026, 9, 30), 'amount_invoiced': Decimal('5.20')},
        {'parent_monday_id': 'B', 'invoice_date': date(2026, 9, 2), 'amount_invoiced': Decimal('-2')},
        {'parent_monday_id': 'C', 'invoice_date': None, 'amount_invoiced': Decimal('90')},
        {'parent_monday_id': 'D', 'invoice_date': date(2026, 10, 1), 'amount_invoiced': Decimal('90')},
        {'parent_monday_id': 'E', 'invoice_date': date(2026, 8, 1), 'amount_invoiced': None},
    ]
    result = invoice_period_results(invoices, date(2026, 10, 8))
    assert result['invoice_last_month'] == [{'invoice_rows': 2, 'projects': 1, 'amount': Decimal('15.30')}]
    assert result['invoice_previous_month'] == [{'invoice_rows': 0, 'projects': 0, 'amount': Decimal(0)}]
    assert len(result['invoice_monthly']) == 12
    assert result['invoice_monthly'][0]['month'] == date(2025, 10, 1)


def test_invoice_calendar_boundaries_zero_and_half_up_rounding():
    rows = [
        {'parent_monday_id': 'A', 'invoice_date': date(2025, 12, 31), 'amount_invoiced': Decimal('0.005')},
        {'parent_monday_id': 'B', 'invoice_date': date(2025, 12, 1), 'amount_invoiced': Decimal(0)},
        {'parent_monday_id': 'C', 'invoice_date': date(2024, 12, 31), 'amount_invoiced': Decimal(10)},
    ]
    result = invoice_period_results(rows, date(2026, 1, 1))
    assert result['invoice_last_month'] == [{'invoice_rows': 1, 'projects': 1, 'amount': Decimal('0.01')}]
    assert result['invoice_monthly'][0]['month'] == date(2025, 1, 1)
    assert sum(row['invoice_rows'] for row in result['invoice_monthly']) == 1


def test_reference_approval_cannot_be_inferred_from_passing_checks(artifacts):
    evidence = reference_evidence(artifacts)
    review = review_template(evidence)
    assert evaluate_reference_review(evidence, review, now=NOW)['status'] == 'blocked'
    review.update(status='approved', owner='Fixture owner', value='Reviewed calculation answers',
                  reviewed_by='Fixture reviewer', reviewed_at=NOW.isoformat(),
                  evidence=['restricted/independent-review'])
    assert evaluate_reference_review(evidence, review, now=NOW)['status'] == 'approved_with_owner_attestation'
    changed = deepcopy(artifacts)
    changed['expected']['invoice_monthly']['rows'][0]['amount'] = '123.46'
    with pytest.raises(ValueError, match='bound'):
        evaluate_reference_review(reference_evidence(changed), review, now=NOW)
    review['reviewed_at'] = '2026-10-08T15:00:00+00:00'
    assert evaluate_reference_review(evidence, review, now=NOW)['status'] == 'blocked'


@pytest.mark.parametrize('field,value', [
    ('status', 'pending'), ('status', 'rejected'), ('owner', ' '),
    ('reviewed_by', None), ('reviewed_at', '2026-10-07T13:00:00'), ('evidence', []),
])
def test_incomplete_reference_approvals_stay_blocked(artifacts, field, value):
    evidence = reference_evidence(artifacts)
    review = review_template(evidence)
    review.update(status='approved', owner='Fixture owner', value='Reviewed fixture answers',
                  reviewed_by='Fixture reviewer', reviewed_at=NOW.isoformat(), evidence=['fixture'])
    review[field] = value
    assert evaluate_reference_review(evidence, review, now=NOW)['status'] == 'blocked'


def test_reference_approval_cannot_expand_to_source_certification(artifacts):
    evidence = reference_evidence(artifacts)
    review = review_template(evidence)
    review['scope'] = 'source_certification'
    with pytest.raises(ValueError, match='scope'):
        evaluate_reference_review(evidence, review, now=NOW)


def test_reference_packets_round_trip_through_standard_loader(tmp_path, artifacts):
    evidence = reference_evidence(artifacts)
    gate = seal_packet(evaluate_reference_review(evidence, review_template(evidence), now=NOW))
    for name, packet in [('evidence', evidence), ('gate', gate)]:
        path = tmp_path / (name + '.json')
        path.write_text(json.dumps(packet), encoding='utf-8')
        assert load_packet(path) == packet


def test_package_has_raw_inputs_and_dax_but_no_answer_rows(artifacts):
    files = package_files(artifacts, 'test.invalid:5432', 'postgres')
    assert '123.45' not in ''.join(files.values())
    assert '_key' not in files['DC Projects.m']
    assert 'reference_1_1.sql' not in ''.join(files.values())
    assert len([name for name in files if name.endswith('.dax')]) == 8
    assert "Won - Closed (Invoiced)" not in dax_query('invoice_monthly')
    assert "Won - Closed (Invoiced)" in dax_query('conversion_five_year')
    assert 'IN __Parents' in dax_query('invoice_monthly')
    assert 'Currency.Type' in files['DC Subitems.m']
    assert all('TODAY(' not in files[name] for name in files if name.endswith('.dax'))
    assert 'rows' not in json.loads(files['export.json'])['comparisons'][0]


def test_missing_power_bi_output_never_becomes_a_success(tmp_path, artifacts):
    folder = tmp_path / 'package'
    write_package(folder, artifacts)
    with pytest.raises(FileNotFoundError):
        load_exports(folder)
    assert json.loads((folder / 'package.json').read_text())['payload']['status'] == 'awaiting_independent_Power_BI_execution'


def test_csv_package_round_trip_and_alignment(tmp_path, artifacts):
    folder = tmp_path / 'package'
    write_package(folder, artifacts)
    synthetic_exports(folder, artifacts)
    exports = load_exports(folder)
    assert all(result['matches'] for result in compare_power_bi(exports, artifacts['expected'], artifacts['manifest']).values())
    exports['comparisons'][0]['rows'].append(deepcopy(exports['comparisons'][0]['rows'][0]))
    assert not compare_power_bi(exports, artifacts['expected'], artifacts['manifest'])['enquiry_monthly']['matches']
    (folder / 'invoice_monthly.dax').write_text('EVALUATE ROW("amount", 0)')
    with pytest.raises(ValueError, match='source changed'):
        load_exports(folder)


@pytest.mark.parametrize('field,value', [
    ('source_kind', 'live_production'), ('population', 'verified_active'),
    ('business_timezone', 'UTC'), ('reference_contract_version', '1.0.0'),
    ('snapshot_captured_at', '2026-10-06T12:00:00Z'),
])
def test_misaligned_power_bi_results_are_rejected(tmp_path, artifacts, field, value):
    folder = tmp_path / 'package'
    write_package(folder, artifacts)
    synthetic_exports(folder, artifacts)
    exports = load_exports(folder)
    exports['alignment'][field] = value
    with pytest.raises(ValueError):
        validate_power_bi_alignment(exports, artifacts['manifest'], now=NOW)


@pytest.mark.parametrize('field,value', [
    ('model_refreshed_at', '2026-10-06T13:00:00Z'),
    ('exported_at', '2026-10-07T16:00:00Z'),
    ('exported_at', '2026-10-07T12:30:00Z'),
    ('exported_at', '2026-10-07T14:00:00'),
])
def test_refresh_and_export_times_are_validated(tmp_path, artifacts, field, value):
    folder = tmp_path / 'package'
    write_package(folder, artifacts)
    synthetic_exports(folder, artifacts)
    exports = load_exports(folder)
    exports['execution'][field] = value
    with pytest.raises(ValueError):
        validate_power_bi_alignment(exports, artifacts['manifest'], now=NOW)


def test_csv_keeps_null_zero_and_rejects_ambiguous_headers(tmp_path):
    path = tmp_path / 'invoice.csv'
    path.write_text('[amount]\n""\n0.00\n0.00\n', encoding='utf-8')
    assert read_csv(path, ['amount']) == [{'amount': None}, {'amount': '0.00'}, {'amount': '0.00'}]
    path.write_text('[amount],amount\n1,2\n', encoding='utf-8')
    with pytest.raises(ValueError, match='headers'):
        read_csv(path, ['amount'])
    path.write_text('[amount]\n"unfinished', encoding='utf-8')
    with pytest.raises(ValueError, match='CSV syntax'):
        read_csv(path, ['amount'])
    path.write_text('[amount]\n1,2\n', encoding='utf-8')
    with pytest.raises(ValueError, match='column count'):
        read_csv(path, ['amount'])


def test_package_rejects_paths_outside_its_directory(tmp_path, artifacts):
    folder = tmp_path / 'package'
    write_package(folder, artifacts)
    package_path = folder / 'package.json'
    payload = load_packet(package_path)['payload']
    payload['files_sha256']['..\\outside.m'] = 'not-a-real-hash'
    package_path.write_text(json.dumps(seal_packet(payload)), encoding='utf-8')
    with pytest.raises(ValueError, match='source changed'):
        load_exports(folder)


def test_legacy_json_exports_remain_loadable(tmp_path):
    path = tmp_path / 'export.json'
    value = {'dataset': 'bi_eval_20261007_v1', 'comparisons': []}
    path.write_text(json.dumps(value), encoding='utf-8')
    assert load_exports(path) == value
