"""Offline safety and fixture tests; no production credentials or network needed."""
from datetime import date
from decimal import Decimal
import importlib
from pathlib import Path
import sys

import pytest

sys.path.insert(0,str(Path(__file__).resolve().parent))
from cases import cases
from dataset import equivalent, names, references
from manage import target_config


def env_file(tmp_path, **overrides):
    values = dict(TEST_SUPABASE_NAME='DataCube TEST',TEST_SUPABASE_URL='https://testref.supabase.co',
        TEST_SUPABASE_DB_URL='postgresql://postgres.testref:fake@aws-0-eu-west-2.pooler.supabase.com:5432/postgres',
        SUPABASE_URL='https://prodref.supabase.co',
        SUPABASE_DB_URL='postgresql://postgres.prodref:fake@aws-0-eu-west-2.pooler.supabase.com:5432/postgres')
    values.update(overrides)
    path = tmp_path/'test.env'
    path.write_text('\n'.join(f'{key}={value}' for key,value in values.items()),encoding='utf-8')
    return path


def test_valid_distinct_test_target(tmp_path):
    _,identity=target_config(env_file(tmp_path))
    assert identity['project_ref']=='testref'


@pytest.mark.parametrize('changes',[
    {'TEST_SUPABASE_URL':'https://prodref.supabase.co','TEST_SUPABASE_DB_URL':'postgresql://postgres.prodref:fake@aws-0-eu-west-2.pooler.supabase.com:5432/postgres'},
    {'TEST_SUPABASE_DB_URL':'postgresql://postgres.prodref:fake@aws-0-eu-west-2.pooler.supabase.com:5432/postgres'},
    {'TEST_SUPABASE_DB_URL':''},
    {'TEST_SUPABASE_NAME':'DataCube production'},
    {'TEST_SUPABASE_URL':'https://testref.supabase.co.attacker.example'},
    {'TEST_SUPABASE_DB_URL':'postgresql://postgres.testref:fake@localhost:5432/postgres'},
])
def test_reject_unsafe_or_missing_target(tmp_path,changes):
    with pytest.raises(ValueError):
        target_config(env_file(tmp_path,**changes))


def test_direct_connection_is_matched_to_test_project(tmp_path):
    _,identity=target_config(env_file(tmp_path,TEST_SUPABASE_DB_URL='postgresql://postgres:fake@db.testref.supabase.co:5432/postgres'))
    assert identity['project_ref']=='testref'


@pytest.mark.parametrize('name',['public','bi_eval_v1','bi_eval_20261007_v0','bi_eval_20261007_v1;DROP SCHEMA public'])
def test_dataset_identifier_is_restricted(name):
    with pytest.raises(ValueError):
        names(name)


def test_question_coverage_and_every_reference_exists():
    pack=cases()
    refs={**references('reference.sql'),**references('fixture_reference.sql')}
    assert len(pack)==70
    assert len({case['id'] for case in pack})==70
    assert len(refs)==50
    for case in pack:
        if case['reference']:
            assert case['reference'] in refs
        for turn in case.get('turns',[]):
            assert turn['reference'] in refs
    assert {case['metric'] for case in pack if case['kind']=='metric'}=={'enquiry','order','invoice','conversion','gestation'}


def test_ambiguous_questions_have_no_numeric_oracle():
    ambiguous=[case for case in cases() if case['kind']=='ambiguous']
    assert len(ambiguous)==10
    assert all(case['reference'] is None and not case['numerical_answer_before_resolution'] for case in ambiguous)


def test_decimal_comparison_preserves_money_and_nulls():
    assert equivalent([{'amount':Decimal('20.000000')}],[{'amount':'20'}])
    assert not equivalent([{'amount':Decimal('20.01')}],[{'amount':'20.00'}])
    assert not equivalent([{'amount':None}],[{'amount':'0.00'}])
    assert not equivalent([{'amount':'9007199254740993.00'}],[{'amount':'9007199254740992.00'}])
    assert not equivalent([{'monday_id':'001'}],[{'monday_id':'1'}])
    assert not equivalent(True,1)


def test_dates_compare_with_serialized_answer_key():
    assert equivalent([{'month':date(2026,9,1)}],[{'month':'2026-09-01'}])
    assert not equivalent([{'month':date(2026,9,1)}],[{'month':'2026-08-01'}])


def test_references_use_fixed_context_not_current_clock():
    for filename in ('reference.sql','fixture_reference.sql'):
        for query in references(filename).values():
            assert 'CURRENT_DATE' not in query.upper()
            assert 'NOW()' not in query.upper()


def test_imports_do_not_load_etl():
    importlib.import_module('manage')
    importlib.import_module('dataset')
    assert 'src.api.app' not in sys.modules
    assert 'src.config' not in sys.modules
