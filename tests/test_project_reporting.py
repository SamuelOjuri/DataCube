from pathlib import Path
from unittest.mock import Mock

import pytest

from scripts import project_reporting as migration
from scripts import project_placeholders as placeholders
from src.services.analysis_service import AnalysisService
from src.services.monday_update_service import MondayUpdateService


def test_analysis_skips_placeholder_before_numeric_llm_or_storage():
    db = Mock()
    db.is_project_reporting_excluded.return_value = True
    service = AnalysisService(db_client=db)
    service.analyze_project = Mock(side_effect=AssertionError('must not analyze'))
    result = service.analyze_and_store('101', with_llm=True)
    assert result['success'] and result['skipped']
    db.client.table.assert_not_called()
    db.store_analysis_result.assert_not_called()


def test_monday_push_skips_existing_analysis_for_placeholder():
    db, monday = Mock(), Mock()
    db.is_project_reporting_excluded.return_value = True
    service = MondayUpdateService(db_client=db,monday_client=monday)
    assert service.sync_project('101', {'rating_score':80})['skipped']
    monday.update_item_columns.assert_not_called()
    monday.create_item_update.assert_not_called()


def test_classification_lookup_failure_does_not_run_analysis():
    db = Mock()
    db.is_project_reporting_excluded.side_effect = RuntimeError('unavailable')
    with pytest.raises(RuntimeError):
        AnalysisService(db_client=db).analyze_and_store('101')
    db.store_analysis_result.assert_not_called()


def test_schema_embeds_the_same_reporting_contract():
    schema = Path('src/database/schema/schema.sql').read_text(encoding='utf-8')
    core = schema.split('-- BEGIN project_reporting.sql\n')[1].split('-- END project_reporting.sql')[0]
    assert core == migration.CORE.read_text(encoding='utf-8')


def test_sql_rewrite_preserves_literals_and_base_grouping():
    text = "SELECT 'projects' AS kind, projects.id FROM projects"
    assert migration.filtered_definition('example',text) == (
        "SELECT 'projects' AS kind, public.reportable_projects.id FROM public.reportable_projects")
    original = 'SELECT p.*, count(s.id) FROM projects p LEFT JOIN subitems s ON true GROUP BY p.id;'
    result = migration.filtered_definition('project_analytics',original)
    assert 'FROM projects p' in result and 'excluded_project_ids()' in result
    assert migration.filtered_definition('project_analytics',result)==result


def test_rebuild_includes_dependents_and_orders_them():
    catalog = dict(views=[dict(name='conversion_metrics',kind='m',definition='SELECT count(*) FROM projects'),
                          dict(name='report',kind='v',definition='SELECT * FROM conversion_metrics'),
                          dict(name='data_freshness',kind='v',definition='SELECT count(*) FROM projects')],
                   edges=[dict(child='report',parent='conversion_metrics')])
    plan = migration.plan(catalog)
    assert plan['changed']==['conversion_metrics']
    assert plan['rebuild']==['conversion_metrics','report']


def empty_source():
    return dict(id='101', name='New project',state='active',
                board={'id':str(placeholders.PARENT_BOARD_ID)},parent_item=None,subitems=[],
                column_values=[dict(id=col,text='Open Enquiry' if f=='pipeline_stage' else '')
                               for f,col in placeholders.PARENT_COLUMNS.items() if f!='name'])


def test_missing_source_can_be_classified_but_never_archived():
    selection = dict(project_ids=['101'],reason='User-reviewed redundant record')
    stored = {'101':dict(empty_candidate=True,child_count=0)}
    decision = placeholders.decisions(selection,stored,{})[0]
    assert decision['classification']=='redundant_placeholder'
    assert not decision['archive_eligible'] and not decision['lifecycle_resolved']
    assert placeholders.source_objections(None)


@pytest.mark.parametrize('change',[
    {'name':'12345'}, {'board':{'id':'unexpected'}}, {'subitems':[{'id':'201'}]},
    {'column_values':[]}, {'state':'deleted'}, {'parent_item':{'id':'201'}},
])
def test_fresh_source_changes_prevent_classification_and_archive(change):
    source = empty_source() | change
    decision = placeholders.decisions(dict(project_ids=['101'],reason='Reviewed'),
        {'101':dict(empty_candidate=True,child_count=0)},{'101':source})[0]
    assert decision['classification']=='needs_review' and not decision['archive_eligible']


def test_free_hold_overrides_empty_source_and_stored_record():
    result = placeholders.decisions(dict(project_ids=['101'],reason='Reviewed',hold_for_review={'101':'FREE'}),
        {'101':dict(empty_candidate=True,child_count=0)},{'101':empty_source()})[0]
    assert result['classification']=='needs_review' and not result['archive_eligible']


def test_current_empty_active_item_is_eligible_for_reversible_archive():
    result = placeholders.decisions(dict(project_ids=['101'],reason='Reviewed'),
        {'101':dict(empty_candidate=True,child_count=0)},{'101':empty_source()})[0]
    assert result['classification']=='redundant_placeholder' and result['archive_eligible']


def test_source_formula_value_stops_archive_even_when_stored_amount_is_blank():
    source = empty_source()
    for col in source['column_values']:
        if col['id']==placeholders.PARENT_COLUMNS['total_order_value']:
            col['display_value']='1200'
    assert 'Source contains total_order_value' in placeholders.source_objections(source)


def test_archived_formula_null_string_is_preserved_as_missing_evidence():
    source = empty_source()
    source['state']='archived'
    for col in source['column_values']:
        if col['id']==placeholders.PARENT_COLUMNS['project_value']:
            col['display_value']='null'
    assert not placeholders.source_objections(source)


def test_archive_checks_source_and_verifies_reversible_mutation(monkeypatch,tmp_path):
    before = empty_source()
    after = before | {'state':'archived'}
    reads = iter([{'101':before},{'101':after}])
    monkeypatch.setattr(placeholders,'source_rows',lambda *a: next(reads))
    monkeypatch.setattr(placeholders,'read_projects',lambda *a: {'101':dict(empty_candidate=True,child_count=0)})
    connection,monday = Mock(),Mock()
    connection.execute.return_value.fetchone.return_value = {'reporting_excluded':True}
    monday.session.post.return_value.json.return_value = {'data':{'archive_item':{'id':'101','state':'archived'}}}
    staged = {'decisions':[dict(monday_id='101',archive_eligible=True)]}
    assert placeholders.archive(connection,monday,staged,tmp_path)==[dict(monday_id='101',status='archived_verified')]
    sent = monday.session.post.call_args.kwargs['json']
    assert sent == {'query':placeholders.ARCHIVE_MUTATION,'variables':{'id':'101'}}
    assert 'delete' not in sent['query']
    assert (tmp_path/'archive-before-101.json').exists() and (tmp_path/'archive-after-101.json').exists()


def test_archive_never_mutates_when_source_is_unavailable(monkeypatch,tmp_path):
    monkeypatch.setattr(placeholders,'source_rows',lambda *a: {})
    monkeypatch.setattr(placeholders,'read_projects',lambda *a: {'101':dict(empty_candidate=True,child_count=0)})
    connection,monday = Mock(),Mock()
    connection.execute.return_value.fetchone.return_value = {'reporting_excluded':True}
    with pytest.raises(ValueError,match='no longer eligible'):
        placeholders.archive(connection,monday,{'decisions':[dict(monday_id='101',archive_eligible=True)]},tmp_path)
    monday.session.post.assert_not_called()
