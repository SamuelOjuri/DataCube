"""Exploratory SQL end to end: real gateways, restricted reader role, LangGraph and the API."""
from uuid import uuid4

from psycopg.conninfo import make_conninfo
import psycopg
import pytest

from bi_analyst.exploration.policy import validate
from bi_analyst.exploration.service import ExplorationService
from test_exploration_policy import RELATIONS, VALID
from test_workflow_postgres import conversation, settle, setup, submit, workflow_database  # noqa: F401

pytestmark = pytest.mark.postgres
LAST_MONTH = ("SELECT sum(amount_invoiced) AS revenue FROM analyst_query.invoice_reporting_facts_v1 "
              "WHERE invoice_date >= date_trunc('month', current_date) - INTERVAL '1 month' "
              "AND invoice_date < date_trunc('month', current_date)")


def draft(sql=LAST_MONTH, columns=(('revenue', 'measure', 'source_currency'),), title='Invoiced revenue, last completed month'):
    return {'action': 'query', 'title': title, 'sql': sql,
            'columns': [{'name': n, 'role': r, 'unit': u} for n, r, u in columns]}


class Explorer:
    """Plans are unsupported by the certified compiler; exploration returns scripted drafts."""
    def __init__(self, *drafts):
        self.drafts, self.calls = list(drafts), []

    async def generate(self, stage, payload, schema):
        self.calls.append((stage, payload))
        data = {'action': 'unsupported', 'reason': 'unsupported'} if stage == 'plan' else self.drafts.pop(0)
        return schema.model_validate(data), {'promptTokenCount': 3, 'candidatesTokenCount': 2}


@pytest.fixture
def explore(setup):
    admin, dsn, owner, config, start = setup
    return admin, dsn, config.model_copy(update={'exploratory_enabled': True}), start


@pytest.mark.parametrize('name', VALID)
def test_regenerated_queries_execute_under_the_reader_role(workflow_database, name):
    _, dsn = workflow_database
    validated = validate(VALID[name], RELATIONS)
    with psycopg.connect(make_conninfo(dsn, user='bi_analyst_reader')) as conn:
        with conn.transaction():
            conn.execute("SET TRANSACTION READ ONLY")
            with conn.cursor(name='exploration_probe') as cursor:
                cursor.execute(validated.sql)
                assert tuple(c.name for c in cursor.description) == validated.columns
                cursor.fetchall()


def test_reader_role_itself_rejects_writes_and_unapproved_sources(workflow_database):
    _, dsn = workflow_database
    with psycopg.connect(make_conninfo(dsn, user='bi_analyst_reader'), autocommit=True) as conn:
        with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
            conn.execute("CREATE TEMP TABLE stolen AS SELECT 1")
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            conn.execute("SELECT secret FROM analyst_query.future_data")


def test_exploratory_answer_is_grounded_stored_and_exportable(explore):
    _, _, config, start = explore
    provider = Explorer(draft())
    with start(provider, config_override=config) as client:
        run_id = submit(client, question='What was revenue last completed month?')
        row = settle(client, run_id)
        assert row['status'] == 'completed', row
        assert row['model_calls'] == 2 and row['tool_calls'] == 1
        answer = row['answer']
        assert answer['claims'][0]['value'] == '100.00' and answer['chart']['kind'] == 'number'
        assert 'GBP 100.00' in answer['text']
        assert any('not a certified metric' in notice for notice in answer['notices'])
        result_id = answer['result']['result_id']
        result = client.get(f'/v1/results/{result_id}').json()
        p = result['provenance']
        assert p['kind'] == 'exploratory' and p['relations'] == ['invoice_reporting_facts_v1']
        assert p['sql'] == validate(LAST_MONTH, RELATIONS).sql and p['columns'][0]['type'] == 'decimal'
        assert result['rows'] == [['100.00']]
        assert client.get(f'/v1/results/{result_id}/projects').status_code == 409
        export = client.get(f'/v1/results/{result_id}/export')
        assert export.status_code == 200 and '100.00' in export.text
        assert 'exploring' in client.get(f'/v1/runs/{run_id}/events').text
        assert [stage for stage, _ in provider.calls] == ['plan', 'explore']


def test_follow_up_stays_exploratory_with_previous_sql(explore):
    _, _, config, start = explore
    by_category = draft("SELECT category, sum(amount_invoiced) AS revenue FROM analyst_query.invoice_reporting_facts_v1 "
                        "GROUP BY category ORDER BY revenue DESC",
                        (('category', 'dimension', 'none'), ('revenue', 'measure', 'source_currency')),
                        'Invoiced revenue by category, all completed months')
    provider = Explorer(draft(), by_category)
    with start(provider, config_override=config) as client:
        thread = conversation(client)
        first = submit(client, thread, question='What was revenue last completed month?')
        assert settle(client, first)['status'] == 'completed'
        second = submit(client, thread, follow_up_to=first, question='Break that down by category')
        row = settle(client, second)
        assert row['status'] == 'completed', row
        assert row['answer']['chart']['kind'] == 'table'
        assert [stage for stage, _ in provider.calls] == ['plan', 'explore', 'explore']
        previous = provider.calls[-1][1]['previous']
        assert previous['question'] == 'What was revenue last completed month?' and 'invoice_reporting_facts_v1' in previous['sql']


@pytest.mark.parametrize('broken,expected,private', [
    ("SELECT sum(amount_invoicd) AS revenue FROM analyst_query.invoice_reporting_facts_v1", '42703', None),
    ("SELECT sum(account::numeric) AS revenue FROM analyst_query.projects_v1", '22P02', 'A, B'),
    ("SELECT pg_sleep(1) AS revenue", 'pg_sleep', None),
])
def test_database_and_policy_errors_are_repaired_with_sanitised_feedback(explore, broken, expected, private):
    _, _, config, start = explore
    provider = Explorer(draft(broken), draft())
    with start(provider, config_override=config) as client:
        row = settle(client, submit(client, question='Revenue last completed month'))
        assert row['status'] == 'completed', row
        feedback = provider.calls[-1][1]['feedback']
        assert len(feedback) == 1 and expected in feedback[0]['error']
        if private:
            assert private not in feedback[0]['error']


def test_statement_timeout_is_repairable_and_explained_after_bounded_attempts(explore):
    _, _, config, start = explore
    slow = draft("SELECT count(*) AS revenue FROM analyst_query.projects_v1 CROSS JOIN generate_series(1, 200000000) AS g",
                 (('revenue', 'measure', 'count'),))
    provider = Explorer(slow, slow, slow)
    with start(provider, config_override=config.model_copy(update={'statement_timeout_ms': 200})) as client:
        row = settle(client, submit(client, question='Count everything'))
        assert row['status'] == 'completed' and row['error_code'] is None and row['tool_calls'] == 3
        assert row['answer']['kind'] == 'explanation' and row['answer']['reason'] == 'query_failed'
        assert all('time limit' in item['error'] for item in provider.calls[-1][1]['feedback'])


def test_declined_question_is_explained_without_querying(explore):
    _, _, config, start = explore
    provider = Explorer({'action': 'decline', 'reason': 'forecast',
                         'explanation': 'Invoices are recorded for completed months up to September 2026.',
                         'suggestions': ['Show monthly invoiced revenue for the last 12 completed months.']})
    with start(provider, config_override=config) as client:
        run_id = submit(client, question='Predict next month revenue')
        row = settle(client, run_id)
        assert row['status'] == 'completed' and row['tool_calls'] == 0
        answer = row['answer']
        assert answer['title'] == "Forecasts aren't available"
        assert 'September 2026' in answer['text'] and len(answer['suggestions']) == 1
        events = client.get(f'/v1/runs/{run_id}/events').text
        assert 'event: answer' in events and 'explaining' in events
        # Explanations carry no dataset and cannot anchor a follow-up plan.
        follow = client.post(f"/v1/conversations/{row['conversation_id']}/messages",
                             json={'question': 'And by month?', 'idempotency_key': str(uuid4()), 'follow_up_to': run_id})
        assert follow.status_code == 409


def test_partial_answer_caveats_are_checked_and_stored(explore):
    _, _, config, start = explore
    caveated = {**draft(), 'caveats': ['The current month is not complete, so it is excluded.',
                                       'Revenue was about £120,000 last year.']}
    with start(Explorer(caveated), config_override=config) as client:
        row = settle(client, submit(client, question='Revenue last completed month'))
        assert row['status'] == 'completed', row
        assert 'The current month is not complete, so it is excluded.' in row['answer']['text']
        assert '120,000' not in row['answer']['text']
        result = client.get(f"/v1/results/{row['answer']['result']['result_id']}").json()
        assert result['provenance']['caveats'] == ['The current month is not complete, so it is excluded.']


def test_unsupported_question_is_explained_when_exploration_is_off(setup):
    _, _, _, config, start = setup
    with start(Explorer(), config_override=config) as client:
        row = settle(client, submit(client, question='What was revenue for January 2026?'))
        assert row['status'] == 'completed' and row['model_calls'] == 1
        assert row['answer']['reason'] == 'unsupported' and len(row['answer']['suggestions']) == 3


def test_service_row_limit_marks_result_limited(explore):
    admin, _, config, start = explore
    many = draft("SELECT g::date AS month, count(p.monday_id) AS projects FROM generate_series(DATE '2020-01-01', "
                 "DATE '2026-01-01', INTERVAL '1 month') AS g LEFT JOIN analyst_query.projects_v1 p "
                 "ON date_trunc('month', p.date_created) = g GROUP BY 1 ORDER BY 1",
                 (('month', 'dimension', 'none'), ('projects', 'measure', 'count')), 'Projects created by month')
    provider = Explorer(many)
    with start(provider, config_override=config.model_copy(update={'exploratory_max_rows': 5})) as client:
        row = settle(client, submit(client, question='Projects created per month since 2020'))
        assert row['status'] == 'completed', row
        result = client.get(f"/v1/results/{row['answer']['result']['result_id']}").json()
        assert len(result['rows']) == 5 and result['provenance']['truncation_reasons'] == ['row_limit']
        assert result['provenance']['columns'][0]['type'] == 'date'
        assert isinstance(client.app_instance.state.workflow.exploration, ExplorationService)
