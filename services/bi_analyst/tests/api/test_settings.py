import os
from pathlib import Path
import subprocess
import sys

import pytest
from pydantic import ValidationError

from bi_analyst.settings import Settings
from bi_analyst.contracts import csv_cell


def config(**kwargs):
    return Settings(read_dsn="host=db.example dbname=analyst user=bi_analyst_reader sslmode=verify-full",
                    state_dsn="host=db.example dbname=analyst user=bi_analyst_state sslmode=verify-full", **kwargs)


def test_reasoning_budget_defaults_preserve_query_and_source_limits():
    settings = config()
    assert settings.model_timeout_seconds == 90
    assert settings.workflow_timeout_seconds == 600
    assert settings.metric_timeout_seconds == 15
    assert settings.statement_timeout_ms == 5000
    assert settings.monday_read_timeout_seconds == 10
    assert settings.workflow_max_model_calls == 8


@pytest.mark.parametrize('values', [
    {'model_timeout_seconds': 0}, {'model_timeout_seconds': 91},
    {'workflow_timeout_seconds': 0}, {'workflow_timeout_seconds': 601},
])
def test_reasoning_budgets_remain_bounded(values):
    with pytest.raises(ValidationError):
        config(**values)


def test_reasoning_budget_environment_overrides(monkeypatch):
    monkeypatch.setenv('BI_ANALYST_READ_DSN', config().read_dsn.get_secret_value())
    monkeypatch.setenv('BI_ANALYST_STATE_DSN', config().state_dsn.get_secret_value())
    monkeypatch.setenv('BI_ANALYST_MODEL_TIMEOUT_SECONDS', '90')
    monkeypatch.setenv('BI_ANALYST_WORKFLOW_TIMEOUT_SECONDS', '600')
    settings = Settings.from_env()
    assert settings.model_timeout_seconds == 90
    assert settings.workflow_timeout_seconds == 600
    assert config(model_timeout_seconds=25, workflow_timeout_seconds=90).model_timeout_seconds == 25


def test_pool_budget_and_explicit_origins():
    assert config().connection_budget == 4
    with pytest.raises(ValidationError):
        config(replica_count=2)
    for origin in ["*", "https://*.example.com", "https://example.com/path", "http://example.com", "https://user:pw@example.com"]:
        with pytest.raises(ValidationError):
            config(cors_origins=[origin])


@pytest.mark.parametrize("change", ["sslmode=disable", "port=6543", "user=postgres", "user=bi_analyst_migrator", "options=-csearch_path=public", "hostaddr=127.0.0.1"])
def test_connection_fails_closed(change):
    data = config().model_dump()
    base = "host=db.example dbname=analyst user=bi_analyst_reader sslmode=verify-full"
    data["read_dsn"] = base + " " + change
    with pytest.raises(ValidationError):
        Settings(**data)


def test_import_does_not_start_etl_or_open_connections():
    code = '''
import socket, sys
def denied(*args, **kwargs):
    raise AssertionError("Network access during import")
socket.socket.connect = denied
import bi_analyst.api
assert not any(name == 'src' or name.startswith('src.') for name in sys.modules)
assert 'bi_analyst.api' in sys.modules
'''
    env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[2]))
    run = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True)
    assert run.returncode == 0, run.stderr


def test_bad_configuration_does_not_print_database_password():
    data = config().model_dump()
    data['read_dsn'] = 'host=db.example dbname=analyst user=postgres password=secret-marker'
    with pytest.raises(ValidationError) as error:
        Settings(**data)
    assert 'secret-marker' not in str(error.value)


@pytest.mark.parametrize("value", ["=SUM(1,2)", "  +1", "-formula", "@X", "\tX", "\nX"])
def test_csv_strings_are_safe(value):
    assert csv_cell(value) == "'" + value
    assert csv_cell(-3) == -3


def test_migration_and_runtime_use_the_same_permission_audit():
    from importlib.resources import files
    root = Path(__file__).resolve().parents[4]
    migration = (root/"src/database/migrations/20261008_003_analyst_permissions.sql").read_text(encoding="utf-8")
    embedded = migration.split("-- BEGIN SHARED AUDIT\n",1)[1].split("-- END SHARED AUDIT",1)[0]
    assert embedded == files("bi_analyst").joinpath("permissions.sql").read_text(encoding="utf-8")


def test_auth_migration_embeds_its_versioned_permission_audit():
    from importlib.resources import files
    root = Path(__file__).resolve().parents[4]
    migration = (root/'src/database/migrations/20261008_005_analyst_auth.sql').read_text(encoding='utf-8')
    embedded = migration.split('-- BEGIN SHARED AUDIT\n',1)[1].split('-- END SHARED AUDIT',1)[0]
    assert embedded == files('bi_analyst').joinpath('permissions_auth.sql').read_text(encoding='utf-8')


@pytest.mark.parametrize('change', [
    {'monday_client_secret':None}, {'monday_account_id':'*'}, {'session_seconds':901},
    {'auth_frontend_url':'https://evil.test/callback'},
    {'auth_frontend_url':'https://analyst.example.test/callback?next=evil'},
    {'monday_redirect_uri':'https://api.example.test/elsewhere'},
    {'monday_redirect_uri':'http://api.example.test/auth/callback'},
    {'monday_redirect_uri':'https://user:secret@api.example.test/auth/callback'},
])
def test_auth_configuration_fails_closed(change):
    values = dict(auth_provider='monday', monday_client_id='client',monday_client_secret='secret',
        monday_account_id='123',monday_redirect_uri='https://api.example.test/auth/callback',
        auth_frontend_url='https://analyst.example.test/auth/callback',cors_origins=['https://analyst.example.test'])
    values.update(change)
    with pytest.raises(ValidationError):
        config(**values)
