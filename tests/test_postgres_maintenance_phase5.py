from datetime import date
from pathlib import Path

from src.tasks import postgres_maintenance


REPO_ROOT = Path(__file__).resolve().parents[1]


class FakeCursor:
    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[object, ...]]] = []
        self.last_sql = ""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def execute(self, sql: str, params: tuple[object, ...] = ()) -> None:
        self.last_sql = sql
        self.executed.append((sql, params))

    def fetchone(self):
        if "to_regprocedure" in self.last_sql:
            return ("function_oid",)
        if "to_regclass" in self.last_sql:
            return ("relation_oid",)
        if "create_pipeline_forecast_snapshot" in self.last_sql:
            return (7,)
        if "create_pipeline_smoothing_forecast_snapshot" in self.last_sql:
            return (12,)
        if "cleanup_old_pipeline_forecast_snapshots" in self.last_sql:
            return (5,)
        if "cleanup_old_pipeline_smoothing_forecast_snapshots" in self.last_sql:
            return (3,)
        return (None,)


class FakeConnection:
    def __init__(self) -> None:
        self.cursor_instance = FakeCursor()
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def cursor(self):
        return self.cursor_instance

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closed = True


def _patch_connection(monkeypatch, connection: FakeConnection) -> None:
    monkeypatch.setenv("SUPABASE_DB_URL", "postgresql://example")
    monkeypatch.setattr(postgres_maintenance.psycopg, "connect", lambda _dsn: connection)


def test_refresh_materialized_views_uses_guarded_sql_function(monkeypatch) -> None:
    connection = FakeConnection()
    _patch_connection(monkeypatch, connection)

    postgres_maintenance.refresh_materialized_views()

    executed_sql = [sql for sql, _params in connection.cursor_instance.executed]
    assert "SELECT refresh_analytics_views();" in executed_sql
    assert connection.commits == 1
    assert connection.closed is True


def test_refresh_conversion_views_wrapper_uses_materialized_refresh(monkeypatch) -> None:
    calls: list[str] = []

    monkeypatch.setattr(
        postgres_maintenance,
        "refresh_materialized_views",
        lambda **_kwargs: calls.append("refresh"),
    )

    postgres_maintenance.refresh_conversion_views(
        logger=postgres_maintenance.logger,
        concurrently=True,
    )

    assert calls == ["refresh"]


def test_create_pipeline_forecast_snapshot_is_dated(monkeypatch) -> None:
    connection = FakeConnection()
    _patch_connection(monkeypatch, connection)

    inserted = postgres_maintenance.create_pipeline_forecast_snapshot(
        snapshot_date=date(2026, 6, 21)
    )

    assert inserted == 7
    assert any(
        sql == "SELECT create_pipeline_forecast_snapshot(%s::date);"
        and params == (date(2026, 6, 21),)
        for sql, params in connection.cursor_instance.executed
    )
    assert connection.commits == 1


def test_create_pipeline_smoothing_forecast_snapshot_is_dated(monkeypatch) -> None:
    connection = FakeConnection()
    _patch_connection(monkeypatch, connection)

    inserted = postgres_maintenance.create_pipeline_smoothing_forecast_snapshot(
        snapshot_date=date(2026, 6, 21)
    )

    assert inserted == 12
    assert any(
        sql == "SELECT create_pipeline_smoothing_forecast_snapshot(%s::date);"
        and params == (date(2026, 6, 21),)
        for sql, params in connection.cursor_instance.executed
    )
    assert connection.commits == 1


def test_cleanup_old_pipeline_forecast_snapshots_uses_retention(monkeypatch) -> None:
    connection = FakeConnection()
    _patch_connection(monkeypatch, connection)

    deleted = postgres_maintenance.cleanup_old_pipeline_forecast_snapshots(
        retain_days=365
    )

    assert deleted == 5
    assert any(
        sql == "SELECT cleanup_old_pipeline_forecast_snapshots(%s::integer);"
        and params == (365,)
        for sql, params in connection.cursor_instance.executed
    )
    assert connection.commits == 1


def test_cleanup_old_pipeline_smoothing_forecast_snapshots_uses_retention(monkeypatch) -> None:
    connection = FakeConnection()
    _patch_connection(monkeypatch, connection)

    deleted = postgres_maintenance.cleanup_old_pipeline_smoothing_forecast_snapshots(
        retain_days=365
    )

    assert deleted == 3
    assert any(
        sql == "SELECT cleanup_old_pipeline_smoothing_forecast_snapshots(%s::integer);"
        and params == (365,)
        for sql, params in connection.cursor_instance.executed
    )
    assert connection.commits == 1


def test_run_daily_maintenance_includes_smoothing(monkeypatch) -> None:
    calls: list[str] = []

    monkeypatch.setattr(
        postgres_maintenance,
        "refresh_materialized_views",
        lambda **_kwargs: calls.append("refresh"),
    )
    monkeypatch.setattr(
        postgres_maintenance,
        "create_pipeline_forecast_snapshot",
        lambda **_kwargs: calls.append("forecast_snapshot") or 7,
    )
    monkeypatch.setattr(
        postgres_maintenance,
        "cleanup_old_pipeline_forecast_snapshots",
        lambda **_kwargs: calls.append("forecast_cleanup") or 5,
    )
    monkeypatch.setattr(
        postgres_maintenance,
        "create_pipeline_smoothing_forecast_snapshot",
        lambda **_kwargs: calls.append("smoothing_snapshot") or 12,
    )
    monkeypatch.setattr(
        postgres_maintenance,
        "cleanup_old_pipeline_smoothing_forecast_snapshots",
        lambda **_kwargs: calls.append("smoothing_cleanup") or 3,
    )

    postgres_maintenance.run_daily_maintenance()

    assert calls == [
        "refresh",
        "forecast_snapshot",
        "forecast_cleanup",
        "smoothing_snapshot",
        "smoothing_cleanup",
    ]


def test_run_daily_forecast_snapshot_maintenance_wrapper(monkeypatch) -> None:
    calls: list[tuple[str, object]] = []

    def fake_run_daily_maintenance(**kwargs):
        calls.append(("daily", kwargs))

    monkeypatch.setattr(
        postgres_maintenance,
        "run_daily_maintenance",
        fake_run_daily_maintenance,
    )

    postgres_maintenance.run_daily_forecast_snapshot_maintenance(
        logger=postgres_maintenance.logger,
        snapshot_date=date(2026, 6, 21),
        retain_days=365,
    )

    assert calls == [
        (
            "daily",
            {
                "snapshot_date": date(2026, 6, 21),
                "retain_days": 365,
                "include_smoothing": True,
            },
        )
    ]


def test_smoothing_snapshot_sql_is_project_level() -> None:
    functions_sql = (
        REPO_ROOT / "src" / "database" / "schema" / "functions.sql"
    ).read_text(encoding="utf-8")
    schema_sql = (
        REPO_ROOT / "src" / "database" / "schema" / "schema.sql"
    ).read_text(encoding="utf-8")

    assert (
        "CREATE OR REPLACE FUNCTION create_pipeline_smoothing_forecast_snapshot"
        in functions_sql
    )
    assert "FROM vw_pipeline_smoothing_score_v1 s" in functions_sql

    for expected_token in (
        "base_forecast_month",
        "forecast_date",
        "combined_smoothed_probability",
        "expected_spread_days",
        "unsmoothed_allocated_value",
        "smoothed_allocated_value",
        "allocated_expected_value",
        "project_count",
    ):
        assert expected_token in functions_sql

    assert "CREATE TABLE IF NOT EXISTS pipeline_smoothing_forecast_snapshot" in schema_sql

    for expected_column in (
        "snapshot_date DATE NOT NULL",
        "project_id TEXT NOT NULL",
        "forecast_month DATE NOT NULL",
        "stage_bucket TEXT NOT NULL",
        "base_forecast_month DATE NOT NULL",
        "forecast_date DATE NOT NULL",
        "combined_smoothed_probability NUMERIC(8,6)",
        "expected_spread_days NUMERIC(10,2)",
        "unsmoothed_allocated_value NUMERIC(12,2)",
        "smoothed_allocated_value NUMERIC(12,2)",
        "allocated_expected_value NUMERIC(12,2)",
        "source_view TEXT NOT NULL DEFAULT 'vw_pipeline_smoothing_score_v1'",
        "PRIMARY KEY (snapshot_date, project_id, forecast_month)",
    ):
        assert expected_column in schema_sql

    for sql in (functions_sql, schema_sql):
        assert "smoothed_value_net" not in sql
        assert "smoothed_value_gross" not in sql