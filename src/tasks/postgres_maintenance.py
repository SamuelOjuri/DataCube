import os
import logging
from datetime import date
from typing import Optional
import psycopg

DEFAULT_FORECAST_RETENTION_DAYS = int(
    os.getenv("FORECAST_SNAPSHOT_RETENTION_DAYS", os.getenv("RETENTION_DAYS", "730"))
)

def _get_dsn() -> str:
    dsn = os.getenv("SUPABASE_DB_URL")
    if not dsn:
        raise RuntimeError("SUPABASE_DB_URL environment variable is required for maintenance jobs")
    return dsn

def _relation_exists(cur: psycopg.Cursor, relation_name: str) -> bool:
    cur.execute("SELECT to_regclass(%s);", (relation_name,))
    row = cur.fetchone()
    return bool(row and row[0])

def refresh_conversion_views(
    *,
    logger: logging.Logger,
    concurrently: bool = True,
    include_forecast: bool = True,
) -> None:
    dsn = _get_dsn()
    refresh_stmt = (
        "REFRESH MATERIALIZED VIEW CONCURRENTLY {view};"
        if concurrently
        else "REFRESH MATERIALIZED VIEW {view};"
    )

    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            views = ["conversion_metrics", "conversion_metrics_recent"]
            if include_forecast:
                if _relation_exists(cur, "public.mv_pipeline_forecast_monthly_12m_v1"):
                    views.append("mv_pipeline_forecast_monthly_12m_v1")
                else:
                    logger.warning(
                        "Forecast materialized view not found; skipping forecast refresh this cycle"
                    )
            for view in views:
                logger.info("Refreshing materialized view %s (concurrent=%s)", view, concurrently)
                cur.execute(refresh_stmt.format(view=view))

def create_pipeline_forecast_snapshot(
    *,
    logger: logging.Logger,
    snapshot_date: Optional[date] = None,
) -> int:
    dsn = _get_dsn()
    target_date = snapshot_date or date.today()
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT create_pipeline_forecast_snapshot(%s::date);",
                (target_date,),
            )
            row = cur.fetchone()
    inserted = int(row[0]) if row and row[0] is not None else 0
    logger.info(
        "Pipeline forecast snapshot created | snapshot_date=%s | rows_inserted=%s",
        target_date.isoformat(),
        inserted,
    )
    return inserted

def cleanup_old_pipeline_forecast_snapshots(
    *,
    logger: logging.Logger,
    retain_days: int = DEFAULT_FORECAST_RETENTION_DAYS,
) -> int:
    dsn = _get_dsn()
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT cleanup_old_pipeline_forecast_snapshots(%s::integer);",
                (retain_days,),
            )
            row = cur.fetchone()
    deleted = int(row[0]) if row and row[0] is not None else 0
    logger.info(
        "Pipeline forecast snapshot cleanup complete | retain_days=%s | rows_deleted=%s",
        retain_days,
        deleted,
    )
    return deleted

def run_daily_forecast_snapshot_maintenance(
    *,
    logger: logging.Logger,
    snapshot_date: Optional[date] = None,
    retain_days: int = DEFAULT_FORECAST_RETENTION_DAYS,
) -> None:
    inserted = create_pipeline_forecast_snapshot(
        logger=logger,
        snapshot_date=snapshot_date,
    )
    deleted = cleanup_old_pipeline_forecast_snapshots(
        logger=logger,
        retain_days=retain_days,
    )
    logger.info(
        "Daily forecast snapshot maintenance complete | rows_inserted=%s | rows_deleted=%s | retain_days=%s",
        inserted,
        deleted,
        retain_days,
    )