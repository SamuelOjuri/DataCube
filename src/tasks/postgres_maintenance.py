import logging
import os
from datetime import date
from typing import Optional

import psycopg

logger = logging.getLogger(__name__)

DEFAULT_FORECAST_RETENTION_DAYS = int(
    os.getenv("FORECAST_SNAPSHOT_RETENTION_DAYS", os.getenv("RETENTION_DAYS", "730"))
)


def _get_dsn() -> str:
    dsn = os.getenv("SUPABASE_DB_URL")
    if not dsn:
        raise RuntimeError("SUPABASE_DB_URL environment variable is required for maintenance jobs")
    return dsn


def _relation_exists(cur, relation_name: str) -> bool:
    cur.execute("SELECT to_regclass(%s);", (relation_name,))
    row = cur.fetchone()
    return bool(row and row[0])


def _function_exists(cur, function_signature: str) -> bool:
    cur.execute("SELECT to_regprocedure(%s);", (function_signature,))
    row = cur.fetchone()
    return bool(row and row[0])


def refresh_materialized_views(*, task_logger: Optional[logging.Logger] = None) -> None:
    """Refresh analytics, forecast, and smoothing materialized views where deployed."""
    log = task_logger or logger
    conn = psycopg.connect(_get_dsn())
    try:
        with conn.cursor() as cur:
            if _function_exists(cur, "public.refresh_analytics_views()"):
                log.info("Refreshing analytics and smoothing materialized views")
                cur.execute("SELECT refresh_analytics_views();")
            else:
                log.warning("refresh_analytics_views() not found; falling back to direct refreshes")
                for relation in (
                    "public.mv_pipeline_velocity_stats_v1",
                    "public.mv_quote_conversion_stats_v1",
                    "public.mv_pipeline_smoothed_revenue_monthly_12m_v1",
                ):
                    if _relation_exists(cur, relation):
                        log.info("Refreshing materialized view %s", relation)
                        cur.execute(f"REFRESH MATERIALIZED VIEW {relation};")
            conn.commit()
            log.info("Materialized view refresh complete")
    except Exception as exc:
        log.error("Error refreshing materialized views: %s", exc)
        conn.rollback()
        raise
    finally:
        conn.close()


def refresh_conversion_views(
    *,
    logger: logging.Logger,
    concurrently: bool = True,
    include_forecast: bool = True,
) -> None:
    """Compatibility wrapper used by the FastAPI APScheduler refresh job."""
    refresh_materialized_views(task_logger=logger)


def create_pipeline_forecast_snapshot(
    *,
    snapshot_date: Optional[date] = None,
    task_logger: Optional[logging.Logger] = None,
) -> int:
    log = task_logger or logger
    target_date = snapshot_date or date.today()
    conn = psycopg.connect(_get_dsn())
    try:
        with conn.cursor() as cur:
            if not _function_exists(cur, "public.create_pipeline_forecast_snapshot(date)"):
                log.warning("create_pipeline_forecast_snapshot(date) not found; skipping base snapshot")
                return 0
            log.info("Creating pipeline forecast snapshot for %s", target_date.isoformat())
            cur.execute(
                "SELECT create_pipeline_forecast_snapshot(%s::date);",
                (target_date,),
            )
            row = cur.fetchone()
            conn.commit()
            inserted = int(row[0]) if row and row[0] is not None else 0
            log.info(
                "Pipeline forecast snapshot created | snapshot_date=%s | rows_inserted=%s",
                target_date.isoformat(),
                inserted,
            )
            return inserted
    except Exception as exc:
        log.error("Error creating pipeline forecast snapshot: %s", exc)
        conn.rollback()
        raise
    finally:
        conn.close()


def create_pipeline_smoothing_forecast_snapshot(
    *,
    snapshot_date: Optional[date] = None,
    task_logger: Optional[logging.Logger] = None,
) -> int:
    log = task_logger or logger
    target_date = snapshot_date or date.today()
    conn = psycopg.connect(_get_dsn())
    try:
        with conn.cursor() as cur:
            if not _function_exists(cur, "public.create_pipeline_smoothing_forecast_snapshot(date)"):
                log.warning(
                    "create_pipeline_smoothing_forecast_snapshot(date) not found; skipping smoothing snapshot"
                )
                return 0
            log.info("Creating pipeline smoothing forecast snapshot for %s", target_date.isoformat())
            cur.execute(
                "SELECT create_pipeline_smoothing_forecast_snapshot(%s::date);",
                (target_date,),
            )
            row = cur.fetchone()
            conn.commit()
            inserted = int(row[0]) if row and row[0] is not None else 0
            log.info(
                "Pipeline smoothing forecast snapshot created | snapshot_date=%s | rows_inserted=%s",
                target_date.isoformat(),
                inserted,
            )
            return inserted
    except Exception as exc:
        log.error("Error creating pipeline smoothing forecast snapshot: %s", exc)
        conn.rollback()
        raise
    finally:
        conn.close()


def cleanup_old_pipeline_forecast_snapshots(
    *,
    retain_days: int = DEFAULT_FORECAST_RETENTION_DAYS,
    task_logger: Optional[logging.Logger] = None,
) -> int:
    log = task_logger or logger
    conn = psycopg.connect(_get_dsn())
    try:
        with conn.cursor() as cur:
            if not _function_exists(cur, "public.cleanup_old_pipeline_forecast_snapshots(integer)"):
                log.warning(
                    "cleanup_old_pipeline_forecast_snapshots(integer) not found; skipping base cleanup"
                )
                return 0
            cur.execute(
                "SELECT cleanup_old_pipeline_forecast_snapshots(%s::integer);",
                (retain_days,),
            )
            row = cur.fetchone()
            conn.commit()
            deleted = int(row[0]) if row and row[0] is not None else 0
            log.info(
                "Pipeline forecast snapshot cleanup complete | retain_days=%s | rows_deleted=%s",
                retain_days,
                deleted,
            )
            return deleted
    except Exception as exc:
        log.error("Error cleaning up pipeline forecast snapshots: %s", exc)
        conn.rollback()
        raise
    finally:
        conn.close()


def cleanup_old_pipeline_smoothing_forecast_snapshots(
    *,
    retain_days: int = DEFAULT_FORECAST_RETENTION_DAYS,
    task_logger: Optional[logging.Logger] = None,
) -> int:
    log = task_logger or logger
    conn = psycopg.connect(_get_dsn())
    try:
        with conn.cursor() as cur:
            if not _function_exists(
                cur, "public.cleanup_old_pipeline_smoothing_forecast_snapshots(integer)"
            ):
                log.warning(
                    "cleanup_old_pipeline_smoothing_forecast_snapshots(integer) not found; skipping smoothing cleanup"
                )
                return 0
            cur.execute(
                "SELECT cleanup_old_pipeline_smoothing_forecast_snapshots(%s::integer);",
                (retain_days,),
            )
            row = cur.fetchone()
            conn.commit()
            deleted = int(row[0]) if row and row[0] is not None else 0
            log.info(
                "Pipeline smoothing forecast snapshot cleanup complete | retain_days=%s | rows_deleted=%s",
                retain_days,
                deleted,
            )
            return deleted
    except Exception as exc:
        log.error("Error cleaning up pipeline smoothing forecast snapshots: %s", exc)
        conn.rollback()
        raise
    finally:
        conn.close()


def run_daily_maintenance(
    *,
    snapshot_date: Optional[date] = None,
    retain_days: int = DEFAULT_FORECAST_RETENTION_DAYS,
    include_smoothing: bool = True,
) -> None:
    """Run daily database maintenance tasks."""
    logger.info("Starting daily maintenance")

    refresh_materialized_views(task_logger=logger)
    base_inserted = create_pipeline_forecast_snapshot(
        snapshot_date=snapshot_date,
        task_logger=logger,
    )
    base_deleted = cleanup_old_pipeline_forecast_snapshots(
        retain_days=retain_days,
        task_logger=logger,
    )

    smoothing_inserted = 0
    smoothing_deleted = 0
    if include_smoothing:
        smoothing_inserted = create_pipeline_smoothing_forecast_snapshot(
            snapshot_date=snapshot_date,
            task_logger=logger,
        )
        smoothing_deleted = cleanup_old_pipeline_smoothing_forecast_snapshots(
            retain_days=retain_days,
            task_logger=logger,
        )

    logger.info(
        "Daily maintenance completed | base_rows_inserted=%s | base_rows_deleted=%s | smoothing_rows_inserted=%s | smoothing_rows_deleted=%s | retain_days=%s",
        base_inserted,
        base_deleted,
        smoothing_inserted,
        smoothing_deleted,
        retain_days,
    )


def run_daily_forecast_snapshot_maintenance(
    *,
    logger: logging.Logger,
    snapshot_date: Optional[date] = None,
    retain_days: int = DEFAULT_FORECAST_RETENTION_DAYS,
    include_smoothing: bool = True,
) -> None:
    """Compatibility wrapper used by the FastAPI APScheduler snapshot job."""
    run_daily_maintenance(
        snapshot_date=snapshot_date,
        retain_days=retain_days,
        include_smoothing=include_smoothing,
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_daily_maintenance()