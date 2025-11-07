# src/tasks/postgres_maintenance.py
import os
import logging
import psycopg

def refresh_conversion_views(*, logger: logging.Logger, concurrently: bool = True) -> None:
    dsn = os.getenv("SUPABASE_DB_URL")
    if not dsn:
        raise RuntimeError("SUPABASE_DB_URL environment variable is required for refresh job")

    refresh_stmt = (
        "REFRESH MATERIALIZED VIEW CONCURRENTLY {view};"
        if concurrently else
        "REFRESH MATERIALIZED VIEW {view};"
    )

    views = ("conversion_metrics", "conversion_metrics_recent")

    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            for view in views:
                logger.info("Refreshing materialized view %s (concurrent=%s)", view, concurrently)
                cur.execute(refresh_stmt.format(view=view))