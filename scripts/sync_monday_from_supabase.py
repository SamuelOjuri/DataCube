# scripts/sync_monday_from_supabase.py
import argparse
import logging
import sys
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.database.supabase_client import SupabaseClient  # noqa: E402
from src.services.monday_update_service import MondayUpdateService  # noqa: E402

DEFAULT_SINCE = "2023-01-01"


def _fetch_project_batch(
    db: SupabaseClient,
    since: str,
    offset: int,
    limit: int,
) -> List[Dict]:
    query = (
        db.client.table("projects")
        .select("monday_id,date_created")
        .gte("date_created", since)
        .order("monday_id", desc=False)
        .range(offset, offset + limit - 1)
    )
    response = query.execute()
    return response.data or []


def iter_projects(
    db: SupabaseClient,
    since: str,
    batch_size: int,
    limit: Optional[int],
) -> Iterable[List[Dict]]:
    offset = 0
    processed = 0

    while True:
        batch = _fetch_project_batch(db, since, offset, batch_size)
        if not batch:
            break

        if limit is not None and processed + len(batch) > limit:
            batch = batch[: max(0, limit - processed)]

        if not batch:
            break

        yield batch
        processed += len(batch)

        if limit is not None and processed >= limit:
            break
        if len(batch) < batch_size:
            break

        offset += len(batch)


def main() -> None:
    parser = argparse.ArgumentParser(description="Push Supabase analysis results into Monday.com.")
    parser.add_argument("--since", default=DEFAULT_SINCE, help="Minimum projects.date_created (YYYY-MM-DD). Default: 2023-01-01.")
    parser.add_argument("--batch-size", type=int, default=100, help="Supabase fetch size. Default: 200.")
    parser.add_argument("--limit", type=int, default=None, help="Stop after syncing this many projects.")
    parser.add_argument("--sleep", type=float, default=0.5, help="Extra delay between Monday requests. Default: 0.2s.")
    parser.add_argument("--dry-run", action="store_true", help="Log what would happen without calling Monday.")
    parser.add_argument("--no-updates", action="store_true", help="Skip creating/editing Monday item updates.")
    parser.add_argument("--no-throttle", action="store_true", help="Disable Monday client's built-in rate delay.")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    logging.info(
        "Sync start | since=%s | batch_size=%s | limit=%s | dry_run=%s",
        args.since,
        args.batch_size,
        args.limit,
        args.dry_run,
    )

    try:
        db = SupabaseClient()
    except ValueError as exc:
        logging.error("Supabase client initialisation failed: %s", exc)
        sys.exit(1)

    service = MondayUpdateService(db_client=db)

    stats = {
        "total": 0,
        "synced": 0,
        "dry_run": 0,
        "missing": 0,
        "errors": 0,
    }
    failed_projects: List[str] = []

    include_update = not args.no_updates
    throttle = not args.no_throttle

    try:
        for batch_index, batch in enumerate(
            iter_projects(db, args.since, max(1, args.batch_size), args.limit), start=1
        ):
            logging.info("Fetched batch %s with %s project(s)", batch_index, len(batch))

            for project in batch:
                project_id = str(project.get("monday_id") or "").strip()
                stats["total"] += 1

                if not project_id:
                    stats["missing"] += 1
                    logging.warning("Skipping project with missing monday_id: %s", project)
                    continue

                analysis = db.get_latest_analysis_result(project_id)
                if not analysis:
                    stats["missing"] += 1
                    logging.warning("No analysis_results row for project_id=%s", project_id)
                    failed_projects.append(project_id)
                    continue

                if args.dry_run:
                    stats["dry_run"] += 1
                    logging.info(
                        "Dry-run: would sync project %s | rating=%s | conversion=%.3f | gestation=%s",
                        project_id,
                        analysis.get("rating_score"),
                        analysis.get("expected_conversion_rate") or 0.0,
                        analysis.get("expected_gestation_days"),
                    )
                    continue

                try:
                    result = service.sync_project(
                        project_id=project_id,
                        analysis=analysis,
                        include_update=include_update,
                        throttle=throttle,
                    )
                    if result.get("success"):
                        stats["synced"] += 1
                        logging.info(
                            "Synced project %s | columns=%s | update_action=%s",
                            project_id,
                            result.get("columns"),
                            result.get("update_action"),
                        )
                    else:
                        stats["errors"] += 1
                        logging.warning(
                            "Failed to sync project %s | error=%s",
                            project_id,
                            result.get("error") or "unknown",
                        )
                        failed_projects.append(project_id)
                except Exception as exc:  # noqa: BLE001
                    stats["errors"] += 1
                    logging.exception("Error syncing project %s: %s", project_id, exc)
                    failed_projects.append(project_id)

                if args.sleep > 0:
                    time.sleep(args.sleep)

    except KeyboardInterrupt:
        logging.warning("Interrupted by user; summarising progress…")

    logging.info(
        "Sync complete | total=%s | synced=%s | dry_run=%s | missing=%s | errors=%s",
        stats["total"],
        stats["synced"],
        stats["dry_run"],
        stats["missing"],
        stats["errors"],
    )
    if failed_projects:
        unique_failed = sorted(set(failed_projects))
        logging.warning(
            "Failed project IDs (%s): %s",
            len(unique_failed),
            ", ".join(unique_failed),
        )

    if stats["errors"] and not args.dry_run:
        sys.exit(1)


if __name__ == "__main__":
    main()