# scripts/resync_monday_projects.py
"""
Retry Monday sync for specific project IDs.

Usage examples:
    python scripts/resync_monday_projects.py --ids 5048042707,5052054666
    python scripts/resync_monday_projects.py --ids-file failed_ids.txt --dry-run
    python scripts/resync_monday_projects.py --ids 5048042707 --sleep 0.3 --no-updates
"""

import argparse
import logging
import sys
import time
from pathlib import Path
from typing import Iterable, List, Optional, Set

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.database.supabase_client import SupabaseClient  # noqa: E402
from src.services.monday_update_service import MondayUpdateService  # noqa: E402


def _load_ids(args: argparse.Namespace) -> List[str]:
    ids: Set[str] = set()

    if args.ids:
        for part in args.ids.split(","):
            trimmed = part.strip()
            if trimmed:
                ids.add(trimmed)

    if args.ids_file:
        path = args.ids_file if args.ids_file.is_absolute() else PROJECT_ROOT / args.ids_file
        if not path.exists():
            raise FileNotFoundError(f"IDs file not found: {path}")
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                trimmed = line.strip().strip(",")
                if trimmed:
                    ids.add(trimmed)

    if not ids:
        raise ValueError("No project IDs were provided. Use --ids and/or --ids-file.")

    return sorted(ids)


def main() -> None:
    parser = argparse.ArgumentParser(description="Retry Monday sync for specific project IDs.")
    parser.add_argument(
        "--ids",
        help="Comma-separated Monday item IDs to resync (e.g. 5048042707,5052054666).",
    )
    parser.add_argument(
        "--ids-file",
        type=Path,
        help="Path to a file containing one Monday item ID per line.",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.2,
        help="Delay (seconds) between updates (default: 0.2).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log what would happen without writing to Monday.",
    )
    parser.add_argument(
        "--no-updates",
        action="store_true",
        help="Skip creating/editing Monday item updates; only numeric columns are changed.",
    )
    parser.add_argument(
        "--no-throttle",
        action="store_true",
        help="Disable Monday client's built-in rate-delay when posting mutations.",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    try:
        target_ids = _load_ids(args)
    except Exception as exc:  # noqa: BLE001
        logging.error("Failed to read project IDs: %s", exc)
        sys.exit(1)

    logging.info(
        "Resync start | ids=%s | dry_run=%s | sleep=%s",
        len(target_ids),
        args.dry_run,
        args.sleep,
    )

    try:
        db = SupabaseClient()
    except ValueError as exc:
        logging.error("Supabase client initialisation failed: %s", exc)
        sys.exit(1)

    service = MondayUpdateService(db_client=db)
    include_update = not args.no_updates
    throttle = not args.no_throttle

    stats = {
        "total": 0,
        "synced": 0,
        "dry_run": 0,
        "missing": 0,
        "errors": 0,
    }
    failed_ids: List[str] = []

    for project_id in target_ids:
        project_id = project_id.strip()
        if not project_id:
            continue

        stats["total"] += 1
        logging.info("Processing %s", project_id)

        try:
            proj_resp = (
                db.client.table("projects")
                .select("monday_id, date_created")
                .eq("monday_id", project_id)
                .limit(1)
                .execute()
            )
        except Exception as exc:  # noqa: BLE001
            stats["errors"] += 1
            failed_ids.append(project_id)
            logging.exception("Supabase query failed for project %s: %s", project_id, exc)
            continue

        if not proj_resp.data:
            stats["missing"] += 1
            failed_ids.append(project_id)
            logging.warning("Project %s not found in Supabase 'projects' table.", project_id)
            continue

        analysis = db.get_latest_analysis_result(project_id)
        if not analysis:
            stats["missing"] += 1
            failed_ids.append(project_id)
            logging.warning("No analysis_results payload for project %s.", project_id)
            continue

        if args.dry_run:
            stats["dry_run"] += 1
            logging.info(
                "Dry-run: would sync %s | rating=%s | conversion=%.3f | gestation=%s",
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
        except Exception as exc:  # noqa: BLE001
            stats["errors"] += 1
            failed_ids.append(project_id)
            logging.exception("Error syncing project %s: %s", project_id, exc)
            if args.sleep > 0:
                time.sleep(args.sleep)
            continue

        if result.get("success"):
            stats["synced"] += 1
            logging.info(
                "Synced %s | columns=%s | update_action=%s",
                project_id,
                result.get("columns"),
                result.get("update_action"),
            )
        else:
            stats["errors"] += 1
            failed_ids.append(project_id)
            logging.warning(
                "Failed to sync %s | error=%s | detail=%s",
                project_id,
                result.get("error"),
                result.get("detail"),
            )

        if args.sleep > 0:
            time.sleep(args.sleep)

    logging.info(
        "Resync complete | total=%s | synced=%s | dry_run=%s | missing=%s | errors=%s",
        stats["total"],
        stats["synced"],
        stats["dry_run"],
        stats["missing"],
        stats["errors"],
    )

    if failed_ids:
        logging.warning(
            "Still failing (%s): %s",
            len(failed_ids),
            ", ".join(sorted(set(failed_ids))),
        )

    if stats["errors"] and not args.dry_run:
        sys.exit(1)


if __name__ == "__main__":
    main()