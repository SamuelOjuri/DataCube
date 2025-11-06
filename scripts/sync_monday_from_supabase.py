# scripts/sync_monday_from_supabase.py
import argparse
import logging
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.tasks.pipeline import sync_projects_to_monday  # noqa: E402

DEFAULT_SINCE = "2023-01-01"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Push Supabase analysis results into Monday.com.")
    parser.add_argument(
        "--since",
        default=DEFAULT_SINCE,
        help="Minimum projects.date_created (YYYY-MM-DD). Default: 2023-01-01.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Supabase fetch size. Default: 100.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Stop after syncing this many projects.",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.5,
        help="Extra delay between Monday requests. Default: 0.5s.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log what would happen without calling Monday.",
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
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO).",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="[%(asctime)s] %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    log = logging.getLogger("sync_monday_from_supabase")

    log.info(
        "Sync start | since=%s | batch_size=%s | limit=%s | dry_run=%s",
        args.since,
        args.batch_size,
        args.limit,
        args.dry_run,
    )

    start = time.time()
    stats = sync_projects_to_monday(
        since=args.since,
        batch_size=args.batch_size,
        limit=args.limit,
        sleep_interval=args.sleep,
        dry_run=args.dry_run,
        include_updates=not args.no_updates,
        throttle=not args.no_throttle,
        logger=log,
    )

    elapsed = time.time() - start
    log.info("Sync complete | stats=%s | runtime=%.1fs", stats, elapsed)

    if stats.get("errors") and not args.dry_run:
        sys.exit(1)


if __name__ == "__main__":
    main()