#!/usr/bin/env python
"""
Backfill numeric-only analysis for all projects created on/after a cutoff date.

This script:
1) Queries `projects` where `date_created >= cutoff`
2) Calls `AnalysisService.analyze_and_store(monday_id, with_llm=False)` for each project
3) Writes numeric-only results to `analysis_results` (unless --dry-run is used)

Usage:
    python scripts/backfill_analysis_numeric.py --cutoff 2024-01-01
    python scripts/backfill_analysis_numeric.py --cutoff 2024-01-01 --limit 500
    python scripts/backfill_analysis_numeric.py --cutoff 2024-01-01 --resume-after 1776022610
    python scripts/backfill_analysis_numeric.py --cutoff 2024-01-01 --dry-run
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterator, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.database.supabase_client import SupabaseClient  # noqa: E402
from src.services.analysis_service import AnalysisService  # noqa: E402

logger = logging.getLogger("backfill_analysis_numeric")


def _parse_date(value: str) -> str:
    try:
        dt = datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"cutoff must be YYYY-MM-DD (got {value!r})"
        ) from exc
    return dt.strftime("%Y-%m-%d")


def _iter_project_batches(
    db: SupabaseClient,
    *,
    cutoff: str,
    batch_size: int,
    resume_after: Optional[str],
    limit: Optional[int],
) -> Iterator[List[Dict[str, object]]]:
    """
    Keyset pagination over projects table by monday_id.
    Fetches all rows where date_created >= cutoff.
    """
    cursor = resume_after
    processed = 0

    while True:
        query = (
            db.client.table("projects")
            .select("monday_id,date_created")
            .gte("date_created", cutoff)
            .order("monday_id", desc=False)
            .limit(batch_size)
        )
        if cursor:
            query = query.gt("monday_id", cursor)

        rows = query.execute().data or []
        if not rows:
            break

        if limit is not None and processed + len(rows) > limit:
            rows = rows[: max(0, limit - processed)]

        if not rows:
            break

        yield rows
        processed += len(rows)
        cursor = str(rows[-1].get("monday_id") or "")

        if limit is not None and processed >= limit:
            break


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run numeric-only analysis backfill from a cutoff date."
    )
    parser.add_argument(
        "--cutoff",
        required=True,
        type=_parse_date,
        help="Only process projects with date_created on/after this date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=200,
        help="Supabase fetch size per batch (default: 200).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Stop after this many projects (for smoke tests).",
    )
    parser.add_argument(
        "--resume-after",
        default=None,
        help="Resume after this monday_id (exclusive).",
    )
    parser.add_argument(
        "--throttle",
        type=float,
        default=0.0,
        help="Seconds to sleep between project analyses (default: 0).",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=None,
        help="Override AnalysisService lookback window used for historical baselines.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute analysis only (no write to analysis_results).",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=100,
        help="Log progress every N projects (default: 100).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO).",
    )
    return parser


def main() -> None:
    args = _build_parser().parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="[%(asctime)s] %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    db = SupabaseClient()
    svc = AnalysisService(db_client=db, lookback_days=args.lookback_days)

    stats = {
        "total": 0,
        "updated": 0,   # persisted rows (or analyzed rows in dry-run)
        "missing": 0,
        "errors": 0,
    }
    failed_ids: List[str] = []

    logger.info(
        "Starting numeric backfill | cutoff=%s | batch_size=%s | limit=%s | resume_after=%s | dry_run=%s",
        args.cutoff,
        args.batch_size,
        args.limit,
        args.resume_after,
        args.dry_run,
    )

    started_at = time.time()

    for batch_idx, batch in enumerate(
        _iter_project_batches(
            db,
            cutoff=args.cutoff,
            batch_size=max(1, args.batch_size),
            resume_after=args.resume_after,
            limit=args.limit,
        ),
        start=1,
    ):
        logger.info("Processing batch %s (%s projects)", batch_idx, len(batch))

        for row in batch:
            project_id = str(row.get("monday_id") or "").strip()
            if not project_id:
                stats["missing"] += 1
                logger.warning("Skipping row with missing monday_id: %s", row)
                continue

            stats["total"] += 1

            try:
                if args.dry_run:
                    project = (
                        db.client.table("projects")
                        .select("*")
                        .eq("monday_id", project_id)
                        .single()
                        .execute()
                        .data
                    )
                    if not project:
                        stats["errors"] += 1
                        failed_ids.append(project_id)
                        logger.warning("Project not found in dry-run: %s", project_id)
                        continue

                    # numeric compute only, no persistence
                    _ = svc.analyze_project(project)
                    stats["updated"] += 1
                else:
                    result = svc.analyze_and_store(project_id, with_llm=False)
                    if result.get("success"):
                        stats["updated"] += 1
                    else:
                        stats["errors"] += 1
                        failed_ids.append(project_id)
                        logger.warning(
                            "Analysis failed for %s: %s",
                            project_id,
                            result.get("error") or "unknown error",
                        )

            except Exception as exc:  # noqa: BLE001
                stats["errors"] += 1
                failed_ids.append(project_id)
                logger.exception("Exception while processing %s: %s", project_id, exc)

            if args.progress_every > 0 and stats["total"] % args.progress_every == 0:
                elapsed = max(0.001, time.time() - started_at)
                rate = stats["total"] / elapsed
                logger.info(
                    "Progress | total=%s updated=%s errors=%s missing=%s | %.2f/s",
                    stats["total"],
                    stats["updated"],
                    stats["errors"],
                    stats["missing"],
                    rate,
                )

            if args.throttle > 0:
                time.sleep(max(0.0, args.throttle))

    elapsed = time.time() - started_at
    logger.info(
        "Finished | total=%s updated=%s errors=%s missing=%s | runtime=%.1fs",
        stats["total"],
        stats["updated"],
        stats["errors"],
        stats["missing"],
        elapsed,
    )

    if failed_ids:
        unique_failed = sorted(set(failed_ids))
        logger.warning(
            "Failed project IDs (%s): %s%s",
            len(unique_failed),
            ", ".join(unique_failed[:50]),
            " ..." if len(unique_failed) > 50 else "",
        )

    if stats["errors"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()