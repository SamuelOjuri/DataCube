#!/usr/bin/env python
"""
Backfill Supabase analysis_results with LLM reasoning.

This temporarily replaces the webhook-driven flow: it scans projects created on/after
a cutoff date, runs the AnalysisService with Gemini reasoning, and upserts the
results (including the structured reasoning payload) into analysis_results.

Example:
    python scripts/backfill_analysis_with_llm.py --cutoff 2024-01-01 --batch-size 100 --throttle 0.35

Environment prerequisites:
    * SUPABASE_URL / SUPABASE_SERVICE_KEY
    * GEMINI_API_KEY (or GOOGLE_API_KEY) – required for LLM reasoning
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime, date
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Sequence

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import ANALYSIS_LOOKBACK_DAYS, GEMINI_API_KEY
from src.database.supabase_client import SupabaseClient
from src.services.analysis_service import AnalysisService

logger = logging.getLogger("backfill_analysis_with_llm")


def parse_cutoff(value: str) -> str:
    """Validate cutoff string (YYYY-MM-DD) and return ISO date string."""
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"cutoff must be YYYY-MM-DD (got {value!r})") from exc
    return parsed.isoformat()


def iter_projects(
    db: SupabaseClient,
    *,
    cutoff: str,
    batch_size: int,
    resume_after: Optional[str] = None,
    limit: Optional[int] = None,
) -> Iterator[List[Dict[str, object]]]:
    """
    Yield batches of projects ordered by monday_id, filtered by date_created >= cutoff.
    Uses keyset pagination so we can resume safely between runs.
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

        response = query.execute()
        rows: List[Dict[str, object]] = response.data or []
        if not rows:
            break

        if limit is not None and processed + len(rows) > limit:
            rows = rows[: max(0, limit - processed)]

        if not rows:
            break

        yield rows
        processed += len(rows)
        cursor = str(rows[-1]["monday_id"])

        if limit is not None and processed >= limit:
            break


def has_llm_result(
    db: SupabaseClient,
    project_id: str,
    *,
    min_timestamp: Optional[datetime],
) -> bool:
    """
    Return True if analysis_results already has an LLM-enriched row for this project
    (reasoning with gestation/conversion/rating) that is newer than min_timestamp.
    """
    row = db.get_latest_analysis_result(project_id)
    if not row:
        return False

    reasoning = row.get("reasoning")
    if not isinstance(reasoning, dict):
        return False
    if not {"gestation", "conversion", "rating"} <= set(reasoning):
        return False

    if min_timestamp and row.get("analysis_timestamp"):
        try:
            existing_ts = datetime.fromisoformat(row["analysis_timestamp"])
            if existing_ts >= min_timestamp:
                return True
        except ValueError:
            return False
        return False

    # If no timestamp filter, presence of structured reasoning is enough
    return True


def process_projects(
    svc: AnalysisService,
    db: SupabaseClient,
    rows: Sequence[Dict[str, object]],
    *,
    throttle: float,
    skip_existing_after: Optional[datetime],
    force: bool,
    stats: Dict[str, int],
) -> None:
    """Run LLM analysis for each project in the batch, updating stats dict in-place."""
    for row in rows:
        project_id = str(row.get("monday_id") or "").strip()
        if not project_id:
            stats["missing"] += 1
            logger.warning("Skipping row with missing monday_id: %s", row)
            continue

        stats["total"] += 1

        if not force and has_llm_result(db, project_id, min_timestamp=skip_existing_after):
            stats["skipped"] += 1
            logger.info("Skipping %s: existing LLM analysis detected", project_id)
            continue

        try:
            result = svc.analyze_and_store(project_id, with_llm=True)
        except Exception as exc:  # noqa: BLE001
            stats["errors"] += 1
            logger.exception("LLM analysis failed for %s: %s", project_id, exc)
            continue

        if result.get("success"):
            stats["updated"] += 1
            logger.info(
                "Stored analysis for %s | rating=%s | conversion=%.3f | gestation=%s",
                project_id,
                result["result"].get("rating_score"),
                result["result"].get("expected_conversion_rate") or 0.0,
                result["result"].get("expected_gestation_days"),
            )
        else:
            stats["errors"] += 1
            logger.warning(
                "Analysis service returned error for %s: %s",
                project_id,
                result.get("error"),
            )

        if throttle > 0:
            time.sleep(throttle)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill analysis_results with Gemini reasoning for recent projects.",
    )
    parser.add_argument(
        "--cutoff",
        type=parse_cutoff,
        required=True,
        help="Only analyze projects with date_created on/after this date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Supabase fetch size (default: 100).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Stop after processing this many projects (for smoke tests).",
    )
    parser.add_argument(
        "--resume-after",
        default=None,
        help="Resume from the last processed monday_id (useful if a previous run was interrupted).",
    )
    parser.add_argument(
        "--throttle",
        type=float,
        default=0.35,
        help="Seconds to sleep between LLM calls (default: 0.35).",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=None,
        help="Override AnalysisService lookback window (defaults to ANALYSIS_LOOKBACK_DAYS).",
    )
    parser.add_argument(
        "--skip-existing-after",
        type=parse_cutoff,
        default=None,
        help="If set, skip projects that already have LLM reasoning with analysis_timestamp on/after this date.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-run analysis even if an LLM result already exists for the project.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO).",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="[%(asctime)s] %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    if not GEMINI_API_KEY:
        logger.error("GEMINI_API_KEY is not set; aborting.")
        sys.exit(1)

    try:
        db = SupabaseClient()
    except ValueError as exc:
        logger.error("Supabase client initialisation failed: %s", exc)
        sys.exit(1)

    lookback = args.lookback_days or ANALYSIS_LOOKBACK_DAYS
    svc = AnalysisService(db_client=db, lookback_days=lookback)

    stats = {"total": 0, "updated": 0, "skipped": 0, "missing": 0, "errors": 0}
    cutoff_iso = args.cutoff
    skip_after_dt = (
        datetime.strptime(args.skip_existing_after, "%Y-%m-%d").replace(tzinfo=None)
        if args.skip_existing_after
        else None
    )

    logger.info(
        "Starting LLM backfill | cutoff=%s | batch_size=%s | throttle=%.2fs | limit=%s | force=%s",
        cutoff_iso,
        args.batch_size,
        args.throttle,
        args.limit,
        args.force,
    )

    started_at = time.time()
    try:
        for batch_index, batch in enumerate(
            iter_projects(
                db,
                cutoff=cutoff_iso,
                batch_size=max(1, args.batch_size),
                resume_after=args.resume_after,
                limit=args.limit,
            ),
            start=1,
        ):
            logger.info("Processing batch %s (%s projects)", batch_index, len(batch))
            process_projects(
                svc,
                db,
                batch,
                throttle=max(0.0, args.throttle),
                skip_existing_after=skip_after_dt,
                force=args.force,
                stats=stats,
            )
    except KeyboardInterrupt:
        logger.warning("Interrupted by user; summarising progress…")

    elapsed = time.time() - started_at
    logger.info(
        "Finished | total=%s | updated=%s | skipped=%s | missing=%s | errors=%s | runtime=%.1fs",
        stats["total"],
        stats["updated"],
        stats["skipped"],
        stats["missing"],
        stats["errors"],
        elapsed,
    )

    if stats["errors"]:
        sys.exit(1)


if __name__ == "__main__":
    main()