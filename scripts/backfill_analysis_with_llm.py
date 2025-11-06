#!/usr/bin/env python
"""Backfill Supabase analysis_results with LLM reasoning."""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import GEMINI_API_KEY  # noqa: E402
from src.tasks.pipeline import backfill_llm  # noqa: E402

logger = logging.getLogger("backfill_analysis_with_llm")


def _parse_cutoff(value: str) -> str:
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:  # noqa: B905
        raise argparse.ArgumentTypeError(f"cutoff must be YYYY-MM-DD (got {value!r})") from exc
    return value


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill analysis_results with Gemini reasoning for recent projects.",
    )
    parser.add_argument(
        "--cutoff",
        type=_parse_cutoff,
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
        type=_parse_cutoff,
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
    return parser.parse_args()


def _to_optional_date(value: Optional[str]) -> Optional[datetime]:
    if value is None:
        return None
    return datetime.strptime(value, "%Y-%m-%d")


def main() -> None:
    args = _parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="[%(asctime)s] %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    if not GEMINI_API_KEY:
        logger.error("GEMINI_API_KEY is not set; aborting.")
        sys.exit(1)

    stats = backfill_llm(
        cutoff=args.cutoff,
        batch_size=args.batch_size,
        limit=args.limit,
        resume_after=args.resume_after,
        throttle=args.throttle,
        lookback_days=args.lookback_days,
        skip_existing_after=_to_optional_date(args.skip_existing_after),
        force=args.force,
        logger=logging.getLogger("backfill_analysis_with_llm"),
    )

    if stats.get("errors"):
        sys.exit(1)


if __name__ == "__main__":
    main()