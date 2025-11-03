# scripts/llm_analysis_all.py
import os
import sys
import time
import json
import csv
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, AsyncGenerator, Generator, Iterable, List, Optional, Tuple

import pandas as pd

# Make sure project modules are importable
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.database.supabase_client import SupabaseClient
from src.services.analysis_service import AnalysisService
from src.core.llm_analyzer import LLMAnalyzer
from src.core.models import ProjectFeatures, NumericPredictions, SegmentStatistics
from src.core.numeric_analyzer import NumericBaseline
from src.config import ANALYSIS_LOOKBACK_DAYS, GEMINI_API_KEY, GEMINI_MODEL

from scripts.test_llm_analysis import (
    build_segment_stats,
    fetch_segment_df,
    fetch_global_df,
    to_project_features,
    write_csv,
)

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger("llm_all")


def stream_projects(
    db: SupabaseClient,
    batch_size: int = 200,
    order_field: str = "monday_id",
    resume_after: Optional[str] = None,
    since: Optional[str] = None,
) -> Generator[List[Dict[str, Any]], None, None]:
    """
    Yield batches of projects ordered by `order_field`, using keyset pagination.

    Args:
        db: SupabaseClient instance.
        batch_size: Number of rows to fetch per batch.
        order_field: Column to order by (must be indexed / sortable).
        resume_after: Optional last-seen value to resume from.
        since: Optional ISO timestamp string to only include projects created on/after this date.

    Yields:
        Lists of project dicts.
    """
    cursor = resume_after
    total = 0

    while True:
        query = (
            db.client.table("projects")
            .select("*")
            .order(order_field, desc=False)
            .limit(batch_size)
        )
        if since:
            query = query.gte("date_created", since)
        if cursor:
            query = query.gt(order_field, cursor)

        resp = query.execute()
        rows = resp.data or []
        if not rows:
            break

        total += len(rows)
        cursor = rows[-1][order_field]
        yield rows

    logger.info(f"Streamed {total} total projects")


def process_projects(
    projects: Iterable[Dict[str, Any]],
    db: SupabaseClient,
    svc: AnalysisService,
    llm: LLMAnalyzer,
    nb: NumericBaseline,
    global_df: pd.DataFrame,
    lookback_days: Optional[int] = None,
    throttle: float = 0.35,
) -> List[Dict[str, Any]]:
    """
    Run numeric + LLM analysis for a batch of project records.

    Returns:
        List of result payloads ready for JSON/CSV export.
    """
    results: List[Dict[str, Any]] = []
    for project in projects:
        pid = project.get("monday_id")
        try:
            features = to_project_features(project)
            key = {
                "account": project.get("account") or None,
                "type": project.get("type") or None,
                "category": project.get("category") or None,
                "product_type": project.get("product_type") or None,
            }

            segment_df, segment_keys, backoff_tier = fetch_segment_df(
                db, key, lookback_days=lookback_days
            )

            predictions = nb.analyze_project(
                features,
                historical_data=segment_df,
                segment_data=segment_df,
                segment_keys=segment_keys,
                backoff_tier=backoff_tier,
                global_data=global_df,
                prior_strength=20,
            )
            seg_stats = predictions.segment_statistics

            llm_out, meta = llm.analyze_project(features, predictions, seg_stats)
            final = llm.create_final_analysis(features, predictions, llm_out, meta)

            result = {
                "project_id": pid,
                "item_name": project.get("item_name"),
                "project_name": features.name,
                "account": features.account,
                "type": features.type,
                "category": features.category,
                "product_type": features.product_type,
                "date_created": project.get("date_created"),
                "value": features.new_enquiry_value,
                "value_band": features.value_band,
                "baseline": {
                    "expected_gestation_days": predictions.expected_gestation_days,
                    "expected_conversion_rate": predictions.expected_conversion_rate,
                    "rating_score": predictions.rating_score,
                },
                "final_predictions": {
                    "expected_gestation_days": final.predictions.expected_gestation_days,
                    "expected_conversion_rate": final.predictions.expected_conversion_rate,
                    "rating_score": final.predictions.rating_score,
                },
                "reasoning": llm_out.reasoning,
                "adjustments": llm_out.adjustments,
                "confidence_notes": llm_out.confidence_notes,
                "llm_model": meta.get("llm_model", GEMINI_MODEL),
                "response_time_s": meta.get("response_time", 0.0),
                "tokens_used": meta.get("tokens_used", 0),
                "segment": {
                    "keys": seg_stats.segment_keys if seg_stats else [],
                    "sample_size": seg_stats.sample_size if seg_stats else 0,
                    "backoff_tier": seg_stats.backoff_tier if seg_stats else 5,
                    "win_rate": seg_stats.conversion_rate if seg_stats else None,
                    "win_rate_inclusive": getattr(seg_stats, "inclusive_conversion_rate", None),
                    "win_rate_closed": getattr(seg_stats, "closed_conversion_rate", None),
                    "conversion_confidence": seg_stats.conversion_confidence if seg_stats else 0.0,
                    "wins": seg_stats.wins if seg_stats else 0,
                    "losses": seg_stats.losses if seg_stats else 0,
                    "open": seg_stats.open if seg_stats else 0,
                    "gestation_median": seg_stats.gestation_median,
                    "gestation_p25": seg_stats.gestation_p25,
                    "gestation_p75": seg_stats.gestation_p75,
                },
            }
            results.append(result)

            if throttle:
                time.sleep(throttle)

        except Exception as exc:  # noqa: BLE001
            logger.warning(f"Failed to analyze project {pid}: {exc}")

    return results


def write_json(results: List[Dict[str, Any]], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as fh:
        json.dump(results, fh, indent=2, ensure_ascii=False)


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Run LLM analysis across all projects.")
    parser.add_argument("--batch-size", type=int, default=200, help="Supabase fetch chunk size.")
    parser.add_argument("--out-json", type=Path, help="Destination JSON file.")
    parser.add_argument("--out-csv", type=Path, help="Destination CSV file.")
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=None,
        help="Historical window for baselines (defaults to ANALYSIS_LOOKBACK_DAYS).",
    )
    parser.add_argument("--resume-after", help="Resume key (monday_id) to continue from.")
    parser.add_argument(
        "--throttle",
        type=float,
        default=0.35,
        help="Sleep seconds between LLM calls to respect rate limits.",
    )
    parser.add_argument(
        "--since",
        type=str,
        help="Only analyze projects with date_created on/after YYYY-MM-DD",
    )

    args = parser.parse_args()

    if not GEMINI_API_KEY:
        logger.error("GEMINI_API_KEY not set. Export it before running.")
        sys.exit(1)

    lookback = args.lookback_days or ANALYSIS_LOOKBACK_DAYS
    logger.info(f"Using {lookback} day lookback ({lookback/365:.1f} years)")

    db = SupabaseClient()
    svc = AnalysisService()
    llm = LLMAnalyzer()
    nb = NumericBaseline(lookback_days=lookback)
    global_df = fetch_global_df(db, lookback)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = args.out_json or (PROJECT_ROOT / "outputs" / "analysis" / f"llm_all_{timestamp}.json")
    csv_path = args.out_csv or json_path.with_suffix(".csv")

    since = None
    if args.since:
        try:
            since = datetime.strptime(args.since, "%Y-%m-%d").date().isoformat()
        except ValueError:
            parser.error("--since must be YYYY-MM-DD")

    all_results: List[Dict[str, Any]] = []
    processed = 0
    failed = 0
    t0 = time.time()

    for batch_idx, rows in enumerate(
        stream_projects(
            db,
            batch_size=args.batch_size,
            resume_after=args.resume_after,
            since=since,
        ),
        start=1,
    ):
        logger.info(f"Batch {batch_idx}: {len(rows)} projects")
        results = process_projects(
            rows,
            db=db,
            svc=svc,
            llm=llm,
            nb=nb,
            global_df=global_df,
            lookback_days=lookback,
            throttle=args.throttle,
        )
        processed += len(results)
        failed += len(rows) - len(results)
        all_results.extend(results)
        logger.info(f"Progress: processed={processed} failed={failed}")

    elapsed = time.time() - t0
    logger.info(f"Done in {elapsed:.1f}s; processed={processed} failed={failed}")

    if all_results:
        write_json(all_results, json_path)
        write_csv(all_results, csv_path)
        try:
            avg_rating = sum(r["final_predictions"]["rating_score"] for r in all_results) / len(all_results)
            logger.info(f"Wrote {len(all_results)} rows to {json_path} / {csv_path} | avg_rating={avg_rating:.2f}")
        except Exception:  # noqa: BLE001
            logger.info(f"Wrote {len(all_results)} rows to {json_path} / {csv_path}")
    else:
        logger.warning("No successful analyses; nothing written.")


if __name__ == "__main__":
    main()