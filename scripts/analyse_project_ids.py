# scripts/analyse_project_ids.py
import argparse
import json
import logging
import sys
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import ANALYSIS_LOOKBACK_DAYS, GEMINI_API_KEY, GEMINI_MODEL
from src.core.llm_analyzer import LLMAnalyzer
from src.core.models import NumericPredictions, ProjectFeatures, SegmentStatistics
from src.core.numeric_analyzer import NumericBaseline
from src.database.supabase_client import SupabaseClient
from src.services.analysis_service import AnalysisService
from tests.test_llm_analysis import (
    build_segment_stats,
    fetch_global_df,
    fetch_segment_df,
    to_project_features,
    write_csv,
)

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("llm_subset")


def chunked(values: List[str], size: int = 50) -> Iterable[List[str]]:
    for idx in range(0, len(values), size):
        yield values[idx : idx + size]


def load_project_ids(cli_ids: Optional[List[str]], file_path: Optional[Path]) -> List[str]:
    ids: List[str] = []
    if cli_ids:
        ids.extend(cli_ids)

    if file_path:
        text = file_path.read_text(encoding="utf-8").splitlines()
        ids.extend(line.strip() for line in text if line.strip())

    unique_ids = sorted(set(ids))
    if not unique_ids:
        raise ValueError("No project IDs provided. Use --project-id or --ids-file.")
    return unique_ids


def fetch_projects_by_ids(db: SupabaseClient, project_ids: List[str]) -> Dict[str, Dict]:
    found: Dict[str, Dict] = {}
    missing: List[str] = []

    for batch in chunked(project_ids, size=50):
        query = (
            db.client.table("projects")
            .select("*")
            .in_("monday_id", batch)
            .execute()
        )
        rows = query.data or []
        for row in rows:
            pid = row.get("monday_id")
            if pid:
                found[str(pid)] = row

    for pid in project_ids:
        if pid not in found:
            missing.append(pid)

    if missing:
        logger.warning("Missing %s project(s) in Supabase: %s", len(missing), ", ".join(missing))
    return found


def build_numeric_predictions(
    nb: NumericBaseline,
    features: ProjectFeatures,
    segment_df: pd.DataFrame,
    segment_keys: List[str],
    backoff_tier: int,
    global_df: pd.DataFrame,
) -> NumericPredictions:
    return nb.analyze_project(
        features,
        historical_data=segment_df,
        segment_data=segment_df,
        segment_keys=segment_keys,
        backoff_tier=backoff_tier,
        global_data=global_df,
        prior_strength=20,
    )


def process_selected_projects(
    project_ids: List[str],
    db: SupabaseClient,
    svc: AnalysisService,
    llm: LLMAnalyzer,
    nb: NumericBaseline,
    global_df: pd.DataFrame,
    lookback_days: int,
    throttle: float,
) -> Tuple[List[Dict], List[str], List[str]]:
    records = fetch_projects_by_ids(db, project_ids)
    processed: List[Dict] = []
    failed: List[str] = []
    missing = [pid for pid in project_ids if pid not in records]

    for idx, pid in enumerate(project_ids, start=1):
        project = records.get(pid)
        if not project:
            continue

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
            predictions = build_numeric_predictions(
                nb, features, segment_df, segment_keys, backoff_tier, global_df
            )
            seg_stats: Optional[SegmentStatistics] = predictions.segment_statistics

            llm_out, meta = llm.analyze_project(features, predictions, seg_stats)
            final = llm.create_final_analysis(features, predictions, llm_out, meta)

            record = {
                "project_id": pid,
                "project_name": features.name,
                "item_name": project.get("item_name"),
                "account": features.account,
                "type": features.type,
                "category": features.category,
                "product_type": features.product_type,
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
                    "wins": seg_stats.wins if seg_stats else 0,
                    "losses": seg_stats.losses if seg_stats else 0,
                    "open": seg_stats.open if seg_stats else 0,
                    "conversion_confidence": seg_stats.conversion_confidence if seg_stats else 0.0,
                    "gestation_median": seg_stats.gestation_median if seg_stats else None,
                    "gestation_p25": seg_stats.gestation_p25 if seg_stats else None,
                    "gestation_p75": seg_stats.gestation_p75 if seg_stats else None,
                },
            }
            processed.append(record)
            logger.info("Processed %s/%s (%s)", idx, len(project_ids), pid)

            if throttle:
                time.sleep(throttle)

        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to analyze %s: %s", pid, exc)
            failed.append(pid)

    return processed, failed, missing


def write_json(results: List[Dict], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run LLM+numeric analysis for a specific set of projects."
    )
    parser.add_argument(
        "--project-id",
        dest="project_ids",
        action="append",
        help="Project monday_id to analyze. Provide multiple times for multiple IDs.",
    )
    parser.add_argument(
        "--ids-file",
        type=Path,
        help="Text file containing monday_id values (one per line) to analyze.",
    )
    parser.add_argument("--out-json", type=Path, help="Destination JSON file.")
    parser.add_argument("--out-csv", type=Path, help="Destination CSV file.")
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=None,
        help="Historical window for baselines (defaults to ANALYSIS_LOOKBACK_DAYS).",
    )
    parser.add_argument(
        "--throttle",
        type=float,
        default=0.35,
        help="Sleep seconds between LLM calls (default: 0.35).",
    )

    args = parser.parse_args()

    try:
        project_ids = load_project_ids(args.project_ids, args.ids_file)
    except ValueError as err:
        parser.error(str(err))
        return

    if not GEMINI_API_KEY:
        logger.error("GEMINI_API_KEY not set. Export it before running.")
        sys.exit(1)

    lookback = args.lookback_days or ANALYSIS_LOOKBACK_DAYS
    logger.info("Using %s day lookback (%.1f years)", lookback, lookback / 365)

    db = SupabaseClient()
    svc = AnalysisService()
    llm = LLMAnalyzer()
    nb = NumericBaseline(lookback_days=lookback)
    global_df = fetch_global_df(db, lookback)

    timestamp = time.strftime("%Y%m%d_%H%M%S")
    default_json = PROJECT_ROOT / "outputs" / "analysis" / f"llm_subset_{timestamp}.json"
    json_path = args.out_json or default_json
    csv_path = args.out_csv or json_path.with_suffix(".csv")

    t0 = time.time()
    results, failed, missing = process_selected_projects(
        project_ids,
        db=db,
        svc=svc,
        llm=llm,
        nb=nb,
        global_df=global_df,
        lookback_days=lookback,
        throttle=args.throttle,
    )
    elapsed = time.time() - t0

    logger.info("Finished in %.1fs; success=%s failed=%s missing=%s", elapsed, len(results), len(failed), len(missing))

    if missing:
        logger.warning("Missing project IDs: %s", ", ".join(missing))
    if failed:
        logger.warning("Failed project IDs: %s", ", ".join(failed))

    if results:
        write_json(results, json_path)
        write_csv(results, csv_path)
        logger.info("Wrote %s records to %s and %s", len(results), json_path, csv_path)
    else:
        logger.warning("No successful analyses; nothing written.")


if __name__ == "__main__":
    main()