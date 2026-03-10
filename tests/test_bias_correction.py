"""
Test bias-corrected numeric baseline on closed projects (no LLM).

Runs AnalysisService.analyze_and_store(with_llm=False) for Won/Lost projects
since a configurable cutoff, then prints before/after error metrics so you can
verify the bias correction is working without waiting for an LLM backfill.

Usage:
    python tests/test_bias_correction.py --dry-run --cutoff 2023-01-01
    python tests/test_bias_correction.py                 # default: all eligible
    python tests/test_bias_correction.py --limit 200     # quick spot-check
    python tests/test_bias_correction.py --dry-run       # compute but don't persist
    python tests/test_bias_correction.py --cutoff 2024-01-01
"""

import argparse
import logging
import math
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.database.supabase_client import SupabaseClient
from src.services.analysis_service import AnalysisService

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("test_bias_correction")


PAGE_SIZE = 500


def fetch_eligible_projects(
    db: SupabaseClient,
    cutoff: str,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Fetch closed projects with a known gestation_period."""
    all_rows: List[Dict[str, Any]] = []
    offset = 0

    while True:
        q = (
            db.client.table("projects")
            .select("monday_id, type, category, gestation_period, status_category")
            .gte("date_created", cutoff)
            .in_("status_category", ["Won", "Lost"])
            .gt("gestation_period", 0)
            .lte("gestation_period", 1460)
            .order("monday_id")
            .range(offset, offset + PAGE_SIZE - 1)
        )
        rows = q.execute().data or []
        if not rows:
            break
        all_rows.extend(rows)
        if limit and len(all_rows) >= limit:
            all_rows = all_rows[:limit]
            break
        if len(rows) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    return all_rows


def fetch_current_predictions(
    db: SupabaseClient,
    project_ids: List[str],
) -> Dict[str, Optional[int]]:
    """Fetch existing expected_gestation_days from analysis_results for comparison."""
    lookup: Dict[str, Optional[int]] = {}
    for start in range(0, len(project_ids), PAGE_SIZE):
        batch = project_ids[start : start + PAGE_SIZE]
        rows = (
            db.client.table("analysis_results")
            .select("project_id, expected_gestation_days")
            .in_("project_id", batch)
            .execute()
            .data
            or []
        )
        for r in rows:
            pid = r.get("project_id")
            val = r.get("expected_gestation_days")
            try:
                lookup[pid] = int(val) if val is not None else None
            except (TypeError, ValueError):
                lookup[pid] = None
    return lookup


def compute_metrics(errors: List[float]) -> Dict[str, float]:
    if not errors:
        return {
            "n": 0,
            "mae": 0.0,
            "mean_bias": 0.0,
            "median_ae": 0.0,
            "p90_ae": 0.0,
            "under_rate": 0.0,
            "over_rate": 0.0,
            "severe_under_180": 0.0,
            "severe_over_180": 0.0,
        }

    import statistics

    abs_errors = [abs(e) for e in errors]
    abs_sorted = sorted(abs_errors)

    def percentile(sorted_values: List[float], p: float) -> float:
        if not sorted_values:
            return 0.0
        if len(sorted_values) == 1:
            return float(sorted_values[0])

        rank = (len(sorted_values) - 1) * p
        lower = math.floor(rank)
        upper = math.ceil(rank)
        if lower == upper:
            return float(sorted_values[lower])

        weight = rank - lower
        return float(
            sorted_values[lower] * (1.0 - weight) + sorted_values[upper] * weight
        )

    under = sum(1 for e in errors if e > 0)
    over = sum(1 for e in errors if e < 0)

    return {
        "n": len(errors),
        "mae": sum(abs_errors) / len(abs_errors),
        "mean_bias": sum(errors) / len(errors),
        "median_ae": statistics.median(abs_errors),
        "p90_ae": percentile(abs_sorted, 0.90),
        "under_rate": under / len(errors),
        "over_rate": over / len(errors),
        "severe_under_180": sum(1 for e in errors if e > 180) / len(errors),
        "severe_over_180": sum(1 for e in errors if e < -180) / len(errors),
    }


def main():
    parser = argparse.ArgumentParser(description="Test bias-corrected analysis (numeric-only, no LLM)")
    parser.add_argument("--cutoff", default="2023-01-01", help="Earliest date_created (YYYY-MM-DD)")
    parser.add_argument("--limit", type=int, default=None, help="Max projects to process")
    parser.add_argument("--dry-run", action="store_true", help="Compute predictions but do not persist to analysis_results")
    parser.add_argument("--throttle", type=float, default=0.0, help="Seconds to sleep between projects")
    args = parser.parse_args()

    db = SupabaseClient()
    svc = AnalysisService(db_client=db)

    logger.info("Fetching eligible closed projects (cutoff=%s, limit=%s) ...", args.cutoff, args.limit)
    projects = fetch_eligible_projects(db, args.cutoff, args.limit)
    if not projects:
        logger.warning("No eligible projects found.")
        return

    logger.info("Found %d eligible projects", len(projects))

    project_ids = [p["monday_id"] for p in projects]
    logger.info("Loading current analysis_results predictions for comparison ...")
    old_predictions = fetch_current_predictions(db, project_ids)
    logger.info("Loaded %d existing predictions", len(old_predictions))

    ok = 0
    errs = 0
    failed_ids: List[str] = []
    paired_old_errors: List[float] = []
    paired_new_errors: List[float] = []
    all_new_errors: List[float] = []
    paired_improved = 0
    paired_worsened = 0
    paired_unchanged = 0
    comparison_rows: List[Dict[str, Any]] = []
    start_time = time.time()

    for i, proj in enumerate(projects, 1):
        monday_id = proj["monday_id"]
        actual = proj["gestation_period"]
        proj_type = proj.get("type", "?")
        proj_cat = proj.get("category", "?")

        try:
            if args.dry_run:
                full_proj = (
                    db.client.table("projects")
                    .select("*")
                    .eq("monday_id", monday_id)
                    .single()
                    .execute()
                    .data
                )
                if not full_proj:
                    errs += 1
                    failed_ids.append(monday_id)
                    continue
                result_data = svc.analyze_project(full_proj)
                new_expected = result_data.get("expected_gestation_days")
                success = new_expected is not None
            else:
                result = svc.analyze_and_store(monday_id, with_llm=False)
                success = result.get("success", False)
                new_expected = result.get("result", {}).get("expected_gestation_days") if success else None

            if success and new_expected is not None:
                ok += 1
                old_expected = old_predictions.get(monday_id)
                new_err = actual - new_expected
                all_new_errors.append(new_err)
                old_err = None

                if old_expected is not None:
                    old_err = actual - old_expected
                    paired_old_errors.append(old_err)
                    paired_new_errors.append(new_err)
                    old_abs = abs(old_err)
                    new_abs = abs(new_err)
                    if new_abs < old_abs:
                        paired_improved += 1
                    elif new_abs > old_abs:
                        paired_worsened += 1
                    else:
                        paired_unchanged += 1

                comparison_rows.append({
                    "monday_id": monday_id,
                    "type": proj_type,
                    "category": proj_cat,
                    "actual": actual,
                    "old_expected": old_expected,
                    "new_expected": new_expected,
                    "old_err": old_err,
                    "new_err": new_err,
                })
            else:
                errs += 1
                failed_ids.append(monday_id)

        except Exception as exc:
            errs += 1
            failed_ids.append(monday_id)
            logger.debug("Failed %s: %s", monday_id, exc)

        if i % 100 == 0 or i == len(projects):
            elapsed = max(0.001, time.time() - start_time)
            rate = i / elapsed
            remaining = len(projects) - i
            eta = int(remaining / rate) if rate > 0 else 0
            logger.info(
                "Progress %d/%d (%.1f%%) | ok=%d err=%d | %.1f/s | ETA %dm%02ds",
                i, len(projects), i / len(projects) * 100,
                ok, errs, rate, eta // 60, eta % 60,
            )

        if args.throttle > 0:
            time.sleep(args.throttle)

    paired_old_metrics = compute_metrics(paired_old_errors)
    paired_new_metrics = compute_metrics(paired_new_errors)
    all_new_metrics = compute_metrics(all_new_errors)

    print("\n" + "=" * 78)
    print("BIAS CORRECTION TEST RESULTS")
    print("=" * 78)
    print(f"  Mode:              {'DRY RUN (not persisted)' if args.dry_run else 'LIVE (persisted to analysis_results)'}")
    print(f"  Projects processed: {ok} ok, {errs} errors, {len(projects)} total")
    print(f"  Cutoff:            {args.cutoff}")
    print()
    print("  PAIRED COMPARISON (only rows with both old and new predictions)")
    print(f"  {'Metric':<24} {'BEFORE (old)':>16} {'AFTER (new)':>16} {'Change':>14}")
    print(f"  {'-'*24} {'-'*16} {'-'*16} {'-'*14}")

    paired_metric_rows = [
        ("MAE (days)", "mae", False),
        ("Mean Bias (days)", "mean_bias", False),
        ("Median AE (days)", "median_ae", False),
        ("P90 AE (days)", "p90_ae", False),
        ("Underpredict %", "under_rate", True),
        ("Overpredict %", "over_rate", True),
        ("Severe under >180d %", "severe_under_180", True),
        ("Severe over >180d %", "severe_over_180", True),
    ]

    for label, key, is_pct in paired_metric_rows:
        old_val = paired_old_metrics.get(key, 0.0)
        new_val = paired_new_metrics.get(key, 0.0)
        delta = new_val - old_val
        if is_pct:
            print(
                f"  {label:<24} "
                f"{old_val * 100:>15.1f}% "
                f"{new_val * 100:>15.1f}% "
                f"{delta * 100:>+13.1f}pp"
            )
        else:
            print(
                f"  {label:<24} "
                f"{old_val:>16.1f} "
                f"{new_val:>16.1f} "
                f"{delta:>+14.1f}"
            )

    print(f"  {'Paired sample size':<24} {paired_old_metrics['n']:>16} {paired_new_metrics['n']:>16}")
    print()

    if paired_new_metrics["n"] > 0:
        paired_n = paired_new_metrics["n"]
        print(f"  Paired rows improved:  {paired_improved}/{paired_n} ({paired_improved/paired_n*100:.1f}%)")
        print(f"  Paired rows worsened:  {paired_worsened}/{paired_n} ({paired_worsened/paired_n*100:.1f}%)")
        print(f"  Paired rows unchanged: {paired_unchanged}/{paired_n} ({paired_unchanged/paired_n*100:.1f}%)")
        print()

    print("  ALL NEW PREDICTIONS (coverage snapshot)")
    print(f"  Sample size:               {all_new_metrics['n']}")
    print(f"  MAE (days):                {all_new_metrics['mae']:.1f}")
    print(f"  Mean Bias (days):          {all_new_metrics['mean_bias']:.1f}")
    print(f"  Median AE (days):          {all_new_metrics['median_ae']:.1f}")
    print(f"  P90 AE (days):             {all_new_metrics['p90_ae']:.1f}")
    print(f"  Underpredict rate:         {all_new_metrics['under_rate']*100:.1f}%")
    print(f"  Overpredict rate:          {all_new_metrics['over_rate']*100:.1f}%")
    print(f"  Severe under >180d rate:   {all_new_metrics['severe_under_180']*100:.1f}%")
    print(f"  Severe over >180d rate:    {all_new_metrics['severe_over_180']*100:.1f}%")
    print()

    if all_new_metrics["n"] > 0:
        within_30 = sum(1 for e in all_new_errors if abs(e) <= 30)
        within_90 = sum(1 for e in all_new_errors if abs(e) <= 90)
        n = all_new_metrics["n"]
        print(f"  New predictions within 30 days: {within_30}/{n} ({within_30/n*100:.1f}%)")
        print(f"  New predictions within 90 days: {within_90}/{n} ({within_90/n*100:.1f}%)")

    print()
    print("  TOP 10 LARGEST REMAINING ERRORS (after correction):")
    print(f"  {'monday_id':<14} {'type':<16} {'category':<14} {'actual':>7} {'old':>7} {'new':>7} {'new_err':>8}")
    print(f"  {'-'*14} {'-'*16} {'-'*14} {'-'*7} {'-'*7} {'-'*7} {'-'*8}")
    sorted_rows = sorted(comparison_rows, key=lambda r: abs(r["new_err"]), reverse=True)
    for row in sorted_rows[:10]:
        old_str = str(row["old_expected"]) if row["old_expected"] is not None else "n/a"
        print(
            f"  {row['monday_id']:<14} {row['type']:<16} {row['category']:<14} "
            f"{row['actual']:>7} {old_str:>7} {row['new_expected']:>7} {row['new_err']:>+8}"
        )

    print("=" * 78)

    if failed_ids:
        print(f"\nFailed IDs ({len(failed_ids)}): {', '.join(failed_ids[:30])}" + (" ..." if len(failed_ids) > 30 else ""))


if __name__ == "__main__":
    main()