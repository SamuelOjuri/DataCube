"""
Test numeric gestation bias correction on closed projects (no LLM adjustments).

For Won/Lost projects since a configurable cutoff, this script computes:
- the raw pre-correction numeric gestation prediction
- the bias-corrected numeric gestation prediction
- the currently stored prediction in analysis_results for before/after comparison

It then prints:
- stored-vs-current paired metrics
- raw-vs-corrected metrics for the current run
- raw pre-correction bias tables suitable as calibration inputs
- residual post-correction bias tables for monitoring only

In live mode, the corrected numeric result is persisted to analysis_results.
In dry-run mode, predictions are computed but not stored.

Usage:
    python tests/test_bias_correction.py --dry-run --cutoff 2023-01-01
    python tests/test_bias_correction.py
    python tests/test_bias_correction.py --limit 200
    python tests/test_bias_correction.py --dry-run
    python tests/test_bias_correction.py --cutoff 2024-01-01
"""

import argparse
import logging
import math
import sys
import time
from contextlib import contextmanager
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional
from collections import defaultdict

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import src.core.numeric_analyzer as numeric_analyzer_module
from src.core.normalization import compute_product_key, normalize_category_value
from src.database.supabase_client import SupabaseClient
from src.services.analysis_service import AnalysisService

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("test_bias_correction")


PAGE_SIZE = 500


@contextmanager
def temporary_gestation_bias_correction(enabled: bool):
    """Temporarily toggle gestation bias correction for this single-threaded diagnostic."""
    previous = numeric_analyzer_module.GESTATION_BIAS_CORRECTION_ENABLED
    numeric_analyzer_module.GESTATION_BIAS_CORRECTION_ENABLED = enabled
    try:
        yield
    finally:
        numeric_analyzer_module.GESTATION_BIAS_CORRECTION_ENABLED = previous

def analyze_raw_and_corrected(
    svc: AnalysisService,
    project: Dict[str, Any],
) -> Dict[str, Dict[str, Any]]:
    """Return both pre-correction and post-correction numeric outputs."""
    with temporary_gestation_bias_correction(False):
        raw_result = svc.analyze_project(project)

    with temporary_gestation_bias_correction(True):
        corrected_result = svc.analyze_project(project)

    return {
        "raw": raw_result,
        "corrected": corrected_result,
    }


def normalized_project_category(project: Dict[str, Any]) -> str:
    return normalize_category_value(project.get("category")) or project.get("category") or "?"


def normalized_project_product_key(project: Dict[str, Any]) -> Optional[str]:
    for raw_value in (project.get("product_key"), project.get("product_type")):
        normalized = compute_product_key(raw_value)
        if normalized and normalized != "unknown":
            return normalized
    return None


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
            .select("monday_id, type, category, product_key, gestation_period, status_category, value_band")
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


def _build_bias_tables_from_key(rows, key_builder, error_key="new_err", min_n=8):
    buckets = defaultdict(list)
    all_err = []

    for r in rows:
        err = r.get(error_key)
        if err is None:
            continue

        key = key_builder(r)
        if key is None:
            continue

        err_value = float(err)
        buckets[key].append(err_value)
        all_err.append(err_value)

    seg_bias = {}
    seg_n = {}

    for seg, errs in buckets.items():
        n = len(errs)
        if n < min_n:
            continue
        seg_n[seg] = n
        seg_bias[seg] = int(round(sum(errs) / n))

    global_bias = int(round(sum(all_err) / len(all_err))) if all_err else 0
    global_fallback = int(round(global_bias * 0.85))

    return seg_bias, seg_n, global_bias, global_fallback


def build_bias_tables(rows, error_key="new_err", min_n=8):
    return _build_bias_tables_from_key(
        rows,
        key_builder=lambda row: (row.get("type") or "?", row.get("category") or "?"),
        error_key=error_key,
        min_n=min_n,
    )


def build_value_band_bias_tables(rows, error_key="new_err", min_n=15):
    """Compute bias tables keyed on (type, category, value_band)."""
    seg_bias, seg_n, _, _ = _build_bias_tables_from_key(
        rows,
        key_builder=lambda row: (
            (row.get("type") or "?"),
            (row.get("category") or "?"),
            row.get("value_band"),
        ) if row.get("value_band") else None,
        error_key=error_key,
        min_n=min_n,
    )
    return seg_bias, seg_n


def build_product_key_bias_tables(rows, error_key="new_err", min_n=10):
    """Compute bias tables keyed on (type, category, product_key)."""
    seg_bias, seg_n, _, _ = _build_bias_tables_from_key(
        rows,
        key_builder=lambda row: (
            (row.get("type") or "?"),
            (row.get("category") or "?"),
            row.get("product_key"),
        ) if row.get("product_key") else None,
        error_key=error_key,
        min_n=min_n,
    )
    return seg_bias, seg_n


def build_product_key_value_band_bias_tables(rows, error_key="new_err", min_n=15):
    """Compute bias tables keyed on (type, category, product_key, value_band)."""
    seg_bias, seg_n, _, _ = _build_bias_tables_from_key(
        rows,
        key_builder=lambda row: (
            (row.get("type") or "?"),
            (row.get("category") or "?"),
            row.get("product_key"),
            row.get("value_band"),
        ) if row.get("product_key") and row.get("value_band") else None,
        error_key=error_key,
        min_n=min_n,
    )
    return seg_bias, seg_n




def main():
    parser = argparse.ArgumentParser(description="Test bias-corrected analysis (numeric-only, no LLM)")
    parser.add_argument("--cutoff", default="2023-01-01", help="Earliest date_created (YYYY-MM-DD)")
    parser.add_argument("--limit", type=int, default=None, help="Max projects to process")
    parser.add_argument("--product-key-min-n", type=int, default=10, help="Minimum rows for (type, category, product_key) calibration cells")
    parser.add_argument("--product-key-value-band-min-n", type=int, default=15, help="Minimum rows for (type, category, product_key, value_band) calibration cells")
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
    all_raw_errors: List[float] = []
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

            proj_type = full_proj.get("type") or proj_type
            proj_cat = normalized_project_category(full_proj)
            proj_product_key = normalized_project_product_key(full_proj)

            run_pair = analyze_raw_and_corrected(svc, full_proj)
            raw_result = run_pair["raw"]
            corrected_result = run_pair["corrected"]

            raw_expected = raw_result.get("expected_gestation_days")
            new_expected = corrected_result.get("expected_gestation_days")
            success = new_expected is not None

            if success and not args.dry_run:
                db.store_analysis_result(monday_id, corrected_result)

            if success and new_expected is not None:
                ok += 1
                old_expected = old_predictions.get(monday_id)
                raw_err = actual - raw_expected if raw_expected is not None else None
                new_err = actual - new_expected
                if raw_err is not None:
                    all_raw_errors.append(raw_err)
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
                    "product_key": proj_product_key,
                    "value_band": proj.get("value_band"),
                    "actual": actual,
                    "raw_expected": raw_expected,
                    "raw_err": raw_err,
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
    raw_metrics = compute_metrics(all_raw_errors)
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

    print("  CURRENT RUN: RAW BASELINE VS BIAS-CORRECTED")
    print(f"  {'Metric':<24} {'RAW':>16} {'CORRECTED':>16} {'Change':>14}")
    print(f"  {'-'*24} {'-'*16} {'-'*16} {'-'*14}")
    for label, key, is_pct in paired_metric_rows:
        raw_val = raw_metrics.get(key, 0.0)
        corrected_val = all_new_metrics.get(key, 0.0)
        delta = corrected_val - raw_val
        if is_pct:
            print(
                f"  {label:<24} "
                f"{raw_val * 100:>15.1f}% "
                f"{corrected_val * 100:>15.1f}% "
                f"{delta * 100:>+13.1f}pp"
            )
        else:
            print(
                f"  {label:<24} "
                f"{raw_val:>16.1f} "
                f"{corrected_val:>16.1f} "
                f"{delta:>+14.1f}"
            )
    print(f"  {'Sample size':<24} {raw_metrics['n']:>16} {all_new_metrics['n']:>16}")
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
    print(f"  {'monday_id':<14} {'type':<16} {'category':<14} {'actual':>7} {'raw':>7} {'old':>7} {'new':>7} {'new_err':>8}")
    print(f"  {'-'*14} {'-'*16} {'-'*14} {'-'*7} {'-'*7} {'-'*7} {'-'*7} {'-'*8}")
    sorted_rows = sorted(comparison_rows, key=lambda r: abs(r["new_err"]), reverse=True)
    for row in sorted_rows[:10]:
        raw_str = str(row["raw_expected"]) if row["raw_expected"] is not None else "n/a"
        old_str = str(row["old_expected"]) if row["old_expected"] is not None else "n/a"
        print(
            f"  {row['monday_id']:<14} {row['type']:<16} {row['category']:<14} "
            f"{row['actual']:>7} {raw_str:>7} {old_str:>7} {row['new_expected']:>7} {row['new_err']:>+8}"
        )

    print("=" * 78)

    # --- Raw pre-correction calibration tables ---
    raw_seg_bias, raw_seg_n, raw_global_bias, raw_global_fallback = build_bias_tables(
        comparison_rows,
        error_key="raw_err",
        min_n=8,
    )
    raw_vb_bias, raw_vb_n = build_value_band_bias_tables(
        comparison_rows,
        error_key="raw_err",
        min_n=15,
    )
    raw_pk_bias, raw_pk_n = build_product_key_bias_tables(
        comparison_rows,
        error_key="raw_err",
        min_n=max(1, args.product_key_min_n),
    )
    raw_pk_vb_bias, raw_pk_vb_n = build_product_key_value_band_bias_tables(
        comparison_rows,
        error_key="raw_err",
        min_n=max(1, args.product_key_value_band_min_n),
    )

    print("\n" + "=" * 78)
    print("RAW PRE-CORRECTION BIAS TABLES (paste into src/config.py)")
    print("=" * 78)
    print(f"GESTATION_BIAS_GLOBAL_DAYS = {raw_global_bias}")
    print(f"GESTATION_BIAS_GLOBAL_FALLBACK_DAYS = {raw_global_fallback}")
    print()
    print("GESTATION_BIAS_BY_SEGMENT = {")
    for (t, c), v in sorted(raw_seg_bias.items(), key=lambda kv: (kv[0][0], kv[0][1])):
        print(f"    ({t!r}, {c!r}): {v},")
    print("}")
    print()
    print("GESTATION_BIAS_SEGMENT_SAMPLE_SIZES = {")
    for (t, c), n in sorted(raw_seg_n.items(), key=lambda kv: (kv[0][0], kv[0][1])):
        print(f"    ({t!r}, {c!r}): {n},")
    print("}")
    print()
    print("GESTATION_BIAS_BY_PRODUCT_KEY_SEGMENT = {")
    for (t, c, pk), v in sorted(raw_pk_bias.items()):
        print(f"    ({t!r}, {c!r}, {pk!r}): {v},")
    print("}")
    print()
    print("GESTATION_BIAS_PRODUCT_KEY_SEGMENT_SAMPLE_SIZES = {")
    for (t, c, pk), n in sorted(raw_pk_n.items()):
        print(f"    ({t!r}, {c!r}, {pk!r}): {n},")
    print("}")
    print()
    print("GESTATION_BIAS_BY_VALUE_BAND_SEGMENT = {")
    for (t, c, vb), v in sorted(raw_vb_bias.items()):
        print(f"    ({t!r}, {c!r}, {vb!r}): {v},")
    print("}")
    print()
    print("GESTATION_BIAS_VALUE_BAND_SEGMENT_SAMPLE_SIZES = {")
    for (t, c, vb), n in sorted(raw_vb_n.items()):
        print(f"    ({t!r}, {c!r}, {vb!r}): {n},")
    print("}")
    print()
    print("GESTATION_BIAS_BY_PRODUCT_KEY_VALUE_BAND_SEGMENT = {")
    for (t, c, pk, vb), v in sorted(raw_pk_vb_bias.items()):
        print(f"    ({t!r}, {c!r}, {pk!r}, {vb!r}): {v},")
    print("}")
    print()
    print("GESTATION_BIAS_PRODUCT_KEY_VALUE_BAND_SEGMENT_SAMPLE_SIZES = {")
    for (t, c, pk, vb), n in sorted(raw_pk_vb_n.items()):
        print(f"    ({t!r}, {c!r}, {pk!r}, {vb!r}): {n},")
    print("}")
    print("=" * 78)

    # --- Post-correction residual monitoring tables ---
    residual_seg_bias, residual_seg_n, residual_global_bias, residual_global_fallback = build_bias_tables(
        comparison_rows,
        error_key="new_err",
        min_n=8,
    )
    residual_vb_bias, residual_vb_n = build_value_band_bias_tables(
        comparison_rows,
        error_key="new_err",
        min_n=15,
    )
    residual_pk_bias, residual_pk_n = build_product_key_bias_tables(
        comparison_rows,
        error_key="new_err",
        min_n=max(1, args.product_key_min_n),
    )
    residual_pk_vb_bias, residual_pk_vb_n = build_product_key_value_band_bias_tables(
        comparison_rows,
        error_key="new_err",
        min_n=max(1, args.product_key_value_band_min_n),
    )

    print("\n" + "=" * 78)
    print("RESIDUAL BIAS AFTER CORRECTION (monitor only)")
    print("=" * 78)
    print(f"Residual global bias days:          {residual_global_bias}")
    print(f"Residual global fallback days:      {residual_global_fallback}")
    print()
    print("RESIDUAL_BIAS_BY_SEGMENT = {")
    for (t, c), v in sorted(residual_seg_bias.items(), key=lambda kv: (kv[0][0], kv[0][1])):
        print(f"    ({t!r}, {c!r}): {v},")
    print("}")
    print()
    print("RESIDUAL_BIAS_SEGMENT_SAMPLE_SIZES = {")
    for (t, c), n in sorted(residual_seg_n.items(), key=lambda kv: (kv[0][0], kv[0][1])):
        print(f"    ({t!r}, {c!r}): {n},")
    print("}")
    print()
    print("RESIDUAL_BIAS_BY_PRODUCT_KEY_SEGMENT = {")
    for (t, c, pk), v in sorted(residual_pk_bias.items()):
        print(f"    ({t!r}, {c!r}, {pk!r}): {v},")
    print("}")
    print()
    print("RESIDUAL_BIAS_PRODUCT_KEY_SEGMENT_SAMPLE_SIZES = {")
    for (t, c, pk), n in sorted(residual_pk_n.items()):
        print(f"    ({t!r}, {c!r}, {pk!r}): {n},")
    print("}")
    print()
    print("RESIDUAL_BIAS_BY_VALUE_BAND_SEGMENT = {")
    for (t, c, vb), v in sorted(residual_vb_bias.items()):
        print(f"    ({t!r}, {c!r}, {vb!r}): {v},")
    print("}")
    print()
    print("RESIDUAL_BIAS_VALUE_BAND_SEGMENT_SAMPLE_SIZES = {")
    for (t, c, vb), n in sorted(residual_vb_n.items()):
        print(f"    ({t!r}, {c!r}, {vb!r}): {n},")
    print("}")
    print()
    print("RESIDUAL_BIAS_BY_PRODUCT_KEY_VALUE_BAND_SEGMENT = {")
    for (t, c, pk, vb), v in sorted(residual_pk_vb_bias.items()):
        print(f"    ({t!r}, {c!r}, {pk!r}, {vb!r}): {v},")
    print("}")
    print()
    print("RESIDUAL_BIAS_PRODUCT_KEY_VALUE_BAND_SEGMENT_SAMPLE_SIZES = {")
    for (t, c, pk, vb), n in sorted(residual_pk_vb_n.items()):
        print(f"    ({t!r}, {c!r}, {pk!r}, {vb!r}): {n},")
    print("}")
    print("=" * 78)

    if failed_ids:
        print(f"\nFailed IDs ({len(failed_ids)}): {', '.join(failed_ids[:30])}" + (" ..." if len(failed_ids) > 30 else ""))


if __name__ == "__main__":
    main()