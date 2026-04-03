"""
Test raw, legacy tail-aware, tuned tail-aware, and tuned-plus-post-tail models.

For Won/Lost projects since a configurable cutoff, this script computes:
- the raw numeric gestation prediction (p50 target)
- the legacy tail-aware target (broad p50 -> p60/p70 behavior)
- the tuned tail-aware target (current config-driven selective behavior)
- a tuned tail-aware plus post-tail additive model using candidate tables built
    from tuned residuals

It prints:
- pairwise summaries for the available model comparisons
- shift / improvement / worsening counts
- top remaining tuned-plus-post-tail errors
- top post-tail improvements versus tuned tail-aware
- tier and segment breakdowns for the selected comparison mode
- candidate post-tail config tables derived from tuned residuals

By default, candidate tables are built on the same sample they are scored on.
If --holdout-start is provided, candidate tables are fit only on rows before the
holdout boundary and the post-tail model is scored only on rows on/after it.

Usage:
        python tests/test_tail_aware_target.py --cutoff 2023-01-01
        python tests/test_tail_aware_target.py --comparison-mode tuned-vs-post-tail
        python tests/test_tail_aware_target.py --holdout-start 2025-01-01
        python tests/test_tail_aware_target.py --holdout-start 2025-01-01 --limit 200
    python tests/test_tail_aware_target.py --holdout-start 2025-01-01 --comparison-mode tuned-vs-post-tail --post-tail-ablation-mode product-key-only
    python tests/test_tail_aware_target.py --comparison-mode tuned-vs-post-tail --post-tail-ablation-mode value-band-only
    python tests/test_tail_aware_target.py --holdout-start 2025-01-01 --comparison-mode tuned-vs-post-tail --post-tail-ablation-mode product-led --post-tail-product-key-min-n 20 --post-tail-product-key-value-band-min-n 25
"""

import argparse
import logging
import math
import sys
import time
from collections import defaultdict
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import src.core.numeric_analyzer as numeric_analyzer_module
from src.core.models import get_backoff_priority_metadata
from src.core.normalization import compute_product_key, normalize_category_value
from src.database.supabase_client import SupabaseClient
from src.services.analysis_service import AnalysisService

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("test_tail_aware_target")

PAGE_SIZE = 500
_SENTINEL = object()
BASE_MODEL_ORDER = ("raw", "legacy", "tuned")
MODEL_ORDER = ("raw", "legacy", "tuned", "tuned_post")
MODEL_LABELS = {
    "raw": "RAW P50",
    "legacy": "LEGACY TAIL",
    "tuned": "TUNED TAIL",
    "tuned_post": "TUNED + POST",
}
PAIR_MODES = {
    "raw-vs-legacy": ("raw", "legacy"),
    "raw-vs-tuned": ("raw", "tuned"),
    "legacy-vs-tuned": ("legacy", "tuned"),
    "tuned-vs-post-tail": ("tuned", "tuned_post"),
}
PAIR_TITLES = {
    "raw-vs-legacy": "LEGACY CHECK: RAW P50 BASELINE VS LEGACY TAIL TARGET",
    "raw-vs-tuned": "CURRENT RUN: RAW P50 BASELINE VS TUNED TAIL TARGET",
    "legacy-vs-tuned": "HEAD TO HEAD: LEGACY TAIL TARGET VS TUNED TAIL TARGET",
    "tuned-vs-post-tail": "NEXT STEP: TUNED TAIL TARGET VS TUNED + POST-TAIL CALIBRATION",
}
POST_TAIL_ABLATION_LABELS = {
    "full": "full hierarchy",
    "no-segment": "segment layer off; keep product-key and value-band refinements",
    "product-led": "product-key + product-key/value-band only",
    "product-key-only": "product-key only",
    "value-band-only": "value-band only",
}


@contextmanager
def temporary_numeric_overrides(**overrides):
    """Temporarily override module-level numeric-analyzer flags."""
    previous: Dict[str, Any] = {}
    for name, value in overrides.items():
        previous[name] = getattr(numeric_analyzer_module, name, _SENTINEL)
        setattr(numeric_analyzer_module, name, value)

    try:
        yield
    finally:
        for name, old_value in previous.items():
            if old_value is _SENTINEL:
                delattr(numeric_analyzer_module, name)
            else:
                setattr(numeric_analyzer_module, name, old_value)


def analyze_raw_legacy_and_tuned(
    svc: AnalysisService,
    project: Dict[str, Any],
) -> Dict[str, Dict[str, Any]]:
    """Return raw, legacy tail-aware, and tuned tail-aware outputs."""
    with temporary_numeric_overrides(
        GESTATION_BIAS_CORRECTION_ENABLED=False,
        GESTATION_TAIL_AWARE_TARGET_ENABLED=False,
    ):
        raw_result = svc.analyze_project(project)

    with temporary_numeric_overrides(
        GESTATION_BIAS_CORRECTION_ENABLED=False,
        GESTATION_TAIL_AWARE_TARGET_ENABLED=True,
        GESTATION_TAIL_TARGET_PROFILE="legacy",
        GESTATION_TAIL_TARGET_ALLOWED_SEGMENTS=(),
        GESTATION_TAIL_TARGET_SEGMENT_OVERRIDES={},
    ):
        legacy_result = svc.analyze_project(project)

    with temporary_numeric_overrides(
        GESTATION_BIAS_CORRECTION_ENABLED=False,
        GESTATION_TAIL_AWARE_TARGET_ENABLED=True,
        GESTATION_TAIL_TARGET_PROFILE="selective",
    ):
        tuned_result = svc.analyze_project(project)

    return {
        "raw": raw_result,
        "legacy": legacy_result,
        "tuned": tuned_result,
    }


def analyze_tuned_post_tail(
    svc: AnalysisService,
    project: Dict[str, Any],
    candidate_config: Dict[str, Any],
) -> Dict[str, Any]:
    """Return tuned selective tail-aware output plus post-tail additive calibration."""
    with temporary_numeric_overrides(
        GESTATION_BIAS_CORRECTION_ENABLED=False,
        GESTATION_TAIL_AWARE_TARGET_ENABLED=True,
        GESTATION_TAIL_TARGET_PROFILE="selective",
        GESTATION_POST_TAIL_BIAS_CORRECTION_ENABLED=True,
        GESTATION_POST_TAIL_BIAS_GLOBAL_DAYS=int(candidate_config.get("global_days", 0)),
        GESTATION_POST_TAIL_BIAS_GLOBAL_FALLBACK_DAYS=int(candidate_config.get("global_fallback_days", 0)),
        GESTATION_POST_TAIL_BIAS_ALLOWED_SEGMENTS=set(candidate_config.get("allowed_segments", set())),
        GESTATION_POST_TAIL_BIAS_BY_SEGMENT=dict(candidate_config.get("segment_bias", {})),
        GESTATION_POST_TAIL_BIAS_SEGMENT_SAMPLE_SIZES=dict(candidate_config.get("segment_n", {})),
        GESTATION_POST_TAIL_BIAS_PRODUCT_KEY_ENABLED=bool(candidate_config.get("product_key_bias")),
        GESTATION_POST_TAIL_BIAS_PRODUCT_KEY_MIN_N=int(
            candidate_config.get(
                "product_key_min_n",
                getattr(numeric_analyzer_module, "GESTATION_POST_TAIL_BIAS_PRODUCT_KEY_MIN_N", 10),
            )
        ),
        GESTATION_POST_TAIL_BIAS_BY_PRODUCT_KEY_SEGMENT=dict(candidate_config.get("product_key_bias", {})),
        GESTATION_POST_TAIL_BIAS_PRODUCT_KEY_SEGMENT_SAMPLE_SIZES=dict(candidate_config.get("product_key_n", {})),
        GESTATION_POST_TAIL_BIAS_VALUE_BAND_ENABLED=bool(candidate_config.get("value_band_bias")),
        GESTATION_POST_TAIL_BIAS_VALUE_BAND_MIN_N=int(
            candidate_config.get(
                "value_band_min_n",
                getattr(numeric_analyzer_module, "GESTATION_POST_TAIL_BIAS_VALUE_BAND_MIN_N", 15),
            )
        ),
        GESTATION_POST_TAIL_BIAS_BY_VALUE_BAND_SEGMENT=dict(candidate_config.get("value_band_bias", {})),
        GESTATION_POST_TAIL_BIAS_VALUE_BAND_SEGMENT_SAMPLE_SIZES=dict(candidate_config.get("value_band_n", {})),
        GESTATION_POST_TAIL_BIAS_PRODUCT_KEY_VALUE_BAND_ENABLED=bool(
            candidate_config.get("product_key_value_band_bias")
        ),
        GESTATION_POST_TAIL_BIAS_PRODUCT_KEY_VALUE_BAND_MIN_N=int(
            candidate_config.get(
                "product_key_value_band_min_n",
                getattr(numeric_analyzer_module, "GESTATION_POST_TAIL_BIAS_PRODUCT_KEY_VALUE_BAND_MIN_N", 15),
            )
        ),
        GESTATION_POST_TAIL_BIAS_BY_PRODUCT_KEY_VALUE_BAND_SEGMENT=dict(
            candidate_config.get("product_key_value_band_bias", {})
        ),
        GESTATION_POST_TAIL_BIAS_PRODUCT_KEY_VALUE_BAND_SEGMENT_SAMPLE_SIZES=dict(
            candidate_config.get("product_key_value_band_n", {})
        ),
    ):
        return svc.analyze_project(project)


def fetch_eligible_projects(
    db: SupabaseClient,
    cutoff: str,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Fetch closed projects with a known gestation_period and creation date."""
    all_rows: List[Dict[str, Any]] = []
    offset = 0

    while True:
        q = (
            db.client.table("projects")
            .select("monday_id, type, category, product_key, gestation_period, status_category, value_band, date_created")
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


def _normalize_segment_key(raw_key: Any) -> Optional[Tuple[Any, Any]]:
    if isinstance(raw_key, (tuple, list)) and len(raw_key) >= 2:
        return (raw_key[0], raw_key[1])
    return None


def configured_post_tail_allowed_segments() -> Set[Tuple[Any, Any]]:
    configured = getattr(
        numeric_analyzer_module,
        "GESTATION_POST_TAIL_BIAS_ALLOWED_SEGMENTS",
        (),
    )
    normalized = {
        segment_key
        for raw_key in (configured or ())
        for segment_key in [_normalize_segment_key(raw_key)]
        if segment_key is not None
    }
    return normalized


def _candidate_segments_from_entries(raw_entries: Any) -> Set[Tuple[Any, Any]]:
    return {
        segment_key
        for raw_key in (raw_entries or ())
        for segment_key in [_normalize_segment_key(raw_key)]
        if segment_key is not None
    }


def _copy_post_tail_candidate_config(candidate_config: Dict[str, Any]) -> Dict[str, Any]:
    copied: Dict[str, Any] = {}
    for key, value in candidate_config.items():
        if isinstance(value, dict):
            copied[key] = dict(value)
        elif isinstance(value, set):
            copied[key] = set(value)
        else:
            copied[key] = value
    return copied


def _derive_active_post_tail_allowed_segments(candidate_config: Dict[str, Any]) -> Set[Tuple[Any, Any]]:
    active_segments: Set[Tuple[Any, Any]] = set()
    for table_name in (
        "segment_bias",
        "product_key_bias",
        "value_band_bias",
        "product_key_value_band_bias",
    ):
        active_segments.update(
            _candidate_segments_from_entries((candidate_config.get(table_name) or {}).keys())
        )

    if active_segments:
        return active_segments

    return _candidate_segments_from_entries(candidate_config.get("allowed_segments") or ())


def apply_post_tail_ablation_mode(candidate_config: Dict[str, Any], mode: str) -> Dict[str, Any]:
    normalized_mode = str(mode or "full")
    if normalized_mode not in POST_TAIL_ABLATION_LABELS:
        raise ValueError(f"Unsupported post-tail ablation mode: {normalized_mode}")

    ablated = _copy_post_tail_candidate_config(candidate_config)
    ablated["post_tail_ablation_mode"] = normalized_mode
    ablated["allowed_segments"] = _candidate_segments_from_entries(
        ablated.get("allowed_segments") or ()
    )

    if normalized_mode == "no-segment":
        ablated["segment_bias"] = {}
        ablated["segment_n"] = {}
    elif normalized_mode == "product-led":
        ablated["segment_bias"] = {}
        ablated["segment_n"] = {}
        ablated["value_band_bias"] = {}
        ablated["value_band_n"] = {}
    elif normalized_mode == "product-key-only":
        ablated["segment_bias"] = {}
        ablated["segment_n"] = {}
        ablated["value_band_bias"] = {}
        ablated["value_band_n"] = {}
        ablated["product_key_value_band_bias"] = {}
        ablated["product_key_value_band_n"] = {}
    elif normalized_mode == "value-band-only":
        ablated["segment_bias"] = {}
        ablated["segment_n"] = {}
        ablated["product_key_bias"] = {}
        ablated["product_key_n"] = {}
        ablated["product_key_value_band_bias"] = {}
        ablated["product_key_value_band_n"] = {}

    if normalized_mode != "full":
        ablated["allowed_segments"] = _derive_active_post_tail_allowed_segments(ablated)

    return ablated


def _normalize_date_key(raw_value: Any) -> str:
    if raw_value is None:
        return ""
    return str(raw_value)[:10]


def normalized_project_category(project: Dict[str, Any]) -> str:
    return normalize_category_value(project.get("category")) or project.get("category") or "?"


def normalized_project_product_key(project: Dict[str, Any]) -> Optional[str]:
    for raw_value in (project.get("product_key"), project.get("product_type")):
        normalized = compute_product_key(raw_value)
        if normalized and normalized != "unknown":
            return normalized
    return None


def _build_bias_tables_from_key(
    rows: List[Dict[str, Any]],
    key_builder,
    error_key: str,
    min_n: int,
    min_bias_days: int = 1,
    positive_only: bool = False,
    allowed_segments: Optional[Set[Tuple[Any, Any]]] = None,
) -> Tuple[Dict[Any, int], Dict[Any, int], int, int]:
    buckets: Dict[Any, List[float]] = defaultdict(list)
    allowed = set(allowed_segments or ())

    for row in rows:
        err = row.get(error_key)
        if err is None:
            continue

        seg = (row.get("type") or "?", row.get("category") or "?")
        if allowed and seg not in allowed:
            continue

        key = key_builder(row)
        if key is None:
            continue

        buckets[key].append(float(err))

    seg_bias: Dict[Any, int] = {}
    seg_n: Dict[Any, int] = {}
    used_errors: List[float] = []

    for key, errs in buckets.items():
        if len(errs) < min_n:
            continue
        mean_err = int(round(sum(errs) / len(errs)))
        if positive_only and mean_err <= 0:
            continue
        if abs(mean_err) < int(min_bias_days):
            continue
        seg_bias[key] = mean_err
        seg_n[key] = len(errs)
        used_errors.extend(errs)

    global_bias = int(round(sum(used_errors) / len(used_errors))) if used_errors else 0
    global_fallback = global_bias
    return seg_bias, seg_n, global_bias, global_fallback


def build_bias_tables(
    rows: List[Dict[str, Any]],
    error_key: str,
    min_n: int,
    min_bias_days: int = 1,
    positive_only: bool = False,
    allowed_segments: Optional[Set[Tuple[Any, Any]]] = None,
) -> Tuple[Dict[Tuple[Any, Any], int], Dict[Tuple[Any, Any], int], int, int]:
    return _build_bias_tables_from_key(
        rows,
        key_builder=lambda row: (row.get("type") or "?", row.get("category") or "?"),
        error_key=error_key,
        min_n=min_n,
        min_bias_days=min_bias_days,
        positive_only=positive_only,
        allowed_segments=allowed_segments,
    )


def build_value_band_bias_tables(
    rows: List[Dict[str, Any]],
    error_key: str,
    min_n: int,
    min_bias_days: int = 1,
    positive_only: bool = False,
    allowed_segments: Optional[Set[Tuple[Any, Any]]] = None,
) -> Tuple[Dict[Tuple[Any, Any, Any], int], Dict[Tuple[Any, Any, Any], int]]:
    value_band_bias, value_band_n, _, _ = _build_bias_tables_from_key(
        rows,
        key_builder=lambda row: (
            (row.get("type") or "?"),
            (row.get("category") or "?"),
            row.get("value_band"),
        ) if row.get("value_band") else None,
        error_key=error_key,
        min_n=min_n,
        min_bias_days=min_bias_days,
        positive_only=positive_only,
        allowed_segments=allowed_segments,
    )
    return value_band_bias, value_band_n


def build_product_key_bias_tables(
    rows: List[Dict[str, Any]],
    error_key: str,
    min_n: int,
    min_bias_days: int = 1,
    positive_only: bool = False,
    allowed_segments: Optional[Set[Tuple[Any, Any]]] = None,
) -> Tuple[Dict[Tuple[Any, Any, Any], int], Dict[Tuple[Any, Any, Any], int]]:
    product_key_bias, product_key_n, _, _ = _build_bias_tables_from_key(
        rows,
        key_builder=lambda row: (
            (row.get("type") or "?"),
            (row.get("category") or "?"),
            row.get("product_key"),
        ) if row.get("product_key") else None,
        error_key=error_key,
        min_n=min_n,
        min_bias_days=min_bias_days,
        positive_only=positive_only,
        allowed_segments=allowed_segments,
    )
    return product_key_bias, product_key_n


def build_product_key_value_band_bias_tables(
    rows: List[Dict[str, Any]],
    error_key: str,
    min_n: int,
    min_bias_days: int = 1,
    positive_only: bool = False,
    allowed_segments: Optional[Set[Tuple[Any, Any]]] = None,
) -> Tuple[Dict[Tuple[Any, Any, Any, Any], int], Dict[Tuple[Any, Any, Any, Any], int]]:
    product_key_value_band_bias, product_key_value_band_n, _, _ = _build_bias_tables_from_key(
        rows,
        key_builder=lambda row: (
            (row.get("type") or "?"),
            (row.get("category") or "?"),
            row.get("product_key"),
            row.get("value_band"),
        ) if row.get("product_key") and row.get("value_band") else None,
        error_key=error_key,
        min_n=min_n,
        min_bias_days=min_bias_days,
        positive_only=positive_only,
        allowed_segments=allowed_segments,
    )
    return product_key_value_band_bias, product_key_value_band_n


def build_post_tail_candidate_config(
    rows: List[Dict[str, Any]],
    segment_min_n: int,
    value_band_min_n: int,
    product_key_min_n: int,
    product_key_value_band_min_n: int,
    min_bias_days: int,
) -> Dict[str, Any]:
    configured_allowed = configured_post_tail_allowed_segments()
    initial_allowed = configured_allowed or None

    segment_bias, segment_n, _, _ = build_bias_tables(
        rows,
        error_key="tuned_err",
        min_n=segment_min_n,
        min_bias_days=min_bias_days,
        positive_only=True,
        allowed_segments=initial_allowed,
    )

    product_key_bias, product_key_n = build_product_key_bias_tables(
        rows,
        error_key="tuned_err",
        min_n=product_key_min_n,
        min_bias_days=min_bias_days,
        positive_only=True,
        allowed_segments=initial_allowed,
    )

    value_band_bias, value_band_n = build_value_band_bias_tables(
        rows,
        error_key="tuned_err",
        min_n=value_band_min_n,
        min_bias_days=min_bias_days,
        positive_only=True,
        allowed_segments=initial_allowed,
    )

    product_key_value_band_bias, product_key_value_band_n = build_product_key_value_band_bias_tables(
        rows,
        error_key="tuned_err",
        min_n=product_key_value_band_min_n,
        min_bias_days=min_bias_days,
        positive_only=True,
        allowed_segments=initial_allowed,
    )

    if configured_allowed:
        allowed_segments = set(configured_allowed)
    else:
        allowed_segments = set(segment_bias.keys())
        allowed_segments.update((key[0], key[1]) for key in product_key_bias)
        allowed_segments.update((key[0], key[1]) for key in value_band_bias)
        allowed_segments.update((key[0], key[1]) for key in product_key_value_band_bias)

    return {
        "global_days": 0,
        "global_fallback_days": 0,
        "allowed_segments": allowed_segments,
        "segment_bias": segment_bias,
        "segment_n": segment_n,
        "product_key_min_n": int(product_key_min_n),
        "product_key_bias": product_key_bias,
        "product_key_n": product_key_n,
        "value_band_min_n": int(value_band_min_n),
        "value_band_bias": value_band_bias,
        "value_band_n": value_band_n,
        "product_key_value_band_min_n": int(product_key_value_band_min_n),
        "product_key_value_band_bias": product_key_value_band_bias,
        "product_key_value_band_n": product_key_value_band_n,
    }


def split_rows_for_holdout(
    rows: List[Dict[str, Any]],
    holdout_start: Optional[str],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    if not holdout_start:
        return rows, rows

    holdout_key = _normalize_date_key(holdout_start)
    calibration_rows = [
        row for row in rows
        if _normalize_date_key(row.get("date_created")) < holdout_key
    ]
    evaluation_rows = [
        row for row in rows
        if _normalize_date_key(row.get("date_created")) >= holdout_key
    ]
    return calibration_rows, evaluation_rows


def compute_pair_counts(
    rows: List[Dict[str, Any]],
    left_key: str,
    right_key: str,
) -> Dict[str, int]:
    left_expected_key = f"{left_key}_expected"
    right_expected_key = f"{right_key}_expected"
    left_err_key = f"{left_key}_err"
    right_err_key = f"{right_key}_err"

    changed = 0
    improved = 0
    worsened = 0
    unchanged = 0

    for row in rows:
        if row[right_expected_key] != row[left_expected_key]:
            changed += 1

        left_abs = abs(row[left_err_key])
        right_abs = abs(row[right_err_key])
        if right_abs < left_abs:
            improved += 1
        elif right_abs > left_abs:
            worsened += 1
        else:
            unchanged += 1

    return {
        "n": len(rows),
        "changed": changed,
        "improved": improved,
        "worsened": worsened,
        "unchanged": unchanged,
    }


def build_group_summary(
    rows: List[Dict[str, Any]],
    group_keys: List[str],
    min_n: int,
    left_key: str,
    right_key: str,
) -> List[Dict[str, Any]]:
    grouped: Dict[tuple, List[Dict[str, Any]]] = defaultdict(list)
    left_err_key = f"{left_key}_err"
    right_err_key = f"{right_key}_err"
    left_expected_key = f"{left_key}_expected"
    right_expected_key = f"{right_key}_expected"

    for row in rows:
        group_key = tuple(row.get(key) for key in group_keys)
        grouped[group_key].append(row)

    summary_rows: List[Dict[str, Any]] = []
    for group_key, group_rows in grouped.items():
        if len(group_rows) < min_n:
            continue

        left_metrics = compute_metrics([row[left_err_key] for row in group_rows])
        right_metrics = compute_metrics([row[right_err_key] for row in group_rows])
        shifted = sum(1 for row in group_rows if row[right_expected_key] != row[left_expected_key])
        improved = sum(1 for row in group_rows if abs(row[right_err_key]) < abs(row[left_err_key]))
        worsened = sum(1 for row in group_rows if abs(row[right_err_key]) > abs(row[left_err_key]))

        summary_rows.append(
            {
                "group_key": group_key,
                "n": len(group_rows),
                "shifted": shifted,
                "improved": improved,
                "worsened": worsened,
                "left_metrics": left_metrics,
                "right_metrics": right_metrics,
                "mae_delta": right_metrics["mae"] - left_metrics["mae"],
                "bias_delta": right_metrics["mean_bias"] - left_metrics["mean_bias"],
                "p90_delta": right_metrics["p90_ae"] - left_metrics["p90_ae"],
            }
        )

    return summary_rows


def print_pair_summary(
    title: str,
    left_key: str,
    right_key: str,
    metrics_by_model: Dict[str, Dict[str, float]],
    rows: List[Dict[str, Any]],
):
    left_metrics = metrics_by_model[left_key]
    right_metrics = metrics_by_model[right_key]
    pair_counts = compute_pair_counts(rows, left_key, right_key)

    print(f"  {title}")
    print(
        f"  {'Metric':<24} "
        f"{MODEL_LABELS[left_key]:>16} "
        f"{MODEL_LABELS[right_key]:>16} "
        f"{'Change':>14}"
    )
    print(f"  {'-'*24} {'-'*16} {'-'*16} {'-'*14}")

    metric_rows = [
        ("MAE (days)", "mae", False),
        ("Mean Bias (days)", "mean_bias", False),
        ("Median AE (days)", "median_ae", False),
        ("P90 AE (days)", "p90_ae", False),
        ("Underpredict %", "under_rate", True),
        ("Overpredict %", "over_rate", True),
        ("Severe under >180d %", "severe_under_180", True),
        ("Severe over >180d %", "severe_over_180", True),
    ]

    for label, key, is_pct in metric_rows:
        left_val = left_metrics.get(key, 0.0)
        right_val = right_metrics.get(key, 0.0)
        delta = right_val - left_val
        if is_pct:
            print(
                f"  {label:<24} "
                f"{left_val * 100:>15.1f}% "
                f"{right_val * 100:>15.1f}% "
                f"{delta * 100:>+13.1f}pp"
            )
        else:
            print(
                f"  {label:<24} "
                f"{left_val:>16.1f} "
                f"{right_val:>16.1f} "
                f"{delta:>+14.1f}"
            )

    print(f"  {'Sample size':<24} {left_metrics['n']:>16} {right_metrics['n']:>16}")
    print()

    n = pair_counts["n"]
    if n > 0:
        print(f"  Point target changed:      {pair_counts['changed']}/{n} ({pair_counts['changed']/n*100:.1f}%)")
        print(f"  Right-side model improved: {pair_counts['improved']}/{n} ({pair_counts['improved']/n*100:.1f}%)")
        print(f"  Right-side model worsened: {pair_counts['worsened']}/{n} ({pair_counts['worsened']/n*100:.1f}%)")
        print(f"  Rows unchanged:            {pair_counts['unchanged']}/{n} ({pair_counts['unchanged']/n*100:.1f}%)")
    else:
        print("  Point target changed:      0/0 (0.0%)")
        print("  Right-side model improved: 0/0 (0.0%)")
        print("  Right-side model worsened: 0/0 (0.0%)")
        print("  Rows unchanged:            0/0 (0.0%)")


def print_hit_rate_snapshot(
    metrics_by_model: Dict[str, Dict[str, float]],
    error_series: Dict[str, List[float]],
):
    print("  MODEL HIT-RATE SNAPSHOT")
    print(f"  {'Model':<16} {'<=30d':>10} {'<=90d':>10} {'MAE':>10} {'Bias':>10}")
    print(f"  {'-'*16} {'-'*10} {'-'*10} {'-'*10} {'-'*10}")
    for model_key in MODEL_ORDER:
        errors = error_series[model_key]
        n = len(errors)
        within_30 = sum(1 for err in errors if abs(err) <= 30)
        within_90 = sum(1 for err in errors if abs(err) <= 90)
        metrics = metrics_by_model[model_key]
        within_30_pct = (within_30 / n * 100) if n else 0.0
        within_90_pct = (within_90 / n * 100) if n else 0.0
        print(
            f"  {MODEL_LABELS[model_key]:<16} "
            f"{within_30_pct:>9.1f}% "
            f"{within_90_pct:>9.1f}% "
            f"{metrics['mae']:>10.1f} "
            f"{metrics['mean_bias']:>+10.1f}"
        )


def main():
    parser = argparse.ArgumentParser(
        description="Compare raw, legacy, tuned, and tuned-plus-post-tail gestation targets"
    )
    parser.add_argument("--cutoff", default="2023-01-01", help="Earliest date_created (YYYY-MM-DD)")
    parser.add_argument("--limit", type=int, default=None, help="Max projects to process")
    parser.add_argument("--throttle", type=float, default=0.0, help="Seconds to sleep between projects")
    parser.add_argument("--summary-min-n", type=int, default=20, help="Minimum rows for segment summary tables")
    parser.add_argument(
        "--holdout-start",
        default=None,
        help="Optional holdout boundary; fit candidate tables on rows before this date and score on rows on/after it",
    )
    parser.add_argument("--post-tail-min-n", type=int, default=8, help="Minimum rows for post-tail segment candidate tables")
    parser.add_argument("--post-tail-value-band-min-n", type=int, default=15, help="Minimum rows for post-tail value-band candidate tables")
    parser.add_argument("--post-tail-product-key-min-n", type=int, default=10, help="Minimum rows for post-tail product-key candidate tables")
    parser.add_argument("--post-tail-product-key-value-band-min-n", type=int, default=15, help="Minimum rows for post-tail product-key + value-band candidate tables")
    parser.add_argument("--post-tail-min-bias", type=int, default=10, help="Minimum positive mean residual to include in post-tail tables")
    parser.add_argument(
        "--post-tail-ablation-mode",
        choices=list(POST_TAIL_ABLATION_LABELS.keys()),
        default="full",
        help=(
            "Which post-tail hierarchy to evaluate: full, no-segment, product-led, product-key-only, or value-band-only"
        ),
    )
    parser.add_argument(
        "--comparison-mode",
        choices=["all", "raw-vs-legacy", "raw-vs-tuned", "legacy-vs-tuned", "tuned-vs-post-tail"],
        default="all",
        help="Which pairwise summaries to print (all still computes every model)",
    )
    args = parser.parse_args()

    if args.holdout_start and _normalize_date_key(args.holdout_start) <= _normalize_date_key(args.cutoff):
        parser.error("--holdout-start must be later than --cutoff")

    db = SupabaseClient()
    svc = AnalysisService(db_client=db)

    fetch_limit = None if args.holdout_start else args.limit
    if args.holdout_start and args.limit is not None:
        logger.info(
            "Holdout mode enabled; fetching full sample so limit=%s applies to evaluation rows only",
            args.limit,
        )

    logger.info(
        "Fetching eligible closed projects (cutoff=%s, limit=%s) ...",
        args.cutoff,
        fetch_limit,
    )
    projects = fetch_eligible_projects(db, args.cutoff, fetch_limit)
    if not projects:
        logger.warning("No eligible projects found.")
        return

    logger.info("Found %d eligible projects", len(projects))

    ok = 0
    errs = 0
    failed_ids: List[str] = []
    error_series: Dict[str, List[float]] = {
        "raw": [],
        "legacy": [],
        "tuned": [],
        "tuned_post": [],
    }
    comparison_rows: List[Dict[str, Any]] = []
    project_payloads: Dict[Any, Dict[str, Any]] = {}
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
            proj_value_band = full_proj.get("value_band") or proj.get("value_band")

            project_payloads[monday_id] = full_proj

            seg_key = svc._cluster_key(full_proj)
            _, _, backoff_tier = svc._fetch_segment_df(seg_key)
            tier_meta = get_backoff_priority_metadata(backoff_tier)

            run_triplet = analyze_raw_legacy_and_tuned(svc, full_proj)
            expected_days = {
                model_key: result.get("expected_gestation_days")
                for model_key, result in run_triplet.items()
            }

            if any(expected_days[model_key] is None for model_key in BASE_MODEL_ORDER):
                errs += 1
                failed_ids.append(monday_id)
                continue

            ok += 1
            model_errors = {
                model_key: actual - int(expected_days[model_key])
                for model_key in BASE_MODEL_ORDER
            }

            for model_key, model_err in model_errors.items():
                error_series[model_key].append(model_err)

            comparison_rows.append(
                {
                    "monday_id": monday_id,
                    "date_created": full_proj.get("date_created") or proj.get("date_created"),
                    "type": proj_type,
                    "category": proj_cat,
                    "product_key": proj_product_key,
                    "value_band": proj_value_band,
                    "backoff_tier": backoff_tier,
                    "backoff_priority_tier": tier_meta["priority_tier"],
                    "backoff_label": tier_meta["label"],
                    "actual": actual,
                    "raw_expected": int(expected_days["raw"]),
                    "legacy_expected": int(expected_days["legacy"]),
                    "tuned_expected": int(expected_days["tuned"]),
                    "raw_err": model_errors["raw"],
                    "legacy_err": model_errors["legacy"],
                    "tuned_err": model_errors["tuned"],
                    "legacy_abs_gain_vs_raw": abs(model_errors["raw"]) - abs(model_errors["legacy"]),
                    "tuned_abs_gain_vs_raw": abs(model_errors["raw"]) - abs(model_errors["tuned"]),
                    "tuned_abs_gain_vs_legacy": abs(model_errors["legacy"]) - abs(model_errors["tuned"]),
                    "tuned_positive_tail_reduction_vs_legacy": model_errors["legacy"] - model_errors["tuned"],
                }
            )

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
                i,
                len(projects),
                i / len(projects) * 100,
                ok,
                errs,
                rate,
                eta // 60,
                eta % 60,
            )

        if args.throttle > 0:
            time.sleep(args.throttle)

    calibration_rows, evaluation_rows = split_rows_for_holdout(
        comparison_rows,
        args.holdout_start,
    )
    if args.holdout_start and args.limit is not None:
        evaluation_rows = evaluation_rows[:args.limit]

    if args.holdout_start:
        if not calibration_rows:
            logger.error("No calibration rows found before holdout boundary %s", args.holdout_start)
            return
        if not evaluation_rows:
            logger.error("No evaluation rows found on/after holdout boundary %s", args.holdout_start)
            return
        logger.info(
            "Holdout split at %s: calibration=%d rows, evaluation=%d rows",
            args.holdout_start,
            len(calibration_rows),
            len(evaluation_rows),
        )
    else:
        logger.info("Same-sample diagnostic mode: %d rows", len(comparison_rows))

    report_rows = evaluation_rows

    derived_post_tail_candidate_config = build_post_tail_candidate_config(
        calibration_rows,
        segment_min_n=max(1, args.post_tail_min_n),
        value_band_min_n=max(1, args.post_tail_value_band_min_n),
        product_key_min_n=max(1, args.post_tail_product_key_min_n),
        product_key_value_band_min_n=max(1, args.post_tail_product_key_value_band_min_n),
        min_bias_days=max(1, args.post_tail_min_bias),
    )
    logger.info(
        "Derived full post-tail candidates: %d segments, %d product-key cells, %d value-band cells, %d product-key/value-band cells",
        len(derived_post_tail_candidate_config["segment_bias"]),
        len(derived_post_tail_candidate_config["product_key_bias"]),
        len(derived_post_tail_candidate_config["value_band_bias"]),
        len(derived_post_tail_candidate_config["product_key_value_band_bias"]),
    )

    post_tail_candidate_config = apply_post_tail_ablation_mode(
        derived_post_tail_candidate_config,
        args.post_tail_ablation_mode,
    )
    logger.info(
        "Active post-tail mode '%s': %d segments, %d product-key cells, %d value-band cells, %d product-key/value-band cells",
        args.post_tail_ablation_mode,
        len(post_tail_candidate_config["segment_bias"]),
        len(post_tail_candidate_config["product_key_bias"]),
        len(post_tail_candidate_config["value_band_bias"]),
        len(post_tail_candidate_config["product_key_value_band_bias"]),
    )
    if (
        not post_tail_candidate_config["segment_bias"]
        and not post_tail_candidate_config["product_key_bias"]
        and not post_tail_candidate_config["value_band_bias"]
        and not post_tail_candidate_config["product_key_value_band_bias"]
    ):
        logger.warning("No post-tail candidates derived from the selected calibration sample")

    error_series = {
        model_key: [row[f"{model_key}_err"] for row in report_rows]
        for model_key in BASE_MODEL_ORDER
    }
    error_series["tuned_post"] = []

    post_tail_fallback_ids: List[Any] = []
    for i, row in enumerate(report_rows, 1):
        monday_id = row["monday_id"]
        actual = row["actual"]
        full_proj = project_payloads.get(monday_id)

        try:
            tuned_post_result = analyze_tuned_post_tail(
                svc,
                full_proj,
                post_tail_candidate_config,
            )
            tuned_post_expected = tuned_post_result.get("expected_gestation_days")
        except Exception as exc:
            tuned_post_expected = None
            logger.debug("Post-tail failed %s: %s", monday_id, exc)

        if tuned_post_expected is None:
            tuned_post_expected = row["tuned_expected"]
            post_tail_fallback_ids.append(monday_id)

        tuned_post_expected = int(tuned_post_expected)
        tuned_post_err = actual - tuned_post_expected
        error_series["tuned_post"].append(tuned_post_err)

        row.update(
            {
                "tuned_post_expected": tuned_post_expected,
                "tuned_post_err": tuned_post_err,
                "tuned_post_abs_gain_vs_tuned": abs(row["tuned_err"]) - abs(tuned_post_err),
                "tuned_post_abs_gain_vs_raw": abs(row["raw_err"]) - abs(tuned_post_err),
            }
        )

        if i % 200 == 0 or i == len(report_rows):
            logger.info(
                "Post-tail pass %d/%d (%.1f%%)",
                i,
                len(report_rows),
                i / max(1, len(report_rows)) * 100,
            )

    metrics_by_model = {
        model_key: compute_metrics(errors)
        for model_key, errors in error_series.items()
    }

    print("\n" + "=" * 78)
    print("TAIL-AWARE TARGET TEST RESULTS")
    print("=" * 78)
    print(f"  Base rows analyzed: {ok} ok, {errs} errors, {len(projects)} total fetched")
    if args.holdout_start:
        print("  Evaluation mode:   time-split holdout")
        print(f"  Holdout start:     {args.holdout_start}")
        print(f"  Calibration rows:  {len(calibration_rows)}")
        print(f"  Evaluation rows:   {len(report_rows)}")
        if args.limit is not None:
            print(f"  Eval row limit:    {args.limit}")
    else:
        print("  Evaluation mode:   same-sample diagnostic")
        print(f"  Projects scored:   {len(report_rows)}")
    print(f"  Cutoff:            {args.cutoff}")
    print(
        f"  Post-tail mode:    {POST_TAIL_ABLATION_LABELS[args.post_tail_ablation_mode]} "
        f"({args.post_tail_ablation_mode})"
    )
    print(
        f"  Post-tail tables:  {len(post_tail_candidate_config['segment_bias'])} segments, "
        f"{len(post_tail_candidate_config['product_key_bias'])} product-key cells, "
        f"{len(post_tail_candidate_config['value_band_bias'])} value-band cells, "
        f"{len(post_tail_candidate_config['product_key_value_band_bias'])} product-key/value-band cells"
    )
    print()
    summary_modes = list(PAIR_MODES) if args.comparison_mode == "all" else [args.comparison_mode]
    for index, mode in enumerate(summary_modes):
        left_key, right_key = PAIR_MODES[mode]
        print_pair_summary(
            PAIR_TITLES[mode],
            left_key,
            right_key,
            metrics_by_model,
            report_rows,
        )
        if index != len(summary_modes) - 1:
            print()

    print_hit_rate_snapshot(metrics_by_model, error_series)
    print()

    print("  TOP 10 LARGEST REMAINING ERRORS (tuned + post-tail):")
    print(
        f"  {'monday_id':<14} {'type':<16} {'category':<14} {'actual':>7} "
        f"{'raw':>7} {'legacy':>7} {'tuned':>7} {'post':>7} {'raw_err':>8} {'tuned_err':>10} {'post_err':>10}"
    )
    print(
        f"  {'-'*14} {'-'*16} {'-'*14} {'-'*7} {'-'*7} {'-'*7} {'-'*7} {'-'*7} {'-'*8} {'-'*10} {'-'*10}"
    )
    sorted_rows = sorted(report_rows, key=lambda r: abs(r["tuned_post_err"]), reverse=True)
    for row in sorted_rows[:10]:
        print(
            f"  {row['monday_id']:<14} {row['type']:<16} {row['category']:<14} "
            f"{row['actual']:>7} {row['raw_expected']:>7} {row['legacy_expected']:>7} {row['tuned_expected']:>7} {row['tuned_post_expected']:>7} "
            f"{row['raw_err']:>+8} {row['tuned_err']:>+10} {row['tuned_post_err']:>+10}"
        )

    print()
    print("  TOP 10 BIGGEST POST-TAIL IMPROVEMENTS VS TUNED:")
    print(
        f"  {'monday_id':<14} {'type':<16} {'category':<14} "
        f"{'tuned_err':>10} {'post_err':>10} {'abs_reduction':>14}"
    )
    print(f"  {'-'*14} {'-'*16} {'-'*14} {'-'*10} {'-'*10} {'-'*14}")
    improved_tail_rows = [
        row
        for row in report_rows
        if row["tuned_post_abs_gain_vs_tuned"] > 0
    ]
    improved_tail_rows.sort(key=lambda r: r["tuned_post_abs_gain_vs_tuned"], reverse=True)
    for row in improved_tail_rows[:10]:
        print(
            f"  {row['monday_id']:<14} {row['type']:<16} {row['category']:<14} "
            f"{row['tuned_err']:>+10} {row['tuned_post_err']:>+10} {row['tuned_post_abs_gain_vs_tuned']:>+14.1f}"
        )

    breakdown_mode = "tuned-vs-post-tail" if args.comparison_mode == "all" else args.comparison_mode
    left_key, right_key = PAIR_MODES[breakdown_mode]

    tier_summaries = build_group_summary(
        report_rows,
        group_keys=["backoff_priority_tier", "backoff_label"],
        min_n=1,
        left_key=left_key,
        right_key=right_key,
    )
    tier_summaries.sort(key=lambda row: row["group_key"][0])

    print()
    print(f"  BACKOFF TIER SUMMARY ({MODEL_LABELS[left_key]} VS {MODEL_LABELS[right_key]}):")
    print(f"  {'tier':<4} {'n':>5} {'shift%':>8} {'impr%':>8} {'left_mae':>9} {'right_mae':>9} {'delta':>8} {'left_bias':>10} {'right_bias':>10}")
    print(f"  {'-'*4} {'-'*5} {'-'*8} {'-'*8} {'-'*9} {'-'*9} {'-'*8} {'-'*10} {'-'*10}")
    for row in tier_summaries:
        tier_num, _tier_label = row["group_key"]
        n = row["n"]
        shift_pct = row["shifted"] / n * 100 if n else 0.0
        impr_pct = row["improved"] / n * 100 if n else 0.0
        left_group = row["left_metrics"]
        right_group = row["right_metrics"]
        print(
            f"  {tier_num:<4} {n:>5} {shift_pct:>7.1f}% {impr_pct:>7.1f}% "
            f"{left_group['mae']:>9.1f} {right_group['mae']:>9.1f} {row['mae_delta']:>+8.1f} "
            f"{left_group['mean_bias']:>+10.1f} {right_group['mean_bias']:>+10.1f}"
        )

    segment_summaries = build_group_summary(
        report_rows,
        group_keys=["type", "category"],
        min_n=max(1, args.summary_min_n),
        left_key=left_key,
        right_key=right_key,
    )

    best_segments = sorted(segment_summaries, key=lambda row: (row["mae_delta"], row["p90_delta"], -row["n"]))[:8]
    worst_segments = sorted(segment_summaries, key=lambda row: (row["mae_delta"], row["p90_delta"], row["n"]), reverse=True)[:8]

    def _print_segment_summary(title: str, rows: List[Dict[str, Any]]):
        print()
        print(f"  {title}:")
        print(f"  {'type':<16} {'category':<14} {'n':>5} {'shift%':>8} {'left_mae':>9} {'right_mae':>10} {'delta':>8} {'left_p90':>9} {'right_p90':>10}")
        print(f"  {'-'*16} {'-'*14} {'-'*5} {'-'*8} {'-'*9} {'-'*9} {'-'*8} {'-'*9} {'-'*9}")
        for row in rows:
            proj_type, proj_cat = row["group_key"]
            n = row["n"]
            shift_pct = row["shifted"] / n * 100 if n else 0.0
            left_group = row["left_metrics"]
            right_group = row["right_metrics"]
            print(
                f"  {str(proj_type):<16} {str(proj_cat):<14} {n:>5} {shift_pct:>7.1f}% "
                f"{left_group['mae']:>9.1f} {right_group['mae']:>10.1f} {row['mae_delta']:>+8.1f} "
                f"{left_group['p90_ae']:>9.1f} {right_group['p90_ae']:>10.1f}"
            )

    if segment_summaries:
        _print_segment_summary(
            f"TOP SEGMENTS BY MAE IMPROVEMENT ({MODEL_LABELS[left_key]} -> {MODEL_LABELS[right_key]}; n >= {args.summary_min_n})",
            best_segments,
        )
        _print_segment_summary(
            f"TOP SEGMENTS BY MAE WORSENING ({MODEL_LABELS[left_key]} -> {MODEL_LABELS[right_key]}; n >= {args.summary_min_n})",
            worst_segments,
        )

    print()
    candidate_source_label = (
        "holdout calibration sample"
        if args.holdout_start
        else "same-sample tuned residuals; diagnostic only"
    )
    print(
        "  POST-TAIL CANDIDATE TABLES "
        f"({candidate_source_label}; mode={args.post_tail_ablation_mode})"
    )
    print(f"  GESTATION_POST_TAIL_BIAS_GLOBAL_DAYS = {post_tail_candidate_config['global_days']}")
    print(f"  GESTATION_POST_TAIL_BIAS_GLOBAL_FALLBACK_DAYS = {post_tail_candidate_config['global_fallback_days']}")
    print(f"  GESTATION_POST_TAIL_BIAS_PRODUCT_KEY_MIN_N = {post_tail_candidate_config['product_key_min_n']}")
    print(f"  GESTATION_POST_TAIL_BIAS_VALUE_BAND_MIN_N = {post_tail_candidate_config['value_band_min_n']}")
    print(
        "  GESTATION_POST_TAIL_BIAS_PRODUCT_KEY_VALUE_BAND_MIN_N = "
        f"{post_tail_candidate_config['product_key_value_band_min_n']}"
    )
    print("  GESTATION_POST_TAIL_BIAS_ALLOWED_SEGMENTS = [")
    for seg in sorted(post_tail_candidate_config["allowed_segments"]):
        print(f"      {seg!r},")
    print("  ]")
    print("  GESTATION_POST_TAIL_BIAS_BY_SEGMENT = {")
    for key, value in sorted(post_tail_candidate_config["segment_bias"].items()):
        print(f"      {key!r}: {value},")
    print("  }")
    print("  GESTATION_POST_TAIL_BIAS_SEGMENT_SAMPLE_SIZES = {")
    for key, value in sorted(post_tail_candidate_config["segment_n"].items()):
        print(f"      {key!r}: {value},")
    print("  }")
    print("  GESTATION_POST_TAIL_BIAS_BY_PRODUCT_KEY_SEGMENT = {")
    for key, value in sorted(post_tail_candidate_config["product_key_bias"].items()):
        print(f"      {key!r}: {value},")
    print("  }")
    print("  GESTATION_POST_TAIL_BIAS_PRODUCT_KEY_SEGMENT_SAMPLE_SIZES = {")
    for key, value in sorted(post_tail_candidate_config["product_key_n"].items()):
        print(f"      {key!r}: {value},")
    print("  }")
    print("  GESTATION_POST_TAIL_BIAS_BY_VALUE_BAND_SEGMENT = {")
    for key, value in sorted(post_tail_candidate_config["value_band_bias"].items()):
        print(f"      {key!r}: {value},")
    print("  }")
    print("  GESTATION_POST_TAIL_BIAS_VALUE_BAND_SEGMENT_SAMPLE_SIZES = {")
    for key, value in sorted(post_tail_candidate_config["value_band_n"].items()):
        print(f"      {key!r}: {value},")
    print("  }")
    print("  GESTATION_POST_TAIL_BIAS_BY_PRODUCT_KEY_VALUE_BAND_SEGMENT = {")
    for key, value in sorted(post_tail_candidate_config["product_key_value_band_bias"].items()):
        print(f"      {key!r}: {value},")
    print("  }")
    print("  GESTATION_POST_TAIL_BIAS_PRODUCT_KEY_VALUE_BAND_SEGMENT_SAMPLE_SIZES = {")
    for key, value in sorted(post_tail_candidate_config["product_key_value_band_n"].items()):
        print(f"      {key!r}: {value},")
    print("  }")

    print("=" * 78)

    if failed_ids:
        preview = ", ".join(failed_ids[:30])
        suffix = " ..." if len(failed_ids) > 30 else ""
        print(f"\nFailed IDs ({len(failed_ids)}): {preview}{suffix}")

    if post_tail_fallback_ids:
        preview = ", ".join(str(mid) for mid in post_tail_fallback_ids[:30])
        suffix = " ..." if len(post_tail_fallback_ids) > 30 else ""
        print(
            f"\nPost-tail fallback IDs ({len(post_tail_fallback_ids)}): {preview}{suffix}"
        )


def test_build_post_tail_candidate_config_tracks_product_key_specific_cells(monkeypatch):
    monkeypatch.setattr(
        numeric_analyzer_module,
        "GESTATION_POST_TAIL_BIAS_ALLOWED_SEGMENTS",
        (),
    )

    rows = [
        {
            "type": "Refurbishment",
            "category": "Commercial",
            "product_key": "pir_tissue",
            "value_band": "Small (<15k)",
            "tuned_err": 70,
        },
        {
            "type": "Refurbishment",
            "category": "Commercial",
            "product_key": "pir_tissue",
            "value_band": "Small (<15k)",
            "tuned_err": 90,
        },
    ]

    candidate_config = build_post_tail_candidate_config(
        rows,
        segment_min_n=3,
        value_band_min_n=3,
        product_key_min_n=2,
        product_key_value_band_min_n=2,
        min_bias_days=10,
    )

    assert candidate_config["segment_bias"] == {}
    assert candidate_config["product_key_bias"] == {
        ("Refurbishment", "Commercial", "pir_tissue"): 80,
    }
    assert candidate_config["product_key_value_band_bias"] == {
        ("Refurbishment", "Commercial", "pir_tissue", "Small (<15k)"): 80,
    }
    assert candidate_config["allowed_segments"] == {
        ("Refurbishment", "Commercial"),
    }


def test_analyze_tuned_post_tail_applies_product_key_candidate_overrides():
    class StubService:
        def analyze_project(self, _project):
            return {
                "product_key_enabled": numeric_analyzer_module.GESTATION_POST_TAIL_BIAS_PRODUCT_KEY_ENABLED,
                "product_key_min_n": numeric_analyzer_module.GESTATION_POST_TAIL_BIAS_PRODUCT_KEY_MIN_N,
                "product_key_bias": dict(numeric_analyzer_module.GESTATION_POST_TAIL_BIAS_BY_PRODUCT_KEY_SEGMENT),
                "value_band_enabled": numeric_analyzer_module.GESTATION_POST_TAIL_BIAS_VALUE_BAND_ENABLED,
                "value_band_min_n": numeric_analyzer_module.GESTATION_POST_TAIL_BIAS_VALUE_BAND_MIN_N,
                "product_key_value_band_enabled": numeric_analyzer_module.GESTATION_POST_TAIL_BIAS_PRODUCT_KEY_VALUE_BAND_ENABLED,
                "product_key_value_band_min_n": numeric_analyzer_module.GESTATION_POST_TAIL_BIAS_PRODUCT_KEY_VALUE_BAND_MIN_N,
                "product_key_value_band_bias": dict(
                    numeric_analyzer_module.GESTATION_POST_TAIL_BIAS_BY_PRODUCT_KEY_VALUE_BAND_SEGMENT
                ),
            }

    candidate_config = {
        "global_days": 0,
        "global_fallback_days": 0,
        "allowed_segments": {("Refurbishment", "Commercial")},
        "segment_bias": {},
        "segment_n": {},
        "product_key_min_n": 4,
        "product_key_bias": {
            ("Refurbishment", "Commercial", "pir_tissue"): 60,
        },
        "product_key_n": {
            ("Refurbishment", "Commercial", "pir_tissue"): 12,
        },
        "value_band_min_n": 6,
        "value_band_bias": {},
        "value_band_n": {},
        "product_key_value_band_min_n": 5,
        "product_key_value_band_bias": {
            ("Refurbishment", "Commercial", "pir_tissue", "Small (<15k)"): 55,
        },
        "product_key_value_band_n": {
            ("Refurbishment", "Commercial", "pir_tissue", "Small (<15k)"): 11,
        },
    }

    result = analyze_tuned_post_tail(StubService(), {"monday_id": "1"}, candidate_config)

    assert result["product_key_enabled"] is True
    assert result["product_key_min_n"] == 4
    assert result["product_key_bias"] == {
        ("Refurbishment", "Commercial", "pir_tissue"): 60,
    }
    assert result["value_band_enabled"] is False
    assert result["value_band_min_n"] == 6
    assert result["product_key_value_band_enabled"] is True
    assert result["product_key_value_band_min_n"] == 5
    assert result["product_key_value_band_bias"] == {
        ("Refurbishment", "Commercial", "pir_tissue", "Small (<15k)"): 55,
    }


def test_apply_post_tail_ablation_mode_prunes_expected_layers():
    candidate_config = {
        "global_days": 0,
        "global_fallback_days": 0,
        "allowed_segments": {
            ("New Build", "Apartments"),
            ("Refurbishment", "Commercial"),
        },
        "segment_bias": {
            ("New Build", "Apartments"): 30,
            ("Refurbishment", "Commercial"): 40,
        },
        "segment_n": {
            ("New Build", "Apartments"): 12,
            ("Refurbishment", "Commercial"): 10,
        },
        "product_key_min_n": 4,
        "product_key_bias": {
            ("Refurbishment", "Commercial", "pir_tissue"): 60,
        },
        "product_key_n": {
            ("Refurbishment", "Commercial", "pir_tissue"): 12,
        },
        "value_band_min_n": 6,
        "value_band_bias": {
            ("New Build", "Apartments", "Small (<15k)"): 25,
        },
        "value_band_n": {
            ("New Build", "Apartments", "Small (<15k)"): 11,
        },
        "product_key_value_band_min_n": 5,
        "product_key_value_band_bias": {
            ("Refurbishment", "Commercial", "pir_tissue", "Small (<15k)"): 55,
        },
        "product_key_value_band_n": {
            ("Refurbishment", "Commercial", "pir_tissue", "Small (<15k)"): 9,
        },
    }

    full_config = apply_post_tail_ablation_mode(candidate_config, "full")
    no_segment_config = apply_post_tail_ablation_mode(candidate_config, "no-segment")
    product_led_config = apply_post_tail_ablation_mode(candidate_config, "product-led")
    product_key_only_config = apply_post_tail_ablation_mode(candidate_config, "product-key-only")
    value_band_only_config = apply_post_tail_ablation_mode(candidate_config, "value-band-only")

    assert full_config["segment_bias"] == candidate_config["segment_bias"]
    assert full_config["allowed_segments"] == candidate_config["allowed_segments"]

    assert no_segment_config["segment_bias"] == {}
    assert no_segment_config["product_key_bias"] == candidate_config["product_key_bias"]
    assert no_segment_config["value_band_bias"] == candidate_config["value_band_bias"]
    assert no_segment_config["product_key_value_band_bias"] == candidate_config["product_key_value_band_bias"]
    assert no_segment_config["allowed_segments"] == {
        ("New Build", "Apartments"),
        ("Refurbishment", "Commercial"),
    }

    assert product_led_config["segment_bias"] == {}
    assert product_led_config["value_band_bias"] == {}
    assert product_led_config["product_key_bias"] == candidate_config["product_key_bias"]
    assert product_led_config["product_key_value_band_bias"] == candidate_config["product_key_value_band_bias"]
    assert product_led_config["allowed_segments"] == {
        ("Refurbishment", "Commercial"),
    }

    assert product_key_only_config["segment_bias"] == {}
    assert product_key_only_config["value_band_bias"] == {}
    assert product_key_only_config["product_key_value_band_bias"] == {}
    assert product_key_only_config["product_key_bias"] == candidate_config["product_key_bias"]
    assert product_key_only_config["allowed_segments"] == {
        ("Refurbishment", "Commercial"),
    }

    assert value_band_only_config["segment_bias"] == {}
    assert value_band_only_config["product_key_bias"] == {}
    assert value_band_only_config["product_key_value_band_bias"] == {}
    assert value_band_only_config["value_band_bias"] == candidate_config["value_band_bias"]
    assert value_band_only_config["allowed_segments"] == {
        ("New Build", "Apartments"),
    }


def test_analyze_tuned_post_tail_value_band_only_mode_disables_product_layers():
    class StubService:
        def analyze_project(self, _project):
            return {
                "allowed_segments": set(numeric_analyzer_module.GESTATION_POST_TAIL_BIAS_ALLOWED_SEGMENTS),
                "segment_bias": dict(numeric_analyzer_module.GESTATION_POST_TAIL_BIAS_BY_SEGMENT),
                "product_key_enabled": numeric_analyzer_module.GESTATION_POST_TAIL_BIAS_PRODUCT_KEY_ENABLED,
                "product_key_bias": dict(numeric_analyzer_module.GESTATION_POST_TAIL_BIAS_BY_PRODUCT_KEY_SEGMENT),
                "value_band_enabled": numeric_analyzer_module.GESTATION_POST_TAIL_BIAS_VALUE_BAND_ENABLED,
                "value_band_bias": dict(numeric_analyzer_module.GESTATION_POST_TAIL_BIAS_BY_VALUE_BAND_SEGMENT),
                "product_key_value_band_enabled": numeric_analyzer_module.GESTATION_POST_TAIL_BIAS_PRODUCT_KEY_VALUE_BAND_ENABLED,
                "product_key_value_band_bias": dict(
                    numeric_analyzer_module.GESTATION_POST_TAIL_BIAS_BY_PRODUCT_KEY_VALUE_BAND_SEGMENT
                ),
            }

    candidate_config = apply_post_tail_ablation_mode(
        {
            "global_days": 0,
            "global_fallback_days": 0,
            "allowed_segments": {
                ("New Build", "Apartments"),
                ("Refurbishment", "Commercial"),
            },
            "segment_bias": {
                ("New Build", "Apartments"): 30,
            },
            "segment_n": {
                ("New Build", "Apartments"): 12,
            },
            "product_key_min_n": 4,
            "product_key_bias": {
                ("Refurbishment", "Commercial", "pir_tissue"): 60,
            },
            "product_key_n": {
                ("Refurbishment", "Commercial", "pir_tissue"): 12,
            },
            "value_band_min_n": 6,
            "value_band_bias": {
                ("New Build", "Apartments", "Small (<15k)"): 25,
            },
            "value_band_n": {
                ("New Build", "Apartments", "Small (<15k)"): 11,
            },
            "product_key_value_band_min_n": 5,
            "product_key_value_band_bias": {
                ("Refurbishment", "Commercial", "pir_tissue", "Small (<15k)"): 55,
            },
            "product_key_value_band_n": {
                ("Refurbishment", "Commercial", "pir_tissue", "Small (<15k)"): 9,
            },
        },
        "value-band-only",
    )

    result = analyze_tuned_post_tail(StubService(), {"monday_id": "1"}, candidate_config)

    assert result["allowed_segments"] == {
        ("New Build", "Apartments"),
    }
    assert result["segment_bias"] == {}
    assert result["product_key_enabled"] is False
    assert result["product_key_bias"] == {}
    assert result["value_band_enabled"] is True
    assert result["value_band_bias"] == {
        ("New Build", "Apartments", "Small (<15k)"): 25,
    }
    assert result["product_key_value_band_enabled"] is False
    assert result["product_key_value_band_bias"] == {}


def test_analyze_tuned_post_tail_product_key_only_mode_disables_product_key_value_band():
    class StubService:
        def analyze_project(self, _project):
            return {
                "allowed_segments": set(numeric_analyzer_module.GESTATION_POST_TAIL_BIAS_ALLOWED_SEGMENTS),
                "segment_bias": dict(numeric_analyzer_module.GESTATION_POST_TAIL_BIAS_BY_SEGMENT),
                "product_key_enabled": numeric_analyzer_module.GESTATION_POST_TAIL_BIAS_PRODUCT_KEY_ENABLED,
                "product_key_bias": dict(numeric_analyzer_module.GESTATION_POST_TAIL_BIAS_BY_PRODUCT_KEY_SEGMENT),
                "value_band_enabled": numeric_analyzer_module.GESTATION_POST_TAIL_BIAS_VALUE_BAND_ENABLED,
                "value_band_bias": dict(numeric_analyzer_module.GESTATION_POST_TAIL_BIAS_BY_VALUE_BAND_SEGMENT),
                "product_key_value_band_enabled": numeric_analyzer_module.GESTATION_POST_TAIL_BIAS_PRODUCT_KEY_VALUE_BAND_ENABLED,
                "product_key_value_band_bias": dict(
                    numeric_analyzer_module.GESTATION_POST_TAIL_BIAS_BY_PRODUCT_KEY_VALUE_BAND_SEGMENT
                ),
            }

    candidate_config = apply_post_tail_ablation_mode(
        {
            "global_days": 0,
            "global_fallback_days": 0,
            "allowed_segments": {
                ("New Build", "Apartments"),
                ("Refurbishment", "Commercial"),
            },
            "segment_bias": {
                ("New Build", "Apartments"): 30,
            },
            "segment_n": {
                ("New Build", "Apartments"): 12,
            },
            "product_key_min_n": 4,
            "product_key_bias": {
                ("Refurbishment", "Commercial", "pir_tissue"): 60,
            },
            "product_key_n": {
                ("Refurbishment", "Commercial", "pir_tissue"): 12,
            },
            "value_band_min_n": 6,
            "value_band_bias": {
                ("New Build", "Apartments", "Small (<15k)"): 25,
            },
            "value_band_n": {
                ("New Build", "Apartments", "Small (<15k)"): 11,
            },
            "product_key_value_band_min_n": 5,
            "product_key_value_band_bias": {
                ("Refurbishment", "Commercial", "pir_tissue", "Small (<15k)"): 55,
            },
            "product_key_value_band_n": {
                ("Refurbishment", "Commercial", "pir_tissue", "Small (<15k)"): 9,
            },
        },
        "product-key-only",
    )

    result = analyze_tuned_post_tail(StubService(), {"monday_id": "1"}, candidate_config)

    assert result["allowed_segments"] == {
        ("Refurbishment", "Commercial"),
    }
    assert result["segment_bias"] == {}
    assert result["product_key_enabled"] is True
    assert result["product_key_bias"] == {
        ("Refurbishment", "Commercial", "pir_tissue"): 60,
    }
    assert result["value_band_enabled"] is False
    assert result["value_band_bias"] == {}
    assert result["product_key_value_band_enabled"] is False
    assert result["product_key_value_band_bias"] == {}


if __name__ == "__main__":
    main()