"""
Numeric baseline engine for deterministic calculations.
"""

import numpy as np
import pandas as pd
from typing import Dict, Tuple, Optional, List, Any
from scipy import stats
import logging

# Handle both relative and absolute imports
try:
    from .models import (
        ProjectFeatures,
        NumericPredictions,
        SegmentStatistics,
        StatusCategory,
        PipelineStage
    )
except ImportError:
    from models import (
        ProjectFeatures,
        NumericPredictions,
        SegmentStatistics,
        StatusCategory,
        PipelineStage
    )

try:
    from .. import config as _config  # type: ignore
except ImportError:  # pragma: no cover - fallback for direct execution
    try:
        import src.config as _config  # type: ignore
    except ImportError:  # pragma: no cover - ultimate fallback
        _config = None  # type: ignore

TIME_WEIGHTING_ENABLED = getattr(_config, "TIME_WEIGHTING_ENABLED", True)
TIME_WEIGHTING_HALF_LIFE_DAYS = getattr(_config, "TIME_WEIGHTING_HALF_LIFE_DAYS", 730)

GESTATION_BIAS_CORRECTION_ENABLED = getattr(_config, "GESTATION_BIAS_CORRECTION_ENABLED", False)
GESTATION_BIAS_GLOBAL_DAYS = int(getattr(_config, "GESTATION_BIAS_GLOBAL_DAYS", 0))
GESTATION_BIAS_GLOBAL_FALLBACK_DAYS = int(
    getattr(_config, "GESTATION_BIAS_GLOBAL_FALLBACK_DAYS", GESTATION_BIAS_GLOBAL_DAYS)
)

GESTATION_BIAS_BY_SEGMENT = getattr(_config, "GESTATION_BIAS_BY_SEGMENT", {})
GESTATION_BIAS_SEGMENT_SAMPLE_SIZES = getattr(_config, "GESTATION_BIAS_SEGMENT_SAMPLE_SIZES", {})
GESTATION_BIAS_MIN_SEGMENT_N = int(getattr(_config, "GESTATION_BIAS_MIN_SEGMENT_N", 20))
GESTATION_BIAS_DAMPING_FACTOR = float(getattr(_config, "GESTATION_BIAS_DAMPING_FACTOR", 0.5))
GESTATION_BIAS_MAX_ABS_DAYS = int(getattr(_config, "GESTATION_BIAS_MAX_ABS_DAYS", 60))

# New tuning knobs for continuous, support-aware shrinkage.
_DEFAULT_GESTATION_BIAS_TIER_WEIGHTS = {
    0: 0.50,  # account + type + category + product
    1: 0.65,  # account + type + category
    2: 0.85,  # account + type
    3: 1.00,  # type + category
    4: 0.90,  # type
    5: 0.75,  # global
}

GESTATION_BIAS_SUPPORT_SHRINKAGE = float(
    getattr(
        _config,
        "GESTATION_BIAS_SUPPORT_SHRINKAGE",
        float(GESTATION_BIAS_MIN_SEGMENT_N or 20),
    )
)

_raw_tier_weights = getattr(
    _config,
    "GESTATION_BIAS_TIER_WEIGHTS",
    _DEFAULT_GESTATION_BIAS_TIER_WEIGHTS,
)

try:
    GESTATION_BIAS_TIER_WEIGHTS = {
        int(k): float(v) for k, v in dict(_raw_tier_weights).items()
    }
except Exception:
    GESTATION_BIAS_TIER_WEIGHTS = _DEFAULT_GESTATION_BIAS_TIER_WEIGHTS.copy()

GESTATION_OUTLIER_FILTERING_ENABLED = getattr(_config, "GESTATION_OUTLIER_FILTERING_ENABLED", True)
GESTATION_OUTLIER_IQR_MULTIPLIER = float(getattr(_config, "GESTATION_OUTLIER_IQR_MULTIPLIER", 1.5))
GESTATION_OUTLIER_MIN_SAMPLES = int(getattr(_config, "GESTATION_OUTLIER_MIN_SAMPLES", 8))

GESTATION_VARIANCE_AWARE_DAMPING_ENABLED = getattr(_config, "GESTATION_VARIANCE_AWARE_DAMPING_ENABLED", True)
GESTATION_VARIANCE_DAMPING_FLOOR = float(getattr(_config, "GESTATION_VARIANCE_DAMPING_FLOOR", 0.20))

GESTATION_BIAS_VALUE_BAND_ENABLED = getattr(_config, "GESTATION_BIAS_VALUE_BAND_ENABLED", False)
GESTATION_BIAS_VALUE_BAND_MIN_N = int(getattr(_config, "GESTATION_BIAS_VALUE_BAND_MIN_N", 15))
GESTATION_BIAS_VALUE_BAND_SHRINKAGE = float(getattr(_config, "GESTATION_BIAS_VALUE_BAND_SHRINKAGE", 20.0))
GESTATION_BIAS_BY_VALUE_BAND_SEGMENT = getattr(_config, "GESTATION_BIAS_BY_VALUE_BAND_SEGMENT", {})
GESTATION_BIAS_VALUE_BAND_SEGMENT_SAMPLE_SIZES = getattr(_config, "GESTATION_BIAS_VALUE_BAND_SEGMENT_SAMPLE_SIZES", {})

logger = logging.getLogger(__name__)


class NumericBaseline:
    """
    Compute deterministic baselines before LLM processing
    """
    
    def __init__(self, lookback_days: Optional[int] = None):
        self.smoothing_k = 1.0  # Laplace smoothing parameter
        self.lookback_days = lookback_days or 1825 # Default to 5 years
        self.time_weighting_enabled = TIME_WEIGHTING_ENABLED
        self.time_weighting_half_life_days = TIME_WEIGHTING_HALF_LIFE_DAYS
        logger.info(f"NumericBaseline initialized with {self.lookback_days} days ({self.lookback_days/365:.1f} years) lookback")

    def _piecewise_scale(self, x: Optional[float], points: List[Tuple[float, float]]) -> float:
        """
        Linear interpolation through (x,y) breakpoints. Returns y in [0,1].
        """
        if x is None:
            return 0.5
        if not points:
            return 0.5
        # Ensure points sorted by x
        pts = sorted(points, key=lambda p: p[0])
        if x <= pts[0][0]:
            return pts[0][1]
        for i in range(1, len(pts)):
            x0, y0 = pts[i - 1]
            x1, y1 = pts[i]
            if x <= x1:
                if x1 == x0:
                    return y1
                t = (x - x0) / (x1 - x0)
                return float(y0 + t * (y1 - y0))
        return float(pts[-1][1])

    def _calculate_time_weights(
        self,
        dates: pd.Series,
        half_life_days: float = 730.0
    ) -> pd.Series:
        """Generate exponential decay weights that favour recent projects.

        Rows with missing or invalid ``date_created`` values are assigned the
        minimum observed weight so they still contribute while preventing NaNs
        from propagating through weighted statistics.
        """
        if dates is None or dates.empty:
            return pd.Series(dtype=float)

        half_life_days = max(float(half_life_days or 1.0), 1.0)

        dt_series = pd.to_datetime(dates, errors='coerce')
        valid_mask = dt_series.notna()

        if not valid_mask.any():
            return pd.Series(1.0, index=dates.index, dtype=float)

        reference_date = pd.Timestamp.now(tz=dt_series.dt.tz)
        days_ago = (reference_date - dt_series[valid_mask]).dt.total_seconds() / 86400.0
        days_ago = np.maximum(days_ago, 0.0)

        valid_weights = np.power(0.5, days_ago / half_life_days)
        weights = pd.Series(0.0, index=dates.index, dtype=float)
        weights.loc[valid_mask] = valid_weights

        if (~valid_mask).any():
            min_weight = float(valid_weights.min()) if len(valid_weights) else 1.0
            weights.loc[~valid_mask] = min_weight

        total_weight = float(weights.sum())
        if total_weight <= 0:
            weights.loc[:] = 1.0
        else:
            weights *= len(weights) / total_weight

        return weights

    def _effective_sample_size(self, weights: Optional[pd.Series]) -> float:
        """Kish effective sample size for weighted rows."""
        if weights is None or weights.empty:
            return 0.0

        arr = pd.to_numeric(weights, errors="coerce").fillna(0.0).to_numpy(dtype=float)
        total = float(arr.sum())
        denom = float(np.square(arr).sum())

        if total <= 0 or denom <= 0:
            return 0.0

        return float((total ** 2) / denom)

    def _gestation_support_stats(
        self,
        segment_data: pd.DataFrame,
        apply_time_weighting: Optional[bool] = None,
        half_life_days: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Return valid gestation values plus raw and effective support.

        When segment outlier filtering is enabled and the segment has enough
        raw samples, values beyond per-segment Tukey fences are excluded
        *before* time-weighting and median computation.
        """
        empty_result = {
            "values": pd.Series(dtype=float),
            "weights": None,
            "count": 0,
            "effective_n": 0.0,
            "weighting": "unweighted",
            "outliers_removed": 0,
        }

        if segment_data is None or segment_data.empty or "gestation_period" not in segment_data.columns:
            return empty_result

        gestation_values = segment_data["gestation_period"].dropna()
        gestation_values = gestation_values[
            (gestation_values > 0) & (gestation_values < 1460)
        ]

        if gestation_values.empty:
            return {**empty_result, "values": gestation_values}

        outliers_removed = 0

        if (
            GESTATION_OUTLIER_FILTERING_ENABLED
            and len(gestation_values) >= GESTATION_OUTLIER_MIN_SAMPLES
        ):
            q25 = float(gestation_values.quantile(0.25))
            q75 = float(gestation_values.quantile(0.75))
            iqr = q75 - q25

            if iqr > 0:
                k_iqr = GESTATION_OUTLIER_IQR_MULTIPLIER
                lower_fence = max(1.0, q25 - k_iqr * iqr)
                upper_fence = q75 + k_iqr * iqr

                pre_count = len(gestation_values)
                gestation_values = gestation_values[
                    (gestation_values >= lower_fence) & (gestation_values <= upper_fence)
                ]
                outliers_removed = pre_count - len(gestation_values)

                if outliers_removed > 0:
                    logger.info(
                        "Segment outlier filter: removed %d/%d values outside [%.0f, %.0f] "
                        "(IQR=%.0f, k=%.1f)",
                        outliers_removed, pre_count, lower_fence, upper_fence, iqr, k_iqr,
                    )

        if gestation_values.empty:
            return {**empty_result, "outliers_removed": outliers_removed}

        use_weighting = (
            self.time_weighting_enabled
            if apply_time_weighting is None
            else bool(apply_time_weighting)
        )
        weight_half_life = (
            half_life_days if half_life_days is not None else self.time_weighting_half_life_days
        )

        weights = None
        weighting_note = "unweighted"

        if use_weighting and "date_created" in segment_data.columns:
            weights = self._calculate_time_weights(
                segment_data.loc[gestation_values.index, "date_created"],
                half_life_days=weight_half_life,
            )
            weights = weights.reindex(gestation_values.index).fillna(0.0)

            if weights.empty or float(weights.sum()) <= 0:
                weights = None
            else:
                weighting_note = f"time_weighted_half_life_{int(weight_half_life)}d"

        effective_n = (
            self._effective_sample_size(weights)
            if weights is not None
            else float(len(gestation_values))
        )

        return {
            "values": gestation_values,
            "weights": weights,
            "count": int(len(gestation_values)),
            "effective_n": float(effective_n),
            "weighting": weighting_note,
            "outliers_removed": outliers_removed,
        }

    def _compute_gestation_bias_adjustment(
        self,
        project: ProjectFeatures,
        support_data: pd.DataFrame,
        backoff_tier: int,
        gest_stats: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Compute a shrunk, tier-aware gestation bias adjustment.

        Key behaviours:
        - continuous shrinkage instead of binary segment/global switching
        - uses actual effective support from the current baseline data
        - uses GESTATION_BIAS_GLOBAL_DAYS as the primary shrinkage anchor
        - reserves GESTATION_BIAS_GLOBAL_FALLBACK_DAYS for global-only or zero-support cases
        - attenuates broad type/category bias when the baseline came from a narrower tier
        - optionally refines with (type, category, value_band) when support is sufficient
        - scales damping inversely with segment CV when variance-aware damping is enabled
        """
        seg_key = (
            getattr(project, "type", None),
            getattr(project, "category", None),
        )
        seg_bias = GESTATION_BIAS_BY_SEGMENT.get(seg_key)
        config_segment_n = int(GESTATION_BIAS_SEGMENT_SAMPLE_SIZES.get(seg_key, 0))

        # Primary shrinkage anchor for any row with usable segment support.
        global_primary_bias = GESTATION_BIAS_GLOBAL_DAYS
        if global_primary_bias is None:
            global_primary_bias = GESTATION_BIAS_GLOBAL_FALLBACK_DAYS
        global_primary_bias = int(global_primary_bias or 0)

        # Conservative fallback reserved for global-only or truly zero-support cases.
        global_fallback_bias = GESTATION_BIAS_GLOBAL_FALLBACK_DAYS
        if global_fallback_bias is None:
            global_fallback_bias = global_primary_bias
        global_fallback_bias = int(global_fallback_bias or 0)

        actual_support = 0.0
        if gest_stats:
            actual_support = float(
                gest_stats.get("effective_n")
                or gest_stats.get("count")
                or 0.0
            )
        if actual_support <= 0 and support_data is not None and not support_data.empty:
            actual_support = float(
                self._gestation_support_stats(support_data).get("effective_n", 0.0)
            )

        support_signal = actual_support if actual_support > 0 else float(config_segment_n)
        shrinkage = max(1.0, float(GESTATION_BIAS_SUPPORT_SHRINKAGE))

        if seg_bias is None:
            segment_trust = 0.0
            global_bias = float(global_fallback_bias)
            blended_bias = global_bias
            source = "global_only"
        elif support_signal <= 0:
            segment_trust = 0.0
            global_bias = float(global_fallback_bias)
            blended_bias = global_bias
            source = "segment_zero_support"
        else:
            global_bias = float(global_primary_bias)
            segment_trust = support_signal / (support_signal + shrinkage)
            blended_bias = (
                segment_trust * float(seg_bias)
                + (1.0 - segment_trust) * global_bias
            )
            source = "segment_shrunk" if actual_support > 0 else "segment_table"

        # --- Value-band refinement (optional second-pass) ---
        vb_key = None
        vb_support = 0.0
        vb_trust = None
        if GESTATION_BIAS_VALUE_BAND_ENABLED:
            vb = getattr(project, "value_band", None)
            if vb:
                vb_key = (seg_key[0], seg_key[1], vb)  # (type, category, value_band)
                vb_bias = GESTATION_BIAS_BY_VALUE_BAND_SEGMENT.get(vb_key)

                if vb_bias is not None:
                    # Compute value-band support from the live segment data
                    if support_data is not None and not support_data.empty:
                        vb_col = support_data.get("value_band")
                        if vb_col is not None:
                            vb_mask = vb_col == vb
                            if vb_mask.any():
                                vb_slice = support_data.loc[vb_mask]
                                vb_stats = self._gestation_support_stats(vb_slice)
                                vb_support = float(
                                    vb_stats.get("effective_n") or vb_stats.get("count") or 0.0
                                )

                    # Fall back to config table if live support is zero
                    if vb_support <= 0:
                        vb_support = float(
                            GESTATION_BIAS_VALUE_BAND_SEGMENT_SAMPLE_SIZES.get(vb_key, 0)
                        )

                    vb_min_n = float(GESTATION_BIAS_VALUE_BAND_MIN_N)
                    vb_shrinkage = max(1.0, float(GESTATION_BIAS_VALUE_BAND_SHRINKAGE))

                    if vb_support >= vb_min_n:
                        # Enough events — shrink toward the coarser bias already selected above.
                        vb_trust = vb_support / (vb_support + vb_shrinkage)
                        blended_bias = (
                            vb_trust * float(vb_bias)
                            + (1.0 - vb_trust) * blended_bias
                        )
                        source = "value_band_shrunk"
                        logger.info(
                            "Value-band refinement applied: vb_key=%s, vb_bias=%d, "
                            "vb_support=%.1f, vb_trust=%.2f, blended_bias=%.1f",
                            vb_key, vb_bias, vb_support, vb_trust, blended_bias,
                        )
                    else:
                        logger.info(
                            "Value-band segment %s rejected: support=%.1f < min_n=%d; "
                            "keeping (type,category) bias=%.1f",
                            vb_key, vb_support, int(vb_min_n), blended_bias,
                        )

        tier_weight = float(GESTATION_BIAS_TIER_WEIGHTS.get(int(backoff_tier), 1.0))
        tier_adjusted_bias = blended_bias * tier_weight

        raw_bias_days = int(round(tier_adjusted_bias))

        k = max(0.0, float(GESTATION_BIAS_DAMPING_FACTOR))
        segment_cv = None
        variance_damping = 1.0
        if GESTATION_VARIANCE_AWARE_DAMPING_ENABLED and gest_stats:
            mean_val = gest_stats.get("mean", 0)
            std_val = gest_stats.get("std", 0)
            if mean_val and mean_val > 0:
                segment_cv = float(std_val) / float(mean_val)
                floor = max(0.0, float(GESTATION_VARIANCE_DAMPING_FLOOR))
                variance_damping = max(floor, 1.0 - segment_cv)
                k *= variance_damping

        adjustment_days = int(round(raw_bias_days * k))

        cap_days = abs(int(GESTATION_BIAS_MAX_ABS_DAYS)) if GESTATION_BIAS_MAX_ABS_DAYS else 0
        if cap_days > 0:
            adjustment_days = max(-cap_days, min(cap_days, adjustment_days))

        return {
            "segment_key": seg_key,
            "segment_bias_days": int(seg_bias) if seg_bias is not None else None,
            "global_bias_days": int(global_bias),
            "global_primary_bias_days": int(global_primary_bias),
            "global_fallback_bias_days": int(global_fallback_bias),
            "config_segment_n": config_segment_n,
            "actual_support": float(actual_support),
            "segment_trust": float(segment_trust),
            "tier_weight": float(tier_weight),
            "raw_bias_days": int(raw_bias_days),
            "adjustment_days": int(adjustment_days),
            "cap_days": int(cap_days),
            "source": source,
            "segment_cv": round(segment_cv, 4) if segment_cv is not None else None,
            "variance_damping": round(variance_damping, 4),
            "value_band_key": vb_key,
            "value_band_support": round(vb_support, 2) if vb_key else None,
            "value_band_trust": round(vb_trust, 4) if vb_trust is not None else None,
        }
    
    def _compute_global_closed_prior(self, df: pd.DataFrame) -> Tuple[Optional[float], int]:
        if df is None or df.empty:
            return None, 0
        if 'pipeline_stage' in df.columns:
            won_closed = (df['pipeline_stage'] == PipelineStage.WON_CLOSED.value).sum()
            lost = (df['pipeline_stage'] == PipelineStage.LOST.value).sum()
            total = int(won_closed + lost)
            if total == 0:
                return None, 0
            return float(won_closed) / total, total
        if 'status_category' in df.columns:
            # Fallback if pipeline_stage not available
            won = (df['status_category'] == StatusCategory.WON.value).sum()
            lost = (df['status_category'] == StatusCategory.LOST.value).sum()
            total = int(won + lost)
            if total == 0:
                return None, 0
            return float(won) / total, total
        return None, 0

    def _beta_smoothed_rate(self, won: float, lost: float, prior_rate: Optional[float], prior_strength: Optional[int]) -> float:
        if prior_rate is not None and prior_strength and prior_strength > 0:
            alpha = prior_rate * prior_strength
            beta = (1.0 - prior_rate) * prior_strength
            return float(won + alpha) / float((won + lost) + alpha + beta)
        # Fallback to Laplace (Beta(1,1))
        k = self.smoothing_k
        return float(won + k) / float((won + lost) + 2 * k)

    def calculate_gestation_baseline(
        self,
        segment_data: pd.DataFrame,
        apply_time_weighting: Optional[bool] = None,
        half_life_days: Optional[float] = None
    ) -> Tuple[Optional[int], Dict[str, Any]]:
        """
        Calculate expected gestation period using median with IQR.
        Uses effective sample size for confidence when time weighting is active.
        Returns: (expected_days, statistics)
        """
        if segment_data.empty:
            logger.warning("Empty segment data for gestation calculation")
            return None, {"confidence": 0, "sample_size": 0, "effective_n": 0.0}

        support = self._gestation_support_stats(
            segment_data,
            apply_time_weighting=apply_time_weighting,
            half_life_days=half_life_days,
        )
        gestation_values = support["values"]
        weights = support["weights"]
        weighting_note = support["weighting"]
        effective_n = float(support["effective_n"])
        outliers_removed = int(support.get("outliers_removed", 0))

        if len(gestation_values) < 3:
            logger.warning(f"Insufficient gestation data: {len(gestation_values)} samples")
            return None, {
                "confidence": 0,
                "sample_size": len(gestation_values),
                "effective_n": round(effective_n, 2),
            }

        mean_val = float(gestation_values.mean())
        std_val = float(gestation_values.std())
        median_val = float(gestation_values.median())
        p25 = float(gestation_values.quantile(0.25))
        p75 = float(gestation_values.quantile(0.75))
        p10 = float(gestation_values.quantile(0.10))
        p90 = float(gestation_values.quantile(0.90))

        if weights is not None:
            sorted_idx = gestation_values.sort_values().index
            sorted_values = gestation_values.loc[sorted_idx].to_numpy(dtype=float)
            sorted_weights = weights.loc[sorted_idx].to_numpy(dtype=float)
            cumulative = np.cumsum(sorted_weights)
            if len(cumulative) and cumulative[-1] > 0:
                cumulative /= cumulative[-1]
                median_val = float(np.interp(0.5, cumulative, sorted_values))
                p25 = float(np.interp(0.25, cumulative, sorted_values))
                p75 = float(np.interp(0.75, cumulative, sorted_values))
                p10 = float(np.interp(0.10, cumulative, sorted_values))
                p90 = float(np.interp(0.90, cumulative, sorted_values))
                mean_val = float(np.average(sorted_values, weights=sorted_weights))
                variance = float(
                    np.average((sorted_values - mean_val) ** 2, weights=sorted_weights)
                )
                std_val = float(np.sqrt(max(variance, 0.0)))

        iqr = p75 - p25

        # Effective sample size gives a fairer confidence signal under weighting.
        sample_confidence = min(effective_n / 30.0, 1.0)

        cv = std_val / mean_val if mean_val > 0 else 1.0

        spread_confidence = max(0.0, 1.0 - cv)

        overall_confidence = sample_confidence * 0.7 + spread_confidence * 0.3

        statistics = {
            "median": int(round(median_val)),
            "p25": int(round(p25)),
            "p75": int(round(p75)),
            "p10": max(1, int(round(p10))),
            "p90": int(round(p90)),
            "iqr": int(round(iqr)),
            "mean": int(round(mean_val)),
            "std": int(round(std_val)),
            "cv": round(cv, 4),
            "count": int(len(gestation_values)),
            "effective_n": round(effective_n, 2),
            "confidence": round(overall_confidence, 2),
            "weighting": weighting_note,
            "outliers_removed": outliers_removed,
            "prediction_interval": {
                "p10": max(1, int(round(p10))),
                "p50": int(round(median_val)),
                "p90": int(round(p90)),
            },
        }

        logger.info(
            "Gestation baseline: %d days (n=%d, n_eff=%.1f, cv=%.2f, "
            "PI=[%d, %d, %d], outliers_removed=%d, %s)",
            int(round(median_val)),
            len(gestation_values),
            effective_n,
            cv,
            statistics["prediction_interval"]["p10"],
            statistics["prediction_interval"]["p50"],
            statistics["prediction_interval"]["p90"],
            outliers_removed,
            weighting_note,
        )

        return int(round(median_val)), statistics
    
    def calculate_conversion_rate(
        self,
        segment_data: pd.DataFrame,
        method: str = 'inclusive',
        prior_rate: Optional[float] = None,
        prior_strength: Optional[int] = None,
        apply_time_weighting: Optional[bool] = None,
        half_life_days: Optional[float] = None
    ) -> Tuple[float, Dict[str, Any]]:
        if segment_data.empty:
            return 0.5, {'confidence': 0}

        total = len(segment_data)
        use_pipeline = 'pipeline_stage' in segment_data.columns

        if use_pipeline:
            stages = segment_data['pipeline_stage'].fillna('')
            won_mask = stages == PipelineStage.WON_CLOSED.value
            lost_mask = stages == PipelineStage.LOST.value
        else:
            # Fallback: treat 'Won' as closed-won if pipeline_stage missing
            sc = segment_data['status_category'] if 'status_category' in segment_data.columns else pd.Series(index=segment_data.index, dtype=object)
            won_mask = sc == StatusCategory.WON.value
            lost_mask = sc == StatusCategory.LOST.value

        open_mask = ~(won_mask | lost_mask)

        won_closed = int(won_mask.sum())
        lost = int(lost_mask.sum())
        open_count = int(open_mask.sum())

        use_weighting = self.time_weighting_enabled if apply_time_weighting is None else bool(apply_time_weighting)
        weight_half_life = half_life_days if half_life_days is not None else self.time_weighting_half_life_days

        weights = None
        weighting_note = "unweighted"
        if use_weighting and 'date_created' in segment_data.columns:
            weights = self._calculate_time_weights(
                segment_data['date_created'],
                half_life_days=weight_half_life
            )
            weights = weights.reindex(segment_data.index).fillna(0.0)
            if weights.empty or float(weights.sum()) <= 0:
                weights = None
            else:
                weighting_note = f"time_weighted_half_life_{int(weight_half_life)}d"

        if weights is not None:
            won_closed_weighted = float(weights[won_mask].sum())
            lost_weighted = float(weights[lost_mask].sum())
            open_weighted = float(weights[open_mask].sum())
            total_weighted = float(weights.sum())
        else:
            won_closed_weighted = float(won_closed)
            lost_weighted = float(lost)
            open_weighted = float(open_count)
            total_weighted = float(total)

        if method == 'closed_only':
            total_closed = won_closed + lost
            if total_closed == 0:
                return None, {
                    'confidence': 0,
                    'wins': won_closed,
                    'losses': lost,
                    'open': open_count,
                    'note': 'No closed projects in segment',
                    'method': 'closed_only',
                    'weighting': weighting_note
                }
            total_closed_weighted = won_closed_weighted + lost_weighted
            if total_closed_weighted <= 0:
                total_closed_weighted = float(total_closed)
            rate = self._beta_smoothed_rate(won_closed_weighted, lost_weighted, prior_rate, prior_strength)
            confidence = min(total_closed_weighted / 100, 1.0)
            statistics = {
                'wins': won_closed,
                'losses': lost,
                'open': open_count,
                'total': int(total_closed),
                'raw_rate': (won_closed_weighted / total_closed_weighted) if total_closed_weighted > 0 else 0.0,
                'smoothed_rate': rate,
                'confidence': round(confidence, 2),
                'method': 'closed_only',
                'wins_weighted': round(won_closed_weighted, 2),
                'losses_weighted': round(lost_weighted, 2),
                'total_weighted': round(total_closed_weighted, 2),
                'weighting': weighting_note
            }
            return rate, statistics
        else:
            # Inclusive: won_closed / (won_closed + lost + open)
            k = self.smoothing_k
            denominator = total_weighted + 2 * k
            rate = float(won_closed_weighted + k) / float(denominator) if denominator > 0 else 0.5
            confidence = min(total_weighted / 100, 1.0)
            statistics = {
                'wins': won_closed,
                'losses': lost,
                'open': open_count,
                'total': int(total),
                'raw_rate': (won_closed_weighted / total_weighted) if total_weighted > 0 else 0.0,
                'smoothed_rate': rate,
                'confidence': round(confidence, 2),
                'method': 'inclusive_closed_wins',
                'wins_weighted': round(won_closed_weighted, 2),
                'losses_weighted': round(lost_weighted, 2),
                'open_weighted': round(open_weighted, 2),
                'total_weighted': round(total_weighted, 2),
                'weighting': weighting_note
            }
            return rate, statistics
    
    def calculate_rating_score(
        self, 
        project_metrics: Dict[str, Any],
        segment_data: pd.DataFrame = None
    ) -> Tuple[int, Dict[str, Any]]:
        """
        Deterministic rating score (1-100) based on weighted factors:
          - conversion_rate (anchored to 23% benchmark)
          - expected_gestation_days (shorter is better)
          - new_enquiry_value (higher is better)
          - small modifiers for account/product performance
        Returns: (score, components)
        """
        # Inputs
        cr = float(project_metrics.get('conversion_rate', 0.5) or 0.5)
        expected_gestation = project_metrics.get('expected_gestation_days')
        new_value = project_metrics.get('new_enquiry_value', 0) or 0
        value_band = project_metrics.get('value_band')

        # UPDATED: More generous conversion rate scaling aligned with stakeholder expectations
        # 23% is average, 35-50% is very good, should score high
        conv_points = [
            (0.00, 0.10),   # 0% → 10/100
            (0.10, 0.30),   # 10% → 30/100
            (0.20, 0.50),   # 20% → 50/100 (slightly below average)
            (0.23, 0.65),   # 23% → 65/100 (average benchmark)
            (0.30, 0.80),   # 30% → 80/100 (above average)
            (0.35, 0.88),   # 35% → 88/100 (very good start)
            (0.45, 0.95),   # 45% → 95/100 (excellent)
            (0.55, 1.00),   # 55%+ → 100/100 (exceptional)
        ]
        cr_score = self._piecewise_scale(cr, conv_points)

        # Segment-derived stats (optional)
        gest_p25 = gest_p75 = None
        if segment_data is not None and not segment_data.empty:
            try:
                gs = segment_data['gestation_period'].dropna()
                gs = gs[(gs > 0) & (gs < 1000)]
                if not gs.empty:
                    gest_p25 = float(gs.quantile(0.25))
                    gest_p75 = float(gs.quantile(0.75))
            except Exception:
                pass

        # UPDATED: More aggressive gestation scoring - shorter periods get higher scores
        def scale_gestation(days: Optional[float]) -> float:
            if days is None:
                # Fallback heuristic
                return 0.6
            if gest_p25 is not None and gest_p75 is not None and gest_p75 > gest_p25:
                if days <= gest_p25:
                    return 1.0
                if days >= gest_p75:
                    return 0.15  # Reduced from 0.2
                t = (days - gest_p25) / (gest_p75 - gest_p25)
                return 1.0 - 0.85 * float(max(0.0, min(1.0, t)))  # Increased penalty range
            # Heuristic bins - more generous for short gestations
            if days <= 30:
                return 1.0
            if days <= 60:
                return 0.90
            if days <= 90:
                return 0.75
            if days <= 120:
                return 0.60
            if days <= 180:
                return 0.45
            if days <= 300:
                return 0.25
            return 0.15

        gestation_score = scale_gestation(expected_gestation)

        # UPDATED: More generous value scoring with higher impact
        # Value scoring mixes absolute band strength with within-segment percentile
        band_baseline = {
            'Zero': 0.05,
            'Small (<15k)': 0.35,
            'Medium (15-40k)': 0.55,
            'Large (40-100k)': 0.75,
            'XLarge (>100k)': 0.92,
        }
        band_cap = {
            'Zero': 0.30,
            'Small (<15k)': 0.70,
            'Medium (15-40k)': 0.85,
            'Large (40-100k)': 0.95,
            'XLarge (>100k)': 1.00,
        }

        def value_score_from_segment(val: float) -> float:
            baseline = band_baseline.get(value_band, 0.5)
            cap = band_cap.get(value_band, 0.9)
            if segment_data is None or segment_data.empty:
                return baseline
            vs = segment_data['new_enquiry_value'].dropna()
            vs = vs[vs > 0]
            if vs.empty:
                return baseline
            arr = np.sort(vs.to_numpy())
            percentile = float(np.searchsorted(arr, val, side='right')) / float(len(arr))
            blended = baseline + (cap - baseline) * percentile ** 0.8
            return float(max(baseline, min(cap, blended)))

        value_score = value_score_from_segment(new_value)

        # Account/product performance (small modifiers with smoothing)
        account_score = self._calculate_account_performance(project_metrics.get('account'), segment_data) if segment_data is not None else 0.5
        product_score = self._calculate_product_performance(project_metrics.get('product_type'), segment_data) if segment_data is not None else 0.5

        # UPDATED weights to balance priorities
        weights = {
            'conversion': 0.30,
            'value': 0.43,
            'gestation': 0.18,
            'account': 0.06,
            'product': 0.03,
        }

        raw_score = (
            weights['conversion'] * cr_score +
            weights['value'] * value_score +
            weights['gestation'] * gestation_score +
            weights['account'] * account_score +
            weights['product'] * product_score
        )

        alignment_boost = 0.0
        all_weak_penalty = 0.0
        momentum_bonus = 0.0

        if value_score >= 0.80 and value_band not in ('Zero', 'Small (<15k)') and cr >= 0.35 and gestation_score >= 0.75:
            alignment_boost = 0.10
        elif value_score >= 0.70 and value_band not in ('Zero', 'Small (<15k)') and cr >= 0.30 and gestation_score >= 0.65:
            alignment_boost = 0.06
        elif value_score >= 0.60 and cr >= 0.25:
            alignment_boost = 0.04

        if cr_score <= 0.25 and value_score <= 0.30 and gestation_score <= 0.40:
            all_weak_penalty = -0.08

        if value_score >= 0.85 and value_band not in ('Zero', 'Small (<15k)') and gestation_score >= 0.75:
            momentum_bonus += 0.08
        if cr >= 0.35 and gestation_score >= 0.85:
            momentum_bonus += 0.06
        if cr >= 0.40 and value_band not in ('Zero', 'Small (<15k)'):
            momentum_bonus += 0.06
        if cr >= 0.50 and value_band not in ('Zero', 'Small (<15k)'):
            momentum_bonus += 0.05

        total_bonus = alignment_boost + momentum_bonus + all_weak_penalty
        total_bonus = max(-0.15, min(0.30, total_bonus))

        raw_score = max(0.0, min(1.0, raw_score + total_bonus))

        band_max_score = {
            'Zero': 55,
            'Small (<15k)': 65,
            'Medium (15-40k)': 85,
            'Large (40-100k)': 95,
            'XLarge (>100k)': 100,
        }
        max_score_cap = band_max_score.get(value_band)
        if max_score_cap is not None:
            raw_score = min(raw_score, max_score_cap / 100.0)

        if raw_score > 0.7:
            raw_score = 0.7 + (raw_score - 0.7) * 1.3
            raw_score = min(1.0, raw_score)

        final_score = max(1, min(100, round(raw_score * 98 + 2)))

        # Build components dictionary for transparency
        components = {
            'conversion_rate': cr,
            'conversion_score': round(cr_score, 3),
            'gestation_days': expected_gestation,
            'gestation_score': round(gestation_score, 3),
            'value': new_value,
            'value_band': value_band,
            'value_score': round(value_score, 3),
            'value_band_cap': band_max_score.get(value_band),
            'account_score': round(account_score, 3),
            'product_score': round(product_score, 3),
            'raw_weighted_score': round(raw_score - total_bonus, 3),
            'alignment_boost': round(alignment_boost, 3),
            'momentum_bonus': round(momentum_bonus, 3),
            'weak_penalty': round(all_weak_penalty, 3),
            'total_bonus': round(total_bonus, 3),
            'final_raw_score': round(raw_score, 3),
            'final_score': final_score,
            'weights': weights,
        }

        return final_score, components
    
    def _calculate_account_performance(
        self, 
        account: Optional[str], 
        segment_data: pd.DataFrame
    ) -> float:
        """Calculate win rate for specific account"""
        if not account or segment_data.empty:
            return 0.5
        
        account_data = segment_data[segment_data['account'] == account]
        if account_data.empty:
            return 0.5
        
        won = (account_data['status_category'] == StatusCategory.WON.value).sum()
        total = len(account_data)
        
        # Apply smoothing
        return (won + self.smoothing_k) / (total + 2 * self.smoothing_k)
    
    def _calculate_product_performance(
        self, 
        product: Optional[str], 
        segment_data: pd.DataFrame
    ) -> float:
        """Calculate win rate for specific product type"""
        if not product or segment_data.empty:
            return 0.5
        
        # Use product_key column if available, fall back to product_type
        col = 'product_key' if 'product_key' in segment_data.columns else 'product_type'
        product_data = segment_data[segment_data[col] == product]
        if product_data.empty:
            return 0.5
        
        won = (product_data['status_category'] == StatusCategory.WON.value).sum()
        total = len(product_data)
        
        # Apply smoothing
        return (won + self.smoothing_k) / (total + 2 * self.smoothing_k)
    
    def _calculate_value_band_performance(
        self, 
        value_band: Optional[str], 
        segment_data: pd.DataFrame
    ) -> float:
        """Calculate win rate for specific value band"""
        if not value_band or segment_data.empty:
            return 0.5
        
        band_data = segment_data[segment_data['value_band'] == value_band]
        if band_data.empty:
            return 0.5
        
        won = (band_data['status_category'] == StatusCategory.WON.value).sum()
        total = len(band_data)
        
        # Apply smoothing
        return (won + self.smoothing_k) / (total + 2 * self.smoothing_k)
    
    def create_segment_statistics(
        self, 
        segment_data: pd.DataFrame,
        segment_keys: List[str],
        backoff_tier: int,
        prior_rate: Optional[float] = None,
        prior_strength: Optional[int] = None
    ) -> SegmentStatistics:
        stats = SegmentStatistics(
            segment_keys=segment_keys,
            sample_size=len(segment_data),
            backoff_tier=backoff_tier
        )
        
        if not segment_data.empty:
            gestation_days, gest_stats = self.calculate_gestation_baseline(segment_data)
            if gestation_days:
                stats.gestation_median = gest_stats['median']
                stats.gestation_p25 = gest_stats['p25']
                stats.gestation_p75 = gest_stats['p75']
                stats.gestation_count = gest_stats['count']
            
            # Conversion statistics
            conv_rate_incl, conv_stats_incl = self.calculate_conversion_rate(segment_data, method='inclusive')
            conv_rate_closed, conv_stats_closed = self.calculate_conversion_rate(
                segment_data, method='closed_only',
                prior_rate=prior_rate, prior_strength=prior_strength
            )
            # Inclusive (closed-won / all) is the primary metric
            stats.conversion_rate = conv_rate_incl
            stats.conversion_confidence = conv_stats_incl['confidence']
            stats.inclusive_conversion_rate = conv_rate_incl
            stats.closed_conversion_rate = conv_rate_closed
            stats.wins = conv_stats_incl['wins']
            stats.losses = conv_stats_incl['losses']
            stats.open = conv_stats_incl['open']
            
            if 'new_enquiry_value' in segment_data.columns:
                stats.average_value = float(segment_data['new_enquiry_value'].mean())
        
        return stats
    
    def analyze_project(
        self,
        project: ProjectFeatures,
        historical_data: pd.DataFrame,
        segment_data: pd.DataFrame,
        segment_keys: List[str],
        backoff_tier: int,
        global_data: Optional[pd.DataFrame] = None,
        prior_strength: int = 20
    ) -> NumericPredictions:
        predictions = NumericPredictions()

        prior_rate, global_closed_n = self._compute_global_closed_prior(global_data) if global_data is not None else (None, 0)
        use_prior_rate = prior_rate if (prior_rate is not None and global_closed_n > 0) else None
        use_prior_strength = prior_strength if use_prior_rate is not None else None

        gestation_source_data = segment_data
        gestation_bias_tier = backoff_tier

        gestation_days, gest_stats = self.calculate_gestation_baseline(segment_data)

        if not gestation_days and global_data is not None and not global_data.empty:
            g2, gs2 = self.calculate_gestation_baseline(global_data)
            if g2:
                gestation_days, gest_stats = g2, gs2
                gestation_source_data = global_data
                gestation_bias_tier = 5
            else:
                gestation_days, gest_stats = (None, {"confidence": 0, "effective_n": 0.0})

        if gestation_days:
            predictions.expected_gestation_days = gestation_days
            predictions.gestation_confidence = gest_stats["confidence"]
            predictions.gestation_range = {
                "p25": gest_stats.get("p25"),
                "p75": gest_stats.get("p75"),
            }
            predictions.gestation_cv = gest_stats.get("cv")
            pi = gest_stats.get("prediction_interval")
            if pi:
                predictions.gestation_prediction_interval = {
                    "p10": pi.get("p10"),
                    "p50": pi.get("p50"),
                    "p90": pi.get("p90"),
                }

            if GESTATION_BIAS_CORRECTION_ENABLED:
                bias_meta = self._compute_gestation_bias_adjustment(
                    project=project,
                    support_data=gestation_source_data,
                    backoff_tier=gestation_bias_tier,
                    gest_stats=gest_stats,
                )
                adjustment_days = int(bias_meta["adjustment_days"])
                if adjustment_days != 0:
                    corrected = max(1, int(gestation_days) + adjustment_days)
                    logger.info(
                        (
                            "Bias correction: %d -> %d days "
                            "(segment=%s, seg_bias=%s, global=%+d, global_primary=%+d, "
                            "global_fallback=%+d, support=%.1f, config_n=%d, trust=%.2f, "
                            "tier=%d, tier_w=%.2f, raw=%+d, k=%.2f, var_damp=%.2f, "
                            "cv=%s, adj=%+d, cap=%d, source=%s)"
                        ),
                        gestation_days,
                        corrected,
                        bias_meta["segment_key"],
                        bias_meta["segment_bias_days"],
                        bias_meta["global_bias_days"],
                        bias_meta["global_primary_bias_days"],
                        bias_meta["global_fallback_bias_days"],
                        bias_meta["actual_support"],
                        bias_meta["config_segment_n"],
                        bias_meta["segment_trust"],
                        gestation_bias_tier,
                        bias_meta["tier_weight"],
                        bias_meta["raw_bias_days"],
                        GESTATION_BIAS_DAMPING_FACTOR,
                        bias_meta["variance_damping"],
                        bias_meta["segment_cv"],
                        adjustment_days,
                        bias_meta["cap_days"],
                        bias_meta["source"],
                    )
                    predictions.expected_gestation_days = corrected
                    if predictions.gestation_range.get("p25") is not None:
                        predictions.gestation_range["p25"] = max(
                            1, int(predictions.gestation_range["p25"]) + adjustment_days
                        )
                    if predictions.gestation_range.get("p75") is not None:
                        predictions.gestation_range["p75"] = max(
                            1, int(predictions.gestation_range["p75"]) + adjustment_days
                        )
                    if (
                        hasattr(predictions, "gestation_prediction_interval")
                        and predictions.gestation_prediction_interval is not None
                    ):
                        if predictions.gestation_prediction_interval.get("p10") is not None:
                            predictions.gestation_prediction_interval["p10"] = max(
                                1, int(predictions.gestation_prediction_interval["p10"]) + adjustment_days
                            )
                        if predictions.gestation_prediction_interval.get("p50") is not None:
                            predictions.gestation_prediction_interval["p50"] = max(
                                1, int(predictions.gestation_prediction_interval["p50"]) + adjustment_days
                            )
                        if predictions.gestation_prediction_interval.get("p90") is not None:
                            predictions.gestation_prediction_interval["p90"] = max(
                                1, int(predictions.gestation_prediction_interval["p90"]) + adjustment_days
                            )

        # Conversion: inclusive (closed-won / all) is the main signal; closed-only retained as reference
        conv_rate_incl, conv_stats_incl = self.calculate_conversion_rate(segment_data, method="inclusive")
        conv_rate_closed, conv_stats_closed = self.calculate_conversion_rate(
            segment_data, method="closed_only",
            prior_rate=use_prior_rate, prior_strength=use_prior_strength
        )
        predictions.expected_conversion_rate = conv_rate_incl
        predictions.conversion_confidence = conv_stats_incl["confidence"]
        predictions.conversion_method = "inclusive"

        # Rating
        project_metrics = {
            "conversion_rate": predictions.expected_conversion_rate,
            "expected_gestation_days": predictions.expected_gestation_days,
            "new_enquiry_value": project.new_enquiry_value,
            "account": project.account,
            "product_type": project.product_type,
            "value_band": project.value_band
        }
        rating, rating_components = self.calculate_rating_score(project_metrics, segment_data)
        predictions.rating_score = rating
        predictions.rating_components = rating_components

        predictions.segment_statistics = self.create_segment_statistics(
            segment_data,
            segment_keys,
            backoff_tier,
            prior_rate=use_prior_rate,
            prior_strength=use_prior_strength
        )

        return predictions
