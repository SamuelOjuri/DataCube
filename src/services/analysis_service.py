import os

from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timedelta
import pandas as pd
import logging
from ..config import ANALYSIS_LOOKBACK_DAYS, GEMINI_API_KEY, GEMINI_MODEL
from ..database.supabase_client import SupabaseClient
from ..core.llm_analyzer import LLMAnalyzer
from ..core.models import ProjectFeatures, NumericPredictions, SegmentStatistics
from ..core.numeric_analyzer import NumericBaseline

logger = logging.getLogger(__name__)

# Backoff tiers at or beyond this value indicate coarse/global segments.
GLOBAL_BACKOFF_THRESHOLD = 4

# Hierarchical segment fallback tiers: (keys, min_n)
# Tier 0: Account × Type × Category × Product Key (n >= 10)
# Tier 1: Account × Type × Category               (n >= 8)
# Tier 2: Account × Type                          (n >= 5)
# Tier 3: Type × Category                         (n >= 5)
# Tier 4: Type                                    (n >= 3)
# Tier 5: Global                                  (n >= 1)
SEGMENT_BACKOFF_TIERS: List[Tuple[List[str], int]] = [
    (['account', 'type', 'category', 'product_type'], 10),
    (['account', 'type', 'category'], 8),
    (['account', 'type'], 5),
    (['type', 'category'], 5),
    (['type'], 3),
    ([], 1),
]

class AnalysisService:
    def __init__(self, db_client: Optional[SupabaseClient] = None, lookback_days: Optional[int] = None):
        self.db = db_client or SupabaseClient()
        self.numeric_baseline = NumericBaseline()
        self.lookback_days = lookback_days or ANALYSIS_LOOKBACK_DAYS
        self.llm_enabled = bool(GEMINI_API_KEY)
        self.llm_analyzer: Optional[LLMAnalyzer] = None
        logger.info(
            "AnalysisService initialized with %s days (%.1f years) lookback; LLM enabled=%s",
            self.lookback_days,
            self.lookback_days / 365,
            self.llm_enabled,
        )

    def _cluster_key(self, p: Dict[str, Any]) -> Dict[str, Any]:
        return {
            'account': p.get('account') or None,
            'type': p.get('type') or None,
            'category': p.get('category') or None,
            'product_type': p.get('product_key') or p.get('product_type') or None,
        }

    def _get_cluster_metrics(self, key: Dict[str, Any]) -> Dict[str, Any]:
        q = self.db.client.table('conversion_metrics').select('*')
        for f in ['account','type','category','product_type']:
            v = key.get(f)
            if v: q = q.eq(f, v)
        res = q.limit(1).execute().data
        if res: return res[0]
        q = self.db.client.table('conversion_metrics').select('*')
        for f in ['type','category']:
            v = key.get(f)
            if v: q = q.eq(f, v)
        res = q.limit(1).execute().data
        return res[0] if res else {}

    def analyze_project(self, project: Dict[str, Any]) -> Dict[str, Any]:
        key = self._cluster_key(project)
        seg_df, seg_keys, backoff_tier = self._fetch_segment_df(key)
        pf = self._to_project_features(project)
        nb = self.numeric_baseline

        global_df: Optional[pd.DataFrame] = None
        if seg_df.empty or len(seg_df) < 3 or backoff_tier >= GLOBAL_BACKOFF_THRESHOLD:
            # Supply broader context so NumericBaseline can apply priors/backoff logic.
            global_df = self._fetch_global_df()

        preds = nb.analyze_project(
            pf,
            historical_data=seg_df,
            segment_data=seg_df,
            segment_keys=seg_keys,
            backoff_tier=backoff_tier,
            global_data=global_df,
        )

        expected_conversion_rate = preds.expected_conversion_rate or 0.0

        return {
            'expected_gestation_days': preds.expected_gestation_days,
            'gestation_confidence': preds.gestation_confidence,
            'expected_conversion_rate': expected_conversion_rate,
            'conversion_confidence': preds.conversion_confidence,
            'rating_score': preds.rating_score,
            'reasoning': {
                'cluster': key,
                'metrics_used': {
                    'win_rate_closed': preds.expected_conversion_rate,
                    'gestation_median': preds.expected_gestation_days,
                    'gestation_p25': preds.gestation_range.get('p25'),
                    'gestation_p75': preds.gestation_range.get('p75')
                }
            },
            'llm_model': 'numeric-baseline',
            'analysis_version': 'v0.2',
            'processing_time_ms': 0
        }

    def _to_project_features(self, p: Dict[str, Any]) -> ProjectFeatures:
        return ProjectFeatures(
            project_id=p['monday_id'],
            name=p.get('project_name') or p.get('item_name') or '',
            account=p.get('account'),
            type=p.get('type'),
            category=p.get('category'),
            product_type=p.get('product_type'),
            new_enquiry_value=p.get('new_enquiry_value') or 0,
            gestation_period=p.get('gestation_period'),
            pipeline_stage=p.get('pipeline_stage'),
            status_category=p.get('status_category'),
            value_band=p.get('value_band')
        )

    def _to_numeric_predictions(self, base: Dict[str, Any], seg: SegmentStatistics) -> NumericPredictions:
        return NumericPredictions(
            expected_gestation_days=base.get('expected_gestation_days'),
            gestation_confidence=float(base.get('gestation_confidence') or 0.0),
            gestation_range={},  # optional
            expected_conversion_rate=float(base.get('expected_conversion_rate') or 0.0),
            conversion_confidence=float(base.get('conversion_confidence') or 0.0),
            rating_score=int(base.get('rating_score') or 50),
            rating_components=base.get('reasoning', {}).get('metrics_used', {}),
            segment_statistics=seg
        )

    def _build_segment_stats(self, project: Dict[str, Any]) -> SegmentStatistics:
        key = self._cluster_key(project)
        seg_df, seg_keys, backoff_tier = self._fetch_segment_df(key)
        nb = self.numeric_baseline
        stats = nb.create_segment_statistics(
            seg_df,
            segment_keys=seg_keys,
            backoff_tier=backoff_tier
        )
        return stats

    def _fetch_segment_df(self, key: Dict[str, Any]):
        """
        Hierarchical backoff with tier-specific minimum sample thresholds.
        Tier index mapping (0=most specific, 5=global):
          0: account + type + category + product_type (n >= 10)
          1: account + type + category                (n >= 8)
          2: account + type                           (n >= 5)
          3: type + category                          (n >= 5)
          4: type                                     (n >= 3)
          5: global                                   (n >= 1)
        Returns: (segment_df, segment_keys_used, backoff_tier)
        """
        SEGMENT_BACKOFF_TIERS = [
            (['account', 'type', 'category', 'product_type'], 10),
            (['account', 'type', 'category'], 8),
            (['account', 'type'], 5),
            (['type', 'category'], 5),
            (['type'], 3),
            ([], 1),
        ]
        recency_cutoff = (datetime.now().date() - timedelta(days=self.lookback_days)).isoformat()
        for tier, (fields, min_required) in enumerate(SEGMENT_BACKOFF_TIERS):
            # Skip tiers that require keys missing on the target project
            if fields and any(key.get(field) in (None, "") for field in fields):
                continue
            q = self.db.client.table('projects').select('*').gte('date_created', recency_cutoff)
            for field in fields:
                # 'product_type' in key maps to 'product_key' column in projects table
                col = 'product_key' if field == 'product_type' else field
                q = q.eq(col, key.get(field))
            rows = q.limit(5000).execute().data or []
            if len(rows) >= min_required:
                return pd.DataFrame(rows), fields, tier
        # Should only happen if there are no rows at all in the lookback window.
        return pd.DataFrame(), [], len(SEGMENT_BACKOFF_TIERS) - 1

    def _fetch_global_df(self) -> pd.DataFrame:
        """Fetch global dataset with configurable lookback period."""
        recency_cutoff = (datetime.now().date() - timedelta(days=self.lookback_days)).isoformat()
        rows = self.db.client.table('projects').select('*').gte('date_created', recency_cutoff).limit(10000).execute().data or []
        return pd.DataFrame(rows)

    def analyze_and_store(self, monday_id: str, with_llm: Optional[bool] = None) -> Dict[str, Any]:
        if with_llm is None:
            with_llm = self.llm_enabled
        proj = self.db.client.table('projects').select('*').eq('monday_id', monday_id).single().execute().data
        if not proj:
            return {'success': False, 'error': 'project not found'}

        base = self.analyze_project(proj)
        pf = self._to_project_features(proj)

        def _persist_numeric_only() -> Dict[str, Any]:
            self.db.store_analysis_result(monday_id, base)
            returned = {
                'name': pf.name,
                'account': pf.account,
                'type': pf.type,
                'category': pf.category,
                'product_type': pf.product_type,
                'value': pf.new_enquiry_value,
                'value_band': pf.value_band,
                **base,
            }
            return {'success': True, 'result': returned}

        if not with_llm or not self.llm_enabled:
            return _persist_numeric_only()

        if self.llm_analyzer is None:
            try:
                self.llm_analyzer = LLMAnalyzer()
            except Exception as exc:  # noqa: BLE001
                logger.warning("Gemini analyzer unavailable (%s). Falling back to numeric baseline.", exc)
                self.llm_enabled = False
                return _persist_numeric_only()

        seg = self._build_segment_stats(proj)
        np = self._to_numeric_predictions(base, seg)
        llm = self.llm_analyzer

        if llm is None:
            return _persist_numeric_only()

        llm_out, meta = llm.analyze_project(pf, np, seg)
        final = llm.create_final_analysis(pf, np, llm_out, meta)

        stored = {
            'expected_gestation_days': final.predictions.expected_gestation_days,
            'gestation_confidence': final.predictions.gestation_confidence,
            'expected_conversion_rate': final.predictions.expected_conversion_rate,
            'conversion_confidence': final.predictions.conversion_confidence,
            'rating_score': final.predictions.rating_score,
            'reasoning': final.reasoning,
            'adjustments': llm_out.adjustments,
            'confidence_notes': llm_out.confidence_notes,
            'special_factors': final.analysis_metadata.get('special_factors'),
            'llm_model': meta.get('llm_model', GEMINI_MODEL),
            'analysis_version': 'v1.0-llm',
            'processing_time_ms': int(float(meta.get('response_time', 0)) * 1000),
        }

        self.db.store_analysis_result(monday_id, stored)
        returned = {
            'name': pf.name,
            'account': pf.account,
            'type': pf.type,
            'category': pf.category,
            'product_type': pf.product_type,
            'value': pf.new_enquiry_value,
            'value_band': pf.value_band,
            **stored,
        }
        return {'success': True, 'result': returned}
