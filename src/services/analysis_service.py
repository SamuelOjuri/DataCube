import os
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
import pandas as pd
import logging
from ..config import ANALYSIS_LOOKBACK_DAYS, GEMINI_API_KEY, GEMINI_MODEL
from ..database.supabase_client import SupabaseClient
from ..core.llm_analyzer import LLMAnalyzer
from ..core.models import ProjectFeatures, NumericPredictions, SegmentStatistics
from ..core.numeric_analyzer import NumericBaseline

logger = logging.getLogger(__name__)

class AnalysisService:
    def __init__(self, db_client: Optional[SupabaseClient] = None, lookback_days: Optional[int] = None):
        self.db = db_client or SupabaseClient()
        self.llm_analyzer = LLMAnalyzer()
        self.numeric_baseline = NumericBaseline()
        self.lookback_days = lookback_days or ANALYSIS_LOOKBACK_DAYS
        logger.info(f"AnalysisService initialized with {self.lookback_days} days ({self.lookback_days/365:.1f} years) lookback")

    def _cluster_key(self, p: Dict[str, Any]) -> Dict[str, Any]:
        return {
            'account': p.get('account') or None,
            'type': p.get('type') or None,
            'category': p.get('category') or None,
            'product_type': p.get('product_type') or None
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
        # Unified backoff to get segment rows
        seg_df, seg_keys, backoff_tier = self._fetch_segment_df(key)
        pf = self._to_project_features(project)
        nb = NumericBaseline()
        preds = nb.analyze_project(
            pf,
            historical_data=seg_df,
            segment_data=seg_df,
            segment_keys=seg_keys,
            backoff_tier=backoff_tier
        )

        return {
            'expected_gestation_days': preds.expected_gestation_days,
            'gestation_confidence': preds.gestation_confidence,
            'expected_conversion_rate': round(preds.expected_conversion_rate or 0.0, 3),
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
        nb = NumericBaseline()
        stats = nb.create_segment_statistics(
            seg_df,
            segment_keys=seg_keys,
            backoff_tier=backoff_tier
        )
        return stats

    def _fetch_segment_df(self, key: Dict[str, Any], min_n: int = 15):
        """
        Hierarchical backoff with recency filter (configurable lookback period).
        Returns: (segment_df, segment_keys_used, backoff_tier)
        """
        candidates = [
            ['account','type','category','product_type'],
            ['type','category','product_type'],
            ['type','category'],
            ['category'],
            []  # global
        ]
        recency_cutoff = (datetime.now().date() - timedelta(days=self.lookback_days)).isoformat()
        for tier, fields in enumerate(candidates, start=1):
            q = self.db.client.table('projects').select('*').gte('date_created', recency_cutoff)
            for f in fields:
                v = key.get(f)
                if v:
                    q = q.eq(f, v)
            rows = q.limit(5000).execute().data or []
            if len(rows) >= (min_n if fields else 50) or not fields:
                return pd.DataFrame(rows), fields, tier
        return pd.DataFrame(), [], 5

    def _fetch_global_df(self) -> pd.DataFrame:
        """Fetch global dataset with configurable lookback period."""
        recency_cutoff = (datetime.now().date() - timedelta(days=self.lookback_days)).isoformat()
        rows = self.db.client.table('projects').select('*').gte('date_created', recency_cutoff).limit(10000).execute().data or []
        return pd.DataFrame(rows)

    def analyze_and_store(self, monday_id: str, with_llm: bool = False) -> Dict[str, Any]:
        proj = self.db.client.table('projects').select('*').eq('monday_id', monday_id).single().execute().data
        if not proj:
            return {'success': False, 'error': 'project not found'}

        # 1) Baseline numeric analysis (existing)
        base = self.analyze_project(proj)

        # Prepare normalized features for consistent value/value_band serialization
        pf = self._to_project_features(proj)

        # 2) Store baseline if LLM disabled or no key
        if not with_llm or not GEMINI_API_KEY:
            self.db.store_analysis_result(monday_id, base)
            # Enrich returned result (do not store these extra fields)
            returned = {
                'name': pf.name,
                'account': pf.account,
                'type': pf.type,
                'category': pf.category,
                'product_type': pf.product_type,
                'value': pf.new_enquiry_value,
                'value_band': pf.value_band,
                **base
            }
            return {'success': True, 'result': returned}

        # 3) Build inputs for LLM reasoning
        seg = self._build_segment_stats(proj)
        np = self._to_numeric_predictions(base, seg)

        # 4) LLM reasoning + optional ±1 rating adjustment
        llm = LLMAnalyzer()  # uses GEMINI
        llm_out, meta = llm.analyze_project(pf, np, seg)
        final = llm.create_final_analysis(pf, np, llm_out, meta)

        # 5) Flatten to analysis_results schema (stored payload only includes DB columns)
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
            'processing_time_ms': int(float(meta.get('response_time', 0)) * 1000)
        }

        self.db.store_analysis_result(monday_id, stored)

        # Enrich returned result with project value info after product_type
        returned = {
            'name': pf.name,
            'account': pf.account,
            'type': pf.type,
            'category': pf.category,
            'product_type': pf.product_type,
            'value': pf.new_enquiry_value,
            'value_band': pf.value_band,
            **stored
        }
        return {'success': True, 'result': returned}
