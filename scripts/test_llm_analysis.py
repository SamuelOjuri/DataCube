# scripts/test_llm_analysis.py

import os
import sys
import time
import json
import csv
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.database.supabase_client import SupabaseClient
from src.services.analysis_service import AnalysisService
from src.core.llm_analyzer import LLMAnalyzer
from src.core.models import ProjectFeatures, NumericPredictions, SegmentStatistics
from src.core.numeric_analyzer import NumericBaseline
from datetime import timedelta
import pandas as pd
from src.config import ANALYSIS_LOOKBACK_DAYS, GEMINI_API_KEY, GEMINI_MODEL

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger("llm_test")


def fetch_projects(db: SupabaseClient, limit: int = 100) -> List[Dict[str, Any]]:
    # Prefer recent projects; fall back to any if date ordering fails
    try:
        res = db.client.table('projects')\
            .select('*')\
            .order('date_created', desc=True)\
            .limit(limit)\
            .execute()
        rows = res.data or []
        if len(rows) < limit:
            more = db.client.table('projects').select('*').limit(limit - len(rows)).execute().data or []
            seen = {r['monday_id'] for r in rows}
            rows += [r for r in more if r.get('monday_id') not in seen]
        return rows[:limit]
    except Exception:
        res = db.client.table('projects').select('*').limit(limit).execute()
        return res.data or []


def build_segment_stats(db: SupabaseClient, project: Dict[str, Any]) -> SegmentStatistics:
    key = {
        'account': project.get('account') or None,
        'type': project.get('type') or None,
        'category': project.get('category') or None,
        'product_type': project.get('product_type') or None
    }

    # Try full key
    q = db.client.table('conversion_metrics').select('*')
    for f in ['account', 'type', 'category', 'product_type']:
        v = key.get(f)
        if v:
            q = q.eq(f, v)
    res = q.limit(1).execute().data

    cm = res[0] if res else None
    backoff = 1

    # Fallback to (type, category)
    if not cm:
        q2 = db.client.table('conversion_metrics').select('*')
        for f in ['type', 'category']:
            v = key.get(f)
            if v:
                q2 = q2.eq(f, v)
        res2 = q2.limit(1).execute().data
        cm = res2[0] if res2 else None
        backoff = 3 if cm else 5

    total = int((cm or {}).get('total_projects') or 0)
    wins = int((cm or {}).get('won_projects') or 0)
    losses = int((cm or {}).get('lost_projects') or 0)
    open_cnt = int((cm or {}).get('open_projects') or 0)
    win_rate = float((cm or {}).get('win_rate') or 0.0)

    return SegmentStatistics(
        segment_keys=[k for k, v in key.items() if v],
        sample_size=total,
        backoff_tier=backoff,
        gestation_median=(cm or {}).get('median_gestation'),
        wins=wins,
        losses=losses,
        open=open_cnt,
        conversion_rate=win_rate,
        conversion_confidence=0.7 if cm else 0.4
    )



def fetch_segment_df(db: SupabaseClient, key: Dict[str, Any], min_n: int = 15, lookback_days: Optional[int] = None):
    """
    Return (segment_df, segment_keys_used, backoff_tier).
    Backoff hierarchy with recency filter (configurable lookback period).
    """
    candidates = [
        ['account','type','category','product_type'],
        ['type','category','product_type'],
        ['type','category'],
        ['category'],
        []  # global
    ]
    # Use provided lookback_days or fall back to config default
    days_back = lookback_days or ANALYSIS_LOOKBACK_DAYS
    recency_cutoff = (datetime.now().date() - timedelta(days=days_back)).isoformat()
    
    for tier, fields in enumerate(candidates, start=1):
        q = db.client.table('projects').select('*').gte('date_created', recency_cutoff)
        for f in fields:
            v = key.get(f)
            if v:
                q = q.eq(f, v)
        rows = q.limit(5000).execute().data or []
        # Require fewer rows for specific segments; more for global
        if len(rows) >= (min_n if fields else 50) or not fields:
            return pd.DataFrame(rows), fields, tier
    # Fallback (should rarely happen)
    return pd.DataFrame(), [], 5


def to_project_features(p: Dict[str, Any]) -> ProjectFeatures:
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


def to_numeric_predictions(base: Dict[str, Any], seg: Optional[SegmentStatistics]) -> NumericPredictions:
    return NumericPredictions(
        expected_gestation_days=base.get('expected_gestation_days'),
        gestation_confidence=float(base.get('gestation_confidence') or 0.0),
        gestation_range={},
        expected_conversion_rate=float(base.get('expected_conversion_rate') or 0.0),
        conversion_confidence=float(base.get('conversion_confidence') or 0.0),
        rating_score=int(base.get('rating_score') or 50),
        rating_components=base.get('reasoning', {}).get('metrics_used', {}),
        segment_statistics=seg
    )


def write_csv(results: List[Dict[str, Any]], csv_file: Path) -> None:
    headers = [
        'project_id', 'item_name', 'project_name', 'account', 'type', 'category', 'product_type', 'date_created', 'value', 'value_band',
        'baseline_expected_gestation_days', 'baseline_expected_conversion_rate', 'baseline_rating_score',
        'final_expected_gestation_days', 'final_expected_conversion_rate', 'final_rating_score',
        'rating_adjustment', 'confidence_notes',
        'segment_keys', 'segment_sample_size', 'segment_backoff_tier',
        'segment_win_rate_closed', 'segment_win_rate_inclusive',
        'segment_conversion_confidence',
        'segment_gestation_median', 'segment_gestation_p25', 'segment_gestation_p75',
        'segment_wins', 'segment_losses', 'segment_open',
        'llm_model', 'response_time_s', 'tokens_used',
        'reasoning_gestation', 'reasoning_conversion', 'reasoning_rating'
    ]
    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in results:
            baseline = r.get('baseline', {}) or {}
            final = r.get('final_predictions', {}) or {}
            seg = r.get('segment', {}) or {}
            reason = r.get('reasoning', {}) or {}
            adj = r.get('adjustments', {}) or {}
            row = {
                'project_id': r.get('project_id'),
                'item_name': r.get('item_name'),
                'project_name': r.get('project_name'),
                'account': r.get('account'),
                'type': r.get('type'),
                'category': r.get('category'),
                'product_type': r.get('product_type'),
                'date_created': r.get('date_created'),
                'value': r.get('value') if r.get('value') is not None else r.get('new_enquiry_value'),
                'value_band': r.get('value_band'),
                'baseline_expected_gestation_days': baseline.get('expected_gestation_days'),
                'baseline_expected_conversion_rate': baseline.get('expected_conversion_rate'),
                'baseline_rating_score': baseline.get('rating_score'),
                'final_expected_gestation_days': final.get('expected_gestation_days'),
                'final_expected_conversion_rate': final.get('expected_conversion_rate'),
                'final_rating_score': final.get('rating_score'),
                'rating_adjustment': adj.get('rating_adjustment', 0),
                'confidence_notes': r.get('confidence_notes'),
                'segment_keys': ', '.join(seg.get('keys', []) or []),
                'segment_sample_size': seg.get('sample_size'),
                'segment_backoff_tier': seg.get('backoff_tier'),
                'segment_win_rate_closed': seg.get('win_rate_closed') or seg.get('win_rate'),
                'segment_win_rate_inclusive': seg.get('win_rate_inclusive'),
                'segment_conversion_confidence': seg.get('conversion_confidence'),
                'segment_gestation_median': seg.get('gestation_median'),
                'segment_gestation_p25': seg.get('gestation_p25'),
                'segment_gestation_p75': seg.get('gestation_p75'),
                'segment_wins': seg.get('wins'),
                'segment_losses': seg.get('losses'),
                'segment_open': seg.get('open'),
                'llm_model': r.get('llm_model'),
                'response_time_s': r.get('response_time_s'),
                'tokens_used': r.get('tokens_used'),
                'reasoning_gestation': reason.get('gestation', ''),
                'reasoning_conversion': reason.get('conversion', ''),
                'reasoning_rating': reason.get('rating', '')
            }
            w.writerow(row)


def fetch_global_df(db: SupabaseClient, lookback_days: Optional[int] = None) -> pd.DataFrame:
    # Use provided lookback_days or fall back to config default
    days_back = lookback_days or ANALYSIS_LOOKBACK_DAYS
    recency_cutoff = (datetime.now().date() - timedelta(days=days_back)).isoformat()
    rows = db.client.table('projects').select('*').gte('date_created', recency_cutoff).limit(10000).execute().data or []
    return pd.DataFrame(rows)


def main(n: int = 100, out_path: Optional[str] = None, csv_path: Optional[str] = None, lookback_days: Optional[int] = None):
    if not GEMINI_API_KEY:
        logger.error("GEMINI_API_KEY not set. Export it and retry.")
        sys.exit(1)

    # Log the lookback period being used
    days_back = lookback_days or ANALYSIS_LOOKBACK_DAYS
    logger.info(f"Using {days_back} days ({days_back/365:.1f} years) of historical data for analysis")

    db = SupabaseClient()
    svc = AnalysisService()
    llm = LLMAnalyzer()
    nb = NumericBaseline()
    global_df = fetch_global_df(db, lookback_days)

    projects = fetch_projects(db, n)
    if not projects:
        logger.error("No projects found.")
        sys.exit(1)

    logger.info(f"Testing LLM-enabled analysis for {len(projects)} projects (no DB writes)")
    results = []
    ok = 0
    errs = 0
    t0 = time.time()

    for i, p in enumerate(projects, 1):
        pid = p.get('monday_id')
        try:
            pf = to_project_features(p)

            key = {
                'account': p.get('account') or None,
                'type': p.get('type') or None,
                'category': p.get('category') or None,
                'product_type': p.get('product_type') or None
            }
            seg_df, seg_keys, backoff_tier = fetch_segment_df(db, key, lookback_days=lookback_days)

            # Numeric baselines from actual segment data (closed-only preferred)
            npred = nb.analyze_project(
                pf,
                historical_data=seg_df,
                segment_data=seg_df,
                segment_keys=seg_keys,
                backoff_tier=backoff_tier,
                global_data=global_df,
                prior_strength=20
            )
            seg_stats = npred.segment_statistics

            # LLM reasoning on top of numeric baselines
            out, meta = llm.analyze_project(pf, npred, seg_stats)
            final = llm.create_final_analysis(pf, npred, out, meta)

            res = {
                'project_id': pid,
                'item_name': p.get('item_name'),
                'project_name': pf.name,
                'account': pf.account,
                'type': pf.type,
                'category': pf.category,
                'product_type': pf.product_type,
                'value': pf.new_enquiry_value,
                'value_band': pf.value_band,
                'baseline': {
                    'expected_gestation_days': npred.expected_gestation_days,
                    'expected_conversion_rate': npred.expected_conversion_rate,
                    'rating_score': npred.rating_score
                },
                'final_predictions': {
                    'expected_gestation_days': final.predictions.expected_gestation_days,
                    'expected_conversion_rate': final.predictions.expected_conversion_rate,
                    'rating_score': final.predictions.rating_score
                },
                'reasoning': out.reasoning,
                'adjustments': out.adjustments,
                'confidence_notes': out.confidence_notes,
                'llm_model': meta.get('llm_model', GEMINI_MODEL),
                'response_time_s': meta.get('response_time', 0.0),
                'tokens_used': meta.get('tokens_used', 0),
                'segment': {
                    'keys': seg_stats.segment_keys if seg_stats else [],
                    'sample_size': seg_stats.sample_size if seg_stats else 0,
                    'backoff_tier': seg_stats.backoff_tier if seg_stats else 5,
                    'win_rate': seg_stats.conversion_rate if seg_stats else None,  # closed-only
                    'win_rate_inclusive': getattr(seg_stats, 'inclusive_conversion_rate', None),
                    'win_rate_closed': getattr(seg_stats, 'closed_conversion_rate', None),
                    'conversion_confidence': seg_stats.conversion_confidence if seg_stats else 0.0,
                    'wins': seg_stats.wins if seg_stats else 0,
                    'losses': seg_stats.losses if seg_stats else 0,
                    'open': seg_stats.open if seg_stats else 0,
                    'gestation_median': seg_stats.gestation_median,
                    'gestation_p25': seg_stats.gestation_p25,
                    'gestation_p75': seg_stats.gestation_p75
                }
            }
            results.append(res)
            ok += 1
            if i % 5 == 0:
                logger.info(f"[{i}/{len(projects)}] ok={ok} err={errs}")
            if i < len(projects):
                time.sleep(0.4)
        except Exception as e:
            errs += 1
            logger.warning(f"[{i}/{len(projects)}] Failed for {pid}: {e}")

    elapsed = time.time() - t0
    logger.info(f"Done: ok={ok} err={errs} in {elapsed:.1f}s ({(ok+errs)/max(1,elapsed):.2f}/s)")

    # JSON path
    if out_path:
        out_file = Path(out_path)
    else:
        out_dir = project_root / "outputs" / "analysis"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"llm_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

    # CSV path
    if csv_path:
        csv_file = Path(csv_path)
    else:
        csv_file = out_file.with_suffix('.csv')

    # Save JSON (no DB writes)
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    # Save CSV (flattened)
    write_csv(results, csv_file)

    # Tiny summary
    try:
        avg_rating = sum(r['final_predictions']['rating_score'] for r in results) / max(1, len(results))
        logger.info(f"Saved {len(results)} results to {out_file} and {csv_file} | avg_rating={avg_rating:.2f}")
    except Exception:
        logger.info(f"Saved {len(results)} results to {out_file} and {csv_file}")


if __name__ == "__main__":
    # Usage: python scripts/test_llm_analysis.py [N] [OUT_JSON_PATH] [OUT_CSV_PATH] [LOOKBACK_DAYS]
    N = int(sys.argv[1]) if len(sys.argv) > 1 else 100
    OUT_JSON = sys.argv[2] if len(sys.argv) > 2 else None
    OUT_CSV = sys.argv[3] if len(sys.argv) > 3 else None
    LOOKBACK_DAYS = int(sys.argv[4]) if len(sys.argv) > 4 else None
    main(N, OUT_JSON, OUT_CSV, LOOKBACK_DAYS)
