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

logger = logging.getLogger(__name__)


class NumericBaseline:
    """
    Compute deterministic baselines before LLM processing
    """
    
    def __init__(self, lookback_days: Optional[int] = None):
        self.smoothing_k = 1.0  # Laplace smoothing parameter
        self.lookback_days = lookback_days or 1825 # Default to 5 years
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

    def _beta_smoothed_rate(self, won: int, lost: int, prior_rate: Optional[float], prior_strength: Optional[int]) -> float:
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
        apply_time_weighting: bool = True,
        half_life_days: float = 730  # 2 years
    ) -> Tuple[Optional[int], Dict[str, Any]]:
        """
        Calculate expected gestation period using weighted median/statistics
        """
        if segment_data.empty:
            logger.warning("Empty segment data for gestation calculation")
            return None, {'confidence': 0}
        
        # Extract and clean gestation values
        gestation_values = segment_data['gestation_period'].dropna()
        gestation_values = gestation_values[
            (gestation_values > 0) & (gestation_values < 1000)
        ]
        
        if len(gestation_values) < 3:
            logger.warning(f"Insufficient gestation data: {len(gestation_values)} samples")
            return None, {'confidence': 0, 'sample_size': len(gestation_values)}
        
        # Apply time weighting if enabled
        if apply_time_weighting and 'date_created' in segment_data.columns:
            weights = self._calculate_time_weights(
                segment_data.loc[gestation_values.index, 'date_created'],
                half_life_days=half_life_days
            )
            
            # Weighted percentiles
            sorted_indices = gestation_values.argsort()
            sorted_values = gestation_values.iloc[sorted_indices].values
            sorted_weights = weights.iloc[sorted_indices].values
            cumsum = np.cumsum(sorted_weights)
            cumsum = cumsum / cumsum[-1]  # Normalize to [0, 1]
            
            median_val = np.interp(0.5, cumsum, sorted_values)
            p25 = np.interp(0.25, cumsum, sorted_values)
            p75 = np.interp(0.75, cumsum, sorted_values)
            
            # Weighted mean and std
            mean_val = np.average(gestation_values, weights=weights)
            variance = np.average((gestation_values - mean_val)**2, weights=weights)
            std_val = np.sqrt(variance)
            
            weighting_note = f"time_weighted_half_life_{half_life_days}d"
        else:
            # Original unweighted calculations
            median_val = gestation_values.median()
            p25 = gestation_values.quantile(0.25)
            p75 = gestation_values.quantile(0.75)
            mean_val = gestation_values.mean()
            std_val = gestation_values.std()
            weighting_note = "unweighted"
        
        # Calculate confidence based on sample size and spread
        sample_confidence = min(len(gestation_values) / 30, 1.0)
        
        # Penalize high variance
        cv = std_val / mean_val if mean_val > 0 else 1
        spread_confidence = max(0, 1 - cv)
        
        overall_confidence = sample_confidence * 0.7 + spread_confidence * 0.3
        
        iqr = p75 - p25
        
        statistics = {
            'median': int(median_val),
            'p25': int(p25),
            'p75': int(p75),
            'iqr': int(iqr),
            'mean': int(mean_val),
            'std': int(std_val),
            'count': len(gestation_values),
            'confidence': round(overall_confidence, 2),
            'weighting': weighting_note
        }
        
        logger.info(f"Gestation baseline: {int(median_val)} days (n={len(gestation_values)}, {weighting_note})")
        
        return int(median_val), statistics
    
    def calculate_conversion_rate(
        self, 
        segment_data: pd.DataFrame, 
        method: str = 'inclusive',
        prior_rate: Optional[float] = None,
        prior_strength: Optional[int] = None,
        apply_time_weighting: bool = True,
        half_life_days: float = 730
    ) -> Tuple[float, Dict[str, Any]]:
        if segment_data.empty:
            return 0.5, {'confidence': 0}

        total = len(segment_data)
        use_pipeline = 'pipeline_stage' in segment_data.columns

        if use_pipeline:
            stages = segment_data['pipeline_stage'].fillna('')
            won_closed = int((stages == PipelineStage.WON_CLOSED.value).sum())
            lost = int((stages == PipelineStage.LOST.value).sum())
        else:
            # Fallback: treat 'Won' as closed-won if pipeline_stage missing
            sc = segment_data['status_category'] if 'status_category' in segment_data.columns else pd.Series([], dtype=object)
            won_closed = int((sc == StatusCategory.WON.value).sum())
            lost = int((sc == StatusCategory.LOST.value).sum())

        open_count = int(total - won_closed - lost)

        if apply_time_weighting and 'date_created' in segment_data.columns:
            weights = self._calculate_time_weights(
                segment_data['date_created'],
                half_life_days=half_life_days
            )
            
            # Calculate weighted counts
            if use_pipeline:
                won_closed_weighted = weights[stages == PipelineStage.WON_CLOSED.value].sum()
                lost_weighted = weights[stages == PipelineStage.LOST.value].sum()
            else:
                won_closed_weighted = weights[sc == StatusCategory.WON.value].sum()
                lost_weighted = weights[sc == StatusCategory.LOST.value].sum()
            
            # For closed_only method
            if method == 'closed_only':
                total_weighted = won_closed_weighted + lost_weighted
                if total_weighted < 1.0:  # Effective sample size too small
                    return None, {
                        'confidence': 0, 
                        'note': 'Insufficient weighted samples',
                        'wins': won_closed,
                        'losses': lost,
                        'open': open_count
                    }
                
                # Beta smoothing with weighted counts
                rate = self._beta_smoothed_rate(
                    int(won_closed_weighted), 
                    int(lost_weighted), 
                    prior_rate, 
                    prior_strength
                )
                confidence = min(total_weighted / 100, 1.0)
                
                statistics = {
                    'wins_weighted': round(won_closed_weighted, 2),
                    'losses_weighted': round(lost_weighted, 2),
                    'total_weighted': round(total_weighted, 2),
                    'wins': won_closed,  # Keep actual counts for backward compatibility
                    'losses': lost,
                    'open': open_count,
                    'total': total,
                    'smoothed_rate': rate,
                    'confidence': round(confidence, 2),
                    'method': 'closed_only_time_weighted',
                    'half_life_days': half_life_days
                }
                return rate, statistics
            
            # For inclusive method
            else:
                total_weighted = weights.sum()
                rate = float(won_closed_weighted + self.smoothing_k) / float(total_weighted + 2 * self.smoothing_k)
                confidence = min(total_weighted / 100, 1.0)
                
                statistics = {
                    'wins_weighted': round(won_closed_weighted, 2),
                    'total_weighted': round(total_weighted, 2),
                    'wins': won_closed,  # Keep actual counts for backward compatibility
                    'losses': lost,
                    'open': open_count,
                    'total': total,
                    'raw_rate': (won_closed / total) if total > 0 else 0.0,
                    'smoothed_rate': rate,
                    'confidence': round(confidence, 2),
                    'method': 'inclusive_time_weighted',
                    'half_life_days': half_life_days
                }
                return rate, statistics
        
        # Original unweighted calculation as fallback
        if method == 'closed_only':
            total_closed = won_closed + lost
            if total_closed == 0:
                return None, {
                    'confidence': 0,
                    'wins': won_closed,
                    'losses': lost,
                    'open': open_count,
                    'note': 'No closed projects in segment',
                    'method': 'closed_only'
                }
            rate = self._beta_smoothed_rate(won_closed, lost, prior_rate, prior_strength)
            confidence = min(total_closed / 100, 1.0)
            statistics = {
                'wins': won_closed,
                'losses': lost,
                'open': open_count,
                'total': int(total_closed),
                'raw_rate': (won_closed / total_closed) if total_closed > 0 else 0.0,
                'smoothed_rate': rate,
                'confidence': round(confidence, 2),
                'method': 'closed_only'
            }
            return rate, statistics
        else:
            # Inclusive: won_closed / (won_closed + lost + open)
            k = self.smoothing_k
            rate = float(won_closed + k) / float(total + 2 * k)
            confidence = min(total / 100, 1.0)
            statistics = {
                'wins': won_closed,
                'losses': lost,
                'open': open_count,
                'total': int(total),
                'raw_rate': (won_closed / total) if total > 0 else 0.0,
                'smoothed_rate': rate,
                'confidence': round(confidence, 2),
                'method': 'inclusive_closed_wins'
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
        def value_score_from_segment(val: float) -> Optional[float]:
            try:
                if segment_data is None or segment_data.empty:
                    return None
                vs = segment_data['new_enquiry_value'].dropna()
                vs = vs[vs > 0]
                if vs.empty:
                    return None
                arr = np.sort(vs.to_numpy() if isinstance(vs, pd.Series) else np.array(list(vs)))
                pos = int(np.searchsorted(arr, val, side='right'))
                # More aggressive percentile scaling
                percentile = float(pos) / float(len(arr))
                # Apply power curve to boost higher values
                return percentile ** 0.8  # Power curve favors higher values
            except Exception:
                return None

        value_score = value_score_from_segment(new_value)
        if value_score is None:
            # UPDATED: More generous fallback mapping by band
            band_map = {
                'Zero': 0.10,
                'Small (<15k)': 0.40,
                'Medium (15-40k)': 0.65,
                'Large (40-100k)': 0.80,
                'XLarge (>100k)': 1.00
            }
            value_score = band_map.get(value_band, 0.5)

        # Account/product performance (small modifiers with smoothing)
        account_score = self._calculate_account_performance(project_metrics.get('account'), segment_data) if segment_data is not None else 0.5
        product_score = self._calculate_product_performance(project_metrics.get('product_type'), segment_data) if segment_data is not None else 0.5

        # UPDATED: Adjusted weights to better reflect stakeholder priorities
        weights = {
            'conversion': 0.30,  # Reduced from 0.55 to balance with other factors
            'value': 0.43,       # Increased from 0.25
            'gestation': 0.18,   # Increased from 0.12
            'account': 0.06,     # Slight increase from 0.05
            'product': 0.03
        }

        raw_score = (
            weights['conversion'] * cr_score +
            weights['value'] * value_score +
            weights['gestation'] * gestation_score +
            weights['account'] * account_score +
            weights['product'] * product_score
        )

        # UPDATED: Stronger alignment bonuses for high-performing projects
        alignment_boost = 0.0
        all_weak_penalty = 0.0
        momentum_bonus = 0.0

        # High performance alignment bonuses (significantly increased)
        if cr >= 0.35 and value_score >= 0.70 and gestation_score >= 0.75:
            # Very good conversion + high value + low gestation = major boost
            alignment_boost = 0.15  # Increased from 0.08
        elif cr >= 0.30 and value_score >= 0.65 and gestation_score >= 0.65:
            # Above average on all fronts
            alignment_boost = 0.10  # Increased from 0.05
        elif cr >= 0.25 and value_score >= 0.60:
            # Slightly above average conversion with decent value
            alignment_boost = 0.06  # New tier

        # Weak performance penalties (unchanged)
        if cr_score <= 0.25 and value_score <= 0.30 and gestation_score <= 0.40:
            all_weak_penalty = -0.08

        # UPDATED: Stronger momentum bonuses for standout factors
        if value_score >= 0.85 and gestation_score >= 0.75:
            momentum_bonus += 0.08  # Increased from 0.05
        if cr >= 0.35 and gestation_score >= 0.85:
            # Very good conversion with excellent gestation
            momentum_bonus += 0.10  # Increased from 0.04
        if cr >= 0.40:
            # Excellent conversion rate deserves recognition
            momentum_bonus += 0.08  # Increased from 0.04
        if cr >= 0.50:
            # Exceptional conversion rate
            momentum_bonus += 0.05  # New bonus tier

        # Cap total bonuses to avoid overshooting
        total_bonus = alignment_boost + momentum_bonus + all_weak_penalty
        total_bonus = max(-0.15, min(0.30, total_bonus))  # Cap at ±0.30

        raw_score = max(0.0, min(1.0, raw_score + total_bonus))

        # UPDATED: More aggressive scaling to use full 1-100 range
        # Apply slight exponential curve to push high scores higher
        if raw_score > 0.7:
            # Apply boost for high performers
            raw_score = 0.7 + (raw_score - 0.7) * 1.3  # Amplify high scores
            raw_score = min(1.0, raw_score)
        
        # Scale to 1–100 with better distribution
        final_score = max(1, min(100, round(raw_score * 98 + 2)))  # Changed from 99+1 to 98+2

        # Build components dictionary for transparency
        components = {
            'conversion_rate': cr,
            'conversion_score': round(cr_score, 3),
            'gestation_days': expected_gestation,
            'gestation_score': round(gestation_score, 3),
            'value': new_value,
            'value_band': value_band,
            'value_score': round(value_score, 3),
            'account_score': round(account_score, 3),
            'product_score': round(product_score, 3),
            'raw_weighted_score': round(raw_score - total_bonus, 3),
            'alignment_boost': round(alignment_boost, 3),
            'momentum_bonus': round(momentum_bonus, 3),
            'weak_penalty': round(all_weak_penalty, 3),
            'total_bonus': round(total_bonus, 3),
            'final_raw_score': round(raw_score, 3),
            'final_score': final_score,
            'weights': weights
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
        
        product_data = segment_data[segment_data['product_type'] == product]
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

        gestation_days, gest_stats = self.calculate_gestation_baseline(segment_data)
        if not gestation_days and global_data is not None and not global_data.empty:
            g2, gs2 = self.calculate_gestation_baseline(global_data)
            gestation_days, gest_stats = (g2, gs2) if g2 else (None, {'confidence': 0})
        if gestation_days:
            predictions.expected_gestation_days = gestation_days
            predictions.gestation_confidence = gest_stats['confidence']
            predictions.gestation_range = {
                'p25': gest_stats.get('p25'),
                'p75': gest_stats.get('p75')
            }

        # Conversion: inclusive (closed-won / all) is the main signal; closed-only retained as reference
        conv_rate_incl, conv_stats_incl = self.calculate_conversion_rate(segment_data, method='inclusive')
        conv_rate_closed, conv_stats_closed = self.calculate_conversion_rate(
            segment_data, method='closed_only',
            prior_rate=use_prior_rate, prior_strength=use_prior_strength
        )
        predictions.expected_conversion_rate = conv_rate_incl
        predictions.conversion_confidence = conv_stats_incl['confidence']
        predictions.conversion_method = 'inclusive'

        # Rating
        project_metrics = {
            'conversion_rate': predictions.expected_conversion_rate,
            'expected_gestation_days': predictions.expected_gestation_days,
            'new_enquiry_value': project.new_enquiry_value,
            'account': project.account,
            'product_type': project.product_type,
            'value_band': project.value_band
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

    def _calculate_time_weights(self, dates: pd.Series, half_life_days: float = 730) -> pd.Series:
        """
        Calculate exponential decay weights based on project age.
        
        Args:
            dates: Series of date_created values
            half_life_days: Number of days for weight to decay to 50% (default: 2 years)
        
        Returns:
            Series of weights (0-1) where recent projects have weight ~1.0
        """
        if dates.empty:
            return pd.Series(dtype=float)
        
        # Convert to datetime if needed
        dates = pd.to_datetime(dates)
        reference_date = pd.Timestamp.now()
        
        # Calculate days ago
        days_ago = (reference_date - dates).dt.days
        
        # Exponential decay: weight = 0.5 ^ (days_ago / half_life_days)
        weights = np.power(0.5, days_ago / half_life_days)
        
        # Normalize weights to sum to effective sample size
        weights = weights / weights.sum() * len(weights)
        
        return weights

    def _adjust_confidence_for_recency(
        self,
        base_confidence: float,
        segment_data: pd.DataFrame,
        recency_threshold_days: int = 730
    ) -> Tuple[float, str]:
        """
        Boost confidence if segment has substantial recent data.
        """
        if segment_data.empty or 'date_created' not in segment_data.columns:
            return base_confidence, "no_date_data"
        
        cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=recency_threshold_days)
        segment_data['date_created'] = pd.to_datetime(segment_data['date_created'])
        
        recent_count = (segment_data['date_created'] >= cutoff_date).sum()
        total_count = len(segment_data)
        recent_ratio = recent_count / total_count if total_count > 0 else 0
        
        # Boost confidence if high proportion of data is recent
        if recent_ratio >= 0.5 and recent_count >= 15:
            boost = 0.15  # +15% confidence
            note = f"high_recent_data_{recent_count}/{total_count}"
        elif recent_ratio >= 0.3 and recent_count >= 10:
            boost = 0.10  # +10% confidence
            note = f"moderate_recent_data_{recent_count}/{total_count}"
        else:
            boost = 0.0
            note = f"low_recent_data_{recent_count}/{total_count}"
        
        adjusted_confidence = min(1.0, base_confidence + boost)
        
        return adjusted_confidence, note
