import logging
from typing import Dict, Optional, Tuple

import pandas as pd
from scipy.stats import beta as beta_distribution

from ..models import PipelineStage, StatusCategory

logger = logging.getLogger(__name__)


class HierarchicalBayes:
    """
    Beta-Binomial helper that enables partial pooling for sparse segments.

    Usage:
        pooler = HierarchicalBayes()
        pooler.fit_from_frame(global_df)
        segment_stats = pooler.posterior_from_frame(segment_df)
    """

    def __init__(
        self,
        default_rate: float = 0.23,
        default_strength: float = 20.0,
        cred_mass: float = 0.8,
    ) -> None:
        self.default_rate = float(min(max(default_rate, 0.01), 0.99))
        self.default_strength = max(float(default_strength), 2.0)
        self.cred_mass = float(min(max(cred_mass, 0.5), 0.99))
        self.reset()

    def reset(self) -> None:
        self.alpha = self.default_rate * self.default_strength
        self.beta = (1.0 - self.default_rate) * self.default_strength

    @property
    def prior_strength(self) -> float:
        return float(self.alpha + self.beta)

    # ------------------------------------------------------------------ #
    # Global prior fitting
    # ------------------------------------------------------------------ #

    def fit_from_frame(self, df: Optional[pd.DataFrame]) -> None:
        wins, losses = self._count_outcomes(df)
        self.fit_from_counts(wins, losses)

    def fit_from_counts(self, wins: float, losses: float) -> None:
        total = float(wins + losses)
        if total <= 0:
            self.reset()
            return
        # Global prior is roughly Beta(1 + wins, 1 + losses)
        self.alpha = float(wins) + 1.0
        self.beta = float(losses) + 1.0

    # ------------------------------------------------------------------ #
    # Posterior calculations
    # ------------------------------------------------------------------ #

    def posterior_from_frame(self, df: Optional[pd.DataFrame]) -> Dict[str, float]:
        wins, losses = self._count_outcomes(df)
        return self.posterior_from_counts(wins, losses)

    def posterior_from_counts(self, wins: float, losses: float) -> Dict[str, float]:
        wins = max(float(wins), 0.0)
        losses = max(float(losses), 0.0)
        alpha_post = self.alpha + wins
        beta_post = self.beta + losses
        denom = alpha_post + beta_post
        mean = alpha_post / denom if denom > 0 else self.default_rate
        sample = wins + losses
        shrinkage_weight = (
            sample / (sample + self.prior_strength) if self.prior_strength > 0 else 0.0
        )

        tail = (1.0 - self.cred_mass) / 2.0
        try:
            lower = float(beta_distribution.ppf(tail, alpha_post, beta_post))
            upper = float(beta_distribution.ppf(1.0 - tail, alpha_post, beta_post))
        except Exception as exc:  # noqa: BLE001
            logger.debug("Failed to compute beta interval: %s", exc)
            lower, upper = 0.0, 1.0

        return {
            "mean": mean,
            "lower": max(0.0, lower),
            "upper": min(1.0, upper),
            "sample": sample,
            "shrinkage_weight": max(0.0, min(1.0, shrinkage_weight)),
            "global_prior_mean": self.alpha / (self.alpha + self.beta),
            "global_prior_strength": self.prior_strength,
        }

    # ------------------------------------------------------------------ #
    # Utility
    # ------------------------------------------------------------------ #

    def _count_outcomes(self, df: Optional[pd.DataFrame]) -> Tuple[float, float]:
        if df is None or df.empty:
            return 0.0, 0.0
        if "pipeline_stage" in df.columns:
            stages = df["pipeline_stage"].fillna("")
            wins = (stages == PipelineStage.WON_CLOSED.value).sum()
            losses = (stages == PipelineStage.LOST.value).sum()
        else:
            statuses = df.get("status_category", pd.Series("", index=df.index)).fillna("")
            wins = (statuses == StatusCategory.WON.value).sum()
            losses = (statuses == StatusCategory.LOST.value).sum()
        return float(wins), float(losses)