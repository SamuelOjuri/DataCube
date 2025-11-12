I scanned the code to locate the current estimation logic for both metrics, and I’ll suggest concrete statistical/ML upgrades that will improve accuracy and robustness without hurting performance.

### What’s implemented now (relevant snippets)

- Conversion rate
  - Inclusive rate (default) = Laplace-smoothed wins over all (wins + losses + open), with optional exponential time-weighting.
```327:348:src/core/numeric_analyzer.py
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
```

  - Closed-only (reference) uses a Beta-Binomial update if a global prior is provided; otherwise Laplace.
```139:146:src/core/numeric_analyzer.py
def _beta_smoothed_rate(self, won: float, lost: float, prior_rate: Optional[float], prior_strength: Optional[int]) -> float:
    if prior_rate is not None and prior_strength and prior_strength > 0:
        alpha = prior_rate * prior_strength
        beta = (1.0 - prior_rate) * prior_strength
        return float(won + alpha) / float((won + lost) + alpha + beta)
    # Fallback to Laplace (Beta(1,1))
    k = self.smoothing_k
    return float(won + k) / float((won + lost) + 2 * k)
```

  - The default prediction uses the inclusive rate, not the closed-only Bayesian estimate:
```674:683:src/core/numeric_analyzer.py
        conv_rate_incl, conv_stats_incl = self.calculate_conversion_rate(segment_data, method='inclusive')
        conv_rate_closed, conv_stats_closed = self.calculate_conversion_rate(
            segment_data, method='closed_only',
            prior_rate=use_prior_rate, prior_strength=use_prior_strength
        )
        predictions.expected_conversion_rate = conv_rate_incl
        predictions.conversion_confidence = conv_stats_incl['confidence']
        predictions.conversion_method = 'inclusive'
```

- Gestation period
  - Robust baseline via median and IQR, with optional exponential time-weighting (weighted median/quantiles).
```187:206:src/core/numeric_analyzer.py
        if use_weighting and 'date_created' in segment_data.columns:
            weights = self._calculate_time_weights(
                segment_data.loc[gestation_values.index, 'date_created'],
                half_life_days=weight_half_life
            )
            weights = weights.reindex(gestation_values.index).fillna(0.0)
            if not weights.empty and float(weights.sum()) > 0:
                sorted_idx = gestation_values.sort_values().index
                sorted_values = gestation_values.loc[sorted_idx].to_numpy(dtype=float)
                sorted_weights = weights.loc[sorted_idx].to_numpy(dtype=float)
                cumulative = np.cumsum(sorted_weights)
                if cumulative[-1] > 0:
                    cumulative /= cumulative[-1]
                    median_val = float(np.interp(0.5, cumulative, sorted_values))
                    p25 = float(np.interp(0.25, cumulative, sorted_values))
                    p75 = float(np.interp(0.75, cumulative, sorted_values))
                    mean_val = float(np.average(sorted_values, weights=sorted_weights))
                    variance = float(np.average((sorted_values - mean_val) ** 2, weights=sorted_weights))
                    std_val = float(np.sqrt(max(variance, 0.0)))
                    weighting_note = f"time_weighted_half_life_{int(weight_half_life)}d"
```

- Time-weighting is enabled by default:
```45:46:src/config.py
TIME_WEIGHTING_ENABLED = True  # Prioritise recent projects in baseline stats
TIME_WEIGHTING_HALF_LIFE_DAYS = 730  # Two-year half-life for exponential decay
```


### Key opportunities to improve accuracy

- Conversion rate
  - Use the closed-only Bayesian estimate (with a global/cluster prior) for the primary expected rate. The inclusive method depresses the rate by counting still-open deals in the denominator.
  - Add credible intervals from the Beta posterior and use their width (and effective sample size) as the confidence score instead of a simple min(total/100, 1).
  - Pass a true global prior. `AnalysisService.analyze_project` doesn’t currently provide `global_data`, so the Beta prior is never used. A small change improves stability, especially in sparse segments:
    ```python
    # in AnalysisService.analyze_project(...)
    global_df = self._fetch_global_df()
    preds = nb.analyze_project(
        pf,
        historical_data=seg_df,
        segment_data=seg_df,
        segment_keys=seg_keys,
        backoff_tier=backoff_tier,
        global_data=global_df,         # enable prior
        prior_strength=50              # tune (e.g., 20–100)
    )
    ```

  - Account for time exposure: model conversion as an event over time instead of a single ratio. Use survival/discrete-time hazard to estimate “probability to eventually win” and “probability to win within H days,” avoiding penalizing young, open deals.

- Gestation period
  - Replace the raw median with a parametric time-to-event model. Gestation is positive and skewed; a log-normal, Gamma, or Weibull model (MLE or Bayesian) gives better central tendency and predictive intervals.
  - Incorporate censored data. Deals not yet invoiced are right-censored; survival models (Cox, accelerated failure time with log-normal/Weibull, or gradient-boosted survival) will use them instead of discarding, improving accuracy on small samples.
  - Add hierarchical shrinkage (empirical Bayes or full Bayesian) so segment estimates partially pool toward similar groups or the global prior when n is small.

### Practical, low-effort upgrades (no heavy infra)

- Conversion (now)
  - Switch the primary estimate to closed-only Beta-Binomial with a global prior and report a 95% credible interval from the posterior. Keep the inclusive rate for reporting, but don’t use it as the main expected conversion.
  - Use effective sample size under time weights for confidence: neff = (Σw)^2 / (Σw^2). Base confidence and prior blending on neff rather than raw weighted totals.
  - Calibrate the rate: if you also train a simple logistic regression or gradient boosting classifier, calibrate it (isotonic/Platt) and blend with the Beta estimate for small-sample segments.

- Gestation (now)
  - Robust parametric fit: fit a log-normal by MLE to positive gestations; return median = exp(μ̂) and a 50% IQR from the posterior/parametric quantiles. Fall back to current weighted median if fit fails or n is tiny.
  - Use hierarchical shrinkage on μ (log-days) via empirical Bayes: shrink segment μ̂ toward global μ0 with weight proportional to n/(n+τ), improving stability.

- Confidence scoring
  - Replace ad-hoc confidence with distribution-aware measures:
    - Conversion: width of Beta(α, β) 90–95% CI (narrower = higher confidence), adjusted by neff.
    - Gestation: width of predicted IQR or a 90–95% interval from the parametric/survival model.

### Higher-return upgrades (light ML)

- Survival models
  - Conversion: discrete-time hazard (logistic on age buckets + features) or Cox/gradient-boosted survival to predict probability of eventual win and win-by-H-days. Handles censoring from open items correctly and uses age, value band, type, product_type, account, etc.
  - Gestation: accelerated failure time (log-normal/Weibull) with the same features; predict median and quantiles for each new project.

- Quantile regression for gestation
  - Train LightGBM/CatBoost with quantile objectives (α=0.25/0.5/0.75) to predict p25/median/p75. It’s fast, robust to nonlinearity, and often more accurate than global medians.

- Hierarchical modeling
  - Multi-level Beta-Binomial for conversion and log-normal AFT for gestation with partial pooling across segments (type/category/product/account). This yields strong accuracy on sparse data. Use an efficient engine (e.g., variational inference) if you want to avoid MCMC.

### Minimal code changes you can apply now

- Use closed-only Beta posterior as the main output and expose credible intervals:
  - In `AnalysisService`, pass `global_data` (shown above).
  - In `NumericBaseline.analyze_project`, set `expected_conversion_rate = conv_rate_closed` and add posterior CI to `predictions`:
    ```python
    # after computing conv_rate_closed and closed stats
    predictions.expected_conversion_rate = conv_rate_closed
    predictions.conversion_method = 'closed_only'
    # Example CI inside calculate_conversion_rate (closed_only branch):
    # alpha = won_closed_weighted + prior_alpha; beta = lost_weighted + prior_beta
    # lo, hi = stats.beta.ppf([0.05, 0.95], alpha, beta)
    ```

- Add effective sample size and use it in confidence for both metrics:
  ```python
  neff = (weights.sum() ** 2) / (np.square(weights).sum() + 1e-9)
  confidence = min(neff / 100, 1.0)
  ```


If you want, I can draft the exact edits for `src/services/analysis_service.py` and `src/core/numeric_analyzer.py` to:

- pass `global_data` and a tuned `prior_strength`,
- switch to closed-only as the primary conversion estimate,
- compute and return Beta credible intervals,
- and integrate `neff`-based confidence.

- Expected impact
  - Better calibration and higher stability on sparse segments.
  - No longer penalizing active, young opportunities in conversion.
  - More informative confidence via credible intervals.
  - Improved gestation accuracy with parametric or survival-based estimates, especially when censoring is common.

- Validation
  - Backtest on historical projects with time-based splits; measure Brier/log-loss for conversion, coverage of CIs, and median absolute error for gestation.
  - Monitor drift and recalibrate priors/half-life.

- Tooling
  - For survival/quantile models: `scikit-survival`, `lightgbm` (quantile), or `xgboost` (survival/quantile); calibration via `sklearn` isotonic/Platt.

- Rollout
  - Start with the Beta closed-only switch and credible intervals (quick win), then add gestation parametric fit; later, graduate to survival and quantile regression.

- I found the gestation and conversion implementations in `src/core/numeric_analyzer.py` and confirmed time-weighting in `src/config.py`. The current primary conversion estimate uses the inclusive method; I suggested switching to closed-only Beta with a global prior and adding credible intervals, plus parametric/survival modeling for gestation and survival modeling for conversion to handle censoring and exposure time.