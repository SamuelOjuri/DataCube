import pandas as pd
import numpy as np
from scipy import stats
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

# Load .env file from project root
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

def get_db_engine():
    """Create database engine from environment or config."""
    database_url = os.getenv("SUPABASE_DB_URL")
    return create_engine(database_url)

def load_evaluation_set(engine):
    """Load projects with known gestation periods for evaluation."""
    query = text("""
        SELECT
            p.monday_id,
            p.item_name,
            p.type,
            p.category,
            p.product_key,
            p.product_type,
            p.account,
            p.gestation_period::numeric AS actual_days,
            p.date_created
        FROM projects p
        WHERE p.date_created >= DATE '2025-01-01'
          AND p.gestation_period IS NOT NULL
          AND p.gestation_period > 0
          AND p.gestation_period < 1460
    """)
    return pd.read_sql(query, engine)


def load_model_predictions(engine):
    """
    Load the actual model predictions from analysis_results.
    These reflect the real production model, not simple segment means.
    """
    query = text("""
        WITH latest_analysis AS (
            SELECT
                ar.project_id,
                ar.expected_gestation_days,
                ar.analysis_timestamp,
                ROW_NUMBER() OVER (
                    PARTITION BY ar.project_id
                    ORDER BY ar.analysis_timestamp DESC NULLS LAST, ar.id DESC
                ) AS rn
            FROM analysis_results ar
            WHERE ar.expected_gestation_days IS NOT NULL
        )
        SELECT
            project_id,
            expected_gestation_days::numeric AS model_predicted_days
        FROM latest_analysis
        WHERE rn = 1
    """)
    return pd.read_sql(query, engine)


LEGACY_BACKOFF_TIERS = [
    {"name": "A×T×C×PK", "fields": ["account", "type", "category", "product_key"], "min_n": 15},
    {"name": "T×C×PK",   "fields": ["type", "category", "product_key"], "min_n": 15},
    {"name": "T×C",      "fields": ["type", "category"], "min_n": 15},
    {"name": "C",        "fields": ["category"], "min_n": 15},
    {"name": "GLOBAL",   "fields": [], "min_n": 1},
]

NEW_BACKOFF_TIERS = [
    {"name": "A×T×C×PK", "fields": ["account", "type", "category", "product_key"], "min_n": 10},
    {"name": "A×T×C",    "fields": ["account", "type", "category"], "min_n": 8},
    {"name": "A×T",      "fields": ["account", "type"], "min_n": 5},
    {"name": "T×C",      "fields": ["type", "category"], "min_n": 5},
    {"name": "T",        "fields": ["type"], "min_n": 3},
    {"name": "GLOBAL",   "fields": [], "min_n": 1},
]


def _norm_key_value(v):
    if pd.isna(v):
        return None
    if isinstance(v, str):
        s = v.strip()
        return s if s else None
    return v


def predict_hierarchical_backoff_loo(df, target_idx, tiers):
    """
    Leave-one-out hierarchical backoff prediction for one project.
    Returns: (predicted_days, tier_idx, tier_name, segment_n)
    """
    target = df.loc[target_idx]
    train = df.drop(index=target_idx)

    for tier_idx, tier in enumerate(tiers):
        fields = tier["fields"]
        min_n = int(tier["min_n"])

        # Skip tiers requiring missing target keys
        if fields and any(_norm_key_value(target.get(f)) is None for f in fields):
            continue

        if not fields:
            seg_vals = train["actual_days"].dropna()
        else:
            mask = pd.Series(True, index=train.index)
            for f in fields:
                mask &= train[f] == target[f]
            seg_vals = train.loc[mask, "actual_days"].dropna()

        seg_n = int(seg_vals.shape[0])
        if seg_n >= min_n:
            return float(seg_vals.median()), tier_idx, tier["name"], seg_n

    # Hard fallback
    global_vals = train["actual_days"].dropna()
    if global_vals.empty:
        return np.nan, len(tiers) - 1, tiers[-1]["name"], 0
    return float(global_vals.median()), len(tiers) - 1, tiers[-1]["name"], int(global_vals.shape[0])


def compute_hierarchical_predictions(df, tiers, tag):
    out = df.copy()
    preds, tier_ids, tier_names, seg_ns = [], [], [], []

    for idx in out.index:
        pred, tier_idx, tier_name, seg_n = predict_hierarchical_backoff_loo(out, idx, tiers)
        preds.append(pred)
        tier_ids.append(tier_idx)
        tier_names.append(tier_name)
        seg_ns.append(seg_n)

    out[f"predicted_{tag}"] = preds
    out[f"tier_{tag}"] = tier_ids
    out[f"tier_name_{tag}"] = tier_names
    out[f"segment_n_{tag}"] = seg_ns
    return out


def tier_usage_distribution(df, tier_col, seg_n_col, tiers):
    total = len(df)
    rows = []
    for i, t in enumerate(tiers):
        subset = df[df[tier_col] == i]
        n = len(subset)
        seg_sizes = subset[seg_n_col] if n > 0 else pd.Series(dtype=float)
        rows.append({
            "tier_idx": i,
            "tier_name": t["name"],
            "min_n": t["min_n"],
            "count": n,
            "pct": round(100.0 * n / total, 2) if total else 0.0,
            "mean_segment_n": round(float(seg_sizes.mean()), 2) if n else 0.0,
            "median_segment_n": round(float(seg_sizes.median()), 2) if n else 0.0,
            "min_segment_n": int(seg_sizes.min()) if n else 0,
            "max_segment_n": int(seg_sizes.max()) if n else 0,
        })
    return pd.DataFrame(rows)


def paired_backoff_eval(actual, pred_a, pred_b, label_a, label_b, n_boot=4000, seed=42):
    """
    Paired evaluation of strategy B vs A.
    Negative deltas mean B improves error.
    """
    actual = np.asarray(actual, dtype=float)
    pred_a = np.asarray(pred_a, dtype=float)
    pred_b = np.asarray(pred_b, dtype=float)

    valid = ~(np.isnan(actual) | np.isnan(pred_a) | np.isnan(pred_b))
    actual = actual[valid]
    pred_a = pred_a[valid]
    pred_b = pred_b[valid]

    err_a = pred_a - actual
    err_b = pred_b - actual
    abs_a = np.abs(err_a)
    abs_b = np.abs(err_b)
    sq_a = err_a ** 2
    sq_b = err_b ** 2

    # Paired tests
    t_abs, p_abs = stats.ttest_rel(abs_a, abs_b)
    t_sq, p_sq = stats.ttest_rel(sq_a, sq_b)

    try:
        w_abs, wp_abs = stats.wilcoxon(abs_a, abs_b)
    except ValueError:
        w_abs, wp_abs = np.nan, np.nan

    try:
        w_sq, wp_sq = stats.wilcoxon(sq_a, sq_b)
    except ValueError:
        w_sq, wp_sq = np.nan, np.nan

    # Sign test on absolute errors
    non_tie = abs_a != abs_b
    n_non_tie = int(non_tie.sum())
    wins_b = int((abs_b[non_tie] < abs_a[non_tie]).sum()) if n_non_tie else 0
    sign_p = stats.binomtest(wins_b, n_non_tie, p=0.5, alternative="greater").pvalue if n_non_tie else np.nan

    # Bootstrap CIs for metric deltas: (B - A)
    rng = np.random.default_rng(seed)
    n = len(actual)
    idx = rng.integers(0, n, size=(n_boot, n))

    mae_delta_samples = abs_b[idx].mean(axis=1) - abs_a[idx].mean(axis=1)
    rmse_delta_samples = np.sqrt(sq_b[idx].mean(axis=1)) - np.sqrt(sq_a[idx].mean(axis=1))
    medae_delta_samples = np.median(abs_b[idx], axis=1) - np.median(abs_a[idx], axis=1)

    def ci95(x):
        return (round(float(np.percentile(x, 2.5)), 3), round(float(np.percentile(x, 97.5)), 3))

    return {
        "comparison": f"{label_a} vs {label_b}",
        "n_projects": int(n),
        "delta_mae_b_minus_a": round(float(abs_b.mean() - abs_a.mean()), 3),
        "delta_rmse_b_minus_a": round(float(np.sqrt(sq_b.mean()) - np.sqrt(sq_a.mean())), 3),
        "delta_medae_b_minus_a": round(float(np.median(abs_b) - np.median(abs_a)), 3),
        "delta_mae_ci95": ci95(mae_delta_samples),
        "delta_rmse_ci95": ci95(rmse_delta_samples),
        "delta_medae_ci95": ci95(medae_delta_samples),
        "paired_t_pvalue_abs_error": round(float(p_abs), 6),
        "paired_t_pvalue_squared_error": round(float(p_sq), 6),
        "wilcoxon_pvalue_abs_error": round(float(wp_abs), 6) if not np.isnan(wp_abs) else None,
        "wilcoxon_pvalue_squared_error": round(float(wp_sq), 6) if not np.isnan(wp_sq) else None,
        "sign_test_pvalue_abs_error": round(float(sign_p), 6) if not np.isnan(sign_p) else None,
        "wins_for_b_on_abs_error": wins_b,
        "non_ties_abs_error": n_non_tie,
        "winner": label_b if abs_b.mean() < abs_a.mean() else label_a,
    }


def compute_segment_predictions(df, segment_col):
    """
    Compute leave-one-out mean predictions based on a segmentation column.
    This simulates what the model does: predict gestation as the historical
    average for that segment.
    """
    predictions = []
    for idx, row in df.iterrows():
        segment_value = row[segment_col]
        # Leave-one-out: compute mean of all OTHER projects in the same segment
        mask = (df[segment_col] == segment_value) & (df.index != idx)
        segment_peers = df.loc[mask, 'actual_days']

        if len(segment_peers) >= 2:
            predicted = segment_peers.mean()
        else:
            # Fall back to global mean if segment too small
            global_mask = df.index != idx
            predicted = df.loc[global_mask, 'actual_days'].mean()

        predictions.append(predicted)

    df[f'predicted_{segment_col}'] = predictions
    return df


def compute_metrics(actual, predicted, label):
    """Compute a full suite of accuracy metrics."""
    signed_error = predicted - actual
    abs_error = np.abs(signed_error)

    metrics = {
        'segmentation': label,
        'n_projects': len(actual),
        'mae_days': round(abs_error.mean(), 2),
        'medae_days': round(np.median(abs_error), 2),
        'rmse_days': round(np.sqrt((signed_error ** 2).mean()), 2),
        'bias_days': round(signed_error.mean(), 2),
        'within_7d_pct': round(100 * (abs_error <= 7).mean(), 2),
        'within_14d_pct': round(100 * (abs_error <= 14).mean(), 2),
        'within_30d_pct': round(100 * (abs_error <= 30).mean(), 2),
        'mape_pct': round(100 * (abs_error / actual.replace(0, np.nan)).mean(), 2),
        'corr_pred_actual': round(np.corrcoef(predicted, actual)[0, 1], 4),
    }
    return metrics


def paired_significance_test(actual, pred_a, pred_b, label_a, label_b):
    """
    Diebold-Mariano style test: are the absolute errors from the two
    methods significantly different?
    Uses a paired t-test on absolute errors.
    """
    abs_error_a = np.abs(pred_a - actual)
    abs_error_b = np.abs(pred_b - actual)
    error_diff = abs_error_a - abs_error_b

    t_stat, p_value = stats.ttest_rel(abs_error_a, abs_error_b)

    # Wilcoxon signed-rank test (non-parametric alternative)
    try:
        w_stat, w_p_value = stats.wilcoxon(abs_error_a, abs_error_b)
    except ValueError:
        w_stat, w_p_value = np.nan, np.nan

    result = {
        'comparison': f'{label_a} vs {label_b}',
        'mean_error_diff': round(error_diff.mean(), 2),
        'paired_t_stat': round(t_stat, 4),
        'paired_t_pvalue': round(p_value, 4),
        'wilcoxon_stat': round(w_stat, 4) if not np.isnan(w_stat) else None,
        'wilcoxon_pvalue': round(w_p_value, 4) if not np.isnan(w_p_value) else None,
        'significant_at_005': p_value < 0.05,
        'winner': label_b if error_diff.mean() > 0 else label_a
    }
    return result


def segment_level_comparison(df, segment_col_a, segment_col_b):
    """Compare accuracy across sub-segments to see where each method wins."""
    results = []
    for (proj_type, category), group in df.groupby(['type', 'category']):
        if len(group) < 5:
            continue

        actual = group['actual_days'].values
        pred_a = group[f'predicted_{segment_col_a}'].values
        pred_b = group[f'predicted_{segment_col_b}'].values

        mae_a = round(np.abs(pred_a - actual).mean(), 2)
        mae_b = round(np.abs(pred_b - actual).mean(), 2)

        results.append({
            'type': proj_type,
            'category': category,
            'n': len(group),
            f'mae_{segment_col_a}': mae_a,
            f'mae_{segment_col_b}': mae_b,
            'mae_diff': round(mae_a - mae_b, 2),
            'winner': segment_col_b if mae_a > mae_b else segment_col_a
        })

    return pd.DataFrame(results).sort_values('mae_diff', ascending=False)


def run_comparison():
    """Main comparison pipeline."""
    engine = get_db_engine()
    df = load_evaluation_set(engine)
    model_preds = load_model_predictions(engine)

    # Merge production predictions (optional reference line)
    df = df.merge(model_preds, left_on='monday_id', right_on='project_id', how='inner')

    print(f"Loaded {len(df)} projects for evaluation")
    print(f"Unique product_type values: {df['product_type'].nunique()}")
    print(f"Unique product_key values:  {df['product_key'].nunique()}")
    print()

    # Shadow A/B on same projects with leave-one-out medians
    print("Computing predictions with LEGACY hierarchical backoff...")
    df = compute_hierarchical_predictions(df, LEGACY_BACKOFF_TIERS, "legacy_backoff")

    print("Computing predictions with NEW hierarchical backoff...")
    df = compute_hierarchical_predictions(df, NEW_BACKOFF_TIERS, "new_backoff")

    # Optional existing baselines for context
    print("Computing predictions with product_type segmentation...")
    df = compute_segment_predictions(df, 'product_type')

    print("Computing predictions with product_key segmentation...")
    df = compute_segment_predictions(df, 'product_key')

    print("\n" + "=" * 70)
    print("OVERALL ACCURACY COMPARISON")
    print("=" * 70)

    metrics_model = compute_metrics(
        df['actual_days'], df['model_predicted_days'], 'production_model'
    )

    metrics_legacy = compute_metrics(
        df['actual_days'], df['predicted_legacy_backoff'], 'legacy_backoff_sim'
    )

    metrics_new = compute_metrics(
        df['actual_days'], df['predicted_new_backoff'], 'new_backoff_sim'
    )

    comparison_df = pd.DataFrame([metrics_model, metrics_legacy, metrics_new])
    print(comparison_df.to_string(index=False))

    # Paired significance: legacy vs new (same rows, paired deltas)
    print("\n" + "=" * 70)
    print("PAIRED SIGNIFICANCE: LEGACY vs NEW BACKOFF")
    print("=" * 70)

    paired_legacy_vs_new = paired_backoff_eval(
        df['actual_days'].values,
        df['predicted_legacy_backoff'].values,
        df['predicted_new_backoff'].values,
        'legacy_backoff',
        'new_backoff',
    )

    for k, v in paired_legacy_vs_new.items():
        print(f"  {k}: {v}")

    # Tier usage distributions
    print("\n" + "=" * 70)
    print("TIER USAGE DISTRIBUTION")
    print("=" * 70)

    legacy_tier_usage = tier_usage_distribution(
        df, "tier_legacy_backoff", "segment_n_legacy_backoff", LEGACY_BACKOFF_TIERS
    )

    new_tier_usage = tier_usage_distribution(
        df, "tier_new_backoff", "segment_n_new_backoff", NEW_BACKOFF_TIERS
    )

    print("\nLegacy tier usage:")
    print(legacy_tier_usage.to_string(index=False))

    print("\nNew tier usage:")
    print(new_tier_usage.to_string(index=False))

    # Segment-level A/B (type + category)
    print("\n" + "=" * 70)
    print("SEGMENT-LEVEL COMPARISON: LEGACY vs NEW BACKOFF")
    print("=" * 70)

    seg_comparison = segment_level_comparison(df, 'legacy_backoff', 'new_backoff')
    print(seg_comparison.to_string(index=False))

    return comparison_df, paired_legacy_vs_new, legacy_tier_usage, new_tier_usage, seg_comparison, df


if __name__ == '__main__':
    (
        comparison_df,
        paired_legacy_vs_new,
        legacy_tier_usage,
        new_tier_usage,
        seg_comparison,
        eval_df,
    ) = run_comparison()