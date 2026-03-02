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

    # Merge actual model predictions
    df = df.merge(model_preds, left_on='monday_id', right_on='project_id', how='inner')

    print(f"Loaded {len(df)} projects for evaluation")
    print(f"Unique product_type values: {df['product_type'].nunique()}")
    print(f"Unique product_key values:  {df['product_key'].nunique()}")
    print()

    # --- Generate predictions under both segmentation approaches ---
    print("Computing predictions with product_type segmentation...")
    df = compute_segment_predictions(df, 'product_type')

    print("Computing predictions with product_key segmentation...")
    df = compute_segment_predictions(df, 'product_key')

    # --- Overall metrics comparison (3-way: actual model vs both baselines) ---
    print("\n" + "=" * 70)
    print("OVERALL ACCURACY COMPARISON")
    print("=" * 70)

    metrics_model = compute_metrics(
        df['actual_days'], df['model_predicted_days'], 'production_model (product_key)'
    )
    metrics_product_type = compute_metrics(
        df['actual_days'], df['predicted_product_type'], 'baseline: product_type mean'
    )
    metrics_product_key = compute_metrics(
        df['actual_days'], df['predicted_product_key'], 'baseline: product_key mean'
    )

    comparison_df = pd.DataFrame([metrics_model, metrics_product_type, metrics_product_key])
    print(comparison_df.to_string(index=False))

    # --- Is the production model better than product_type baseline? ---
    print("\n" + "=" * 70)
    print("SIGNIFICANCE: Production Model vs product_type Baseline")
    print("=" * 70)

    sig_model_vs_type = paired_significance_test(
        df['actual_days'].values,
        df['model_predicted_days'].values,
        df['predicted_product_type'].values,
        'production_model',
        'product_type_baseline'
    )
    for k, v in sig_model_vs_type.items():
        print(f"  {k}: {v}")

    # --- product_type vs product_key baseline ---
    print("\n" + "=" * 70)
    print("SIGNIFICANCE: product_type vs product_key Baselines")
    print("=" * 70)

    sig_type_vs_key = paired_significance_test(
        df['actual_days'].values,
        df['predicted_product_type'].values,
        df['predicted_product_key'].values,
        'product_type',
        'product_key'
    )
    for k, v in sig_type_vs_key.items():
        print(f"  {k}: {v}")

    # --- Where does the production model struggle? ---
    print("\n" + "=" * 70)
    print("PRODUCTION MODEL: ACCURACY BY SEGMENT")
    print("=" * 70)

    for (proj_type, category), group in df.groupby(['type', 'category']):
        if len(group) < 5:
            continue
        actual = group['actual_days'].values
        predicted = group['model_predicted_days'].values
        mae = round(np.abs(predicted - actual).mean(), 2)
        medae = round(np.median(np.abs(predicted - actual)), 2)
        bias = round((predicted - actual).mean(), 2)
        print(f"  {proj_type:15s} | {category:12s} | n={len(group):3d} | MAE={mae:7.2f} | MedAE={medae:7.2f} | Bias={bias:7.2f}")

    # --- Segment-level breakdown ---
    print("\n" + "=" * 70)
    print("SEGMENT-LEVEL COMPARISON: product_type vs product_key baselines")
    print("=" * 70)

    seg_comparison = segment_level_comparison(df, 'product_type', 'product_key')
    print(seg_comparison.to_string(index=False))

    # --- Granularity analysis ---
    print("\n" + "=" * 70)
    print("SEGMENT SIZE ANALYSIS (risk of overfitting with finer granularity)")
    print("=" * 70)

    for col in ['product_type', 'product_key']:
        sizes = df.groupby(col).size()
        print(f"\n  {col}:")
        print(f"    Number of segments: {len(sizes)}")
        print(f"    Mean segment size:  {sizes.mean():.1f}")
        print(f"    Median segment size: {sizes.median():.1f}")
        print(f"    Min segment size:   {sizes.min()}")
        print(f"    Segments with < 5:  {(sizes < 5).sum()}")
        print(f"    Segments with < 10: {(sizes < 10).sum()}")

    return comparison_df, sig_model_vs_type, sig_type_vs_key, seg_comparison


if __name__ == '__main__':
    comparison_df, sig_model_vs_type, sig_type_vs_key, seg_comparison = run_comparison()