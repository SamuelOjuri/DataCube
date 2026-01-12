"""
Analyze win time distribution to determine optimal evaluation horizons.

Usage:
    python -m scripts.analyze_win_times
"""

import pandas as pd
import numpy as np
from pathlib import Path


def main():
    # Find the latest snapshot
    processed_dir = Path("data/processed")
    snapshots = sorted(processed_dir.glob("survival_training_*.csv"), reverse=True)
    
    if not snapshots:
        print("No survival training snapshots found!")
        return
    
    snapshot_path = snapshots[0]
    print(f"Analyzing: {snapshot_path.name}")
    print("=" * 60)
    
    df = pd.read_csv(snapshot_path)
    
    # Filter to won projects only
    won = df[df["event_observed"] == 1].copy()
    
    print("\n=== WIN TIME DISTRIBUTION ===")
    print(f"Total won projects: {len(won)}")
    print()
    print("Duration (days) statistics:")
    print(won["duration_days"].describe())
    print()
    print("Percentiles:")
    for p in [10, 25, 50, 75, 90, 95]:
        val = won["duration_days"].quantile(p / 100)
        count_within = (won["duration_days"] <= val).sum()
        print(f"  {p}th percentile: {val:.0f} days ({count_within} projects win by this time)")
    
    print()
    print("=== HORIZON ANALYSIS (All Won Projects) ===")
    horizons = [30, 60, 90, 120, 180, 270, 365, 500, 730]
    print(f"{'Horizon':<10} {'Won within':<12} {'Won after':<12} {'Positive %':<12}")
    print("-" * 50)
    for h in horizons:
        within = (won["duration_days"] <= h).sum()
        after = (won["duration_days"] > h).sum()
        pct = within / len(won) * 100
        print(f"{h:<10} {within:<12} {after:<12} {pct:.1f}%")
    
    print()
    print("=== FULL DATASET BREAKDOWN ===")
    print(f"Total rows: {len(df)}")
    print(f"Won (event=1): {(df['event_observed'] == 1).sum()}")
    print(f"Open/Lost (event=0): {(df['event_observed'] == 0).sum()}")
    
    # Check validation set specifically (newest 20%)
    df["date_created"] = pd.to_datetime(df["date_created"])
    df_sorted = df.sort_values("date_created")
    split_idx = int(len(df_sorted) * 0.8)
    val_df = df_sorted.iloc[split_idx:]
    val_won = val_df[val_df["event_observed"] == 1]
    
    print()
    print("=== VALIDATION SET (newest 20%) ===")
    print(f"Val total: {len(val_df)}")
    print(f"Val won: {len(val_won)}")
    
    if len(val_won) > 0:
        print()
        print("Val won duration stats:")
        print(val_won["duration_days"].describe())
        print()
        print("Val horizon analysis:")
        for h in horizons:
            within = (val_won["duration_days"] <= h).sum()
            pct = within / len(val_won) * 100 if len(val_won) > 0 else 0
            print(f"  {h}d: {within} wins ({pct:.1f}%)")
    
    # Recommend horizons
    print()
    print("=== RECOMMENDATIONS ===")
    median = won["duration_days"].median()
    q25 = won["duration_days"].quantile(0.25)
    q75 = won["duration_days"].quantile(0.75)
    
    print(f"Median win time: {median:.0f} days")
    print(f"IQR: {q25:.0f} - {q75:.0f} days")
    print()
    print("Suggested horizons for evaluation:")
    print(f"  - Short-term: {int(q25)} days (25th percentile)")
    print(f"  - Medium-term: {int(median)} days (median)")
    print(f"  - Long-term: {int(q75)} days (75th percentile)")
    print()
    print("For best ROC/PR discrimination, choose horizons near the median")
    print("where ~50% of wins fall on each side of the cutoff.")


if __name__ == "__main__":
    main()

