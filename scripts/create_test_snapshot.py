"""
Create a test snapshot from fresh projects for model evaluation.

Usage:
    python -m scripts.create_test_snapshot --cutoff-date 2025-12-15 --output test_snapshot.csv
    python -m scripts.create_test_snapshot --cutoff-date 2025-12-15 --output data/processed/test_snapshot.csv
"""

import argparse
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.core.ml.dataset import FeatureLoader
from src.database.supabase_client import SupabaseClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("create_test_snapshot")


def main():
    parser = argparse.ArgumentParser(
        description="Create test snapshot for projects after a cutoff date"
    )
    parser.add_argument(
        "--cutoff-date",
        type=str,
        default="2025-12-15",
        help="Only include projects created after this date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="data/processed/test_snapshot.csv",
        help="Output path for test snapshot CSV",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=365,
        help="How far back to query (from today)",
    )
    parser.add_argument(
        "--min-duration",
        type=int,
        default=5,
        help="Minimum duration_days to include",
    )
    
    args = parser.parse_args()
    
    # Parse cutoff date
    cutoff_date = pd.to_datetime(args.cutoff_date)
    logger.info("Creating test snapshot for projects after: %s", cutoff_date.date())
    
    # Fetch data from Supabase
    logger.info("Fetching data from Supabase (lookback=%d days)...", args.lookback_days)
    client = SupabaseClient()
    loader = FeatureLoader(client)
    
    # Get historical data
    raw_df = loader.fetch_historical_data(lookback_days=args.lookback_days)
    
    if raw_df.empty:
        logger.error("No data returned from database")
        return
    
    logger.info("Fetched %d total projects", len(raw_df))
    
    # Filter for projects created after cutoff
    raw_df['date_created'] = pd.to_datetime(raw_df['date_created'], errors='coerce')
    test_df = raw_df[raw_df['date_created'] > cutoff_date].copy()
    
    logger.info("Found %d projects created after %s", len(test_df), cutoff_date.date())
    
    if test_df.empty:
        logger.warning("No projects found after cutoff date")
        return
    
    # Apply minimum duration filter (like training)
    if 'duration_days' in test_df.columns:
        before_filter = len(test_df)
        test_df = test_df[test_df['duration_days'] >= args.min_duration].copy()
        logger.info(
            "Applied min_duration=%d filter: %d → %d rows",
            args.min_duration,
            before_filter,
            len(test_df)
        )
    
    # Remove rows with missing event_observed
    if 'event_observed' in test_df.columns:
        before_filter = len(test_df)
        test_df = test_df[test_df['event_observed'].notna()].copy()
        logger.info(
            "Removed rows with missing event_observed: %d → %d rows",
            before_filter,
            len(test_df)
        )
    
    if test_df.empty:
        logger.warning("No valid test samples after filtering")
        return
    
    # Save snapshot
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    test_df.to_csv(output_path, index=False)
    
    logger.info("✓ Saved test snapshot: %s", output_path)
    logger.info("  Total rows: %d", len(test_df))
    
    if 'event_observed' in test_df.columns:
        n_won = test_df['event_observed'].sum()
        logger.info("  Won projects: %d (%.1f%%)", n_won, 100 * n_won / len(test_df))
    
    if 'duration_days' in test_df.columns:
        logger.info("  Median duration: %.1f days", test_df['duration_days'].median())
        logger.info("  Duration range: %.0f - %.0f days", 
                   test_df['duration_days'].min(),
                   test_df['duration_days'].max())
    
    logger.info("  Date range: %s to %s",
               test_df['date_created'].min().date(),
               test_df['date_created'].max().date())


if __name__ == "__main__":
    main()