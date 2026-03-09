"""
Export CatBoost AFT training dataset to CSV.

This uses FeatureLoader.build_aft_frame so the exported file matches
the columns/logic required by AFTTrainer/train_aft.py.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.ml.dataset import FeatureLoader
from src.database.supabase_client import SupabaseClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("export_aft_training_data")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export CatBoost AFT training dataset to CSV."
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=1095,
        help="Historical window to fetch projects (default: 1095).",
    )
    parser.add_argument(
        "--min-gestation-days",
        type=int,
        default=0,
        help="Minimum gestation days for events (default: 0).",
    )
    parser.add_argument(
        "--max-gestation-days",
        type=int,
        default=None,
        help="Maximum gestation days cap for events (default: no cap).",
    )
    parser.add_argument(
        "--output-path",
        type=str,
        default=None,
        help="Output CSV path. Default: data/processed/aft_training_<timestamp>.csv",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    if args.output_path:
        output_path = Path(args.output_path)
    else:
        output_path = project_root / "data" / "processed" / f"aft_training_{timestamp}.csv"

    output_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info("Fetching data from Supabase...")
    client = SupabaseClient()
    loader = FeatureLoader(client)

    dataset = loader.build_aft_frame(
        lookback_days=args.lookback_days,
        min_gestation_days=args.min_gestation_days,
        max_gestation_days=args.max_gestation_days,
        snapshot_path=output_path,  # writes CSV directly
    )

    if dataset.empty:
        raise SystemExit("No data exported. Dataset is empty.")

    # Ensure persisted even if snapshot writing behavior changes
    if not output_path.exists():
        dataset.to_csv(output_path, index=False)

    n_total = len(dataset)
    n_events = int(pd.to_numeric(dataset.get("event_observed", 0), errors="coerce").fillna(0).sum())
    n_censored = n_total - n_events

    logger.info("Export complete: %s", output_path)
    logger.info("Rows=%d | Events=%d | Censored=%d", n_total, n_events, n_censored)
    print(str(output_path))


if __name__ == "__main__":
    main()