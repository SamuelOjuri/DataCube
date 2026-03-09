import logging

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import numpy as np
import pandas as pd

from ..models import StatusCategory

logger = logging.getLogger(__name__)

# Value bands for grouping project values
VALUE_BAND_BINS = [-np.inf, 0, 15_000, 40_000, 100_000, np.inf]
VALUE_BAND_LABELS = [
    "Zero",
    "Small (<15k)",
    "Medium (15-40k)",
    "Large (40-100k)",
    "XLarge (>100k)",
]

SEASON_MAP = {
    12: "winter", 1: "winter", 2: "winter",
    3: "spring", 4: "spring", 5: "spring",
    6: "summer", 7: "summer", 8: "summer",
    9: "autumn", 10: "autumn", 11: "autumn",
}
GESTATION_TEMPORAL_CATEGORY = ["season"]
GESTATION_TEMPORAL_NUMERIC = [
    "created_year",
    "created_quarter",
    "created_month",
    "days_since_created",
    "days_since_updated",
    "recent_activity_days",
]
GESTATION_CATEGORY_COLUMNS = [
    "account",
    "type",
    "category",
    "product_type",
    "project_key",
    "value_band",
    "total_order_value",
    *GESTATION_TEMPORAL_CATEGORY,
]
GESTATION_NUMERIC_COLUMNS = [
    "log_value",
    "new_enquiry_value",
    "duration_days",
    "gestation_target",
    *GESTATION_TEMPORAL_NUMERIC,
]

class FeatureLoader:
    """
    Responsible for fetching raw project data and transforming it into
    feature matrices and survival/quantile targets for ML models.
    """

    def __init__(self, db_client):
        self.db = db_client

    
    def fetch_historical_data(
        self,
        lookback_days: int = 1460,
        *,
        page_size: int = 1000,
        max_pages: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Bulk-fetch historical projects for training.

        Supabase applies a hidden 100-row limit, so we paginate explicitly.

        """
        try:
            cutoff = (
                datetime.now() - pd.Timedelta(days=lookback_days)
            ).strftime("%Y-%m-%d")

            frames: List[pd.DataFrame] = []
            offset = 0
            pages = 0

            while True:
                query = (
                    self.db.client.table("projects")
                    .select("*")
                    .gte("date_created", cutoff)
                    .order("date_created", desc=True)
                    .range(offset, offset + page_size - 1)
                )
                response = query.execute()
                rows = response.data or []

                if not rows:
                    break

                frames.append(pd.DataFrame(rows))
                pages += 1
                offset += page_size

                if len(rows) < page_size:
                    break
                if max_pages and pages >= max_pages:
                    logger.warning(
                        "Historical fetch stopped after %s pages (max_pages=%s)",
                        pages,
                        max_pages,
                    )
                    break

            if not frames:
                logger.warning("Historical fetch returned 0 rows")
                return pd.DataFrame()

            df = pd.concat(frames, ignore_index=True)
            return self._preprocess(df)

        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to fetch historical data: %s", exc)
            return pd.DataFrame()


    def build_aft_frame(
        self,
        *,
        lookback_days: int = 1460,
        min_gestation_days: int = 5,
        max_gestation_days: Optional[int] = 1460,
        snapshot_path: Optional[Path] = None,
    ) -> pd.DataFrame:
        """
        Produce a dataset for AFT survival training.
        This includes both events (won) and censored (open/lost) rows,
        which is required for survival analysis.

        Args:
            lookback_days: Historical window for training data.
            min_gestation_days: Minimum gestation for events (default 5).
            max_gestation_days: Optional upper cap on gestation (default 1460).
            snapshot_path: Optional path to save CSV snapshot.

        Returns:
            DataFrame with all required survival columns for AFT training.
        """
        df = self.fetch_historical_data(lookback_days=lookback_days)
        if df.empty:
            logger.warning("AFT frame: no historical data available.")
            return df

        # Keep both events and censored rows for survival analysis
        dataset = df.copy()

        # Ensure survival columns exist
        required_cols = [
            "event_observed",
            "is_censored",
            "time_open_days",
            "duration_days",
            "gestation_target",
            "date_created",
            "status_category",
        ]

        for col in required_cols:
            if col not in dataset.columns:
                logger.warning("AFT frame: missing required column '%s'", col)
                if col in ["event_observed", "is_censored"]:
                    dataset[col] = 0
                elif col in ["time_open_days", "duration_days"]:
                    dataset[col] = 0.0
                elif col == "gestation_target":
                    dataset[col] = np.nan
                else:
                    dataset[col] = None

        # Preserve missingness for gestation_target; do not fill with 0
        dataset["gestation_target"] = pd.to_numeric(
            dataset["gestation_target"], errors="coerce"
        )
        dataset.loc[dataset["gestation_target"] <= 0, "gestation_target"] = np.nan

        dataset["time_open_days"] = pd.to_numeric(
            dataset["time_open_days"], errors="coerce"
        ).fillna(0.0)
        dataset["duration_days"] = pd.to_numeric(
            dataset["duration_days"], errors="coerce"
        ).fillna(0.0)
        dataset["event_observed"] = pd.to_numeric(
            dataset["event_observed"], errors="coerce"
        ).fillna(0).astype(int)
        dataset["is_censored"] = (dataset["event_observed"] == 0).astype(int)

        event_mask = dataset["event_observed"] == 1

        # Censored rows do not have realized gestation-to-win
        dataset.loc[~event_mask, "gestation_target"] = np.nan

        # Safety fallback for event rows with missing gestation target
        missing_event_target = event_mask & dataset["gestation_target"].isna()
        dataset.loc[missing_event_target, "gestation_target"] = dataset.loc[
            missing_event_target, "duration_days"
        ]

        # Apply gestation constraints to event rows only
        if min_gestation_days > 0:
            invalid_events = event_mask & (
                dataset["gestation_target"].isna()
                | (dataset["gestation_target"] < min_gestation_days)
            )
            dataset = dataset.loc[~invalid_events].copy()
            event_mask = dataset["event_observed"] == 1

        if max_gestation_days:
            dataset.loc[event_mask, "gestation_target"] = dataset.loc[
                event_mask, "gestation_target"
            ].clip(upper=max_gestation_days)

        # AFT training time: event time for won rows, censor time otherwise
        dataset["aft_observed_time"] = np.where(
            dataset["event_observed"] == 1,
            dataset["gestation_target"],
            dataset["duration_days"],
        ).astype(float)

        # Ensure categorical columns exist
        for col in GESTATION_CATEGORY_COLUMNS:
            if col not in dataset.columns:
                dataset[col] = "Unknown"

        # Ensure numeric columns exist
        for col in GESTATION_NUMERIC_COLUMNS:
            if col not in dataset.columns:
                dataset[col] = np.nan if col == "gestation_target" else 0.0

        dataset = dataset.reset_index(drop=True)

        if snapshot_path:
            snapshot_path = Path(snapshot_path)
            snapshot_path.parent.mkdir(parents=True, exist_ok=True)
            dataset.to_csv(snapshot_path, index=False)
            logger.info(
                "Wrote AFT training snapshot → %s (rows=%s, events=%s)",
                snapshot_path,
                len(dataset),
                dataset["event_observed"].sum(),
            )

        return dataset


    def prepare_features(self, project_dict: Dict[str, Any]) -> pd.DataFrame:
        """
        Prepare a single project for inference.
        """
        df = pd.DataFrame([project_dict or {}])
        return self._preprocess(df)

    # --------------------------------------------------------------------- #
    # Internal helpers
    # --------------------------------------------------------------------- #

    def _preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean, engineer, and derive ML features and targets.
        """
        df = df.copy()
        df = self._coerce_datetime_columns(df)
        df = self._coerce_numeric_columns(df)
        df = self._assign_value_bands(df)
        df = self._derive_survival_targets(df)
        df = self._standardize_categories(df)
        df = self._add_temporal_features(df)
        return df

    def _coerce_datetime_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse and standardize all relevant datetime columns, ensuring no time zone info remains.
        """
        date_cols = [
            "date_created",
            "updated_at",
            "date_project_won",
            "date_project_closed",
            "first_date_invoiced",
            "last_date_designed",
            "date_order_received",
        ]
        for col in date_cols:
            if col in df.columns:
                converted = pd.to_datetime(df[col], errors="coerce", utc=True)
                df[col] = converted.dt.tz_convert(None)
        now = pd.Timestamp.utcnow().tz_localize(None)
        df["date_created"] = df.get("date_created").fillna(now)
        df["updated_at"] = df.get("updated_at").fillna(now)
        return df

    def _coerce_numeric_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Ensure all relevant fields are coerced to float and engineer log+numeric features.
        """
        df["new_enquiry_value"] = pd.to_numeric(
            df.get("new_enquiry_value", 0), errors="coerce"
        ).fillna(0.0)
        df["log_value"] = np.log1p(df["new_enquiry_value"].clip(lower=0.0))
        if "gestation_period" in df.columns:
            df["gestation_period"] = pd.to_numeric(
                df["gestation_period"], errors="coerce"
            )
        return df

    def _assign_value_bands(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Bucketize project value into a small set of bands.
        """
        derived = pd.cut(
            df["new_enquiry_value"],
            bins=VALUE_BAND_BINS,
            labels=VALUE_BAND_LABELS,
            include_lowest=True,
        )
        if "value_band" not in df.columns:
            df["value_band"] = derived
        else:
            mask = df["value_band"].isna() | (df["value_band"].astype(str).str.strip() == "")
            df.loc[mask, "value_band"] = derived[mask]
        df["value_band"] = (
            df["value_band"].astype(str).replace({"nan": "Unknown"}).fillna("Unknown")
        )
        return df

    def _derive_survival_targets(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Compute durations, censor flags, and targets for survival/quantile analysis.
        """
        now = pd.Timestamp.utcnow().tz_localize(None)
        observation_end = pd.Series(pd.NaT, index=df.index)

        # The first valid of these is the closure-of-interest date.
        for col in [
            "date_project_won",
            "date_project_closed",
            "first_date_invoiced",
            "last_date_designed",
        ]:
            if col in df.columns:
                observation_end = observation_end.combine_first(df[col])

        observation_end = observation_end.fillna(now)
        df["observation_end_date"] = observation_end

        durations = (observation_end - df["date_created"]).dt.days
        df["duration_days"] = durations.fillna(0).clip(lower=0).round().astype(int)

        df["time_open_days"] = (
            (now - df["date_created"]).dt.days.fillna(df["duration_days"])
        ).clip(lower=0).round().astype(int)

        # Event/censor logic based on project status
        status = df.get("status_category", pd.Series("", index=df.index)).fillna("")
        df["event_observed"] = (status == StatusCategory.WON.value).astype(int)
        df["is_censored"] = (df["event_observed"] == 0).astype(int)
        df["censor_reason"] = np.where(
            df["event_observed"] == 1,
            "won",
            np.where(status == StatusCategory.LOST.value, "lost", "open"),
        )

        # Target for regression (realized win time only)
        df["gestation_target"] = pd.to_numeric(
            df.get("gestation_period"), errors="coerce"
        )

        # 0/negative is invalid
        df.loc[df["gestation_target"] <= 0, "gestation_target"] = np.nan

        won_mask = df["event_observed"] == 1

        # Censored rows do not have realized gestation-to-win
        df.loc[~won_mask, "gestation_target"] = np.nan

        # For won rows only, fallback to observed duration if missing
        missing_gestation = won_mask & df["gestation_target"].isna()
        df.loc[missing_gestation, "gestation_target"] = df.loc[
            missing_gestation, "duration_days"
        ]

        return df


    def _standardize_categories(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Ensure all categorical columns are present, string, and have no blanks/NAs.
        """
        for col in ["account", "type", "category", "product_type", "value_band", *GESTATION_TEMPORAL_CATEGORY]:
            if col not in df.columns:
                df[col] = "Unknown"
            df[col] = (
                df[col]
                .fillna("Unknown")
                .astype(str)
                .str.strip()
                .replace("", "Unknown")
            )
        return df

    def _add_temporal_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Derive additional temporal signals (seasonality, recency) for ML models.
        """
        df = df.copy()
        now = pd.Timestamp.utcnow().tz_localize(None)

        created = df["date_created"]
        updated = df["updated_at"]

        df["created_year"] = created.dt.year.fillna(now.year).astype(int)
        df["created_quarter"] = created.dt.quarter.fillna(1).astype(int)
        df["created_month"] = created.dt.month.fillna(1).astype(int)

        df["season"] = (
            df["created_month"]
            .map(SEASON_MAP)
            .fillna("unknown")
            .astype(str)
            .str.lower()
        )

        df["days_since_created"] = (
            (now - created).dt.days.fillna(0).clip(lower=0).astype(int)
        )

        df["days_since_updated"] = (
            (now - updated).dt.days.fillna(df["days_since_created"]).clip(lower=0).astype(int)
        )

        df["recent_activity_days"] = (
            (updated - created).dt.days.fillna(0).clip(lower=0).astype(int)
        )

        return df

