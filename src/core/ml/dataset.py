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
    "value_band",
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
        lookback_days: int = 730,
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


    def build_gestation_frame(
        self,
        *,
        lookback_days: int = 1095,
        min_gestation_days: int = 5,
        max_gestation_days: Optional[int] = 730,
        clip_quantiles: Optional[Tuple[float, float]] = (0.01, 0.99),
        snapshot_path: Optional[Path] = None,
    ) -> pd.DataFrame:
        """
        Produce a cleaned dataset of completed projects for quantile regression
        training and optionally persist the flattened snapshot to disk.
        """
        df = self.fetch_historical_data(lookback_days=lookback_days)
        if df.empty:
            logger.warning("Gestation frame: no historical data available.")
            return df

        dataset = df[df["event_observed"] == 1].copy()

        if dataset.empty:
            logger.warning("Gestation frame: zero completed projects after filtering.")
            return dataset

        dataset["gestation_target"] = pd.to_numeric(dataset.get("gestation_target"), errors="coerce")
        dataset = dataset[dataset["gestation_target"].notna()]
        dataset = dataset[dataset["gestation_target"] >= max(min_gestation_days, 0)]

        if dataset.empty:
            logger.warning("Gestation frame: no rows left after enforcing gestation target constraints.")
            return dataset

        if clip_quantiles:
            lo_q, hi_q = clip_quantiles
            if 0.0 <= lo_q < hi_q <= 1.0:
                lower = dataset["gestation_target"].quantile(lo_q)
                upper = dataset["gestation_target"].quantile(hi_q)
                dataset["gestation_target"] = dataset["gestation_target"].clip(lower=lower, upper=upper)
        if max_gestation_days:
            dataset["gestation_target"] = dataset["gestation_target"].clip(upper=max_gestation_days)

        dataset["gestation_target"] = dataset["gestation_target"].round().astype(int)
        dataset["duration_days"] = dataset["duration_days"].clip(lower=min_gestation_days)

        for col in GESTATION_CATEGORY_COLUMNS:
            if col not in dataset.columns:
                dataset[col] = "Unknown"
        for col in GESTATION_NUMERIC_COLUMNS:
            if col not in dataset.columns:
                dataset[col] = 0

        ordered_features = (
            GESTATION_CATEGORY_COLUMNS
            + GESTATION_NUMERIC_COLUMNS
            + ["date_created"]
        )
        dataset = dataset[ordered_features].reset_index(drop=True)

        if snapshot_path:
            snapshot_path = Path(snapshot_path)
            snapshot_path.parent.mkdir(parents=True, exist_ok=True)
            dataset.to_csv(snapshot_path, index=False)
            logger.info(
                "Wrote gestation training snapshot → %s (rows=%s)",
                snapshot_path,
                len(dataset),
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
            "date_order_received",
        ]:
            if col in df.columns:
                observation_end = observation_end.combine_first(df[col])
        observation_end = observation_end.fillna(df["updated_at"]).fillna(now)
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
        # Target for regression
        df["gestation_target"] = pd.to_numeric(
            df.get("gestation_period"), errors="coerce"
        )
        won_mask = df["event_observed"] == 1
        missing_gestation = won_mask & df["gestation_target"].isna()
        # Use observed duration as gestation if it's missing, but only for "won"
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

