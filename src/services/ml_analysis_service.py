import logging

from datetime import datetime
from pathlib import Path
from time import perf_counter
from typing import Any, Dict, Optional
from collections import defaultdict

import json
import pandas as pd

from ..config import OUTPUTS_DIR
from ..config import SURVIVAL_MODEL_PATH, AFT_MODEL_PATH, GESTATION_MODEL_TYPE
from ..core.ml.dataset import FeatureLoader
from ..core.ml.survival import SurvivalModel
from ..core.ml.aft_model import AFTGestationModel, load_aft_model
from ..database.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)

class MLAnalysisService:
    """
    Service for executing Machine Learning based analysis in Shadow Mode.
    This runs alongside the standard analysis to validate new models.
    Gestation predictions are produced via CatBoost AFT (p25/p50/p75).
    """
    def __init__(self, db_client: Optional[SupabaseClient] = None):
        self.db = db_client or SupabaseClient()
        self.feature_loader = FeatureLoader(self.db)
        self.survival_model = SurvivalModel()
        self.survival_model_path = Path(SURVIVAL_MODEL_PATH) if SURVIVAL_MODEL_PATH else None
        self.survival_model_version: Optional[str] = None
        self.survival_loaded = False
        self.eventual_horizon_days = 5 * 365  # treat 5-year horizon as "eventual"
        self._load_survival_model()

        # Gestation model selection (AFT-only; legacy LightGBM QuantileModel removed)
        self.gestation_model_type = (GESTATION_MODEL_TYPE or "aft").lower()
        if self.gestation_model_type != "aft":
            logger.warning(
                "GESTATION_MODEL_TYPE=%s is no longer supported; forcing 'aft'.",
                self.gestation_model_type,
            )
            self.gestation_model_type = "aft"
        self.aft_model: Optional[AFTGestationModel] = None
        self.aft_model_loaded = False

        # Load AFT model artifact (if available)
        self._load_aft_model()

    def _resolve_survival_artifact(self) -> Optional[Path]:
        if not self.survival_model_path:
            return None
        path = self.survival_model_path
        if path.is_file():
            return path
        if path.is_dir():
            candidates = sorted(
                path.glob("*.pkl"),
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )
            return candidates[0] if candidates else None
        return None

    def _load_survival_model(self) -> None:
        artifact_path = self._resolve_survival_artifact()
        if not artifact_path:
            logger.warning("No survival model artifact found at %s", self.survival_model_path)
            return
        try:
            self.survival_model.load(artifact_path)
            self.survival_model_version = (
                self.survival_model.metadata.get("model_version") or artifact_path.stem
            )
            self.survival_loaded = True
            logger.info(
                "Loaded survival model (%s) from %s",
                self.survival_model_version,
                artifact_path,
            )
        except Exception as exc:
            logger.error("Failed to load survival model artifact: %s", exc, exc_info=True)
            self.survival_loaded = False

    def _resolve_aft_artifact(self) -> Optional[Path]:
        """Find the most recent AFT model artifact."""
        aft_path = Path(AFT_MODEL_PATH) if AFT_MODEL_PATH else None
        if not aft_path:
            return None
        if aft_path.is_file():
            return aft_path
        if aft_path.is_dir():
            # Look for aft_*.pkl files
            candidates = sorted(
                aft_path.glob("aft_*.pkl"),
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )
            return candidates[0] if candidates else None
        return None

    def _load_aft_model(self) -> None:
        """Load the CatBoost AFT model for gestation predictions."""
        artifact_path = self._resolve_aft_artifact()
        if not artifact_path:
            logger.warning("No AFT model artifact found at %s", AFT_MODEL_PATH)
            return
        try:
            self.aft_model = load_aft_model(artifact_path)
            self.aft_model_loaded = True
            logger.info(
                "Loaded AFT model from %s (dist=%s, scale=%.1f)",
                artifact_path,
                self.aft_model.dist,
                self.aft_model.scale,
            )
        except Exception as exc:
            logger.error("Failed to load AFT model artifact: %s", exc, exc_info=True)
            self.aft_model_loaded = False

    def _predict_gestation_quantiles(self, features_df: pd.DataFrame) -> Dict[str, Any]:
        """
        Predict gestation quantiles using CatBoost AFT only.

        Returns:
            Dict with p25/p50/p75 in days, plus AFT metadata fields if available.
        """
        if self.aft_model_loaded and self.aft_model is not None:
            return self.aft_model.predict(features_df)
        raise RuntimeError("AFT model artifact is not loaded; cannot produce gestation quantiles.")

    def _predict_survival_probabilities(self, feature_row: pd.DataFrame) -> Dict[Any, float]:
        """Return horizon probabilities or fall back to neutral priors."""
        if not self.survival_loaded:
            return {}
        try:
            horizons = [30, 90, 270, self.eventual_horizon_days]
            raw = self.survival_model.predict_win_probabilities(
                feature_row,
                horizons=horizons,
                current_age_col="time_open_days",
            )
            eventual = raw.pop(self.eventual_horizon_days, None)
            if eventual is not None:
                raw["eventual"] = eventual
            return raw
        except Exception as exc:  # noqa: BLE001
            logger.warning("Survival prediction failed; using neutral priors: %s", exc)
            return {}

    def _current_model_version(self) -> str:
        """Create a descriptive version tag we can persist alongside results."""
        if not self.survival_model_version:
            return "shadow-v0.1"
        trained_at = self.survival_model.metadata.get("timestamp_utc")
        if trained_at:
            return f"{self.survival_model_version}@{trained_at}"
        return self.survival_model_version

    def _record_shadow_metrics(self, payload: Dict[str, Any]) -> None:
        try:
            metrics_path = OUTPUTS_DIR / "analysis" / "shadow_metrics_daily.json"
            metrics_path.parent.mkdir(parents=True, exist_ok=True)
            if metrics_path.exists():
                data = json.loads(metrics_path.read_text(encoding="utf-8"))
            else:
                data = {}
            today = datetime.utcnow().strftime("%Y-%m-%d")
            entry = data.get(today, {"count": 0, "avg_win_prob_30d": 0.0, "model_version": payload["model_version"]})
            count = entry["count"] + 1
            entry["avg_win_prob_30d"] = (
                entry["avg_win_prob_30d"] * entry["count"] + payload.get("win_prob_30d", 0.5)
            ) / count
            entry["avg_win_prob_270d"] = (
                entry.get("avg_win_prob_270d", 0.0) * entry["count"] + payload.get("win_prob_270d", 0.5)
            ) / count
            entry["count"] = count
            entry["model_version"] = payload.get("model_version", entry["model_version"])
            data[today] = entry
            metrics_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        except Exception as exc:  # noqa: BLE001
            logger.debug("Failed to record shadow metrics: %s", exc)

    def analyze_and_store(self, project_id: str) -> Dict[str, Any]:
        """
        Perform ML analysis and store results in the shadow table.
        """
        from time import perf_counter

        timing_start = perf_counter()

        try:
            response = (
                self.db.client.table("projects")
                .select("*")
                .eq("monday_id", project_id)
                .single()
                .execute()
            )
            if not response.data:
                logger.warning("ML Analysis: Project %s not found", project_id)
                return {"success": False, "error": "Not found"}

            project = response.data
            features_df = self.feature_loader.prepare_features(project)
            row = features_df.iloc[0]
            event_observed = int(row.get("event_observed", 0))
            actual_gestation = None
            if event_observed == 1:
                # Prefer explicit gestation_period if present; otherwise use observed duration
                actual_gestation = row.get("gestation_period")
                if pd.isna(actual_gestation):
                    actual_gestation = row.get("duration_days")
                if pd.notna(actual_gestation):
                    actual_gestation = int(actual_gestation)
            feature_count = features_df.shape[1]
            survival_probs = self._predict_survival_probabilities(features_df)
            win_prob_30d = survival_probs.get(30, 0.5)
            win_prob_90d = survival_probs.get(90, 0.5)
            win_prob_270d = survival_probs.get(270, 0.5)
            eventual_prob = survival_probs.get("eventual", 0.5)
            gestation_estimates = self._predict_gestation_quantiles(features_df)
            
            # Extract base predictions
            p25 = gestation_estimates.get("p25")
            p50 = gestation_estimates.get("p50")
            p75 = gestation_estimates.get("p75")
            cycle_category = gestation_estimates.get("cycle_category")
            confidence_note = gestation_estimates.get("confidence_note")
            
            # Widen intervals for long-cycle projects to reflect higher uncertainty
            if cycle_category == "long" and p25 is not None and p75 is not None:
                spread = p75 - p25
                p25 = max(0, int(p25 - spread * 0.1))  # Expand by 10% on each side
                p75 = int(p75 + spread * 0.1)
            
            rating_score = int(win_prob_270d * 50 + 30)  # Placeholder composite
            processing_time_ms = int((perf_counter() - timing_start) * 1000)

            result_payload = {
                "project_id": project_id,
                "analysis_timestamp": datetime.now().isoformat(),
                "win_prob_30d": float(win_prob_30d),
                "win_prob_90d": float(win_prob_90d),
                "win_prob_270d": float(win_prob_270d),
                "eventual_win_prob": float(eventual_prob),
                "gestation_p25": p25,
                "gestation_p50": p50,
                "gestation_p75": p75,
                "cycle_category": cycle_category,
                "confidence_note": confidence_note,
                "rating_score": rating_score,
                "event_observed": event_observed,
                "actual_gestation_days": actual_gestation,
                "model_version": self._current_model_version(),
                "features_used": project,
                "processing_time_ms": processing_time_ms,
            }

            (
                self.db.client.table("ml_analysis_results")
                .upsert(
                    result_payload,
                    on_conflict="project_id,analysis_timestamp",
                )
                .execute()
            )

            self._record_shadow_metrics(result_payload)

            logger.info(
                "ML shadow inference | project=%s survival_loaded=%s win30=%.3f win90=%.3f win270=%.3f eventual=%.3f features=%d duration_ms=%d",
                project_id,
                self.survival_loaded,
                win_prob_30d,
                win_prob_90d,
                win_prob_270d,
                eventual_prob,
                feature_count,
                processing_time_ms,
            )
            return {"success": True, "result": result_payload}

        except Exception as exc:  # noqa: BLE001
            logger.error("ML Shadow Analysis failed for %s: %s", project_id, exc, exc_info=True)
            return {"success": False, "error": str(exc)}

