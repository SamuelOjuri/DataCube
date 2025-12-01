import logging
from datetime import datetime
from pathlib import Path
from time import perf_counter
from typing import Any, Dict, Optional
from collections import defaultdict

import json
import pandas as pd

from ..config import OUTPUTS_DIR
from ..config import SURVIVAL_MODEL_PATH
from ..core.ml.dataset import FeatureLoader
from ..core.ml.quantiles import QuantileModel
from ..core.ml.survival import SurvivalModel
from ..database.supabase_client import SupabaseClient



logger = logging.getLogger(__name__)

class MLAnalysisService:
    """
    Service for executing Machine Learning based analysis in Shadow Mode.
    This runs alongside the standard analysis to validate new models.
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
        self.quantile_model = QuantileModel()

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

    def _predict_survival_probabilities(self, feature_row: pd.DataFrame) -> Dict[Any, float]:
        """Return horizon probabilities or fall back to neutral priors."""
        if not self.survival_loaded:
            return {}
        try:
            horizons = [30, 90, self.eventual_horizon_days]
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
            feature_count = features_df.shape[1]
            survival_probs = self._predict_survival_probabilities(features_df)
            win_prob_30d = survival_probs.get(30, 0.5)
            win_prob_90d = survival_probs.get(90, 0.5)
            eventual_prob = survival_probs.get("eventual", 0.5)
            gestation_estimates = self.quantile_model.predict(features_df)
            rating_score = int(win_prob_30d * 50 + 30)  # Placeholder composite
            processing_time_ms = int((perf_counter() - timing_start) * 1000)

            result_payload = {
                "project_id": project_id,
                "analysis_timestamp": datetime.now().isoformat(),
                "win_prob_30d": float(win_prob_30d),
                "win_prob_90d": float(win_prob_90d),
                "eventual_win_prob": float(eventual_prob),
                "gestation_p25": gestation_estimates.get("p25"),
                "gestation_p50": gestation_estimates.get("p50"),
                "gestation_p75": gestation_estimates.get("p75"),
                "rating_score": rating_score,
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
                "ML shadow inference | project=%s survival_loaded=%s win30=%.3f win90=%.3f eventual=%.3f features=%d duration_ms=%d",
                project_id,
                self.survival_loaded,
                win_prob_30d,
                win_prob_90d,
                eventual_prob,
                feature_count,
                processing_time_ms,
            )
            return {"success": True, "result": result_payload}

        except Exception as exc:  # noqa: BLE001
            logger.error("ML Shadow Analysis failed for %s: %s", project_id, exc, exc_info=True)
            return {"success": False, "error": str(exc)}

