
from types import SimpleNamespace

from typing import Dict
import pytest
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.ml.survival import SurvivalModel
from src.services.ml_analysis_service import MLAnalysisService
from tests.test_survival_model import _synthetic_survival_df

def _project_row() -> Dict[str, object]:
    return {
        "monday_id": "proj-123",
        "account": "VIP",
        "type": "New Build",
        "category": "Commercial",
        "product_type": "System A",
        "value_band": "XLarge (>100k)",
        "new_enquiry_value": 120000.0,
        "date_created": "2024-01-01",
        "updated_at": "2024-06-01",
        "pipeline_stage": "Open Enquiry",
        "status_category": "Open",
    }

def _train_artifact(path: Path) -> Path:
    model = SurvivalModel(penalizer=0.5)
    model.fit(_synthetic_survival_df())
    model.metadata.update({"model_version": "unit-test-model", "timestamp_utc": "20990101_000000"})
    artifact_path = path / "survival_unit_test.pkl"
    model.save(artifact_path)
    return artifact_path

class ProjectsTable:
    def __init__(self, row: Dict[str, object]):
        self.row = row
        self._filters = {}

    def select(self, *_):
        return self

    def eq(self, field: str, value: str):
        self._filters[field] = value
        return self

    def single(self):
        return self

    def execute(self):
        if self._filters.get("monday_id") == self.row["monday_id"]:
            return SimpleNamespace(data=self.row)
        return SimpleNamespace(data=None)

class ResultsTable:
    def __init__(self, sink):
        self.sink = sink
        self._payload = None

    def upsert(self, payload, on_conflict=None):
        self.sink.append(payload)
        self._payload = payload
        return self

    def execute(self):
        return SimpleNamespace(data=self._payload)

class DummySupabaseClient:
    def __init__(self, project_row):
        self.project_row = project_row
        self.upserts = []
        self.client = self

    def table(self, name: str):
        if name == "projects":
            return ProjectsTable(self.project_row)
        if name == "ml_analysis_results":
            return ResultsTable(self.upserts)
        raise ValueError(f"Unsupported table {name}")

def test_ml_analysis_service_persists_survival_outputs(monkeypatch, tmp_path):
    artifact = _train_artifact(tmp_path)
    monkeypatch.setattr("src.services.ml_analysis_service.SURVIVAL_MODEL_PATH", str(artifact))

    db = DummySupabaseClient(_project_row())
    service = MLAnalysisService(db_client=db)

    result = service.analyze_and_store("proj-123")
    assert result["success"] is True

    payload = db.upserts[-1]
    assert "win_prob_30d" in payload
    assert "win_prob_90d" in payload
    assert "eventual_win_prob" in payload
    assert payload["processing_time_ms"] > 0
    assert payload["model_version"].startswith("unit-test-model@")

def test_ml_analysis_service_handles_missing_survival_artifact(monkeypatch, tmp_path):
    monkeypatch.setattr("src.services.ml_analysis_service.SURVIVAL_MODEL_PATH", str(tmp_path / "missing-artifact.pkl"))

    db = DummySupabaseClient(_project_row())
    service = MLAnalysisService(db_client=db)

    result = service.analyze_and_store("proj-123")
    assert result["success"] is True

    payload = db.upserts[-1]
    assert payload["win_prob_30d"] == pytest.approx(0.5)
    assert payload["win_prob_90d"] == pytest.approx(0.5)
    assert payload["eventual_win_prob"] == pytest.approx(0.5)
    assert payload["model_version"] == "shadow-v0.1"
