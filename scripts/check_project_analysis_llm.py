"""Exercise project reasoning with synthetic inputs; never read or write CRM/database data."""
from __future__ import annotations

import argparse
import json

from src.config import GEMINI_MODEL
from src.core.llm_analyzer import LLMAnalyzer
from src.core.models import NumericPredictions, ProjectFeatures, SegmentStatistics


def synthetic_cases():
    for name, sample_size, tier, rating in (
        ("supported-segment", 40, 0, 65),
        ("sparse-segment", 3, 5, 2),
        ("upper-rating-bound", 40, 0, 99),
    ):
        stats = SegmentStatistics(
            segment_keys=["type", "category"], sample_size=sample_size, backoff_tier=tier,
            wins=10 if sample_size == 40 else 1, losses=30 if sample_size == 40 else 2,
            gestation_median=120, gestation_p25=90, gestation_p75=150,
            gestation_count=sample_size,
        )
        project = ProjectFeatures(
            project_id=name, name="Synthetic qualification project", account="Synthetic account",
            type="New Build", category="Commercial", product_type="Foil Faced PIR",
            product_key="pir_foil", new_enquiry_value=25000, value_band="Medium (15-40k)",
            pipeline_stage="Open Enquiry",
        )
        predictions = NumericPredictions(
            expected_gestation_days=120, gestation_confidence=0.6,
            gestation_range={"p25": 90, "p75": 150}, expected_conversion_rate=0.25,
            conversion_confidence=0.6, rating_score=rating, rating_components={},
            segment_statistics=stats,
        )
        yield project, predictions, stats


def check_model(model):
    analyzer = LLMAnalyzer(model=model)
    reports = []
    try:
        for project, baseline, stats in synthetic_cases():
            output, metadata = analyzer.analyze_project(project, baseline, stats)
            report = {"case": project.project_id, "metadata": metadata,
                      "baseline": baseline.model_dump(mode="json")}
            if metadata.get("error"):
                report["passed"] = False
            else:
                result = analyzer.create_final_analysis(project, baseline, output, metadata)
                adjustment = output.adjustments["rating_adjustment"]
                report.update(
                    passed=(result.predictions.expected_gestation_days == baseline.expected_gestation_days
                            and result.predictions.expected_conversion_rate == baseline.expected_conversion_rate
                            and -5 <= adjustment <= 5
                            and result.predictions.rating_score == max(1, min(100, baseline.rating_score + adjustment))),
                    output=output.model_dump(mode="json"),
                    final_rating=result.predictions.rating_score,
                )
            reports.append(report)
        return {"model": model, "passed": all(case["passed"] for case in reports),
                "cases": reports, "usage": analyzer.get_usage_statistics()}
    finally:
        analyzer.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", default=GEMINI_MODEL)
    parser.add_argument("--compare-model", help="Optional second model; sends the same synthetic inputs.")
    args = parser.parse_args()
    reports = []
    for model in dict.fromkeys(filter(None, (args.model, args.compare_model))):
        try:
            reports.append(check_model(model))
        except Exception:
            reports.append({"model": model, "passed": False, "error": "model_configuration_error"})
    print(json.dumps(reports, indent=2, default=str))
    return 0 if all(report["passed"] for report in reports) else 1


if __name__ == "__main__":
    raise SystemExit(main())
