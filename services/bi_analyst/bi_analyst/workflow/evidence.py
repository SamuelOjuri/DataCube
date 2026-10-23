"""Claims are addresses into an owned result, rendered using exact stored decimals."""
from decimal import Decimal

from .contracts import ChartIntent, EvidenceClaim, GroundedAnswer, Presentation, ResultReference
from ..metrics.contracts import MetricResult
from .errors import EvidenceError
from ..answer_presentation.service import present_result
from ..answer_presentation.metric_chart import select_metric_chart
from .summary_language import SUMMARY_VERSION


def candidates(result: MetricResult) -> list[EvidenceClaim]:
    p = result.provenance
    period = p.resolved_period.model_dump_json() if p.resolved_period else "all_stored"
    common = dict(result_id=result.id, metric_id=p.metric_id, unit=p.unit, period=period)
    claims = []
    if p.total is not None:
        claims.append(EvidenceClaim(id="total", location="provenance.total.value", kind="total",
                                    value=p.total.value, denominator=p.total.denominator,
                                    label=p.metric_label, **common))
    if p.comparison:
        for name in ("absolute_change", "percentage_change", "percentage_point_change"):
            if name == "percentage_point_change" and p.unit != "ratio":
                continue
            claims.append(EvidenceClaim(id=name, location=f"provenance.comparison.{name}", kind=name,
                value=getattr(p.comparison, name), denominator=p.comparison.baseline.value,
                label=p.metric_label, **{**common, "unit": "percent" if name == "percentage_change" else
                    "percentage_points" if name == "percentage_point_change" else p.unit,
                    "period": period + " compared with " + (p.comparison_period.model_dump_json() if p.comparison_period else "all_stored")}))
    if p.request.dimensions or p.request.grain == "month":
        value_index = result.columns.index("value")
        denominator_index = result.columns.index("denominator") if "denominator" in result.columns else None
        labels = [i for i, name in enumerate(result.columns) if name in {"category", "type", "month"}]
        for i, row in enumerate(result.rows[:8]):
            value = row[value_index]
            claims.append(EvidenceClaim(id=f"row_{i}", location=f"rows.{i}.value", kind="contributor",
                value=Decimal(str(value)) if value is not None else None,
                denominator=Decimal(str(row[denominator_index])) if denominator_index is not None and row[denominator_index] is not None else None,
                label=" / ".join(str(row[j]) if row[j] is not None else "Unclassified" for j in labels), **common))
    return claims


def validate_chart(intent: ChartIntent, result: MetricResult):
    p = result.provenance
    if intent.kind == "number" and (p.total is None or intent.x is not None):
        raise EvidenceError("invalid_number_chart")
    if intent.kind == "comparison" and (intent.x is not None or p.total is None or p.comparison is None
            or p.total.value is None or p.comparison.baseline.value is None):
        raise EvidenceError("invalid_comparison_chart")
    if intent.kind in {"bar", "line"}:
        if (intent.x not in result.columns or not result.rows or len(result.rows) > 100
                or p.truncated or len(p.request.dimensions) + (p.request.grain == "month") != 1):
            raise EvidenceError("unsuitable_chart")
        if intent.kind == "line" and (intent.x != "month" or p.request.grain != "month"):
            raise EvidenceError("unsuitable_line_chart")
        if any(row[result.columns.index("value")] is None for row in result.rows):
            raise EvidenceError("chart_has_unknown_values")


def select_presentation(result: MetricResult) -> Presentation:
    """Select evidence and a chart using the same closed rules used for validation."""
    claims = candidates(result)
    if not claims:
        raise EvidenceError("no_evidence")
    claim_ids = [claim.id for claim in claims[:12]]
    chart = select_metric_chart(result)
    presentation = Presentation(claim_ids=claim_ids, chart=chart)
    validate_chart(presentation.chart, result)
    return presentation


def ground(result: MetricResult, presentation: Presentation, scope_changes: dict, *, asks_for_cause=False):
    available = {claim.id: claim for claim in candidates(result)}
    if len(set(presentation.claim_ids)) != len(presentation.claim_ids) or not set(presentation.claim_ids) <= available.keys():
        raise EvidenceError("unidentified_claim")
    # Always display the complete filtered total, even when the model selects contributors.
    ids = list(dict.fromkeys(["total", *presentation.claim_ids])) if "total" in available else presentation.claim_ids
    claims = [available[key] for key in ids]
    validate_chart(presentation.chart, result)
    p = result.provenance
    notices = list(p.limitations)
    notices.append("Source freshness is unknown; query time does not establish ingestion or refresh time.")
    if p.total and p.total.source_rows == 0:
        notices.append("No matching source records were found.")
    if p.total and p.total.known_values < p.total.source_rows:
        notices.append("Some matching records have unknown values; the known subtotal may be incomplete.")
    if any(p.coverage.values()):
        notices.append("Source coverage diagnostics apply to the unfiltered gateway population; see result provenance.")
    if p.truncated:
        notices.append("Displayed rows are limited. The total covers the complete filtered population.")
    if p.comparison and p.comparison.zero_denominator:
        notices.append("Percentage change is undefined because the baseline is zero or unknown.")
    if asks_for_cause:
        notices.append("These are measured values and differences; they do not establish causes.")
    if p.unit == "source_currency":
        notices.append("Currency: GBP. Amounts are as stored; VAT inclusion is unspecified.")
    lines = []
    for claim in claims:
        value = claim.value
        if value is not None and not value.is_finite():
            raise EvidenceError("nonfinite_value")
        formatted = "unknown" if value is None else (
            f"{value * 100:f}%" if claim.unit == "ratio" else
            f"GBP {value:f}" if claim.unit == "source_currency" else
            f"{value:f} {claim.unit.replace('_', ' ')}")
        label = claim.label if claim.kind in {"total", "contributor"} else claim.kind.replace("_", " ").capitalize()
        # Text is plain text, never interpreted as HTML/Markdown or executable config.
        lines.append(f"{label}: {formatted}.")
    rendered = present_result(result)
    summary, summary_claims, summary_notices = rendered.summary, rendered.claims, rendered.notices
    if asks_for_cause:
        summary_notices.append("These measured values and differences do not establish causes.")
    return GroundedAnswer(result=ResultReference(result_id=result.id, query_reference=p.query_reference,
        catalogue_sha256=p.catalogue_sha256), claims=[*claims, *summary_claims], text="\n".join(lines), chart=rendered.chart,
        notices=list(dict.fromkeys([*notices, *summary_notices])), scope_changes=scope_changes,
        summary=summary, summary_version=SUMMARY_VERSION, summary_notices=summary_notices,
        summary_kind=rendered.kind, chart_reason=rendered.chart_reason)
