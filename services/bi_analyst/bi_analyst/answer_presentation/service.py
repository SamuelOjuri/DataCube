"""One entry point for intent, grounded findings, narrative and chart selection.

The planner may name a task and columns. The result must support that task before
any calculation or chart is chosen. Unsupported interpretations retain useful
descriptive evidence and an explicit explanation of what cannot be concluded.
"""
from dataclasses import dataclass, field

from ..exploration.comparison import infer_comparison, summarize_comparison, validate_comparison
from ..workflow.contracts import ChartIntent, EvidenceClaim, ExploratoryChartIntent
from ..workflow.errors import EvidenceError
from ..workflow.summary_language import SUMMARY_VERSION
from . import findings
from .calendar import dates_for
from .metric_chart import select_metric_chart


@dataclass
class AnswerPresentation:
    summary: str
    kind: str
    claims: list[EvidenceClaim] = field(default_factory=list)
    notices: list[str] = field(default_factory=list)
    chart: ChartIntent | ExploratoryChartIntent | None = None
    chart_reason: str | None = None

    def __post_init__(self):
        self.summary = self.summary.strip()
        if not self.summary:
            raise EvidenceError("missing_conversational_summary")
        self.notices = list(dict.fromkeys(self.notices))


def present_explanation(answer):
    """Explanation answers already contain prose, but never allow a blank UI slot."""
    summary = answer.text.strip() or "I couldn't produce an answer from the available data. Try a more specific question or a different reporting period."
    return answer.model_copy(update={"summary": summary, "summary_version": SUMMARY_VERSION})


def present_result(result):
    if getattr(result.provenance, "kind", None) == "exploratory":
        return present_exploration(result)
    # Certified handlers retain their approved business definitions and complete
    # aggregates, rather than pooling their displayed groups into a new metric.
    from ..workflow.summary import summarize_metric
    text, claims, notices = summarize_metric(result)
    p = result.provenance
    ranked = p.request.dimensions and any(o.column == "value" for o in p.request.ordering)
    kind = ("comparison" if p.comparison else "trend" if p.request.grain == "month" else "ranking" if ranked else
            "breakdown" if p.request.dimensions else "total")
    if p.total is not None and p.total.source_rows == 0:
        kind = "empty"
    elif p.total is not None and p.total.value is None:
        kind = "unknown"
    chart = select_metric_chart(result)
    reason = None
    if chart.kind == "table":
        reason = ("The table keeps the available values and breakdowns visible without implying a complete, comparable series."
                  if result.rows else "There are no matching values to plot for this scope.")
    return AnswerPresentation(text, kind, claims, notices, chart, reason)


def present_exploration(result):
    from ..exploration.summary import MATURITY, _summarize_known_shape
    e = findings.Evidence(result)
    if "conversion_cohorts_v1" in e.p.relations:
        e.notices.append(MATURITY)
    if not e.rows:
        return AnswerPresentation("No matching records were found for this scope. Try a different period or broader filters.",
            "empty", e.claims, e.notices, chart_reason="There are no matching values to plot.")
    intent = e.p.analysis_intent
    columns = {c.name: i for i, c in enumerate(e.p.columns)}
    invalid = bool(intent and (
        intent.primary_measure is not None and columns.get(intent.primary_measure) not in e.measures
        or intent.secondary_measure is not None and columns.get(intent.secondary_measure) not in e.measures
        or intent.axis is not None and columns.get(intent.axis) not in e.dimensions))
    # Rates take precedence over their supporting counts on time axes. Money
    # takes precedence over supporting shares in a category breakdown.
    dates = [i for i in e.dimensions if e.p.columns[i].type == "date"]
    rates = [i for i in e.measures if e.p.columns[i].unit in {"ratio", "percent"}]
    additive = [i for i in e.measures if e.p.columns[i].additive]
    preferred = (rates or e.measures) if dates else (additive or rates or e.measures)
    primary = columns.get(intent.primary_measure) if intent and not invalid and intent.primary_measure else next(iter(preferred), None)
    secondary = columns.get(intent.secondary_measure) if intent and not invalid and intent.secondary_measure else None
    axis = columns.get(intent.axis) if intent and not invalid and intent.axis else (e.dimensions[0] if len(e.dimensions) == 1 else None)
    pairing = intent.period_comparison if intent and not invalid else None
    pairing = pairing or e.p.monthly_comparison or (infer_comparison(result) if not intent else None)
    if invalid:
        return _fallback(e, primary, "I couldn't match the requested analysis to the available measures and breakdowns. The table shows what was returned.")
    if pairing:
        if validate_comparison(result, pairing) is None:
            return _fallback(e, primary, "I couldn't reliably align the reporting periods for this comparison.")
        text, notices, chart = summarize_comparison(result, pairing, e.claim)
        reason = None if chart else "The available values are shown in the table because a complete, aligned chart could not be established."
        return AnswerPresentation(text, "comparison", e.claims, [*e.notices, *notices], chart, reason)
    kind = intent.kind if intent else (
        "total" if len(e.rows) == 1 and not e.dimensions and e.measures else
        "trend" if len(e.dimensions) == 1 and dates and e.p.columns[dates[0]].time_grain else
        "breakdown" if e.dimensions and primary is not None and e.p.columns[primary].grouped else "records")
    if e.measures and not any(e.known(i) for i in e.measures):
        names = ", ".join(e.p.columns[i].label for i in e.measures[:3])
        return AnswerPresentation(f"The returned records do not contain known values for {names}, so I can't calculate a reliable finding from them.",
            "unknown", e.claims, e.notices, chart_reason="The table retains the unknown values; they are not plotted as zero.")
    output = None
    if kind == "total" and len(e.rows) == 1 and not e.dimensions and e.measures:
        output = findings.scalar(e, primary)
    elif kind == "trend" and primary is not None and axis is not None and len(e.dimensions) == 1:
        grain = e.p.columns[axis].time_grain
        if grain == "month" and dates_for(result, axis, grain) is not None:
            # Keep the existing weighted-rate and equal-half monthly evidence.
            text, claims, notices, chart = _summarize_known_shape(result, primary=primary)
            reason = None if chart else "The table retains the available monthly values without implying a complete trend."
            return AnswerPresentation(text, kind, claims, notices, chart, reason)
        output = findings.trend(e, primary, axis)
    elif kind in {"ranking", "breakdown"} and primary is not None and e.dimensions:
        keys = [tuple(str(row[i]) if row[i] is not None else None for i in e.dimensions) for row in e.rows]
        if len(set(keys)) == len(keys) and (kind == "ranking" or e.p.columns[primary].grouped):
            output = findings.grouped(e, primary, intent.direction if intent else "highest")
    elif kind == "comparison" and primary is not None:
        output = findings.comparison(e, primary, secondary)
    elif kind == "distribution" and primary is not None:
        output = findings.distribution(e, primary)
    elif kind == "relationship" and primary is not None:
        output = findings.relationship(e, primary, secondary)
    elif kind == "records":
        output = findings.records(e, primary)
    if output is None:
        reasons = {
            "comparison": "I can show the available figures, but the result does not identify a reliable baseline for the requested comparison.",
            "trend": "I can show the available figures, but they do not form a clear sequence of distinct reporting periods.",
            "ranking": "I can show the available figures, but the entries do not identify distinct groups that can be ranked reliably.",
            "relationship": "I can show the available figures, but a relationship needs two numerical measures observed together.",
            "total": "I can describe the available values, but the result does not provide a complete overall figure.",
        }
        return _fallback(e, primary, reasons.get(kind, "The result contains several breakdowns that cannot be reduced to a single reliable finding."))
    text, chart, reason = output
    return AnswerPresentation(text, kind, e.claims, e.notices, chart, reason)


def _fallback(e, primary, reason):
    text, chart, _ = findings.records(e, primary)
    return AnswerPresentation(reason + " " + text, "records", e.claims, e.notices, chart, reason)
