"""Deterministic answer text for exploratory results; numbers come only from result cells."""
from decimal import Decimal

from ..workflow.contracts import ChartIntent, EvidenceClaim, GroundedAnswer, ResultReference
from ..workflow.evidence import EvidenceError
from ..workflow.summary_language import SUMMARY_VERSION
from .contracts import ExploratoryResult
from .summary import summarize

MAX_ROWS_IN_TEXT = 8
MAX_CLAIMS = 24


def number(value, kind):
    if value is None or kind not in {"integer", "decimal"}:
        return None
    result = Decimal(str(value))
    if not result.is_finite():
        raise EvidenceError("nonfinite_value")
    return result


def formatted(value, unit, kind):
    amount = number(value, kind)
    if amount is None:
        return "unknown" if value is None else str(value)
    return {"source_currency": f"GBP {amount:f}", "ratio": f"{amount * 100:f}%", "percent": f"{amount:f}%",
            "days": f"{amount:f} days"}.get(unit, f"{amount:f}")


def ground_exploration(result: ExploratoryResult, *, asks_for_cause=False) -> GroundedAnswer:
    p = result.provenance
    dimensions = [i for i, c in enumerate(p.columns) if c.role == "dimension"]
    measures = [i for i, c in enumerate(p.columns) if c.role == "measure"] or [
        i for i in range(len(p.columns)) if i not in dimensions]
    single = len(result.rows) == 1 and not dimensions
    lines, claims = [], []
    for i, row in enumerate(result.rows[:MAX_ROWS_IN_TEXT]):
        prefix = " / ".join("Unclassified" if row[j] is None else str(row[j]) for j in dimensions)
        for j in measures:
            column = p.columns[j]
            label = f"{prefix} — {column.label}" if prefix else column.label
            lines.append(f"{label}: {formatted(row[j], column.unit, column.type)}.")
            value = number(row[j], column.type)
            if value is not None and len(claims) < MAX_CLAIMS:
                claims.append(EvidenceClaim(id=f"row_{i}_{column.name}", result_id=result.id, location=f"rows.{i}.{j}",
                                            metric_id="exploratory", unit=column.unit, period=p.title, value=value,
                                            label=label, kind="total" if single else "contributor"))
    if not result.rows:
        lines.append("No matching records were found for this scope.")
    elif len(result.rows) > MAX_ROWS_IN_TEXT:
        lines.append(f"{len(result.rows) - MAX_ROWS_IN_TEXT} further rows are shown in the table.")
    partial = partial_answer(result, measures)
    notices = list(p.limitations)
    notices.append("Source freshness is unknown; query time does not establish ingestion or refresh time.")
    notices.extend(partial)
    if any(c.unit == "source_currency" for c in p.columns):
        notices.append("Currency: GBP. Amounts are as stored; VAT inclusion is unspecified.")
    if asks_for_cause:
        notices.append("These are measured values and differences; they do not establish causes.")
    numeric = single and len(measures) == 1 and p.columns[measures[0]].type in {"integer", "decimal"}
    summary, summary_claims, summary_notices, exploratory_chart = summarize(result)
    if asks_for_cause:
        summary_notices.append("These measured values and differences do not establish causes.")
    return GroundedAnswer(
        result=ResultReference(result_id=result.id, query_reference=p.query_reference,
                               catalogue_sha256=p.catalogue_sha256),
        claims=[*claims, *summary_claims], text="\n".join([p.title + ".", *lines, *partial]),
        chart=ChartIntent(kind="number" if numeric else "table"),
        notices=list(dict.fromkeys([*notices, *summary_notices])), scope_changes={},
        summary=summary, summary_version=SUMMARY_VERSION, summary_notices=summary_notices, exploratory_chart=exploratory_chart)


def partial_answer(result: ExploratoryResult, measures) -> list[str]:
    """Plain sentences on what the answer does not cover; counts come from the stored result."""
    p = result.provenance
    sentences = list(p.caveats)
    unknown = sum(row[j] is None for row in result.rows for j in measures)
    if unknown:
        sentences.append(f"{unknown} of {len(result.rows) * len(measures)} values are unknown because no records "
                         "matched or the matching records have no value.")
    if p.truncated:
        sentences.append(f"Only the first {p.returned_rows} rows are included; the query matched more.")
    return sentences


def matched_nothing(result: ExploratoryResult) -> bool:
    """No rows, or only NULL/zero measures, as aggregates over no matching records return."""
    if result.provenance.truncated:
        return False
    measures = [i for i, c in enumerate(result.provenance.columns) if c.role == "measure"]
    return not result.rows or bool(measures) and all(
        row[j] is None or row[j] == 0 for row in result.rows for j in measures)
