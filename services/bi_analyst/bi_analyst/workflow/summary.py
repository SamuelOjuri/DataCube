"""Conversational summaries for every certified metric result shape.

Complete aggregates stay distinct from displayed groups. Monthly calculations
reuse the exploratory fact engine through a typed adapter with identical row
coordinates; rate and mean aggregates are never summed across groups.
"""
from ..exploration.contracts import ExploratoryColumn, ExploratoryProvenance, ExploratoryResult
from ..exploration.summary import amount, summarize
from .contracts import EvidenceClaim
from .summary_language import CONVERSION_MATURITY, comparison_sentence, display, month_text, period_scope, total_sentence

CONVERSION = {
    "conversion_five_year": "inclusive", "conversion_two_year": "inclusive",
    "conversion_closed_five_year": "closed-only", "conversion_closed_two_year": "closed-only",
}


def period_text(period):
    """Keep exact boundaries in evidence claims; prose uses summary_language."""
    if period is None:
        return "all stored history"
    if period.end_date_exclusive is None:
        return f"projects created from {period.start_date} onward"
    return f"{period.start_date} to {period.end_date_exclusive} (end exclusive)"


def monthly_dataset(result):
    p = result.provenance
    columns = []
    for c in p.columns:
        measure = c.name in {"value", "numerator", "denominator"}
        columns.append(ExploratoryColumn(name=c.name, label=p.metric_label if c.name == "value" else c.name,
            type=c.type, role="measure" if measure else "dimension", unit=c.unit or "none",
            time_grain="month" if c.name == "month" else None,
            additive=measure and c.unit in {"source_currency", "count"}, grouped=True,
            numerator="numerator" if c.name == "value" and p.unit == "ratio" else None,
            denominator="denominator" if c.name == "value" and p.unit == "ratio" else None))
    # Diagnostic count columns are not dimensions or primary measures.
    names = [i for i, c in enumerate(columns) if c.name not in {"source_rows", "known_values", "share"}]
    # Keep original positions so all evidence references still address the owned
    # result. Diagnostics are harmless measures ordered after the primary value.
    for i, c in enumerate(columns):
        if i not in names:
            columns[i] = c.model_copy(update={"role": "measure", "unit": "none", "additive": False})
    provenance = ExploratoryProvenance(query_reference=p.query_reference, policy_version=p.compiler_version,
        catalogue_version=p.catalogue_version, catalogue_sha256=p.catalogue_sha256, title=p.metric_label,
        sql="", relations=[p.source_relation.rsplit('.', 1)[-1]], columns=columns,
        business_timezone=p.business_timezone, freshness=p.freshness, limitations=[],
        truncated=p.truncated, truncation_reasons=p.truncation_reasons,
        returned_rows=p.returned_rows, dataset_scope=p.dataset_scope)
    return ExploratoryResult(id=result.id, run_id=result.run_id, permissions_version=result.permissions_version,
        columns=result.columns, rows=result.rows, provenance=provenance, created_at=result.created_at)


def summarize_metric(result):
    p, rows = result.provenance, result.rows
    total, claims, notices = p.total, [], []
    scope, variant = period_text(p.resolved_period), CONVERSION.get(p.metric_id)

    def fact(key, value, *, source, unit=None, label=None, kind="total"):
        item = EvidenceClaim(id=f"summary_{key}", result_id=result.id, location=source,
            metric_id=p.metric_id, unit=unit or p.unit, period=scope, value=amount(value),
            label=label or p.metric_label, kind=kind, sources=[source])
        claims.append(item)
        return item

    if variant:
        notices.append(CONVERSION_MATURITY + (" Closed-only rates also depend on which projects have closed."
                                             if variant == "closed-only" else ""))
        if variant == "closed-only":
            notices.append("Open projects are excluded from closed-only conversion; the denominator includes invoiced wins and lost projects.")
    if total:
        fact("total", total.value, source="provenance.total.value")
        if total.source_rows == 0:
            return total_sentence(p, variant), claims, notices
        if total.known_values < total.source_rows:
            notices.append("Some matching records have unknown values. A known subtotal may be incomplete; unknown values are not zero.")
        if (variant and total.value is not None and total.numerator is not None
                and total.denominator is not None and total.denominator > 0):
            fact("numerator", total.numerator, source="provenance.total.numerator", unit="count", label="Invoiced wins")
            fact("denominator", total.denominator, source="provenance.total.denominator", unit="count", label="Closed projects" if variant == "closed-only" else "Eligible projects")

    sentences = [total_sentence(p, variant)]
    limited = p.truncated or p.dataset_scope == "limited"
    if limited:
        notices.append("The displayed groups are limited. The overall aggregate, when shown, covers the complete filtered population.")
    if rows and p.request.grain == "month" and not p.request.dimensions:
        _, monthly_claims, monthly_notices, _ = summarize(monthly_dataset(result))
        claims.extend(c.model_copy(update={"metric_id": p.metric_id}) for c in monthly_claims)
        notices.extend(n for n in monthly_notices if "only the returned rows" not in n)
        available = {c.id: c for c in monthly_claims}
        before, after = available.get("summary_first_half"), available.get("summary_second_half")
        change = available.get("summary_change")
        first, latest = available.get("summary_first"), available.get("summary_latest")
        if before and after and change:
            direction = "higher" if change.value > 0 else "lower" if change.value < 0 else "unchanged"
            half = len(rows) // 2
            measure = "combined rate" if p.unit == "ratio" else "monthly average"
            difference = f"{display(abs(change.value), change.unit)} {direction}" if change.value else "unchanged"
            sentences.append(f"The {measure} was {display(before.value, p.unit)} for the first {half} months and "
                f"{display(after.value, p.unit)} for the last {half} months "
                f"({difference}).")
        elif first and latest:
            month = result.columns.index("month")
            a, b = int(first.sources[0].split('.')[1]), int(latest.sources[0].split('.')[1])
            if a != b:
                prefix = "Among the returned months, the" if limited else "The"
                sentences.append(f"{prefix} first known monthly value was {display(first.value, p.unit)} in {month_text(rows[a][month])} "
                    f"and the latest was {display(latest.value, p.unit)} in {month_text(rows[b][month])}.")
            elif len(rows) > 1 or limited:
                prefix = "Among the returned months, the" if limited else "The"
                sentences.append(f"{prefix} only known monthly value was {display(first.value, p.unit)} in {month_text(rows[a][month])}.")
        elif not first:
            notices.append("The returned rows do not provide an unambiguous monthly trend.")
    elif rows and p.request.dimensions and "value" in result.columns:
        index = result.columns.index("value")
        known = [n for n, row in enumerate(rows) if row[index] is not None]
        if known:
            ordering = next((o for o in p.request.ordering if o.column == "value"), None)
            lowest = ordering is not None and ordering.direction == "asc"
            top = (min if lowest else max)(known, key=lambda n: amount(rows[n][index]))
            value = amount(rows[top][index])
            keys = (["month"] if p.request.grain == "month" else []) + list(p.request.dimensions)
            labels = [str(rows[top][result.columns.index(d)])[:180] if rows[top][result.columns.index(d)] is not None
                      else "Unclassified" for d in keys]
            label = " / ".join(labels)
            fact("leader", value, source=f"rows.{top}.{index}", label=label, kind="contributor")
            # Friendly month names belong in prose, not in the source claim label.
            label = " / ".join(month_text(value) if key == "month" and value != "Unclassified" else value for key, value in zip(keys, labels))
            qualifier = "Among the returned groups" if limited else "Among the groups"
            if len(known) < len(rows):
                qualifier += " with known values"
            direction = "lowest" if lowest else "highest"
            tied = sum(amount(rows[n][index]) == value for n in known) > 1
            finding = f"{qualifier}, {label} {'was tied for' if tied else 'had'} the {direction} value at {display(value, p.unit)}."
            if ordering:
                sentences.insert(0, finding)
            else:
                sentences.append(finding)
        else:
            notices.append("All returned group values are unknown; no leading group can be identified.")

    change = p.comparison
    if change:
        fact("baseline", change.baseline.value, source="provenance.comparison.baseline.value")
        if change.baseline.source_rows == 0 or change.baseline.value is None or total is None or total.value is None:
            notices.append("A comparison cannot be calculated because a current or baseline aggregate is unknown or has no matching records.")
        else:
            delta = change.percentage_point_change if p.unit == "ratio" else change.absolute_change
            unit = "percentage_points" if p.unit == "ratio" else p.unit
            if delta is not None:
                field = "percentage_point_change" if p.unit == "ratio" else "absolute_change"
                fact("comparison_change", delta, source=f"provenance.comparison.{field}", unit=unit, kind=field)
                sentences.append(comparison_sentence(change.baseline.value, delta, unit, p.unit, p.comparison_period))
        if change.zero_denominator:
            notices.append("Relative percentage change is undefined because the baseline is zero or unknown.")
        if change.baseline.known_values < change.baseline.source_rows:
            notices.append("Some baseline records have unknown values, so the comparison may be incomplete.")
        comparison_filters = p.request.comparison.filters if p.request.comparison else None
        if comparison_filters is not None and comparison_filters != p.request.filters:
            notices.append("The baseline uses different filters from the current scope; see the comparison details.")
    if total is None and not rows:
        sentences = [f"No matching records were found {period_scope(p.resolved_period)}."]
    return " ".join(sentences[:3]), claims, list(dict.fromkeys(notices))
