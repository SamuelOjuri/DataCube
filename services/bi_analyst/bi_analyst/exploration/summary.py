"""Small, deterministic summaries over stored rows, with reproducible evidence."""
from datetime import date
from decimal import Decimal, ROUND_HALF_UP

from ..workflow.contracts import EvidenceClaim, ExploratoryChartIntent
from ..workflow.errors import EvidenceError

MATURITY = ("Recent creation cohorts have had less time to reach invoiced completion. "
            "These rates describe recorded outcomes so far, not final conversion performance; "
            "closed-only rates also depend on which projects have closed.")


def amount(value):
    if value is None:
        return None
    result = Decimal(str(value))
    if not result.is_finite():
        raise EvidenceError("nonfinite_value")
    return result


def display(value, unit):
    if value is None:
        return "unknown"
    if unit in {"ratio", "percent", "percentage_points"}:
        value = value * 100 if unit == "ratio" else value
        suffix = " percentage points" if unit == "percentage_points" else "%"
        return f"{value.quantize(Decimal('0.1'), rounding=ROUND_HALF_UP):,}{suffix}"
    if unit == "source_currency":
        return f"GBP {value.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP):,}"
    return f"{value.normalize():,f}" + (" days" if unit == "days" else "")


def monthly_axis(result):
    dimensions = [i for i, c in enumerate(result.provenance.columns) if c.role == "dimension"]
    if len(dimensions) != 1:
        return None
    index = dimensions[0]
    column = result.provenance.columns[index]
    if column.type != "date" or column.time_grain != "month":
        return None
    try:
        dates = [date.fromisoformat(str(row[index])) for row in result.rows]
    except (ValueError, TypeError):
        return None
    if any(d.day != 1 for d in dates) or len(set(dates)) != len(dates):
        return None
    return index


def summarize(result):
    p, rows = result.provenance, result.rows
    claims, notices = [], []
    limited = p.truncated or p.query_limited or p.dataset_scope == "limited"
    if limited:
        notices.append("This summary covers only the returned rows; it does not describe the full matching population.")
    conversion = "conversion_cohorts_v1" in p.relations
    if conversion:
        notices.append(MATURITY)

    def claim(key, value, column=None, *, kind="total", operation="cell", sources=(), unit=None, denominator=None):
        item = EvidenceClaim(id=f"summary_{key}", result_id=result.id, location=f"derived.summary_{key}",
            metric_id="exploratory", period=p.title, value=value, denominator=denominator,
            label=column.label if column else "Returned records", unit=unit or (column.unit if column else "count"),
            kind=kind, operation=operation, sources=list(sources))
        claims.append(item)
        return item

    count = claim("row_count", Decimal(len(rows)), kind="row_count", operation="row_count", sources=["rows"])
    if not rows:
        return "No matching records were found for this scope.", claims, notices, None
    measures = [(i, c) for i, c in enumerate(p.columns) if c.role == "measure" and c.type in {"integer", "decimal"}]
    # Validate all numeric cells, including rows outside the text/table's first page.
    for i, _ in measures:
        for row in rows:
            amount(row[i])
    if any(row[i] is None for row in rows for i, _ in measures):
        notices.append("Some returned values are unknown. They are not treated as zero, and summaries use known values only.")
    rates = [(i, c) for i, c in measures if c.unit in {"ratio", "percent"}]
    axis = monthly_axis(result)
    # Category results may return a supporting share next to revenue. The money
    # is the primary measure there; conversion rates take priority on time axes.
    additive = [(i, c) for i, c in measures if c.additive]
    chosen = (rates or measures) if axis is not None else (additive or rates or measures)
    chart = None
    dimensions = [i for i, c in enumerate(p.columns) if c.role == "dimension"]
    if not limited and chosen and len(dimensions) == 1:
        selected = [c for _, c in chosen if c.unit == chosen[0][1].unit][:3]
        # Lists are not category charts. Only additive grouped data or rates qualify.
        if axis is not None or (len(rows) <= 30 and all(c.grouped and (c.additive or c.unit in {"ratio", "percent"}) for c in selected)
                                and p.columns[dimensions[0]].type == "text"):
            chart = ExploratoryChartIntent(kind="line" if axis is not None else "bar",
                x=p.columns[dimensions[0]].name, series=[c.name for c in selected])
    if axis is not None and chosen:
        index, column = chosen[0]
        ordered = sorted(range(len(rows)), key=lambda n: str(rows[n][axis]))
        known = [n for n in ordered if rows[n][index] is not None]
        if not known:
            return "All returned monthly values are unknown.", claims, notices, None
        if len(known) != len(rows):
            notices.append("Some monthly values are unknown. Comparisons use known values only; missing values are not zero.")
        values = {n: amount(rows[n][index]) for n in known}
        first, last = known[0], known[-1]
        peak, low = max(known, key=values.get), min(known, key=values.get)
        for name, n in (("first", first), ("latest", last), ("peak", peak), ("low", low)):
            claim(name, values[n], column, kind=name, sources=[f"rows.{n}.{index}"])
        rate = column.unit in {"ratio", "percent"}
        change = (values[last] - values[first]) * (100 if column.unit == "ratio" else 1)
        claim("endpoint_change", change, column, kind="percentage_point_change" if rate else "absolute_change",
            operation="difference", sources=[f"rows.{first}.{index}", f"rows.{last}.{index}"],
            unit="percentage_points" if rate else column.unit)
        scope = "Among the returned months, " if limited else ""
        sentences = [f"{scope}{column.label} was {display(values[first], column.unit)} in {rows[first][axis]} "
                     f"and {display(values[last], column.unit)} in {rows[last][axis]}."]

        def aggregate(indices, key):
            if column.unit == "ratio":
                if column.numerator not in result.columns or column.denominator not in result.columns:
                    return None
                a, b = result.columns.index(column.numerator), result.columns.index(column.denominator)
                pairs = [(amount(rows[n][a]), amount(rows[n][b])) for n in indices]
                if any(x is None or y is None or x < 0 or y < 0 or x > y for x, y in pairs):
                    return None
                top, bottom = sum(x for x, _ in pairs), sum(y for _, y in pairs)
                if bottom == 0:
                    return None
                return claim(key, top / bottom, column, kind="weighted_rate", operation="ratio_of_sums",
                    denominator=bottom, sources=[f"rows.{n}.{j}" for n in indices for j in (a, b)])
            if column.unit in {"percent", "days"} or any(rows[n][index] is None for n in indices):
                return None  # No averaging percentages or already-aggregated means.
            return claim(key, sum(amount(rows[n][index]) for n in indices) / len(indices), column,
                kind="average", operation="mean", sources=[f"rows.{n}.{index}" for n in indices])

        overall = aggregate(ordered, "overall")
        if overall:
            label = "Overall" if column.unit == "ratio" else "The monthly average"
            sentences[0] = sentences[0][:-1] + f"; {label.lower()} for these months was {display(overall.value, column.unit)}."
        elif column.unit == "ratio":
            notices.append("An overall rate is unavailable because its supporting counts are missing, invalid or zero.")
        # Compare equal, consecutive halves. Gaps, partial results and odd windows
        # are described with endpoints instead of implying equivalent periods.
        months = [date.fromisoformat(str(rows[n][axis])) for n in ordered]
        contiguous = all(b.year * 12 + b.month == a.year * 12 + a.month + 1 for a, b in zip(months, months[1:]))
        if not limited and contiguous and len(rows) >= 4 and len(rows) % 2 == 0:
            half = len(rows) // 2
            before, after = aggregate(ordered[:half], "first_half"), aggregate(ordered[half:], "second_half")
            if before and after:
                delta = after.value - before.value
                unit = "percentage_points" if column.unit == "ratio" else column.unit
                if column.unit == "ratio":
                    delta *= 100
                claim("change", delta, column, kind="percentage_point_change" if column.unit == "ratio" else "absolute_change",
                    operation="difference", sources=[f"claims.{before.id}.value", f"claims.{after.id}.value"], unit=unit)
                verb = "higher" if delta > 0 else "lower" if delta < 0 else "unchanged"
                comparison = "had a combined rate of" if column.unit == "ratio" else "averaged"
                sentences.append(f"The first {half} months {comparison} {display(before.value, column.unit)} versus "
                    f"{display(after.value, column.unit)} for the last {half} months "
                    f"({display(abs(delta), unit)} {verb}).")
        sentences.append(f"The highest monthly value was {display(values[peak], column.unit)} in {rows[peak][axis]}; "
                         f"the lowest was {display(values[low], column.unit)} in {rows[low][axis]}.")
        return " ".join(sentences), claims, notices, chart

    if len(rows) == 1 and not dimensions and chosen:
        i, c = chosen[0]
        claim("value", amount(rows[0][i]), c, sources=[f"rows.0.{i}"])
        return f"{c.label}: {display(amount(rows[0][i]), c.unit)}.", claims, notices, None
    # Rank a single grouped dimension, never sum rates or use a top-N subtotal as
    # the denominator for a company-wide share.
    if (len(dimensions) == 1 and chosen and chosen[0][1].grouped
            and (chosen[0][1].additive or chosen[0][1].unit in {"ratio", "percent"})
            and p.columns[dimensions[0]].type == "text"):
        i, c = chosen[0]
        known = [n for n, row in enumerate(rows) if row[i] is not None]
        if known:
            top = max(known, key=lambda n: amount(rows[n][i]))
            label = str(rows[top][dimensions[0]])[:180] if rows[top][dimensions[0]] is not None else "Unclassified"
            value = amount(rows[top][i])
            claim("leader", value, c, kind="contributor", sources=[f"rows.{top}.{i}"])
            qualifier = "Among the returned groups" + (" with known values" if len(known) < len(rows) else "")
            text = f"{qualifier}, {label} had the highest {c.label.lower()} at {display(value, c.unit)}."
            total = sum(amount(rows[n][i]) for n in known)
            if c.additive and not limited and len(known) == len(rows) and total > 0 and all(amount(rows[n][i]) >= 0 for n in known):
                share = value / total
                claim("share", share, c, kind="share", operation="share", unit="ratio", denominator=total,
                    sources=[f"rows.{top}.{i}", *[f"rows.{n}.{i}" for n in known]])
                text += f" This represents {display(share, 'ratio')} of the returned total."
            return text, claims, notices, chart
    text = f"The result contains {display(count.value, 'count')} records" + (" from a limited result." if limited else ".")
    totals = []
    for i, c in measures:
        if c.additive and all(row[i] is not None for row in rows):
            value = sum(amount(row[i]) for row in rows)
            claim(f"total_{c.name}", value, c, operation="sum", sources=[f"rows.{n}.{i}" for n in range(len(rows))])
            totals.append(f"{c.label}: {display(value, c.unit)}")
    if totals:
        text += " Totals for these records: " + "; ".join(totals[:2]) + "."
    return text, claims, notices, chart
