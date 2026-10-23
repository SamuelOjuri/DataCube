"""Period comparisons from explicit column pairs and verified result cells.

Pairing does not make a measure additive. Totals require the existing SQL-derived
metadata; rates and averages are compared period by period, never summed.
"""
from datetime import date
from decimal import Decimal
import re
from zoneinfo import ZoneInfo

from .contracts import MonthlyComparison
from ..workflow.contracts import ExploratoryChartIntent
from ..workflow.summary_language import MONTHS, display
from ..answer_presentation.calendar import coordinate, label as period_label, period_start


def infer_comparison(result):
    """Compatibility for saved paired-column results, not arbitrary name guessing.

    New queries declare the pairing explicitly. Legacy recognition requires matching
    stems and units; multiple measures of the same preferred unit are ambiguous.
    """
    columns = {c.name: c for c in result.provenance.columns}
    candidates = []
    for current in ("current_period_", "current_", "this_year_"):
        for baseline in ("prior_period_", "prior_", "previous_period_", "previous_", "last_year_"):
            if current + "month" not in columns or baseline + "month" not in columns:
                continue
            for name, column in columns.items():
                other = columns.get(baseline + name.removeprefix(current))
                if (name.startswith(current) and column.role == "measure" and other
                        and other.role == "measure" and column.unit == other.unit
                        and column.unit in {"source_currency", "count", "ratio", "percent", "days"}):
                    candidates.append(MonthlyComparison(current_month=current + "month",
                        baseline_month=baseline + "month", current_value=name, baseline_value=other.name))
    for units in ({"source_currency"}, {"ratio", "percent"}, {"count", "days"}):
        selected = [c for c in candidates if columns[c.current_value].unit in units]
        if selected:
            return selected[0] if len(selected) == 1 else None
    return None


def validate_comparison(result, pairing):
    """Validate dates, unique coordinates, equal offsets and compatible measures."""
    grain = getattr(pairing, "time_grain", "month")
    legacy = isinstance(pairing, MonthlyComparison)
    names = [pairing.current_month if legacy else pairing.current_axis,
             pairing.baseline_month if legacy else pairing.baseline_axis, pairing.current_value, pairing.baseline_value]
    if len(set(names)) != 4 or any(name not in result.columns for name in names):
        return None
    indices = [result.columns.index(name) for name in names]
    columns = [result.provenance.columns[i] for i in indices]
    if any(c.role != "dimension" or c.type != "date" or (c.time_grain and c.time_grain != grain) for c in columns[:2]):
        return None
    if (columns[2].unit not in {"source_currency", "count", "ratio", "percent", "days"}
            or any(c.role != "measure" or c.type not in {"integer", "decimal"}
                   or c.unit != columns[2].unit for c in columns[2:])):
        return None
    # Only redundant month labels may accompany the two dates. A category or a
    # second breakdown must not silently disappear from a comparison chart.
    extras = [(i, c) for i, c in enumerate(result.provenance.columns)
              if c.role == "dimension" and i not in indices[:2]]
    if any(c.name not in {"month_label", "fiscal_month_label"} or c.type != "text" for _, c in extras):
        return None
    points = []
    try:
        for n, row in enumerate(result.rows):
            current, baseline = (date.fromisoformat(str(row[i])) for i in indices[:2])
            if period_start(current, grain) != current or period_start(baseline, grain) != baseline:
                return None
            label = MONTHS[current.month - 1].casefold()
            labels = {label, label[:3]} | ({"sept"} if current.month == 9 else set())
            if any(str(row[i]).casefold() not in labels
                   for i, _ in extras):
                return None
            points.append((n, current, baseline))
    except (ValueError, TypeError, IndexError):
        return None
    if not points or len({p[1] for p in points}) != len(points) or len({p[2] for p in points}) != len(points):
        return None
    offsets = {coordinate(a, grain) - coordinate(b, grain) for _, a, b in points}
    if pairing.kind == "year_over_year":
        if grain == "week":
            aligned = all(a.isocalendar().year == b.isocalendar().year + 1 and a.isocalendar().week == b.isocalendar().week for _, a, b in points)
        else:
            aligned = all(a.year == b.year + 1 and (a.month, a.day) == (b.month, b.day) for _, a, b in points)
        if not aligned:
            return None
    elif len(offsets) != 1 or min(offsets) <= 0:
        return None
    points.sort(key=lambda p: p[1])
    if max(p[2] for p in points) >= points[0][1]:
        return None  # Compared periods must not overlap.
    excluded = False
    completed_only = pairing.completed_months_only if legacy else pairing.completed_periods_only
    if completed_only:
        cutoff = period_start(result.provenance.freshness.queried_at.astimezone(
            ZoneInfo(result.provenance.business_timezone)).date(), grain)
        excluded = any(current >= cutoff for _, current, _ in points)
        points = [p for p in points if p[1] < cutoff]
    return indices, columns, points, excluded


def span(dates, grain="month"):
    return period_label(dates[0], grain) if dates[0] == dates[-1] else f"{period_label(dates[0], grain)} to {period_label(dates[-1], grain)}"


def summarize_comparison(result, pairing, claim):
    grain = getattr(pairing, "time_grain", "month")
    legacy = isinstance(pairing, MonthlyComparison)
    noun = grain
    checked = validate_comparison(result, pairing)
    if checked is None:
        return (f"I couldn't reliably align the {noun}s in these two periods. The returned values are available below.",
                [f"This comparison needs one value per {noun} for the same measure in both periods."], None)
    indices, columns, points, excluded = checked
    p, rows = result.provenance, result.rows
    limited = p.truncated or p.query_limited or p.dataset_scope == "limited"
    notices = []
    if excluded:
        notices.append(f"The current incomplete {noun} and any future {noun}s are excluded from this comparison.")
    if not points:
        return f"There are no completed {noun}s available to compare in these periods.", notices, None
    unit = columns[2].unit
    delta_unit = "percentage_points" if unit in {"ratio", "percent"} else unit
    factor = 100 if unit == "ratio" else 1
    subject = re.sub(r"^(current(?: period| year)?|this year)\s+", "", columns[2].label, flags=re.I).strip()
    subject = subject[:1].upper() + subject[1:]
    subject = subject or "The measure"
    current_dates, baseline_dates = [a for _, a, _ in points], [b for _, _, b in points]
    chart = None if limited else ExploratoryChartIntent(kind="line", x=pairing.current_month if legacy else pairing.current_axis,
        series=[pairing.current_value, pairing.baseline_value], comparison=pairing if legacy else None,
        period_comparison=None if legacy else pairing, time_grain=grain)
    pairs = []
    for n, current_date, baseline_date in points:
        if any(rows[n][i] is None for i in indices[2:]):
            continue
        current, baseline = (Decimal(str(rows[n][i])) for i in indices[2:])
        difference = (current - baseline) * factor
        fact = claim(f"comparison_month_{n}", difference, columns[2], kind="percentage_point_change" if factor != 1 or unit == "percent" else "absolute_change",
            operation="difference", sources=[f"rows.{n}.{indices[3]}", f"rows.{n}.{indices[2]}"], unit=delta_unit)
        pairs.append((n, current_date, baseline_date, current, baseline, difference, fact))
    if len(pairs) != len(points):
        notices.append(f"Only {noun}s with known values in both periods are compared; unknown values are not zero.")
    if not pairs:
        return f"{subject} cannot yet be compared because no {noun} has known values in both periods.", notices, chart
    contiguous = all(coordinate(b, grain) == coordinate(a, grain) + 1 for a, b in zip(current_dates, current_dates[1:]))
    if not contiguous:
        notices.append(f"Some {noun}s are missing. The comparison describes the available pairs, and the chart leaves gaps.")
    scope = f"{span(current_dates, grain)}, compared with {span(baseline_dates, grain)}"
    sentences = []
    if not limited and contiguous and len(pairs) == len(points) and all(c.additive for c in columns[2:]) and unit in {"source_currency", "count"}:
        totals = []
        for name, index in (("current", indices[2]), ("baseline", indices[3])):
            totals.append(claim(f"comparison_{name}", sum(Decimal(str(rows[n][index])) for n, _, _ in points),
                columns[2], operation="sum", sources=[f"rows.{n}.{index}" for n, _, _ in points]))
        current, baseline = totals
        delta = current.value - baseline.value
        claim("comparison_total_change", delta, columns[2], kind="absolute_change", operation="difference",
            sources=[f"claims.{baseline.id}.value", f"claims.{current.id}.value"])
        change = "unchanged" if delta == 0 else f"{display(abs(delta), unit)} {'higher' if delta > 0 else 'lower'}"
        if baseline.value > 0 and delta:
            relative = delta / baseline.value * 100
            claim("comparison_percentage_change", relative, columns[2], unit="percent", kind="percentage_change", operation="relative_change",
                denominator=baseline.value, sources=[f"claims.{baseline.id}.value", f"claims.{current.id}.value"])
            change += f" ({display(abs(relative), 'percent')})"
        sentences.append(f"{subject} totalled {display(current.value, unit)} from {span(current_dates, grain)}, {change} "
                         f"compared with {display(baseline.value, unit)} from {span(baseline_dates, grain)}.")
    has_total = bool(sentences)
    reference = "a year earlier" if pairing.kind == "year_over_year" else f"in the corresponding {noun} of the earlier period"
    # Summarise a sustained change only when every consecutive month supports it.
    runs = []
    for pair in pairs:
        sign = (pair[5] > 0) - (pair[5] < 0)
        if not runs or runs[-1][0] != sign:
            runs.append((sign, []))
        runs[-1][1].append(pair)
    words = {1: "higher than", -1: "lower than", 0: "the same as"}
    if not limited and contiguous and len(pairs) == len(points) and len(pairs) > 1 and len(runs) <= 2:
        parts = [f"{words[sign]} {reference} {'in' if len(run) == 1 else 'from'} {span([r[1] for r in run], grain)}" for sign, run in runs]
        adjective = {"day": "Daily", "week": "Weekly", "month": "Monthly", "quarter": "Quarterly", "year": "Yearly"}[grain]
        sentences.append(f"{adjective} values for {subject[:1].lower() + subject[1:]} were {', then '.join(parts)}.")
    elif len(pairs) > 1:
        sentences.append(f"Among the comparable {noun}s, the value for {subject[:1].lower() + subject[1:]} was {words[(pairs[0][5] > 0) - (pairs[0][5] < 0)]} "
            f"{reference} in {period_label(pairs[0][1], grain)} and {words[(pairs[-1][5] > 0) - (pairs[-1][5] < 0)]} {reference} in {period_label(pairs[-1][1], grain)}.")
    n, current_date, baseline_date, current, baseline, difference, _ = pairs[-1]
    claim("comparison_latest", current, columns[2], sources=[f"rows.{n}.{indices[2]}"])
    claim("comparison_latest_baseline", baseline, columns[3], sources=[f"rows.{n}.{indices[3]}"])
    change = "unchanged" if difference == 0 else f"{display(abs(difference), delta_unit)} {'higher' if difference > 0 else 'lower'}"
    pronoun = "it" if sentences else subject[:1].lower() + subject[1:]
    sentences.append(f"In {period_label(current_date, grain)}, {pronoun} was {display(current, unit)}, {change} than "
                     f"{display(baseline, unit)} in {period_label(baseline_date, grain)}." if difference else
                     f"In {period_label(current_date, grain)}, {pronoun} was {display(current, unit)}, matching {period_label(baseline_date, grain)}.")
    if not has_total:
        sentences.append(f"The comparison covers {'the returned ' + noun + 's in ' if limited else ''}{scope}.")
    return " ".join(sentences), notices, chart
