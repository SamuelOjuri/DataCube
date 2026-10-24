"""Deterministic findings over owned cells; analytical intent supplies no facts."""
from decimal import Decimal, InvalidOperation

from ..workflow.contracts import EvidenceClaim, ExploratoryChartIntent
from ..workflow.errors import EvidenceError
from ..workflow.summary_language import display
from .calendar import coordinate, dates_for, label


def amount(value):
    if value is None:
        return None
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as error:
        raise EvidenceError("invalid_numeric_value") from error
    if not number.is_finite():
        raise EvidenceError("nonfinite_value")
    return number


class Evidence:
    def __init__(self, result):
        self.result, self.p, self.rows = result, result.provenance, result.rows
        self.claims, self.notices = [], []
        self.dimensions = [i for i, c in enumerate(self.p.columns) if c.role == "dimension"]
        self.project_ref = next((i for i in self.dimensions if self.p.columns[i].name == "project"
                                 and self.p.columns[i].type == "text"), None)
        self.project_name = next((i for i in self.dimensions if self.p.columns[i].name == "project_name"
                                  and self.p.columns[i].type == "text"), None)
        self.project_pair = self.project_ref is not None and self.project_name is not None
        # Reference and name describe the same project axis, not two breakdowns.
        self.category_dimensions = [i for i in self.dimensions if not (self.project_pair and i == self.project_name)]
        self.measures = [i for i, c in enumerate(self.p.columns) if c.role == "measure" and c.type in {"integer", "decimal"}]
        self.limited = self.p.truncated or self.p.query_limited or self.p.dataset_scope == "limited"
        if ([c.name for c in self.p.columns] != result.columns
                or any(len(row) != len(result.columns) for row in self.rows)):
            raise EvidenceError("result_columns_mismatch")
        for i in self.measures:
            for row in self.rows:
                amount(row[i])
        self.claim("row_count", Decimal(len(self.rows)), kind="row_count", operation="row_count", sources=["rows"])
        if self.limited:
            self.notices.append("This summary covers only the returned rows; it does not describe the full matching population.")
        if any(row[i] is None for row in self.rows for i in self.measures):
            self.notices.append("Some returned values are unknown. They are not treated as zero, and summaries use known values only.")

    def claim(self, key, value, column=None, *, kind="total", operation="cell", sources=(), unit=None, denominator=None):
        item = EvidenceClaim(id=f"summary_{key}", result_id=self.result.id, location=f"derived.summary_{key}",
            metric_id="exploratory", period=self.p.title, value=value, denominator=denominator,
            label=column.label if column else "Returned records", unit=unit or (column.unit if column else "count"),
            kind=kind, operation=operation, sources=list(sources))
        self.claims.append(item)
        return item

    def known(self, index):
        return [n for n, row in enumerate(self.rows) if row[index] is not None]

    def cell(self, key, n, i, kind="total"):
        return self.claim(key, amount(self.rows[n][i]), self.p.columns[i], kind=kind, sources=[f"rows.{n}.{i}"])

    def group(self, n):
        labels = []
        for i in self.category_dimensions:
            value = "Unclassified" if self.rows[n][i] is None else str(self.rows[n][i])[:120]
            if self.project_pair and i == self.project_ref:
                name = self.rows[n][self.project_name]
                if name is not None and str(name).strip():
                    value += " — " + str(name).strip()[:120]
            labels.append(value)
        return " / ".join(labels)

    def difference(self, key, current, baseline, column, sources):
        rate = column.unit in {"ratio", "percent"}
        unit = "percentage_points" if rate else column.unit
        delta = (current - baseline) * (100 if column.unit == "ratio" else 1)
        self.claim(key, delta, column, kind="percentage_point_change" if rate else "absolute_change",
                   operation="difference", unit=unit, sources=sources)
        text = "unchanged" if not delta else f"{display(abs(delta), unit)} {'higher' if delta > 0 else 'lower'}"
        if not rate and baseline > 0 and delta:
            relative = (current - baseline) / baseline * 100
            self.claim(key + "_relative", relative, column, unit="percent", kind="percentage_change",
                operation="relative_change", sources=sources, denominator=baseline)
            text += f" ({display(abs(relative), 'percent')})"
        return text


def scalar(e, primary):
    indices = ([primary] if primary is not None else []) + [i for i in e.measures if i != primary]
    parts = []
    for i in indices[:3]:
        c = e.p.columns[i]
        value = e.cell("value" if not parts else f"value_{i}", 0, i).value
        if value is None:
            parts.append(f"{c.label} is unknown because the result does not provide a usable value.")
        else:
            parts.append(f"{c.label} {'totalled' if c.additive else 'was'} {display(value, c.unit)}.")
    if len(indices) > 3:
        parts.append("The remaining measures are available in the table.")
    return " ".join(parts), None, None


def grouped(e, primary, direction="highest"):
    c = e.p.columns[primary]
    known = e.known(primary)
    if not known:
        return f"{c.label} is unknown for every returned group, so I can't identify a {direction} value.", None, "The table retains the groups with unknown values."
    best = (min if direction == "lowest" else max)(amount(e.rows[n][primary]) for n in known)
    winners = [n for n in known if amount(e.rows[n][primary]) == best]
    for position, n in enumerate(winners[:3]):
        e.cell("leader" if position == 0 else f"leader_{position}", n, primary, "contributor")
    labels = [e.group(n) for n in winners[:3]]
    qualifier = "Among the returned groups" + (" with known values" if len(known) < len(e.rows) else "")
    if len(winners) == 1:
        text = f"{qualifier}, {labels[0]} had the {direction} {c.label.lower()} at {display(best, c.unit)}."
    else:
        names = ", ".join(labels) + (" and other groups" if len(winners) > 3 else "")
        text = f"{qualifier}, {names} tied for the {direction} {c.label.lower()} at {display(best, c.unit)}."
    total = sum(amount(e.rows[n][primary]) for n in known)
    if c.additive and not e.limited and len(known) == len(e.rows) and total > 0 and all(amount(e.rows[n][primary]) >= 0 for n in known) and len(winners) == 1:
        share = best / total
        e.claim("share", share, c, kind="share", operation="share", unit="ratio", denominator=total,
            sources=[f"rows.{winners[0]}.{primary}", *[f"rows.{n}.{primary}" for n in known]])
        text += f" This represents {display(share, 'ratio')} of the returned total."
    if e.limited:
        return text, None, "The table shows the returned groups; their ranking may not describe the full population."
    if len(e.category_dimensions) != 1 or len(e.rows) > 30:
        return text, None, "The table keeps the individual breakdowns clear; combining them into one chart would obscure the detail."
    axis = e.p.columns[e.category_dimensions[0]]
    if axis.type != "text":
        return text, None, "These groups do not provide a clear category axis, so the values are shown in a table."
    return text, ExploratoryChartIntent(kind="bar", x=axis.name, series=[c.name],
        sort="ascending" if direction == "lowest" else "descending"), None


def trend(e, primary, axis):
    c, time = e.p.columns[primary], e.p.columns[axis]
    dates = dates_for(e.result, axis, time.time_grain)
    if dates is None:
        return None
    known = sorted(e.known(primary), key=lambda n: dates[n])
    if not known:
        return f"{c.label} is unknown throughout the returned period, so a trend cannot be established.", None, "Unknown values are retained in the table."
    first, latest = known[0], known[-1]
    a, b = e.cell("first", first, primary, "first"), e.cell("latest", latest, primary, "latest")
    prefix = "Among the returned periods, " if e.limited else ""
    if first == latest:
        text = f"{prefix}{c.label} was {display(a.value, c.unit)} in {label(dates[first], time.time_grain)}."
        if len(e.rows) > 1:
            text += " This is the only known value, so it does not establish a trend."
    else:
        change = e.difference("endpoint_change", b.value, a.value, c, [a.sources[0], b.sources[0]])
        text = (f"{prefix}{c.label} was {display(b.value, c.unit)} in {label(dates[latest], time.time_grain)}, {change} "
                f"compared with {display(a.value, c.unit)} in {label(dates[first], time.time_grain)}.")
        peak, low = max(known, key=lambda n: amount(e.rows[n][primary])), min(known, key=lambda n: amount(e.rows[n][primary]))
        high = e.cell("peak", peak, primary, "peak")
        small = e.cell("low", low, primary, "low")
        if high.value != small.value and {peak, low} != {first, latest}:
            text += (f" The highest known value was {display(high.value, c.unit)} in {label(dates[peak], time.time_grain)}; "
                     f"the lowest was {display(small.value, c.unit)} in {label(dates[low], time.time_grain)}.")
    ordered = sorted(dates)
    if len(known) < len(e.rows):
        e.notices.append("Some periods have unknown values. The trend uses known values and leaves gaps in the chart.")
    if any(coordinate(b, time.time_grain) != coordinate(a, time.time_grain) + 1 for a, b in zip(ordered, ordered[1:])):
        e.notices.append("Some periods are missing. The chart leaves gaps rather than treating those values as zero.")
    if e.limited:
        return text, None, "The table shows the available periods; a limited result cannot establish the complete trend."
    return text, ExploratoryChartIntent(kind="line", x=time.name, series=[c.name], time_grain=time.time_grain), None


def comparison(e, primary, secondary):
    c = e.p.columns[primary]
    if len(e.rows) == 1 and secondary is not None and not e.dimensions:
        if e.p.columns[secondary].unit != c.unit or primary == secondary:
            return None
        a, b = e.cell("comparison_current", 0, primary), e.cell("comparison_baseline", 0, secondary)
        a_label, b_label = c.label, e.p.columns[secondary].label
        chart = ExploratoryChartIntent(kind="comparison", x=c.name, series=[c.name, e.p.columns[secondary].name])
    elif len(e.rows) == 2 and len(e.category_dimensions) == 1 and e.group(0) != e.group(1):
        a, b = e.cell("comparison_current", 0, primary), e.cell("comparison_baseline", 1, primary)
        a_label, b_label = e.group(0), e.group(1)
        axis = e.p.columns[e.category_dimensions[0]]
        chart = ExploratoryChartIntent(kind="bar", x=axis.name, series=[c.name]) if axis.type == "text" else None
    else:
        return None
    if a.value is None or b.value is None:
        return (f"I can't compare {a_label} with {b_label} because one or both values are unknown.", None,
                "The available figures are shown in the table; unknown values are not zero.")
    change = e.difference("comparison_change", a.value, b.value, c, [b.sources[0], a.sources[0]])
    text = f"{a_label} was {display(a.value, c.unit)}, {change} compared with {b_label} at {display(b.value, c.unit)}."
    return text, None if e.limited else chart, "This comparison covers only the returned values." if e.limited else None


def distribution(e, primary):
    c, known = e.p.columns[primary], e.known(primary)
    if not known:
        return f"There are no known {c.label.lower()} values to describe the distribution.", None, "The table retains the unknown values."
    ordered = sorted(known, key=lambda n: amount(e.rows[n][primary]))
    low, high = e.cell("low", ordered[0], primary, "low"), e.cell("peak", ordered[-1], primary, "peak")
    values = [amount(e.rows[n][primary]) for n in ordered]
    middle = len(values) // 2
    median = values[middle] if len(values) % 2 else (values[middle - 1] + values[middle]) / 2
    e.claim("median", median, c, kind="median", operation="median", sources=[f"rows.{n}.{primary}" for n in ordered])
    text = (f"Among the returned values, {c.label.lower()} ranged from {display(low.value, c.unit)} to "
            f"{display(high.value, c.unit)}, with a median of {display(median, c.unit)}.")
    if c.grouped:
        e.notices.append("This distribution describes the returned group values, not the individual records within each group.")
    if e.limited or len(known) < 2:
        return text, None, "The available values are shown in the table; they do not establish a complete distribution." if e.limited else "One known value is not enough to show a distribution."
    return text, ExploratoryChartIntent(kind="histogram", x=c.name, series=[c.name]), None


def relationship(e, primary, secondary):
    if secondary is None or secondary == primary:
        return None
    a, b = e.p.columns[primary], e.p.columns[secondary]
    pairs = [n for n in range(len(e.rows)) if all(e.rows[n][i] is not None for i in (primary, secondary))]
    if len(pairs) < len(e.rows):
        e.notices.append("Only records with known values for both measures are compared.")
    if len(pairs) < 3:
        return f"There are too few complete pairs of {a.label.lower()} and {b.label.lower()} to describe a reliable relationship.", None, "The available pairs are shown in the table."
    x, y = [amount(e.rows[n][primary]) for n in pairs], [amount(e.rows[n][secondary]) for n in pairs]
    mx, my = sum(x) / len(x), sum(y) / len(y)
    xx, yy = sum((v - mx) ** 2 for v in x), sum((v - my) ** 2 for v in y)
    if xx == 0 or yy == 0:
        text = f"At least one of {a.label.lower()} and {b.label.lower()} is constant across the complete pairs, so a correlation cannot be calculated."
    else:
        r = sum((u - mx) * (v - my) for u, v in zip(x, y)) / (xx * yy).sqrt()
        e.claim("correlation", r, unit="none", kind="correlation", operation="pearson",
            sources=[f"rows.{n}.{i}" for n in pairs for i in (primary, secondary)])
        direction = "positive" if r > 0 else "negative" if r < 0 else "zero"
        text = (f"Among the returned pairs, {a.label.lower()} and {b.label.lower()} had a {direction} linear correlation "
                f"(r = {r:.2f}). This describes an association, not a cause.")
    if e.limited:
        return text, None, "The relationship describes only the returned pairs; use the table to inspect this limited sample."
    return text, ExploratoryChartIntent(kind="scatter", x=a.name, series=[b.name]), None


def records(e, primary, reason=None):
    labels = [c.label for c in e.p.columns[:4]]
    details = ", ".join(labels) + (" and other details" if len(e.p.columns) > 4 else "")
    parts = [f"I found {len(e.rows)} matching {'record' if len(e.rows) == 1 else 'records'}. The table shows {details}."]
    for i in ([primary] if primary is not None else []) + [i for i in e.measures if i != primary]:
        c, known = e.p.columns[i], e.known(i)
        if not known:
            parts.append(f"{c.label} is unknown in the returned records.")
        elif c.additive and len(known) == len(e.rows):
            total = sum(amount(e.rows[n][i]) for n in known)
            e.claim(f"total_{c.name}", total, c, operation="sum", sources=[f"rows.{n}.{i}" for n in known])
            parts.append(f"{c.label} totalled {display(total, c.unit)} across these records.")
        elif len(known) > 1:
            low, high = min(known, key=lambda n: amount(e.rows[n][i])), max(known, key=lambda n: amount(e.rows[n][i]))
            a, b = e.cell(f"low_{c.name}", low, i, "low"), e.cell(f"high_{c.name}", high, i, "peak")
            parts.append(f"Known {c.label.lower()} values range from {display(a.value, c.unit)} to {display(b.value, c.unit)}.")
        else:
            value = e.cell(f"known_{c.name}", known[0], i)
            parts.append(f"The known {c.label.lower()} value is {display(value.value, c.unit)}.")
        if len(parts) >= 3:
            break
    fallback = reason or ("These are individual records, so the table preserves their detail without implying an aggregate trend."
                          if e.measures else "There is no numerical measure to plot; the matching entries are listed in the table.")
    return " ".join(parts), None, fallback
