"""Presentation policy for certified results; no model supplied chart specs."""
from ..workflow.contracts import ChartIntent


def select_metric_chart(result):
    p = result.provenance
    axes = (["month"] if p.request.grain == "month" else []) + list(p.request.dimensions)
    index = result.columns.index("value") if "value" in result.columns else None
    if not axes and p.total is not None:
        if p.comparison is None:
            return ChartIntent(kind="number")
        if p.total.value is not None and p.comparison.baseline.value is not None:
            return ChartIntent(kind="comparison")
    unsuitable = (p.truncated or not result.rows or len(result.rows) > 100 or len(axes) != 1
                  or index is None or any(row[index] is None for row in result.rows))
    if unsuitable:
        return ChartIntent(kind="table")
    return ChartIntent(kind="line" if axes[0] == "month" else "bar", x=axes[0])
