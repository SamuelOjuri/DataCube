"""Fixed decimal arithmetic over complete aggregates."""
from decimal import Decimal, localcontext

from .contracts import Aggregate, Change


def compare(current: Aggregate, baseline: Aggregate, *, unit: str) -> Change:
    a, b = current.value, baseline.value
    with localcontext() as context:
        context.prec = 50
        delta = None if a is None or b is None else a - b
        percent = None if delta is None or b == 0 else delta / abs(b) * Decimal(100)
        points = delta * Decimal(100) if delta is not None and unit == "ratio" else None
    return Change(current=current, baseline=baseline, absolute_change=delta,
                  percentage_change=percent, percentage_point_change=points,
                  zero_denominator=b == 0)


def share(value: Decimal | None, total: Decimal | None) -> Decimal | None:
    if value is None or total in (None, 0):
        return None
    with localcontext() as context:
        context.prec = 50
        return value / total
