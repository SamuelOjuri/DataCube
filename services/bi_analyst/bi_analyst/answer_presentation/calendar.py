"""Calendar coordinates shared by trend and period comparison handlers."""
from datetime import date, timedelta

from ..workflow.summary_language import date_text, month_text

GRAINS = {"day", "week", "month", "quarter", "year"}


def period_start(day, grain):
    if grain == "day":
        return day
    if grain == "week":
        return day - timedelta(days=day.weekday())
    if grain == "month":
        return day.replace(day=1)
    if grain == "quarter":
        return day.replace(month=(day.month - 1) // 3 * 3 + 1, day=1)
    if grain == "year":
        return day.replace(month=1, day=1)
    raise ValueError("unsupported_time_grain")


def coordinate(day, grain):
    if grain == "day":
        return day.toordinal()
    if grain == "week":
        return day.toordinal() // 7
    if grain == "month":
        return day.year * 12 + day.month - 1
    if grain == "quarter":
        return day.year * 4 + (day.month - 1) // 3
    return day.year


def label(day, grain):
    day = date.fromisoformat(str(day))
    if grain == "month":
        return month_text(day)
    if grain == "quarter":
        return f"Q{(day.month - 1) // 3 + 1} {day.year}"
    if grain == "year":
        return str(day.year)
    return ("the week of " if grain == "week" else "") + date_text(day)


def dates_for(result, axis, grain):
    if grain not in GRAINS:
        return None
    try:
        dates = [date.fromisoformat(str(row[axis])) for row in result.rows]
    except (ValueError, TypeError):
        return None
    if len(set(dates)) != len(dates) or any(period_start(day, grain) != day for day in dates):
        return None
    return dates
