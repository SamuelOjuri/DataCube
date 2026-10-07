"""Resolve reporting boundaries explicitly; never use the machine timezone."""
from datetime import date, datetime, time, timezone
from typing import Literal
from zoneinfo import ZoneInfo

from .catalogue import Contract


class ResolvedPeriod(Contract):
    name: str
    business_timezone: str
    resolved_at: datetime
    as_of_date: date
    start_date: date
    end_date_exclusive: date | None
    start_utc: datetime
    end_utc_exclusive: datetime | None
    completed_months_only: bool


def month_offset(day: date, delta: int) -> date:
    year, month = divmod(day.year * 12 + day.month - 1 + delta, 12)
    return date(year, month + 1, 1)


def resolve_period(
    name: Literal["last_month", "previous_month", "completed_12_months", "five_year_cohort", "two_year_cohort"],
    *, now: datetime, business_timezone: str,
) -> ResolvedPeriod:
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("An offset-aware resolution timestamp is required")
    zone = ZoneInfo(business_timezone)
    as_of = now.astimezone(zone).date()
    end = as_of.replace(day=1)
    if name == "last_month":
        start = month_offset(end, -1)
    elif name == "previous_month":
        start, end = month_offset(end, -2), month_offset(end, -1)
    elif name == "completed_12_months":
        start = month_offset(end, -12)
    elif name in ("five_year_cohort", "two_year_cohort"):
        years = 5 if name == "five_year_cohort" else 2
        try:
            start = as_of.replace(year=as_of.year - years)
        except ValueError:  # PostgreSQL interval subtraction clamps leap day.
            start = as_of.replace(year=as_of.year - years, day=28)
        end = None  # Preserve the certified baseline's lower-bound-only cohort.
    else:
        raise ValueError("Unsupported period; MTD and fiscal periods require separate definitions")
    def utc(day):
        return datetime.combine(day, time.min, tzinfo=zone).astimezone(timezone.utc) if day else None
    return ResolvedPeriod(name=name, business_timezone=business_timezone, resolved_at=now,
                          as_of_date=as_of, start_date=start, end_date_exclusive=end,
                          start_utc=utc(start), end_utc_exclusive=utc(end),
                          completed_months_only=end is not None)
