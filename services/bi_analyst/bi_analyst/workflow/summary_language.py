"""Presentation only: render verified values without changing their calculations."""
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP


SUMMARY_VERSION = "5"
CONVERSION_MATURITY = ("This reflects recorded outcomes so far. More recent projects have had less time "
                       "to reach invoiced completion.")
MONTHS = ("January", "February", "March", "April", "May", "June", "July", "August",
          "September", "October", "November", "December")
# Copy is separate from the signed business catalogue. Never infer a metric's
# definition (gross/weighted, source population, etc.) from the user's wording.
SUBJECTS = {
    "new_enquiry_value": "New enquiry value",
    "order_parent_value": "Project order value",
    "order_hidden_complete_subtotal": "Hidden total customer order value (complete-input subtotal)",
    "invoice_project_value": "Project invoiced value",
    "invoice_hidden_value": "Hidden amount invoiced",
    "invoice_stored_child_value": "Stored child invoiced value",
    "enquiry_monthly_actual": "New enquiry value",
    "bookings_monthly_actual": "Bookings",
    "invoice_monthly_actual": "Invoiced revenue",
}


def display(value, unit):
    if value is None:
        return "unknown"
    if unit in {"ratio", "percent", "percentage_points"}:
        value = value * 100 if unit == "ratio" else value
        suffix = " percentage points" if unit == "percentage_points" else "%"
        return f"{value.quantize(Decimal('0.1'), rounding=ROUND_HALF_UP):,}{suffix}"
    if unit == "source_currency":
        return f"£{value.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP):,}"
    return f"{value.normalize():,f}" + ((" day" if abs(value) == 1 else " days") if unit == "days" else "")


def month_text(value):
    day = date.fromisoformat(str(value))
    return f"{MONTHS[day.month - 1]} {day.year}"


def date_text(day):
    return f"{day.day} {month_text(day)}"


def period_text(period):
    """Describe the saved boundaries, never today's interpretation of 'last month'."""
    if period is None:
        return "all recorded history"
    start, end = period.start_date, period.end_date_exclusive
    if end is None:
        return f"projects created on or after {date_text(start)}"
    last = end - timedelta(days=1)
    if start.day == end.day == 1:
        return month_text(start) if start == last.replace(day=1) else f"{month_text(start)} to {month_text(last)}"
    return date_text(start) if start == last else f"{date_text(start)} to {date_text(last)}"


def period_scope(period):
    label = period_text(period)
    if period is None:
        return f"across {label}"
    if period.end_date_exclusive is None:
        return f"for {label}"
    if " to " in label:
        return f"from {label}"
    return f"{'in' if period.start_date.day == period.end_date_exclusive.day == 1 else 'on'} {label}"


def total_sentence(provenance, variant):
    p, total = provenance, provenance.total
    subject = SUBJECTS.get(p.metric_id, p.metric_label)
    if variant:
        subject = f"The {variant} conversion rate"
    elif p.unit == "days":
        subject = f"The {'median' if 'median' in p.metric_id else 'average'} observed gestation"
    scope = period_scope(p.resolved_period)
    if total is None:
        return f"Here is the breakdown of {subject[:1].lower() + subject[1:]} {scope}; an overall figure was not requested."
    if total.source_rows == 0:
        return f"No matching records were found {scope}."
    if total.value is None:
        if variant and total.denominator == 0:
            denominator = "closed" if variant == "closed-only" else "eligible"
            return f"There are no {denominator} projects {scope.removeprefix('for projects ')}, so the {variant} conversion rate cannot be calculated."
        return f"{subject} is unknown {scope} because the matching records do not provide a usable value."
    value = display(total.value, p.unit)
    if variant:
        text = f"{subject} was {value} {scope}"
        if total.numerator is not None and total.denominator is not None and total.denominator > 0:
            denominator = "closed" if variant == "closed-only" else "eligible"
            text += (f": {display(total.numerator, 'count')} invoiced wins out of "
                     f"{display(total.denominator, 'count')} {denominator} projects")
        text += "."
        if variant == "inclusive":
            text += (" The denominator includes open and lost projects, as well as wins; "
                     "projects with an unknown stage also remain eligible.")
        return text
    if p.unit == "days":
        return f"{subject} was {value} {scope}."
    if total.known_values < total.source_rows:
        return f"The known subtotal for {subject[:1].lower() + subject[1:]} was {value} {scope}."
    return f"{subject} totalled {value} {scope}."


def comparison_sentence(value, delta, unit, baseline_unit, period):
    comparison = (f"That was {display(abs(delta), unit)} {'higher' if delta > 0 else 'lower'} than the comparison value"
                  if delta else "That matched the comparison value")
    return f"{comparison} of {display(value, baseline_unit)} {period_scope(period)}."
