"""Schema and business-definition context for SQL generation, derived from the catalogue."""
from datetime import date

from ..database import GATEWAYS
from ..semantic.catalogue import Catalogue

CALENDAR = ("Use half-open date ranges (>= start AND < end). Monthly views and invoice_reporting_facts_v1 "
            "contain completed months only, so the current month has no rows there. The fiscal year runs "
            "1 November to 31 October; state exact date ranges in the title.")
# Exploration-only join keys between gateway grains; the sealed catalogue joins are unchanged.
EXPLORATION_JOINS = [
    {"left": "invoice_reporting_facts_v1", "right": "projects_v1",
     "on": ["invoice_reporting_facts_v1.parent_monday_id = projects_v1.monday_id"], "cardinality": "many_to_one",
     "rule": "Attribute invoice amounts to parent classifications such as account or product_type. Sum invoice "
             "amounts at invoice grain; never sum project-level amounts after this join."},
    {"left": "conversion_cohorts_v1", "right": "projects_v1",
     "on": ["conversion_cohorts_v1.monday_id = projects_v1.monday_id"], "cardinality": "many_to_one",
     "rule": "Filter exactly one cohort_years value before joining; each project appears once per cohort."},
]
# Stored Monday labels; tests/api/test_exploration_context.py checks them against a live read DSN.
TYPES = ["New Build", "Refurbishment"]
CATEGORIES = ["Apartments", "Commercial", "Commodity", "Consultancy", "Datacentre", "Education", "Health", "House",
              "Industrial", "Leisure", "Military", "Mixed Use", "Student Accommodation"]
PIPELINE_STAGES = ["Open Enquiry", "Won - Open (Order Received)", "Won - Closed (Invoiced)", "Won Via Other Ref", "Lost"]
CHANGE_REASONS = [
    "New Enquiry", "New Enquiry - Option", "New Enquiry- Option", "New Enquiry - Separate Roof",
    "Amendment - Additional Roof Area", "Amendment - Alternative Customer", "Amendment - Alternative Material",
    "Amendment - Change In Falls", "Amendment - Deck Build Up", "Amendment - Dimensions", "Amendment - Max Height",
    "Amendment - New Issue Roof Plans", "Amendment - New Outlet / Drainage", "Amendment - Other",
    "Amendment - Roof Area Omitted", "Amendment - Scheme Change", "Amendment - Site Survey",
    "Amendment - U - Value", "Amendment - Updated Price",
]
CLASSIFICATION = {
    "category": {"meaning": "Building sector.", "values": CATEGORIES},
    "type": {"meaning": "Construction type; refurbishment and new-build questions filter this column.",
             "values": TYPES},
}
# Reviewed gateway additions, separate from the fingerprinted metric catalogue.
# Migrations 011/012 expose labels without changing certified metric definitions.
GATEWAY_COLUMNS = {"projects_v1": {"item_name": "text", "project_name": "text"}}
# Exploration-only column notes; the sealed catalogue is unchanged.
COLUMN_NOTES = {
    "projects_v1": {
        **CLASSIFICATION,
        "monday_id": {"meaning": "Internal Monday project ID. Use for joins and per-project grouping, not the default display label."},
        "item_name": {"meaning": (
            "Monday item name, shown in the board's Project column (a project reference, not the separate Project Name). "
            "Display alongside project_name in project reports and visuals. Keep as text, including leading zeroes. "
            "May be blank or duplicated: use COALESCE(NULLIF(TRIM(item_name), ''), monday_id) AS project "
            "for display, and retain monday_id in per-project GROUP BY keys.")},
        "project_name": {"meaning": (
            "Descriptive project name from the board's Project Name column. Include as project_name alongside "
            "the project reference in project records, rankings and visuals. Use NULLIF(TRIM(project_name), '') "
            "and keep missing names NULL. Names may be duplicated; retain monday_id in per-project GROUP BY keys.")},
        "pipeline_stage": {"meaning": "Current sales stage. Won stages are the three 'Won ...' values.",
                           "values": PIPELINE_STAGES},
        "account": {"meaning": "Customer name(s) as one stored string; may name several organisations."},
        "product_type": {"meaning": "Product name(s) as one stored string; may list several products."},
        "date_created": {"meaning": "Enquiry creation date."},
        "date_order_received": {"meaning": "Order received (won) date; bookings use it with the won stages."},
        "first_date_designed": {"meaning": "First design completion date."},
        "first_date_invoiced": {"meaning": "First invoice date."},
        "gestation_period": {"meaning": (
            "Days from first design completion to first invoice. 0 means designed but not yet invoiced. NULL means "
            "it cannot be measured: no design date, or the first invoice is dated before the first design. "
            "Recently created projects are mostly not yet invoiced: for gestation completed in a recent "
            "period filter first_date_invoiced; otherwise add a caveat.")},
    },
    "children_v1": {
        "reason_for_change": {"meaning": ("Why the child quote was issued. One stored value may join several labels "
                                          "with ', '; blank is ''."), "values": CHANGE_REASONS},
        "formula_enquiry_value": {"meaning": "quote_amount when reason_for_change is exactly 'New Enquiry', else 0."},
    },
    "enquiry_monthly_v1": {
        "enquiry_month": {"meaning": "Calendar month of the project's enquiry creation date (date_created)."},
    },
    "bookings_monthly_v1": {
        "booking_month": {"meaning": "Calendar month of the project's order-received date (date_order_received)."},
    },
    "invoice_reporting_facts_v1": {
        **CLASSIFICATION,
        "invoice_date": {"meaning": "Recorded child invoice date in a completed month; not the parent's first invoice date."},
    },
    "conversion_cohorts_v1": {
        **CLASSIFICATION,
        "cohort_years": {"meaning": "Cohort length in years; filter exactly one.", "values": [2, 5]},
        "win_count": {"meaning": "1 when the project is 'Won - Closed (Invoiced)', else 0."},
        "closed_count": {"meaning": "1 when the project is 'Won - Closed (Invoiced)' or 'Lost', else 0."},
    },
}


def approved_relations(catalogue: Catalogue) -> frozenset[str]:
    available = {population.id for population in catalogue.populations if population.available}
    return frozenset(r.id for r in catalogue.relations if r.id in GATEWAYS and r.population in available)


def build_context(catalogue: Catalogue, relations: frozenset[str], *, today: date, timezone: str) -> dict:
    populations = {p.id: p.description for p in catalogue.populations}
    return {
        "today": today.isoformat(),
        "business_timezone": timezone,
        "calendar": CALENDAR,
        "views": [{"name": f"analyst_query.{r.id}", "grain": r.grain, "key": r.key,
                   "columns": {name: {"type": kind, **COLUMN_NOTES.get(r.id, {}).get(name, {})}
                               for name, kind in {**r.columns, **GATEWAY_COLUMNS.get(r.id, {})}.items()},
                   "population": populations[r.population], "description": r.description}
                  for r in catalogue.relations if r.id in relations],
        "dimensions": [{"column": d.column, "description": d.description} for d in catalogue.dimensions],
        "joins": [{"left": f"analyst_query.{j.left}", "right": f"analyst_query.{j.right}",
                   "on": [f"{j.left}.{a} = {j.right}.{b}" for a, b in zip(j.left_key, j.right_key)],
                   "cardinality": j.cardinality, "rule": j.description}
                  for j in catalogue.joins if j.left in relations and j.right in relations] + [
                  {**j, "left": f"analyst_query.{j['left']}", "right": f"analyst_query.{j['right']}"}
                  for j in EXPLORATION_JOINS if j["left"] in relations and j["right"] in relations],
        "definitions": [{"id": m.id, "term": m.label, "aliases": m.aliases, "kind": m.kind,
                         "view": f"analyst_query.{m.relation}", "meaning": m.source.priority,
                         "expression": m.calculation.expression,
                         "numerator": m.calculation.numerator, "denominator": m.calculation.denominator,
                         "date_basis": m.date_basis, "periods": m.periods, "filters": m.status_filters,
                         "dimensions": m.dimensions, "unit": m.unit,
                         "rules": [m.calculation.nulls, m.calculation.zeroes, m.calculation.negatives,
                                   m.calculation.empty],
                         "limitations": catalogue.runtime_limitations(m)}
                        for m in catalogue.metrics if m.relation in relations],
        "notices": catalogue.runtime_notices,
    }
