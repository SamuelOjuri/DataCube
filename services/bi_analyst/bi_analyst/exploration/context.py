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


def approved_relations(catalogue: Catalogue) -> frozenset[str]:
    available = {population.id for population in catalogue.populations if population.available}
    return frozenset(r.id for r in catalogue.relations if r.id in GATEWAYS and r.population in available)


def build_context(catalogue: Catalogue, relations: frozenset[str], *, today: date, timezone: str) -> dict:
    populations = {p.id: p.description for p in catalogue.populations}
    return {
        "today": today.isoformat(),
        "business_timezone": timezone,
        "calendar": CALENDAR,
        "views": [{"name": f"analyst_query.{r.id}", "grain": r.grain, "key": r.key, "columns": r.columns,
                   "population": populations[r.population], "description": r.description}
                  for r in catalogue.relations if r.id in relations],
        "joins": [{"left": f"analyst_query.{j.left}", "right": f"analyst_query.{j.right}",
                   "on": [f"{j.left}.{a} = {j.right}.{b}" for a, b in zip(j.left_key, j.right_key)],
                   "cardinality": j.cardinality, "rule": j.description}
                  for j in catalogue.joins if j.left in relations and j.right in relations] + [
                  {**j, "left": f"analyst_query.{j['left']}", "right": f"analyst_query.{j['right']}"}
                  for j in EXPLORATION_JOINS if j["left"] in relations and j["right"] in relations],
        "definitions": [{"id": m.id, "term": m.label, "aliases": m.aliases, "kind": m.kind,
                         "view": f"analyst_query.{m.relation}", "expression": m.calculation.expression,
                         "numerator": m.calculation.numerator, "denominator": m.calculation.denominator,
                         "date_basis": m.date_basis, "filters": m.status_filters, "unit": m.unit,
                         "rules": [m.calculation.nulls, m.calculation.zeroes, m.calculation.negatives],
                         "limitations": catalogue.runtime_limitations(m)}
                        for m in catalogue.metrics if m.relation in relations],
        "notices": catalogue.runtime_notices,
    }
