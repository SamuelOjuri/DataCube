"""Validated metadata, not an SQL executor or a second business calculation engine."""
from __future__ import annotations

from importlib.resources import files
from pathlib import Path
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

Identifier = Annotated[str, Field(pattern=r"^[a-z][a-z0-9_]*$", max_length=63)]
Text = Annotated[str, Field(min_length=1)]


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class Population(Contract):
    id: Identifier
    kind: Literal["reportable", "verified_active", "snapshot", "hidden_inventory", "source_defined"]
    description: Text
    available: bool
    requirements: list[Text] = Field(min_length=1)


class Relation(Contract):
    id: Identifier
    source_relations: list[Text] = Field(min_length=1)
    grain: Text
    key: list[Identifier] = Field(min_length=1)
    columns: dict[Identifier, Text]
    population: Identifier
    description: Text
    optional: bool = False


class Dimension(Contract):
    id: Identifier
    column: Identifier
    semantics: Literal["scalar", "unsplit_membership"]
    description: Text


class Source(Contract):
    board_id: str | None
    columns: list[Text] = Field(min_length=1)
    database: Text
    priority: list[Text] = Field(min_length=1)


class Calculation(Contract):
    expression: Text
    aggregation: Literal["sum", "ratio_of_counts", "mean", "percentile"]
    numerator: Text | None
    denominator: Text | None
    nulls: Text
    zeroes: Text
    negatives: Text
    empty: Text


class Metric(Contract):
    id: Identifier
    version: Annotated[str, Field(pattern=r"^[1-9][0-9]*\.[0-9]+\.[0-9]+$")]
    family: Literal["enquiry", "order", "invoice", "conversion", "gestation"]
    label: Text
    aliases: list[Text] = Field(min_length=1)
    kind: Literal["observed", "predictive", "scenario"]
    source: Source
    relation: Identifier
    value_columns: list[Identifier] = Field(min_length=1)
    population: Identifier
    calculation: Calculation
    date_basis: Identifier | None
    periods: list[Text] = Field(min_length=1)
    status_filters: list[Text]
    unit: Literal["source_currency", "ratio", "days"]
    precision: int = Field(ge=0, le=12)
    dimensions: list[Identifier]
    coverage: list[Text] = Field(min_length=1)
    examples: list[Text] = Field(min_length=1)
    limitations: list[Text] = Field(min_length=1)
    certification: Literal["pending_phase1"]

    @model_validator(mode="after")
    def ratio_parts(self):
        if self.calculation.aggregation == "ratio_of_counts":
            if not self.calculation.numerator or not self.calculation.denominator:
                raise ValueError("Rates require a numerator and denominator")
        return self


class Join(Contract):
    id: Identifier
    left: Identifier
    right: Identifier
    left_key: list[Identifier] = Field(min_length=1)
    right_key: list[Identifier] = Field(min_length=1)
    cardinality: Literal["one_to_zero_or_one", "many_to_one"]
    purpose: Literal["project_measures", "child_detail"]
    description: Text


class Catalogue(Contract):
    version: Text
    populations: list[Population]
    relations: list[Relation]
    dimensions: list[Dimension]
    metrics: list[Metric]
    joins: list[Join]
    notices: list[Text]

    @model_validator(mode="after")
    def consistency(self):
        for name in ("populations", "relations", "dimensions", "metrics", "joins"):
            ids = [item.id for item in getattr(self, name)]
            if len(ids) != len(set(ids)):
                raise ValueError(f"Duplicate {name} IDs")
        populations = {p.id: p for p in self.populations}
        relations = {r.id: r for r in self.relations}
        dimensions = {d.id: d for d in self.dimensions}
        for relation in self.relations:
            if relation.population not in populations or not set(relation.key) <= relation.columns.keys():
                raise ValueError(f"Invalid population or key on {relation.id}")
        for metric in self.metrics:
            relation = relations.get(metric.relation)
            if relation is None or metric.population != relation.population:
                raise ValueError(f"Invalid relation/population on {metric.id}")
            required = set(metric.value_columns)
            if metric.date_basis:
                required.add(metric.date_basis)
            for dimension in metric.dimensions:
                if dimension not in dimensions:
                    raise ValueError(f"Unknown dimension {dimension}")
                required.add(dimensions[dimension].column)
            if not required <= relation.columns.keys():
                raise ValueError(f"Missing columns for {metric.id}: {required - relation.columns.keys()}")
        for join in self.joins:
            left, right = relations.get(join.left), relations.get(join.right)
            if left is None or right is None:
                raise ValueError(f"Unknown join relation {join.id}")
            if (len(join.left_key) != len(join.right_key)
                    or not set(join.left_key) <= left.columns.keys()
                    or set(join.right_key) != set(right.key)):
                raise ValueError(f"Join {join.id} must target a unique relation key")
            if join.purpose == "project_measures" and (
                join.cardinality != "one_to_zero_or_one" or set(join.left_key) != set(left.key)
            ):
                raise ValueError("Project monetary joins must preserve project grain")
        return self

    def resolve(self, name: str) -> Metric:
        matches = [m for m in self.metrics if name.casefold().strip() in
                   {m.id.casefold(), m.label.casefold(), *(a.casefold() for a in m.aliases)}]
        if len(matches) != 1:
            raise ValueError("Metric needs clarification: " + ", ".join(m.id for m in matches))
        return matches[0]

    def require_queryable(self, metric_id: str, population: str) -> Metric:
        """Phase 2 contains candidates only. No flag or environment variable certifies them."""
        metric = next((m for m in self.metrics if m.id == metric_id), None)
        if metric is None or metric.population != population:
            raise ValueError("Unknown metric or unsupported population")
        raise ValueError(f"{metric.id} requires reviewed Phase 1 certification and Phase 2 parity")


def load_catalogue(path: Path | None = None) -> Catalogue:
    content = path.read_text(encoding="utf-8") if path else files(__package__).joinpath("catalogue.json").read_text(encoding="utf-8")
    return Catalogue.model_validate_json(content)
