"""Compile owned SQL templates over the ten Phase 3 gateways.

Only packaged catalogue expressions and validated identifiers enter SQL text.
All caller values are parameters. No SQL, expressions, joins or functions are
accepted from clients. Exploratory SQL remains a separate, later capability.
"""
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
import json

from ..database import GATEWAYS
from ..semantic.catalogue import Catalogue, Metric, load_catalogue
from ..semantic.periods import ResolvedPeriod, resolve_period
from .contracts import Column, MetricRequest

COMPILER_VERSION = "1.0.0"
MONTHLY = {"enquiry_monthly_actual", "bookings_monthly_actual", "invoice_monthly_actual"}


class InvalidMetricRequest(ValueError):
    pass


@dataclass(frozen=True)
class Statement:
    sql: str
    params: tuple


@dataclass(frozen=True)
class CompiledQuery:
    metric: Metric
    request: MetricRequest
    period: ResolvedPeriod | None
    rows: Statement
    total: Statement
    columns: list[Column]
    reference: str
    filtered: Statement


class Compiler:
    def __init__(self, catalogue: Catalogue | None = None):
        self.catalogue = catalogue or load_catalogue()
        self.catalogue_hash = sha256(self.catalogue.model_dump_json().encode()).hexdigest()
        self.metrics = {m.id: m for m in self.catalogue.metrics}
        self.relations = {r.id: r for r in self.catalogue.relations}
        self.dimensions = {d.id: d.column for d in self.catalogue.dimensions}

    def metric(self, metric_id: str, version: str, population: str) -> Metric:
        metric = self.metrics.get(metric_id)
        if (metric is None or metric.version != version or metric.population != population
                or metric.relation not in GATEWAYS
                or not any(p.id == population and p.available for p in self.catalogue.populations)):
            raise InvalidMetricRequest("unsupported_metric_contract")
        return metric

    def validate(self, request: MetricRequest) -> Metric:
        metric = self.metric(request.metric_id, request.metric_version, request.population)
        dimensions = request.dimensions
        filters = request.filters
        if request.comparison and request.comparison.filters is not None:
            filters = filters + request.comparison.filters
        if (len(dimensions) != len(set(dimensions)) or not set(dimensions) <= set(metric.dimensions)
                or any(f.dimension not in metric.dimensions for f in filters)
                or len({f.dimension for f in request.filters}) != len(request.filters)):
            raise InvalidMetricRequest("unsupported_dimensions")
        if request.comparison and request.comparison.filters is not None:
            names = [f.dimension for f in request.comparison.filters]
            if len(names) != len(set(names)):
                raise InvalidMetricRequest("duplicate_comparison_filters")
        if request.period not in metric.periods or (request.comparison and request.comparison.period not in metric.periods):
            raise InvalidMetricRequest("unsupported_period")
        if request.grain == "month" and metric.id not in MONTHLY:
            raise InvalidMetricRequest("unsupported_grain")
        if request.include_share and (metric.calculation.aggregation != "sum"
                                      or not (dimensions or request.grain == "month") or not request.include_total):
            raise InvalidMetricRequest("shares_require_additive_groups")
        allowed_order = {"value", *dimensions}
        if request.grain == "month":
            allowed_order.add("month")
        if any(o.column not in allowed_order for o in request.ordering):
            raise InvalidMetricRequest("unsupported_ordering")
        if len({o.column for o in request.ordering}) != len(request.ordering):
            raise InvalidMetricRequest("duplicate_ordering")
        return metric

    def compile(self, request: MetricRequest, *, now: datetime, business_timezone: str,
                max_cell_bytes: int = 900000) -> CompiledQuery:
        metric = self.validate(request)
        period = None if request.period == "all_stored" else resolve_period(
            request.period, now=now, business_timezone=business_timezone)
        clauses, params = [], []
        if period:
            clauses.append(f'"{metric.date_basis}" >= %s')
            params.append(period.start_date)
            if period.end_date_exclusive:
                clauses.append(f'"{metric.date_basis}" < %s')
                params.append(period.end_date_exclusive)
        if metric.family == "conversion":
            clauses.append("cohort_years = %s")
            params.append(5 if request.period == "five_year_cohort" else 2)
        # Positive/date/stage rules for monthly variants are owned by their views.
        for item in request.filters:
            column = self.dimensions[item.dimension]
            if item.operator == "is_null":
                clauses.append(f'"{column}" IS NULL')
            else:
                clauses.append(f'"{column}" = ANY(%s)')
                params.append(item.values)
        where = " AND ".join(clauses) or "true"
        base = f'WITH filtered AS (SELECT * FROM analyst_query."{metric.relation}" WHERE {where})'
        value = metric.calculation.expression
        if metric.id in MONTHLY:
            value = f"coalesce(({value}),0)"
        # Round only at contract precision, after aggregation.
        value = f"round(({value})::numeric,{metric.precision})"
        known = f'count("{metric.value_columns[0]}")'
        if metric.family == "gestation":
            known = "count(*) FILTER (WHERE gestation_period > 0)"
        numerator = metric.calculation.numerator or "NULL::numeric"
        denominator = metric.calculation.denominator or "NULL::numeric"
        measures = (f"{value} AS value, count(*)::bigint AS source_rows, {known}::bigint AS known_values, "
                    f"({numerator})::numeric AS numerator, ({denominator})::numeric AS denominator")
        total = Statement(base + " SELECT " + measures + " FROM filtered", tuple(params))
        keys = (["month"] if request.grain == "month" else []) + list(request.dimensions)
        key_expr = [f'"{self.dimensions[d]}" AS "{d}"' for d in request.dimensions]
        if request.grain == "month":
            key_expr.insert(0, f'date_trunc(\'month\',"{metric.date_basis}")::date AS month')
        grouping = " GROUP BY " + ",".join(str(i + 1) for i in range(len(keys))) if keys else ""
        grouped = ", aggregated AS (SELECT " + ",".join(key_expr + [measures]) + " FROM filtered" + grouping + ")"
        if request.grain == "month":
            grouped += ", months AS (SELECT generate_series(%s::date,%s::date - interval '1 month',interval '1 month')::date AS month)"
            params.extend([period.start_date, period.end_date_exclusive])
            dims = ",".join(f'"{d}"' for d in request.dimensions)
            combos = f" CROSS JOIN (SELECT DISTINCT {dims} FROM filtered) dimensions" if dims else ""
            select_keys = ["months.month"] + [f'dimensions."{d}"' for d in request.dimensions]
            match = " AND ".join(f'g."{key}" IS NOT DISTINCT FROM {expr}' for key, expr in zip(keys, select_keys))
            grouped += (", groups AS (SELECT " + ",".join(select_keys)
                        + ",coalesce(g.value,0)::numeric AS value,coalesce(g.source_rows,0)::bigint AS source_rows,"
                        + "coalesce(g.known_values,0)::bigint AS known_values,g.numerator,g.denominator"
                        + f" FROM months{combos} LEFT JOIN aggregated g ON {match})")
        else:
            grouped += ", groups AS (SELECT * FROM aggregated)"
        ordering = [f'g."{o.column}" {o.direction.upper()} NULLS LAST' for o in request.ordering]
        ordered = {o.column for o in request.ordering}
        ordering.extend(f'g."{key}" ASC NULLS LAST' for key in keys if key not in ordered)
        if not ordering:
            ordering = ["value DESC NULLS LAST"]
        # Do not transfer an unbounded CRM classification even for a single row.
        projection = ["g.month"] if request.grain == "month" else []
        for dimension in request.dimensions:
            projection.append(f'CASE WHEN octet_length(g."{dimension}")<=%s THEN g."{dimension}" END AS "{dimension}"')
            params.append(max_cell_bytes)
        oversized = " OR ".join(f'coalesce(octet_length(g."{d}"),0)>%s' for d in request.dimensions) or "false"
        params.extend([max_cell_bytes] * len(request.dimensions))
        projection.extend(["g.value", "g.source_rows", "g.known_values", "g.numerator", "g.denominator",
                           f"({oversized}) AS oversized_cell", "count(*) OVER()::bigint AS matched_groups"])
        statement = (base + grouped + " SELECT " + ",".join(projection) + " FROM groups g ORDER BY "
                     + ",".join(ordering) + " LIMIT %s")
        params.append(request.limit + 1)
        columns = [Column(name=k, type="date" if k == "month" else "text") for k in keys]
        columns += [Column(name="value", type="decimal", unit=metric.unit, precision=metric.precision),
                    Column(name="source_rows", type="integer", unit="count"),
                    Column(name="known_values", type="integer", unit="count")]
        if metric.family == "conversion":
            columns += [Column(name=n, type="decimal", unit="count", precision=0) for n in ("numerator", "denominator")]
        if request.include_share:
            columns.append(Column(name="share", type="decimal", unit="ratio"))
        reference = sha256(json.dumps([COMPILER_VERSION, self.catalogue_hash, request.model_dump(mode="json"), statement, params],
                                     default=str, sort_keys=True).encode()).hexdigest()
        return CompiledQuery(metric, request, period, Statement(statement, tuple(params)), total, columns, reference,
                             Statement(base, total.params))
