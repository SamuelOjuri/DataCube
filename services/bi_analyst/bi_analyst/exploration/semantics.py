"""Conservative summary metadata from the executed syntax tree.

Unrecognised expressions still appear in tables; they are never guessed to be
additive or averaged as rates. In particular, window/cumulative totals cannot be
added again. Ratio components must also be returned by the same SELECT.
"""
import re

import sqlglot
from sqlglot import exp
from sqlglot.errors import OptimizeError, SqlglotError
from sqlglot.optimizer.scope import Scope, traverse_scope


def unwrap(node):
    while isinstance(node, (exp.Alias, exp.Paren, exp.Cast, exp.Round)):
        node = node.this
    return node


def projections(select, inherited=None):
    """Follow simple CTE/subquery projections without guessing through joins.

    This supports the usual monthly-aggregate CTE followed by rate expressions.
    The SQL policy has already forbidden recursive CTEs.
    """
    ctes = dict(inherited or {})
    scope = select.args.get("with_")
    if scope:
        for cte in scope.expressions:
            if isinstance(cte.this, exp.Select):
                ctes[cte.alias] = projections(cte.this, ctes)
    source = select.args.get("from_")
    source = source.this if source else None
    parent = None
    if not select.args.get("joins"):
        if isinstance(source, exp.Table) and not source.db:
            parent = ctes.get(source.name)
        elif isinstance(source, exp.Subquery) and isinstance(source.this, exp.Select):
            parent = projections(source.this, ctes)
    mapping = parent[0] if parent else {}

    def resolve(node):
        if isinstance(node, exp.Column) and node.name in mapping and (not node.table or node.table == source.alias_or_name):
            return mapping[node.name].copy()
        return node

    expressions = {n.alias_or_name: unwrap(unwrap(n).transform(resolve)) for n in select.expressions}
    grouped = bool(select.args.get("group")) or bool(parent and parent[1])
    raw_rows = (not grouped and not select.args.get("distinct") and not select.args.get("joins")
                and (parent is None or parent[2]))
    return expressions, grouped, raw_rows


def series_grain(node):
    """Recognise calendar steps; owned dates still have to pass bucket validation."""
    if not isinstance(node, exp.GenerateSeries):
        return None
    step = node.args.get("step")
    if isinstance(step, exp.Interval) and isinstance(step.this, exp.Literal):
        interval = f"{step.this.this} {step.args.get('unit', '')}"
    elif (isinstance(step, exp.Cast) and step.to.this == exp.DataType.Type.INTERVAL
          and isinstance(step.this, exp.Literal) and step.this.is_string):
        interval = step.this.this
    else:
        return None
    match = re.fullmatch(r"\s*([1-9]\d*)\s+([a-z]+)\s*", interval, re.IGNORECASE)
    if not match:
        return None
    amount, unit = int(match[1]), match[2].lower().removesuffix("s")
    return {(1, "day"): "day", (1, "week"): "week", (7, "day"): "week",
            (1, "month"): "month", (3, "month"): "quarter",
            (1, "year"): "year", (12, "month"): "year"}.get((amount, unit))


def calendar_grains(select):
    """Trace only calendar axes through scoped CTEs, subqueries and joins.

    Join cardinality is not evidence of additivity. Keep this separate from the
    measure projections above; duplicate or misaligned dates are checked later.
    """
    inferred = {}
    try:
        for scope in traverse_scope(select):
            if not isinstance(scope.expression, exp.Select):
                inferred[scope] = {}
                continue
            sources = {}
            for alias, (_, source) in scope.selected_sources.items():
                if isinstance(source, Scope):
                    sources[alias] = inferred.get(source, {})
                elif isinstance(source, exp.Table) and isinstance(source.this, exp.GenerateSeries):
                    names = source.alias_column_names or [source.alias_or_name, "generate_series"]
                    sources[alias] = {name: series_grain(source.this) for name in names if name}
                else:
                    sources[alias] = {}

            def grain(node):
                node = unwrap(node)
                if isinstance(node, (exp.DateTrunc, exp.TimestampTrunc)):
                    unit = str(node.args.get("unit", "")).strip("'").lower()
                    return unit if unit in {"day", "week", "month", "quarter", "year"} else None
                if isinstance(node, exp.Column) and not node.db and not node.catalog:
                    if node.table:
                        return sources.get(node.table, {}).get(node.name)
                    # A bare column across joins may refer to a USING/coalesced
                    # key. Do not assign it another source's calendar metadata.
                    if len(sources) == 1:
                        return next(iter(sources.values())).get(node.name)
                    return None
                return series_grain(node)

            outputs = {}
            for i, node in enumerate(scope.expression.expressions):
                name = scope.outer_columns[i] if i < len(scope.outer_columns) else node.alias_or_name
                outputs[name] = grain(node) if name not in outputs else None
            inferred[scope] = outputs
            if scope.expression is select:
                return outputs
    except OptimizeError:
        # Unresolvable aliases must not break otherwise useful table output.
        pass
    return {}


def describe_columns(sql, columns):
    columns = [c.model_copy(update={"additive": False, "grouped": False, "numerator": None,
                                   "denominator": None, "time_grain": None}) for c in columns]
    try:
        tree = sqlglot.parse_one(sql, read="postgres")
    except SqlglotError:
        return columns, True
    limited = any(isinstance(n, (exp.Limit, exp.Offset, exp.Fetch)) for n in tree.walk())
    if not isinstance(tree, exp.Select):
        return columns, limited
    expressions, grouped, raw_rows = projections(tree)
    grains = calendar_grains(tree)
    updates = {}
    for column in columns:
        node = expressions.get(column.name)
        if node is None:
            continue
        identifier = isinstance(node, exp.Column) and (node.name == "id" or node.name.endswith("_id"))
        additive = (isinstance(node, (exp.Sum, exp.Count)) and not node.find(exp.Window)
                    and not node.find(exp.Distinct)) or (raw_rows and isinstance(node, exp.Column) and not identifier)
        updates[column.name] = column.model_copy(update={
            "grouped": grouped,
            "additive": bool(additive and column.role == "measure" and column.unit in {"count", "source_currency"}),
            "time_grain": grains.get(column.name) if column.role == "dimension" and column.type == "date" else None})
    for name, column in list(updates.items()):
        node = expressions[name]
        if column.unit != "ratio" or not isinstance(node, exp.Div):
            continue
        numerator, denominator = unwrap(node.this), unwrap(node.expression)
        if isinstance(denominator, exp.Nullif) and denominator.expression == exp.Literal.number(0):
            denominator = unwrap(denominator.this)
        # Exact expression matching avoids associating an unrelated count with a rate.
        top = [n for n, e in expressions.items() if e == numerator and updates.get(n) and updates[n].additive]
        bottom = [n for n, e in expressions.items() if e == denominator and updates.get(n) and updates[n].additive]
        if len(top) == len(bottom) == 1:
            updates[name] = column.model_copy(update={"numerator": top[0], "denominator": bottom[0]})
    return [updates.get(c.name, c) for c in columns], limited
