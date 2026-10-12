"""Allowlist validation for model-written SQL.

Only the statement regenerated from the validated syntax tree is executed. Parsing
is one layer: the read-only role, read-only transaction, statement timeout and
gateway grants remain the enforcement boundary.
"""
from dataclasses import dataclass
import re

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError

POLICY_VERSION = "1.0.0"
SCHEMA = "analyst_query"
MAX_SQL_CHARS = 8000
MAX_NODES = 4000
MAX_COLUMNS = 20
NAME = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")
WORD = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")

# SQLGlot expression classes permitted anywhere in a read-only analytical query.
ALLOWED = frozenset({
    # Query structure
    "Select", "Union", "Except", "Intersect", "Subquery", "From", "Join", "Lateral", "Where", "Group",
    "Having", "Order", "Ordered", "Limit", "Offset", "Fetch", "Distinct", "With", "CTE", "Table",
    "TableAlias", "Alias", "Column", "Identifier", "Star", "Literal", "Boolean", "Null", "Var",
    "Interval", "DataType", "DataTypeParam", "Paren", "Tuple", "Values", "Window", "WindowSpec",
    "Filter", "WithinGroup",
    # Operators
    "Add", "Sub", "Mul", "Div", "IntDiv", "Mod", "Neg", "EQ", "NEQ", "GT", "GTE", "LT", "LTE", "And",
    "Or", "Not", "Is", "In", "Between", "Like", "ILike", "Exists", "Any", "All", "DPipe",
    "NullSafeEQ", "NullSafeNEQ",
    # Conditional and conversion
    "Case", "If", "Coalesce", "Nullif", "Greatest", "Least", "Cast",
    # Aggregates
    "Sum", "Count", "Avg", "Min", "Max", "Stddev", "StddevSamp", "StddevPop", "Variance", "VariancePop",
    "PercentileCont", "PercentileDisc", "Mode", "Corr", "CovarSamp", "CovarPop", "GroupConcat",
    "LogicalOr", "LogicalAnd",
    # Window functions
    "RowNumber", "Rank", "DenseRank", "Ntile", "CumeDist", "PercentRank", "Lag", "Lead", "FirstValue",
    "LastValue", "NthValue",
    # Numeric
    "Abs", "Ceil", "Floor", "Round", "Trunc", "Sign", "Sqrt", "Pow", "Ln", "Log", "Exp", "WidthBucket",
    # Dates
    "Extract", "TimestampTrunc", "DateTrunc", "CurrentDate", "CurrentTimestamp", "StrToDate",
    "StrToTime", "TimeToStr", "ExplodingGenerateSeries", "GenerateSeries",
    # Text
    "Lower", "Upper", "Trim", "Length", "Substring", "SplitPart", "StrPosition", "Concat", "ConcatWs",
    "Replace", "Left", "Right", "Initcap", "Pad", "StartsWith",
})
# Functions SQLGlot does not model, by PostgreSQL name.
ANONYMOUS = frozenset({
    "age", "make_date", "date_part", "every", "isfinite", "justify_days", "justify_interval",
    "trim_scale", "scale", "div", "log10", "cbrt", "starts_with", "btrim", "ltrim", "rtrim", "lpad",
    "rpad", "strpos", "num_nulls", "num_nonnulls",
})
TABLE_FUNCTIONS = frozenset({"ExplodingGenerateSeries", "GenerateSeries"})
TYPES = frozenset({"DATE", "TIMESTAMP", "TIMESTAMPTZ", "DECIMAL", "INT", "BIGINT", "SMALLINT", "TEXT",
                   "VARCHAR", "CHAR", "BOOLEAN", "DOUBLE", "FLOAT", "INTERVAL"})
# PostgreSQL evaluates these bare keywords as session functions, not columns.
SESSION_KEYWORDS = frozenset({"current_user", "session_user", "user", "current_role", "current_catalog",
                              "current_schema", "system_user"})


class UnsafeQuery(ValueError):
    """Fixed text naming only SQL constructs; safe to return to the model for repair."""


@dataclass(frozen=True)
class ValidatedQuery:
    sql: str
    relations: tuple[str, ...]
    columns: tuple[str, ...]


def validate(text: str, relations: frozenset[str]) -> ValidatedQuery:
    tree = _parse(text)
    _check(tree, relations)
    sql = tree.sql(dialect="postgres", comments=False)
    # Validate the exact statement that will execute, not only the model's text.
    regenerated = _parse(sql)
    used = _check(regenerated, relations)
    if regenerated.sql(dialect="postgres", comments=False) != sql:
        raise UnsafeQuery("The query could not be normalised deterministically; simplify it.")
    if not used:
        raise UnsafeQuery("Read at least one approved analyst_query view.")
    return ValidatedQuery(sql, tuple(sorted(used)), _outputs(regenerated))


def _parse(text):
    if not isinstance(text, str) or not text.strip():
        raise UnsafeQuery("Provide one SELECT statement.")
    if len(text) > MAX_SQL_CHARS:
        raise UnsafeQuery(f"The query exceeds {MAX_SQL_CHARS} characters.")
    try:
        statements = [s for s in sqlglot.parse(text, read="postgres") if s is not None]
    except SqlglotError:
        raise UnsafeQuery("The query is not valid PostgreSQL syntax.") from None
    if len(statements) != 1:
        raise UnsafeQuery("Provide exactly one statement.")
    tree = statements[0]
    if not isinstance(tree, (exp.Select, exp.Union, exp.Except, exp.Intersect)):
        raise UnsafeQuery("Only a single SELECT query is permitted.")
    return tree


def _check(tree, relations) -> set[str]:
    used = set()
    for count, node in enumerate(tree.walk(), 1):
        if count > MAX_NODES:
            raise UnsafeQuery("The query is too complex; simplify it.")
        kind = type(node).__name__
        if isinstance(node, exp.Anonymous):
            if not isinstance(node.this, str) or node.this.lower() not in ANONYMOUS:
                raise UnsafeQuery(f"Function {str(node.this)[:63]} is not permitted.")
        elif kind not in ALLOWED:
            label = node.sql_name() if isinstance(node, exp.Func) else kind
            raise UnsafeQuery(f"{label} is not permitted in exploratory queries.")
        elif isinstance(node, exp.DataType):
            name = node.this.name if isinstance(node.this, exp.DataType.Type) else None
            if name not in TYPES or (node.expressions and name not in {"DECIMAL", "VARCHAR", "CHAR"}):
                raise UnsafeQuery("Cast only to date, timestamp, numeric, integer, text, boolean, double or interval types.")
        elif isinstance(node, exp.Var):
            if not WORD.match(node.name):
                raise UnsafeQuery("Unsupported keyword in the query.")
        elif isinstance(node, exp.With):
            if node.args.get("recursive"):
                raise UnsafeQuery("Recursive CTEs are not permitted.")
        elif isinstance(node, exp.CTE):
            if not NAME.match(node.alias):
                raise UnsafeQuery("Name CTEs with lower-case snake_case identifiers.")
        elif isinstance(node, exp.Column):
            if not node.table and node.name.lower() in SESSION_KEYWORDS:
                raise UnsafeQuery(f"{node.name} is not permitted.")
        elif isinstance(node, exp.Table):
            _table(node, relations, used)
    return used


def _table(node, relations, used):
    if node.args.get("catalog"):
        raise UnsafeQuery("Database-qualified relations are not permitted.")
    source = node.this
    if isinstance(source, exp.Func):
        if type(source).__name__ not in TABLE_FUNCTIONS:
            raise UnsafeQuery("Only generate_series may be used as a table function.")
        return
    name, schema = node.name, node.db
    if schema:
        if schema != SCHEMA or name not in relations:
            raise UnsafeQuery(f"{schema[:63]}.{name[:63]} is not an approved view.")
        used.add(name)
    elif name not in _visible_ctes(node):
        raise UnsafeQuery(f"{name[:63]} is not an approved view; reference analyst_query.<view> or an earlier CTE.")


def _visible_ctes(node) -> set[str]:
    """CTE names PostgreSQL would resolve here; anything else falls through to search_path."""
    names, came_from, parent = set(), node, node.parent
    while parent is not None:
        if isinstance(parent, exp.With):
            # A non-recursive CTE sees only earlier siblings, never itself or later ones.
            index = next((i for i, cte in enumerate(parent.expressions) if cte is came_from), len(parent.expressions))
            names.update(cte.alias for cte in parent.expressions[:index])
            came_from, parent = parent.parent, parent.parent.parent if parent.parent is not None else None
            continue
        scope = parent.args.get("with_")
        if isinstance(scope, exp.With):
            names.update(cte.alias for cte in scope.expressions)
        came_from, parent = parent, parent.parent
    return names


def _outputs(tree) -> tuple[str, ...]:
    select = tree
    while isinstance(select, (exp.Union, exp.Except, exp.Intersect, exp.Subquery)):
        select = select.this
    if not isinstance(select, exp.Select):
        raise UnsafeQuery("Only a single SELECT query is permitted.")
    names = []
    for projection in select.expressions:
        if isinstance(projection, exp.Alias):
            name = projection.alias
        elif isinstance(projection, exp.Column) and not isinstance(projection.this, exp.Star):
            name = projection.name
        else:
            raise UnsafeQuery("Give every output column an explicit lower-case snake_case alias; SELECT * is not permitted.")
        if not NAME.match(name):
            raise UnsafeQuery("Output column aliases must be lower-case snake_case.")
        names.append(name)
    if len(set(names)) != len(names):
        raise UnsafeQuery("Output column names must be unique.")
    if not 1 <= len(names) <= MAX_COLUMNS:
        raise UnsafeQuery(f"Return between 1 and {MAX_COLUMNS} columns.")
    return tuple(names)
