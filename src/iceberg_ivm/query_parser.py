"""Parse a view query and rewrite it for one refresh.

A view query is what you would write after ``CREATE MATERIALIZED VIEW … AS``:
one SELECT over one Iceberg table, grouped by a ``date_trunc('<unit>', <col>)``
time bucket (plus any other keys). Everything a refresh needs is derived from
it: the source table, the time column, the bucket unit, and the merge keys.

Parsing and rewriting use sqlglot's Trino dialect, so the time-range filter
and the snapshot pin are added to the syntax tree rather than spliced into
SQL text.
"""

from __future__ import annotations

import functools
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError

DIALECT = "trino"

IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
QUALIFIED_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$")

VALID_GRANULARITIES = frozenset(("millisecond", "second", "minute", "hour", "day", "week", "month", "quarter", "year"))

_DATE_TRUNC = (exp.TimestampTrunc, exp.DateTrunc)


@dataclass(frozen=True)
class ParsedView:
    source_table: str  # [catalog.][schema.]table, as written
    filter_column: str  # the time column inside the bucket's date_trunc
    granularity: str  # the bucket's date_trunc unit, lower case
    bucket_alias: str  # output column that holds the bucket
    merge_keys: tuple[str, ...]  # GROUP BY keys as output column names (bucket included)
    _tree: exp.Select = field(repr=False, compare=False)
    _time_column: exp.Column = field(repr=False, compare=False)

    def range_query(self, start: datetime, end: datetime, snapshot_id: int) -> str:
        """The view query restricted to ``start <= time column < end``, reading
        the source as of ``snapshot_id``."""
        query = self._tree.copy()
        query.find(exp.Table).set(
            "version",
            exp.Version(this="VERSION", kind="AS OF", expression=exp.Literal.number(snapshot_id)),
        )
        in_range = exp.and_(
            exp.GTE(this=self._time_column.copy(), expression=timestamp_literal(start)),
            exp.LT(this=self._time_column.copy(), expression=timestamp_literal(end)),
        )
        return query.where(in_range, append=True, copy=False).sql(dialect=DIALECT)


def timestamp_literal(value: datetime) -> exp.Expression:
    """A Trino timestamp literal for ``value``. Aware values are written in UTC
    as ``TIMESTAMP WITH TIME ZONE`` so they denote the same instant."""
    if value.tzinfo is not None:
        text = value.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.%f") + " UTC"
        type_ = "timestamp(6) with time zone"
    else:
        text = value.strftime("%Y-%m-%d %H:%M:%S.%f")
        type_ = "timestamp(6)"
    return exp.cast(exp.Literal.string(text), exp.DataType.build(type_, dialect=DIALECT))


@functools.lru_cache(maxsize=256)
def parse_view_query(sql: str) -> ParsedView:
    """Validate a view query and derive its metadata.

    Raises ``ValueError`` with a message that tells the user what to change.
    """
    query = _single_select(sql)
    source_table = _source_table(query)

    group = query.args.get("group")
    if group is None or not group.expressions:
        raise ValueError("query must have a GROUP BY clause; the merge keys are derived from it")
    if any(group.args.get(k) for k in ("all", "rollup", "cube", "grouping_sets")):
        raise ValueError("GROUP BY ALL / ROLLUP / CUBE / GROUPING SETS are not supported; list the keys")

    projections = list(query.expressions)
    names = [_output_name(p) for p in projections]
    duplicates = sorted({n for n in names if names.count(n) > 1})
    if duplicates:
        raise ValueError(f"duplicate output column names: {duplicates}")
    merge_keys = tuple(dict.fromkeys(_resolve_group_item(g, projections, names) for g in group.expressions))

    bucket_alias, bucket = _time_bucket(query, projections, names, merge_keys)
    return ParsedView(
        source_table=source_table,
        filter_column=bucket.this.name,
        granularity=bucket.text("unit").lower(),
        bucket_alias=bucket_alias,
        merge_keys=merge_keys,
        _tree=query,
        _time_column=bucket.this,
    )


# ---------------------------------------------------------------------------
# internals
# ---------------------------------------------------------------------------


def _single_select(sql: str) -> exp.Select:
    if "{range_filter}" in sql:
        raise ValueError(
            "query contains the legacy {range_filter} placeholder; remove it — "
            "iceberg-ivm adds the time-range filter itself"
        )
    try:
        statements = [s for s in sqlglot.parse(sql, read=DIALECT) if s is not None]
    except ParseError as e:
        detail = e.errors[0]["description"] if e.errors else str(e)
        raise ValueError(f"could not parse query: {detail}") from None
    if not statements:
        raise ValueError("query is empty")
    if len(statements) > 1:
        raise ValueError("query must be a single SELECT statement; found several")
    query = statements[0]
    if isinstance(query, exp.SetOperation):
        raise ValueError("set operations (UNION/INTERSECT/EXCEPT) are not supported")
    if not isinstance(query, exp.Select):
        raise ValueError("query must be a SELECT")
    if query.args.get("with_") or query.args.get("with"):
        raise ValueError("CTEs (WITH clauses) are not supported")
    if query.args.get("joins"):
        raise ValueError("joins are not supported; the query must read from a single source table")
    if any(s is not query for s in query.find_all(exp.Select)):
        raise ValueError("subqueries are not supported")
    return query


def _source_table(query: exp.Select) -> str:
    tables = list(query.find_all(exp.Table))
    if len(tables) != 1:
        raise ValueError("query must read from exactly one table")
    table = tables[0]
    if table.args.get("version"):
        raise ValueError("remove FOR VERSION/TIMESTAMP AS OF; iceberg-ivm pins the snapshot itself")
    name = ".".join(part.name for part in table.parts)
    if not QUALIFIED_NAME_RE.match(name):
        raise ValueError(f"source table {name!r} must be a plain [catalog.][schema.]table name")
    return name


def _output_name(projection: exp.Expression) -> str:
    if isinstance(projection, exp.Star) or (isinstance(projection, exp.Column) and projection.is_star):
        raise ValueError("SELECT * is not supported; list the output columns")
    if isinstance(projection, (exp.Alias, exp.Column)):
        return projection.alias_or_name.lower()
    raise ValueError(
        f"projection {projection.sql(dialect=DIALECT)!r} has no alias; add `AS <name>` "
        "so the target table has a stable column name"
    )


def _resolve_group_item(item: exp.Expression, projections: list, names: list[str]) -> str:
    """Map a GROUP BY item (ordinal, expression or alias) to an output column name."""
    if isinstance(item, exp.Literal) and item.is_int:
        position = int(item.this)
        if not 1 <= position <= len(projections):
            raise ValueError(f"GROUP BY position {position} is out of range (the SELECT list has {len(projections)})")
        return names[position - 1]
    for projection, name in zip(projections, names, strict=True):
        if projection.unalias() == item:
            return name
    if isinstance(item, exp.Column) and not item.table and item.name.lower() in names:
        return item.name.lower()
    raise ValueError(f"GROUP BY item {item.sql(dialect=DIALECT)!r} must also appear in the SELECT list")


def _time_bucket(query: exp.Select, projections: list, names: list[str], merge_keys: tuple[str, ...]):
    """Return ``(alias, date_trunc node)`` of the single time bucket that is
    both selected and grouped by.

    Recomputing a time range is only correct when every group lives inside one
    bucket of that range. Without the bucket among the merge keys, a refresh
    would overwrite all-time totals with totals for just the new rows.
    """
    buckets = [
        (name, projection.unalias())
        for projection, name in zip(projections, names, strict=True)
        if isinstance(projection.unalias(), _DATE_TRUNC) and name in merge_keys
    ]
    if not buckets:
        example = next(query.find_all(*_DATE_TRUNC), None)
        hint = (
            f"date_trunc('{example.text('unit').lower()}', {example.this.sql(dialect=DIALECT)})"
            if example is not None
            else "date_trunc('<unit>', <time column>)"
        )
        raise ValueError(
            f"the GROUP BY must include a time bucket: select {hint} AS <name> and group by it. "
            "Without it, each refresh would overwrite all-time totals with totals for just the new rows"
        )
    if len(buckets) > 1:
        raise ValueError(f"the GROUP BY has more than one time bucket ({', '.join(n for n, _ in buckets)}); keep one")
    name, bucket = buckets[0]
    unit = bucket.text("unit").lower()
    if unit not in VALID_GRANULARITIES:
        raise ValueError(f"date_trunc granularity must be one of {sorted(VALID_GRANULARITIES)}; got {unit!r}")
    if not isinstance(bucket.this, exp.Column):
        raise ValueError(f"date_trunc must be applied directly to a column, not {bucket.this.sql(dialect=DIALECT)!r}")
    return name, bucket
