"""AST-based parser for materialized view definitions.

Extracts the source table, filter column, granularity, and merge keys from a
standalone `SELECT … FROM … [WHERE …] GROUP BY …` query so operators can write
exactly what they would put after ``CREATE MATERIALIZED VIEW … AS``.

Uses sqlparse for tokenization/grouping.  It is a token-tree library, not a
full parser, but the structural node types (``Function``, ``Operation``,
``Where``, ``IdentifierList``, ``Identifier``) are sufficient for the handful
of decisions we need to make.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterator

import sqlparse
from sqlparse.sql import (
    Function,
    Identifier,
    IdentifierList,
    Operation,
    Parenthesis,
    Statement,
    Token,
    Where,
)
from sqlparse.tokens import (
    CTE,
    DML,
    Keyword,
    Literal,
    Name,
    Punctuation,
    Whitespace,
)

log = logging.getLogger(__name__)

IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
QUALIFIED_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$")
_DATE_TRUNC_RE = re.compile(
    r"^\s*date_trunc\s*\(\s*'([^']+)'\s*,\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)\s*$",
    re.IGNORECASE,
)

VALID_GRANULARITIES = frozenset(
    ("minute", "hour", "day", "week", "month", "quarter", "year")
)


@dataclass(frozen=True)
class ParsedView:
    source_table: str
    filter_column: str
    granularity: str
    merge_keys: tuple[str, ...]


def parse_view_query(sql: str) -> ParsedView:
    """Validate the query shape and derive all view metadata.

    Raises ``ValueError`` on any violation with a message that names the field
    the user needs to fix.
    """
    stmt = _single_statement(sql)
    _reject_unsupported_shapes(stmt)
    source_table = _extract_source_table(stmt)
    granularity, filter_column = _extract_date_trunc(stmt)
    merge_keys = _extract_merge_keys(stmt)
    return ParsedView(
        source_table=source_table,
        filter_column=filter_column,
        granularity=granularity,
        merge_keys=merge_keys,
    )


_TRAILING_CLAUSES = ("GROUP BY", "HAVING", "ORDER BY", "LIMIT", "OFFSET", "FETCH")


def inject_range_filter(
    sql: str, filter_column: str, start: datetime, end: datetime
) -> str:
    """AND-append a ``col >= TIMESTAMP 'start' AND col < TIMESTAMP 'end'``
    predicate to the query's WHERE clause, inserting one if absent.

    Returns the resulting SQL as a string.  The query is never executed here;
    Trino parses the emitted string at refresh time.
    """
    stmt = _single_statement(sql)
    predicate = _build_range_predicate(filter_column, start, end)

    where = next((t for t in stmt.tokens if isinstance(t, Where)), None)
    if where is not None:
        # Append " AND <predicate>" onto the existing WHERE, keeping any
        # trailing whitespace so the next keyword stays separated.
        original = str(where)
        stripped = original.rstrip()
        trailing = original[len(stripped):] or " "
        where.tokens = [Token(None, f"{stripped} AND {predicate}{trailing}")]
        return str(stmt)

    # No WHERE: insert one before the first trailing clause, else at end.
    insert_idx = next(
        (i for i, t in enumerate(stmt.tokens)
         if t.ttype is Keyword and t.normalized.upper() in _TRAILING_CLAUSES),
        len(stmt.tokens),
    )
    stmt.tokens.insert(insert_idx, Token(None, f"WHERE {predicate} "))
    return str(stmt)


# ---------------------------------------------------------------------------
# internals
# ---------------------------------------------------------------------------


def _single_statement(sql: str) -> Statement:
    stmts = [s for s in sqlparse.parse(sql) if str(s).strip()]
    if not stmts:
        raise ValueError("query is empty")
    if len(stmts) > 1:
        raise ValueError(
            "query must contain a single SELECT statement; multiple statements found"
        )
    return stmts[0]


def _reject_unsupported_shapes(stmt: Statement) -> None:
    """Reject queries whose shape the parser is not built to handle."""
    if "{range_filter}" in str(stmt):
        raise ValueError(
            "query contains the legacy {range_filter} placeholder; "
            "remove it — the orchestrator now injects the time-range WHERE "
            "predicate automatically"
        )

    top_keywords = [
        tok.normalized.upper()
        for tok in stmt.tokens
        if tok.ttype in (Keyword, DML, CTE)
    ]

    if "WITH" in top_keywords:
        raise ValueError("CTEs (WITH clauses) are not supported")
    if "UNION" in top_keywords or "INTERSECT" in top_keywords or "EXCEPT" in top_keywords:
        raise ValueError("set operations (UNION/INTERSECT/EXCEPT) are not supported")
    if any("JOIN" in k for k in top_keywords):
        raise ValueError("joins are not supported; query must reference a single source table")
    if top_keywords.count("SELECT") > 1:
        raise ValueError("subqueries are not supported")
    if "GROUP BY" not in top_keywords:
        raise ValueError(
            "query must have a GROUP BY clause; merge keys are derived from it"
        )


def _extract_source_table(stmt: Statement) -> str:
    """Return the qualified name following FROM."""
    from_idx = next(
        (i for i, t in enumerate(stmt.tokens)
         if t.ttype is Keyword and t.normalized.upper() == "FROM"),
        None,
    )
    if from_idx is None:
        raise ValueError("FROM clause missing")
    _, t = stmt.token_next(from_idx, skip_ws=True, skip_cm=True)
    if isinstance(t, Parenthesis):
        raise ValueError("subqueries in FROM are not supported")
    if isinstance(t, Identifier) or (t is not None and t.ttype is Name):
        name = str(t).strip()
        # A subquery-with-alias (e.g. `(SELECT …) x`) is wrapped as an
        # Identifier whose text starts with `(`.
        if name.startswith("("):
            raise ValueError("subqueries in FROM are not supported")
        return name
    raise ValueError(
        f"could not parse table reference after FROM: {str(t)!r}"
    )


def _iter_tokens(node) -> Iterator[object]:
    """Yield every descendant token of ``node`` (pre-order)."""
    for t in getattr(node, "tokens", []):
        yield t
        yield from _iter_tokens(t)


def _in_arithmetic(tok) -> bool:
    """True if ``tok`` is nested inside an ``Operation`` node."""
    cur = tok.parent
    while cur is not None:
        if isinstance(cur, Operation):
            return True
        cur = cur.parent
    return False


def _extract_date_trunc(stmt: Statement) -> tuple[str, str]:
    """Return (granularity, column_name) from date_trunc('X', col)."""
    calls = [
        tok
        for tok in _iter_tokens(stmt)
        if isinstance(tok, Function)
        and (tok.get_real_name() or "").lower() == "date_trunc"
    ]
    if not calls:
        raise ValueError(
            "query must contain a date_trunc('X', col) expression; "
            "granularity and filter column are derived from it"
        )

    granularities: set[str] = set()
    column: str | None = None
    for call in calls:
        if _in_arithmetic(call):
            raise ValueError(
                "date_trunc(...) must not be wrapped in arithmetic; "
                "use date_trunc('X', col) directly in SELECT / GROUP BY"
            )
        g, col = _read_date_trunc_args(call)
        if g not in VALID_GRANULARITIES:
            raise ValueError(
                f"date_trunc granularity must be one of {sorted(VALID_GRANULARITIES)}; got {g!r}"
            )
        granularities.add(g)
        if column is None:
            column = col
        elif column != col:
            raise ValueError(
                f"date_trunc is used on multiple columns ({column!r} and {col!r}); "
                "a view may only have a single filter column"
            )

    if len(granularities) > 1:
        raise ValueError(
            f"query has multiple distinct granularities: {sorted(granularities)}"
        )
    return granularities.pop(), column


def _read_date_trunc_args(fn: Function) -> tuple[str, str]:
    """Return (granularity_literal, column_name) from a date_trunc Function.

    sqlparse has already identified this token as a function call named
    ``date_trunc``; we just pull the two arguments out of its source text.
    The regex requires a string-literal first arg and a bare-identifier
    second arg — the same shape the old token-walker accepted — which
    also admits Trino reserved words (``minute``, ``hour``, …) as
    unquoted column names.
    """
    m = _DATE_TRUNC_RE.fullmatch(str(fn))
    if not m:
        raise ValueError(
            f"malformed date_trunc call {str(fn)!r}; "
            "expected date_trunc('<granularity>', <column>)"
        )
    return m.group(1).lower(), m.group(2)


def _clause_items(tokens: list, start_idx: int, stop_keywords: frozenset[str]) -> list:
    """Collect items after ``tokens[start_idx]`` up to a stop keyword (or end).

    Flattens a single trailing ``IdentifierList`` into its children so the
    caller gets a uniform list regardless of whether sqlparse chose to group
    comma-separated items.
    """
    items = []
    for t in tokens[start_idx + 1:]:
        if t.is_whitespace or not str(t).strip():
            continue
        if t.ttype is Keyword and t.normalized.upper() in stop_keywords:
            break
        items.append(t)
    if len(items) == 1 and isinstance(items[0], IdentifierList):
        return [
            it for it in items[0].tokens
            if not it.is_whitespace
            and it.ttype is not Punctuation
            and str(it).strip()
        ]
    return items


def _projection_list(stmt: Statement) -> list:
    """Return the list of projection items (between SELECT and FROM)."""
    tokens = list(stmt.tokens)
    select_idx = next(
        (i for i, t in enumerate(tokens)
         if t.ttype is DML and t.normalized.upper() == "SELECT"),
        None,
    )
    if select_idx is None:
        raise ValueError("query must be of the form SELECT ... FROM ...")
    items = _clause_items(tokens, select_idx, frozenset({"FROM"}))
    if not items:
        raise ValueError("SELECT list is empty")
    return items


def _projection_alias(item) -> str:
    """Return the column name this projection item becomes in the output.

    A bare column (Identifier with no alias) → its name.
    An aliased expression (Identifier with AS) → the alias.
    A bare Function with no alias → raises (operator must add an alias).
    """
    if isinstance(item, Identifier):
        alias = item.get_alias()
        if alias is not None:
            return alias
        real = item.get_real_name()
        if real and not isinstance(item.token_first(skip_cm=True), Function):
            # plain column reference — its own name is the output name
            return real
        raise ValueError(
            f"projection item {str(item)!r} has no alias; "
            "add `AS <name>` so the target table has a stable column name"
        )
    if isinstance(item, Function):
        raise ValueError(
            f"projection item {str(item)!r} has no alias; "
            "add `AS <name>` so the target table has a stable column name"
        )
    if item.ttype is Name:
        return str(item)
    raise ValueError(f"cannot determine output name for projection item {str(item)!r}")


_AFTER_GROUP_BY = frozenset({"HAVING", "ORDER BY", "LIMIT", "OFFSET", "FETCH"})


def _extract_merge_keys(stmt: Statement) -> tuple[str, ...]:
    """Resolve GROUP BY items against the projection; return aliases."""
    tokens = list(stmt.tokens)
    gb_idx = next(
        (i for i, t in enumerate(tokens)
         if t.ttype is Keyword and t.normalized.upper() == "GROUP BY"),
        None,
    )
    if gb_idx is None:
        raise ValueError("GROUP BY clause missing")
    items = _clause_items(tokens, gb_idx, _AFTER_GROUP_BY)

    projection = _projection_list(stmt)
    proj_aliases = [_projection_alias(p) for p in projection]
    proj_canonical = [_canonical(p) for p in projection]

    keys: list[str] = []
    for it in items:
        if it.ttype is Literal.Number.Integer:
            idx = int(str(it)) - 1
            if idx < 0 or idx >= len(projection):
                raise ValueError(
                    f"GROUP BY position {str(it)} out of range (projection has "
                    f"{len(projection)} items)"
                )
            keys.append(proj_aliases[idx])
            continue

        # Match by alias first (e.g. `GROUP BY d` referencing `… AS d`),
        # then by canonical expression (e.g. `GROUP BY date_trunc('week', ts)`).
        canon = _canonical(it)
        if canon in proj_aliases:
            keys.append(canon)
            continue
        try:
            pos = proj_canonical.index(canon)
        except ValueError:
            raise ValueError(
                f"GROUP BY expression {str(it)!r} does not match any projection item"
            )
        keys.append(proj_aliases[pos])

    if not keys:
        raise ValueError("GROUP BY is empty")
    return tuple(keys)


def _canonical(tok) -> str:
    """Normalize a projection / group-by item for equality comparison.

    Strips whitespace and any trailing `AS alias` clause.
    """
    if isinstance(tok, Identifier):
        # Drop the alias part: Identifier contains [Expr, (ws), 'AS', (ws), Alias]
        first = None
        for t in tok.tokens:
            if t.ttype is Whitespace:
                continue
            first = t
            break
        if first is not None:
            return str(first).strip().lower()
    return str(tok).strip().lower()


def _build_range_predicate(col: str, start: datetime, end: datetime) -> str:
    """Emit ``col >= TIMESTAMP '…' AND col < TIMESTAMP '…'``.

    Tz-aware inputs are first converted to UTC and suffixed with ' UTC' so
    Trino (running with session timezone=UTC) reads them as the correct instant.
    """
    if start.tzinfo is not None:
        start = start.astimezone(timezone.utc)
        end = end.astimezone(timezone.utc)
        suffix = " UTC"
    else:
        suffix = ""
    fmt = "%Y-%m-%d %H:%M:%S.%f"
    return (
        f"{col} >= TIMESTAMP '{start.strftime(fmt)}{suffix}' AND "
        f"{col} < TIMESTAMP '{end.strftime(fmt)}{suffix}'"
    )


