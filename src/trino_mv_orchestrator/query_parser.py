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

    where = _find_where(stmt)
    if where is not None:
        # Append " AND <predicate>" onto the existing WHERE.  Any trailing
        # whitespace inside the Where node must stay at the tail so the
        # next keyword (GROUP BY / ORDER BY / …) stays separated.
        _append_to_where(where, predicate)
    else:
        _insert_new_where(stmt, predicate)
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
    if "JOIN" in top_keywords or any("JOIN" in k for k in top_keywords):
        raise ValueError("joins are not supported; query must reference a single source table")
    if top_keywords.count("SELECT") > 1:
        raise ValueError("subqueries are not supported")
    if "GROUP BY" not in top_keywords:
        raise ValueError(
            "query must have a GROUP BY clause; merge keys are derived from it"
        )


def _extract_source_table(stmt: Statement) -> str:
    """Return the qualified name following FROM."""
    tokens = list(stmt.tokens)
    for i, tok in enumerate(tokens):
        if tok.ttype is Keyword and tok.normalized.upper() == "FROM":
            for t in tokens[i + 1:]:
                if t.ttype in (Whitespace,) or (t.ttype is not None and t.ttype in Whitespace):
                    continue
                if isinstance(t, Identifier):
                    name = str(t).strip()
                    if name.startswith("("):
                        raise ValueError("subqueries in FROM are not supported")
                    return name
                if isinstance(t, Parenthesis):
                    raise ValueError("subqueries in FROM are not supported")
                if t.ttype is Name:
                    return str(t).strip()
                if not str(t).strip():
                    continue
                raise ValueError(
                    f"could not parse table reference after FROM: {str(t)!r}"
                )
    raise ValueError("FROM clause missing")


def _walk(node) -> Iterator[tuple[object, object]]:
    """Yield (parent, child) for every descendant token of ``node``."""
    for t in getattr(node, "tokens", []):
        yield node, t
        yield from _walk(t)


def _ancestors_contain_operation(stmt: Statement, target) -> bool:
    """True if the target Function has an Operation in its ancestor chain."""
    parents: dict[int, object] = {}
    for parent, child in _walk(stmt):
        parents[id(child)] = parent
    cur = parents.get(id(target))
    while cur is not None:
        if isinstance(cur, Operation):
            return True
        cur = parents.get(id(cur))
    return False


def _extract_date_trunc(stmt: Statement) -> tuple[str, str]:
    """Return (granularity, column_name) from date_trunc('X', col)."""
    calls = [
        tok
        for _parent, tok in _walk(stmt)
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
        if _ancestors_contain_operation(stmt, call):
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
    """Return (granularity_literal, column_name) from a date_trunc Function."""
    paren = next((t for t in fn.tokens if isinstance(t, Parenthesis)), None)
    if paren is None:
        raise ValueError("malformed date_trunc call")
    parts: list = []
    for t in paren.tokens:
        if t.ttype is Whitespace:
            continue
        if t.ttype is Punctuation and str(t) in ("(", ")", ","):
            continue
        if isinstance(t, IdentifierList):
            for inner in t.tokens:
                if inner.ttype is Whitespace:
                    continue
                if inner.ttype is Punctuation and str(inner) in (",",):
                    continue
                parts.append(inner)
        else:
            parts.append(t)
    if len(parts) < 2:
        raise ValueError("date_trunc requires two arguments: granularity and column")
    first, second = parts[0], parts[1]
    g_raw = str(first).strip()
    if not (g_raw.startswith("'") and g_raw.endswith("'")):
        raise ValueError(
            f"first argument to date_trunc must be a string literal, got {g_raw!r}"
        )
    g = g_raw[1:-1].lower()
    if isinstance(second, Identifier):
        col = second.get_real_name()
    elif second.ttype is Name:
        col = str(second)
    else:
        raise ValueError(
            "second argument to date_trunc must be a bare column name; "
            f"got {str(second)!r}"
        )
    return g, col


def _projection_list(stmt: Statement) -> list:
    """Return the list of projection items (between SELECT and FROM).

    Each item is the bare sqlparse token (Identifier / Function / Token).
    """
    tokens = list(stmt.tokens)
    select_idx = next(
        (i for i, t in enumerate(tokens) if t.ttype is DML and t.normalized.upper() == "SELECT"),
        None,
    )
    from_idx = next(
        (i for i, t in enumerate(tokens) if t.ttype is Keyword and t.normalized.upper() == "FROM"),
        None,
    )
    if select_idx is None or from_idx is None:
        raise ValueError("query must be of the form SELECT ... FROM ...")
    proj_tokens = [
        t
        for t in tokens[select_idx + 1:from_idx]
        if t.ttype not in (Whitespace,) and str(t).strip()
    ]
    if not proj_tokens:
        raise ValueError("SELECT list is empty")
    if len(proj_tokens) == 1 and isinstance(proj_tokens[0], IdentifierList):
        return [
            it
            for it in proj_tokens[0].tokens
            if it.ttype not in (Whitespace, Punctuation) and str(it).strip()
        ]
    return proj_tokens


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


def _extract_merge_keys(stmt: Statement) -> tuple[str, ...]:
    """Resolve GROUP BY items against the projection; return aliases."""
    tokens = list(stmt.tokens)
    gb_idx = next(
        (
            i
            for i, t in enumerate(tokens)
            if t.ttype is Keyword and t.normalized.upper() == "GROUP BY"
        ),
        None,
    )
    if gb_idx is None:
        raise ValueError("GROUP BY clause missing")

    # Collect items after GROUP BY, stopping at the next keyword
    gb_tokens = []
    for t in tokens[gb_idx + 1:]:
        if t.ttype is Whitespace or not str(t).strip():
            continue
        if t.ttype is Keyword and t.normalized.upper() in (
            "HAVING",
            "ORDER BY",
            "LIMIT",
            "OFFSET",
            "FETCH",
        ):
            break
        gb_tokens.append(t)

    items: list = []
    if len(gb_tokens) == 1 and isinstance(gb_tokens[0], IdentifierList):
        for t in gb_tokens[0].tokens:
            if t.ttype in (Whitespace, Punctuation):
                continue
            if not str(t).strip():
                continue
            items.append(t)
    else:
        items = gb_tokens

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


def _find_where(stmt: Statement) -> Where | None:
    for t in stmt.tokens:
        if isinstance(t, Where):
            return t
    return None


def _append_to_where(where: Where, predicate: str) -> None:
    """Append `` AND (<predicate>)`` to an existing WHERE, preserving any
    trailing whitespace that separates it from the next clause.
    """
    # Move trailing-whitespace tokens out so we insert before them.
    # (Whitespace ttype may be the Newline subtype — `in` does a hierarchy check.)
    trailing: list = []
    while where.tokens and where.tokens[-1].ttype in Whitespace:
        trailing.insert(0, where.tokens.pop())
    where.tokens.append(Token(Whitespace, " "))
    where.tokens.append(Token(Keyword, "AND"))
    where.tokens.append(Token(Whitespace, " "))
    where.tokens.append(Token(None, predicate))
    if trailing:
        where.tokens.extend(trailing)
    else:
        where.tokens.append(Token(Whitespace, " "))


def _insert_new_where(stmt: Statement, predicate: str) -> None:
    """Insert a new WHERE clause between FROM and GROUP BY / HAVING / ORDER / LIMIT."""
    insert_idx = None
    for i, t in enumerate(stmt.tokens):
        if t.ttype is Keyword and t.normalized.upper() in (
            "GROUP BY",
            "HAVING",
            "ORDER BY",
            "LIMIT",
            "OFFSET",
            "FETCH",
        ):
            insert_idx = i
            break
    if insert_idx is None:
        insert_idx = len(stmt.tokens)

    where_str = f"WHERE {predicate} "
    where_tok = Token(None, where_str)
    # Pad so the WHERE sits on its own line visually
    stmt.tokens.insert(insert_idx, where_tok)
