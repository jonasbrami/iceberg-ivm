"""Tests for parsing a view query and rewriting it for a refresh."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest
import sqlglot

from iceberg_ivm.query_parser import VALID_GRANULARITIES, parse_view_query

# ---------------------------------------------------------------------------
# Accepted queries
# ---------------------------------------------------------------------------


class TestParseViewQuery:
    def test_full_shape(self):
        sql = """
            SELECT symbol, date_trunc('week', ts) AS week, sum(qty) AS volume
            FROM iceberg.md.trades
            WHERE color = 'red'
            GROUP BY 1, 2
        """
        p = parse_view_query(sql)
        assert (p.source_table, p.filter_column, p.granularity, p.merge_keys, p.bucket_alias) == (
            "iceberg.md.trades",
            "ts",
            "week",
            ("symbol", "week"),
            "week",
        )

    @pytest.mark.parametrize("granularity", sorted(VALID_GRANULARITIES))
    def test_every_granularity(self, granularity):
        sql = f"SELECT date_trunc('{granularity}', ts) AS bucket FROM t GROUP BY 1"
        p = parse_view_query(sql)
        assert p.granularity == granularity
        assert p.filter_column == "ts"
        assert p.merge_keys == ("bucket",)

    def test_qualified_table_name(self):
        p = parse_view_query("SELECT date_trunc('hour', ts) AS h FROM cat.sch.tbl GROUP BY 1")
        assert p.source_table == "cat.sch.tbl"

    def test_group_by_expression_matches_projection(self):
        sql = """
            SELECT symbol, date_trunc('week', ts) AS w
            FROM t
            GROUP BY symbol, date_trunc('week', ts)
        """
        p = parse_view_query(sql)
        assert p.merge_keys == ("symbol", "w")

    def test_group_by_positional(self):
        sql = """
            SELECT symbol, date_trunc('day', ts) AS d, sum(x) AS s
            FROM t
            GROUP BY 1, 2
        """
        p = parse_view_query(sql)
        assert p.merge_keys == ("symbol", "d")

    @pytest.mark.parametrize(
        "col",
        [
            # The 7 granularity keywords — common column names in chained MVs
            # where the upstream view aliased `date_trunc('X', ts) AS X`.
            "minute",
            "hour",
            "day",
            "week",
            "month",
            "quarter",
            "year",
        ],
    )
    def test_accepts_reserved_word_column(self, col):
        """Granularity keywords are common column names in chained views."""
        sql = f"SELECT date_trunc('hour', {col}) AS h FROM t GROUP BY 1"
        p = parse_view_query(sql)
        assert p.filter_column == col
        assert p.granularity == "hour"
        assert p.merge_keys == ("h",)

    @pytest.mark.parametrize("col", ["minute", "hour", "day"])
    def test_group_by_expression_with_reserved_word_column(self, col):
        """GROUP BY date_trunc('X', <keyword-col>) matches the projection."""
        sql = f"SELECT symbol, date_trunc('hour', {col}) AS h FROM t GROUP BY symbol, date_trunc('hour', {col})"
        p = parse_view_query(sql)
        assert p.filter_column == col
        assert p.merge_keys == ("symbol", "h")

    def test_chained_mv_pattern(self):
        """The canonical chained-MV shape: one MV reading another, with its
        time column named after the upstream granularity."""
        sql = """
            SELECT symbol, date_trunc('hour', minute) AS hour,
                   min_by(open, minute) AS open, max(high) AS high,
                   sum(volume) AS volume
            FROM iceberg.analytics.ohlcv_1m
            GROUP BY 1, 2
        """
        p = parse_view_query(sql)
        assert p.source_table == "iceberg.analytics.ohlcv_1m"
        assert p.filter_column == "minute"
        assert p.granularity == "hour"
        assert p.merge_keys == ("symbol", "hour")

    def test_bare_column_alias_is_its_own_name(self):
        sql = "SELECT symbol, date_trunc('day', ts) AS d FROM t GROUP BY symbol, d"
        p = parse_view_query(sql)
        # First merge key comes from the bare `symbol` projection
        assert p.merge_keys[0] == "symbol"

    def test_from_table_with_alias_strips_alias(self):
        # Regression: an alias on the FROM table used to leak into source_table,
        # producing invalid system-table references like "iceberg.x.y t$snapshots".
        sql = "SELECT date_trunc('day', ts) AS d FROM iceberg.market.trades t GROUP BY 1"
        p = parse_view_query(sql)
        assert p.source_table == "iceberg.market.trades"

    def test_from_table_with_as_alias_strips_alias(self):
        sql = "SELECT date_trunc('day', ts) AS d FROM iceberg.market.trades AS t GROUP BY 1"
        p = parse_view_query(sql)
        assert p.source_table == "iceberg.market.trades"

    # ---------------------------------------------------------------------------
    # Rejection cases
    # ---------------------------------------------------------------------------

    def test_bucket_grouped_by_alias(self):
        p = parse_view_query("SELECT sym, date_trunc('day', ts) AS d, count(*) AS n FROM t GROUP BY sym, d")
        assert (p.merge_keys, p.bucket_alias) == (("sym", "d"), "d")

    def test_output_names_are_lower_case(self):
        p = parse_view_query("SELECT Sym, date_trunc('day', ts) AS Day FROM t GROUP BY 1, 2")
        assert p.merge_keys == ("sym", "day")

    def test_date_trunc_elsewhere_is_allowed(self):
        """Only the grouped bucket matters; other date_trunc calls are ordinary expressions."""
        p = parse_view_query(
            "SELECT date_trunc('hour', ts) AS h, max(date_trunc('minute', ts)) AS last_minute FROM t "
            "WHERE date_trunc('day', created) > DATE '2026-01-01' GROUP BY 1"
        )
        assert (p.granularity, p.filter_column) == ("hour", "ts")

    def test_any_aggregate(self):
        p = parse_view_query(
            "SELECT date_trunc('minute', ts) AS m, approx_percentile(px, 0.5) AS p50, count(DISTINCT u) AS users, "
            "sum(x) FILTER (WHERE y > 0) AS pos FROM t GROUP BY 1"
        )
        assert p.merge_keys == ("m",)


# ---------------------------------------------------------------------------
# Rejected queries
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("sql", "message"),
    [
        # The time bucket must be selected and grouped by — else totals get overwritten.
        (
            "SELECT sym, count(*) AS n FROM t WHERE date_trunc('day', ts) > DATE '2026-01-01' GROUP BY sym",
            "time bucket",
        ),
        ("SELECT sym, count(*) AS n, max(date_trunc('day', ts)) AS d FROM t GROUP BY sym", "time bucket"),
        ("SELECT date_trunc('day', ts) + INTERVAL '1' HOUR AS d, count(*) AS n FROM t GROUP BY 1", "time bucket"),
        ("SELECT sym, count(*) AS n FROM t GROUP BY sym", "time bucket"),
        (
            "SELECT date_trunc('hour', ts) AS h, date_trunc('day', ts) AS d FROM t GROUP BY 1, 2",
            "more than one time bucket",
        ),
        ("SELECT date_trunc('fortnight', ts) AS d FROM t GROUP BY 1", "granularity"),
        ("SELECT date_trunc('day', ts + INTERVAL '1' HOUR) AS d FROM t GROUP BY 1", "directly to a column"),
        # Shape
        ("SELECT date_trunc('day', ts) AS d FROM t", "GROUP BY"),
        ("SELECT date_trunc('day', a.ts) AS d FROM a JOIN b ON a.id = b.id GROUP BY 1", "join"),
        (
            "SELECT date_trunc('day', ts) AS d FROM a GROUP BY 1 UNION SELECT date_trunc('day', ts) AS d FROM b GROUP BY 1",
            "set operations",
        ),
        ("WITH x AS (SELECT * FROM t) SELECT date_trunc('day', ts) AS d FROM x GROUP BY 1", "CTE"),
        ("SELECT date_trunc('day', ts) AS d FROM (SELECT * FROM t) GROUP BY 1", "subqueries"),
        ("SELECT date_trunc('day', ts) AS d FROM t WHERE x IN (SELECT x FROM u) GROUP BY 1", "subqueries"),
        # #66: a second source nested anywhere would be neither watched nor pinned
        (
            "SELECT date_trunc('day', ts) AS d, max(p) AS hi FROM t GROUP BY 1 HAVING max(p) > (SELECT avg(x) FROM o)",
            "subqueries",
        ),
        (
            "SELECT date_trunc('day', ts) AS d FROM t WHERE s IN (SELECT s FROM a UNION SELECT s FROM b) GROUP BY 1",
            "subqueries",
        ),
        ("SELECT date_trunc('day', ts) AS d, max(p) - (SELECT avg(x) FROM o) AS hi FROM t GROUP BY 1", "subqueries"),
        ("SELECT date_trunc('day', ts) AS d, count(*) FROM t GROUP BY 1", "alias"),
        ("SELECT * FROM t GROUP BY 1", "SELECT \\*"),
        ("SELECT date_trunc('day', ts) AS d, x AS d FROM t GROUP BY 1, 2", "duplicate"),
        ("SELECT date_trunc('day', ts) AS d FROM t GROUP BY 3", "out of range"),
        ("SELECT date_trunc('day', ts) AS d FROM t GROUP BY d, other", "must also appear"),
        ("SELECT date_trunc('day', ts) AS d FROM t GROUP BY ROLLUP (d)", "ROLLUP"),
        ("SELECT date_trunc('day', ts) AS d FROM t FOR VERSION AS OF 1 GROUP BY 1", "FOR VERSION"),
        ("SELECT date_trunc('day', ts) AS d FROM t {range_filter} GROUP BY 1", "range_filter"),
        ("SELECT date_trunc('day', ts) AS d, count(*) AS n FROM t GROUP BY 1 LIMIT 10", "LIMIT"),
        ("SELECT date_trunc('day', ts) AS d, count(*) AS n FROM t GROUP BY 1 OFFSET 5", "OFFSET"),
        (
            "SELECT date_trunc('day', ts) AS d, rank() OVER (ORDER BY count(*)) AS r FROM t GROUP BY 1",
            "window functions",
        ),
        ("SELECT 1; SELECT 2", "single"),
        ("   ", "empty"),
        ("SELEC date_trunc('day', ts) FROM", "could not parse"),
    ],
)
def test_rejected(sql, message):
    with pytest.raises(ValueError, match=message):
        parse_view_query(sql)


@pytest.mark.parametrize(
    "where",
    ["-- date_trunc('day', ts)", "/* date_trunc('day', ts) */", "AND y = 'date_trunc(''day'', ts)'"],
)
def test_date_trunc_in_comments_or_strings_does_not_count(where):
    with pytest.raises(ValueError, match="time bucket"):
        parse_view_query(f"SELECT sym, count(*) AS n FROM t WHERE x = 1 {where}\nGROUP BY sym")


# ---------------------------------------------------------------------------
# range_query
# ---------------------------------------------------------------------------

START = datetime(2026, 4, 6, tzinfo=UTC)
END = datetime(2026, 4, 13, tzinfo=UTC)


def where_of(sql: str) -> str:
    return sqlglot.parse_one(sql, read="trino").args["where"].sql(dialect="trino")


def test_range_query_adds_filter_and_pin():
    p = parse_view_query("SELECT date_trunc('day', ts) AS d, sum(x) AS s FROM iceberg.md.trades GROUP BY 1")
    out = p.range_query(START, END, 42)
    assert "FROM iceberg.md.trades FOR VERSION AS OF 42" in out
    assert where_of(out) == (
        "WHERE ts >= CAST('2026-04-06 00:00:00.000000 UTC' AS TIMESTAMP(6) WITH TIME ZONE) "
        "AND ts < CAST('2026-04-13 00:00:00.000000 UTC' AS TIMESTAMP(6) WITH TIME ZONE)"
    )


def test_range_query_parenthesises_an_existing_or():
    """``A OR B AND range`` would leave A-rows unfiltered by time."""
    p = parse_view_query("SELECT date_trunc('day', ts) AS d FROM t WHERE a = 1 OR b = 2 GROUP BY 1")
    assert where_of(p.range_query(START, END, 1)).startswith("WHERE (a = 1 OR b = 2) AND (ts >= ")


def test_range_query_keeps_comments_from_swallowing_the_filter():
    p = parse_view_query("SELECT date_trunc('day', ts) AS d FROM t WHERE a = 1 -- trailing\nGROUP BY 1")
    out = p.range_query(START, END, 1)
    assert "--" not in out
    assert "ts >= " in where_of(out)


def test_range_query_keeps_the_table_alias_and_qualifier():
    p = parse_view_query("SELECT date_trunc('day', t.ts) AS d FROM iceberg.md.trades AS t GROUP BY 1")
    out = p.range_query(START, END, 5)
    assert "FROM iceberg.md.trades FOR VERSION AS OF 5 AS t" in out
    assert "t.ts >= " in out


def test_range_query_naive_timestamps_stay_naive():
    p = parse_view_query("SELECT date_trunc('day', ts) AS d FROM t GROUP BY 1")
    out = p.range_query(datetime(2026, 4, 6), datetime(2026, 4, 7), 1)
    assert "CAST('2026-04-06 00:00:00.000000' AS TIMESTAMP(6))" in out


def test_range_query_converts_other_zones_to_utc():
    p = parse_view_query("SELECT date_trunc('day', ts) AS d FROM t GROUP BY 1")
    plus4 = timezone(timedelta(hours=4))
    out = p.range_query(datetime(2026, 4, 6, 4, tzinfo=plus4), datetime(2026, 4, 7, 4, tzinfo=plus4), 1)
    assert "'2026-04-06 00:00:00.000000 UTC'" in out


def test_range_query_does_not_mutate_the_parsed_view():
    p = parse_view_query("SELECT date_trunc('day', ts) AS d FROM t GROUP BY 1")
    first = p.range_query(START, END, 1)
    assert p.range_query(START, END, 1) == first
    assert first.count("FOR VERSION AS OF") == 1
