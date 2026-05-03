"""Tests for the AST-based query parser."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from iceberg_ivm.query_parser import (
    ParsedView,
    VALID_GRANULARITIES,
    inject_range_filter,
    parse_view_query,
)


# ---------------------------------------------------------------------------
# Happy path
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
        assert p == ParsedView(
            source_table="iceberg.md.trades",
            filter_column="ts",
            granularity="week",
            merge_keys=("symbol", "week"),
            bucket_alias="week",
        )

    @pytest.mark.parametrize("granularity", sorted(VALID_GRANULARITIES))
    def test_every_granularity(self, granularity):
        sql = f"SELECT date_trunc('{granularity}', ts) AS bucket FROM t GROUP BY 1"
        p = parse_view_query(sql)
        assert p.granularity == granularity
        assert p.filter_column == "ts"
        assert p.merge_keys == ("bucket",)

    def test_qualified_table_name(self):
        p = parse_view_query(
            "SELECT date_trunc('hour', ts) AS h FROM cat.sch.tbl GROUP BY 1"
        )
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

    @pytest.mark.parametrize("col", [
        # The 7 granularity keywords — common column names in chained MVs
        # where the upstream view aliased `date_trunc('X', ts) AS X`.
        "minute", "hour", "day", "week", "month", "quarter", "year",
        # Other SQL reserved words Trino accepts as unquoted column names.
        "order", "group", "desc", "asc", "by",
    ])
    def test_accepts_reserved_word_column(self, col):
        """date_trunc's second arg can be any valid Trino identifier, including
        SQL reserved words. sqlparse tokenizes those as Keyword, not Name,
        so the parser must accept Keyword tokens as column references too."""
        sql = f"SELECT date_trunc('hour', {col}) AS h FROM t GROUP BY 1"
        p = parse_view_query(sql)
        assert p.filter_column == col
        assert p.granularity == "hour"
        assert p.merge_keys == ("h",)

    @pytest.mark.parametrize("col", ["minute", "hour", "day"])
    def test_group_by_expression_with_reserved_word_column(self, col):
        """GROUP BY date_trunc('X', <keyword-col>) matches the projection."""
        sql = (
            f"SELECT symbol, date_trunc('hour', {col}) AS h "
            f"FROM t GROUP BY symbol, date_trunc('hour', {col})"
        )
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
        sql = (
            "SELECT date_trunc('day', ts) AS d "
            "FROM iceberg.market.trades t GROUP BY 1"
        )
        p = parse_view_query(sql)
        assert p.source_table == "iceberg.market.trades"

    def test_from_table_with_as_alias_strips_alias(self):
        sql = (
            "SELECT date_trunc('day', ts) AS d "
            "FROM iceberg.market.trades AS t GROUP BY 1"
        )
        p = parse_view_query(sql)
        assert p.source_table == "iceberg.market.trades"


# ---------------------------------------------------------------------------
# Rejection cases
# ---------------------------------------------------------------------------


class TestRejections:
    def test_no_date_trunc(self):
        with pytest.raises(ValueError, match="date_trunc"):
            parse_view_query("SELECT a FROM t GROUP BY 1")

    def test_date_trunc_in_arithmetic(self):
        with pytest.raises(ValueError, match="arithmetic"):
            parse_view_query(
                "SELECT date_trunc('day', ts) - INTERVAL '1' DAY AS x "
                "FROM t GROUP BY 1"
            )

    def test_date_trunc_multiplied(self):
        with pytest.raises(ValueError, match="arithmetic"):
            parse_view_query(
                "SELECT 2 * date_trunc('day', ts) AS x FROM t GROUP BY 1"
            )

    def test_multiple_granularities(self):
        with pytest.raises(ValueError, match="multiple distinct granularities"):
            parse_view_query(
                "SELECT date_trunc('day', ts) AS d, date_trunc('hour', ts) AS h "
                "FROM t GROUP BY 1, 2"
            )

    def test_date_trunc_on_different_columns(self):
        with pytest.raises(ValueError, match="multiple columns"):
            parse_view_query(
                "SELECT date_trunc('day', a) AS a_d, date_trunc('day', b) AS b_d "
                "FROM t GROUP BY 1, 2"
            )

    def test_date_trunc_inside_string_literal_is_not_matched(self):
        # The regex-based predecessor would falsely match.
        with pytest.raises(ValueError, match="date_trunc"):
            parse_view_query(
                "SELECT 'date_trunc(''week'', ts)' AS s FROM t GROUP BY 1"
            )

    def test_date_trunc_inside_line_comment_is_not_matched(self):
        with pytest.raises(ValueError, match="date_trunc"):
            parse_view_query(
                "SELECT a FROM t -- date_trunc('hour', ts)\nGROUP BY 1"
            )

    def test_date_trunc_inside_block_comment_is_not_matched(self):
        with pytest.raises(ValueError, match="date_trunc"):
            parse_view_query(
                "SELECT a FROM t /* date_trunc('hour', ts) */ GROUP BY 1"
            )

    def test_join_rejected(self):
        with pytest.raises(ValueError, match="join"):
            parse_view_query(
                "SELECT a FROM t1 JOIN t2 ON t1.x = t2.x GROUP BY 1"
            )

    def test_union_rejected(self):
        with pytest.raises(ValueError, match="set operations"):
            parse_view_query(
                "SELECT date_trunc('day', ts) AS d FROM t1 UNION "
                "SELECT date_trunc('day', ts) AS d FROM t2"
            )

    def test_with_cte_rejected(self):
        with pytest.raises(ValueError, match="CTE"):
            parse_view_query(
                "WITH x AS (SELECT 1) SELECT date_trunc('day', ts) AS d "
                "FROM x GROUP BY 1"
            )

    def test_subquery_in_from_rejected(self):
        with pytest.raises(ValueError, match="subqueries"):
            parse_view_query(
                "SELECT date_trunc('day', ts) AS d FROM (SELECT ts FROM t) x "
                "GROUP BY 1"
            )

    def test_no_group_by_rejected(self):
        with pytest.raises(ValueError, match="GROUP BY"):
            parse_view_query("SELECT date_trunc('day', ts) AS d FROM t")

    def test_legacy_range_filter_placeholder_rejected(self):
        with pytest.raises(ValueError, match="range_filter"):
            parse_view_query(
                "SELECT date_trunc('day', ts) AS d FROM t "
                "WHERE {range_filter} GROUP BY 1"
            )

    def test_projection_function_without_alias_rejected(self):
        with pytest.raises(ValueError, match="alias"):
            parse_view_query(
                "SELECT date_trunc('day', ts), symbol FROM t GROUP BY 1, 2"
            )

    def test_multi_statement_rejected(self):
        with pytest.raises(ValueError, match="single"):
            parse_view_query(
                "SELECT date_trunc('day', ts) AS d FROM t GROUP BY 1; "
                "SELECT 1"
            )

    def test_empty_query_rejected(self):
        with pytest.raises(ValueError, match="empty"):
            parse_view_query("")

    def test_invalid_granularity_rejected(self):
        with pytest.raises(ValueError, match="granularity"):
            parse_view_query(
                "SELECT date_trunc('millennium', ts) AS m FROM t GROUP BY 1"
            )


# ---------------------------------------------------------------------------
# bucket_alias
# ---------------------------------------------------------------------------


class TestBucketAlias:
    def test_simple(self):
        p = parse_view_query(
            "SELECT date_trunc('day', ts) AS d FROM t GROUP BY 1"
        )
        assert p.bucket_alias == "d"

    def test_among_other_projections(self):
        p = parse_view_query(
            "SELECT symbol, date_trunc('hour', ts) AS bucket, sum(x) AS s "
            "FROM t GROUP BY 1, 2"
        )
        assert p.bucket_alias == "bucket"

    def test_reserved_word_alias(self):
        """A Trino reserved word as alias still becomes a string."""
        p = parse_view_query(
            "SELECT date_trunc('minute', time) AS minute FROM t GROUP BY 1"
        )
        assert p.bucket_alias == "minute"

    def test_chained_mv(self):
        """date_trunc('hour', minute) AS hour — alias, not the source column."""
        p = parse_view_query(
            "SELECT date_trunc('hour', minute) AS hour FROM t GROUP BY 1"
        )
        assert p.bucket_alias == "hour"

    def test_none_when_nested_inside_another_function(self):
        """If ``date_trunc`` only appears inside another function, there is
        no direct projection to alias, so ``bucket_alias`` is ``None``.
        Config validation rejects such views only when ``full_refresh_chunk``
        is set (see test_config.py); they remain valid otherwise."""
        p = parse_view_query(
            "SELECT from_iso8601_date(CAST(date_trunc('day', ts) AS varchar)) AS d "
            "FROM t GROUP BY 1"
        )
        # date_trunc still detected by _extract_date_trunc, but no direct
        # projection item equals date_trunc('day', ts) so bucket_alias is None.
        assert p.granularity == "day"
        assert p.filter_column == "ts"
        assert p.bucket_alias is None


# ---------------------------------------------------------------------------
# inject_range_filter
# ---------------------------------------------------------------------------


class TestInjectRangeFilter:
    START = datetime(2026, 4, 6, tzinfo=timezone.utc)
    END = datetime(2026, 4, 13, tzinfo=timezone.utc)

    def _predicate(self):
        return (
            "ts >= TIMESTAMP '2026-04-06 00:00:00.000000 UTC' AND "
            "ts < TIMESTAMP '2026-04-13 00:00:00.000000 UTC'"
        )

    def test_appends_onto_existing_where(self):
        sql = (
            "SELECT date_trunc('day', ts) AS d FROM t "
            "WHERE color = 'red' GROUP BY 1"
        )
        out = inject_range_filter(sql, "ts", self.START, self.END)
        # Original predicate still there, AND-joined with the new one,
        # and GROUP BY still present.
        assert "color = 'red'" in out
        assert self._predicate() in out
        assert out.count("WHERE") == 1
        assert "GROUP BY 1" in out

    def test_inserts_new_where_when_absent(self):
        sql = "SELECT date_trunc('day', ts) AS d FROM t GROUP BY 1"
        out = inject_range_filter(sql, "ts", self.START, self.END)
        assert "WHERE" in out
        assert self._predicate() in out
        assert "GROUP BY 1" in out
        # Ordering: WHERE comes before GROUP BY
        assert out.index("WHERE") < out.index("GROUP BY")

    def test_places_before_having(self):
        sql = (
            "SELECT date_trunc('day', ts) AS d, count(*) AS c "
            "FROM t GROUP BY 1 HAVING c > 5"
        )
        out = inject_range_filter(sql, "ts", self.START, self.END)
        assert out.index("WHERE") < out.index("GROUP BY") < out.index("HAVING")

    def test_places_before_order_by(self):
        sql = "SELECT date_trunc('day', ts) AS d FROM t GROUP BY 1 ORDER BY d"
        out = inject_range_filter(sql, "ts", self.START, self.END)
        assert out.index("WHERE") < out.index("GROUP BY") < out.index("ORDER BY")

    def test_places_before_limit(self):
        sql = "SELECT date_trunc('day', ts) AS d FROM t GROUP BY 1 LIMIT 10"
        out = inject_range_filter(sql, "ts", self.START, self.END)
        assert out.index("WHERE") < out.index("LIMIT")

    def test_naive_datetime_omits_utc_suffix(self):
        sql = "SELECT date_trunc('day', ts) AS d FROM t GROUP BY 1"
        start = datetime(2026, 4, 6)
        end = datetime(2026, 4, 13)
        out = inject_range_filter(sql, "ts", start, end)
        assert "UTC" not in out
        assert "TIMESTAMP '2026-04-06 00:00:00.000000'" in out

    def test_tz_aware_non_utc_converted_to_utc(self):
        from datetime import timedelta
        tz = timezone(timedelta(hours=5))
        start = datetime(2026, 4, 6, 5, 0, 0, tzinfo=tz)   # 00:00 UTC
        end = datetime(2026, 4, 13, 5, 0, 0, tzinfo=tz)    # 00:00 UTC
        sql = "SELECT date_trunc('day', ts) AS d FROM t GROUP BY 1"
        out = inject_range_filter(sql, "ts", start, end)
        assert "TIMESTAMP '2026-04-06 00:00:00.000000 UTC'" in out
        assert "TIMESTAMP '2026-04-13 00:00:00.000000 UTC'" in out

    def test_existing_where_with_string_literal_containing_where(self):
        # Regex paranoia: a string literal whose value contains the word
        # 'WHERE' must land inside the parens, not split the body.
        sql = (
            "SELECT date_trunc('day', ts) AS d FROM t "
            "WHERE label = 'WHERE clause' GROUP BY 1"
        )
        out = inject_range_filter(sql, "ts", self.START, self.END)
        body = out.split("WHERE", 1)[1].split("GROUP BY", 1)[0]
        assert "(label = 'WHERE clause')" in body
        assert self._predicate() in out

    def test_existing_where_multiline_with_or(self):
        sql = (
            "SELECT date_trunc('day', ts) AS d FROM t\n"
            "WHERE region = 'US'\n"
            "   OR region = 'EU'\n"
            "GROUP BY 1"
        )
        out = inject_range_filter(sql, "ts", self.START, self.END)
        body = out.split("WHERE", 1)[1].split("GROUP BY", 1)[0]
        assert "(" in body and ")" in body
        assert "region = 'US'" in body and "region = 'EU'" in body
        assert self._predicate() in out

    def test_existing_where_with_or_is_parenthesised(self):
        # Regression: AND binds tighter than OR, so AND-appending the time
        # predicate onto `WHERE A OR B` used to yield `A OR (B AND ts in range)`,
        # leaving A-rows unfiltered by time and silently corrupting the target.
        sql = (
            "SELECT date_trunc('day', ts) AS d FROM t "
            "WHERE region = 'US' OR region = 'EU' GROUP BY 1"
        )
        out = inject_range_filter(sql, "ts", self.START, self.END)
        # The injected SQL must filter every branch of the OR.
        # Re-parsing as a simple precedence check: the OR must be inside parens
        # so the trailing AND-chain applies to the whole disjunction.
        where_body = out.split("WHERE", 1)[1].split("GROUP BY", 1)[0]
        assert "(region = 'US' OR region = 'EU')" in where_body
        assert self._predicate() in out

    def test_result_parses_as_valid_query(self):
        """Round-trip: inject, re-parse, extract same fields."""
        sql = (
            "SELECT symbol, date_trunc('week', ts) AS week, sum(qty) AS v "
            "FROM t WHERE color = 'red' GROUP BY 1, 2"
        )
        out = inject_range_filter(sql, "ts", self.START, self.END)
        p = parse_view_query(out)
        assert p.granularity == "week"
        assert p.filter_column == "ts"
        assert p.merge_keys == ("symbol", "week")
