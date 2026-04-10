"""Tests for the refresh executor SQL generation."""
from datetime import datetime, timezone

from trino_mv_orchestrator.config import ViewConfig
from trino_mv_orchestrator.executor import build_merge_sql, build_range_filter


def make_view(**overrides) -> ViewConfig:
    defaults = dict(
        name="ohlcv_1m",
        source_table="iceberg.market_data.trades",
        query="SELECT symbol, minute, open FROM t WHERE {range_filter} GROUP BY 1, 2",
        merge_keys=["symbol", "minute"],
        filter_column="ts",
        filter_granularity="day",
    )
    defaults.update(overrides)
    return ViewConfig(**defaults)


class TestBuildRangeFilter:
    def test_basic(self):
        start = datetime(2026, 4, 8, 0, 0, tzinfo=timezone.utc)
        end = datetime(2026, 4, 9, 0, 0, tzinfo=timezone.utc)
        f = build_range_filter("ts", start, end)
        assert "ts >= TIMESTAMP '2026-04-08 00:00:00.000000 UTC'" in f
        assert "ts < TIMESTAMP '2026-04-09 00:00:00.000000 UTC'" in f

    def test_pushdown_friendly(self):
        # The filter must be a plain column range, not a function call
        f = build_range_filter("ts", datetime(2026, 1, 1, tzinfo=timezone.utc), datetime(2026, 1, 2, tzinfo=timezone.utc))
        assert "date_trunc" not in f
        assert "CAST" not in f


class TestBuildMergeSql:
    def test_structure(self):
        view = make_view()
        sql = build_merge_sql(view, "iceberg.out.mv", "ts >= X", ["open"])
        assert "MERGE INTO iceberg.out.mv AS t" in sql
        assert "ON t.symbol = s.symbol AND t.minute = s.minute" in sql
        assert "WHEN MATCHED THEN UPDATE SET open = s.open" in sql
        assert "WHEN NOT MATCHED THEN INSERT" in sql
        assert "ts >= X" in sql
        assert "{range_filter}" not in sql
