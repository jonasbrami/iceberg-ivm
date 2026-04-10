"""Tests for the refresh executor SQL generation."""
from datetime import datetime, timezone

from trino_mv_orchestrator.config import ViewConfig
from trino_mv_orchestrator.executor import (
    RefreshResult,
    build_merge_sql,
    build_range_filter,
    execute_full_refresh,
    execute_incremental_refresh,
    format_ts,
)


def make_view(**overrides) -> ViewConfig:
    defaults = dict(
        name="ohlcv_1m",
        source_table="iceberg.market_data.trades",
        query="SELECT symbol, minute, open FROM t WHERE {range_filter} GROUP BY 1, 2",
        merge_keys=("symbol", "minute"),
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


class TestFormatTs:
    def test_microsecond_precision(self):
        ts = datetime(2026, 4, 8, 10, 30, 45, 123456, tzinfo=timezone.utc)
        assert format_ts(ts) == "2026-04-08 10:30:45.123456"


class MockCursorWithStats:
    """Cursor mock that exposes Trino query stats."""
    def __init__(self, stats: dict | None = None):
        self._stats = stats or {}
        self.executed = []

    async def execute(self, sql: str):
        self.executed.append(sql)

    @property
    def stats(self):
        return self._stats

    async def fetchone(self):
        return None

    async def fetchall(self):
        return []


class TestRefreshResult:
    def test_dataclass_fields(self):
        r = RefreshResult(elapsed=1.5, processed_rows=100, processed_bytes=4096)
        assert r.elapsed == 1.5
        assert r.processed_rows == 100
        assert r.processed_bytes == 4096

    def test_defaults(self):
        r = RefreshResult(elapsed=0.5)
        assert r.processed_rows == 0
        assert r.processed_bytes == 0


class TestExecuteFullRefresh:
    async def test_returns_refresh_result_with_stats(self):
        cursor = MockCursorWithStats(stats={
            "processedRows": 5000,
            "processedBytes": 128000,
        })
        view = make_view()
        result = await execute_full_refresh(cursor, view, "iceberg.out.mv")
        assert isinstance(result, RefreshResult)
        assert result.elapsed > 0
        assert result.processed_rows == 5000
        assert result.processed_bytes == 128000

    async def test_returns_zero_stats_when_missing(self):
        cursor = MockCursorWithStats(stats={})
        view = make_view()
        result = await execute_full_refresh(cursor, view, "iceberg.out.mv")
        assert result.processed_rows == 0
        assert result.processed_bytes == 0


class TestExecuteIncrementalRefresh:
    async def test_returns_refresh_result_with_stats(self):
        cursor = MockCursorWithStats(stats={
            "processedRows": 200,
            "processedBytes": 8192,
        })
        view = make_view()
        start = datetime(2026, 4, 8, 0, 0, tzinfo=timezone.utc)
        end = datetime(2026, 4, 9, 0, 0, tzinfo=timezone.utc)
        result = await execute_incremental_refresh(
            cursor, view, "iceberg.out.mv", ["open"], (start, end),
        )
        assert isinstance(result, RefreshResult)
        assert result.processed_rows == 200
        assert result.processed_bytes == 8192
