"""Tests for the refresh executor SQL generation."""
from datetime import datetime, timezone

from trino_mv_orchestrator.config import ViewConfig
from trino_mv_orchestrator.executor import (
    RefreshResult,
    build_merge_sql,
    execute_full_refresh,
    execute_incremental_refresh,
)


def make_view(**overrides) -> ViewConfig:
    defaults = dict(
        name="ohlcv_1m",
        query=(
            "SELECT symbol, date_trunc('day', ts) AS minute, open "
            "FROM iceberg.market_data.trades "
            "GROUP BY 1, 2"
        ),
    )
    defaults.update(overrides)
    return ViewConfig(**defaults)


class TestBuildMergeSql:
    def test_structure(self):
        sql = build_merge_sql(
            "iceberg.out.mv",
            "SELECT a FROM t WHERE ts >= X AND ts < Y GROUP BY 1",
            merge_keys=("symbol", "minute"),
            value_columns=["open"],
        )
        assert "MERGE INTO iceberg.out.mv AS t" in sql
        assert "ON t.symbol = s.symbol AND t.minute = s.minute" in sql
        assert "WHEN MATCHED THEN UPDATE SET open = s.open" in sql
        assert "WHEN NOT MATCHED THEN INSERT" in sql
        assert "ts >= X AND ts < Y" in sql


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


class MockCursorWithStats:
    """Cursor mock that exposes Trino query stats and a monotonic query_id.

    Every ``execute()`` increments the query id so tests can assert the
    executor captured a distinct id per query.
    """
    def __init__(self, stats: dict | None = None):
        self._stats = stats or {}
        self.executed = []
        self._counter = 0

    async def execute(self, sql: str):
        self.executed.append(sql)
        self._counter += 1

    @property
    def stats(self):
        return self._stats

    @property
    def query_id(self) -> str:
        return f"20260417_000000_{self._counter:05d}_abcde"

    @property
    def info_uri(self) -> str:
        return f"http://trino/ui/query.html?{self.query_id}"

    async def fetchone(self):
        return None

    async def fetchall(self):
        return []


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
            cursor, view, "iceberg.out.mv",
            filter_column="ts",
            merge_keys=("symbol", "minute"),
            value_columns=["open"],
            filter_range=(start, end),
        )
        assert isinstance(result, RefreshResult)
        assert result.processed_rows == 200
        assert result.processed_bytes == 8192

    async def test_merge_sql_contains_range_predicate(self):
        cursor = MockCursorWithStats()
        view = make_view()
        start = datetime(2026, 4, 8, tzinfo=timezone.utc)
        end = datetime(2026, 4, 9, tzinfo=timezone.utc)
        await execute_incremental_refresh(
            cursor, view, "iceberg.out.mv",
            filter_column="ts",
            merge_keys=("symbol", "minute"),
            value_columns=["open"],
            filter_range=(start, end),
        )
        merge_sql = cursor.executed[-1]
        assert "ts >= TIMESTAMP '2026-04-08 00:00:00.000000 UTC'" in merge_sql
        assert "ts < TIMESTAMP '2026-04-09 00:00:00.000000 UTC'" in merge_sql
        assert "MERGE INTO iceberg.out.mv" in merge_sql


# ── RefreshResult.queries — captured for the UI to link to the Trino UI ──

class TestQueryCapture:
    async def test_full_refresh_captures_delete_and_insert(self):
        cursor = MockCursorWithStats(stats={"processedRows": 100, "processedBytes": 2048})
        view = make_view()
        r = await execute_full_refresh(cursor, view, "iceberg.out.mv")
        assert len(r.queries) == 2
        stages = [q.stage for q in r.queries]
        assert stages == ["full_delete", "full_insert"]
        # distinct, populated query IDs
        ids = [q.query_id for q in r.queries]
        assert all(ids) and len(set(ids)) == 2
        # info_uri is the full Trino UI link
        assert all(q.info_uri.endswith(q.query_id) for q in r.queries)
        assert all("/ui/query.html?" in q.info_uri for q in r.queries)

    async def test_incremental_refresh_captures_merge(self):
        cursor = MockCursorWithStats(stats={"processedRows": 20, "processedBytes": 512})
        view = make_view()
        r = await execute_incremental_refresh(
            cursor, view, "iceberg.out.mv",
            filter_column="ts",
            merge_keys=("symbol", "minute"),
            value_columns=["open"],
            filter_range=(
                datetime(2026, 4, 8, tzinfo=timezone.utc),
                datetime(2026, 4, 9, tzinfo=timezone.utc),
            ),
        )
        assert len(r.queries) == 1
        q = r.queries[0]
        assert q.stage == "merge"
        assert q.query_id
        assert q.processed_rows == 20
        assert q.processed_bytes == 512
        assert q.elapsed_ms >= 0
        assert q.started_at > 0

    async def test_query_captured_when_stats_absent(self):
        cursor = MockCursorWithStats(stats={})
        view = make_view()
        r = await execute_full_refresh(cursor, view, "iceberg.out.mv")
        # still captures query_id even with no stats
        assert all(q.query_id for q in r.queries)
        # zero stats, not None
        assert all(q.processed_rows == 0 and q.processed_bytes == 0 for q in r.queries)
