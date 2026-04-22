"""Tests for the refresh executor SQL generation."""
from datetime import datetime, timezone

from trino_mv_orchestrator.config import ViewConfig
from trino_mv_orchestrator.executor import (
    RefreshResult,
    build_merge_sql,
    execute_chunked_full_refresh,
    execute_full_refresh,
    execute_incremental_refresh,
)
from trino_mv_orchestrator.query_parser import parse_view_query


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

    ``fetchall_responses`` lets a test script the rows returned by
    ``fetchall()`` — each call pops the next list. Used by the chunked
    refresh tests to mock ``$files`` metadata reads (source range and
    target bucket max) before any MERGE runs.
    """
    def __init__(self, stats: dict | None = None, fetchall_responses: list[list] | None = None):
        self._stats = stats or {}
        self.executed = []
        self._counter = 0
        self._fetchall_responses = list(fetchall_responses or [])

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
        if self._fetchall_responses:
            return self._fetchall_responses.pop(0)
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


# ── execute_chunked_full_refresh ──


def _files_row(column: str, lower: str, upper: str) -> tuple:
    """One ($files, readable_metrics,) row for MockCursorWithStats."""
    return ({column: {"lower_bound": lower, "upper_bound": upper}},)


def _chunked_view() -> ViewConfig:
    # Mirrors the real hyperliquid_ohlcv_m1 shape closely enough:
    # source is fully qualified, has a date_trunc bucket alias, value columns.
    return ViewConfig(
        name="ohlcv_1m",
        query=(
            "SELECT symbol, date_trunc('minute', ts) AS minute, "
            "sum(qty) AS volume "
            "FROM iceberg.market_data.trades "
            "GROUP BY 1, 2"
        ),
        full_refresh_chunk="day",
    )


class TestExecuteChunkedFullRefresh:
    async def test_emits_one_merge_per_day_chunk(self):
        """Source covers Apr 8 10:00 → Apr 10 15:00. Snapped to day bounds
        that's Apr 8 00:00 → Apr 11 00:00 → three chunks."""
        view = _chunked_view()
        parsed = parse_view_query(view.query)
        cursor = MockCursorWithStats(
            stats={"processedRows": 100, "processedBytes": 4096},
            fetchall_responses=[
                # get_source_column_range → source $files
                [_files_row("ts", "2026-04-08T10:00:00+00:00",
                                 "2026-04-10T15:00:00+00:00")],
                # get_target_bucket_max → target $files (empty)
                [],
            ],
        )
        r = await execute_chunked_full_refresh(
            cursor, view, "iceberg.out.mv", parsed,
            value_columns=["volume"],
            chunk_granularity="day",
        )
        assert not r.interrupted
        assert len(r.queries) == 3
        assert all(q.stage == "chunk_merge" for q in r.queries)
        # Three MERGE statements emitted after the two $files reads, in
        # chronological order — non-overlapping and contiguous.
        merges = [s for s in cursor.executed if "MERGE INTO" in s]
        assert len(merges) == 3
        expected_windows = [
            ("2026-04-08 00:00:00.000000 UTC", "2026-04-09 00:00:00.000000 UTC"),
            ("2026-04-09 00:00:00.000000 UTC", "2026-04-10 00:00:00.000000 UTC"),
            ("2026-04-10 00:00:00.000000 UTC", "2026-04-11 00:00:00.000000 UTC"),
        ]
        for merge, (lo, hi) in zip(merges, expected_windows):
            assert f"ts >= TIMESTAMP '{lo}'" in merge
            assert f"ts < TIMESTAMP '{hi}'" in merge
        # And contiguity: the upper of chunk i equals the lower of chunk i+1.
        for (_, hi), (lo, _) in zip(expected_windows, expected_windows[1:]):
            assert hi == lo
        # Aggregate stats summed across chunks
        assert r.processed_rows == 300
        assert r.processed_bytes == 4096 * 3

    async def test_resume_from_target_bucket_max(self):
        """Target already contains Apr 8's data (max minute = 23:59).
        Resume should skip Apr 8 and start at Apr 9 00:00."""
        view = _chunked_view()
        parsed = parse_view_query(view.query)
        cursor = MockCursorWithStats(
            stats={"processedRows": 50},
            fetchall_responses=[
                # Source spans Apr 8 → Apr 10
                [_files_row("ts", "2026-04-08T10:00:00+00:00",
                                 "2026-04-10T15:00:00+00:00")],
                # Target has minute buckets through Apr 8 23:59
                [_files_row("minute", "2026-04-08T00:00:00+00:00",
                                      "2026-04-08T23:59:00+00:00")],
            ],
        )
        r = await execute_chunked_full_refresh(
            cursor, view, "iceberg.out.mv", parsed,
            value_columns=["volume"],
            chunk_granularity="day",
        )
        # Expect 2 chunks: Apr 9 and Apr 10. Apr 8 already done.
        assert len(r.queries) == 2
        merges = [s for s in cursor.executed if "MERGE INTO" in s]
        assert "ts >= TIMESTAMP '2026-04-09 00:00:00.000000 UTC'" in merges[0]
        assert "ts >= TIMESTAMP '2026-04-10 00:00:00.000000 UTC'" in merges[1]
        # And crucially NOT Apr 8
        assert not any(
            "ts >= TIMESTAMP '2026-04-08 00:00:00.000000 UTC'" in m
            for m in merges
        )

    async def test_should_stop_between_chunks_sets_interrupted(self):
        view = _chunked_view()
        parsed = parse_view_query(view.query)
        cursor = MockCursorWithStats(
            stats={"processedRows": 1},
            fetchall_responses=[
                [_files_row("ts", "2026-04-08T10:00:00+00:00",
                                 "2026-04-10T15:00:00+00:00")],
                [],
            ],
        )
        r = await execute_chunked_full_refresh(
            cursor, view, "iceberg.out.mv", parsed,
            value_columns=["volume"],
            chunk_granularity="day",
            should_stop=lambda: True,  # trip immediately after the first chunk
        )
        assert r.interrupted
        assert len(r.queries) == 1

    async def test_empty_source_returns_empty_result(self):
        view = _chunked_view()
        parsed = parse_view_query(view.query)
        cursor = MockCursorWithStats(
            fetchall_responses=[[]],   # source $files empty
        )
        r = await execute_chunked_full_refresh(
            cursor, view, "iceberg.out.mv", parsed,
            value_columns=["volume"],
            chunk_granularity="day",
        )
        assert not r.interrupted
        assert r.queries == []

    async def test_on_chunk_callback_invoked_per_chunk(self):
        view = _chunked_view()
        parsed = parse_view_query(view.query)
        cursor = MockCursorWithStats(
            stats={"processedRows": 1},
            fetchall_responses=[
                [_files_row("ts", "2026-04-08T10:00:00+00:00",
                                 "2026-04-09T15:00:00+00:00")],
                [],
            ],
        )
        from trino_mv_orchestrator.executor import ChunkProgress
        captured: list[ChunkProgress] = []

        async def _capture(p: ChunkProgress) -> None:
            captured.append(p)

        r = await execute_chunked_full_refresh(
            cursor, view, "iceberg.out.mv", parsed,
            value_columns=["volume"],
            chunk_granularity="day",
            on_chunk=_capture,
        )
        assert len(captured) == len(r.queries) == 2
        # Range bookkeeping — first chunk starts at source min, last chunk ends
        # at source max (bucket-aligned).
        assert captured[0].chunk_range[0] == datetime(2026, 4, 8, tzinfo=timezone.utc)
        assert captured[-1].chunk_range[1] == datetime(2026, 4, 10, tzinfo=timezone.utc)
        # Progress counters are 1-indexed and the total is stable across calls.
        assert [p.chunks_done for p in captured] == [1, 2]
        assert {p.chunks_total for p in captured} == {2}
        # Each payload carries the concrete QueryInfo for that chunk (so the
        # status consumer can surface query_id / duration / rows without
        # inspecting RefreshResult after the fact).
        assert captured[0].query is r.queries[0]
        assert captured[-1].query is r.queries[-1]
        assert captured[0].query.stage == "chunk_merge"

    async def test_on_chunk_callback_reports_total_on_interrupt(self):
        """When ``should_stop`` fires mid-backfill, the last emitted payload
        must still carry the original ``chunks_total`` (so the UI can show
        e.g. ``3/68`` not ``3/3``)."""
        view = _chunked_view()
        parsed = parse_view_query(view.query)
        cursor = MockCursorWithStats(
            stats={"processedRows": 1},
            fetchall_responses=[
                [_files_row("ts", "2026-04-08T10:00:00+00:00",
                                 "2026-04-10T15:00:00+00:00")],
                [],
            ],
        )
        from trino_mv_orchestrator.executor import ChunkProgress
        captured: list[ChunkProgress] = []
        # Stop after the first chunk fires.
        stop_flag = {"tripped": False}

        def _stop() -> bool:
            return stop_flag["tripped"]

        async def _capture(p: ChunkProgress) -> None:
            captured.append(p)
            stop_flag["tripped"] = True

        r = await execute_chunked_full_refresh(
            cursor, view, "iceberg.out.mv", parsed,
            value_columns=["volume"],
            chunk_granularity="day",
            should_stop=_stop,
            on_chunk=_capture,
        )
        assert r.interrupted is True
        assert len(captured) == 1
        # Total reflects what the backfill *would* have done, not what ran.
        assert captured[0].chunks_total == 3
        assert captured[0].chunks_done == 1

    async def test_fully_caught_up_target_emits_no_merges(self):
        """Target has ingested through Apr 10 — same as source max. No chunks
        should be emitted; next tick will observe last_source_snapshot=None
        but there's simply nothing left to do."""
        view = _chunked_view()
        parsed = parse_view_query(view.query)
        cursor = MockCursorWithStats(
            fetchall_responses=[
                [_files_row("ts", "2026-04-08T10:00:00+00:00",
                                 "2026-04-10T15:00:00+00:00")],
                [_files_row("minute", "2026-04-08T00:00:00+00:00",
                                      "2026-04-10T23:59:00+00:00")],
            ],
        )
        r = await execute_chunked_full_refresh(
            cursor, view, "iceberg.out.mv", parsed,
            value_columns=["volume"],
            chunk_granularity="day",
        )
        assert r.queries == []
        assert not r.interrupted
