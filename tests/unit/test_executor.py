"""Tests for the refresh executor."""

from datetime import UTC, datetime, timedelta

from iceberg_ivm.config import ViewConfig
from iceberg_ivm.executor import (
    build_merge_sql,
    execute_maintenance,
    execute_refresh,
)
from iceberg_ivm.query_parser import parse_view_query


def make_view(**overrides) -> ViewConfig:
    defaults = {
        "name": "ohlcv_1m",
        "query": (
            "SELECT symbol, date_trunc('day', ts) AS day, sum(qty) AS volume "
            "FROM iceberg.market_data.trades "
            "GROUP BY 1, 2"
        ),
        "target_table": "iceberg.analytics.ohlcv_1m",
    }
    defaults.update(overrides)
    return ViewConfig(**defaults)


class MockCursor:
    def __init__(self, stats: dict | None = None, fetchall_responses: list[list] | None = None):
        self._stats = stats or {}
        self.executed: list[str] = []
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
        return self._fetchall_responses.pop(0) if self._fetchall_responses else []


def _files_row(column: str, lower: str, upper: str) -> tuple:
    return ({column: {"lower_bound": lower, "upper_bound": upper}},)


# ── build_merge_sql ──


class TestBuildMergeSql:
    def test_structure(self):
        sql = build_merge_sql(
            "iceberg.out.mv",
            "SELECT a FROM t WHERE ts >= X AND ts < Y GROUP BY 1",
            merge_keys=("symbol", "day"),
            value_columns=["volume"],
        )
        assert "MERGE INTO iceberg.out.mv AS t" in sql
        assert "ON t.symbol = s.symbol AND t.day = s.day" in sql
        assert "WHEN MATCHED THEN UPDATE SET volume = s.volume" in sql
        assert "WHEN NOT MATCHED THEN INSERT" in sql
        assert "ts >= X AND ts < Y" in sql


# ── execute_refresh: incremental path ──


class TestExecuteRefreshIncremental:
    async def test_emits_one_merge_over_range(self):
        cursor = MockCursor(stats={"processedRows": 200, "processedBytes": 8192})
        view = make_view()
        parsed = parse_view_query(view.query)
        r_start = datetime(2026, 4, 8, tzinfo=UTC)
        r_end = datetime(2026, 4, 9, tzinfo=UTC)
        queries = [
            q
            async for q in execute_refresh(
                cursor,
                view,
                "iceberg.out.mv",
                parsed,
                ["volume"],
                incremental_range=(r_start, r_end),
            )
        ]
        assert len(queries) == 1
        q = queries[0]
        assert q.stage == "merge"
        assert q.processed_rows == 200 and q.processed_bytes == 8192
        assert q.range_start == r_start and q.range_end == r_end
        assert q.chunks_done == 1 and q.chunks_total == 1
        merge = cursor.executed[-1]
        assert "ts >= TIMESTAMP '2026-04-08 00:00:00.000000 UTC'" in merge
        assert "ts < TIMESTAMP '2026-04-09 00:00:00.000000 UTC'" in merge
        assert "MERGE INTO iceberg.out.mv" in merge


# ── execute_refresh: single-shot full refresh (no chunk) ──


class TestExecuteRefreshSingleShotFull:
    async def test_one_merge_over_snapped_source_range(self):
        view = make_view()  # full_refresh_chunk = None, granularity = day
        parsed = parse_view_query(view.query)
        cursor = MockCursor(
            stats={"processedRows": 1000},
            fetchall_responses=[
                # source $files: Apr 8 10:00 → Apr 10 15:00
                [_files_row("ts", "2026-04-08T10:00:00+00:00", "2026-04-10T15:00:00+00:00")],
            ],
        )
        queries = [
            q
            async for q in execute_refresh(
                cursor,
                view,
                "iceberg.out.mv",
                parsed,
                ["volume"],
            )
        ]
        assert len(queries) == 1
        q = queries[0]
        assert q.stage == "merge"
        # Snapped to day boundaries (view's own granularity)
        assert q.range_start == datetime(2026, 4, 8, tzinfo=UTC)
        assert q.range_end == datetime(2026, 4, 11, tzinfo=UTC)
        merge = cursor.executed[-1]
        assert "ts >= TIMESTAMP '2026-04-08 00:00:00.000000 UTC'" in merge
        assert "ts < TIMESTAMP '2026-04-11 00:00:00.000000 UTC'" in merge

    async def test_empty_source_emits_nothing(self):
        view = make_view()
        parsed = parse_view_query(view.query)
        cursor = MockCursor(fetchall_responses=[[]])  # empty $files
        queries = [
            q
            async for q in execute_refresh(
                cursor,
                view,
                "iceberg.out.mv",
                parsed,
                ["volume"],
            )
        ]
        assert queries == []


# ── execute_refresh: chunked full refresh ──


class TestExecuteRefreshChunked:
    async def test_emits_one_merge_per_day_chunk(self):
        view = make_view(full_refresh_chunk="day")
        parsed = parse_view_query(view.query)
        cursor = MockCursor(
            stats={"processedRows": 100, "processedBytes": 4096},
            fetchall_responses=[
                # source $files: Apr 8 10:00 → Apr 10 15:00
                [_files_row("ts", "2026-04-08T10:00:00+00:00", "2026-04-10T15:00:00+00:00")],
                # target $files: empty
                [],
            ],
        )
        queries = [
            q
            async for q in execute_refresh(
                cursor,
                view,
                "iceberg.out.mv",
                parsed,
                ["volume"],
            )
        ]
        assert len(queries) == 3
        assert all(q.stage == "chunk_merge" for q in queries)
        assert [q.chunks_done for q in queries] == [1, 2, 3]
        assert {q.chunks_total for q in queries} == {3}
        # Ranges are contiguous, bucket-aligned, ordered.
        for i, q in enumerate(queries):
            assert q.range_start == datetime(2026, 4, 8 + i, tzinfo=UTC)
            assert q.range_end == datetime(2026, 4, 9 + i, tzinfo=UTC)
        # Distinct query_ids captured.
        assert len({q.query_id for q in queries}) == 3

    async def test_resume_from_target_bucket_max(self):
        view = make_view(
            full_refresh_chunk="day",
            query=("SELECT symbol, date_trunc('minute', ts) AS minute FROM iceberg.market_data.trades GROUP BY 1, 2"),
        )
        parsed = parse_view_query(view.query)
        cursor = MockCursor(
            stats={"processedRows": 50},
            fetchall_responses=[
                [_files_row("ts", "2026-04-08T10:00:00+00:00", "2026-04-10T15:00:00+00:00")],
                # target has minute buckets through Apr 8 23:59
                [_files_row("minute", "2026-04-08T00:00:00+00:00", "2026-04-08T23:59:00+00:00")],
            ],
        )
        queries = [
            q
            async for q in execute_refresh(
                cursor,
                view,
                "iceberg.out.mv",
                parsed,
                [],
            )
        ]
        # The day-chunk containing target_max (Apr 8) is re-MERGEd in full —
        # this is the documented "always full chunks" cost: every restart
        # re-aggregates the chunk overlapping target_max. Idempotent because
        # the aggregation is deterministic over an append-only source.
        assert len(queries) == 3
        assert queries[0].range_start == datetime(2026, 4, 8, tzinfo=UTC)
        assert queries[1].range_start == datetime(2026, 4, 9, tzinfo=UTC)
        assert queries[2].range_start == datetime(2026, 4, 10, tzinfo=UTC)

    async def test_chunk_size_increase_mid_backfill_remerges_partial_chunk(self):
        # Operator changed full_refresh_chunk from "day" to "month" mid-backfill.
        # target_max = Apr 15 (last committed day bucket). The April chunk
        # contains uncommitted days (16-30) — under the old "snap to next
        # chunk" math those days were silently dropped. Under floor-to-
        # containing-chunk the entire April chunk is re-MERGEd in full at
        # the new size, then May and June continue as full chunks.
        view = make_view(full_refresh_chunk="month")
        parsed = parse_view_query(view.query)
        cursor = MockCursor(
            stats={"processedRows": 50},
            fetchall_responses=[
                # source Apr 8 -> Jun 15
                [_files_row("ts", "2026-04-08T10:00:00+00:00", "2026-06-15T15:00:00+00:00")],
                # target day buckets through Apr 15 (was chunking by day pre-switch)
                [_files_row("day", "2026-04-08T00:00:00+00:00", "2026-04-15T00:00:00+00:00")],
            ],
        )
        queries = [
            q
            async for q in execute_refresh(
                cursor,
                view,
                "iceberg.out.mv",
                parsed,
                ["volume"],
            )
        ]
        assert len(queries) == 3
        assert queries[0].range_start == datetime(2026, 4, 1, tzinfo=UTC)
        assert queries[0].range_end == datetime(2026, 5, 1, tzinfo=UTC)
        assert queries[1].range_start == datetime(2026, 5, 1, tzinfo=UTC)
        assert queries[1].range_end == datetime(2026, 6, 1, tzinfo=UTC)
        assert queries[2].range_start == datetime(2026, 6, 1, tzinfo=UTC)
        assert queries[2].range_end == datetime(2026, 7, 1, tzinfo=UTC)

    async def test_chunk_size_decrease_mid_backfill_remerges_partial_chunk(self):
        # Operator changed full_refresh_chunk from "month" to "day" mid-backfill.
        # target_max sits mid-day (minute buckets through Apr 30 12:00 — a
        # monthly chunk was interrupted partway). The Apr 30 day-chunk is
        # re-MERGEd in full, then May 1/2/3 continue as full day chunks.
        view = make_view(
            full_refresh_chunk="day",
            query=("SELECT symbol, date_trunc('minute', ts) AS minute FROM iceberg.market_data.trades GROUP BY 1, 2"),
        )
        parsed = parse_view_query(view.query)
        cursor = MockCursor(
            stats={"processedRows": 50},
            fetchall_responses=[
                # source Apr 8 -> May 3
                [_files_row("ts", "2026-04-08T10:00:00+00:00", "2026-05-03T15:00:00+00:00")],
                # target minute buckets through Apr 30 12:00 (interrupted mid-month)
                [_files_row("minute", "2026-04-08T00:00:00+00:00", "2026-04-30T12:00:00+00:00")],
            ],
        )
        queries = [
            q
            async for q in execute_refresh(
                cursor,
                view,
                "iceberg.out.mv",
                parsed,
                [],
            )
        ]
        assert len(queries) == 4
        assert queries[0].range_start == datetime(2026, 4, 30, tzinfo=UTC)
        assert queries[0].range_end == datetime(2026, 5, 1, tzinfo=UTC)
        assert queries[1].range_start == datetime(2026, 5, 1, tzinfo=UTC)
        assert queries[2].range_start == datetime(2026, 5, 2, tzinfo=UTC)
        assert queries[3].range_start == datetime(2026, 5, 3, tzinfo=UTC)
        assert queries[3].range_end == datetime(2026, 5, 4, tzinfo=UTC)

    async def test_target_max_exactly_on_chunk_boundary_remerges_that_chunk(self):
        # Edge case: target_max landing exactly on a chunk boundary still
        # re-MERGEs that chunk (floor of a boundary is itself). Locks the
        # "always full chunks" behavior at the boundary case.
        view = make_view(full_refresh_chunk="day")
        parsed = parse_view_query(view.query)
        cursor = MockCursor(
            stats={"processedRows": 1},
            fetchall_responses=[
                [_files_row("ts", "2026-04-08T10:00:00+00:00", "2026-04-10T15:00:00+00:00")],
                # target has one day-bucket: Apr 8 (group-by is day). target_max
                # = Apr 8 00:00, exactly the start of the Apr 8 chunk.
                [_files_row("day", "2026-04-08T00:00:00+00:00", "2026-04-08T00:00:00+00:00")],
            ],
        )
        queries = [
            q
            async for q in execute_refresh(
                cursor,
                view,
                "iceberg.out.mv",
                parsed,
                ["volume"],
            )
        ]
        assert len(queries) == 3
        assert queries[0].range_start == datetime(2026, 4, 8, tzinfo=UTC)
        assert queries[0].range_end == datetime(2026, 4, 9, tzinfo=UTC)

    async def test_target_max_in_last_chunk_emits_that_chunk(self):
        # target_max sits inside the last source-aligned chunk. The fix
        # ensures we still emit that chunk; the old code would have
        # snapped past `end` and emitted nothing, silently dropping the
        # remaining buckets in that chunk.
        view = make_view(
            full_refresh_chunk="day",
            query=("SELECT symbol, date_trunc('minute', ts) AS minute FROM iceberg.market_data.trades GROUP BY 1, 2"),
        )
        parsed = parse_view_query(view.query)
        cursor = MockCursor(
            stats={"processedRows": 1},
            fetchall_responses=[
                # source Apr 8 -> Apr 10 (so end = Apr 11)
                [_files_row("ts", "2026-04-08T10:00:00+00:00", "2026-04-10T15:00:00+00:00")],
                # target_max = Apr 10 06:00 (mid-day in the last source chunk)
                [_files_row("minute", "2026-04-08T00:00:00+00:00", "2026-04-10T06:00:00+00:00")],
            ],
        )
        queries = [
            q
            async for q in execute_refresh(
                cursor,
                view,
                "iceberg.out.mv",
                parsed,
                [],
            )
        ]
        assert len(queries) == 1
        assert queries[0].range_start == datetime(2026, 4, 10, tzinfo=UTC)
        assert queries[0].range_end == datetime(2026, 4, 11, tzinfo=UTC)

    async def test_no_group_by_bucket_dropped_when_coarsening_chunk_size(self):
        # Property: every GROUP BY bucket between target_max and source_max
        # appears in exactly one emitted range. This is the no-silent-data-
        # skip invariant. Asserts coverage rather than a specific chunk list
        # so a future refactor that produces a different-but-equivalent
        # chunk plan still passes — only a regression that drops buckets
        # fails.
        view = make_view(full_refresh_chunk="month")  # day -> month coarsen
        parsed = parse_view_query(view.query)
        cursor = MockCursor(
            stats={"processedRows": 50},
            fetchall_responses=[
                [_files_row("ts", "2026-04-08T10:00:00+00:00", "2026-06-15T15:00:00+00:00")],
                # target day-chunked through Apr 15 (so target_max = Apr 15,
                # first uncommitted day = Apr 16)
                [_files_row("day", "2026-04-08T00:00:00+00:00", "2026-04-15T00:00:00+00:00")],
            ],
        )
        queries = [
            q
            async for q in execute_refresh(
                cursor,
                view,
                "iceberg.out.mv",
                parsed,
                ["volume"],
            )
        ]
        first_uncommitted_day = datetime(2026, 4, 16, tzinfo=UTC)
        source_max_day = datetime(2026, 6, 15, tzinfo=UTC)
        d = first_uncommitted_day
        while d <= source_max_day:
            covered = sum(1 for q in queries if q.range_start <= d < q.range_end)
            assert covered == 1, f"day {d.date()} covered by {covered} ranges (expected 1)"
            d += timedelta(days=1)

    async def test_caller_can_break_early(self):
        """The whole point of the async generator: caller cancels by ``break``."""
        view = make_view(full_refresh_chunk="day")
        parsed = parse_view_query(view.query)
        cursor = MockCursor(
            stats={"processedRows": 1},
            fetchall_responses=[
                [_files_row("ts", "2026-04-08T10:00:00+00:00", "2026-04-10T15:00:00+00:00")],
                [],
            ],
        )
        collected = []
        async for q in execute_refresh(
            cursor,
            view,
            "iceberg.out.mv",
            parsed,
            ["volume"],
        ):
            collected.append(q)
            break  # stop after the first chunk commits
        assert len(collected) == 1
        assert collected[0].chunks_done == 1
        assert collected[0].chunks_total == 3  # "still 3 planned, we did 1"

    async def test_empty_source_emits_nothing(self):
        view = make_view(full_refresh_chunk="day")
        parsed = parse_view_query(view.query)
        cursor = MockCursor(fetchall_responses=[[]])
        queries = [
            q
            async for q in execute_refresh(
                cursor,
                view,
                "iceberg.out.mv",
                parsed,
                ["volume"],
            )
        ]
        assert queries == []

    async def test_fully_caught_up_target_remerges_last_chunk(self):
        # Even when the target covers the whole source range, the chunk
        # containing target_max gets re-MERGEd in full on the next tick.
        # This is the documented "always full chunks" cost (see
        # test_resume_from_target_bucket_max). Cheap for typical chunk
        # sizes, and the alternative — branching on "is the containing
        # chunk already complete?" — adds complexity without changing
        # correctness.
        view = make_view(
            full_refresh_chunk="day",
            query=("SELECT symbol, date_trunc('minute', ts) AS minute FROM iceberg.market_data.trades GROUP BY 1, 2"),
        )
        parsed = parse_view_query(view.query)
        cursor = MockCursor(
            fetchall_responses=[
                [_files_row("ts", "2026-04-08T10:00:00+00:00", "2026-04-10T15:00:00+00:00")],
                # target covers the whole source range
                [_files_row("minute", "2026-04-08T00:00:00+00:00", "2026-04-10T23:59:00+00:00")],
            ],
        )
        queries = [
            q
            async for q in execute_refresh(
                cursor,
                view,
                "iceberg.out.mv",
                parsed,
                [],
            )
        ]
        assert len(queries) == 1
        assert queries[0].range_start == datetime(2026, 4, 10, tzinfo=UTC)
        assert queries[0].range_end == datetime(2026, 4, 11, tzinfo=UTC)


# ── execute_maintenance ──


class TestExecuteMaintenance:
    async def test_optimize_without_params(self):
        cursor = MockCursor(stats={"processedRows": 0})
        q = await execute_maintenance(cursor, "iceberg.out.mv", "optimize", {})
        assert cursor.executed == ["ALTER TABLE iceberg.out.mv EXECUTE optimize"]
        assert q.stage == "maintenance_optimize"
        assert q.query_id

    async def test_optimize_with_file_size_threshold(self):
        cursor = MockCursor()
        await execute_maintenance(
            cursor,
            "iceberg.out.mv",
            "optimize",
            {"file_size_threshold": "128MB"},
        )
        assert cursor.executed == [
            "ALTER TABLE iceberg.out.mv EXECUTE optimize(file_size_threshold => '128MB')",
        ]

    async def test_expire_snapshots_with_retention(self):
        cursor = MockCursor()
        q = await execute_maintenance(
            cursor,
            "iceberg.out.mv",
            "expire_snapshots",
            {"retention_threshold": "7d"},
        )
        assert cursor.executed == [
            "ALTER TABLE iceberg.out.mv EXECUTE expire_snapshots(retention_threshold => '7d')",
        ]
        assert q.stage == "maintenance_expire_snapshots"

    async def test_remove_orphan_files_with_retention(self):
        cursor = MockCursor()
        q = await execute_maintenance(
            cursor,
            "iceberg.out.mv",
            "remove_orphan_files",
            {"retention_threshold": "30d"},
        )
        assert cursor.executed == [
            "ALTER TABLE iceberg.out.mv EXECUTE remove_orphan_files(retention_threshold => '30d')",
        ]
        assert q.stage == "maintenance_remove_orphan_files"
