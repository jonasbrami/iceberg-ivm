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
                max_snapshot=99,
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

    async def test_large_window_with_chunk_emits_one_merge_per_chunk(self):
        # Issue #61: a large catch-up incremental window must be chunked the
        # same way the full path is when full_refresh_chunk is set, so the
        # single giant MERGE that OOMs the join build side is split into N
        # bounded per-chunk MERGEs.
        cursor = MockCursor(stats={"processedRows": 100, "processedBytes": 4096})
        view = make_view(full_refresh_chunk="day")
        parsed = parse_view_query(view.query)
        # 3-day catch-up window, already bucket-aligned by the detector.
        r_start = datetime(2026, 5, 16, tzinfo=UTC)
        r_end = datetime(2026, 5, 19, tzinfo=UTC)
        queries = [
            q
            async for q in execute_refresh(
                cursor,
                view,
                "iceberg.out.mv",
                parsed,
                ["volume"],
                incremental_range=(r_start, r_end),
                max_snapshot=99,
            )
        ]
        assert len(queries) == 3
        assert all(q.stage == "chunk_merge" for q in queries)
        assert [q.chunks_done for q in queries] == [1, 2, 3]
        assert {q.chunks_total for q in queries} == {3}
        for i, q in enumerate(queries):
            assert q.range_start == datetime(2026, 5, 16 + i, tzinfo=UTC)
            assert q.range_end == datetime(2026, 5, 17 + i, tzinfo=UTC)
        assert len({q.query_id for q in queries}) == 3

    async def test_incremental_resume_marker_floors_to_containing_chunk(self):
        # Issue #61/#62 (incremental resume): an interrupted chunked-incremental
        # run leaves an incremental_progress marker. On restart the SAME window
        # is recomputed from the unchanged bookmark; the marker (end of the last
        # committed chunk) is floored to the start of its containing chunk and
        # earlier, already-committed chunks are skipped. Idempotent re-MERGE of
        # the boundary chunk keeps it gap-free.
        cursor = MockCursor(stats={"processedRows": 100, "processedBytes": 4096})
        view = make_view(full_refresh_chunk="day")
        parsed = parse_view_query(view.query)
        # 3-day window; marker says the May 16 chunk committed (ended May 17).
        r_start = datetime(2026, 5, 16, tzinfo=UTC)
        r_end = datetime(2026, 5, 19, tzinfo=UTC)
        queries = [
            q
            async for q in execute_refresh(
                cursor,
                view,
                "iceberg.out.mv",
                parsed,
                ["volume"],
                incremental_range=(r_start, r_end),
                resume_from=datetime(2026, 5, 17, tzinfo=UTC),
                max_snapshot=99,
            )
        ]
        assert len(queries) == 2
        assert all(q.stage == "chunk_merge" for q in queries)
        assert queries[0].range_start == datetime(2026, 5, 17, tzinfo=UTC)
        assert queries[1].range_start == datetime(2026, 5, 18, tzinfo=UTC)

    async def test_incremental_resume_marker_mid_chunk_remerges_containing_chunk(self):
        # Marker lands mid-chunk (interrupted run committed a coarser chunk,
        # then chunk size shrank). Floor to the containing chunk start so the
        # partial chunk is re-MERGEd in full — gap-free.
        cursor = MockCursor(stats={"processedRows": 100})
        view = make_view(full_refresh_chunk="day")
        parsed = parse_view_query(view.query)
        r_start = datetime(2026, 5, 16, tzinfo=UTC)
        r_end = datetime(2026, 5, 19, tzinfo=UTC)
        queries = [
            q
            async for q in execute_refresh(
                cursor,
                view,
                "iceberg.out.mv",
                parsed,
                ["volume"],
                incremental_range=(r_start, r_end),
                resume_from=datetime(2026, 5, 18, 6, tzinfo=UTC),
                max_snapshot=99,
            )
        ]
        assert len(queries) == 1
        assert queries[0].range_start == datetime(2026, 5, 18, tzinfo=UTC)
        assert queries[0].range_end == datetime(2026, 5, 19, tzinfo=UTC)

    async def test_incremental_resume_marker_before_window_start_is_clamped(self):
        # A marker that predates the window's start (e.g. left from a coarser
        # earlier window) must never resume before the window begins — ``max``
        # guards against it, so all chunks run.
        cursor = MockCursor(stats={"processedRows": 100})
        view = make_view(full_refresh_chunk="day")
        parsed = parse_view_query(view.query)
        r_start = datetime(2026, 5, 16, tzinfo=UTC)
        r_end = datetime(2026, 5, 19, tzinfo=UTC)
        queries = [
            q
            async for q in execute_refresh(
                cursor,
                view,
                "iceberg.out.mv",
                parsed,
                ["volume"],
                incremental_range=(r_start, r_end),
                resume_from=datetime(2026, 5, 10, tzinfo=UTC),
                max_snapshot=99,
            )
        ]
        assert len(queries) == 3
        assert queries[0].range_start == datetime(2026, 5, 16, tzinfo=UTC)

    async def test_small_window_with_chunk_stays_single_merge(self):
        # Steady-state incremental: a window that fits in one chunk emits
        # exactly one chunk_merge — no behavior change beyond the stage label.
        cursor = MockCursor(stats={"processedRows": 10})
        view = make_view(full_refresh_chunk="day")
        parsed = parse_view_query(view.query)
        r_start = datetime(2026, 5, 16, tzinfo=UTC)
        r_end = datetime(2026, 5, 17, tzinfo=UTC)
        queries = [
            q
            async for q in execute_refresh(
                cursor,
                view,
                "iceberg.out.mv",
                parsed,
                ["volume"],
                incremental_range=(r_start, r_end),
                max_snapshot=99,
            )
        ]
        assert len(queries) == 1
        assert queries[0].stage == "chunk_merge"
        assert queries[0].range_start == r_start and queries[0].range_end == r_end

    async def test_single_shot_snaps_window_to_view_granularity(self):
        # Regression guard for the unified ranging path: a non-chunked
        # incremental refresh round-trips its window through
        # expand_to_bucket_bounds(lo, hi - 1µs, granularity). To make the snap
        # OBSERVABLE (and load-bearing), feed a window that is NOT aligned to the
        # view's coarse (month) granularity: the single emitted MERGE must be
        # snapped OUTWARD to whole-month bounds. This distinguishes the correct
        # month snap from both a raw passthrough (would keep the unaligned input)
        # and a finer-granularity round-trip (would also keep it).
        cursor = MockCursor(stats={"processedRows": 10})
        view = make_view(
            query="SELECT symbol, date_trunc('month', ts) AS month, sum(qty) AS volume "
            "FROM iceberg.market_data.trades GROUP BY 1, 2",
        )
        parsed = parse_view_query(view.query)
        queries = [
            q
            async for q in execute_refresh(
                cursor,
                view,
                "iceberg.out.mv",
                parsed,
                ["volume"],
                incremental_range=(datetime(2026, 2, 10, tzinfo=UTC), datetime(2026, 4, 15, tzinfo=UTC)),
                max_snapshot=99,
            )
        ]
        assert len(queries) == 1
        assert queries[0].stage == "merge"
        # Feb 10 → floored to Feb 1; Apr 15 → ceiled to May 1 (whole-month grid).
        assert queries[0].range_start == datetime(2026, 2, 1, tzinfo=UTC)
        assert queries[0].range_end == datetime(2026, 5, 1, tzinfo=UTC)

    async def test_resume_marker_floors_to_containing_week_chunk(self):
        # Coarser-chunk-than-granularity resume (granularity=minute, chunk=week),
        # and the grow-the-chunk-mid-run direction: a marker left at a day
        # boundary (from when the chunk was finer) floors to the start of its
        # containing WEEK (Monday), re-MERGEing that week in full — gap-free.
        cursor = MockCursor(stats={"processedRows": 10})
        view = make_view(
            full_refresh_chunk="week",
            query="SELECT symbol, date_trunc('minute', ts) AS minute FROM iceberg.market_data.trades GROUP BY 1, 2",
        )
        parsed = parse_view_query(view.query)
        # Week-aligned window (2026-01-05, -12, -19, -26 are Mondays); marker is a
        # mid-week Wednesday in the second week → floors to Monday 2026-01-12.
        r_start = datetime(2026, 1, 5, tzinfo=UTC)
        r_end = datetime(2026, 1, 26, tzinfo=UTC)
        queries = [
            q
            async for q in execute_refresh(
                cursor,
                view,
                "iceberg.out.mv",
                parsed,
                [],
                incremental_range=(r_start, r_end),
                resume_from=datetime(2026, 1, 14, tzinfo=UTC),
                max_snapshot=99,
            )
        ]
        assert len(queries) == 2
        assert queries[0].range_start == datetime(2026, 1, 12, tzinfo=UTC)
        assert queries[1].range_start == datetime(2026, 1, 19, tzinfo=UTC)
        assert queries[1].range_end == datetime(2026, 1, 26, tzinfo=UTC)

    async def test_resume_marker_at_window_end_emits_nothing(self):
        # Forward-progress invariant: a run resumed after every chunk already
        # committed (marker == window end) emits zero MERGEs. The caller then
        # falls through to clean completion (advance bookmark, clear current_work)
        # rather than looping — so resume always terminates.
        cursor = MockCursor(stats={"processedRows": 10})
        view = make_view(full_refresh_chunk="day")
        parsed = parse_view_query(view.query)
        queries = [
            q
            async for q in execute_refresh(
                cursor,
                view,
                "iceberg.out.mv",
                parsed,
                ["volume"],
                incremental_range=(datetime(2026, 5, 16, tzinfo=UTC), datetime(2026, 5, 19, tzinfo=UTC)),
                resume_from=datetime(2026, 5, 19, tzinfo=UTC),
                max_snapshot=99,
            )
        ]
        assert queries == []


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
                max_snapshot=99,
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
                max_snapshot=99,
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
                max_snapshot=99,
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

    async def test_no_resume_marker_starts_from_source_beginning_despite_target_data(self):
        # CORRECTNESS (issue #62): a forced full refresh with NO committed
        # progress marker (bookmark gone → from-beginning recompute) must
        # start at the SOURCE's beginning, even when the target already holds
        # historical buckets. The old code resumed from max(bucket) in the
        # target and walked forward only, silently skipping older buckets the
        # source may have overwritten. The executor must NOT read the target's
        # $files on this path: with resume_from=None it walks every chunk.
        view = make_view(
            full_refresh_chunk="day",
            query=("SELECT symbol, date_trunc('minute', ts) AS minute FROM iceberg.market_data.trades GROUP BY 1, 2"),
        )
        parsed = parse_view_query(view.query)
        cursor = MockCursor(
            stats={"processedRows": 50},
            fetchall_responses=[
                # source $files: Apr 8 -> Apr 10
                [_files_row("ts", "2026-04-08T10:00:00+00:00", "2026-04-10T15:00:00+00:00")],
                # target already holds buckets through Apr 8 — a previous run's
                # data. Under the bug this would cause resume at Apr 8 and
                # skip nothing-but it also means an overwrite to Apr 8 would be
                # ignored. Bookmark-absent → recompute everything from Apr 8.
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
                resume_from=None,
                max_snapshot=99,
            )
        ]
        # Every day-chunk from the source's start is re-MERGEd from scratch.
        assert len(queries) == 3
        assert queries[0].range_start == datetime(2026, 4, 8, tzinfo=UTC)
        assert queries[1].range_start == datetime(2026, 4, 9, tzinfo=UTC)
        assert queries[2].range_start == datetime(2026, 4, 10, tzinfo=UTC)
        # The target's $files must never be consulted on the full-refresh path:
        # the only metadata query issued is the source $files range read.
        assert sum("$files" in s for s in cursor.executed if s.upper().startswith("SELECT")) == 1
        assert not any("content = 0" in s for s in cursor.executed), (
            "full-refresh path must not read the target's bucket max"
        )

    async def test_resume_from_marker_floors_to_containing_chunk(self):
        # Interruption-resume: a committed-progress marker from THIS
        # from-beginning run lets us skip already-committed chunks. The marker
        # is the end of the last committed chunk; we floor it to the start of
        # the chunk it lands in (idempotent re-MERGE of the boundary chunk) and
        # walk forward.
        view = make_view(
            full_refresh_chunk="day",
            query=("SELECT symbol, date_trunc('minute', ts) AS minute FROM iceberg.market_data.trades GROUP BY 1, 2"),
        )
        parsed = parse_view_query(view.query)
        cursor = MockCursor(
            stats={"processedRows": 50},
            fetchall_responses=[
                # source Apr 8 -> Apr 10
                [_files_row("ts", "2026-04-08T10:00:00+00:00", "2026-04-10T15:00:00+00:00")],
            ],
        )
        # Marker: last committed chunk ended at Apr 9 00:00 (Apr 8 chunk done).
        queries = [
            q
            async for q in execute_refresh(
                cursor,
                view,
                "iceberg.out.mv",
                parsed,
                [],
                resume_from=datetime(2026, 4, 9, tzinfo=UTC),
                max_snapshot=99,
            )
        ]
        assert len(queries) == 2
        assert queries[0].range_start == datetime(2026, 4, 9, tzinfo=UTC)
        assert queries[1].range_start == datetime(2026, 4, 10, tzinfo=UTC)
        # No target $files read even when resuming.
        assert not any("content = 0" in s for s in cursor.executed)

    async def test_resume_marker_mid_chunk_remerges_containing_chunk(self):
        # Marker lands mid-chunk (an interrupted run committed a coarser chunk,
        # then chunk size shrank). Floor to the containing chunk start so the
        # partial chunk is re-MERGEd in full — gap-free.
        view = make_view(
            full_refresh_chunk="day",
            query=("SELECT symbol, date_trunc('minute', ts) AS minute FROM iceberg.market_data.trades GROUP BY 1, 2"),
        )
        parsed = parse_view_query(view.query)
        cursor = MockCursor(
            stats={"processedRows": 50},
            fetchall_responses=[
                # source Apr 8 -> Apr 10 (end = Apr 11)
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
                [],
                resume_from=datetime(2026, 4, 10, 6, tzinfo=UTC),
                max_snapshot=99,
            )
        ]
        assert len(queries) == 1
        assert queries[0].range_start == datetime(2026, 4, 10, tzinfo=UTC)
        assert queries[0].range_end == datetime(2026, 4, 11, tzinfo=UTC)

    async def test_no_group_by_bucket_dropped_from_source_beginning(self):
        # Property: with no resume marker, every GROUP BY bucket in the source
        # appears in exactly one emitted range. This is the no-silent-data-skip
        # invariant — the heart of issue #62.
        view = make_view(full_refresh_chunk="month")  # day buckets, month chunks
        parsed = parse_view_query(view.query)
        cursor = MockCursor(
            stats={"processedRows": 50},
            fetchall_responses=[
                [_files_row("ts", "2026-04-08T10:00:00+00:00", "2026-06-15T15:00:00+00:00")],
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
                resume_from=None,
                max_snapshot=99,
            )
        ]
        source_min_day = datetime(2026, 4, 8, tzinfo=UTC)
        source_max_day = datetime(2026, 6, 15, tzinfo=UTC)
        d = source_min_day
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
            ],
        )
        collected = []
        async for q in execute_refresh(
            cursor,
            view,
            "iceberg.out.mv",
            parsed,
            ["volume"],
            max_snapshot=99,
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
                max_snapshot=99,
            )
        ]
        assert queries == []

    async def test_full_target_with_no_marker_remerges_everything(self):
        # CORRECTNESS (issue #62): even when the target already covers the
        # whole source range, a bookmark-absent full refresh (resume_from=None)
        # re-MERGEs EVERY chunk from the source's beginning. The target's data
        # presence is irrelevant — only the committed-progress marker may skip
        # a chunk, and there is none here. This guarantees a source that
        # overwrote historical buckets is fully recomputed, not skipped.
        view = make_view(
            full_refresh_chunk="day",
            query=("SELECT symbol, date_trunc('minute', ts) AS minute FROM iceberg.market_data.trades GROUP BY 1, 2"),
        )
        parsed = parse_view_query(view.query)
        cursor = MockCursor(
            fetchall_responses=[
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
                [],
                resume_from=None,
                max_snapshot=99,
            )
        ]
        assert len(queries) == 3
        assert queries[0].range_start == datetime(2026, 4, 8, tzinfo=UTC)
        assert queries[-1].range_end == datetime(2026, 4, 11, tzinfo=UTC)


# ── snapshot pinning (no source mixing across chunks) ──


class TestExecuteRefreshPinsSnapshot:
    async def test_all_chunks_pin_the_same_snapshot_for_version_as_of(self):
        # Issue #62, "snapshot mixing": every chunk's MERGE source must read the
        # source FOR VERSION AS OF the SAME pinned snapshot, so a multi-chunk run
        # never blends data from different source commits (commit t-2 with t
        # without t-1). Without pinning the MERGE reads the live source and a
        # long run drifts across snapshots.
        cursor = MockCursor(stats={"processedRows": 10})
        view = make_view(full_refresh_chunk="day")
        parsed = parse_view_query(view.query)
        queries = [
            q
            async for q in execute_refresh(
                cursor,
                view,
                "iceberg.out.mv",
                parsed,
                ["volume"],
                max_snapshot=12345,
                incremental_range=(datetime(2026, 5, 16, tzinfo=UTC), datetime(2026, 5, 19, tzinfo=UTC)),
            )
        ]
        assert len(queries) == 3
        merges = [m for m in cursor.executed if "MERGE INTO" in m]
        assert len(merges) == 3
        # Identical pin on every chunk.
        for m in merges:
            assert "FOR VERSION AS OF 12345" in m


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
