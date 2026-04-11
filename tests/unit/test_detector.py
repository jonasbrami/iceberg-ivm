"""Tests for the change detector."""
from datetime import datetime, timedelta, timezone

from trino_mv_orchestrator.detector import (
    ExpiredSnapshotError,
    MissingFilterColumnError,
    RefreshAction,
    UnexpectedOperationError,
    _parse_ts,
    detect_changes,
    get_current_snapshot,
    get_new_files_column_range,
    get_snapshots_since,
    snap_range,
)


class MockCursor:
    def __init__(self, results: list[list[tuple]]):
        self._results = results
        self._idx = 0
        self._rows = []
        self.executed_sql: list[str] = []

    async def execute(self, sql: str):
        self.executed_sql.append(sql)
        if self._idx < len(self._results):
            self._rows = list(self._results[self._idx])
        else:
            self._rows = []
        self._idx += 1

    async def fetchone(self):
        return self._rows[0] if self._rows else None

    async def fetchall(self):
        return self._rows


# ── snap_range ──

class TestSnapRange:
    def test_minute(self):
        ts = datetime(2026, 4, 8, 10, 30, 45, 123456, tzinfo=timezone.utc)
        s, e = snap_range(ts, ts, "minute")
        assert s == datetime(2026, 4, 8, 10, 30, 0, 0, tzinfo=timezone.utc)
        assert e == datetime(2026, 4, 8, 10, 31, 0, 0, tzinfo=timezone.utc)

    def test_hour(self):
        s, e = snap_range(
            datetime(2026, 4, 8, 10, 30, tzinfo=timezone.utc),
            datetime(2026, 4, 8, 11, 45, tzinfo=timezone.utc),
            "hour",
        )
        assert s == datetime(2026, 4, 8, 10, 0, tzinfo=timezone.utc)
        assert e == datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)

    def test_day(self):
        s, e = snap_range(
            datetime(2026, 4, 8, 10, 0, tzinfo=timezone.utc),
            datetime(2026, 4, 9, 15, 0, tzinfo=timezone.utc),
            "day",
        )
        assert s == datetime(2026, 4, 8, tzinfo=timezone.utc)
        assert e == datetime(2026, 4, 10, tzinfo=timezone.utc)

    def test_week(self):
        # 2026-04-08 is a Wednesday
        s, e = snap_range(
            datetime(2026, 4, 8, 10, 0, tzinfo=timezone.utc),
            datetime(2026, 4, 8, 15, 0, tzinfo=timezone.utc),
            "week",
        )
        # Monday of that week = April 6
        assert s == datetime(2026, 4, 6, tzinfo=timezone.utc)
        # Next Monday = April 13
        assert e == datetime(2026, 4, 13, tzinfo=timezone.utc)

    def test_week_spanning_two_weeks(self):
        s, e = snap_range(
            datetime(2026, 4, 8, tzinfo=timezone.utc),   # Wed week 1
            datetime(2026, 4, 15, tzinfo=timezone.utc),   # Wed week 2
            "week",
        )
        assert s == datetime(2026, 4, 6, tzinfo=timezone.utc)   # Mon week 1
        assert e == datetime(2026, 4, 20, tzinfo=timezone.utc)  # Mon week 3

    def test_month(self):
        s, e = snap_range(
            datetime(2026, 4, 15, tzinfo=timezone.utc),
            datetime(2026, 4, 20, tzinfo=timezone.utc),
            "month",
        )
        assert s == datetime(2026, 4, 1, tzinfo=timezone.utc)
        assert e == datetime(2026, 5, 1, tzinfo=timezone.utc)

    def test_month_spanning_year_boundary(self):
        s, e = snap_range(
            datetime(2026, 12, 15, tzinfo=timezone.utc),
            datetime(2026, 12, 25, tzinfo=timezone.utc),
            "month",
        )
        assert s == datetime(2026, 12, 1, tzinfo=timezone.utc)
        assert e == datetime(2027, 1, 1, tzinfo=timezone.utc)

    def test_quarter(self):
        # Apr 15 is in Q2 (Apr-Jun)
        s, e = snap_range(
            datetime(2026, 4, 15, tzinfo=timezone.utc),
            datetime(2026, 5, 20, tzinfo=timezone.utc),
            "quarter",
        )
        assert s == datetime(2026, 4, 1, tzinfo=timezone.utc)
        assert e == datetime(2026, 7, 1, tzinfo=timezone.utc)

    def test_quarter_spanning_two(self):
        # Mar (Q1) to May (Q2)
        s, e = snap_range(
            datetime(2026, 3, 15, tzinfo=timezone.utc),
            datetime(2026, 5, 10, tzinfo=timezone.utc),
            "quarter",
        )
        assert s == datetime(2026, 1, 1, tzinfo=timezone.utc)   # Q1 start
        assert e == datetime(2026, 7, 1, tzinfo=timezone.utc)    # Q3 start (next after Q2)

    def test_quarter_year_boundary(self):
        # Q4 (Oct-Dec) spanning into year boundary
        s, e = snap_range(
            datetime(2026, 11, 1, tzinfo=timezone.utc),
            datetime(2026, 12, 15, tzinfo=timezone.utc),
            "quarter",
        )
        assert s == datetime(2026, 10, 1, tzinfo=timezone.utc)  # Q4 start
        assert e == datetime(2027, 1, 1, tzinfo=timezone.utc)    # Q1 next year

    def test_year(self):
        s, e = snap_range(
            datetime(2026, 6, 15, tzinfo=timezone.utc),
            datetime(2026, 9, 20, tzinfo=timezone.utc),
            "year",
        )
        assert s == datetime(2026, 1, 1, tzinfo=timezone.utc)
        assert e == datetime(2027, 1, 1, tzinfo=timezone.utc)

    def test_year_spanning(self):
        # Dec 2026 to Jan 2027
        s, e = snap_range(
            datetime(2026, 12, 15, tzinfo=timezone.utc),
            datetime(2027, 1, 10, tzinfo=timezone.utc),
            "year",
        )
        assert s == datetime(2026, 1, 1, tzinfo=timezone.utc)
        assert e == datetime(2028, 1, 1, tzinfo=timezone.utc)


# ── snap_range is the inverse of date_trunc ──

def _py_date_trunc(granularity: str, ts: datetime) -> datetime:
    """Python equivalent of Trino's date_trunc — the forward function."""
    if granularity == "minute":
        return ts.replace(second=0, microsecond=0)
    elif granularity == "hour":
        return ts.replace(minute=0, second=0, microsecond=0)
    elif granularity == "day":
        return ts.replace(hour=0, minute=0, second=0, microsecond=0)
    elif granularity == "week":
        return (ts - timedelta(days=ts.weekday())).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
    elif granularity == "month":
        return ts.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    elif granularity == "quarter":
        q = ((ts.month - 1) // 3) * 3 + 1
        return ts.replace(month=q, day=1, hour=0, minute=0, second=0, microsecond=0)
    elif granularity == "year":
        return ts.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    raise ValueError(granularity)


# Sample timestamps designed to hit edge cases: mid-bucket, bucket boundaries,
# year/quarter/month/week transitions.
_SAMPLE_PAIRS = [
    # same point
    (datetime(2026, 4, 8, 10, 30, 45, 123456, tzinfo=timezone.utc),
     datetime(2026, 4, 8, 10, 30, 45, 123456, tzinfo=timezone.utc)),
    # within same day
    (datetime(2026, 4, 8, 9, 15, tzinfo=timezone.utc),
     datetime(2026, 4, 8, 16, 45, tzinfo=timezone.utc)),
    # spanning days
    (datetime(2026, 4, 8, 23, 59, 59, tzinfo=timezone.utc),
     datetime(2026, 4, 9, 0, 0, 1, tzinfo=timezone.utc)),
    # spanning weeks (Wed to next Wed)
    (datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc),
     datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)),
    # spanning months
    (datetime(2026, 3, 28, tzinfo=timezone.utc),
     datetime(2026, 4, 5, tzinfo=timezone.utc)),
    # spanning quarters (Q1 → Q2)
    (datetime(2026, 3, 15, tzinfo=timezone.utc),
     datetime(2026, 5, 10, tzinfo=timezone.utc)),
    # spanning year boundary
    (datetime(2026, 12, 31, 23, 59, tzinfo=timezone.utc),
     datetime(2027, 1, 1, 0, 1, tzinfo=timezone.utc)),
    # Q4 year boundary
    (datetime(2026, 11, 15, tzinfo=timezone.utc),
     datetime(2026, 12, 20, tzinfo=timezone.utc)),
    # exact bucket boundary (minute)
    (datetime(2026, 4, 8, 10, 0, 0, 0, tzinfo=timezone.utc),
     datetime(2026, 4, 8, 11, 0, 0, 0, tzinfo=timezone.utc)),
]

_GRANULARITIES = ["minute", "hour", "day", "week", "month", "quarter", "year"]


import pytest


class TestSnapRangeInversesDateTrunc:
    """Verify the invariant: snap_range is the exact inverse of date_trunc.

    For every granularity and every sample timestamp pair:
    1. start and end are bucket boundaries (date_trunc is idempotent on them)
    2. The bucket containing min_ts starts at or after start
    3. The bucket containing max_ts starts before end
    4. No bucket is partially covered — start and end ARE bucket boundaries
    """

    @pytest.mark.parametrize("granularity", _GRANULARITIES)
    @pytest.mark.parametrize("min_ts, max_ts", _SAMPLE_PAIRS)
    def test_boundaries_are_bucket_aligned(self, granularity, min_ts, max_ts):
        start, end = snap_range(min_ts, max_ts, granularity)

        # start is a bucket boundary: date_trunc(start) == start
        assert _py_date_trunc(granularity, start) == start, (
            f"start {start} is not a {granularity} boundary"
        )
        # end is a bucket boundary: date_trunc(end) == end
        assert _py_date_trunc(granularity, end) == end, (
            f"end {end} is not a {granularity} boundary"
        )

    @pytest.mark.parametrize("granularity", _GRANULARITIES)
    @pytest.mark.parametrize("min_ts, max_ts", _SAMPLE_PAIRS)
    def test_touched_buckets_are_fully_covered(self, granularity, min_ts, max_ts):
        start, end = snap_range(min_ts, max_ts, granularity)

        # The bucket containing min_ts is within [start, end)
        bucket_min = _py_date_trunc(granularity, min_ts)
        assert bucket_min >= start, (
            f"min_ts bucket {bucket_min} is before start {start}"
        )
        assert bucket_min < end, (
            f"min_ts bucket {bucket_min} is at or after end {end}"
        )

        # The bucket containing max_ts is within [start, end)
        bucket_max = _py_date_trunc(granularity, max_ts)
        assert bucket_max >= start, (
            f"max_ts bucket {bucket_max} is before start {start}"
        )
        assert bucket_max < end, (
            f"max_ts bucket {bucket_max} is at or after end {end}"
        )

    @pytest.mark.parametrize("granularity", _GRANULARITIES)
    @pytest.mark.parametrize("min_ts, max_ts", _SAMPLE_PAIRS)
    def test_range_is_tight(self, granularity, min_ts, max_ts):
        """start is the earliest bucket boundary that covers min_ts."""
        start, end = snap_range(min_ts, max_ts, granularity)

        # start == date_trunc(min_ts): the range starts exactly at the
        # bucket containing min_ts, not one bucket earlier
        assert start == _py_date_trunc(granularity, min_ts), (
            f"start {start} is not tight — should be {_py_date_trunc(granularity, min_ts)}"
        )


# ── _parse_ts ──

class TestParseTs:
    def test_iso_with_tz(self):
        dt = _parse_ts("2026-04-08T10:30:45.123456+00:00")
        assert dt.year == 2026 and dt.month == 4 and dt.day == 8

    def test_iso_no_tz(self):
        dt = _parse_ts("2026-04-08T10:30:45.123456")
        assert dt.hour == 10 and dt.minute == 30

    def test_raises_on_unparseable(self):
        """Unrecognized formats must raise, not fall back to date-only.

        The old date-only fallback could silently shift a snapped range
        by up to 24 hours (floor to midnight UTC), corrupting incremental
        recomputation ranges.
        """
        with pytest.raises(ValueError, match="unparseable"):
            _parse_ts("not-a-timestamp")

    def test_raises_on_nanosecond_precision(self):
        """9-digit fractional seconds exceed strptime's %f (6-digit max).

        Today this silently falls back to date-only, losing the time
        component. After the fix we raise rather than produce a wrong
        range.
        """
        with pytest.raises(ValueError):
            _parse_ts("2026-04-08T10:30:45.123456789+00:00")


# ── get_current_snapshot ──

class TestGetCurrentSnapshot:
    async def test_returns_id(self):
        assert await get_current_snapshot(MockCursor([[(12345,)]]), "db.t") == 12345

    async def test_returns_none(self):
        assert await get_current_snapshot(MockCursor([[]]), "db.t") is None


# ── get_snapshots_since ──

class TestGetSnapshotsSince:
    async def test_raises_on_missing_last_snap(self):
        """If last_snap is no longer in $snapshots (Iceberg expired it),
        the orchestrator must fail loudly rather than silently return []."""
        # First execute is the committed_at lookup for last_snap: empty.
        cursor = MockCursor([[]])
        with pytest.raises(ExpiredSnapshotError):
            await get_snapshots_since(cursor, "db.t", last_snap=999)

    async def test_sql_uses_snapshot_id_tiebreak(self):
        """The committed_at > (...) comparison must tiebreak with snapshot_id,
        otherwise sibling snapshots sharing a millisecond-precision
        committed_at with last_snap would be dropped.

        We assert the shape of the generated SQL (two-query split, with the
        second query referencing snapshot_id in its predicate).
        """
        cursor = MockCursor([
            [(1_700_000_000_000,)],           # committed_at of last_snap
            [(200, "append"), (300, "append")],
        ])
        await get_snapshots_since(cursor, "db.t", last_snap=100)
        assert len(cursor.executed_sql) == 2, (
            "expected two queries (committed_at lookup + later snapshots), "
            f"got {cursor.executed_sql}"
        )
        second = cursor.executed_sql[1]
        assert "snapshot_id" in second
        assert "100" in second  # last_snap appears in the tiebreak clause


# ── get_new_files_column_range ──

class TestGetNewFilesColumnRange:
    async def test_computes_range(self):
        cursor = MockCursor([[
            ({"ts": {"lower_bound": "2026-04-08T09:00:00+00:00", "upper_bound": "2026-04-08T12:00:00+00:00"}},),
            ({"ts": {"lower_bound": "2026-04-08T11:00:00+00:00", "upper_bound": "2026-04-08T15:00:00+00:00"}},),
        ]])
        result = await get_new_files_column_range(cursor, "db.t", [100, 200], "ts")
        assert result is not None
        lo, hi = result
        assert lo == datetime(2026, 4, 8, 9, 0, tzinfo=timezone.utc)
        assert hi == datetime(2026, 4, 8, 15, 0, tzinfo=timezone.utc)

    async def test_no_data_files(self):
        cursor = MockCursor([[]])
        assert await get_new_files_column_range(cursor, "db.t", [100], "ts") is None

    async def test_raises_when_filter_column_absent_from_metrics(self):
        """File rows exist, but none contain the filter_column in their
        per-column metrics. That's a configuration error (typo'd column
        name, schema drift) — must fail loudly instead of silently
        returning None and freezing the view.
        """
        cursor = MockCursor([[
            ({"other_col": {"lower_bound": "1", "upper_bound": "2"}},),
            ({"other_col": {"lower_bound": "3", "upper_bound": "4"}},),
        ]])
        with pytest.raises(MissingFilterColumnError):
            await get_new_files_column_range(cursor, "db.t", [1], "ts")

    async def test_min_max_ignores_lex_order(self):
        """Two files whose chronological order disagrees with lex order.

        The first row's lower_bound is "2026-04-08T09:00:00.000000+00:00".
        The second row's lower_bound is "2026-04-08T08:00:00.000000-01:00",
        which is the SAME instant (09:00 UTC). Its upper_bound is
        "2026-04-08T09:30:00.000000-01:00" = 10:30 UTC, which is LATER
        chronologically than the first row's upper_bound (10:00 UTC) —
        but LEXICOGRAPHICALLY its string is EARLIER because "08..." < "09...".

        After the fix, get_new_files_column_range returns datetimes and
        computes min/max on the instants, not the strings.
        """
        cursor = MockCursor([[
            ({"ts": {"lower_bound": "2026-04-08T09:00:00.000000+00:00",
                     "upper_bound": "2026-04-08T10:00:00.000000+00:00"}},),
            ({"ts": {"lower_bound": "2026-04-08T08:00:00.000000-01:00",
                     "upper_bound": "2026-04-08T09:30:00.000000-01:00"}},),
        ]])
        result = await get_new_files_column_range(cursor, "db.t", [1, 2], "ts")
        assert result is not None
        lo, hi = result
        assert lo == datetime(2026, 4, 8, 9, 0, tzinfo=timezone.utc)
        assert hi == datetime(2026, 4, 8, 10, 30, tzinfo=timezone.utc)


# ── detect_changes ──

class TestDetectChanges:
    async def test_no_change_same_snapshot(self):
        cursor = MockCursor([[(100,)]])
        r = await detect_changes(cursor, "db.t", "ts", "day", last_snapshot=100)
        assert r.action == RefreshAction.NO_CHANGE

    async def test_full_refresh_first_run(self):
        cursor = MockCursor([[(200,)]])
        r = await detect_changes(cursor, "db.t", "ts", "day", last_snapshot=None)
        assert r.action == RefreshAction.FULL_REFRESH
        assert r.current_snapshot == 200

    async def test_incremental_with_range(self):
        cursor = MockCursor([
            [(200,)],                    # get_current_snapshot
            [(1_700_000_000_000,)],      # committed_at lookup for last_snap
            [(200, "append")],           # snapshots strictly since last_snap
            # get_new_files_column_range — readable_metrics per file
            [({"ts": {"lower_bound": "2026-04-08T10:00:00+00:00", "upper_bound": "2026-04-08T15:30:00+00:00"}},)],
        ])
        r = await detect_changes(cursor, "db.t", "ts", "day", last_snapshot=100)
        assert r.action == RefreshAction.INCREMENTAL
        assert r.filter_range is not None
        # Day granularity: should snap to full day
        start, end = r.filter_range
        assert start.day == 8 and start.hour == 0
        assert end.day == 9 and end.hour == 0

    async def test_incremental_week_granularity(self):
        cursor = MockCursor([
            [(200,)],
            [(1_700_000_000_000,)],      # committed_at lookup
            [(200, "append")],
            [({"ts": {"lower_bound": "2026-04-08T10:00:00+00:00", "upper_bound": "2026-04-08T15:00:00+00:00"}},)],
        ])
        r = await detect_changes(cursor, "db.t", "ts", "week", last_snapshot=100)
        assert r.action == RefreshAction.INCREMENTAL
        start, end = r.filter_range
        # Apr 8 is Wednesday → week snaps to Mon Apr 6 – Mon Apr 13
        assert start.day == 6
        assert end.day == 13

    async def test_no_data_files_in_new_snapshots(self):
        cursor = MockCursor([
            [(200,)],
            [(1_700_000_000_000,)],      # committed_at lookup
            [(200, "append")],
            [],  # no entries from $all_entries
        ])
        r = await detect_changes(cursor, "db.t", "ts", "day", last_snapshot=100)
        assert r.action == RefreshAction.NO_CHANGE

    async def test_compaction_only_no_change_advances_state(self):
        """Only `replace` (compaction) snapshots since last_snap: no data
        changed, just files rewritten. Detector should return NO_CHANGE
        with the advanced current_snapshot and must NOT issue the
        $all_entries file-range query.
        """
        cursor = MockCursor([
            [(200,)],                          # current_snapshot
            [(1_700_000_000_000,)],            # committed_at lookup
            [(200, "replace")],                # compaction-only
            # no $all_entries query should follow
        ])
        r = await detect_changes(cursor, "db.t", "ts", "day", last_snapshot=100)
        assert r.action == RefreshAction.NO_CHANGE
        assert r.current_snapshot == 200
        # Exactly 3 queries: current_snapshot, committed_at lookup,
        # snapshots-since. No file-range query.
        assert len(cursor.executed_sql) == 3, cursor.executed_sql
        assert not any("all_entries" in s for s in cursor.executed_sql)

    async def test_mixed_append_and_replace_uses_only_append_snapshots(self):
        """When the new-snapshot set contains both an append and a
        compaction, the file-range query must scope to the append
        snapshot only. Compaction-added files contain the same rows
        rewritten and would uselessly expand the range.
        """
        cursor = MockCursor([
            [(51,)],                                                 # current_snapshot
            [(1_700_000_000_000,)],                                  # committed_at lookup
            [(50, "append"), (51, "replace")],                       # both ops
            [({"ts": {"lower_bound": "2026-04-08T09:00:00+00:00",
                      "upper_bound": "2026-04-08T10:00:00+00:00"}},)],
        ])
        r = await detect_changes(cursor, "db.t", "ts", "day", last_snapshot=100)
        assert r.action == RefreshAction.INCREMENTAL
        file_query = next(s for s in cursor.executed_sql if "all_entries" in s)
        assert "IN (50)" in file_query, (
            f"expected file-range query to scope to append snapshot 50 only, "
            f"got: {file_query}"
        )
        assert "51" not in file_query

    async def test_unexpected_operation_raises(self):
        """`overwrite`, `delete`, and any unknown op violate the
        append-only assumption and must raise, not silently trigger a
        FULL_REFRESH (which was the old behavior)."""
        cursor = MockCursor([
            [(200,)],
            [(1_700_000_000_000,)],
            [(200, "overwrite")],
        ])
        with pytest.raises(UnexpectedOperationError):
            await detect_changes(cursor, "db.t", "ts", "day", last_snapshot=100)
