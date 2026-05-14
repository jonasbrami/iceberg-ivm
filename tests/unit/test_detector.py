"""Tests for the change detector."""

from datetime import UTC, datetime, timedelta

import pytest

from iceberg_ivm.detector import (
    ExpiredSnapshotError,
    MissingFilterColumnError,
    RefreshAction,
    UnexpectedOperationError,
    detect_changes,
    expand_to_bucket_bounds,
    get_current_snapshot,
    get_new_files_column_range,
    get_snapshots_since,
    get_source_column_range,
    get_target_bucket_max,
    walk_buckets,
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


# ── expand_to_bucket_bounds ──


class TestExpandToBucketBounds:
    def test_minute(self):
        ts = datetime(2026, 4, 8, 10, 30, 45, 123456, tzinfo=UTC)
        s, e = expand_to_bucket_bounds(ts, ts, "minute")
        assert s == datetime(2026, 4, 8, 10, 30, 0, 0, tzinfo=UTC)
        assert e == datetime(2026, 4, 8, 10, 31, 0, 0, tzinfo=UTC)

    def test_hour(self):
        s, e = expand_to_bucket_bounds(
            datetime(2026, 4, 8, 10, 30, tzinfo=UTC),
            datetime(2026, 4, 8, 11, 45, tzinfo=UTC),
            "hour",
        )
        assert s == datetime(2026, 4, 8, 10, 0, tzinfo=UTC)
        assert e == datetime(2026, 4, 8, 12, 0, tzinfo=UTC)

    def test_day(self):
        s, e = expand_to_bucket_bounds(
            datetime(2026, 4, 8, 10, 0, tzinfo=UTC),
            datetime(2026, 4, 9, 15, 0, tzinfo=UTC),
            "day",
        )
        assert s == datetime(2026, 4, 8, tzinfo=UTC)
        assert e == datetime(2026, 4, 10, tzinfo=UTC)

    def test_week(self):
        # 2026-04-08 is a Wednesday
        s, e = expand_to_bucket_bounds(
            datetime(2026, 4, 8, 10, 0, tzinfo=UTC),
            datetime(2026, 4, 8, 15, 0, tzinfo=UTC),
            "week",
        )
        # Monday of that week = April 6
        assert s == datetime(2026, 4, 6, tzinfo=UTC)
        # Next Monday = April 13
        assert e == datetime(2026, 4, 13, tzinfo=UTC)

    def test_week_spanning_two_weeks(self):
        s, e = expand_to_bucket_bounds(
            datetime(2026, 4, 8, tzinfo=UTC),  # Wed week 1
            datetime(2026, 4, 15, tzinfo=UTC),  # Wed week 2
            "week",
        )
        assert s == datetime(2026, 4, 6, tzinfo=UTC)  # Mon week 1
        assert e == datetime(2026, 4, 20, tzinfo=UTC)  # Mon week 3

    def test_month(self):
        s, e = expand_to_bucket_bounds(
            datetime(2026, 4, 15, tzinfo=UTC),
            datetime(2026, 4, 20, tzinfo=UTC),
            "month",
        )
        assert s == datetime(2026, 4, 1, tzinfo=UTC)
        assert e == datetime(2026, 5, 1, tzinfo=UTC)

    def test_month_spanning_year_boundary(self):
        s, e = expand_to_bucket_bounds(
            datetime(2026, 12, 15, tzinfo=UTC),
            datetime(2026, 12, 25, tzinfo=UTC),
            "month",
        )
        assert s == datetime(2026, 12, 1, tzinfo=UTC)
        assert e == datetime(2027, 1, 1, tzinfo=UTC)

    def test_quarter(self):
        # Apr 15 is in Q2 (Apr-Jun)
        s, e = expand_to_bucket_bounds(
            datetime(2026, 4, 15, tzinfo=UTC),
            datetime(2026, 5, 20, tzinfo=UTC),
            "quarter",
        )
        assert s == datetime(2026, 4, 1, tzinfo=UTC)
        assert e == datetime(2026, 7, 1, tzinfo=UTC)

    def test_quarter_spanning_two(self):
        # Mar (Q1) to May (Q2)
        s, e = expand_to_bucket_bounds(
            datetime(2026, 3, 15, tzinfo=UTC),
            datetime(2026, 5, 10, tzinfo=UTC),
            "quarter",
        )
        assert s == datetime(2026, 1, 1, tzinfo=UTC)  # Q1 start
        assert e == datetime(2026, 7, 1, tzinfo=UTC)  # Q3 start (next after Q2)

    def test_quarter_year_boundary(self):
        # Q4 (Oct-Dec) spanning into year boundary
        s, e = expand_to_bucket_bounds(
            datetime(2026, 11, 1, tzinfo=UTC),
            datetime(2026, 12, 15, tzinfo=UTC),
            "quarter",
        )
        assert s == datetime(2026, 10, 1, tzinfo=UTC)  # Q4 start
        assert e == datetime(2027, 1, 1, tzinfo=UTC)  # Q1 next year

    def test_year(self):
        s, e = expand_to_bucket_bounds(
            datetime(2026, 6, 15, tzinfo=UTC),
            datetime(2026, 9, 20, tzinfo=UTC),
            "year",
        )
        assert s == datetime(2026, 1, 1, tzinfo=UTC)
        assert e == datetime(2027, 1, 1, tzinfo=UTC)

    def test_year_spanning(self):
        # Dec 2026 to Jan 2027
        s, e = expand_to_bucket_bounds(
            datetime(2026, 12, 15, tzinfo=UTC),
            datetime(2027, 1, 10, tzinfo=UTC),
            "year",
        )
        assert s == datetime(2026, 1, 1, tzinfo=UTC)
        assert e == datetime(2028, 1, 1, tzinfo=UTC)


class TestExpandToBucketBoundsBoundaryExact:
    """A row whose timestamp lands exactly on a bucket boundary belongs to
    the bucket *starting* at that boundary, never the previous one. The
    snapped range must therefore include that bucket — i.e. end must be
    strictly greater than the boundary timestamp.

    These cases catch off-by-one regressions where ceil_excl mistakenly
    returns the boundary itself instead of the next boundary.
    """

    def test_both_at_millisecond_boundary(self):
        ts = datetime(2026, 4, 8, 10, 30, 45, 1000, tzinfo=UTC)
        s, e = expand_to_bucket_bounds(ts, ts, "millisecond")
        assert s == ts
        assert e == ts + timedelta(milliseconds=1)

    def test_max_exactly_at_next_millisecond(self):
        # min mid-millisecond (us=500), max exactly on the next ms boundary.
        s, e = expand_to_bucket_bounds(
            datetime(2026, 4, 8, 10, 30, 45, 500, tzinfo=UTC),
            datetime(2026, 4, 8, 10, 30, 45, 1000, tzinfo=UTC),
            "millisecond",
        )
        assert s == datetime(2026, 4, 8, 10, 30, 45, 0, tzinfo=UTC)
        assert e == datetime(2026, 4, 8, 10, 30, 45, 2000, tzinfo=UTC)

    def test_both_at_second_boundary(self):
        ts = datetime(2026, 4, 8, 10, 30, 45, 0, tzinfo=UTC)
        s, e = expand_to_bucket_bounds(ts, ts, "second")
        assert s == ts
        assert e == ts + timedelta(seconds=1)

    def test_max_exactly_at_next_second(self):
        # min mid-second (us=500_000), max exactly on the next-second boundary.
        s, e = expand_to_bucket_bounds(
            datetime(2026, 4, 8, 10, 30, 45, 500_000, tzinfo=UTC),
            datetime(2026, 4, 8, 10, 30, 46, 0, tzinfo=UTC),
            "second",
        )
        assert s == datetime(2026, 4, 8, 10, 30, 45, 0, tzinfo=UTC)
        assert e == datetime(2026, 4, 8, 10, 30, 47, 0, tzinfo=UTC)

    def test_min_and_max_both_at_minute_boundary(self):
        ts = datetime(2026, 4, 8, 10, 0, 0, 0, tzinfo=UTC)
        s, e = expand_to_bucket_bounds(ts, ts, "minute")
        # The single point belongs to the minute starting at ts
        assert s == ts
        assert e == ts + timedelta(minutes=1)

    def test_max_at_hour_boundary_advances_end(self):
        # min mid-hour, max exactly on the next hour boundary.
        # The max row sits in the [11:00, 12:00) bucket → end must be 12:00.
        s, e = expand_to_bucket_bounds(
            datetime(2026, 4, 8, 10, 30, tzinfo=UTC),
            datetime(2026, 4, 8, 11, 0, tzinfo=UTC),
            "hour",
        )
        assert s == datetime(2026, 4, 8, 10, 0, tzinfo=UTC)
        assert e == datetime(2026, 4, 8, 12, 0, tzinfo=UTC)

    def test_both_at_day_boundary(self):
        d = datetime(2026, 4, 8, 0, 0, 0, 0, tzinfo=UTC)
        s, e = expand_to_bucket_bounds(d, d, "day")
        assert s == d
        assert e == d + timedelta(days=1)

    def test_max_exactly_at_next_day_midnight(self):
        # min on Apr 8, max exactly Apr 9 00:00 → max row belongs to Apr 9
        s, e = expand_to_bucket_bounds(
            datetime(2026, 4, 8, 10, 0, tzinfo=UTC),
            datetime(2026, 4, 9, 0, 0, 0, 0, tzinfo=UTC),
            "day",
        )
        assert s == datetime(2026, 4, 8, tzinfo=UTC)
        assert e == datetime(2026, 4, 10, tzinfo=UTC)

    def test_both_at_week_start(self):
        # 2026-04-06 is a Monday — exact week boundary
        mon = datetime(2026, 4, 6, 0, 0, 0, 0, tzinfo=UTC)
        s, e = expand_to_bucket_bounds(mon, mon, "week")
        assert s == mon
        assert e == mon + timedelta(days=7)

    def test_max_at_next_week_start(self):
        # min mid-week, max on next Monday 00:00 → max belongs to next week
        s, e = expand_to_bucket_bounds(
            datetime(2026, 4, 8, 10, 0, tzinfo=UTC),  # Wed week 1
            datetime(2026, 4, 13, 0, 0, tzinfo=UTC),  # Mon week 2
            "week",
        )
        assert s == datetime(2026, 4, 6, tzinfo=UTC)  # Mon week 1
        assert e == datetime(2026, 4, 20, tzinfo=UTC)  # Mon week 3

    def test_both_at_month_start(self):
        m = datetime(2026, 4, 1, 0, 0, 0, 0, tzinfo=UTC)
        s, e = expand_to_bucket_bounds(m, m, "month")
        assert s == m
        assert e == datetime(2026, 5, 1, tzinfo=UTC)

    def test_max_at_next_month_start_year_boundary(self):
        # max exactly on Jan 1 next year → max belongs to January
        s, e = expand_to_bucket_bounds(
            datetime(2026, 12, 15, tzinfo=UTC),
            datetime(2027, 1, 1, 0, 0, 0, 0, tzinfo=UTC),
            "month",
        )
        assert s == datetime(2026, 12, 1, tzinfo=UTC)
        assert e == datetime(2027, 2, 1, tzinfo=UTC)

    def test_both_at_quarter_start(self):
        q = datetime(2026, 4, 1, 0, 0, 0, 0, tzinfo=UTC)  # Q2 start
        s, e = expand_to_bucket_bounds(q, q, "quarter")
        assert s == q
        assert e == datetime(2026, 7, 1, tzinfo=UTC)  # Q3 start

    def test_both_at_year_start(self):
        y = datetime(2026, 1, 1, 0, 0, 0, 0, tzinfo=UTC)
        s, e = expand_to_bucket_bounds(y, y, "year")
        assert s == y
        assert e == datetime(2027, 1, 1, tzinfo=UTC)


# ── expand_to_bucket_bounds is the inverse of date_trunc ──


def _py_date_trunc(granularity: str, ts: datetime) -> datetime:
    """Python equivalent of Trino's date_trunc — the forward function."""
    if granularity == "millisecond":
        return ts.replace(microsecond=(ts.microsecond // 1000) * 1000)
    elif granularity == "second":
        return ts.replace(microsecond=0)
    elif granularity == "minute":
        return ts.replace(second=0, microsecond=0)
    elif granularity == "hour":
        return ts.replace(minute=0, second=0, microsecond=0)
    elif granularity == "day":
        return ts.replace(hour=0, minute=0, second=0, microsecond=0)
    elif granularity == "week":
        return (ts - timedelta(days=ts.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
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
    (datetime(2026, 4, 8, 10, 30, 45, 123456, tzinfo=UTC), datetime(2026, 4, 8, 10, 30, 45, 123456, tzinfo=UTC)),
    # within same day
    (datetime(2026, 4, 8, 9, 15, tzinfo=UTC), datetime(2026, 4, 8, 16, 45, tzinfo=UTC)),
    # spanning days
    (datetime(2026, 4, 8, 23, 59, 59, tzinfo=UTC), datetime(2026, 4, 9, 0, 0, 1, tzinfo=UTC)),
    # spanning weeks (Wed to next Wed)
    (datetime(2026, 4, 8, 12, 0, tzinfo=UTC), datetime(2026, 4, 15, 12, 0, tzinfo=UTC)),
    # spanning months
    (datetime(2026, 3, 28, tzinfo=UTC), datetime(2026, 4, 5, tzinfo=UTC)),
    # spanning quarters (Q1 → Q2)
    (datetime(2026, 3, 15, tzinfo=UTC), datetime(2026, 5, 10, tzinfo=UTC)),
    # spanning year boundary
    (datetime(2026, 12, 31, 23, 59, tzinfo=UTC), datetime(2027, 1, 1, 0, 1, tzinfo=UTC)),
    # Q4 year boundary
    (datetime(2026, 11, 15, tzinfo=UTC), datetime(2026, 12, 20, tzinfo=UTC)),
    # exact bucket boundary (minute)
    (datetime(2026, 4, 8, 10, 0, 0, 0, tzinfo=UTC), datetime(2026, 4, 8, 11, 0, 0, 0, tzinfo=UTC)),
    # sub-second spread (covers millisecond/second invariants)
    (datetime(2026, 4, 8, 10, 30, 45, 500_000, tzinfo=UTC), datetime(2026, 4, 8, 10, 30, 46, 999_000, tzinfo=UTC)),
    # cross-second with sub-millisecond offset on both ends (exercises
    # millisecond and second invariants across a whole-second boundary)
    (datetime(2026, 4, 8, 10, 30, 45, 999_500, tzinfo=UTC), datetime(2026, 4, 8, 10, 30, 47, 1_500, tzinfo=UTC)),
]

_GRANULARITIES = [
    "millisecond",
    "second",
    "minute",
    "hour",
    "day",
    "week",
    "month",
    "quarter",
    "year",
]


class TestExpandToBucketBoundsInversesDateTrunc:
    """Verify the invariant: expand_to_bucket_bounds is the exact inverse of date_trunc.

    For every granularity and every sample timestamp pair:
    1. start and end are bucket boundaries (date_trunc is idempotent on them)
    2. The bucket containing min_ts starts at or after start
    3. The bucket containing max_ts starts before end
    4. No bucket is partially covered — start and end ARE bucket boundaries
    """

    @pytest.mark.parametrize("granularity", _GRANULARITIES)
    @pytest.mark.parametrize("min_ts, max_ts", _SAMPLE_PAIRS)
    def test_boundaries_are_bucket_aligned(self, granularity, min_ts, max_ts):
        start, end = expand_to_bucket_bounds(min_ts, max_ts, granularity)

        # start is a bucket boundary: date_trunc(start) == start
        assert _py_date_trunc(granularity, start) == start, f"start {start} is not a {granularity} boundary"
        # end is a bucket boundary: date_trunc(end) == end
        assert _py_date_trunc(granularity, end) == end, f"end {end} is not a {granularity} boundary"

    @pytest.mark.parametrize("granularity", _GRANULARITIES)
    @pytest.mark.parametrize("min_ts, max_ts", _SAMPLE_PAIRS)
    def test_touched_buckets_are_fully_covered(self, granularity, min_ts, max_ts):
        start, end = expand_to_bucket_bounds(min_ts, max_ts, granularity)

        # The bucket containing min_ts is within [start, end)
        bucket_min = _py_date_trunc(granularity, min_ts)
        assert bucket_min >= start, f"min_ts bucket {bucket_min} is before start {start}"
        assert bucket_min < end, f"min_ts bucket {bucket_min} is at or after end {end}"

        # The bucket containing max_ts is within [start, end)
        bucket_max = _py_date_trunc(granularity, max_ts)
        assert bucket_max >= start, f"max_ts bucket {bucket_max} is before start {start}"
        assert bucket_max < end, f"max_ts bucket {bucket_max} is at or after end {end}"

    @pytest.mark.parametrize("granularity", _GRANULARITIES)
    @pytest.mark.parametrize("min_ts, max_ts", _SAMPLE_PAIRS)
    def test_range_is_tight(self, granularity, min_ts, max_ts):
        """start is the earliest bucket boundary that covers min_ts."""
        start, end = expand_to_bucket_bounds(min_ts, max_ts, granularity)

        # start == date_trunc(min_ts): the range starts exactly at the
        # bucket containing min_ts, not one bucket earlier
        assert start == _py_date_trunc(granularity, min_ts), (
            f"start {start} is not tight — should be {_py_date_trunc(granularity, min_ts)}"
        )


# ── get_current_snapshot ──


class TestGetCurrentSnapshot:
    async def test_returns_id(self):
        assert await get_current_snapshot(MockCursor([[(12345,)]]), "db.t") == 12345

    async def test_returns_none(self):
        assert await get_current_snapshot(MockCursor([[]]), "db.t") is None

    async def test_sql_uses_snapshot_id_tiebreak(self):
        """ORDER BY committed_at alone is non-deterministic when two siblings
        share a millisecond-precision committed_at — the wrong winner can leave
        last_snapshot == current_snap on the next tick and skip real new data.
        Mirrors the tiebreak in get_snapshots_since."""
        cursor = MockCursor([[(12345,)]])
        await get_current_snapshot(cursor, "db.t")
        sql = cursor.executed_sql[0]
        # Both keys should be in the ORDER BY with committed_at first and
        # snapshot_id as the tiebreak, descending so LIMIT 1 returns the head.
        # Snapshot IDs are not strictly time-ordered across writers, so key
        # order matters.
        order_by = sql.split("ORDER BY", 1)[1]
        assert "committed_at" in order_by
        assert "snapshot_id" in order_by
        assert order_by.index("committed_at") < order_by.index("snapshot_id")


# ── get_snapshots_since ──


class TestGetSnapshotsSince:
    async def test_raises_on_missing_last_snap(self):
        """If last_snap is no longer in $snapshots (Iceberg expired it),
        iceberg-ivm must fail loudly rather than silently return []."""
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
        cursor = MockCursor(
            [
                [(1_700_000_000_000,)],  # committed_at of last_snap
                [(200, "append"), (300, "append")],
            ]
        )
        await get_snapshots_since(cursor, "db.t", last_snap=100)
        assert len(cursor.executed_sql) == 2, (
            f"expected two queries (committed_at lookup + later snapshots), got {cursor.executed_sql}"
        )
        second = cursor.executed_sql[1]
        assert "snapshot_id" in second
        assert "100" in second  # last_snap appears in the tiebreak clause


# ── get_new_files_column_range ──


class TestGetNewFilesColumnRange:
    async def test_computes_range(self):
        cursor = MockCursor(
            [
                [
                    ({"ts": {"lower_bound": "2026-04-08T09:00:00+00:00", "upper_bound": "2026-04-08T12:00:00+00:00"}},),
                    ({"ts": {"lower_bound": "2026-04-08T11:00:00+00:00", "upper_bound": "2026-04-08T15:00:00+00:00"}},),
                ]
            ]
        )
        result = await get_new_files_column_range(cursor, "db.t", [100, 200], "ts")
        assert result is not None
        lo, hi = result
        assert lo == datetime(2026, 4, 8, 9, 0, tzinfo=UTC)
        assert hi == datetime(2026, 4, 8, 15, 0, tzinfo=UTC)

    async def test_no_data_files(self):
        cursor = MockCursor([[]])
        assert await get_new_files_column_range(cursor, "db.t", [100], "ts") is None

    async def test_raises_when_filter_column_absent_from_metrics(self):
        """File rows exist, but none contain the filter_column in their
        per-column metrics. That's a configuration error (typo'd column
        name, schema drift) — must fail loudly instead of silently
        returning None and freezing the view.
        """
        cursor = MockCursor(
            [
                [
                    ({"other_col": {"lower_bound": "1", "upper_bound": "2"}},),
                    ({"other_col": {"lower_bound": "3", "upper_bound": "4"}},),
                ]
            ]
        )
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
        cursor = MockCursor(
            [
                [
                    (
                        {
                            "ts": {
                                "lower_bound": "2026-04-08T09:00:00.000000+00:00",
                                "upper_bound": "2026-04-08T10:00:00.000000+00:00",
                            }
                        },
                    ),
                    (
                        {
                            "ts": {
                                "lower_bound": "2026-04-08T08:00:00.000000-01:00",
                                "upper_bound": "2026-04-08T09:30:00.000000-01:00",
                            }
                        },
                    ),
                ]
            ]
        )
        result = await get_new_files_column_range(cursor, "db.t", [1, 2], "ts")
        assert result is not None
        lo, hi = result
        assert lo == datetime(2026, 4, 8, 9, 0, tzinfo=UTC)
        assert hi == datetime(2026, 4, 8, 10, 30, tzinfo=UTC)


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
        cursor = MockCursor(
            [
                [(200,)],  # get_current_snapshot
                [(1_700_000_000_000,)],  # committed_at lookup for last_snap
                [(200, "append")],  # snapshots strictly since last_snap
                # get_new_files_column_range — readable_metrics per file
                [({"ts": {"lower_bound": "2026-04-08T10:00:00+00:00", "upper_bound": "2026-04-08T15:30:00+00:00"}},)],
            ]
        )
        r = await detect_changes(cursor, "db.t", "ts", "day", last_snapshot=100)
        assert r.action == RefreshAction.INCREMENTAL
        assert r.filter_range is not None
        # Day granularity: should snap to full day
        start, end = r.filter_range
        assert start.day == 8 and start.hour == 0
        assert end.day == 9 and end.hour == 0

    async def test_incremental_week_granularity(self):
        cursor = MockCursor(
            [
                [(200,)],
                [(1_700_000_000_000,)],  # committed_at lookup
                [(200, "append")],
                [({"ts": {"lower_bound": "2026-04-08T10:00:00+00:00", "upper_bound": "2026-04-08T15:00:00+00:00"}},)],
            ]
        )
        r = await detect_changes(cursor, "db.t", "ts", "week", last_snapshot=100)
        assert r.action == RefreshAction.INCREMENTAL
        start, end = r.filter_range
        # Apr 8 is Wednesday → week snaps to Mon Apr 6 – Mon Apr 13
        assert start.day == 6
        assert end.day == 13

    async def test_no_data_files_in_new_snapshots(self):
        cursor = MockCursor(
            [
                [(200,)],
                [(1_700_000_000_000,)],  # committed_at lookup
                [(200, "append")],
                [],  # no entries from $all_entries
            ]
        )
        r = await detect_changes(cursor, "db.t", "ts", "day", last_snapshot=100)
        assert r.action == RefreshAction.NO_CHANGE

    async def test_compaction_only_no_change_advances_state(self):
        """Only `replace` (compaction) snapshots since last_snap: no data
        changed, just files rewritten. Detector should return NO_CHANGE
        with the advanced current_snapshot and must NOT issue the
        $all_entries file-range query.
        """
        cursor = MockCursor(
            [
                [(200,)],  # current_snapshot
                [(1_700_000_000_000,)],  # committed_at lookup
                [(200, "replace")],  # compaction-only
                # no $all_entries query should follow
            ]
        )
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
        cursor = MockCursor(
            [
                [(51,)],  # current_snapshot
                [(1_700_000_000_000,)],  # committed_at lookup
                [(50, "append"), (51, "replace")],  # both ops
                [({"ts": {"lower_bound": "2026-04-08T09:00:00+00:00", "upper_bound": "2026-04-08T10:00:00+00:00"}},)],
            ]
        )
        r = await detect_changes(cursor, "db.t", "ts", "day", last_snapshot=100)
        assert r.action == RefreshAction.INCREMENTAL
        file_query = next(s for s in cursor.executed_sql if "all_entries" in s)
        assert "IN (50)" in file_query, (
            f"expected file-range query to scope to append snapshot 50 only, got: {file_query}"
        )
        assert "51" not in file_query

    async def test_delete_operation_raises(self):
        """`delete` and any unknown op violate the no-data-loss assumption
        and must raise, not silently trigger a FULL_REFRESH (which was the
        old behavior)."""
        cursor = MockCursor(
            [
                [(200,)],
                [(1_700_000_000_000,)],
                [(200, "delete")],
            ]
        )
        with pytest.raises(UnexpectedOperationError):
            await detect_changes(cursor, "db.t", "ts", "day", last_snapshot=100)

    async def test_unknown_operation_raises(self):
        """An entirely unknown operation name must also raise loudly so
        new Iceberg ops are surfaced to the operator instead of silently
        ignored."""
        cursor = MockCursor(
            [
                [(200,)],
                [(1_700_000_000_000,)],
                [(200, "rewrite")],  # not in the allowed set
            ]
        )
        with pytest.raises(UnexpectedOperationError):
            await detect_changes(cursor, "db.t", "ts", "day", last_snapshot=100)

    async def test_overwrite_drives_incremental_refresh(self):
        """`overwrite` snapshots are produced by MERGE INTO (e.g. an upstream
        chained MV's refresh). They are real data changes and must drive
        an incremental refresh exactly like `append`, scoping the
        $all_entries file-range query to the overwrite snapshot's added
        files."""
        cursor = MockCursor(
            [
                [(200,)],  # current_snapshot
                [(1_700_000_000_000,)],  # committed_at lookup
                [(200, "overwrite")],  # one MERGE-driven snapshot
                [({"ts": {"lower_bound": "2026-04-08T10:00:00+00:00", "upper_bound": "2026-04-08T15:30:00+00:00"}},)],
            ]
        )
        r = await detect_changes(cursor, "db.t", "ts", "day", last_snapshot=100)
        assert r.action == RefreshAction.INCREMENTAL
        assert r.current_snapshot == 200
        assert r.filter_range is not None
        start, end = r.filter_range
        # Day granularity: should snap to full day [Apr 8, Apr 9)
        assert start == datetime(2026, 4, 8, tzinfo=UTC)
        assert end == datetime(2026, 4, 9, tzinfo=UTC)
        # The file-range query must scope to snapshot 200.
        file_query = next(s for s in cursor.executed_sql if "all_entries" in s)
        assert "IN (200)" in file_query

    async def test_mixed_append_overwrite_replace_uses_change_snapshots(self):
        """When the new-snapshot set mixes append, overwrite (MERGE) and
        replace (compaction), the file-range query must scope to the
        change-driving snapshots only (append + overwrite), never to
        compaction-rewritten files."""
        cursor = MockCursor(
            [
                [(53,)],  # current_snapshot
                [(1_700_000_000_000,)],  # committed_at lookup
                [(50, "append"), (51, "replace"), (52, "overwrite"), (53, "replace")],  # mixed
                [({"ts": {"lower_bound": "2026-04-08T09:00:00+00:00", "upper_bound": "2026-04-08T10:00:00+00:00"}},)],
            ]
        )
        r = await detect_changes(cursor, "db.t", "ts", "day", last_snapshot=100)
        assert r.action == RefreshAction.INCREMENTAL
        file_query = next(s for s in cursor.executed_sql if "all_entries" in s)
        # Both 50 (append) and 52 (overwrite) drive change; 51/53 (replace) must not.
        assert "50" in file_query
        assert "52" in file_query
        assert "51" not in file_query
        assert "53" not in file_query


# ── walk_buckets ──


class TestWalkBuckets:
    def test_empty_range(self):
        ts = datetime(2026, 4, 8, tzinfo=UTC)
        assert list(walk_buckets(ts, ts, "day")) == []
        assert list(walk_buckets(ts, ts - timedelta(days=1), "day")) == []

    def test_day_simple(self):
        chunks = list(
            walk_buckets(
                datetime(2026, 4, 8, tzinfo=UTC),
                datetime(2026, 4, 11, tzinfo=UTC),
                "day",
            )
        )
        assert chunks == [
            (datetime(2026, 4, 8, tzinfo=UTC), datetime(2026, 4, 9, tzinfo=UTC)),
            (datetime(2026, 4, 9, tzinfo=UTC), datetime(2026, 4, 10, tzinfo=UTC)),
            (datetime(2026, 4, 10, tzinfo=UTC), datetime(2026, 4, 11, tzinfo=UTC)),
        ]

    def test_hour_spanning_day(self):
        chunks = list(
            walk_buckets(
                datetime(2026, 4, 8, 22, tzinfo=UTC),
                datetime(2026, 4, 9, 2, tzinfo=UTC),
                "hour",
            )
        )
        assert len(chunks) == 4
        assert chunks[0][0] == datetime(2026, 4, 8, 22, tzinfo=UTC)
        assert chunks[-1][1] == datetime(2026, 4, 9, 2, tzinfo=UTC)
        # half-open and contiguous
        for a, b in zip(chunks, chunks[1:], strict=False):
            assert a[1] == b[0]

    def test_month_spanning_year(self):
        chunks = list(
            walk_buckets(
                datetime(2026, 11, 1, tzinfo=UTC),
                datetime(2027, 2, 1, tzinfo=UTC),
                "month",
            )
        )
        assert chunks == [
            (datetime(2026, 11, 1, tzinfo=UTC), datetime(2026, 12, 1, tzinfo=UTC)),
            (datetime(2026, 12, 1, tzinfo=UTC), datetime(2027, 1, 1, tzinfo=UTC)),
            (datetime(2027, 1, 1, tzinfo=UTC), datetime(2027, 2, 1, tzinfo=UTC)),
        ]

    def test_quarter_three_months_per_step(self):
        chunks = list(
            walk_buckets(
                datetime(2026, 1, 1, tzinfo=UTC),
                datetime(2027, 1, 1, tzinfo=UTC),
                "quarter",
            )
        )
        assert len(chunks) == 4
        assert chunks[0] == (
            datetime(2026, 1, 1, tzinfo=UTC),
            datetime(2026, 4, 1, tzinfo=UTC),
        )
        assert chunks[-1] == (
            datetime(2026, 10, 1, tzinfo=UTC),
            datetime(2027, 1, 1, tzinfo=UTC),
        )

    def test_year_step(self):
        chunks = list(
            walk_buckets(
                datetime(2024, 1, 1, tzinfo=UTC),
                datetime(2027, 1, 1, tzinfo=UTC),
                "year",
            )
        )
        assert chunks == [
            (datetime(2024, 1, 1, tzinfo=UTC), datetime(2025, 1, 1, tzinfo=UTC)),
            (datetime(2025, 1, 1, tzinfo=UTC), datetime(2026, 1, 1, tzinfo=UTC)),
            (datetime(2026, 1, 1, tzinfo=UTC), datetime(2027, 1, 1, tzinfo=UTC)),
        ]

    def test_unsupported_granularity_raises(self):
        with pytest.raises(ValueError, match="unsupported granularity"):
            list(
                walk_buckets(
                    datetime(2026, 1, 1, tzinfo=UTC),
                    datetime(2026, 2, 1, tzinfo=UTC),
                    "decade",
                )
            )

    def test_misaligned_start_clamps_last_chunk_to_end(self):
        """If the caller passes an unaligned ``start`` or ``end``, walk_buckets
        still steps forward by the granularity step but clamps the last
        chunk's upper bound to ``end``. Practically callers always align
        via ``expand_to_bucket_bounds`` first, but the clamp keeps the
        function from yielding a chunk that extends past ``end``."""
        # start on Apr 8 10:30, end on Apr 10 05:15, step = 1 day
        chunks = list(
            walk_buckets(
                datetime(2026, 4, 8, 10, 30, tzinfo=UTC),
                datetime(2026, 4, 10, 5, 15, tzinfo=UTC),
                "day",
            )
        )
        # The last yielded upper bound is clamped to end (Apr 10 05:15),
        # not extended to Apr 11 10:30.
        assert chunks[-1][1] == datetime(2026, 4, 10, 5, 15, tzinfo=UTC)
        # And contiguity still holds.
        for a, b in zip(chunks, chunks[1:], strict=False):
            assert a[1] == b[0]


# ── get_source_column_range ──


class TestGetSourceColumnRange:
    async def test_reads_from_files_system_table(self):
        cursor = MockCursor(
            [
                [
                    ({"ts": {"lower_bound": "2024-01-01T00:00:00+00:00", "upper_bound": "2024-01-02T00:00:00+00:00"}},),
                    ({"ts": {"lower_bound": "2026-04-01T00:00:00+00:00", "upper_bound": "2026-04-21T23:59:59+00:00"}},),
                ]
            ]
        )
        result = await get_source_column_range(cursor, "iceberg.db.t", "ts")
        assert result is not None
        lo, hi = result
        assert lo == datetime(2024, 1, 1, tzinfo=UTC)
        assert hi == datetime(2026, 4, 21, 23, 59, 59, tzinfo=UTC)
        # Queries $files, not $all_entries — $all_entries would only return
        # files added BY a single commit, not the live set.
        assert 'iceberg.db."t$files"' in cursor.executed_sql[0]
        assert "$all_entries" not in cursor.executed_sql[0]

    async def test_empty_table(self):
        cursor = MockCursor([[]])
        assert await get_source_column_range(cursor, "db.t", "ts") is None

    async def test_raises_when_filter_column_absent(self):
        cursor = MockCursor(
            [
                [
                    ({"other": {"lower_bound": "1", "upper_bound": "2"}},),
                ]
            ]
        )
        with pytest.raises(MissingFilterColumnError):
            await get_source_column_range(cursor, "db.t", "ts")


# ── get_target_bucket_max ──


class TestGetTargetBucketMax:
    async def test_reads_max_upper_bound(self):
        cursor = MockCursor(
            [
                [
                    (
                        {
                            "minute": {
                                "lower_bound": "2026-04-08T00:00:00+00:00",
                                "upper_bound": "2026-04-08T23:59:00+00:00",
                            }
                        },
                    ),
                    (
                        {
                            "minute": {
                                "lower_bound": "2026-04-09T00:00:00+00:00",
                                "upper_bound": "2026-04-09T23:59:00+00:00",
                            }
                        },
                    ),
                ]
            ]
        )
        result = await get_target_bucket_max(cursor, "iceberg.out.mv", "minute")
        assert result == datetime(2026, 4, 9, 23, 59, tzinfo=UTC)

    async def test_empty_target_returns_none(self):
        cursor = MockCursor([[]])
        assert await get_target_bucket_max(cursor, "db.t", "minute") is None

    async def test_bucket_column_absent_returns_none(self):
        """Target has files but none carry metrics for the bucket column —
        treat as empty resume (not an error), since the target may be a
        fresh table whose first chunk is still in flight."""
        cursor = MockCursor(
            [
                [
                    ({"other": {"lower_bound": "1", "upper_bound": "2"}},),
                ]
            ]
        )
        assert await get_target_bucket_max(cursor, "db.t", "minute") is None

    async def test_filters_out_delete_files(self):
        """$files exposes V2 position/equality delete files via the `content`
        column (0=DATA, 1=POS_DELETES, 2=EQ_DELETES). The resume point must
        be computed from data files only — including delete-file metrics
        would skew the max upward and skip live buckets on resume."""
        cursor = MockCursor([[]])
        await get_target_bucket_max(cursor, "db.t", "minute")
        sql = cursor.executed_sql[0]
        assert "content = 0" in sql or "content=0" in sql
