"""Tests for change detection: Iceberg metadata reads and bucket math."""

import json
from datetime import UTC, datetime, timedelta

import pytest

from iceberg_ivm.detector import (
    FullRefreshNeeded,
    MissingStatsError,
    RefreshAction,
    changed_range,
    column_range,
    detect_changes,
    expand_to_bucket_bounds,
    get_current_snapshot,
    incremental_range_since,
    snapshots_between,
    system_table,
    walk_buckets,
)


class MetaCursor:
    """Fake Trino cursor answering by which Iceberg metadata table a query reads."""

    def __init__(self, *, main=None, snapshots=(), entries=(), files=()):
        self.main = main
        self.snapshots = list(snapshots)  # (snapshot_id, parent_id, operation)
        self.entries = list(entries)  # (readable_metrics,)
        self.files = list(files)  # (readable_metrics,)
        self.sql: list[str] = []
        self._rows: list = []

    async def execute(self, sql):
        self.sql.append(sql)
        if "$refs" in sql:
            self._rows = [(self.main,)] if self.main is not None else []
        elif "$snapshots" in sql:
            self._rows = list(self.snapshots)
        elif "$all_entries" in sql:
            self._rows = list(self.entries)
        elif "$files" in sql:
            self._rows = list(self.files)
        else:
            raise AssertionError(f"unexpected SQL: {sql}")

    async def fetchone(self):
        return self._rows[0] if self._rows else None

    async def fetchall(self):
        return self._rows


def metrics(column="ts", lower="2026-04-09T10:15:00", upper="2026-04-09T15:30:00", values=10, nulls=0):
    return (
        json.dumps(
            {column: {"value_count": values, "null_value_count": nulls, "lower_bound": lower, "upper_bound": upper}}
        ),
    )


SRC = "iceberg.md.trades"


def test_system_table_quotes_the_metadata_suffix():
    assert system_table("iceberg.md.trades", "snapshots") == 'iceberg.md."trades$snapshots"'
    assert system_table("trades", "files") == '"trades$files"'


# ── current snapshot and lineage ──


async def test_current_snapshot_is_the_main_branch_ref():
    cur = MetaCursor(main=42)
    assert await get_current_snapshot(cur, SRC) == 42
    assert "name = 'main'" in cur.sql[0]


async def test_current_snapshot_of_empty_table_is_none():
    assert await get_current_snapshot(MetaCursor(), SRC) is None


async def test_snapshots_between_follows_parents_oldest_first():
    # 4 is on a side branch (parent 2) and must not be included.
    cur = MetaCursor(
        snapshots=[(1, None, "append"), (2, 1, "append"), (3, 2, "replace"), (4, 2, "append"), (5, 3, "overwrite")]
    )
    assert await snapshots_between(cur, SRC, 1, 5) == [(2, "append"), (3, "replace"), (5, "overwrite")]


async def test_snapshots_between_same_snapshot_is_empty():
    assert await snapshots_between(MetaCursor(snapshots=[(1, None, "append")]), SRC, 1, 1) == []


async def test_snapshots_between_survives_an_expired_bookmark():
    """Expiring the bookmark itself is harmless: its children still name it as
    parent, and their manifests (which the diff reads) are still live."""
    cur = MetaCursor(snapshots=[(2, 1, "append"), (3, 2, "append")])
    assert await snapshots_between(cur, SRC, 1, 3) == [(2, "append"), (3, "append")]


@pytest.mark.parametrize(
    "snapshots",
    [
        [(1, None, "append"), (3, None, "append")],  # table replaced: 3 doesn't descend from 1
        [(1, None, "append"), (2, 9, "append"), (3, 2, "append")],  # interior parent 9 expired
    ],
)
async def test_snapshots_between_raises_when_bookmark_is_not_an_ancestor(snapshots):
    with pytest.raises(FullRefreshNeeded, match="not an ancestor"):
        await snapshots_between(MetaCursor(snapshots=snapshots), SRC, 1, 3)


# ── file statistics ──


async def test_changed_range_spans_all_files():
    cur = MetaCursor(
        entries=[
            metrics(lower="2026-04-09T10:15:00", upper="2026-04-09T11:00:00"),
            metrics(lower="2026-04-08T23:59:00", upper="2026-04-09T15:30:00"),
        ]
    )
    assert await changed_range(cur, SRC, [2, 3], "ts") == (datetime(2026, 4, 8, 23, 59), datetime(2026, 4, 9, 15, 30))
    sql = cur.sql[0]
    assert "snapshot_id IN (2, 3)" in sql and "status IN (1, 2)" in sql and "data_file.content = 0" in sql


async def test_changed_range_skips_files_whose_values_are_all_null():
    cur = MetaCursor(entries=[metrics(lower=None, upper=None, values=5, nulls=5), metrics()])
    assert await changed_range(cur, SRC, [2], "ts") == (datetime(2026, 4, 9, 10, 15), datetime(2026, 4, 9, 15, 30))


@pytest.mark.parametrize(
    "row",
    [
        metrics(lower=None, upper=None, values=1000, nulls=0),  # metrics mode counts
        metrics(lower=None, upper=None, values=None, nulls=None),  # metrics mode none
        metrics(column="other"),  # column missing from metrics
    ],
)
async def test_changed_range_refuses_files_without_bounds(row):
    """Skipping such a file would silently drop its rows from the view."""
    with pytest.raises(MissingStatsError, match="no min/max statistics for column 'ts'"):
        await changed_range(MetaCursor(entries=[metrics(), row]), SRC, [2], "ts")


async def test_changed_range_of_no_files_is_none():
    assert await changed_range(MetaCursor(), SRC, [2], "ts") is None


async def test_column_range_reads_live_data_files():
    cur = MetaCursor(files=[metrics(lower="2026-04-01T00:00:00+00:00", upper="2026-04-30T12:00:00+00:00")])
    lo, hi = await column_range(cur, SRC, "ts")
    assert (lo, hi) == (datetime(2026, 4, 1, tzinfo=UTC), datetime(2026, 4, 30, 12, tzinfo=UTC))
    assert '"trades$files" WHERE content = 0' in cur.sql[0]


# ── incremental_range_since ──


async def test_incremental_range_is_snapped_to_buckets():
    cur = MetaCursor(snapshots=[(1, None, "append"), (2, 1, "append")], entries=[metrics()])
    assert await incremental_range_since(cur, SRC, "ts", "day", 1, 2) == (datetime(2026, 4, 9), datetime(2026, 4, 10))


async def test_incremental_range_ignores_compaction_files():
    cur = MetaCursor(snapshots=[(1, None, "append"), (2, 1, "replace"), (3, 2, "append")], entries=[metrics()])
    await incremental_range_since(cur, SRC, "ts", "hour", 1, 3)
    assert "snapshot_id IN (3)" in cur.sql[-1]


async def test_incremental_range_of_compaction_only_is_none():
    cur = MetaCursor(snapshots=[(1, None, "append"), (2, 1, "replace")])
    assert await incremental_range_since(cur, SRC, "ts", "hour", 1, 2) is None


async def test_deletes_need_a_full_refresh():
    cur = MetaCursor(snapshots=[(1, None, "append"), (2, 1, "delete")])
    with pytest.raises(FullRefreshNeeded, match="delete"):
        await incremental_range_since(cur, SRC, "ts", "hour", 1, 2)


async def test_missing_stats_need_a_full_refresh():
    cur = MetaCursor(snapshots=[(1, None, "append"), (2, 1, "append")], entries=[metrics(lower=None, upper=None)])
    with pytest.raises(FullRefreshNeeded, match="statistics"):
        await incremental_range_since(cur, SRC, "ts", "hour", 1, 2)


# ── detect_changes ──


async def test_detect_no_change_when_bookmark_is_current():
    r = await detect_changes(MetaCursor(main=7), SRC, "ts", "day", 7)
    assert (r.action, r.current_snapshot) == (RefreshAction.NO_CHANGE, 7)


async def test_detect_no_change_on_empty_source():
    r = await detect_changes(MetaCursor(), SRC, "ts", "day", None)
    assert (r.action, r.current_snapshot) == (RefreshAction.NO_CHANGE, None)


async def test_detect_first_run_is_full_refresh():
    r = await detect_changes(MetaCursor(main=7), SRC, "ts", "day", None)
    assert (r.action, r.current_snapshot) == (RefreshAction.FULL_REFRESH, 7)


async def test_detect_incremental():
    cur = MetaCursor(main=2, snapshots=[(1, None, "append"), (2, 1, "append")], entries=[metrics()])
    r = await detect_changes(cur, SRC, "ts", "hour", 1)
    assert r.action == RefreshAction.INCREMENTAL
    assert r.filter_range == (datetime(2026, 4, 9, 10), datetime(2026, 4, 9, 16))


async def test_detect_compaction_only_advances_without_refresh():
    cur = MetaCursor(main=2, snapshots=[(1, None, "append"), (2, 1, "replace")])
    r = await detect_changes(cur, SRC, "ts", "hour", 1)
    assert (r.action, r.current_snapshot) == (RefreshAction.NO_CHANGE, 2)


async def test_detect_falls_back_to_full_refresh_when_change_is_unbounded():
    cur = MetaCursor(main=3, snapshots=[(1, None, "append"), (3, None, "append")])  # table replaced
    r = await detect_changes(cur, SRC, "ts", "hour", 1)
    assert (r.action, r.current_snapshot) == (RefreshAction.FULL_REFRESH, 3)


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
