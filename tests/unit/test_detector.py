"""Tests for the change detector."""
from datetime import datetime, timedelta, timezone

from trino_mv_orchestrator.detector import (
    RefreshAction,
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

    def execute(self, sql: str):
        if self._idx < len(self._results):
            self._rows = list(self._results[self._idx])
        else:
            self._rows = []
        self._idx += 1

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
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


# ── _parse_ts ──

class TestParseTs:
    def test_iso_with_tz(self):
        dt = _parse_ts("2026-04-08T10:30:45.123456+00:00")
        assert dt.year == 2026 and dt.month == 4 and dt.day == 8

    def test_iso_no_tz(self):
        dt = _parse_ts("2026-04-08T10:30:45.123456")
        assert dt.hour == 10 and dt.minute == 30

    def test_date_only(self):
        dt = _parse_ts("2026-04-08")
        assert dt.day == 8


# ── get_current_snapshot ──

class TestGetCurrentSnapshot:
    def test_returns_id(self):
        assert get_current_snapshot(MockCursor([[(12345,)]]), "db.t") == 12345

    def test_returns_none(self):
        assert get_current_snapshot(MockCursor([[]]), "db.t") is None


# ── get_new_files_column_range ──

class TestGetNewFilesColumnRange:
    def test_computes_range(self):
        cursor = MockCursor([[
            ({"ts": {"lower_bound": "2026-04-08T09:00:00+00:00", "upper_bound": "2026-04-08T12:00:00+00:00"}},),
            ({"ts": {"lower_bound": "2026-04-08T11:00:00+00:00", "upper_bound": "2026-04-08T15:00:00+00:00"}},),
        ]])
        result = get_new_files_column_range(cursor, "db.t", [100, 200], "ts")
        assert result is not None
        assert "09:00:00" in result[0]
        assert "15:00:00" in result[1]

    def test_no_data_files(self):
        cursor = MockCursor([[]])
        assert get_new_files_column_range(cursor, "db.t", [100], "ts") is None


# ── detect_changes ──

class TestDetectChanges:
    def test_no_change_same_snapshot(self):
        cursor = MockCursor([[(100,)]])
        r = detect_changes(cursor, "db.t", "ts", "day", last_snapshot=100)
        assert r.action == RefreshAction.NO_CHANGE

    def test_full_refresh_first_run(self):
        cursor = MockCursor([[(200,)]])
        r = detect_changes(cursor, "db.t", "ts", "day", last_snapshot=None)
        assert r.action == RefreshAction.FULL_REFRESH
        assert r.current_snapshot == 200

    def test_full_refresh_on_overwrite(self):
        cursor = MockCursor([
            [(200,)],                                  # get_current_snapshot
            [{"snapshot_id": 200, "operation": "overwrite"}],  # get_snapshots_since
        ])
        # get_snapshots_since returns list of rows; mock returns dicts directly
        # Need to mock differently — the cursor returns tuples
        cursor = MockCursor([
            [(200,)],              # get_current_snapshot
            [(200, "overwrite")],  # get_snapshots_since
        ])
        r = detect_changes(cursor, "db.t", "ts", "day", last_snapshot=100)
        assert r.action == RefreshAction.FULL_REFRESH

    def test_incremental_with_range(self):
        cursor = MockCursor([
            [(200,)],              # get_current_snapshot
            [(200, "append")],     # get_snapshots_since
            # get_new_files_column_range — readable_metrics per file
            [({"ts": {"lower_bound": "2026-04-08T10:00:00+00:00", "upper_bound": "2026-04-08T15:30:00+00:00"}},)],
        ])
        r = detect_changes(cursor, "db.t", "ts", "day", last_snapshot=100)
        assert r.action == RefreshAction.INCREMENTAL
        assert r.filter_range is not None
        # Day granularity: should snap to full day
        start, end = r.filter_range
        assert start.day == 8 and start.hour == 0
        assert end.day == 9 and end.hour == 0

    def test_incremental_week_granularity(self):
        cursor = MockCursor([
            [(200,)],
            [(200, "append")],
            [({"ts": {"lower_bound": "2026-04-08T10:00:00+00:00", "upper_bound": "2026-04-08T15:00:00+00:00"}},)],
        ])
        r = detect_changes(cursor, "db.t", "ts", "week", last_snapshot=100)
        assert r.action == RefreshAction.INCREMENTAL
        start, end = r.filter_range
        # Apr 8 is Wednesday → week snaps to Mon Apr 6 – Mon Apr 13
        assert start.day == 6
        assert end.day == 13

    def test_no_data_files_in_new_snapshots(self):
        cursor = MockCursor([
            [(200,)],
            [(200, "append")],
            [],  # no entries from $all_entries
        ])
        r = detect_changes(cursor, "db.t", "ts", "day", last_snapshot=100)
        assert r.action == RefreshAction.NO_CHANGE
