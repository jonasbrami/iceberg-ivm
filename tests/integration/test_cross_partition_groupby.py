"""Tests for cross-partition GROUP BY correctness.

Validates that when the GROUP BY granularity is coarser than the source
partition granularity, the file-stats-based detector correctly snaps the
range to complete GROUP BY buckets.
"""
from __future__ import annotations

import pytest

from trino_mv_orchestrator.config import ViewConfig, infer_granularity
from trino_mv_orchestrator.detector import RefreshAction, detect_changes
from trino_mv_orchestrator.executor import execute_full_refresh, execute_incremental_refresh
from trino_mv_orchestrator.introspect import discover_columns, build_create_table_sql
from trino_mv_orchestrator.state import write_last_snapshot

pytestmark = [pytest.mark.integration, pytest.mark.xdist_group("integration")]

SOURCE_TABLE = "iceberg.test_schema.trades"
WEEKLY_TARGET = "iceberg.test_schema.ohlcv_weekly"
MONTHLY_TARGET = "iceberg.test_schema.ohlcv_monthly"

CREATE_SOURCE = f"""
CREATE TABLE {SOURCE_TABLE} (
    symbol VARCHAR, ts TIMESTAMP(6) WITH TIME ZONE, price DOUBLE, quantity DOUBLE
) WITH (format = 'PARQUET', partitioning = ARRAY['day(ts)'])
"""

WEEKLY_VIEW = ViewConfig(
    name="test_weekly", source_table=SOURCE_TABLE, filter_column="ts",
    query=f"""
        SELECT symbol, date_trunc('week', ts) AS week,
               min_by(price, ts) AS open, max(price) AS high,
               min(price) AS low, max_by(price, ts) AS close,
               sum(quantity) AS volume, count(*) AS trade_count
        FROM {SOURCE_TABLE} WHERE {{range_filter}} GROUP BY 1, 2
    """,
    merge_keys=["symbol", "week"],
    target_table=WEEKLY_TARGET, target_partitioning="ARRAY['day(week)']",
)

MONTHLY_VIEW = ViewConfig(
    name="test_monthly", source_table=SOURCE_TABLE, filter_column="ts",
    query=f"""
        SELECT symbol, date_trunc('month', ts) AS month,
               min_by(price, ts) AS open, max(price) AS high,
               min(price) AS low, max_by(price, ts) AS close,
               sum(quantity) AS volume, count(*) AS trade_count
        FROM {SOURCE_TABLE} WHERE {{range_filter}} GROUP BY 1, 2
    """,
    merge_keys=["symbol", "month"],
    target_table=MONTHLY_TARGET, target_partitioning="ARRAY['day(month)']",
)


async def insert_trades(cursor, day, trades):
    for sym, t, p, q in trades:
        await cursor.execute(
            f"INSERT INTO {SOURCE_TABLE} VALUES "
            f"('{sym}', TIMESTAMP '{day} {t} UTC', {p}, {q})"
        )


async def setup_and_full_refresh(cursor, view, target):
    cols = await discover_columns(cursor, view.query)
    value_cols = [c.name for c in cols if c.name not in view.merge_keys]
    await cursor.execute(build_create_table_sql(target, cols, view.target_partitioning))
    await execute_full_refresh(cursor, view, target)
    result = await detect_changes(cursor, SOURCE_TABLE, "ts", infer_granularity(view.query), last_snapshot=None)
    await write_last_snapshot(cursor, target, result.current_snapshot)
    return result, value_cols


class TestWeeklyBarsCrossPartition:
    """Weekly bars from daily-partitioned source.

    The GROUP BY (week) spans 7 source partitions (days). When new data
    arrives for Wednesday, the detector must snap the range to the full
    week (Mon-Sun) so the MERGE recomputes the weekly bar correctly.
    """

    async def test_incremental_refresh_preserves_all_days(self, trino_conn):
        cursor = await trino_conn.cursor()
        await cursor.execute(CREATE_SOURCE)

        # Monday + Tuesday trades (same week, 2026-04-06 is Monday)
        await insert_trades(cursor, "2026-04-06", [("AAPL", "10:00:00", 150.0, 100)])
        await insert_trades(cursor, "2026-04-07", [("AAPL", "10:00:00", 160.0, 200)])

        result, value_cols = await setup_and_full_refresh(cursor, WEEKLY_VIEW, WEEKLY_TARGET)
        last_snap = result.current_snapshot

        # Verify full refresh is correct
        await cursor.execute(f"SELECT volume, trade_count FROM {WEEKLY_TARGET} WHERE symbol = 'AAPL'")
        row = await cursor.fetchone()
        assert row[0] == 300.0
        assert row[1] == 2

        # Add Wednesday trade
        await insert_trades(cursor, "2026-04-08", [("AAPL", "10:00:00", 155.0, 50)])

        result = await detect_changes(cursor, SOURCE_TABLE, "ts", "week", last_snap)
        assert result.action == RefreshAction.INCREMENTAL
        # Range should cover the full week (Mon-Sun), not just Wednesday
        start, end = result.filter_range
        assert start.day == 6   # Monday
        assert end.day == 13    # Next Monday (exclusive)

        await execute_incremental_refresh(cursor, WEEKLY_VIEW, WEEKLY_TARGET, value_cols, result.filter_range)

        await cursor.execute(f"SELECT volume, trade_count, high, low FROM {WEEKLY_TARGET} WHERE symbol = 'AAPL'")
        row = await cursor.fetchone()
        assert row[0] == 350.0, f"volume should be 350, got {row[0]}"
        assert row[1] == 3, f"trade_count should be 3, got {row[1]}"
        assert row[2] == 160.0, f"high should be 160, got {row[2]}"
        assert row[3] == 150.0, f"low should be 150, got {row[3]}"

    async def test_new_data_in_next_week(self, trino_conn):
        """New data in a different week should not affect the previous week."""
        cursor = await trino_conn.cursor()
        await cursor.execute(CREATE_SOURCE)

        # Week 1: Mon Apr 6
        await insert_trades(cursor, "2026-04-06", [("AAPL", "10:00:00", 150.0, 100)])
        result, value_cols = await setup_and_full_refresh(cursor, WEEKLY_VIEW, WEEKLY_TARGET)

        # Week 2: Mon Apr 13
        await insert_trades(cursor, "2026-04-13", [("AAPL", "10:00:00", 200.0, 50)])

        result = await detect_changes(cursor, SOURCE_TABLE, "ts", "week", result.current_snapshot)
        assert result.action == RefreshAction.INCREMENTAL
        start, end = result.filter_range
        assert start.day == 13  # Monday of week 2
        assert end.day == 20    # Next Monday

        await execute_incremental_refresh(cursor, WEEKLY_VIEW, WEEKLY_TARGET, value_cols, result.filter_range)

        await cursor.execute(f"SELECT week, volume FROM {WEEKLY_TARGET} WHERE symbol = 'AAPL' ORDER BY week")
        rows = await cursor.fetchall()
        assert len(rows) == 2
        assert rows[0][1] == 100.0  # Week 1 untouched
        assert rows[1][1] == 50.0   # Week 2 new


class TestLateArrivingData:
    """Out-of-order ingestion: a row whose timestamp is *earlier* than
    data already processed in a previous refresh cycle.

    This is the canonical streaming case the orchestrator exists for.
    The detector reads the late row's min/max from $all_entries, snaps
    to the bucket containing the late timestamp, and the MERGE
    recomputes that historical bucket from complete source data.
    """

    async def test_late_row_recomputes_old_weekly_bucket(self, trino_conn):
        """Setup: process week of Apr 6 (Mon+Tue), then process week of
        Apr 13. Then a Wednesday-Apr-8 row arrives late — it belongs to
        the *first* week, which has already been written.

        Expected: the Apr-6 week bar is recomputed to include the late
        row (volume goes 300 → 350, trade_count goes 2 → 3) WITHOUT
        touching the Apr-13 week bar.
        """
        cursor = await trino_conn.cursor()
        await cursor.execute(CREATE_SOURCE)

        # Week 1: Mon + Tue. Full refresh.
        await insert_trades(cursor, "2026-04-06", [("AAPL", "10:00:00", 150.0, 100)])
        await insert_trades(cursor, "2026-04-07", [("AAPL", "10:00:00", 160.0, 200)])
        result, value_cols = await setup_and_full_refresh(cursor, WEEKLY_VIEW, WEEKLY_TARGET)
        last_snap = result.current_snapshot

        # Week 2: Mon. Incremental refresh — advances state past week 1.
        await insert_trades(cursor, "2026-04-13", [("AAPL", "10:00:00", 200.0, 50)])
        result = await detect_changes(cursor, SOURCE_TABLE, "ts", "week", last_snap)
        assert result.action == RefreshAction.INCREMENTAL
        await execute_incremental_refresh(cursor, WEEKLY_VIEW, WEEKLY_TARGET, value_cols, result.filter_range)
        await write_last_snapshot(cursor, WEEKLY_TARGET, result.current_snapshot)
        last_snap = result.current_snapshot

        # Sanity: target now has both weekly bars
        await cursor.execute(f"SELECT week, volume FROM {WEEKLY_TARGET} WHERE symbol = 'AAPL' ORDER BY week")
        rows = await cursor.fetchall()
        assert len(rows) == 2
        assert rows[0][1] == 300.0
        assert rows[1][1] == 50.0

        # LATE-ARRIVING row: Wednesday Apr 8 — belongs to week 1, which
        # has already been processed and persisted.
        await insert_trades(cursor, "2026-04-08", [("AAPL", "10:00:00", 155.0, 50)])

        result = await detect_changes(cursor, SOURCE_TABLE, "ts", "week", last_snap)
        assert result.action == RefreshAction.INCREMENTAL
        # The detector must snap back to week 1 — Mon Apr 6 to Mon Apr 13.
        start, end = result.filter_range
        assert start.day == 6, f"late row should re-open week of Apr 6, got start={start}"
        assert end.day == 13,  f"late row should not extend into week 2, got end={end}"

        await execute_incremental_refresh(cursor, WEEKLY_VIEW, WEEKLY_TARGET, value_cols, result.filter_range)

        # Week 1 is recomputed (now 350); week 2 is untouched (still 50).
        await cursor.execute(f"SELECT week, volume, trade_count FROM {WEEKLY_TARGET} WHERE symbol = 'AAPL' ORDER BY week")
        rows = await cursor.fetchall()
        assert len(rows) == 2, f"expected 2 weekly bars, got {len(rows)}"
        assert rows[0][1] == 350.0, f"week 1 volume should be 350 (late row included), got {rows[0][1]}"
        assert rows[0][2] == 3,     f"week 1 trade_count should be 3, got {rows[0][2]}"
        assert rows[1][1] == 50.0,  f"week 2 should be untouched, got {rows[1][1]}"
        assert rows[1][2] == 1,     f"week 2 trade_count should be 1, got {rows[1][2]}"


class TestMonthlyBarsCrossPartition:
    """Monthly bars from daily-partitioned source."""

    async def test_incremental_refresh_reads_full_month(self, trino_conn):
        cursor = await trino_conn.cursor()
        await cursor.execute(CREATE_SOURCE)

        # Apr 1 and Apr 15
        await insert_trades(cursor, "2026-04-01", [("AAPL", "10:00:00", 100.0, 10)])
        await insert_trades(cursor, "2026-04-15", [("AAPL", "10:00:00", 200.0, 20)])

        result, value_cols = await setup_and_full_refresh(cursor, MONTHLY_VIEW, MONTHLY_TARGET)

        # Add Apr 20
        await insert_trades(cursor, "2026-04-20", [("AAPL", "10:00:00", 150.0, 5)])

        result = await detect_changes(cursor, SOURCE_TABLE, "ts", "month", result.current_snapshot)
        assert result.action == RefreshAction.INCREMENTAL
        start, end = result.filter_range
        assert start.month == 4 and start.day == 1
        assert end.month == 5 and end.day == 1

        await execute_incremental_refresh(cursor, MONTHLY_VIEW, MONTHLY_TARGET, value_cols, result.filter_range)

        await cursor.execute(f"SELECT volume, trade_count FROM {MONTHLY_TARGET} WHERE symbol = 'AAPL'")
        row = await cursor.fetchone()
        # All 3 trades: 10 + 20 + 5 = 35
        assert row[0] == 35.0, f"volume should be 35, got {row[0]}"
        assert row[1] == 3
