"""Tests for cross-partition GROUP BY correctness.

Validates that when the GROUP BY granularity is coarser than the source
partition granularity, the file-stats-based detector correctly snaps the
range to complete GROUP BY buckets.
"""
from __future__ import annotations

import pytest

from trino_mv_orchestrator.config import ViewConfig
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
    filter_granularity="week",
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
    filter_granularity="month",
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


def insert_trades(cursor, day, trades):
    for sym, t, p, q in trades:
        cursor.execute(
            f"INSERT INTO {SOURCE_TABLE} VALUES "
            f"('{sym}', TIMESTAMP '{day} {t} UTC', {p}, {q})"
        )


def setup_and_full_refresh(cursor, view, target):
    cols = discover_columns(cursor, view.query)
    value_cols = [c.name for c in cols if c.name not in view.merge_keys]
    cursor.execute(build_create_table_sql(target, cols, view.target_partitioning))
    execute_full_refresh(cursor, view, target)
    result = detect_changes(cursor, SOURCE_TABLE, "ts", view.filter_granularity, last_snapshot=None)
    write_last_snapshot(cursor, target, result.current_snapshot)
    return result, value_cols


class TestWeeklyBarsCrossPartition:
    """Weekly bars from daily-partitioned source.

    The GROUP BY (week) spans 7 source partitions (days). When new data
    arrives for Wednesday, the detector must snap the range to the full
    week (Mon-Sun) so the MERGE recomputes the weekly bar correctly.
    """

    def test_incremental_refresh_preserves_all_days(self, trino_conn):
        cursor = trino_conn.cursor()
        cursor.execute(CREATE_SOURCE)

        # Monday + Tuesday trades (same week, 2026-04-06 is Monday)
        insert_trades(cursor, "2026-04-06", [("AAPL", "10:00:00", 150.0, 100)])
        insert_trades(cursor, "2026-04-07", [("AAPL", "10:00:00", 160.0, 200)])

        result, value_cols = setup_and_full_refresh(cursor, WEEKLY_VIEW, WEEKLY_TARGET)
        last_snap = result.current_snapshot

        # Verify full refresh is correct
        cursor.execute(f"SELECT volume, trade_count FROM {WEEKLY_TARGET} WHERE symbol = 'AAPL'")
        row = cursor.fetchone()
        assert row[0] == 300.0
        assert row[1] == 2

        # Add Wednesday trade
        insert_trades(cursor, "2026-04-08", [("AAPL", "10:00:00", 155.0, 50)])

        result = detect_changes(cursor, SOURCE_TABLE, "ts", "week", last_snap)
        assert result.action == RefreshAction.INCREMENTAL
        # Range should cover the full week (Mon-Sun), not just Wednesday
        start, end = result.filter_range
        assert start.day == 6   # Monday
        assert end.day == 13    # Next Monday (exclusive)

        execute_incremental_refresh(cursor, WEEKLY_VIEW, WEEKLY_TARGET, value_cols, result.filter_range)

        cursor.execute(f"SELECT volume, trade_count, high, low FROM {WEEKLY_TARGET} WHERE symbol = 'AAPL'")
        row = cursor.fetchone()
        assert row[0] == 350.0, f"volume should be 350, got {row[0]}"
        assert row[1] == 3, f"trade_count should be 3, got {row[1]}"
        assert row[2] == 160.0, f"high should be 160, got {row[2]}"
        assert row[3] == 150.0, f"low should be 150, got {row[3]}"

    def test_new_data_in_next_week(self, trino_conn):
        """New data in a different week should not affect the previous week."""
        cursor = trino_conn.cursor()
        cursor.execute(CREATE_SOURCE)

        # Week 1: Mon Apr 6
        insert_trades(cursor, "2026-04-06", [("AAPL", "10:00:00", 150.0, 100)])
        result, value_cols = setup_and_full_refresh(cursor, WEEKLY_VIEW, WEEKLY_TARGET)

        # Week 2: Mon Apr 13
        insert_trades(cursor, "2026-04-13", [("AAPL", "10:00:00", 200.0, 50)])

        result = detect_changes(cursor, SOURCE_TABLE, "ts", "week", result.current_snapshot)
        assert result.action == RefreshAction.INCREMENTAL
        start, end = result.filter_range
        assert start.day == 13  # Monday of week 2
        assert end.day == 20    # Next Monday

        execute_incremental_refresh(cursor, WEEKLY_VIEW, WEEKLY_TARGET, value_cols, result.filter_range)

        cursor.execute(f"SELECT week, volume FROM {WEEKLY_TARGET} WHERE symbol = 'AAPL' ORDER BY week")
        rows = cursor.fetchall()
        assert len(rows) == 2
        assert rows[0][1] == 100.0  # Week 1 untouched
        assert rows[1][1] == 50.0   # Week 2 new


class TestMonthlyBarsCrossPartition:
    """Monthly bars from daily-partitioned source."""

    def test_incremental_refresh_reads_full_month(self, trino_conn):
        cursor = trino_conn.cursor()
        cursor.execute(CREATE_SOURCE)

        # Apr 1 and Apr 15
        insert_trades(cursor, "2026-04-01", [("AAPL", "10:00:00", 100.0, 10)])
        insert_trades(cursor, "2026-04-15", [("AAPL", "10:00:00", 200.0, 20)])

        result, value_cols = setup_and_full_refresh(cursor, MONTHLY_VIEW, MONTHLY_TARGET)

        # Add Apr 20
        insert_trades(cursor, "2026-04-20", [("AAPL", "10:00:00", 150.0, 5)])

        result = detect_changes(cursor, SOURCE_TABLE, "ts", "month", result.current_snapshot)
        assert result.action == RefreshAction.INCREMENTAL
        start, end = result.filter_range
        assert start.month == 4 and start.day == 1
        assert end.month == 5 and end.day == 1

        execute_incremental_refresh(cursor, MONTHLY_VIEW, MONTHLY_TARGET, value_cols, result.filter_range)

        cursor.execute(f"SELECT volume, trade_count FROM {MONTHLY_TARGET} WHERE symbol = 'AAPL'")
        row = cursor.fetchone()
        # All 3 trades: 10 + 20 + 5 = 35
        assert row[0] == 35.0, f"volume should be 35, got {row[0]}"
        assert row[1] == 3
