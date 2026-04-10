"""End-to-end integration tests for the MV orchestrator.

Requires: docker compose -f tests/docker-compose.yml up -d
"""
from __future__ import annotations

import pytest

from trino_mv_orchestrator.config import ViewConfig
from trino_mv_orchestrator.detector import RefreshAction, detect_changes
from trino_mv_orchestrator.executor import execute_full_refresh, execute_incremental_refresh
from trino_mv_orchestrator.introspect import discover_columns, discover_source_tables, build_create_table_sql
from trino_mv_orchestrator.state import read_last_snapshot, write_last_snapshot

pytestmark = [pytest.mark.integration, pytest.mark.xdist_group("integration")]

SOURCE_TABLE = "iceberg.test_schema.trades"
TARGET_TABLE = "iceberg.test_schema.ohlcv_1m"

CREATE_SOURCE = f"""
CREATE TABLE {SOURCE_TABLE} (
    symbol VARCHAR, ts TIMESTAMP(6) WITH TIME ZONE, price DOUBLE, quantity DOUBLE
) WITH (format = 'PARQUET', partitioning = ARRAY['day(ts)'])
"""

VIEW = ViewConfig(
    name="test_ohlcv", source_table=SOURCE_TABLE, filter_column="ts",
    filter_granularity="day",
    query=f"""
        SELECT symbol, date_trunc('minute', ts) AS minute,
               min_by(price, ts) AS open, max(price) AS high,
               min(price) AS low, max_by(price, ts) AS close,
               sum(quantity) AS volume, count(*) AS trade_count
        FROM {SOURCE_TABLE} WHERE {{range_filter}} GROUP BY 1, 2
    """,
    merge_keys=["symbol", "minute"],
    target_table=TARGET_TABLE, target_partitioning="ARRAY['day(minute)']",
)


def insert_trades(cursor, day, trades):
    for sym, t, p, q in trades:
        cursor.execute(
            f"INSERT INTO {SOURCE_TABLE} VALUES "
            f"('{sym}', TIMESTAMP '{day} {t} UTC', {p}, {q})"
        )


def query_bars(cursor):
    cursor.execute(f"SELECT symbol, minute, open, high, low, close, volume, trade_count FROM {TARGET_TABLE} ORDER BY symbol, minute")
    return [dict(zip(["symbol", "minute", "open", "high", "low", "close", "volume", "trade_count"], r)) for r in cursor.fetchall()]


class TestIntrospection:
    def test_discover_source_tables(self, trino_conn):
        cursor = trino_conn.cursor()
        cursor.execute(CREATE_SOURCE)
        insert_trades(cursor, "2026-04-08", [("AAPL", "09:30:00", 150.0, 100)])
        assert SOURCE_TABLE in discover_source_tables(cursor, VIEW.query)

    def test_discover_columns(self, trino_conn):
        cursor = trino_conn.cursor()
        cursor.execute(CREATE_SOURCE)
        insert_trades(cursor, "2026-04-08", [("AAPL", "09:30:00", 150.0, 100)])
        cols = discover_columns(cursor, VIEW.query)
        assert len(cols) == 8
        assert "symbol" in [c.name for c in cols]


class TestFullRefresh:
    def test_first_run(self, trino_conn):
        cursor = trino_conn.cursor()
        cursor.execute(CREATE_SOURCE)
        insert_trades(cursor, "2026-04-08", [
            ("AAPL", "09:30:00", 150.0, 100),
            ("AAPL", "09:30:30", 151.0, 200),
            ("AAPL", "09:31:00", 149.0, 150),
        ])
        insert_trades(cursor, "2026-04-09", [("AAPL", "09:30:00", 152.0, 100)])

        cols = discover_columns(cursor, VIEW.query)
        cursor.execute(build_create_table_sql(TARGET_TABLE, cols, "ARRAY['day(minute)']"))

        result = detect_changes(cursor, SOURCE_TABLE, "ts", "day", last_snapshot=None)
        assert result.action == RefreshAction.FULL_REFRESH

        execute_full_refresh(cursor, VIEW, TARGET_TABLE)
        bars = query_bars(cursor)
        assert len(bars) == 3
        b = [b for b in bars if "09:30" in str(b["minute"])][0]
        assert b["high"] == 151.0
        assert b["volume"] == 300.0
        assert b["trade_count"] == 2


class TestIncrementalRefresh:
    def test_new_day(self, trino_conn):
        cursor = trino_conn.cursor()
        cursor.execute(CREATE_SOURCE)
        insert_trades(cursor, "2026-04-08", [("AAPL", "09:30:00", 150.0, 100)])

        cols = discover_columns(cursor, VIEW.query)
        value_cols = [c.name for c in cols if c.name not in VIEW.merge_keys]
        cursor.execute(build_create_table_sql(TARGET_TABLE, cols, "ARRAY['day(minute)']"))
        execute_full_refresh(cursor, VIEW, TARGET_TABLE)

        result = detect_changes(cursor, SOURCE_TABLE, "ts", "day", last_snapshot=None)
        write_last_snapshot(cursor, TARGET_TABLE, result.current_snapshot)

        insert_trades(cursor, "2026-04-09", [("AAPL", "10:00:00", 155.0, 200)])

        result = detect_changes(cursor, SOURCE_TABLE, "ts", "day", last_snapshot=result.current_snapshot)
        assert result.action == RefreshAction.INCREMENTAL
        assert result.filter_range is not None

        execute_incremental_refresh(cursor, VIEW, TARGET_TABLE, value_cols, result.filter_range)
        assert len(query_bars(cursor)) == 2

    def test_same_day_update(self, trino_conn):
        cursor = trino_conn.cursor()
        cursor.execute(CREATE_SOURCE)
        insert_trades(cursor, "2026-04-08", [("AAPL", "09:30:00", 150.0, 100)])

        cols = discover_columns(cursor, VIEW.query)
        value_cols = [c.name for c in cols if c.name not in VIEW.merge_keys]
        cursor.execute(build_create_table_sql(TARGET_TABLE, cols, "ARRAY['day(minute)']"))
        execute_full_refresh(cursor, VIEW, TARGET_TABLE)

        result = detect_changes(cursor, SOURCE_TABLE, "ts", "day", last_snapshot=None)
        write_last_snapshot(cursor, TARGET_TABLE, result.current_snapshot)

        insert_trades(cursor, "2026-04-08", [("AAPL", "09:30:30", 160.0, 200)])

        result = detect_changes(cursor, SOURCE_TABLE, "ts", "day", last_snapshot=result.current_snapshot)
        assert result.action == RefreshAction.INCREMENTAL

        execute_incremental_refresh(cursor, VIEW, TARGET_TABLE, value_cols, result.filter_range)
        bars = query_bars(cursor)
        assert len(bars) == 1
        assert bars[0]["high"] == 160.0
        assert bars[0]["volume"] == 300.0
        assert bars[0]["trade_count"] == 2


class TestState:
    def test_roundtrip(self, trino_conn):
        cursor = trino_conn.cursor()
        cols = discover_columns(cursor, f"SELECT 1 AS x WHERE {{range_filter}}".replace("{range_filter}", "true"))
        cursor.execute(build_create_table_sql(TARGET_TABLE, [("x", "INTEGER")]))
        assert read_last_snapshot(cursor, TARGET_TABLE) is None
        write_last_snapshot(cursor, TARGET_TABLE, 12345)
        assert read_last_snapshot(cursor, TARGET_TABLE) == 12345


class TestNoChangeSkip:
    def test_skip(self, trino_conn):
        cursor = trino_conn.cursor()
        cursor.execute(CREATE_SOURCE)
        insert_trades(cursor, "2026-04-08", [("AAPL", "09:30:00", 150.0, 100)])
        result = detect_changes(cursor, SOURCE_TABLE, "ts", "day", last_snapshot=None)
        r2 = detect_changes(cursor, SOURCE_TABLE, "ts", "day", last_snapshot=result.current_snapshot)
        assert r2.action == RefreshAction.NO_CHANGE
