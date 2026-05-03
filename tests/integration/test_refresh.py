"""End-to-end integration tests for the MV orchestrator.

Requires: docker compose -f tests/docker-compose.yml up -d
"""
from __future__ import annotations

import pytest

from iceberg_ivm.config import ViewConfig
from iceberg_ivm.detector import RefreshAction, detect_changes
from iceberg_ivm.executor import execute_refresh
from iceberg_ivm.introspect import build_create_table_sql, discover_columns
from iceberg_ivm.query_parser import parse_view_query

pytestmark = [pytest.mark.integration, pytest.mark.xdist_group("integration")]

SOURCE_TABLE = "iceberg.test_schema.trades"
TARGET_TABLE = "iceberg.test_schema.ohlcv_1m"

CREATE_SOURCE = f"""
CREATE TABLE {SOURCE_TABLE} (
    symbol VARCHAR, ts TIMESTAMP(6) WITH TIME ZONE, price DOUBLE, quantity DOUBLE
) WITH (format = 'PARQUET', partitioning = ARRAY['day(ts)'])
"""

VIEW = ViewConfig(
    name="test_ohlcv",
    query=f"""
        SELECT symbol, date_trunc('minute', ts) AS minute,
               min_by(price, ts) AS open, max(price) AS high,
               min(price) AS low, max_by(price, ts) AS close,
               sum(quantity) AS volume, count(*) AS trade_count
        FROM {SOURCE_TABLE} GROUP BY 1, 2
    """,
    target_table=TARGET_TABLE, target_partitioning="ARRAY['day(minute)']",
)
PARSED = parse_view_query(VIEW.query)


async def insert_trades(cursor, day, trades):
    for sym, t, p, q in trades:
        await cursor.execute(
            f"INSERT INTO {SOURCE_TABLE} VALUES "
            f"('{sym}', TIMESTAMP '{day} {t} UTC', {p}, {q})"
        )


async def query_bars(cursor):
    await cursor.execute(f"SELECT symbol, minute, open, high, low, close, volume, trade_count FROM {TARGET_TABLE} ORDER BY symbol, minute")
    return [dict(zip(["symbol", "minute", "open", "high", "low", "close", "volume", "trade_count"], r)) for r in await cursor.fetchall()]


async def _value_cols(cursor) -> list[str]:
    cols = await discover_columns(cursor, VIEW.query)
    return [c.name for c in cols if c.name not in PARSED.merge_keys]


async def _drain(agen, *, stop_after: int | None = None) -> list:
    """Consume the async generator, optionally breaking after ``stop_after`` items."""
    items = []
    async for q in agen:
        items.append(q)
        if stop_after is not None and len(items) >= stop_after:
            break
    return items


class TestIntrospection:
    async def test_discover_columns(self, trino_conn):
        cursor = await trino_conn.cursor()
        await cursor.execute(CREATE_SOURCE)
        await insert_trades(cursor, "2026-04-08", [("AAPL", "09:30:00", 150.0, 100)])
        cols = await discover_columns(cursor, VIEW.query)
        assert len(cols) == 8
        assert "symbol" in [c.name for c in cols]


class TestFullRefresh:
    async def test_first_run(self, trino_conn):
        cursor = await trino_conn.cursor()
        await cursor.execute(CREATE_SOURCE)
        await insert_trades(cursor, "2026-04-08", [
            ("AAPL", "09:30:00", 150.0, 100),
            ("AAPL", "09:30:30", 151.0, 200),
            ("AAPL", "09:31:00", 149.0, 150),
        ])
        await insert_trades(cursor, "2026-04-09", [("AAPL", "09:30:00", 152.0, 100)])

        cols = await discover_columns(cursor, VIEW.query)
        value_cols = [c.name for c in cols if c.name not in PARSED.merge_keys]
        await cursor.execute(build_create_table_sql(TARGET_TABLE, cols, "ARRAY['day(minute)']"))

        result = await detect_changes(cursor, SOURCE_TABLE, "ts", "minute", last_snapshot=None)
        assert result.action == RefreshAction.FULL_REFRESH

        await _drain(execute_refresh(cursor, VIEW, TARGET_TABLE, PARSED, value_cols))
        bars = await query_bars(cursor)
        assert len(bars) == 3
        b = [b for b in bars if "09:30" in str(b["minute"])][0]
        assert b["high"] == 151.0
        assert b["volume"] == 300.0
        assert b["trade_count"] == 2


class TestIncrementalRefresh:
    async def test_new_day(self, trino_conn):
        cursor = await trino_conn.cursor()
        await cursor.execute(CREATE_SOURCE)
        await insert_trades(cursor, "2026-04-08", [("AAPL", "09:30:00", 150.0, 100)])

        value_cols = await _value_cols(cursor)
        cols = await discover_columns(cursor, VIEW.query)
        await cursor.execute(build_create_table_sql(TARGET_TABLE, cols, "ARRAY['day(minute)']"))
        await _drain(execute_refresh(cursor, VIEW, TARGET_TABLE, PARSED, value_cols))

        result = await detect_changes(cursor, SOURCE_TABLE, "ts", "minute", last_snapshot=None)

        await insert_trades(cursor, "2026-04-09", [("AAPL", "10:00:00", 155.0, 200)])

        result = await detect_changes(cursor, SOURCE_TABLE, "ts", "minute", last_snapshot=result.current_snapshot)
        assert result.action == RefreshAction.INCREMENTAL
        assert result.filter_range is not None

        await _drain(execute_refresh(
            cursor, VIEW, TARGET_TABLE, PARSED, value_cols,
            incremental_range=result.filter_range,
        ))
        assert len(await query_bars(cursor)) == 2

    async def test_same_day_update(self, trino_conn):
        cursor = await trino_conn.cursor()
        await cursor.execute(CREATE_SOURCE)
        await insert_trades(cursor, "2026-04-08", [("AAPL", "09:30:00", 150.0, 100)])

        value_cols = await _value_cols(cursor)
        cols = await discover_columns(cursor, VIEW.query)
        await cursor.execute(build_create_table_sql(TARGET_TABLE, cols, "ARRAY['day(minute)']"))
        await _drain(execute_refresh(cursor, VIEW, TARGET_TABLE, PARSED, value_cols))

        result = await detect_changes(cursor, SOURCE_TABLE, "ts", "minute", last_snapshot=None)

        await insert_trades(cursor, "2026-04-08", [("AAPL", "09:30:30", 160.0, 200)])

        result = await detect_changes(cursor, SOURCE_TABLE, "ts", "minute", last_snapshot=result.current_snapshot)
        assert result.action == RefreshAction.INCREMENTAL

        await _drain(execute_refresh(
            cursor, VIEW, TARGET_TABLE, PARSED, value_cols,
            incremental_range=result.filter_range,
        ))
        bars = await query_bars(cursor)
        assert len(bars) == 1
        assert bars[0]["high"] == 160.0
        assert bars[0]["volume"] == 300.0
        assert bars[0]["trade_count"] == 2


class TestNoChangeSkip:
    async def test_skip(self, trino_conn):
        cursor = await trino_conn.cursor()
        await cursor.execute(CREATE_SOURCE)
        await insert_trades(cursor, "2026-04-08", [("AAPL", "09:30:00", 150.0, 100)])
        result = await detect_changes(cursor, SOURCE_TABLE, "ts", "minute", last_snapshot=None)
        r2 = await detect_changes(cursor, SOURCE_TABLE, "ts", "minute", last_snapshot=result.current_snapshot)
        assert r2.action == RefreshAction.NO_CHANGE


CHUNKED_VIEW = ViewConfig(
    name="test_ohlcv_chunked",
    query=VIEW.query,
    target_table=TARGET_TABLE,
    target_partitioning="ARRAY['day(minute)']",
    full_refresh_chunk="day",
)


class TestChunkedFullRefresh:
    async def _seed(self, cursor) -> list[str]:
        """Seed three days of trades across two symbols and return the value
        columns for the chunked MERGE."""
        await cursor.execute(CREATE_SOURCE)
        await insert_trades(cursor, "2026-04-08", [
            ("AAPL", "09:30:00", 150.0, 100),
            ("AAPL", "09:30:30", 151.0, 200),
            ("MSFT", "10:00:00", 300.0, 50),
        ])
        await insert_trades(cursor, "2026-04-09", [
            ("AAPL", "09:30:00", 152.0, 100),
            ("MSFT", "10:00:00", 305.0, 75),
        ])
        await insert_trades(cursor, "2026-04-10", [
            ("AAPL", "09:31:00", 149.0, 150),
        ])
        cols = await discover_columns(cursor, VIEW.query)
        await cursor.execute(build_create_table_sql(TARGET_TABLE, cols, "ARRAY['day(minute)']"))
        return [c.name for c in cols if c.name not in PARSED.merge_keys]

    async def test_row_counts_match_non_chunked(self, trino_conn):
        """A chunked refresh must materialize the same target as a single-shot
        full refresh. Row counts and aggregate sums must be identical."""
        cursor = await trino_conn.cursor()
        value_cols = await self._seed(cursor)

        chunked_qs = await _drain(execute_refresh(
            cursor, CHUNKED_VIEW, TARGET_TABLE, PARSED, value_cols,
        ))
        assert all(q.stage == "chunk_merge" for q in chunked_qs)
        assert len(chunked_qs) == 3
        chunked_bars = await query_bars(cursor)

        await cursor.execute(f"DELETE FROM {TARGET_TABLE} WHERE true")
        single_qs = await _drain(execute_refresh(
            cursor, VIEW, TARGET_TABLE, PARSED, value_cols,
        ))
        assert len(single_qs) == 1
        assert single_qs[0].stage == "merge"
        single_bars = await query_bars(cursor)

        assert chunked_bars == single_bars

    async def test_interrupt_and_resume(self, trino_conn):
        """Breaking out of the generator after the first chunk must leave only
        that chunk's data in target. A second call must resume from target
        metadata and complete the remaining chunks without re-emitting the
        first."""
        cursor = await trino_conn.cursor()
        value_cols = await self._seed(cursor)

        first = await _drain(
            execute_refresh(cursor, CHUNKED_VIEW, TARGET_TABLE, PARSED, value_cols),
            stop_after=1,
        )
        assert len(first) == 1
        assert first[0].chunks_done == 1
        assert first[0].chunks_total == 3

        second = await _drain(execute_refresh(
            cursor, CHUNKED_VIEW, TARGET_TABLE, PARSED, value_cols,
        ))
        assert len(second) == 2

        resumed_bars = await query_bars(cursor)
        await cursor.execute(f"DELETE FROM {TARGET_TABLE} WHERE true")
        await _drain(execute_refresh(cursor, VIEW, TARGET_TABLE, PARSED, value_cols))
        reference = await query_bars(cursor)
        assert resumed_bars == reference

    async def test_replay_is_idempotent(self, trino_conn):
        """Running the same chunked refresh twice (simulating a committed-
        but-not-acked retry) must not duplicate rows: resume from target
        metadata sees everything already covered and emits zero chunks."""
        cursor = await trino_conn.cursor()
        value_cols = await self._seed(cursor)

        await _drain(execute_refresh(
            cursor, CHUNKED_VIEW, TARGET_TABLE, PARSED, value_cols,
        ))
        first = await query_bars(cursor)

        replay = await _drain(execute_refresh(
            cursor, CHUNKED_VIEW, TARGET_TABLE, PARSED, value_cols,
        ))
        assert replay == []
        second = await query_bars(cursor)
        assert first == second
