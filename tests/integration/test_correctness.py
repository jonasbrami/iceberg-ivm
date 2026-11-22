"""End-to-end correctness against real Trino: after every refresh the target
must equal the view query run directly on the source."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from iceberg_ivm.config import ViewConfig
from iceberg_ivm.server import refresh_view

from ._driver import Trade, insert_trades_batch

pytestmark = [pytest.mark.integration, pytest.mark.xdist_group("integration")]

SOURCE = "iceberg.test_schema.trades"
TARGET = "iceberg.test_schema.ohlcv_1m"


def ts(day: int, hh_mm_ss: str) -> datetime:
    return datetime.fromisoformat(f"2026-04-{day:02d} {hh_mm_ss}").replace(tzinfo=UTC)


def view(query: str, name: str = "v") -> ViewConfig:
    return ViewConfig(name=name, query=query.format(src=SOURCE), target_table=TARGET)


async def create_source(cursor) -> None:
    await cursor.execute(
        f"CREATE TABLE {SOURCE} (symbol VARCHAR, ts TIMESTAMP(6) WITH TIME ZONE, price DOUBLE, quantity DOUBLE) "
        "WITH (partitioning = ARRAY['day(ts)'])"
    )


async def assert_target_matches_query(cursor, v: ViewConfig) -> None:
    await cursor.execute(f"SELECT * FROM ({v.query}) ORDER BY 1, 2")
    expected = await cursor.fetchall()
    await cursor.execute(f"SELECT * FROM {TARGET} ORDER BY 1, 2")
    assert await cursor.fetchall() == expected


async def refresh(app_state, v: ViewConfig) -> str:
    await refresh_view(app_state, v)
    status = app_state.view_statuses[v.name]
    assert status.last_error is None, status.last_error
    return status.last_action


async def test_null_group_keys_are_updated_not_duplicated(trino_conn, app_state):
    cursor = await trino_conn.cursor()
    await create_source(cursor)
    v = view(
        "SELECT date_trunc('minute', ts) AS minute, symbol, count(*) AS n, sum(quantity) AS qty "
        "FROM {src} GROUP BY 1, 2"
    )
    await cursor.execute(f"INSERT INTO {SOURCE} VALUES (NULL, TIMESTAMP '2026-04-09 10:00:01 UTC', 1, 1)")
    assert await refresh(app_state, v) == "full"
    for second in ("02", "03"):
        await cursor.execute(f"INSERT INTO {SOURCE} VALUES (NULL, TIMESTAMP '2026-04-09 10:00:{second} UTC', 1, 1)")
        assert await refresh(app_state, v) == "incremental"
    await assert_target_matches_query(cursor, v)


async def test_groups_that_drop_out_of_having_are_deleted(trino_conn, app_state):
    cursor = await trino_conn.cursor()
    await create_source(cursor)
    v = view(
        "SELECT date_trunc('minute', ts) AS minute, symbol, count(*) AS n FROM {src} GROUP BY 1, 2 HAVING count(*) < 3"
    )
    await insert_trades_batch(
        cursor, SOURCE, [Trade("A", ts(9, "10:00:01"), 1, 1), Trade("B", ts(9, "10:00:02"), 1, 1)]
    )
    await refresh(app_state, v)
    await insert_trades_batch(
        cursor, SOURCE, [Trade("A", ts(9, "10:00:03"), 1, 1), Trade("A", ts(9, "10:00:04"), 1, 1)]
    )
    assert await refresh(app_state, v) == "incremental"
    await assert_target_matches_query(cursor, v)  # A now has 3 rows → gone; B stays


async def test_deleting_source_rows_triggers_a_correct_full_refresh(trino_conn, app_state):
    cursor = await trino_conn.cursor()
    await create_source(cursor)
    v = view("SELECT date_trunc('hour', ts) AS hour, symbol, sum(quantity) AS qty FROM {src} GROUP BY 1, 2")
    await insert_trades_batch(
        cursor, SOURCE, [Trade("A", ts(8, "10:00:00"), 1, 5), Trade("A", ts(9, "11:00:00"), 1, 7)]
    )
    await refresh(app_state, v)
    await cursor.execute(f"DELETE FROM {SOURCE} WHERE ts < TIMESTAMP '2026-04-09 00:00:00 UTC'")
    assert await refresh(app_state, v) == "full"
    await assert_target_matches_query(cursor, v)


async def test_recreated_view_on_an_existing_target_leaves_no_stale_rows(trino_conn, app_state):
    cursor = await trino_conn.cursor()
    await create_source(cursor)
    await insert_trades_batch(
        cursor, SOURCE, [Trade("A", ts(9, "10:00:00"), 1, 1), Trade("B", ts(9, "10:00:30"), 1, 1)]
    )
    first = view(
        "SELECT date_trunc('minute', ts) AS minute, symbol, count(*) AS n FROM {src} WHERE symbol = 'A' GROUP BY 1, 2"
    )
    await refresh(app_state, first)
    await app_state.history.delete_view(first.name)  # what DELETE /api/views/{name} does
    second = view(
        "SELECT date_trunc('minute', ts) AS minute, symbol, count(*) AS n FROM {src} WHERE symbol = 'B' GROUP BY 1, 2"
    )
    assert await refresh(app_state, second) == "full"
    await assert_target_matches_query(cursor, second)  # no leftover 'A' row
