"""Streaming-update integration tests via ``refresh_view``.

Each test scripts a deterministic sequence of cycles. Per cycle:
  1. Insert a fixed batch of rows (or run optimize) on the source.
  2. Call ``server.refresh_view(s, view)`` — the daemon's per-tick
     driver — which auto-manages snapshot state and target creation.
  3. Assert ``vs.last_action`` matches the expected transition.
  4. Assert target rows match the Python-side OHLCV oracle, row-for-row.

These tests cover gaps in the existing integration suite:
  * No prior test loops more than 2 incremental cycles.
  * No prior test goes through ``refresh_view`` (all bypass it and
    drive ``execute_refresh`` directly with manual state mgmt).
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from iceberg_ivm.config import ViewConfig
from iceberg_ivm.server import refresh_view

from ._driver import Cycle, Trade, fetch_target_rows, insert_trades_batch
from ._oracle import OhlcvOracle

pytestmark = [pytest.mark.integration, pytest.mark.xdist_group("integration")]

SOURCE_TABLE = "iceberg.test_schema.trades"
TARGET_TABLE = "iceberg.test_schema.streaming_ohlcv"

CREATE_SOURCE = f"""
CREATE TABLE {SOURCE_TABLE} (
    symbol VARCHAR, ts TIMESTAMP(6) WITH TIME ZONE, price DOUBLE, quantity DOUBLE
) WITH (format = 'PARQUET', partitioning = ARRAY['day(ts)'])
"""

VIEW = ViewConfig(
    name="streaming_ohlcv",
    query=f"""
        SELECT symbol, date_trunc('minute', ts) AS minute,
               min_by(price, ts) AS open, max(price) AS high,
               min(price) AS low, max_by(price, ts) AS close,
               sum(quantity) AS volume, count(*) AS trade_count
        FROM {SOURCE_TABLE} GROUP BY 1, 2
    """,
    target_table=TARGET_TABLE,
    target_partitioning="ARRAY['day(minute)']",
)


def _ts(date_str: str, time_str: str) -> datetime:
    """Helper: build a UTC-aware datetime from 'YYYY-MM-DD' + 'HH:MM:SS'."""
    return datetime.fromisoformat(f"{date_str} {time_str}").replace(tzinfo=UTC)


async def _run_scenario(trino_conn, app_state, scenario: list[Cycle]) -> None:
    """Drive a scripted scenario through ``refresh_view``.

    Asserts after every cycle so the failure message names the
    offending cycle's ``note``.
    """
    cursor = await trino_conn.cursor()
    await cursor.execute(CREATE_SOURCE)

    oracle = OhlcvOracle()
    expected_total_refreshes = 0

    for i, cycle in enumerate(scenario, start=1):
        label = f"cycle {i} ({cycle.note})"

        if cycle.compact:
            await cursor.execute(f"ALTER TABLE {SOURCE_TABLE} EXECUTE optimize")
        else:
            await insert_trades_batch(cursor, SOURCE_TABLE, cycle.rows)
            for r in cycle.rows:
                oracle.update(r.symbol, r.ts, r.price, r.quantity)

        await refresh_view(app_state, VIEW)

        vs = app_state.view_statuses[VIEW.name]
        assert vs.last_error is None, f"{label}: unexpected error {vs.last_error!r}"
        assert vs.last_action == cycle.expect_action, (
            f"{label}: expected last_action={cycle.expect_action!r}, got {vs.last_action!r}"
        )

        # total_refreshes ticks on full+incremental, NOT on skip (server.py:323-329 vs :379).
        if cycle.expect_action != "skip":
            expected_total_refreshes += 1
        assert vs.total_refreshes == expected_total_refreshes, (
            f"{label}: total_refreshes={vs.total_refreshes}, expected {expected_total_refreshes}"
        )
        assert vs.total_errors == 0, f"{label}: total_errors={vs.total_errors}"

        actual = await fetch_target_rows(cursor, TARGET_TABLE)
        expected = oracle.expected_rows()
        assert actual == expected, (
            f"{label}: target rows diverged from oracle\n  expected: {expected}\n  actual:   {actual}"
        )


# ── Test 1: Sustained stream ────────────────────────────────────────────


class TestSustainedStream:
    """10 cycles of single-symbol updates. The first creates the target;
    the remaining 9 are incremental. After every cycle the target must
    match the oracle row-for-row, and ``vs.total_refreshes`` must equal
    the cycle index.

    Validates that snapshot bookkeeping (the SQLite-backed
    ``last_source_snapshot`` bookmark on ``view_status``) advances
    correctly across many ticks and
    that no cycle drops or duplicates rows.
    """

    async def test_ten_cycles(self, trino_conn, app_state):
        scenario = [
            Cycle(
                rows=[
                    Trade("AAPL", _ts("2026-04-08", "09:30:00"), 150.0, 100),
                    Trade("AAPL", _ts("2026-04-08", "09:30:30"), 151.0, 200),
                    Trade("AAPL", _ts("2026-04-08", "09:31:00"), 149.0, 150),
                ],
                expect_action="full",
                note="initial 3 trades, target created",
            ),
            Cycle(
                rows=[
                    Trade("AAPL", _ts("2026-04-08", "09:32:00"), 150.5, 100),
                    Trade("AAPL", _ts("2026-04-08", "09:32:30"), 152.0, 200),
                ],
                note="add 09:32 minute",
            ),
            Cycle(
                rows=[
                    Trade("AAPL", _ts("2026-04-08", "09:30:15"), 150.5, 50),
                ],
                note="late-within-minute, updates 09:30 bar",
            ),
            Cycle(
                rows=[
                    Trade("AAPL", _ts("2026-04-08", "09:33:00"), 153.0, 75),
                    Trade("AAPL", _ts("2026-04-08", "09:33:45"), 152.5, 25),
                ],
                note="add 09:33 minute",
            ),
            Cycle(
                rows=[
                    Trade("AAPL", _ts("2026-04-09", "09:30:00"), 154.0, 100),
                ],
                note="new day Apr 9",
            ),
            Cycle(
                rows=[
                    Trade("AAPL", _ts("2026-04-09", "09:31:00"), 155.0, 200),
                    Trade("AAPL", _ts("2026-04-09", "09:31:30"), 154.5, 50),
                ],
                note="add 09:31 on Apr 9",
            ),
            Cycle(
                rows=[
                    Trade("AAPL", _ts("2026-04-09", "09:30:15"), 153.5, 25),
                ],
                note="updates Apr 9 09:30 bar",
            ),
            Cycle(
                rows=[
                    Trade("AAPL", _ts("2026-04-08", "09:34:00"), 156.0, 50),
                    Trade("AAPL", _ts("2026-04-08", "09:34:15"), 157.0, 100),
                    Trade("AAPL", _ts("2026-04-08", "09:34:30"), 156.5, 75),
                ],
                note="back to Apr 8 09:34",
            ),
            Cycle(
                rows=[
                    Trade("AAPL", _ts("2026-04-09", "10:00:00"), 158.0, 200),
                ],
                note="single trade Apr 9 10:00",
            ),
            Cycle(
                rows=[
                    Trade("AAPL", _ts("2026-04-08", "09:35:00"), 155.5, 60),
                    Trade("AAPL", _ts("2026-04-09", "10:00:30"), 159.0, 80),
                ],
                note="cross-day batch",
            ),
        ]
        await _run_scenario(trino_conn, app_state, scenario)


# ── Test 2: Multiple symbols ─────────────────────────────────────────────


class TestMultiSymbol:
    """8 cycles across 3 symbols. Each cycle declares which symbols
    receive a row and at which timestamps. Catches merge-key bugs that
    drop or duplicate rows for a subset of symbols.
    """

    async def test_three_symbols_eight_cycles(self, trino_conn, app_state):
        scenario = [
            Cycle(
                rows=[
                    Trade("AAPL", _ts("2026-04-08", "09:30:00"), 150.0, 100),
                    Trade("MSFT", _ts("2026-04-08", "09:30:00"), 300.0, 50),
                    Trade("GOOG", _ts("2026-04-08", "09:30:00"), 130.0, 30),
                ],
                expect_action="full",
                note="all three symbols open",
            ),
            Cycle(
                rows=[
                    Trade("AAPL", _ts("2026-04-08", "09:30:30"), 151.0, 200),
                ],
                note="AAPL only",
            ),
            Cycle(
                rows=[
                    Trade("MSFT", _ts("2026-04-08", "09:31:00"), 301.0, 75),
                    Trade("GOOG", _ts("2026-04-08", "09:31:00"), 131.0, 25),
                ],
                note="MSFT + GOOG, no AAPL",
            ),
            Cycle(
                rows=[
                    Trade("AAPL", _ts("2026-04-08", "09:31:00"), 149.5, 150),
                    Trade("MSFT", _ts("2026-04-08", "09:31:30"), 300.5, 100),
                ],
                note="AAPL + MSFT update existing minute",
            ),
            Cycle(
                rows=[
                    Trade("GOOG", _ts("2026-04-08", "09:32:00"), 132.0, 40),
                ],
                note="GOOG only, new minute",
            ),
            Cycle(
                rows=[
                    Trade("AAPL", _ts("2026-04-09", "09:30:00"), 152.0, 100),
                    Trade("MSFT", _ts("2026-04-09", "09:30:00"), 302.0, 60),
                    Trade("GOOG", _ts("2026-04-09", "09:30:00"), 133.0, 35),
                ],
                note="all three on new day",
            ),
            Cycle(
                rows=[
                    Trade("AAPL", _ts("2026-04-09", "09:30:15"), 152.5, 50),
                    Trade("GOOG", _ts("2026-04-08", "09:30:30"), 130.5, 20),
                ],
                note="AAPL on-time + GOOG late within Apr 8 09:30",
            ),
            Cycle(
                rows=[
                    Trade("MSFT", _ts("2026-04-09", "09:31:00"), 303.0, 80),
                ],
                note="MSFT only on Apr 9",
            ),
        ]
        await _run_scenario(trino_conn, app_state, scenario)


# ── Test 3: Late arrivals ───────────────────────────────────────────────


class TestLateArrivals:
    """9 cycles with scripted backdating.

    Cycles 1-4 advance through Apr 8 → Apr 11. Cycle 5 inserts a row
    dated Apr 9 (backdated by 2 days from cycle 4's Apr 11). The
    detector's range-snap must re-open the Apr 9 minute bucket, and the
    MERGE must update only that bucket — Apr 8/10/11 bars must remain
    unchanged.
    """

    async def test_backdated_inserts(self, trino_conn, app_state):
        scenario = [
            Cycle(
                rows=[
                    Trade("AAPL", _ts("2026-04-08", "10:00:00"), 100.0, 10),
                ],
                expect_action="full",
                note="Apr 8",
            ),
            Cycle(
                rows=[
                    Trade("AAPL", _ts("2026-04-09", "10:00:00"), 110.0, 20),
                ],
                note="Apr 9",
            ),
            Cycle(
                rows=[
                    Trade("AAPL", _ts("2026-04-10", "10:00:00"), 120.0, 30),
                ],
                note="Apr 10",
            ),
            Cycle(
                rows=[
                    Trade("AAPL", _ts("2026-04-11", "10:00:00"), 130.0, 40),
                ],
                note="Apr 11",
            ),
            Cycle(
                rows=[
                    Trade("AAPL", _ts("2026-04-09", "10:00:30"), 111.0, 5),
                ],
                note="LATE: backdated to Apr 9 (2 days late)",
            ),
            Cycle(
                rows=[
                    Trade("AAPL", _ts("2026-04-12", "10:00:00"), 140.0, 50),
                ],
                note="Apr 12 on-time",
            ),
            Cycle(
                rows=[
                    Trade("AAPL", _ts("2026-04-08", "10:00:30"), 99.0, 7),
                ],
                note="LATE: backdated to Apr 8 (4 days late)",
            ),
            Cycle(
                rows=[
                    Trade("AAPL", _ts("2026-04-13", "10:00:00"), 150.0, 60),
                ],
                note="Apr 13 on-time",
            ),
            Cycle(
                rows=[
                    Trade("AAPL", _ts("2026-04-10", "10:00:30"), 121.0, 8),
                    Trade("AAPL", _ts("2026-04-13", "10:00:30"), 151.0, 12),
                ],
                note="mixed: late Apr 10 + on-time Apr 13",
            ),
        ]
        await _run_scenario(trino_conn, app_state, scenario)


# ── Test 4: Compaction interleaved ──────────────────────────────────────


class TestCompactionInterleaved:
    """Compaction snapshots (op = ``replace``) must NOT trigger an
    incremental refresh — the detector returns NO_CHANGE and
    ``refresh_view`` must:

      * Set ``vs.last_action = "skip"``.
      * Advance the persisted bookmark past the compaction snapshot
        so the next tick starts from there.
      * Leave target rows byte-identical to before the optimize.

    Then a subsequent append must refresh incrementally and pick up
    only the new rows.
    """

    async def test_optimize_between_appends(self, trino_conn, app_state):
        scenario = [
            Cycle(
                rows=[
                    Trade("AAPL", _ts("2026-04-08", "09:30:00"), 150.0, 100),
                    Trade("AAPL", _ts("2026-04-08", "09:30:30"), 151.0, 200),
                ],
                expect_action="full",
                note="initial appends",
            ),
            Cycle(
                rows=[
                    Trade("AAPL", _ts("2026-04-08", "09:31:00"), 152.0, 50),
                    Trade("AAPL", _ts("2026-04-08", "09:31:30"), 153.0, 75),
                ],
                note="more appends",
            ),
            Cycle(compact=True, expect_action="skip", note="optimize → skip"),
            Cycle(
                rows=[
                    Trade("AAPL", _ts("2026-04-08", "09:32:00"), 154.0, 80),
                ],
                note="append after optimize",
            ),
            Cycle(compact=True, expect_action="skip", note="optimize again → skip"),
            Cycle(
                rows=[
                    Trade("AAPL", _ts("2026-04-09", "09:30:00"), 160.0, 100),
                ],
                note="append on new day",
            ),
            Cycle(
                rows=[
                    Trade("AAPL", _ts("2026-04-08", "09:32:30"), 154.5, 25),
                ],
                note="late append on Apr 8",
            ),
        ]

        cursor = await trino_conn.cursor()
        await cursor.execute(CREATE_SOURCE)

        oracle = OhlcvOracle()
        prev_target_rows: list[dict] | None = None
        prev_snapshot: int | None = None
        expected_total_refreshes = 0

        for i, cycle in enumerate(scenario, start=1):
            label = f"cycle {i} ({cycle.note})"

            if cycle.compact:
                await cursor.execute(f"ALTER TABLE {SOURCE_TABLE} EXECUTE optimize")
            else:
                await insert_trades_batch(cursor, SOURCE_TABLE, cycle.rows)
                for r in cycle.rows:
                    oracle.update(r.symbol, r.ts, r.price, r.quantity)

            await refresh_view(app_state, VIEW)

            vs = app_state.view_statuses[VIEW.name]
            assert vs.last_error is None, f"{label}: error {vs.last_error!r}"
            assert vs.last_action == cycle.expect_action, (
                f"{label}: last_action={vs.last_action!r}, expected {cycle.expect_action!r}"
            )

            if cycle.expect_action != "skip":
                expected_total_refreshes += 1
            assert vs.total_refreshes == expected_total_refreshes

            actual = await fetch_target_rows(cursor, TARGET_TABLE)

            if cycle.compact:
                # Skip cycles must leave target byte-identical to the previous cycle.
                assert actual == prev_target_rows, f"{label}: optimize altered the target; expected unchanged"
                # …and must advance the persisted source snapshot past the optimize op.
                cur_snap = await app_state.history.get_last_source_snapshot(VIEW.name)
                assert cur_snap is not None and (prev_snapshot is None or cur_snap != prev_snapshot), (
                    f"{label}: snapshot bookmark did not advance (prev={prev_snapshot}, cur={cur_snap})"
                )
                prev_snapshot = cur_snap
            else:
                expected = oracle.expected_rows()
                assert actual == expected, (
                    f"{label}: target diverged from oracle\n  expected: {expected}\n  actual:   {actual}"
                )
                prev_snapshot = await app_state.history.get_last_source_snapshot(VIEW.name)

            prev_target_rows = actual
