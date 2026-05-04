"""End-to-end test for chained materialized views (MV-on-MV).

Reproduces the scenario from issue #49: a downstream view whose source is
another MV's target. The upstream view writes its target via ``MERGE
INTO``, which Iceberg labels as an ``overwrite`` snapshot. Before the
fix, the downstream view's detector rejected ``overwrite`` and the chain
broke after the first refresh.

This test runs many cycles of (insert source rows → refresh upstream →
refresh downstream) and asserts both views stay healthy: no errors,
incremental refreshes after the initial full refresh, and the
downstream's row counts roll up consistently from the upstream's.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from iceberg_ivm.config import ViewConfig
from iceberg_ivm.server import refresh_view

from ._driver import Trade, insert_trades_batch

pytestmark = [pytest.mark.integration, pytest.mark.xdist_group("integration")]

SOURCE_TABLE = "iceberg.test_schema.trades"
MV1_TABLE = "iceberg.test_schema.ohlcv_1m"
MV2_TABLE = "iceberg.test_schema.ohlcv_1h"

CREATE_SOURCE = f"""
CREATE TABLE {SOURCE_TABLE} (
    symbol VARCHAR, ts TIMESTAMP(6) WITH TIME ZONE, price DOUBLE, quantity DOUBLE
) WITH (format = 'PARQUET', partitioning = ARRAY['day(ts)'])
"""

# Upstream view — minute bars from raw trades. The bucket column is
# aliased ``bucket`` (not ``minute``) because ``minute`` is a SQL
# reserved word the parser refuses as date_trunc's first arg, so a
# downstream chained MV could not reference it as a bare column.
MV1 = ViewConfig(
    name="ohlcv_1m",
    query=f"""
        SELECT symbol, date_trunc('minute', ts) AS bucket,
               sum(quantity) AS volume, count(*) AS trade_count
        FROM {SOURCE_TABLE} GROUP BY 1, 2
    """,
    target_table=MV1_TABLE,
    target_partitioning="ARRAY['day(bucket)']",
)

# Downstream view — hourly bars rolled up from MV1's minute bars.
# Source = MV1's target table. This is the chained MV configuration
# from issue #49.
MV2 = ViewConfig(
    name="ohlcv_1h",
    query=f"""
        SELECT symbol, date_trunc('hour', bucket) AS hour_bucket,
               sum(volume) AS volume, sum(trade_count) AS trade_count
        FROM {MV1_TABLE} GROUP BY 1, 2
    """,
    target_table=MV2_TABLE,
    target_partitioning="ARRAY['day(hour_bucket)']",
)


def _ts(date_str: str, time_str: str) -> datetime:
    return datetime.fromisoformat(f"{date_str} {time_str}").replace(tzinfo=timezone.utc)


async def _expected_hourly_rollup(cursor) -> list[tuple]:
    """Compute the expected hourly rollup straight from the source via Trino,
    bypassing both MVs. This is the oracle for the chained MV's contents.
    """
    await cursor.execute(
        f"SELECT symbol, date_trunc('hour', ts) AS hour_bucket, "
        f"       sum(quantity) AS volume, count(*) AS trade_count "
        f"FROM {SOURCE_TABLE} GROUP BY 1, 2 ORDER BY 1, 2"
    )
    return [tuple(r) for r in await cursor.fetchall()]


async def _fetch_hourly_target(cursor) -> list[tuple]:
    await cursor.execute(
        f"SELECT symbol, hour_bucket, volume, trade_count "
        f"FROM {MV2_TABLE} ORDER BY symbol, hour_bucket"
    )
    return [tuple(r) for r in await cursor.fetchall()]


class TestChainedMv:
    """A chained MV (downstream's source is another MV's target) must
    stay healthy across many refresh cycles.

    Pre-fix: cycle 1 succeeded (FULL_REFRESH, no snapshot history yet).
    Cycle 2 failed because MV1's target had grown an ``overwrite``
    snapshot from its MERGE refresh, which the downstream's detector
    rejected.

    Post-fix: ``overwrite`` is treated as a real data change, exactly
    like ``append``, so the downstream incrementally refreshes.
    """

    async def test_chain_runs_many_cycles_without_errors(self, trino_conn, app_state):
        cursor = await trino_conn.cursor()
        await cursor.execute(CREATE_SOURCE)

        # Cycle batches: each is (cycle_label, list[Trade]).
        # The first batch establishes initial state; the rest exercise
        # different update patterns: same-bucket, new-hour, new-day,
        # multiple symbols.
        batches: list[tuple[str, list[Trade]]] = [
            ("seed Apr 8 09:30 (one symbol)", [
                Trade("AAPL", _ts("2026-04-08", "09:30:00"), 150.0, 100),
                Trade("AAPL", _ts("2026-04-08", "09:30:30"), 151.0, 200),
            ]),
            ("Apr 8 09:31 (same hour)", [
                Trade("AAPL", _ts("2026-04-08", "09:31:00"), 152.0, 50),
            ]),
            ("Apr 8 10:00 (new hour)", [
                Trade("AAPL", _ts("2026-04-08", "10:00:00"), 153.0, 75),
                Trade("AAPL", _ts("2026-04-08", "10:30:00"), 154.0, 60),
            ]),
            ("multi-symbol on Apr 8 10:00", [
                Trade("MSFT", _ts("2026-04-08", "10:15:00"), 300.0, 40),
                Trade("GOOG", _ts("2026-04-08", "10:45:00"), 130.0, 20),
            ]),
            ("Apr 9 09:30 (new day)", [
                Trade("AAPL", _ts("2026-04-09", "09:30:00"), 160.0, 100),
                Trade("MSFT", _ts("2026-04-09", "09:30:00"), 310.0, 50),
            ]),
            ("late Apr 8 09:30 (backdated within bucket)", [
                Trade("AAPL", _ts("2026-04-08", "09:30:45"), 150.5, 25),
            ]),
            ("Apr 9 11:00 across two symbols", [
                Trade("AAPL", _ts("2026-04-09", "11:00:00"), 161.0, 80),
                Trade("GOOG", _ts("2026-04-09", "11:30:00"), 132.0, 30),
            ]),
        ]

        for i, (label, rows) in enumerate(batches, start=1):
            await insert_trades_batch(cursor, SOURCE_TABLE, rows)

            # Refresh upstream first, then downstream — that's the order
            # iceberg-ivm's daemon would tick them in.
            await refresh_view(app_state, MV1)
            await refresh_view(app_state, MV2)

            vs1 = app_state.view_statuses[MV1.name]
            vs2 = app_state.view_statuses[MV2.name]
            assert vs1.last_error is None, (
                f"cycle {i} ({label}): upstream MV1 errored: {vs1.last_error!r}"
            )
            assert vs2.last_error is None, (
                f"cycle {i} ({label}): downstream MV2 errored: {vs2.last_error!r}"
            )
            # First cycle: both views do a full refresh. Every subsequent
            # cycle must be incremental — that's the regression guard for
            # issue #49 (overwrite snapshots from MV1 must drive MV2's
            # incremental path, not raise UnexpectedOperationError).
            if i == 1:
                assert vs1.last_action == "full"
                assert vs2.last_action == "full"
            else:
                assert vs1.last_action == "incremental", (
                    f"cycle {i} ({label}): MV1 last_action={vs1.last_action!r}"
                )
                assert vs2.last_action == "incremental", (
                    f"cycle {i} ({label}): MV2 last_action={vs2.last_action!r} — "
                    f"this is exactly the issue #49 regression if it's anything but 'incremental'"
                )

            # The downstream target must match a fresh-from-source rollup.
            actual = await _fetch_hourly_target(cursor)
            expected = await _expected_hourly_rollup(cursor)
            assert actual == expected, (
                f"cycle {i} ({label}): downstream MV diverged from oracle\n"
                f"  expected: {expected}\n"
                f"  actual:   {actual}"
            )

        # After all cycles, total_errors must still be 0 on both views.
        assert app_state.view_statuses[MV1.name].total_errors == 0
        assert app_state.view_statuses[MV2.name].total_errors == 0
