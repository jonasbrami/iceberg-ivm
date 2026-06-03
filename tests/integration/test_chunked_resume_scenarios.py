"""Scenario-based END-TO-END validation of PR #65 (chunked, snapshot-pinned,
resumable refresh) against a REAL Trino + Iceberg stack.

Design under test (server.refresh_view + executor.execute_refresh):
  * A run PINS the source's current snapshot ``M`` at start and derives its
    window over the frozen ``(bookmark, M]``.
  * The window is split into idempotent per-chunk MERGEs, each reading the
    source ``FOR VERSION AS OF M``.
  * ``current_work = (M, last_committed_chunk_end)`` is persisted after every
    chunk commit; the bookmark advances to M ONLY on clean completion (which
    also clears ``current_work``).
  * Resume reuses the SAME pinned M (never re-detects).
  * If M (or, for an incremental, the bookmark) has expired → discard
    ``current_work`` and recompute full-from-scratch.

Each scenario drives the real ``server.refresh_view`` against seeded Iceberg
tables, inspects ``state.db`` directly, and compares the target to a
from-scratch GROUP BY recompute oracle (run in Trino).

Interruption is injected deterministically by wrapping ``execute_refresh`` so
``s.stop_event`` is set after the Nth committed chunk yields. ``refresh_view``
checks ``stop_event`` after each chunk commit and returns gracefully with
``current_work`` intact (server.py "Graceful shutdown mid-run").
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

import iceberg_ivm.server as server_mod
from iceberg_ivm.config import ViewConfig
from iceberg_ivm.detector import get_current_snapshot, snapshot_exists
from iceberg_ivm.query_parser import parse_view_query
from iceberg_ivm.server import refresh_view

pytestmark = [pytest.mark.integration, pytest.mark.xdist_group("integration")]

SOURCE_TABLE = "iceberg.test_schema.trades"
TARGET_TABLE = "iceberg.test_schema.ohlcv_1m"

CREATE_SOURCE = f"""
CREATE TABLE {SOURCE_TABLE} (
    symbol VARCHAR, ts TIMESTAMP(6) WITH TIME ZONE, price DOUBLE, quantity DOUBLE
) WITH (format = 'PARQUET', partitioning = ARRAY['day(ts)'])
"""

# full_refresh_chunk='day' → one chunk per calendar day of seeded data.
VIEW = ViewConfig(
    name="test_ohlcv",
    query=f"""
        SELECT symbol, date_trunc('minute', ts) AS minute,
               min_by(price, ts) AS open, max(price) AS high,
               min(price) AS low, max_by(price, ts) AS close,
               sum(quantity) AS volume, count(*) AS trade_count
        FROM {SOURCE_TABLE} GROUP BY 1, 2
    """,
    target_table=TARGET_TABLE,
    target_partitioning="ARRAY['day(minute)']",
    full_refresh_chunk="day",
)
PARSED = parse_view_query(VIEW.query)

ORACLE_QUERY = f"""
    SELECT symbol, date_trunc('minute', ts) AS minute,
           min_by(price, ts) AS open, max(price) AS high,
           min(price) AS low, max_by(price, ts) AS close,
           sum(quantity) AS volume, count(*) AS trade_count
    FROM {SOURCE_TABLE} GROUP BY 1, 2
    ORDER BY symbol, minute
"""

TARGET_SELECT = f"""
    SELECT symbol, minute, open, high, low, close, volume, trade_count
    FROM {TARGET_TABLE} ORDER BY symbol, minute
"""

SYMBOLS = ["AAPL", "MSFT", "GOOG"]
N_DAYS = 30
BASE_DATE = datetime(2026, 3, 1, tzinfo=UTC)


# ── seeding helpers ──────────────────────────────────────────────────────


def _seed_rows(
    days: int,
    *,
    start_day: int = 0,
    minutes: int = 4,
    price_base: float = 100.0,
) -> list[tuple[str, datetime, float, float]]:
    """Deterministic (symbol, ts, price, qty) rows.

    ~minutes rows/symbol/day, prices monotone-ish but with intra-minute spread
    so open/high/low/close differ. No two rows share (symbol, ts), so
    min_by/max_by(price, ts) have unique winners (oracle-determinism).
    """
    rows: list[tuple[str, datetime, float, float]] = []
    for d in range(start_day, start_day + days):
        day = BASE_DATE + timedelta(days=d)
        for si, sym in enumerate(SYMBOLS):
            for m in range(minutes):
                ts = day + timedelta(hours=9, minutes=m, seconds=10 * (m % 3))
                price = price_base + si * 50 + d * 0.5 + m * 0.25
                qty = 100 + d + m + si
                rows.append((sym, ts, price, qty))
    return rows


def _values_clause(rows: list[tuple[str, datetime, float, float]]) -> str:
    parts = []
    for sym, ts, price, qty in rows:
        lit = ts.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
        parts.append(f"('{sym}', TIMESTAMP '{lit}', {price}, {qty})")
    return ", ".join(parts)


async def _insert(cursor, rows: list[tuple[str, datetime, float, float]]) -> None:
    if not rows:
        return
    await cursor.execute(f"INSERT INTO {SOURCE_TABLE} VALUES {_values_clause(rows)}")


async def _fetch(cursor, sql: str) -> list[tuple]:
    await cursor.execute(sql)
    return [tuple(r) for r in await cursor.fetchall()]


async def _target_rows(cursor) -> list[tuple]:
    return await _fetch(cursor, TARGET_SELECT)


async def _oracle_rows(cursor) -> list[tuple]:
    """From-scratch GROUP BY recompute over the LIVE source = ground truth."""
    return await _fetch(cursor, ORACLE_QUERY)


# ── interruption injection ───────────────────────────────────────────────


def _install_stop_after(monkeypatch, s, stop_after_chunks: int) -> dict:
    """Wrap executor.execute_refresh so s.stop_event fires after the Nth chunk.

    refresh_view imports execute_refresh into its own module namespace, so we
    patch ``server_mod.execute_refresh``. The counter dict is returned so the
    test can assert how many chunks committed before the interrupt.
    """
    orig = server_mod.execute_refresh
    state = {"yielded": 0}

    async def wrapper(*args, **kwargs):
        # Set stop_event BEFORE yielding the Nth chunk so refresh_view's
        # post-chunk stop check (which runs after it persists this chunk's
        # current_work) fires on exactly this chunk — not one chunk late.
        async for q in orig(*args, **kwargs):
            state["yielded"] += 1
            if state["yielded"] >= stop_after_chunks:
                s.stop_event.set()
            yield q

    monkeypatch.setattr(server_mod, "execute_refresh", wrapper)
    return state


# ── state.db inspection ──────────────────────────────────────────────────


async def _dump_state(s, view_name: str) -> dict:
    """Snapshot the persisted (bookmark, current_work) for assertions/evidence."""
    bookmark = await s.history.get_last_source_snapshot(view_name)
    work_max, work_chunk = await s.history.get_current_work(view_name)
    return {"bookmark": bookmark, "work_max": work_max, "work_chunk": work_chunk}


# ══════════════════════════════════════════════════════════════════════════
# Scenario 1 — Baseline: clean chunked full backfill
# ══════════════════════════════════════════════════════════════════════════


class TestScenario1Baseline:
    """Clean chunked full backfill over ~30 days → target == oracle;
    bookmark == M, current_work cleared, chunks_total cleared."""

    async def test_clean_chunked_full_backfill(self, trino_conn, app_state):
        cursor = await trino_conn.cursor()
        await cursor.execute(CREATE_SOURCE)
        await _insert(cursor, _seed_rows(N_DAYS))

        M = await get_current_snapshot(cursor, SOURCE_TABLE)

        await refresh_view(app_state, VIEW)

        vs = app_state.view_statuses[VIEW.name]
        assert vs.last_error is None, vs.last_error
        assert vs.last_action == "chunked_full", vs.last_action
        assert vs.total_refreshes == 1

        st = await _dump_state(app_state, VIEW.name)
        assert st["bookmark"] == M, f"bookmark should == pinned M; {st}"
        assert st["work_max"] is None and st["work_chunk"] is None, f"current_work not cleared: {st}"
        assert vs.chunks_total is None

        actual = await _target_rows(cursor)
        oracle = await _oracle_rows(cursor)
        assert actual == oracle, f"target diverged from oracle: {len(actual)} vs {len(oracle)} rows"
        # sanity: 3 symbols * 30 days * 4 minutes = 360 bars
        assert len(actual) == len(SYMBOLS) * N_DAYS * 4


# ══════════════════════════════════════════════════════════════════════════
# Scenario 2 — Interrupted resume
# ══════════════════════════════════════════════════════════════════════════


class TestScenario2InterruptedResume:
    """Stop after chunk 1 commits → state.db has current_work=(M, chunk-1 end),
    bookmark NULL. Resume reuses the SAME M, finishes, bookmark==M, output ==
    oracle and gap-free."""

    async def test_interrupt_then_resume(self, trino_conn, app_state, monkeypatch):
        cursor = await trino_conn.cursor()
        await cursor.execute(CREATE_SOURCE)
        await _insert(cursor, _seed_rows(N_DAYS))

        M = await get_current_snapshot(cursor, SOURCE_TABLE)

        # ── interrupt after chunk 1 ──
        st_counter = _install_stop_after(monkeypatch, app_state, stop_after_chunks=1)
        await refresh_view(app_state, VIEW)
        assert st_counter["yielded"] == 1, "expected exactly one chunk before interrupt"

        st = await _dump_state(app_state, VIEW.name)
        assert st["bookmark"] is None, f"bookmark must stay NULL while in-flight: {st}"
        assert st["work_max"] == M, f"pinned M must be persisted: {st}"
        assert st["work_chunk"] is not None, f"chunk-1 end must be recorded: {st}"
        # chunk-1 end is the start of day 2 (the first seeded day boundary + 1d).
        expected_chunk1_end = (BASE_DATE + timedelta(days=1)).replace(tzinfo=UTC)
        assert st["work_chunk"] == expected_chunk1_end, (
            f"chunk-1 end should be {expected_chunk1_end}, got {st['work_chunk']}"
        )

        # Only chunk-1's data should be in the target so far.
        partial = await _target_rows(cursor)
        assert len(partial) == len(SYMBOLS) * 4, f"only day-1 bars expected, got {len(partial)}"

        # ── resume (remove the interrupt wrapper, clear stop_event; M untouched) ──
        monkeypatch.undo()
        app_state.stop_event.clear()
        await refresh_view(app_state, VIEW)

        vs = app_state.view_statuses[VIEW.name]
        assert vs.last_error is None, vs.last_error

        st2 = await _dump_state(app_state, VIEW.name)
        assert st2["bookmark"] == M, f"resume must advance bookmark to the SAME pinned M: {st2}"
        assert st2["work_max"] is None and st2["work_chunk"] is None, f"current_work not cleared after resume: {st2}"

        actual = await _target_rows(cursor)
        oracle = await _oracle_rows(cursor)
        assert actual == oracle, f"resumed output diverged from oracle: {len(actual)} vs {len(oracle)}"
        assert len(actual) == len(SYMBOLS) * N_DAYS * 4


# ══════════════════════════════════════════════════════════════════════════
# Scenario 3 — Drift: overwrite an OLDER bucket mid-interrupt
# ══════════════════════════════════════════════════════════════════════════


class TestScenario3Drift:
    """Headline non-append-only case. Interrupt a backfill mid-run, then
    ``overwrite`` an OLDER (already-committed) bucket in the source — a NEW
    snapshot M'. Resume must reuse pinned M (NOT M') and NOT blend the
    overwrite. The overwrite is picked up only on the NEXT run over (M, M']."""

    async def test_older_bucket_overwrite_during_interrupt(self, trino_conn, app_state, monkeypatch):
        cursor = await trino_conn.cursor()
        await cursor.execute(CREATE_SOURCE)
        await _insert(cursor, _seed_rows(N_DAYS))

        M = await get_current_snapshot(cursor, SOURCE_TABLE)

        # Capture the oracle AS-OF M (target after a clean run should equal this).
        oracle_at_M = await _oracle_rows(cursor)

        # ── interrupt after chunk 3 (days 1-3 committed) ──
        _install_stop_after(monkeypatch, app_state, stop_after_chunks=3)
        await refresh_view(app_state, VIEW)
        st = await _dump_state(app_state, VIEW.name)
        assert st["work_max"] == M and st["bookmark"] is None
        # day-3 end = start of day 4
        assert st["work_chunk"] == (BASE_DATE + timedelta(days=3)).replace(tzinfo=UTC)

        # ── overwrite an OLDER, already-committed bucket (day 0) ──
        # A MERGE that changes existing rows produces an ``overwrite`` snapshot.
        day0 = BASE_DATE
        new_price = 999.0
        await cursor.execute(
            f"MERGE INTO {SOURCE_TABLE} AS t "
            f"USING (SELECT '{SYMBOLS[0]}' AS symbol, "
            f"TIMESTAMP '{(day0 + timedelta(hours=9)).strftime('%Y-%m-%d %H:%M:%S UTC')}' AS ts) AS s "
            f"ON t.symbol = s.symbol AND t.ts = s.ts "
            f"WHEN MATCHED THEN UPDATE SET price = {new_price}"
        )
        M_prime = await get_current_snapshot(cursor, SOURCE_TABLE)
        assert M_prime != M, "overwrite should have produced a new snapshot"

        # ── resume: must reuse pinned M, NOT pick up the day-0 overwrite ──
        monkeypatch.undo()  # remove the stop wrapper
        app_state.stop_event.clear()
        await refresh_view(app_state, VIEW)

        st2 = await _dump_state(app_state, VIEW.name)
        assert st2["bookmark"] == M, f"resume must advance bookmark to pinned M (NOT M'): {st2}"
        assert st2["work_max"] is None

        after_resume = await _target_rows(cursor)
        assert after_resume == oracle_at_M, (
            "resumed output must reflect data AS-OF M only (the day-0 overwrite must NOT appear yet)"
        )

        # ── next run over (M, M']: now the overwrite is picked up ──
        await refresh_view(app_state, VIEW)
        st3 = await _dump_state(app_state, VIEW.name)
        assert st3["bookmark"] == M_prime, f"next run should advance bookmark to M': {st3}"

        final = await _target_rows(cursor)
        oracle_now = await _oracle_rows(cursor)
        assert final == oracle_now, "final output must match a from-scratch recompute over the LIVE source"
        # and the overwritten bar must now reflect new_price
        await cursor.execute(
            f"SELECT high FROM {TARGET_TABLE} WHERE symbol = '{SYMBOLS[0]}' "
            f"AND minute = TIMESTAMP '{(day0 + timedelta(hours=9)).strftime('%Y-%m-%d %H:%M:%S UTC')}'"
        )
        row = await cursor.fetchone()
        assert row is not None and row[0] == new_price, f"overwrite not reflected after next run: {row}"


# ══════════════════════════════════════════════════════════════════════════
# Scenario 4 — Snapshot mixing: append new rows mid-run
# ══════════════════════════════════════════════════════════════════════════


class TestScenario4SnapshotMixing:
    """During a multi-chunk run, append NEW rows (new days) to the source. The
    in-flight run's output must reflect ONLY data as-of M; the concurrent
    appends appear only after the next run."""

    async def test_concurrent_append_isolated_to_next_run(self, trino_conn, app_state, monkeypatch):
        cursor = await trino_conn.cursor()
        await cursor.execute(CREATE_SOURCE)
        await _insert(cursor, _seed_rows(N_DAYS))

        M = await get_current_snapshot(cursor, SOURCE_TABLE)
        oracle_at_M = await _oracle_rows(cursor)

        # ── interrupt after chunk 2 ──
        _install_stop_after(monkeypatch, app_state, stop_after_chunks=2)
        await refresh_view(app_state, VIEW)
        st = await _dump_state(app_state, VIEW.name)
        assert st["work_max"] == M and st["bookmark"] is None

        # ── concurrent append: 5 brand-new days AFTER the seeded range ──
        new_rows = _seed_rows(5, start_day=N_DAYS, price_base=500.0)
        await _insert(cursor, new_rows)
        M_prime = await get_current_snapshot(cursor, SOURCE_TABLE)
        assert M_prime != M

        # ── resume: still pinned to M → must NOT include the new days ──
        monkeypatch.undo()
        app_state.stop_event.clear()
        await refresh_view(app_state, VIEW)

        st2 = await _dump_state(app_state, VIEW.name)
        assert st2["bookmark"] == M, f"resumed run must finish at pinned M: {st2}"

        after_resume = await _target_rows(cursor)
        assert after_resume == oracle_at_M, (
            "in-flight run must reflect ONLY as-of-M data; concurrent appends must be invisible"
        )
        # explicit: none of the new days are present yet
        new_day_count = await _fetch(
            cursor,
            f"SELECT count(*) FROM {TARGET_TABLE} WHERE minute >= TIMESTAMP "
            f"'{(BASE_DATE + timedelta(days=N_DAYS)).strftime('%Y-%m-%d %H:%M:%S UTC')}'",
        )
        assert new_day_count[0][0] == 0, "concurrent-append days leaked into the as-of-M run"

        # ── next run: now incremental over (M, M'] picks up the new days ──
        await refresh_view(app_state, VIEW)
        st3 = await _dump_state(app_state, VIEW.name)
        assert st3["bookmark"] == M_prime, f"next run should advance to M': {st3}"

        final = await _target_rows(cursor)
        oracle_now = await _oracle_rows(cursor)
        assert final == oracle_now, "after next run, output must match the live-source recompute"
        assert len(final) == len(SYMBOLS) * (N_DAYS + 5) * 4


# ══════════════════════════════════════════════════════════════════════════
# Scenario 5 — Expiry / livelock recovery
# ══════════════════════════════════════════════════════════════════════════


class TestScenario5ExpiryRecovery:
    """An interrupted backfill's pinned M is expired from the source. The next
    tick must DISCARD current_work and recompute full-from-scratch — NOT loop
    failing forever on a doomed resume."""

    async def test_expired_pin_discards_and_recomputes(self, trino_conn, app_state, monkeypatch):
        cursor = await trino_conn.cursor()
        await cursor.execute(CREATE_SOURCE)
        await _insert(cursor, _seed_rows(N_DAYS))

        M = await get_current_snapshot(cursor, SOURCE_TABLE)

        # ── interrupt after chunk 2 ──
        _install_stop_after(monkeypatch, app_state, stop_after_chunks=2)
        await refresh_view(app_state, VIEW)
        st = await _dump_state(app_state, VIEW.name)
        assert st["work_max"] == M and st["bookmark"] is None

        # ── advance the source past M, then expire snapshots so M is gone ──
        # Need a newer snapshot to expire M against; append one new day.
        await _insert(cursor, _seed_rows(1, start_day=N_DAYS, price_base=700.0))
        # expire_snapshots with retention_threshold='0s' to drop all but newest.
        # Catalog enforces a 7d min-retention; lower it for this session so the
        # pinned M can actually be expired (simulating a long backfill outliving
        # the source's real retention window).
        await cursor.execute("SET SESSION iceberg.expire_snapshots_min_retention = '0s'")
        await cursor.execute(f"ALTER TABLE {SOURCE_TABLE} EXECUTE expire_snapshots(retention_threshold => '0s')")
        monkeypatch.undo()

        # M must actually be gone for this scenario to be valid.
        m_gone = not await snapshot_exists(cursor, SOURCE_TABLE, M)
        assert m_gone, "expire_snapshots did not remove the pinned M; scenario precondition failed"

        # ── next tick: must DISCARD current_work and recompute from scratch ──
        app_state.stop_event.clear()
        await refresh_view(app_state, VIEW)

        vs = app_state.view_statuses[VIEW.name]
        assert vs.last_error is None, f"expired-pin recovery must not error: {vs.last_error}"
        assert vs.total_errors == 0, f"expired-pin recovery must not loop-fail: total_errors={vs.total_errors}"

        st2 = await _dump_state(app_state, VIEW.name)
        assert st2["work_max"] is None and st2["work_chunk"] is None, f"current_work not discarded: {st2}"
        new_M = await get_current_snapshot(cursor, SOURCE_TABLE)
        assert st2["bookmark"] == new_M, f"recompute should land bookmark on a live snapshot: {st2} vs {new_M}"

        actual = await _target_rows(cursor)
        oracle = await _oracle_rows(cursor)
        assert actual == oracle, "from-scratch recompute after expiry must match the live-source oracle"
