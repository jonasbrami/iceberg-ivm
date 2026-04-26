"""Helpers for streaming-refresh integration tests.

These wrap the orchestrator's daemon driver (``server.refresh_view``)
behind a fixture-friendly API so tests can script a deterministic
sequence of (insert batch → refresh → assert) cycles without touching
the orchestrator's CLI or HTTP surface.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from trino_mv_orchestrator.config import Config, TrinoConfig, ServerConfig
from trino_mv_orchestrator.query_history import QueryHistory
from trino_mv_orchestrator.server import AppState, RECENT_QUERY_LIMIT


@dataclass
class Trade:
    symbol: str
    ts: datetime  # must be timezone-aware UTC for determinism
    price: float
    quantity: float


@dataclass
class Cycle:
    """One step of a scripted scenario.

    ``rows`` are appended to the source in a single multi-row INSERT
    (one Iceberg snapshot). ``compact`` triggers ``ALTER TABLE EXECUTE
    optimize`` on the source instead of an append. ``expect_action``
    is the value ``ViewStatus.last_action`` should hold after the
    cycle's ``refresh_view`` call: ``"full"``, ``"incremental"``, or
    ``"skip"``.
    """
    rows: list[Trade] = field(default_factory=list)
    expect_action: str = "incremental"
    compact: bool = False
    note: str = ""


async def make_app_state(
    host: str, port: int, *, schema: str = "test_schema",
    state_db_path: Path | str | None = None,
) -> AppState:
    """Build a minimal ``AppState`` wired to the docker-compose Trino.

    Opens a real ``QueryHistory`` so the SQLite-backed
    ``last_source_snapshot`` bookmark works end-to-end. No config files
    on disk — ``config_path`` / ``views_path`` are unused once
    ``config`` is set, since refresh_view never reloads. Caller is
    responsible for awaiting ``s.history.close()`` (see the
    ``app_state`` fixture in ``conftest.py``).
    """
    s = AppState()
    s.config = Config(
        trino=TrinoConfig(
            url=f"http://{host}:{port}",
            user="test",
            password=None,
            catalog="iceberg",
            schema=schema,
        ),
        views=[],
        server=ServerConfig(),
    )
    if state_db_path is not None:
        h = QueryHistory(state_db_path, limit=RECENT_QUERY_LIMIT)
        await h.open()
        s.history = h
    return s


def _format_ts(ts: datetime) -> str:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    # Trino accepts "YYYY-MM-DD HH:MM:SS UTC" as a TIMESTAMP WITH TIME ZONE literal.
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


async def insert_trades_batch(cursor, source_table: str, rows: list[Trade]) -> None:
    """One multi-row INSERT → exactly one source snapshot per cycle."""
    if not rows:
        return
    values = ", ".join(
        f"('{r.symbol}', TIMESTAMP '{_format_ts(r.ts)}', {r.price}, {r.quantity})"
        for r in rows
    )
    await cursor.execute(f"INSERT INTO {source_table} VALUES {values}")


async def fetch_target_rows(cursor, target_table: str) -> list[dict]:
    """Match the column order from VIEW_QUERY's SELECT projection."""
    await cursor.execute(
        f"SELECT symbol, minute, open, high, low, close, volume, trade_count "
        f"FROM {target_table} ORDER BY symbol, minute"
    )
    cols = ["symbol", "minute", "open", "high", "low", "close", "volume", "trade_count"]
    return [dict(zip(cols, r)) for r in await cursor.fetchall()]
