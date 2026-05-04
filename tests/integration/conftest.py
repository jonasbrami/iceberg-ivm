"""Integration test fixtures: Trino connection and table setup/teardown."""
from __future__ import annotations

import asyncio
import os
import time

import aiotrino
import pytest_asyncio

from ._driver import make_app_state


TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "18080"))

# Track Trino readiness across tests so we don't pay the "SELECT 1"
# round-trip on every function-scoped fixture setup.
_trino_ready = False


async def wait_for_trino(host: str, port: int, timeout: int = 120) -> None:
    """Block until Trino is ready to accept queries.

    First call per process blocks until Trino answers; subsequent calls
    are no-ops.
    """
    global _trino_ready
    if _trino_ready:
        return
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            conn = aiotrino.dbapi.connect(host=host, port=port, user="test")
            cur = await conn.cursor()
            await cur.execute("SELECT 1")
            await cur.fetchone()
            await conn.close()
            _trino_ready = True
            return
        except Exception:
            await asyncio.sleep(2)
    raise TimeoutError(f"Trino at {host}:{port} not ready after {timeout}s")


@pytest_asyncio.fixture
async def trino_conn():
    """Function-scoped Trino connection.

    Session-scoped would be cheaper but doesn't work with the default
    pytest-asyncio function-scoped event loop: aiotrino holds an
    aiohttp.ClientSession bound to the loop it was created on, and
    using it from a subsequent test's loop raises
    ``Timeout context manager should be used inside a task``. A fresh
    connection per test avoids the loop-affinity problem entirely; the
    overhead is a single HTTP handshake to Trino (~10 ms).
    """
    await wait_for_trino(TRINO_HOST, TRINO_PORT)
    conn = aiotrino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        catalog="iceberg",
        schema="default",
        user="test",
        timezone="UTC",
    )
    try:
        yield conn
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def app_state(tmp_path):
    """Minimal AppState wired to the docker-compose Trino, for tests
    that drive the daemon's top-level ``refresh_view`` loop. A real
    SQLite history is attached so the source-snapshot bookmark
    round-trips through ``view_status.last_source_snapshot``."""
    await wait_for_trino(TRINO_HOST, TRINO_PORT)
    s = await make_app_state(
        TRINO_HOST, TRINO_PORT, state_db_path=tmp_path / "state.db",
    )
    try:
        yield s
    finally:
        if s.history is not None:
            await s.history.close()


@pytest_asyncio.fixture(autouse=True)
async def clean_tables(trino_conn):
    """Drop test tables before and after each test."""
    cursor = await trino_conn.cursor()
    tables = [
        "iceberg.test_schema.trades",
        "iceberg.test_schema.ohlcv_1m",
        "iceberg.test_schema.ohlcv_1h",
        "iceberg.test_schema.ohlcv_weekly",
        "iceberg.test_schema.ohlcv_monthly",
        "iceberg.test_schema.streaming_ohlcv",
    ]
    for t in tables:
        try:
            await cursor.execute(f"DROP TABLE IF EXISTS {t}")
        except Exception:
            pass

    # Ensure test schema exists
    try:
        await cursor.execute("CREATE SCHEMA IF NOT EXISTS iceberg.test_schema")
    except Exception:
        pass

    yield

    for t in tables:
        try:
            await cursor.execute(f"DROP TABLE IF EXISTS {t}")
        except Exception:
            pass
