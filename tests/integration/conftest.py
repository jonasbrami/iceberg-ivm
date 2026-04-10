"""Integration test fixtures: Trino connection and table setup/teardown."""
from __future__ import annotations

import asyncio
import os

import aiotrino
import pytest_asyncio


TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "18080"))


async def wait_for_trino(host: str, port: int, timeout: int = 120) -> None:
    """Block until Trino is ready to accept queries."""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        try:
            conn = aiotrino.dbapi.connect(host=host, port=port, user="test")
            cur = await conn.cursor()
            await cur.execute("SELECT 1")
            await cur.fetchone()
            await conn.close()
            return
        except Exception:
            await asyncio.sleep(2)
    raise TimeoutError(f"Trino at {host}:{port} not ready after {timeout}s")


@pytest_asyncio.fixture(scope="session")
async def trino_conn():
    """Session-scoped Trino connection. Requires docker-compose up."""
    await wait_for_trino(TRINO_HOST, TRINO_PORT)
    conn = aiotrino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        catalog="iceberg",
        schema="default",
        user="test",
    )
    yield conn
    await conn.close()


@pytest_asyncio.fixture(autouse=True)
async def clean_tables(trino_conn):
    """Drop test tables before and after each test."""
    cursor = await trino_conn.cursor()
    tables = [
        "iceberg.test_schema.trades",
        "iceberg.test_schema.ohlcv_1m",
        "iceberg.test_schema.ohlcv_weekly",
        "iceberg.test_schema.ohlcv_monthly",
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
