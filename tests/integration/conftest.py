"""Integration test fixtures: Trino connection and table setup/teardown."""
from __future__ import annotations

import os
import time

import pytest
import trino


TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "18080"))


def wait_for_trino(host: str, port: int, timeout: int = 120) -> None:
    """Block until Trino is ready to accept queries."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            conn = trino.dbapi.connect(host=host, port=port, user="test")
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            conn.close()
            return
        except Exception:
            time.sleep(2)
    raise TimeoutError(f"Trino at {host}:{port} not ready after {timeout}s")


@pytest.fixture(scope="session")
def trino_conn():
    """Session-scoped Trino connection. Requires docker-compose up."""
    wait_for_trino(TRINO_HOST, TRINO_PORT)
    conn = trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        catalog="iceberg",
        schema="default",
        user="test",
    )
    yield conn
    conn.close()


@pytest.fixture(autouse=True)
def clean_tables(trino_conn):
    """Drop test tables before and after each test."""
    cursor = trino_conn.cursor()
    tables = [
        "iceberg.test_schema.trades",
        "iceberg.test_schema.ohlcv_1m",
        "iceberg.test_schema.ohlcv_weekly",
        "iceberg.test_schema.ohlcv_monthly",
    ]
    for t in tables:
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {t}")
        except Exception:
            pass

    # Ensure test schema exists
    try:
        cursor.execute("CREATE SCHEMA IF NOT EXISTS iceberg.test_schema")
    except Exception:
        pass

    yield

    for t in tables:
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {t}")
        except Exception:
            pass
