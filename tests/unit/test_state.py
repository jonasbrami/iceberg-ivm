"""Tests for state read/write."""
from trino_mv_orchestrator.detector import system_table
from trino_mv_orchestrator.state import SNAPSHOT_KEY, read_last_snapshot, write_last_snapshot


class TestSystemTableShared:
    def test_state_uses_detector_system_table(self):
        """state.py should reuse system_table from detector, not duplicate it."""
        result = system_table("iceberg.analytics.ohlcv_1m", "properties")
        assert result == 'iceberg.analytics."ohlcv_1m$properties"'

    def test_unqualified_table(self):
        result = system_table("ohlcv_1m", "properties")
        assert result == '"ohlcv_1m$properties"'


class MockCursor:
    def __init__(self, results=None):
        self._rows = results or []
        self.executed = []

    async def execute(self, sql):
        self.executed.append(sql)

    async def fetchone(self):
        return self._rows[0] if self._rows else None


class TestReadLastSnapshot:
    async def test_returns_id(self):
        cursor = MockCursor(results=[("12345",)])
        assert await read_last_snapshot(cursor, "iceberg.analytics.ohlcv_1m") == 12345
        assert '"ohlcv_1m$properties"' in cursor.executed[0]

    async def test_returns_none(self):
        assert await read_last_snapshot(MockCursor(), "iceberg.analytics.ohlcv_1m") is None


class TestWriteLastSnapshot:
    async def test_writes_alter(self):
        cursor = MockCursor()
        await write_last_snapshot(cursor, "iceberg.analytics.ohlcv_1m", 99999)
        sql = cursor.executed[0]
        assert "ALTER TABLE" in sql
        assert SNAPSHOT_KEY in sql
        assert "99999" in sql
