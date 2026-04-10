"""Tests for state read/write."""
from trino_mv_orchestrator.state import SNAPSHOT_KEY, read_last_snapshot, write_last_snapshot


class MockCursor:
    def __init__(self, results=None):
        self._rows = results or []
        self.executed = []

    def execute(self, sql):
        self.executed.append(sql)

    def fetchone(self):
        return self._rows[0] if self._rows else None


class TestReadLastSnapshot:
    def test_returns_id(self):
        cursor = MockCursor(results=[("12345",)])
        assert read_last_snapshot(cursor, "iceberg.analytics.ohlcv_1m") == 12345
        assert '"ohlcv_1m$properties"' in cursor.executed[0]

    def test_returns_none(self):
        assert read_last_snapshot(MockCursor(), "iceberg.analytics.ohlcv_1m") is None


class TestWriteLastSnapshot:
    def test_writes_alter(self):
        cursor = MockCursor()
        write_last_snapshot(cursor, "iceberg.analytics.ohlcv_1m", 99999)
        sql = cursor.executed[0]
        assert "ALTER TABLE" in sql
        assert SNAPSHOT_KEY in sql
        assert "99999" in sql
