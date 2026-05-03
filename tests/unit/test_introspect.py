"""Tests for the introspection module."""

from iceberg_ivm.introspect import (
    ColumnInfo,
    build_create_table_sql,
    discover_columns,
)


class MockCursor:
    def __init__(self, results: list[list[tuple]]):
        self._results = results
        self._idx = 0
        self._rows = []
        self.executed = []

    async def execute(self, sql: str):
        self.executed.append(sql)
        if self._idx < len(self._results):
            self._rows = list(self._results[self._idx])
        else:
            self._rows = []
        self._idx += 1

    async def fetchone(self):
        return self._rows[0] if self._rows else None

    async def fetchall(self):
        return self._rows


# ── discover_columns ──

class TestDiscoverColumns:
    async def test_basic(self):
        # DESCRIBE OUTPUT returns: (name, catalog, schema, table, type, typeSize, aliased)
        cursor = MockCursor([
            [],  # PREPARE
            [
                ("symbol", "iceberg", "analytics", "trades", "varchar", 0, False),
                ("minute", "iceberg", "analytics", "trades", "timestamp(6)", 0, False),
                ("open", "iceberg", "analytics", "trades", "double", 0, False),
            ],
            [],  # DEALLOCATE
        ])
        columns = await discover_columns(
            cursor,
            "SELECT symbol, minute, open FROM t GROUP BY 1, 2, 3",
        )
        assert len(columns) == 3
        assert columns[0] == ColumnInfo(name="symbol", type="varchar")
        assert columns[1] == ColumnInfo(name="minute", type="timestamp(6)")
        assert columns[2] == ColumnInfo(name="open", type="double")
        # The query is passed to PREPARE verbatim — no placeholder substitution.
        assert "SELECT symbol, minute, open FROM t GROUP BY 1, 2, 3" in cursor.executed[0]


# ── build_create_table_sql ──

class TestBuildCreateTableSql:
    def test_with_column_info(self):
        cols = [ColumnInfo("symbol", "varchar"), ColumnInfo("ts", "timestamp(6)")]
        sql = build_create_table_sql("iceberg.out.mv", cols)
        assert "CREATE TABLE IF NOT EXISTS iceberg.out.mv" in sql
        assert "symbol varchar" in sql
        assert "ts timestamp(6)" in sql
        assert "format = 'PARQUET'" in sql
        assert "partitioning" not in sql

    def test_with_partitioning(self):
        cols = [ColumnInfo("ts", "timestamp(6)")]
        sql = build_create_table_sql("t", cols, "ARRAY['day(ts)']")
        assert "partitioning = ARRAY['day(ts)']" in sql
        assert "format = 'PARQUET'" in sql
