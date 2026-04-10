"""Tests for the introspection module."""
import json

from trino_mv_orchestrator.introspect import (
    ColumnInfo,
    build_create_table_sql,
    discover_columns,
    discover_source_partitioning,
    discover_source_tables,
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


# ── discover_source_partitioning ──

class TestDiscoverSourcePartitioning:
    async def test_partitioned_table(self):
        create_sql = (
            "CREATE TABLE iceberg.market_data.trades (\n"
            "  ts timestamp(6) with time zone,\n"
            "  symbol varchar\n"
            ")\n"
            "WITH (\n"
            "  format = 'PARQUET',\n"
            "  partitioning = ARRAY['day(ts)']\n"
            ")"
        )
        cursor = MockCursor([[(create_sql,)]])
        result = await discover_source_partitioning(cursor, "iceberg.market_data.trades")
        assert result == "ARRAY['day(ts)']"
        assert "SHOW CREATE TABLE" in cursor.executed[0]

    async def test_multi_column_partitioning(self):
        create_sql = (
            "CREATE TABLE t (\n  a int\n)\n"
            "WITH (\n  partitioning = ARRAY['day(ts)', 'bucket(16, id)']\n)"
        )
        cursor = MockCursor([[(create_sql,)]])
        result = await discover_source_partitioning(cursor, "t")
        assert result == "ARRAY['day(ts)', 'bucket(16, id)']"

    async def test_not_partitioned(self):
        create_sql = (
            "CREATE TABLE t (\n  a int\n)\n"
            "WITH (\n  format = 'PARQUET'\n)"
        )
        cursor = MockCursor([[(create_sql,)]])
        result = await discover_source_partitioning(cursor, "t")
        assert result is None

    async def test_whitespace_variations(self):
        create_sql = "WITH (  partitioning  =  ARRAY['month(ts)']  )"
        cursor = MockCursor([[(create_sql,)]])
        result = await discover_source_partitioning(cursor, "t")
        assert result == "ARRAY['month(ts)']"


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
        columns = await discover_columns(cursor, "SELECT symbol, minute, open FROM t WHERE {range_filter}")
        assert len(columns) == 3
        assert columns[0] == ColumnInfo(name="symbol", type="varchar")
        assert columns[1] == ColumnInfo(name="minute", type="timestamp(6)")
        assert columns[2] == ColumnInfo(name="open", type="double")
        # Verify {range_filter} was replaced for the PREPARE
        assert "true" in cursor.executed[0]
        assert "{range_filter}" not in cursor.executed[0]


# ── discover_source_tables ──

class TestDiscoverSourceTables:
    async def test_single_source(self):
        explain_json = json.dumps({
            "inputTableColumnInfos": [
                {
                    "table": {
                        "catalog": "iceberg",
                        "schemaTable": {"schema": "market_data", "table": "trades"},
                    },
                    "columns": [],
                }
            ]
        })
        cursor = MockCursor([[(explain_json,)]])
        tables = await discover_source_tables(cursor, "SELECT * FROM t WHERE {range_filter}")
        assert tables == ["iceberg.market_data.trades"]

    async def test_multiple_sources_deduped(self):
        explain_json = json.dumps({
            "inputTableColumnInfos": [
                {"table": {"catalog": "c", "schemaTable": {"schema": "s", "table": "t1"}}, "columns": []},
                {"table": {"catalog": "c", "schemaTable": {"schema": "s", "table": "t1"}}, "columns": []},
                {"table": {"catalog": "c", "schemaTable": {"schema": "s", "table": "t2"}}, "columns": []},
            ]
        })
        cursor = MockCursor([[(explain_json,)]])
        tables = await discover_source_tables(cursor, "SELECT * FROM t WHERE {range_filter}")
        assert tables == ["c.s.t1", "c.s.t2"]


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

    def test_with_tuples(self):
        cols = [("a", "int"), ("b", "double")]
        sql = build_create_table_sql("t", cols)
        assert "a int" in sql
        assert "b double" in sql

    def test_with_partitioning(self):
        cols = [ColumnInfo("ts", "timestamp(6)")]
        sql = build_create_table_sql("t", cols, "ARRAY['day(ts)']")
        assert "partitioning = ARRAY['day(ts)']" in sql
        assert "format = 'PARQUET'" in sql
