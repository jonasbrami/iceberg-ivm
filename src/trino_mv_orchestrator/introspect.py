"""Auto-discover query column types and source table partitioning."""
from __future__ import annotations

import json
import re
from dataclasses import dataclass


@dataclass(frozen=True)
class ColumnInfo:
    name: str
    type: str


async def discover_source_tables(cursor, query: str) -> list[str]:
    """Use EXPLAIN (TYPE IO, FORMAT JSON) to find all source tables in a query."""
    safe_query = query.replace("{range_filter}", "true")
    await cursor.execute(f"EXPLAIN (TYPE IO, FORMAT JSON) {safe_query}")
    row = await cursor.fetchone()
    explain_json = json.loads(row[0])

    tables = []
    for info in explain_json.get("inputTableColumnInfos", []):
        t = info["table"]
        fqn = f"{t['catalog']}.{t['schemaTable']['schema']}.{t['schemaTable']['table']}"
        if fqn not in tables:
            tables.append(fqn)
    return tables


async def discover_columns(cursor, query: str) -> list[ColumnInfo]:
    """Use PREPARE + DESCRIBE OUTPUT to get column names and types without executing."""
    safe_query = query.replace("{range_filter}", "true")
    stmt_name = "__mv_introspect"
    await cursor.execute(f"PREPARE {stmt_name} FROM {safe_query}")
    await cursor.execute(f"DESCRIBE OUTPUT {stmt_name}")
    columns = [ColumnInfo(name=row[0], type=row[4]) for row in await cursor.fetchall()]
    await cursor.execute(f"DEALLOCATE PREPARE {stmt_name}")
    return columns


async def discover_source_partitioning(cursor, source_table: str) -> str | None:
    """Extract the partitioning spec from SHOW CREATE TABLE.

    Returns the ARRAY[...] string, or None if not partitioned.
    """
    await cursor.execute(f"SHOW CREATE TABLE {source_table}")
    row = await cursor.fetchone()
    create_sql = row[0]
    match = re.search(r"partitioning\s*=\s*(ARRAY\[[^\]]+\])", create_sql)
    return match.group(1) if match else None


def build_create_table_sql(
    target_table: str,
    columns: list[ColumnInfo] | list[tuple[str, str]],
    partitioning: str | None = None,
) -> str:
    """Generate CREATE TABLE IF NOT EXISTS DDL for the target table."""
    cols = []
    for c in columns:
        if isinstance(c, ColumnInfo):
            cols.append(f"{c.name} {c.type}")
        else:
            cols.append(f"{c[0]} {c[1]}")

    col_str = ",\n  ".join(cols)
    sql = f"CREATE TABLE IF NOT EXISTS {target_table} (\n  {col_str}\n)"
    props = ["format = 'PARQUET'"]
    if partitioning:
        props.append(f"partitioning = {partitioning}")
    sql += f" WITH ({', '.join(props)})"
    return sql
