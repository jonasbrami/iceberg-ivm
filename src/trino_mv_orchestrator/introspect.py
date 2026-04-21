"""Auto-discover query column types."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ColumnInfo:
    name: str
    type: str


async def discover_columns(cursor, query: str) -> list[ColumnInfo]:
    """Use PREPARE + DESCRIBE OUTPUT to get column names and types without executing."""
    stmt_name = "__mv_introspect"
    await cursor.execute(f"PREPARE {stmt_name} FROM {query}")
    await cursor.execute(f"DESCRIBE OUTPUT {stmt_name}")
    columns = [ColumnInfo(name=row[0], type=row[4]) for row in await cursor.fetchall()]
    await cursor.execute(f"DEALLOCATE PREPARE {stmt_name}")
    return columns


def build_create_table_sql(
    target_table: str,
    columns: list[ColumnInfo],
    partitioning: str | None = None,
) -> str:
    """Generate CREATE TABLE IF NOT EXISTS DDL for the target table."""
    col_str = ",\n  ".join(f"{c.name} {c.type}" for c in columns)
    sql = f"CREATE TABLE IF NOT EXISTS {target_table} (\n  {col_str}\n)"
    props = ["format = 'PARQUET'"]
    if partitioning:
        props.append(f"partitioning = {partitioning}")
    sql += f" WITH ({', '.join(props)})"
    return sql
