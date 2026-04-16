"""Refresh executor: builds and runs MERGE/INSERT SQL against Trino."""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime

from trino_mv_orchestrator.config import ViewConfig
from trino_mv_orchestrator.query_parser import inject_range_filter

log = logging.getLogger(__name__)


@dataclass
class RefreshResult:
    """Statistics from a refresh execution."""
    elapsed: float
    processed_rows: int = 0
    processed_bytes: int = 0


def _extract_stats(cursor) -> dict:
    """Extract processedRows/processedBytes from Trino cursor stats."""
    stats = getattr(cursor, "stats", None) or {}
    return {
        "processed_rows": stats.get("processedRows", 0) or 0,
        "processed_bytes": stats.get("processedBytes", 0) or 0,
    }


def build_merge_sql(
    target_table: str,
    source_query: str,
    merge_keys: tuple[str, ...] | list[str],
    value_columns: list[str],
) -> str:
    """Build an atomic MERGE statement for incremental refresh.

    ``source_query`` is the view query with the time-range WHERE predicate
    already injected (via ``inject_range_filter``).
    """
    on_clause = " AND ".join(f"t.{k} = s.{k}" for k in merge_keys)
    update_sets = ", ".join(f"{col} = s.{col}" for col in value_columns)
    all_columns = list(merge_keys) + value_columns
    insert_cols = ", ".join(all_columns)
    insert_vals = ", ".join(f"s.{col}" for col in all_columns)

    return (
        f"MERGE INTO {target_table} AS t\n"
        f"USING (\n{source_query}\n) AS s\n"
        f"ON {on_clause}\n"
        f"WHEN MATCHED THEN UPDATE SET {update_sets}\n"
        f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
    )


async def execute_full_refresh(cursor, view: ViewConfig, target_table: str) -> RefreshResult:
    """Full refresh: DELETE all + INSERT all. Returns RefreshResult."""
    start = time.monotonic()
    log.info("%s: full refresh — deleting target %s", view.name, target_table)
    await cursor.execute(f"DELETE FROM {target_table} WHERE true")

    log.info("%s: full refresh — inserting into %s", view.name, target_table)
    await cursor.execute(f"INSERT INTO {target_table} {view.query}")

    elapsed = time.monotonic() - start
    stats = _extract_stats(cursor)
    log.info(
        "%s: full refresh complete (%.1fs, %d rows, %d bytes)",
        view.name, elapsed, stats["processed_rows"], stats["processed_bytes"],
    )
    return RefreshResult(elapsed=elapsed, **stats)


async def execute_incremental_refresh(
    cursor,
    view: ViewConfig,
    target_table: str,
    filter_column: str,
    merge_keys: tuple[str, ...] | list[str],
    value_columns: list[str],
    filter_range: tuple[datetime, datetime],
) -> RefreshResult:
    """Incremental refresh via atomic MERGE on a time range. Returns RefreshResult."""
    start_time = time.monotonic()
    range_start, range_end = filter_range

    source_query = inject_range_filter(view.query, filter_column, range_start, range_end)
    merge_sql = build_merge_sql(target_table, source_query, merge_keys, value_columns)

    log.info(
        "%s: incremental refresh — %s in [%s, %s)",
        view.name, filter_column, range_start, range_end,
    )
    log.debug("%s: executing MERGE:\n%s", view.name, merge_sql)
    await cursor.execute(merge_sql)

    elapsed = time.monotonic() - start_time
    stats = _extract_stats(cursor)
    log.info(
        "%s: incremental refresh complete (%.1fs, %d rows, %d bytes)",
        view.name, elapsed, stats["processed_rows"], stats["processed_bytes"],
    )
    return RefreshResult(elapsed=elapsed, **stats)
