"""Refresh executor: builds and runs MERGE/INSERT SQL against Trino."""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime

from trino_mv_orchestrator.config import ViewConfig
from trino_mv_orchestrator.query_parser import inject_range_filter

log = logging.getLogger(__name__)


@dataclass
class QueryInfo:
    """Metadata for a single Trino query run during a refresh.

    Captures what the UI needs to link to the Trino UI (``info_uri`` points
    at ``/ui/query.html?<query_id>``) plus stats for display.
    """
    query_id: str
    info_uri: str
    stage: str           # "full_delete" | "full_insert" | "merge"
    started_at: float    # wall-clock epoch seconds
    elapsed_ms: float
    processed_rows: int = 0
    processed_bytes: int = 0


@dataclass
class RefreshResult:
    """Statistics from a refresh execution."""
    elapsed: float
    processed_rows: int = 0
    processed_bytes: int = 0
    queries: list[QueryInfo] = field(default_factory=list)


def _extract_stats(cursor) -> dict:
    """Extract processedRows/processedBytes from Trino cursor stats."""
    stats = getattr(cursor, "stats", None) or {}
    return {
        "processed_rows": stats.get("processedRows", 0) or 0,
        "processed_bytes": stats.get("processedBytes", 0) or 0,
    }


async def _execute_tracked(cursor, sql: str, stage: str) -> QueryInfo:
    """Execute ``sql`` and return a QueryInfo capturing query_id, info_uri,
    elapsed time, and row/byte stats from the cursor.

    Safe against mock cursors that don't expose query_id / info_uri — those
    fall back to empty strings so tests keep working.
    """
    started_wall = time.time()
    t0 = time.monotonic()
    await cursor.execute(sql)
    elapsed_ms = (time.monotonic() - t0) * 1000
    stats = _extract_stats(cursor)
    return QueryInfo(
        query_id=getattr(cursor, "query_id", "") or "",
        info_uri=getattr(cursor, "info_uri", "") or "",
        stage=stage,
        started_at=started_wall,
        elapsed_ms=elapsed_ms,
        processed_rows=stats["processed_rows"],
        processed_bytes=stats["processed_bytes"],
    )


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
    delete_q = await _execute_tracked(
        cursor, f"DELETE FROM {target_table} WHERE true", stage="full_delete",
    )

    log.info("%s: full refresh — inserting into %s", view.name, target_table)
    insert_q = await _execute_tracked(
        cursor, f"INSERT INTO {target_table} {view.query}", stage="full_insert",
    )

    elapsed = time.monotonic() - start
    log.info(
        "%s: full refresh complete (%.1fs, %d rows, %d bytes)",
        view.name, elapsed, insert_q.processed_rows, insert_q.processed_bytes,
    )
    return RefreshResult(
        elapsed=elapsed,
        processed_rows=insert_q.processed_rows,
        processed_bytes=insert_q.processed_bytes,
        queries=[delete_q, insert_q],
    )


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
    merge_q = await _execute_tracked(cursor, merge_sql, stage="merge")

    elapsed = time.monotonic() - start_time
    log.info(
        "%s: incremental refresh complete (%.1fs, %d rows, %d bytes)",
        view.name, elapsed, merge_q.processed_rows, merge_q.processed_bytes,
    )
    return RefreshResult(
        elapsed=elapsed,
        processed_rows=merge_q.processed_rows,
        processed_bytes=merge_q.processed_bytes,
        queries=[merge_q],
    )
