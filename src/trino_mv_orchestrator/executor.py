"""Refresh executor: builds and runs MERGE/INSERT SQL against Trino."""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Awaitable, Callable

from trino_mv_orchestrator.config import ViewConfig
from trino_mv_orchestrator.detector import (
    expand_to_bucket_bounds,
    get_source_column_range,
    get_target_bucket_max,
    walk_buckets,
)
from trino_mv_orchestrator.query_parser import ParsedView, inject_range_filter

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
    # Set by ``execute_chunked_full_refresh`` when ``should_stop`` tripped
    # between chunks. The caller uses this to skip ``write_last_snapshot``
    # so the next tick resumes from target metadata.
    interrupted: bool = False


@dataclass
class ChunkProgress:
    """Per-chunk progress payload delivered to ``on_chunk`` callbacks.

    Carries everything a status consumer needs to update ``ViewStatus``
    after each chunk commit (so an operator can observe a long backfill
    without tailing ``docker logs``): the range just committed, the
    ``QueryInfo`` for that chunk's MERGE (query_id, duration, row counts),
    and counters (1-indexed).
    """
    chunk_range: tuple[datetime, datetime]
    query: QueryInfo
    chunks_done: int       # 1-indexed
    chunks_total: int


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


async def execute_chunked_full_refresh(
    cursor,
    view: ViewConfig,
    target_table: str,
    parsed: ParsedView,
    value_columns: list[str],
    *,
    chunk_granularity: str,
    should_stop: Callable[[], bool] = lambda: False,
    on_chunk: Callable[[ChunkProgress], Awaitable[None]] | None = None,
) -> RefreshResult:
    """Chunked first-run full refresh.

    Walks the source's ``filter_column`` range one ``chunk_granularity``
    bucket at a time, emitting a MERGE per chunk. MERGE (not INSERT) so
    that a client-side retry that double-commits the same chunk matches
    all rows on the second run and updates to identical values rather
    than appending duplicates.

    Resume: the next chunk to run is derived from the target's own Iceberg
    ``$files`` metadata — specifically ``max(parsed.bucket_alias)``. Trino
    INSERTs/MERGEs into Iceberg commit as atomic single snapshots, so
    target metadata is a crash-safe authoritative record of completed
    chunks. No orchestrator-side cursor state.

    Returns ``RefreshResult`` with per-chunk ``QueryInfo`` in ``queries``.
    Sets ``interrupted=True`` and returns early if ``should_stop()`` fires
    between chunks.
    """
    start_time = time.monotonic()
    assert parsed.bucket_alias is not None, (
        "execute_chunked_full_refresh requires parsed.bucket_alias; "
        "config validation should have rejected this view"
    )

    source_range = await get_source_column_range(
        cursor, parsed.source_table, parsed.filter_column,
    )
    if source_range is None:
        log.info("%s: source %s is empty, nothing to backfill",
                 view.name, parsed.source_table)
        return RefreshResult(elapsed=time.monotonic() - start_time)

    backfill_start, backfill_end = expand_to_bucket_bounds(
        source_range[0], source_range[1], chunk_granularity,
    )

    target_max = await get_target_bucket_max(
        cursor, target_table, parsed.bucket_alias,
    )
    if target_max is None:
        resume = backfill_start
    else:
        # Snap the last committed bucket upward to the next chunk boundary.
        resume = expand_to_bucket_bounds(target_max, target_max, chunk_granularity)[1]

    log.info(
        "%s: chunked full refresh — %s in [%s, %s), chunk=%s, resume=%s",
        view.name, parsed.filter_column, backfill_start, backfill_end,
        chunk_granularity, resume,
    )

    result = RefreshResult(elapsed=0.0)
    # Materialize the chunk list up front so ``on_chunk`` can report
    # ``chunks_total`` — operators need a denominator to estimate ETA of a
    # multi-hour backfill. ``walk_buckets`` yields datetime pairs over a
    # bounded span, so the list is cheap (1 tuple per bucket).
    chunks = list(walk_buckets(resume, backfill_end, chunk_granularity))
    total = len(chunks)
    for i, (chunk_start, chunk_end) in enumerate(chunks, start=1):
        source_query = inject_range_filter(
            view.query, parsed.filter_column, chunk_start, chunk_end,
        )
        merge_sql = build_merge_sql(
            target_table, source_query, parsed.merge_keys, value_columns,
        )
        log.info(
            "%s: chunk %d/%d [%s, %s) — MERGE",
            view.name, i, total, chunk_start, chunk_end,
        )
        q = await _execute_tracked(cursor, merge_sql, stage="chunk_merge")
        result.queries.append(q)
        result.processed_rows += q.processed_rows
        result.processed_bytes += q.processed_bytes
        if on_chunk is not None:
            await on_chunk(ChunkProgress(
                chunk_range=(chunk_start, chunk_end),
                query=q,
                chunks_done=i,
                chunks_total=total,
            ))
        if should_stop():
            log.info("%s: chunked refresh interrupted after [%s, %s)",
                     view.name, chunk_start, chunk_end)
            result.interrupted = True
            break

    result.elapsed = time.monotonic() - start_time
    log.info(
        "%s: chunked full refresh %s (%.1fs, %d chunks, %d rows, %d bytes)",
        view.name,
        "interrupted" if result.interrupted else "complete",
        result.elapsed, len(result.queries),
        result.processed_rows, result.processed_bytes,
    )
    return result
