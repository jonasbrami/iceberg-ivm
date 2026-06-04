"""Refresh executor: builds and runs MERGE SQL against Trino.

One refresh = one or more MERGE commits over bucket-aligned time ranges:

  - incremental refresh: one MERGE over the detector's snapped range.
  - full refresh: one MERGE over the source's whole range.
  - chunked full refresh: N MERGEs, one per chunk, with per-chunk commit so
    a crash or restart resumes from the ``current_work`` record (never from
    the target's own data).

Every chunk reads its source ``FOR VERSION AS OF`` the run's pinned snapshot,
so a multi-chunk run never mixes data from different source commits.

``execute_refresh`` is a single async generator that yields one ``QueryInfo``
per committed MERGE. Callers cancel via ``break`` — no callback plumbing.
"""

from __future__ import annotations

import logging
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime, timedelta

from iceberg_ivm.config import ViewConfig
from iceberg_ivm.detector import (
    expand_to_bucket_bounds,
    get_source_column_range,
    walk_buckets,
)
from iceberg_ivm.query_parser import ParsedView, inject_range_filter, inject_version_pin

log = logging.getLogger(__name__)


@dataclass
class QueryInfo:
    """Metadata for one Trino query: linking + stats."""

    query_id: str
    info_uri: str
    stage: str  # "merge" | "chunk_merge" | "maintenance_<op>"
    started_at: float
    elapsed_ms: float
    processed_rows: int = 0
    processed_bytes: int = 0
    # Range this query covered, if applicable (always set for refresh stages).
    range_start: datetime | None = None
    range_end: datetime | None = None
    chunks_done: int = 0  # 1-indexed count of committed chunks so far
    chunks_total: int = 0  # 0 for non-chunked


async def _execute_tracked(
    cursor,
    sql: str,
    stage: str,
    *,
    range_start: datetime | None = None,
    range_end: datetime | None = None,
    chunks_done: int = 0,
    chunks_total: int = 0,
) -> QueryInfo:
    """Execute ``sql`` and return a QueryInfo with timing + stats + range."""
    started = time.time()
    t0 = time.monotonic()
    await cursor.execute(sql)
    stats = getattr(cursor, "stats", None) or {}
    return QueryInfo(
        query_id=getattr(cursor, "query_id", "") or "",
        info_uri=getattr(cursor, "info_uri", "") or "",
        stage=stage,
        started_at=started,
        elapsed_ms=(time.monotonic() - t0) * 1000,
        processed_rows=stats.get("processedRows", 0) or 0,
        processed_bytes=stats.get("processedBytes", 0) or 0,
        range_start=range_start,
        range_end=range_end,
        chunks_done=chunks_done,
        chunks_total=chunks_total,
    )


def build_merge_sql(
    target_table: str,
    source_query: str,
    merge_keys: tuple[str, ...] | list[str],
    value_columns: list[str],
) -> str:
    """Build an atomic MERGE statement. ``source_query`` must already have
    the time-range WHERE injected (via ``inject_range_filter``)."""
    on = " AND ".join(f"t.{k} = s.{k}" for k in merge_keys)
    updates = ", ".join(f"{c} = s.{c}" for c in value_columns)
    cols = list(merge_keys) + value_columns
    return (
        f"MERGE INTO {target_table} AS t\n"
        f"USING (\n{source_query}\n) AS s\n"
        f"ON {on}\n"
        f"WHEN MATCHED THEN UPDATE SET {updates}\n"
        f"WHEN NOT MATCHED THEN INSERT ({', '.join(cols)}) "
        f"VALUES ({', '.join(f's.{c}' for c in cols)})"
    )


async def execute_maintenance(
    cursor,
    target_table: str,
    op: str,
    params: dict[str, str],
) -> QueryInfo:
    """Run one Iceberg maintenance op via ``ALTER TABLE ... EXECUTE``.

    ``params`` values are inlined as Trino named args — callers must have
    validated them (``config.validate_maintenance_config``); we don't escape
    because Trino's only legitimate values are duration/DataSize literals.
    """
    args = ", ".join(f"{k} => '{v}'" for k, v in params.items())
    sql = f"ALTER TABLE {target_table} EXECUTE {op}({args})" if args else f"ALTER TABLE {target_table} EXECUTE {op}"
    log.info("%s: maintenance — %s", target_table, sql)
    return await _execute_tracked(cursor, sql, stage=f"maintenance_{op}")


def _floor_resume_to_chunk(
    start: datetime,
    resume_from: datetime,
    chunk: str,
) -> datetime:
    """Floor a resume marker to its containing chunk start, clamped to never
    precede ``start``. The containing chunk is re-MERGEd in full (idempotent,
    gap-free even if ``full_refresh_chunk`` changed); the ``max`` clamp stops a
    stale marker from resuming before the window's own beginning.
    """
    resume_start = expand_to_bucket_bounds(resume_from, resume_from, chunk)[0]
    return max(start, resume_start)


async def _backfill_ranges(
    cursor,
    view: ViewConfig,
    parsed: ParsedView,
    *,
    resume_from: datetime | None = None,
) -> list[tuple[datetime, datetime]]:
    """Ordered (start, end) ranges for a full refresh (1 = single-shot, N =
    chunked, empty = empty source).

    Always recomputes from the source's beginning so overwritten historical
    buckets are re-merged, not skipped (#62). ``resume_from`` (this run's own
    committed progress, never the target's data) skips already-committed chunks.
    """
    source_range = await get_source_column_range(
        cursor,
        parsed.source_table,
        parsed.filter_column,
    )
    if source_range is None:
        log.info("%s: source %s is empty, nothing to backfill", view.name, parsed.source_table)
        return []

    chunk = view.full_refresh_chunk or parsed.granularity
    start, end = expand_to_bucket_bounds(source_range[0], source_range[1], chunk)

    if view.full_refresh_chunk is None:
        return [(start, end)]  # single-shot full refresh

    # Chunked. Config validation guarantees bucket_alias is set whenever
    # full_refresh_chunk is — assert so a future refactor can't silently
    # re-introduce a fallback that breaks chunking.
    assert parsed.bucket_alias is not None, (
        "chunked full refresh requires bucket_alias; validate_chunk_compatibility should have rejected this view"
    )
    if resume_from is not None:
        start = _floor_resume_to_chunk(start, resume_from, view.full_refresh_chunk)
    return list(walk_buckets(start, end, view.full_refresh_chunk))


async def execute_refresh(
    cursor,
    view: ViewConfig,
    target_table: str,
    parsed: ParsedView,
    value_columns: list[str],
    *,
    max_snapshot: int,
    incremental_range: tuple[datetime, datetime] | None = None,
    resume_from: datetime | None = None,
) -> AsyncIterator[QueryInfo]:
    """Execute a refresh as a sequence of per-range MERGE commits.

    ``max_snapshot`` pins the source read: every chunk's MERGE source gets
    ``FROM <source> FOR VERSION AS OF max_snapshot`` (``inject_version_pin``) so
    all chunks see one immutable source state — no snapshot mixing (#62).

    - ``incremental_range`` given → one MERGE over it, or N bucket-aligned
      per-chunk MERGEs when ``view.full_refresh_chunk`` is set (#61: a large
      catch-up window would otherwise OOM as a single MERGE).
    - ``view.full_refresh_chunk`` set (no incremental_range) → N MERGEs from the
      source's beginning.
    - otherwise → one MERGE over the full source range (single-shot full).

    ``resume_from`` (``current_work.work_last_merged_chunk``) skips chunks
    at/below its containing chunk — one resume path for both full and
    incremental, since the window itself is pinned by ``max_snapshot``.

    Yields one ``QueryInfo`` per committed MERGE; caller cancels by ``break``.
    """
    if incremental_range is not None:
        if view.full_refresh_chunk:
            # Re-align the half-open ``[start, end)`` outward to the chunk grid.
            # Expand the last *instant* (``end`` exclusive → ``end - 1µs``) so an
            # already chunk-aligned end isn't over-expanded by a whole empty chunk.
            r_start, r_end = incremental_range
            start, end = expand_to_bucket_bounds(r_start, r_end - timedelta(microseconds=1), view.full_refresh_chunk)
            if resume_from is not None:
                start = _floor_resume_to_chunk(start, resume_from, view.full_refresh_chunk)
            ranges: list[tuple[datetime, datetime]] = list(walk_buckets(start, end, view.full_refresh_chunk))
            stage = "chunk_merge"
        else:
            ranges = [incremental_range]
            stage = "merge"
    else:
        ranges = await _backfill_ranges(cursor, view, parsed, resume_from=resume_from)
        stage = "chunk_merge" if view.full_refresh_chunk else "merge"

    total = len(ranges)
    for i, (start, end) in enumerate(ranges, start=1):
        src = inject_range_filter(view.query, parsed.filter_column, start, end)
        # Pin every chunk to the run's snapshot (no mixing); parser guarantees
        # single-source, so one pin suffices.
        src = inject_version_pin(src, parsed.source_table, max_snapshot)
        sql = build_merge_sql(target_table, src, parsed.merge_keys, value_columns)
        log.info("%s: %s %d/%d [%s, %s) @ snapshot %d", view.name, stage, i, total, start, end, max_snapshot)
        yield await _execute_tracked(
            cursor,
            sql,
            stage,
            range_start=start,
            range_end=end,
            chunks_done=i,
            chunks_total=total,
        )
