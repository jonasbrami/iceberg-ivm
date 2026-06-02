"""Refresh executor: builds and runs MERGE SQL against Trino.

One refresh = one or more MERGE commits over bucket-aligned time ranges:

  - incremental refresh: one MERGE over the detector's snapped range.
  - full refresh: one MERGE over the source's whole range.
  - chunked full refresh: N MERGEs, one per chunk, with per-chunk commit so
    a crash or restart resumes from target metadata.

``execute_refresh`` is a single async generator that yields one ``QueryInfo``
per committed MERGE. Callers cancel via ``break`` — no callback plumbing.
"""

from __future__ import annotations

import logging
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime

from iceberg_ivm.config import ViewConfig
from iceberg_ivm.detector import (
    expand_to_bucket_bounds,
    get_source_column_range,
    walk_buckets,
)
from iceberg_ivm.query_parser import ParsedView, inject_range_filter

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


async def _backfill_ranges(
    cursor,
    view: ViewConfig,
    parsed: ParsedView,
    *,
    resume_from: datetime | None = None,
) -> list[tuple[datetime, datetime]]:
    """Return the ordered (start, end) ranges for a full refresh.

    One element = single-shot; N elements = chunked. Empty = empty source.

    A full refresh always recomputes from the **source's beginning** so a
    source that overwrote historical buckets is fully re-merged — never
    skipped (issue #62). The sole way to skip a chunk is ``resume_from``: a
    committed-progress marker produced by *this* from-beginning run, used to
    resume an interrupted backfill without redoing committed chunks. It is
    NOT derived from the target's data (which may be stale historical rows
    from an earlier incremental cursor) — that conflation was the bug.
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
        # Floor the committed-progress marker to the start of the chunk it
        # lands in so every emitted range is a full chunk. The containing
        # chunk is re-MERGEd in full — idempotent, and gap-free even if
        # full_refresh_chunk changed mid-backfill. ``max`` guards against a
        # marker that predates the current source start (e.g. source data
        # aged out): never resume before the source's beginning.
        resume_start = expand_to_bucket_bounds(resume_from, resume_from, view.full_refresh_chunk)[0]
        start = max(start, resume_start)
    return list(walk_buckets(start, end, view.full_refresh_chunk))


async def execute_refresh(
    cursor,
    view: ViewConfig,
    target_table: str,
    parsed: ParsedView,
    value_columns: list[str],
    *,
    incremental_range: tuple[datetime, datetime] | None = None,
    resume_from: datetime | None = None,
) -> AsyncIterator[QueryInfo]:
    """Execute a refresh as a sequence of per-range MERGE commits.

    - ``incremental_range`` given → one MERGE over it.
    - ``view.full_refresh_chunk`` set → N MERGEs, one per chunk, from the
      source's beginning (or from ``resume_from`` — a committed-progress
      marker for an interrupted backfill, see ``_backfill_ranges``).
    - otherwise → one MERGE over the full source range (single-shot full).

    Yields one ``QueryInfo`` per committed MERGE. Caller cancels by ``break``;
    each MERGE is one Iceberg commit, so a partial run leaves the target in a
    valid state and the caller persists the committed-progress marker so the
    next tick resumes from it.
    """
    if incremental_range is not None:
        ranges: list[tuple[datetime, datetime]] = [incremental_range]
        stage = "merge"
    else:
        ranges = await _backfill_ranges(cursor, view, parsed, resume_from=resume_from)
        stage = "chunk_merge" if view.full_refresh_chunk else "merge"

    total = len(ranges)
    for i, (start, end) in enumerate(ranges, start=1):
        src = inject_range_filter(view.query, parsed.filter_column, start, end)
        sql = build_merge_sql(target_table, src, parsed.merge_keys, value_columns)
        log.info("%s: %s %d/%d [%s, %s)", view.name, stage, i, total, start, end)
        yield await _execute_tracked(
            cursor,
            sql,
            stage,
            range_start=start,
            range_end=end,
            chunks_done=i,
            chunks_total=total,
        )
