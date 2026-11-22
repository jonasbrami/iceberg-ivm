"""Refresh executor: one MERGE per bucket-aligned time range.

Each MERGE makes the target's rows in ``[start, end)`` exactly equal to the
view query's result for that range: new groups are inserted, changed groups
updated, and groups that no longer exist deleted — in a single atomic commit.

A refresh is one MERGE, or one per chunk when ``full_refresh_chunk`` is set.
Every chunk reads the source ``FOR VERSION AS OF`` the run's pinned snapshot,
so a multi-chunk run never mixes source states, and a crash resumes from the
last committed chunk.
"""

from __future__ import annotations

import logging
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime, timedelta

from iceberg_ivm.config import ViewConfig
from iceberg_ivm.detector import column_range, expand_to_bucket_bounds, walk_buckets
from iceberg_ivm.query_parser import DIALECT, ParsedView, quote, timestamp_literal

log = logging.getLogger(__name__)


@dataclass
class QueryInfo:
    """One Trino query run by a refresh: link + stats for the UI and metrics."""

    query_id: str
    info_uri: str
    stage: str  # "merge" | "chunk_merge" | "maintenance_<op>"
    started_at: float
    elapsed_ms: float
    processed_rows: int = 0
    processed_bytes: int = 0
    range_start: datetime | None = None
    range_end: datetime | None = None
    chunks_done: int = 0  # 1-based count of committed chunks so far
    chunks_total: int = 0  # 0 when not chunked


async def _execute_tracked(cursor, sql: str, stage: str, **extra) -> QueryInfo:
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
        **extra,
    )


def build_merge_sql(
    target_table: str,
    range_query: str,
    merge_keys: tuple[str, ...],
    bucket: str,
    value_columns: list[str],
    start: datetime,
    end: datetime,
) -> str:
    """MERGE that replaces the target's ``[start, end)`` bucket range with ``range_query``.

    The source side full-outer-joins the fresh aggregates (``n``) with the keys
    already in the target for that range (``o``); keys only on the ``o`` side
    are flagged ``__ivm_gone`` and deleted. Keys compare NULL-safely, except the
    bucket, which is never NULL inside a time range and gives Trino an
    equi-join to hash on.
    """

    def match(left: str, right: str) -> str:
        return " AND ".join(
            f"{left}.{quote(k)} {'=' if k == bucket else 'IS NOT DISTINCT FROM'} {right}.{quote(k)}" for k in merge_keys
        )

    lo = timestamp_literal(start).sql(dialect=DIALECT)
    hi = timestamp_literal(end).sql(dialect=DIALECT)
    picked = [f'IF(n."__ivm_new", n.{quote(k)}, o.{quote(k)}) AS {quote(k)}' for k in merge_keys]
    picked += [f"n.{quote(c)}" for c in value_columns]
    columns = [quote(c) for c in (*merge_keys, *value_columns)]
    update = (
        f"WHEN MATCHED THEN UPDATE SET {', '.join(f'{quote(c)} = s.{quote(c)}' for c in value_columns)}\n"
        if value_columns
        else ""
    )
    return (
        f"MERGE INTO {target_table} AS t\n"
        f"USING (\n"
        f'  SELECT {", ".join(picked)}, n."__ivm_new" IS NULL AS "__ivm_gone"\n'
        f'  FROM (SELECT *, true AS "__ivm_new" FROM (\n{range_query}\n)) AS n\n'
        f"  FULL OUTER JOIN (\n"
        f"    SELECT {', '.join(quote(k) for k in merge_keys)} FROM {target_table}\n"
        f"    WHERE {quote(bucket)} >= {lo} AND {quote(bucket)} < {hi}\n"
        f"  ) AS o ON {match('n', 'o')}\n"
        f") AS s\n"
        f"ON {match('t', 's')}\n"
        f'WHEN MATCHED AND s."__ivm_gone" THEN DELETE\n'
        f"{update}"
        f'WHEN NOT MATCHED AND NOT s."__ivm_gone" THEN INSERT ({", ".join(columns)}) '
        f"VALUES ({', '.join(f's.{c}' for c in columns)})"
    )


async def execute_maintenance(cursor, target_table: str, op: str, params: dict[str, str]) -> QueryInfo:
    """Run one Iceberg maintenance op via ``ALTER TABLE ... EXECUTE``.

    ``params`` values are validated duration / data-size literals (see config).
    """
    args = ", ".join(f"{k} => '{v}'" for k, v in params.items())
    sql = f"ALTER TABLE {target_table} EXECUTE {op}" + (f"({args})" if args else "")
    log.info("%s: maintenance — %s", target_table, sql)
    return await _execute_tracked(cursor, sql, stage=f"maintenance_{op}")


def _window_ranges(
    lo: datetime,
    hi_inclusive: datetime,
    chunk: str | None,
    granularity: str,
    resume_from: datetime | None,
) -> list[tuple[datetime, datetime]]:
    """Inclusive ``[lo, hi]`` → ordered, bucket-aligned half-open ranges.

    ``chunk`` None → a single range snapped to the view's granularity; else one
    range per chunk. A resumed run restarts at the chunk containing
    ``resume_from`` — re-merging it is idempotent, and stays gap-free even if
    ``full_refresh_chunk`` changed since the run began.
    """
    start, end = expand_to_bucket_bounds(lo, hi_inclusive, chunk or granularity)
    if chunk is None:
        return [(start, end)]
    if resume_from is not None:
        start = max(start, expand_to_bucket_bounds(resume_from, resume_from, chunk)[0])
    return list(walk_buckets(start, end, chunk))


def _hull(*ranges: tuple[datetime, datetime] | None) -> tuple[datetime, datetime] | None:
    present = [r for r in ranges if r is not None]
    if not present:
        return None
    return min(r[0] for r in present), max(r[1] for r in present)


async def execute_refresh(
    cursor,
    view: ViewConfig,
    parsed: ParsedView,
    value_columns: list[str],
    *,
    snapshot: int,
    window: tuple[datetime, datetime] | None = None,
    resume_from: datetime | None = None,
) -> AsyncIterator[QueryInfo]:
    """Refresh ``view`` as of source ``snapshot``, yielding one ``QueryInfo``
    per committed MERGE. Callers stop early with ``break``.

    ``window`` is the detector's bucket-aligned ``[start, end)``. Without one
    (full refresh) the window spans everything the source has and everything
    the target already holds, so the whole target ends up equal to the query.
    """
    if window is not None:
        # Inclusive hi, so an already chunk-aligned end doesn't grow by an empty chunk.
        lo, hi = window[0], window[1] - timedelta(microseconds=1)
    else:
        whole = _hull(
            await column_range(cursor, parsed.source_table, parsed.filter_column),
            await column_range(cursor, view.target_table, parsed.bucket_alias),
        )
        if whole is None:
            log.info("%s: source and target are empty, nothing to refresh", view.name)
            return
        lo, hi = whole

    ranges = _window_ranges(lo, hi, view.full_refresh_chunk, parsed.granularity, resume_from)
    stage = "chunk_merge" if view.full_refresh_chunk else "merge"
    for i, (start, end) in enumerate(ranges, start=1):
        sql = build_merge_sql(
            view.target_table,
            parsed.range_query(start, end, snapshot),
            parsed.merge_keys,
            parsed.bucket_alias,
            value_columns,
            start,
            end,
        )
        log.info("%s: %s %d/%d [%s, %s) @ snapshot %d", view.name, stage, i, len(ranges), start, end, snapshot)
        yield await _execute_tracked(
            cursor, sql, stage, range_start=start, range_end=end, chunks_done=i, chunks_total=len(ranges)
        )
