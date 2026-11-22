"""Change detection from Iceberg metadata only — no source data is scanned.

* ``$refs`` gives the main branch's current snapshot.
* ``$snapshots`` gives the ancestry from the bookmark to it, and each
  snapshot's operation.
* ``$all_entries`` gives the min/max of the time column over the data files
  those snapshots added or removed: the time range that needs recomputing.

That range is then widened to whole ``date_trunc`` buckets, so every group the
new rows touch is recomputed from complete source data.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum, auto

log = logging.getLogger(__name__)


class RefreshAction(Enum):
    NO_CHANGE = auto()
    FULL_REFRESH = auto()
    INCREMENTAL = auto()


class FullRefreshNeeded(Exception):
    """The change since the bookmark can't be bounded to a time range: the
    bookmark is no longer an ancestor of the current snapshot (expired, rolled
    back, table replaced), rows were deleted, or new files lack statistics."""


class MissingStatsError(Exception):
    """Data files have no min/max statistics for the time column."""


class SourceNotFound(Exception):
    """The source table (or its schema) doesn't exist yet — e.g. the upstream
    view of a chain hasn't created its target. Not an error: wait for it."""


@dataclass
class ChangeResult:
    action: RefreshAction
    current_snapshot: int | None = None
    filter_range: tuple[datetime, datetime] | None = None  # bucket-aligned [start, end)


def system_table(table: str, suffix: str) -> str:
    """``catalog.schema."table$suffix"`` — Trino's name for an Iceberg metadata table."""
    head, _, name = table.rpartition(".")
    return f'{head}."{name}${suffix}"' if head else f'"{name}${suffix}"'


async def get_current_snapshot(cursor, table: str) -> int | None:
    """The snapshot the main branch points at, or None for an empty table."""
    try:
        await cursor.execute(f"SELECT snapshot_id FROM {system_table(table, 'refs')} WHERE name = 'main'")
    except Exception as e:
        if getattr(e, "error_name", None) in ("TABLE_NOT_FOUND", "SCHEMA_NOT_FOUND"):
            raise SourceNotFound(f"waiting for {table} to exist") from e
        raise
    row = await cursor.fetchone()
    return row[0] if row else None


async def snapshot_exists(cursor, table: str, snapshot_id: int) -> bool:
    await cursor.execute(f"SELECT 1 FROM {system_table(table, 'snapshots')} WHERE snapshot_id = {snapshot_id}")
    return (await cursor.fetchone()) is not None


async def snapshots_between(cursor, table: str, since: int, upto: int) -> list[tuple[int, str]]:
    """``(snapshot_id, operation)`` of every snapshot after ``since`` up to and
    including ``upto``, oldest first, following ``upto``'s parent chain.

    Raises ``FullRefreshNeeded`` if ``since`` is not an ancestor of ``upto``.
    """
    await cursor.execute(f"SELECT snapshot_id, parent_id, operation FROM {system_table(table, 'snapshots')}")
    parents = {sid: (pid, op) for sid, pid, op in await cursor.fetchall()}
    chain: list[tuple[int, str]] = []
    current: int | None = upto
    while current != since:
        if current not in parents:
            raise FullRefreshNeeded(
                f"{table}: bookmark snapshot {since} is not an ancestor of {upto} "
                "(expired, rolled back, or the table was replaced)"
            )
        parent, operation = parents[current]
        chain.append((current, operation))
        current = parent
    return chain[::-1]


def _bounds(rows: list, table: str, column: str) -> tuple[datetime, datetime] | None:
    """Min lower bound and max upper bound of ``column`` across ``(readable_metrics,)``
    rows of data files. A file whose values are all NULL is skipped; any other
    file without bounds raises ``MissingStatsError``."""
    low: datetime | None = None
    high: datetime | None = None
    for (raw,) in rows:
        metrics = (json.loads(raw) if isinstance(raw, str) else raw) or {}
        stats = metrics.get(column) or {}
        lower, upper = stats.get("lower_bound"), stats.get("upper_bound")
        if lower is None or upper is None:
            values = stats.get("value_count")
            if values is not None and values == stats.get("null_value_count"):
                continue
            raise MissingStatsError(
                f"{table}: data files have no min/max statistics for column {column!r}, so the changed "
                f"time range is unknown. Enable them with write.metadata.metrics.column.{column}=full "
                "and rewrite (optimize) the files written without them."
            )
        lower, upper = datetime.fromisoformat(str(lower)), datetime.fromisoformat(str(upper))
        low = lower if low is None else min(low, lower)
        high = upper if high is None else max(high, upper)
    return None if low is None else (low, high)


async def changed_range(cursor, table: str, snapshot_ids: list[int], column: str) -> tuple[datetime, datetime] | None:
    """Raw ``(min, max)`` of ``column`` over data files added or removed by ``snapshot_ids``."""
    ids = ", ".join(str(s) for s in snapshot_ids)
    await cursor.execute(
        f"SELECT readable_metrics FROM {system_table(table, 'all_entries')} "
        f"WHERE snapshot_id IN ({ids}) AND status IN (1, 2) AND data_file.content = 0"
    )
    return _bounds(await cursor.fetchall(), table, column)


async def column_range(cursor, table: str, column: str) -> tuple[datetime, datetime] | None:
    """Raw ``(min, max)`` of ``column`` over the table's current data files, or None if empty."""
    await cursor.execute(f"SELECT readable_metrics FROM {system_table(table, 'files')} WHERE content = 0")
    return _bounds(await cursor.fetchall(), table, column)


def midnight(dt: datetime) -> datetime:
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def _add_months(dt: datetime, n: int) -> datetime:
    # dt is assumed to be the first of a month; add n months with year rollover.
    idx = (dt.year * 12 + dt.month - 1) + n
    return dt.replace(year=idx // 12, month=(idx % 12) + 1)


def _floor_millisecond(d: datetime) -> datetime:
    return d.replace(microsecond=(d.microsecond // 1000) * 1000)


def _floor_second(d: datetime) -> datetime:
    return d.replace(microsecond=0)


def _floor_minute(d: datetime) -> datetime:
    return d.replace(second=0, microsecond=0)


def _floor_hour(d: datetime) -> datetime:
    return d.replace(minute=0, second=0, microsecond=0)


def _floor_week(d: datetime) -> datetime:
    return midnight(d - timedelta(days=d.weekday()))


def _floor_month(d: datetime) -> datetime:
    return midnight(d).replace(day=1)


def _floor_quarter(d: datetime) -> datetime:
    return midnight(d).replace(month=((d.month - 1) // 3) * 3 + 1, day=1)


def _floor_year(d: datetime) -> datetime:
    return midnight(d).replace(month=1, day=1)


# Per-granularity bucket math: (floor_to_bucket_start, next_bucket_after).
# ``expand_to_bucket_bounds`` applies ``floor`` to min_ts and ``next_bucket`` to max_ts.
# ``walk_buckets`` iterates by repeatedly applying ``next_bucket``.
_BUCKETS: dict[str, tuple[Callable[[datetime], datetime], Callable[[datetime], datetime]]] = {
    "millisecond": (_floor_millisecond, lambda d: _floor_millisecond(d) + timedelta(milliseconds=1)),
    "second": (_floor_second, lambda d: _floor_second(d) + timedelta(seconds=1)),
    "minute": (_floor_minute, lambda d: _floor_minute(d) + timedelta(minutes=1)),
    "hour": (_floor_hour, lambda d: _floor_hour(d) + timedelta(hours=1)),
    "day": (midnight, lambda d: midnight(d) + timedelta(days=1)),
    "week": (_floor_week, lambda d: _floor_week(d) + timedelta(weeks=1)),
    "month": (_floor_month, lambda d: _add_months(_floor_month(d), 1)),
    "quarter": (_floor_quarter, lambda d: _add_months(_floor_quarter(d), 3)),
    "year": (_floor_year, lambda d: _floor_year(d).replace(year=d.year + 1)),
}


def walk_buckets(
    start: datetime,
    end: datetime,
    granularity: str,
) -> Iterator[tuple[datetime, datetime]]:
    """Yield half-open ``[chunk_start, chunk_end)`` intervals, one bucket at a time.

    ``start``/``end`` must already be bucket-aligned (caller snaps them via
    ``expand_to_bucket_bounds``). Yields nothing if ``start >= end``.
    """
    if granularity not in _BUCKETS:
        raise ValueError(f"unsupported granularity: {granularity}")
    _, nxt = _BUCKETS[granularity]
    cur = start
    while cur < end:
        step_end = nxt(cur)
        yield (cur, min(step_end, end))
        cur = step_end


def expand_to_bucket_bounds(
    min_ts: datetime,
    max_ts: datetime,
    granularity: str,
) -> tuple[datetime, datetime]:
    """Expand a timestamp range outward to full GROUP BY bucket boundaries.

    This is the **load-bearing correctness invariant** of iceberg-ivm:
    ``expand_to_bucket_bounds`` is the Python inverse of Trino's
    ``date_trunc(granularity, col)``. The detector reads per-file min/max
    timestamps from ``$all_entries.readable_metrics`` and snaps that raw
    range outward so the resulting ``[start, end)`` filter covers *every*
    GROUP BY bucket that any newly-added row could fall into. Without the
    snap, a late row arriving in the middle of a bucket would cause that
    bucket's aggregate to be recomputed from a partial input — silent data
    corruption.

    We do this in Python rather than letting Trino do it because the
    snapped range is then injected as a literal ``WHERE col >= TIMESTAMP
    '...' AND col < TIMESTAMP '...'`` predicate. Iceberg partition pruning
    requires literal bounds; a ``date_trunc`` expression on the right-hand
    side would defeat pruning and force a full source scan.

    Returns ``(start, end)`` with ``start`` on a bucket boundary ≤ ``min_ts``
    and ``end`` on the next bucket boundary > ``max_ts``.
    """
    floor, nxt = _BUCKETS[granularity]
    return floor(min_ts), nxt(max_ts)


# Snapshot operations, by what they mean for a view:
#   append, overwrite — rows added (or rewritten, e.g. an upstream view's MERGE):
#                       recompute the buckets their data files cover
#   replace           — compaction, no logical change: skip
#   delete, other     — rows removed with no bound on where: full refresh
CHANGE_OPS = frozenset({"append", "overwrite"})
NOOP_OPS = frozenset({"replace"})


async def incremental_range_since(
    cursor,
    source_table: str,
    filter_column: str,
    granularity: str,
    since: int,
    upto: int,
) -> tuple[datetime, datetime] | None:
    """Bucket-aligned ``[start, end)`` touched by snapshots in ``(since, upto]``,
    or None when they hold no data change (only compaction).

    Raises ``FullRefreshNeeded`` when the change can't be bounded.
    """
    snapshots = await snapshots_between(cursor, source_table, since, upto)
    unbounded = sorted({op for _, op in snapshots if op not in CHANGE_OPS | NOOP_OPS})
    if unbounded:
        raise FullRefreshNeeded(f"{source_table}: snapshot operations {unbounded} remove rows without a time bound")
    changed = [sid for sid, op in snapshots if op in CHANGE_OPS]
    if not changed:
        return None
    try:
        raw = await changed_range(cursor, source_table, changed, filter_column)
    except MissingStatsError as e:
        raise FullRefreshNeeded(str(e)) from e
    if raw is None:
        return None
    snapped = expand_to_bucket_bounds(*raw, granularity)
    log.info("%s: %s in [%s, %s] → recompute [%s, %s)", source_table, filter_column, *raw, *snapped)
    return snapped


async def detect_changes(
    cursor,
    source_table: str,
    filter_column: str,
    granularity: str,
    last_snapshot: int | None,
) -> ChangeResult:
    """Decide what a refresh must do to bring the view from ``last_snapshot``
    (None: never refreshed) to the source's current snapshot."""
    current = await get_current_snapshot(cursor, source_table)
    if current is None or current == last_snapshot:
        return ChangeResult(RefreshAction.NO_CHANGE, current_snapshot=current)
    if last_snapshot is None:
        return ChangeResult(RefreshAction.FULL_REFRESH, current_snapshot=current)
    try:
        window = await incremental_range_since(cursor, source_table, filter_column, granularity, last_snapshot, current)
    except FullRefreshNeeded as e:
        log.warning("%s → full refresh", e)
        return ChangeResult(RefreshAction.FULL_REFRESH, current_snapshot=current)
    if window is None:
        return ChangeResult(RefreshAction.NO_CHANGE, current_snapshot=current)
    return ChangeResult(RefreshAction.INCREMENTAL, current_snapshot=current, filter_range=window)
