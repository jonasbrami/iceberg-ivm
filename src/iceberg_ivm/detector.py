"""Change detection via Iceberg file-level metadata ($snapshots, $all_entries).

Instead of diffing partitions, we read column-level min/max from new files'
readable_metrics. This works regardless of partition scheme and correctly
handles GROUP BY expressions that span multiple source partitions.
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


class ExpiredSnapshotError(Exception):
    """last_source_snapshot no longer present in $snapshots (Iceberg expired it)."""


class MissingFilterColumnError(Exception):
    """$all_entries has rows but none expose filter_column — config error."""


class UnexpectedOperationError(Exception):
    """Source snapshot used a `delete` (or otherwise unknown) op — the
    no-data-loss assumption is violated. Allowed ops are `append` and
    `overwrite` (real data changes that drive incremental refresh) and
    `replace` (compaction, skipped)."""


@dataclass
class ChangeResult:
    action: RefreshAction
    current_snapshot: int | None = None
    filter_range: tuple[datetime, datetime] | None = None  # snapped (start, end)


def system_table(table: str, suffix: str) -> str:
    """Build a reference to an Iceberg system table.

    Trino syntax: catalog.schema."table$suffix"
    """
    parts = table.rsplit(".", 1)
    if len(parts) == 1:
        return f'"{parts[0]}${suffix}"'
    return f'{parts[0]}."{parts[1]}${suffix}"'


async def get_current_snapshot(cursor, source_table: str) -> int | None:
    """Get latest snapshot_id from source table. Metadata-only.

    The (committed_at, snapshot_id) tiebreak matches get_snapshots_since:
    sibling snapshots can share a millisecond-precision committed_at, so
    ordering by committed_at alone is non-deterministic and would let the
    next tick observe last_snapshot == current_snap and skip real new data.
    """
    await cursor.execute(
        f"SELECT snapshot_id FROM {system_table(source_table, 'snapshots')} "
        f"ORDER BY committed_at DESC, snapshot_id DESC LIMIT 1"
    )
    row = await cursor.fetchone()
    return row[0] if row else None


async def get_snapshots_since(cursor, source_table: str, last_snap: int) -> list[dict]:
    """Return snapshots strictly after last_snap, ordered by (committed_at, snapshot_id).

    The snapshot_id tiebreak matters: committed_at is millisecond-precision, so
    sibling snapshots can share a timestamp. Raises ExpiredSnapshotError if
    last_snap itself has been expired from $snapshots.
    """
    snaps = system_table(source_table, "snapshots")
    await cursor.execute(f"SELECT committed_at FROM {snaps} WHERE snapshot_id = {last_snap}")
    row = await cursor.fetchone()
    if row is None:
        raise ExpiredSnapshotError(
            f"{source_table}: last_source_snapshot={last_snap} is not in $snapshots "
            f"(expired?). Cannot compute the set of new snapshots."
        )
    committed_at = row[0]
    await cursor.execute(
        f"SELECT snapshot_id, operation FROM {snaps} "
        f"WHERE committed_at > TIMESTAMP '{committed_at}' "
        f"   OR (committed_at = TIMESTAMP '{committed_at}' AND snapshot_id > {last_snap}) "
        f"ORDER BY committed_at, snapshot_id"
    )
    return [{"snapshot_id": r[0], "operation": r[1]} for r in await cursor.fetchall()]


# Snapshot ops the detector understands. `delete` and any unknown op fall
# through to UnexpectedOperationError below.
#
#   append    — new data, drive incremental refresh
#   overwrite — MERGE INTO (e.g. an upstream chained MV's refresh): treat
#               its added files exactly like an append for file-stats
#               purposes and drive incremental refresh
#   replace   — compaction / OPTIMIZE: no logical data change, skip the
#               refresh but advance the bookmark
CHANGE_OPS = frozenset({"append", "overwrite"})
NOOP_OPS = frozenset({"replace"})
ALLOWED_OPS = CHANGE_OPS | NOOP_OPS


async def get_new_files_column_range(
    cursor,
    source_table: str,
    snapshot_ids: list[int],
    filter_column: str,
) -> tuple[datetime, datetime] | None:
    """Read min/max of filter_column across files added in given snapshots.

    Uses $all_entries.readable_metrics — metadata-only, no data scan.
    Returns (min_dt, max_dt) or None if no data files were found.
    """
    snap_list = ", ".join(str(s) for s in snapshot_ids)
    await cursor.execute(
        f"SELECT readable_metrics "
        f"FROM {system_table(source_table, 'all_entries')} "
        f"WHERE snapshot_id IN ({snap_list}) AND status = 1"  # ADDED data files
    )
    rows = await cursor.fetchall()
    if not rows:
        return None
    lows, highs, saw = _iter_column_bounds(rows, filter_column)
    if not saw:
        raise MissingFilterColumnError(
            f"{source_table}: filter_column {filter_column!r} not found in any "
            f"file's readable_metrics. Check the view config."
        )
    if not lows or not highs:
        return None
    return (min(lows), max(highs))


def _iter_column_bounds(
    rows: list,
    column: str,
) -> tuple[list[datetime], list[datetime], bool]:
    """Extract ``(lower_bounds, upper_bounds, saw_column)`` from a list of
    ``(readable_metrics,)`` rows. ``saw_column`` is True iff at least one
    file's metrics included ``column`` — distinguishes "no files" from
    "files exist but column is absent" for missing-column detection.
    """
    lows: list[datetime] = []
    highs: list[datetime] = []
    saw = False
    for (metrics_raw,) in rows:
        if metrics_raw is None:
            continue
        metrics = metrics_raw if isinstance(metrics_raw, dict) else json.loads(metrics_raw)
        col = metrics.get(column)
        if col is None:
            continue
        saw = True
        lb = col.get("lower_bound")
        ub = col.get("upper_bound")
        if lb is not None:
            lows.append(datetime.fromisoformat(str(lb)))
        if ub is not None:
            highs.append(datetime.fromisoformat(str(ub)))
    return lows, highs, saw


async def get_source_column_range(
    cursor,
    source_table: str,
    filter_column: str,
) -> tuple[datetime, datetime] | None:
    """Read ``(min, max)`` of ``filter_column`` across all live files in the
    current snapshot of ``source_table``.

    Uses ``$files.readable_metrics`` — this returns the live file set of
    the current snapshot, not per-commit manifest rows. Metadata-only.
    Raises ``MissingFilterColumnError`` if files exist but none carry
    ``filter_column`` metrics. Returns ``None`` for empty tables.
    """
    await cursor.execute(f"SELECT readable_metrics FROM {system_table(source_table, 'files')}")
    rows = await cursor.fetchall()
    if not rows:
        return None
    lows, highs, saw = _iter_column_bounds(rows, filter_column)
    if not saw:
        raise MissingFilterColumnError(
            f"{source_table}: filter_column {filter_column!r} not found in any "
            f"file's readable_metrics. Check the view config."
        )
    if not lows or not highs:
        return None
    return (min(lows), max(highs))


async def get_target_bucket_max(
    cursor,
    target_table: str,
    bucket_alias: str,
) -> datetime | None:
    """Read the max ``upper_bound`` of ``bucket_alias`` across live data
    files in ``target_table``.

    The chunked full-refresh uses this as its resume point: the target's
    own Iceberg metadata, rather than a separate cursor key, records what
    has been committed. Returns ``None`` for an empty target or one whose
    files carry no bounds on ``bucket_alias`` yet.

    Filters ``content = 0`` (data files) so V2 position/equality delete
    files don't skew the max upward and cause the resume to skip live
    buckets.
    """
    await cursor.execute(f"SELECT readable_metrics FROM {system_table(target_table, 'files')} WHERE content = 0")
    rows = await cursor.fetchall()
    if not rows:
        return None
    _, highs, _ = _iter_column_bounds(rows, bucket_alias)
    return max(highs) if highs else None


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


async def detect_changes(
    cursor,
    source_table: str,
    filter_column: str,
    filter_granularity: str,
    last_snapshot: int | None,
) -> ChangeResult:
    """Detect what changed in the source table since last_snapshot.

    Uses file-level column statistics from $all_entries.readable_metrics
    to compute the minimum range of data that needs recomputing, snapped
    to GROUP BY bucket boundaries.
    """
    current_snap = await get_current_snapshot(cursor, source_table)
    if current_snap is None:
        log.debug("%s: no snapshots found", source_table)
        return ChangeResult(action=RefreshAction.NO_CHANGE)

    if current_snap == last_snapshot:
        log.debug("%s: snapshot unchanged (%d)", source_table, current_snap)
        return ChangeResult(action=RefreshAction.NO_CHANGE, current_snapshot=current_snap)

    # First run → full refresh
    if last_snapshot is None:
        log.info("%s: first run (no last_snapshot) → full refresh", source_table)
        return ChangeResult(action=RefreshAction.FULL_REFRESH, current_snapshot=current_snap)

    # Get intermediate snapshots (raises ExpiredSnapshotError if last_snap
    # has been expired from the source).
    snapshots = await get_snapshots_since(cursor, source_table, last_snapshot)
    if not snapshots:
        log.debug("%s: no new snapshots since %d", source_table, last_snapshot)
        return ChangeResult(action=RefreshAction.NO_CHANGE, current_snapshot=current_snap)

    ops = [s["operation"] for s in snapshots]
    log.debug("%s: %d new snapshots, operations: %s", source_table, len(snapshots), ops)

    # Classify operations:
    #   append, overwrite → real data changes, drive incremental refresh
    #   replace           → compaction (files rewritten, no data change) — skip
    #   anything else     → fail loudly (delete, or some new Iceberg op)
    unknown = [op for op in ops if op not in ALLOWED_OPS]
    if unknown:
        raise UnexpectedOperationError(
            f"{source_table}: unexpected snapshot operations {unknown} — "
            f"source must only see append / overwrite (data changes) or "
            f"replace (compaction); pure deletes are not supported."
        )

    change_snaps = [s for s in snapshots if s["operation"] in CHANGE_OPS]
    if not change_snaps:
        # Only compactions since last_snap; advance state, don't refresh.
        log.info(
            "%s: only compaction (replace) snapshots since %d → advance state, skip",
            source_table,
            last_snapshot,
        )
        return ChangeResult(action=RefreshAction.NO_CHANGE, current_snapshot=current_snap)

    # Read column range from files added in CHANGE snapshots only
    # (append + overwrite). Including compaction-added files would
    # uselessly expand the range with rewritten copies of unchanged data.
    snap_ids = [s["snapshot_id"] for s in change_snaps]
    col_range = await get_new_files_column_range(cursor, source_table, snap_ids, filter_column)
    if col_range is None:
        # New snapshots but no data files (e.g. compaction-only)
        log.debug("%s: new snapshots but no data files (compaction?)", source_table)
        return ChangeResult(action=RefreshAction.NO_CHANGE, current_snapshot=current_snap)

    min_ts, max_ts = col_range
    snapped_start, snapped_end = expand_to_bucket_bounds(min_ts, max_ts, filter_granularity)

    log.info(
        "file stats: %s in [%s, %s] → snapped to [%s, %s)",
        filter_column,
        min_ts,
        max_ts,
        snapped_start,
        snapped_end,
    )

    return ChangeResult(
        action=RefreshAction.INCREMENTAL,
        current_snapshot=current_snap,
        filter_range=(snapped_start, snapped_end),
    )
