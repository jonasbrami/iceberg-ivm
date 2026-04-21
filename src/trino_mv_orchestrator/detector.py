"""Change detection via Iceberg file-level metadata ($snapshots, $all_entries).

Instead of diffing partitions, we read column-level min/max from new files'
readable_metrics. This works regardless of partition scheme and correctly
handles GROUP BY expressions that span multiple source partitions.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum, auto
from typing import Iterator

log = logging.getLogger(__name__)


class RefreshAction(Enum):
    NO_CHANGE = auto()
    FULL_REFRESH = auto()
    INCREMENTAL = auto()


class ExpiredSnapshotError(Exception):
    """Raised when last_source_snapshot is no longer present in $snapshots.

    Iceberg snapshot expiration removed it. This orchestrator assumes the
    last processed snapshot is always retained; if it isn't, we fail loudly
    rather than silently skip ahead.
    """
    def __init__(self, source_table: str, snapshot_id: int):
        super().__init__(
            f"{source_table}: last_source_snapshot={snapshot_id} is not in "
            f"$snapshots (expired?). Cannot compute the set of new snapshots."
        )
        self.source_table = source_table
        self.snapshot_id = snapshot_id


class MissingFilterColumnError(Exception):
    """Raised when $all_entries has rows but none expose filter_column.

    The file-level min/max bounds are needed to compute the incremental
    range; if they're absent, we can't safely MERGE. Usually indicates a
    typo in the view's `filter_column`, or a schema drift where the
    column was added after some files were written.
    """
    def __init__(self, source_table: str, filter_column: str):
        super().__init__(
            f"{source_table}: filter_column {filter_column!r} not found in "
            f"any file's readable_metrics. Check the view config."
        )
        self.source_table = source_table
        self.filter_column = filter_column


class UnexpectedOperationError(Exception):
    """Raised when a source snapshot uses an operation we don't allow.

    The orchestrator assumes source tables are append-only. The only
    legitimate Iceberg snapshot operations are ``append`` (new data) and
    ``replace`` (compaction — files rewritten, no data change). Anything
    else (``overwrite``, ``delete``, or a future unknown name) violates
    the assumption and must fail loudly.
    """
    def __init__(self, source_table: str, operations: list[str]):
        super().__init__(
            f"{source_table}: unexpected snapshot operations {operations} "
            f"— source table must be append-only (compaction allowed)."
        )
        self.source_table = source_table
        self.operations = operations


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
    """Get latest snapshot_id from source table. Metadata-only."""
    await cursor.execute(
        f"SELECT snapshot_id FROM {system_table(source_table, 'snapshots')} "
        f"ORDER BY committed_at DESC LIMIT 1"
    )
    row = await cursor.fetchone()
    return row[0] if row else None


async def get_snapshots_since(cursor, source_table: str, last_snap: int) -> list[dict]:
    """Get snapshot_id and operation for all snapshots strictly after last_snap.

    Snapshot IDs are random longs in Iceberg — not sequential. We:

    1. Look up the committed_at of last_snap. If it's missing, Iceberg has
       expired it and we raise ExpiredSnapshotError rather than silently
       returning an empty list (the old behavior caused permanent view
       staleness).
    2. Return everything with a strictly greater (committed_at, snapshot_id)
       ordering. The snapshot_id tiebreak matters because committed_at is
       millisecond-precision and sibling snapshots can share a timestamp —
       a plain `committed_at > X` would drop them.
    """
    snaps_table = system_table(source_table, "snapshots")

    await cursor.execute(
        f"SELECT committed_at FROM {snaps_table} WHERE snapshot_id = {last_snap}"
    )
    row = await cursor.fetchone()
    if row is None:
        raise ExpiredSnapshotError(source_table, last_snap)

    await cursor.execute(
        f"SELECT snapshot_id, operation FROM {snaps_table} "
        f"WHERE committed_at > ("
        f"    SELECT committed_at FROM {snaps_table} WHERE snapshot_id = {last_snap}"
        f"  ) "
        f"   OR ("
        f"    committed_at = ("
        f"      SELECT committed_at FROM {snaps_table} WHERE snapshot_id = {last_snap}"
        f"    ) AND snapshot_id > {last_snap}"
        f"  ) "
        f"ORDER BY committed_at, snapshot_id"
    )
    return [{"snapshot_id": r[0], "operation": r[1]} for r in await cursor.fetchall()]


APPEND_OP = "append"
COMPACTION_OP = "replace"
ALLOWED_OPS = frozenset({APPEND_OP, COMPACTION_OP})


async def get_new_files_column_range(
    cursor, source_table: str, snapshot_ids: list[int], filter_column: str,
) -> tuple[datetime, datetime] | None:
    """Read min/max of filter_column across files added in given snapshots.

    Uses $all_entries.readable_metrics — metadata-only, no data scan.
    Bounds are parsed through _parse_ts and compared as datetimes, so
    chronological order is correct regardless of the lexicographic form
    of the bound strings. Returns (min_dt, max_dt) or None if no data
    files were found at all.
    """
    snap_list = ", ".join(str(s) for s in snapshot_ids)
    await cursor.execute(
        f"SELECT readable_metrics "
        f"FROM {system_table(source_table, 'all_entries')} "
        f"WHERE snapshot_id IN ({snap_list}) "
        f"AND status = 1"  # ADDED data files
    )

    lows: list[datetime] = []
    highs: list[datetime] = []

    rows = await cursor.fetchall()
    if not rows:
        # No added data files at all — legitimate empty-append case.
        return None

    saw_filter_column = False
    for (metrics_raw,) in rows:
        if metrics_raw is None:
            continue
        metrics = metrics_raw if isinstance(metrics_raw, dict) else json.loads(metrics_raw)
        col_metrics = metrics.get(filter_column)
        if col_metrics is None:
            continue
        saw_filter_column = True
        lb = col_metrics.get("lower_bound")
        ub = col_metrics.get("upper_bound")
        if lb is not None:
            lows.append(_parse_ts(str(lb)))
        if ub is not None:
            highs.append(_parse_ts(str(ub)))

    if not saw_filter_column:
        # Rows exist but filter_column is absent from every file's
        # metrics — configuration error, fail loudly rather than freeze.
        raise MissingFilterColumnError(source_table, filter_column)

    if not lows or not highs:
        return None
    return (min(lows), max(highs))


def _iter_column_bounds(
    rows: list, column: str,
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
            lows.append(_parse_ts(str(lb)))
        if ub is not None:
            highs.append(_parse_ts(str(ub)))
    return lows, highs, saw


async def get_source_column_range(
    cursor, source_table: str, filter_column: str,
) -> tuple[datetime, datetime] | None:
    """Read ``(min, max)`` of ``filter_column`` across all live files in the
    current snapshot of ``source_table``.

    Uses ``$files.readable_metrics`` — this returns the live file set of
    the current snapshot, not per-commit manifest rows. Metadata-only.
    Raises ``MissingFilterColumnError`` if files exist but none carry
    ``filter_column`` metrics. Returns ``None`` for empty tables.
    """
    await cursor.execute(
        f"SELECT readable_metrics FROM {system_table(source_table, 'files')}"
    )
    rows = await cursor.fetchall()
    if not rows:
        return None
    lows, highs, saw = _iter_column_bounds(rows, filter_column)
    if not saw:
        raise MissingFilterColumnError(source_table, filter_column)
    if not lows or not highs:
        return None
    return (min(lows), max(highs))


async def get_target_bucket_max(
    cursor, target_table: str, bucket_alias: str,
) -> datetime | None:
    """Read the max ``upper_bound`` of ``bucket_alias`` across live files in
    ``target_table``.

    The chunked full-refresh uses this as its resume point: the target's
    own Iceberg metadata, rather than a separate cursor key, records what
    has been committed. Returns ``None`` for an empty target or one whose
    files carry no bounds on ``bucket_alias`` yet.
    """
    await cursor.execute(
        f"SELECT readable_metrics FROM {system_table(target_table, 'files')}"
    )
    rows = await cursor.fetchall()
    if not rows:
        return None
    _, highs, _ = _iter_column_bounds(rows, bucket_alias)
    return max(highs) if highs else None


def _parse_ts(value: str) -> datetime:
    """Parse an Iceberg readable_metrics timestamp to a datetime.

    Sub-microsecond precision is silently truncated to microseconds —
    ``expand_to_bucket_bounds`` floors to minute or coarser, so it
    cannot affect which bucket a row belongs to.
    """
    try:
        return datetime.fromisoformat(value)
    except ValueError as e:
        raise ValueError(f"unparseable timestamp: {value!r}") from e


def midnight(dt: datetime) -> datetime:
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def _add_months(dt: datetime, n: int) -> datetime:
    # dt is assumed to be the first of a month; add n months with year rollover.
    idx = (dt.year * 12 + dt.month - 1) + n
    return dt.replace(year=idx // 12, month=(idx % 12) + 1)


_FIXED_STEP = {
    "minute": timedelta(minutes=1),
    "hour": timedelta(hours=1),
    "day": timedelta(days=1),
    "week": timedelta(weeks=1),
}


def walk_buckets(
    start: datetime, end: datetime, granularity: str,
) -> Iterator[tuple[datetime, datetime]]:
    """Yield half-open ``[chunk_start, chunk_end)`` intervals stepping one
    ``granularity`` bucket at a time from ``start`` (inclusive) to ``end``
    (exclusive).

    ``start`` and ``end`` are expected to already be aligned to
    ``granularity`` boundaries (the caller snaps them via
    ``expand_to_bucket_bounds``). If ``start >= end``, yields nothing.
    """
    if start >= end:
        return
    if granularity in _FIXED_STEP:
        step = _FIXED_STEP[granularity]
        cur = start
        while cur < end:
            nxt = cur + step
            yield (cur, min(nxt, end))
            cur = nxt
        return
    months = {"month": 1, "quarter": 3, "year": 12}.get(granularity)
    if months is None:
        raise ValueError(f"unsupported granularity: {granularity}")
    cur = start
    while cur < end:
        nxt = _add_months(cur, months)
        yield (cur, min(nxt, end))
        cur = nxt


def expand_to_bucket_bounds(
    min_ts: datetime, max_ts: datetime, granularity: str,
) -> tuple[datetime, datetime]:
    """Expand a timestamp range outward to full GROUP BY bucket boundaries.

    This is the **inverse** of Trino's ``date_trunc(granularity, ts)`` over a
    range.  Where ``date_trunc`` maps many timestamps to a single bucket
    start (forward, many-to-one), this function maps a ``(min_ts, max_ts)``
    range to the smallest bucket-aligned interval that contains every
    source row belonging to any touched bucket.

    Returns ``(start, end)`` with ``start`` on a bucket boundary ≤ ``min_ts``
    and ``end`` on the next bucket boundary > ``max_ts`` (half-open interval,
    so the emitted WHERE predicate is ``col >= start AND col < end``).

    Trino has no built-in inverse of ``date_trunc``; we do it in Python
    once when the detector reads file stats, then emit concrete TIMESTAMP
    literals into the MERGE so Trino's planner can partition-prune
    without needing to constant-fold a ``date_trunc`` expression.
    """
    if granularity == "minute":
        start = min_ts.replace(second=0, microsecond=0)
        end = max_ts.replace(second=0, microsecond=0) + timedelta(minutes=1)
    elif granularity == "hour":
        start = min_ts.replace(minute=0, second=0, microsecond=0)
        end = max_ts.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    elif granularity == "day":
        start = midnight(min_ts)
        end = midnight(max_ts) + timedelta(days=1)
    elif granularity == "week":
        # ISO week: Monday = 0
        start = midnight(min_ts - timedelta(days=min_ts.weekday()))
        end = midnight(max_ts - timedelta(days=max_ts.weekday())) + timedelta(weeks=1)
    elif granularity == "month":
        start = midnight(min_ts).replace(day=1)
        end = _add_months(midnight(max_ts).replace(day=1), 1)
    elif granularity == "quarter":
        q_start = ((min_ts.month - 1) // 3) * 3 + 1
        start = midnight(min_ts).replace(month=q_start, day=1)
        q_start_max = ((max_ts.month - 1) // 3) * 3 + 1
        end = _add_months(midnight(max_ts).replace(month=q_start_max, day=1), 3)
    elif granularity == "year":
        start = midnight(min_ts).replace(month=1, day=1)
        end = midnight(max_ts).replace(year=max_ts.year + 1, month=1, day=1)
    else:
        raise ValueError(f"unsupported granularity: {granularity}")

    return start, end


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

    # Classify operations under the append-only assumption:
    #   append   → real new data, drive incremental refresh
    #   replace  → compaction (files rewritten, no data change) — skip
    #   anything else → fail loudly, assumption violated
    unknown = [op for op in ops if op not in ALLOWED_OPS]
    if unknown:
        raise UnexpectedOperationError(source_table, unknown)

    append_snaps = [s for s in snapshots if s["operation"] == APPEND_OP]
    if not append_snaps:
        # Only compactions since last_snap; advance state, don't refresh.
        log.info(
            "%s: only compaction (replace) snapshots since %d → advance state, skip",
            source_table, last_snapshot,
        )
        return ChangeResult(action=RefreshAction.NO_CHANGE, current_snapshot=current_snap)

    # Read column range from files added in APPEND snapshots only.
    # Including compaction-added files would uselessly expand the range.
    snap_ids = [s["snapshot_id"] for s in append_snaps]
    col_range = await get_new_files_column_range(cursor, source_table, snap_ids, filter_column)
    if col_range is None:
        # New snapshots but no data files (e.g. compaction-only)
        log.debug("%s: new snapshots but no data files (compaction?)", source_table)
        return ChangeResult(action=RefreshAction.NO_CHANGE, current_snapshot=current_snap)

    min_ts, max_ts = col_range
    snapped_start, snapped_end = expand_to_bucket_bounds(min_ts, max_ts, filter_granularity)

    log.info(
        "file stats: %s in [%s, %s] → snapped to [%s, %s)",
        filter_column, min_ts, max_ts, snapped_start, snapped_end,
    )

    return ChangeResult(
        action=RefreshAction.INCREMENTAL,
        current_snapshot=current_snap,
        filter_range=(snapped_start, snapped_end),
    )
