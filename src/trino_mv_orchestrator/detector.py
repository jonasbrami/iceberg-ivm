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


def _parse_ts(value: str) -> datetime:
    """Parse an Iceberg timestamp string to a Python datetime.

    Iceberg readable_metrics returns ISO-ish strings like
    ``2026-04-08T10:00:41.385604+00:00``. We try the four common Trino
    output shapes. If none matches we raise — silently falling back to
    date-only parsing could drop the time component and shift a snapped
    incremental range by up to 24 hours, corrupting the MERGE.
    """
    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
    ):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise ValueError(f"unparseable timestamp: {value!r}")


def snap_range(
    min_ts: datetime, max_ts: datetime, granularity: str,
) -> tuple[datetime, datetime]:
    """Snap a timestamp range outward to complete GROUP BY bucket boundaries.

    Returns (start, end) where start is the floor of the bucket containing
    min_ts, and end is the ceiling (exclusive) of the bucket containing max_ts.
    """
    if granularity == "minute":
        start = min_ts.replace(second=0, microsecond=0)
        end = max_ts.replace(second=0, microsecond=0) + timedelta(minutes=1)
    elif granularity == "hour":
        start = min_ts.replace(minute=0, second=0, microsecond=0)
        end = max_ts.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    elif granularity == "day":
        start = min_ts.replace(hour=0, minute=0, second=0, microsecond=0)
        end = max_ts.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    elif granularity == "week":
        # ISO week: Monday = 0
        start = (min_ts - timedelta(days=min_ts.weekday())).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        end = (max_ts - timedelta(days=max_ts.weekday())).replace(
            hour=0, minute=0, second=0, microsecond=0
        ) + timedelta(weeks=1)
    elif granularity == "month":
        start = min_ts.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        # First day of next month after max_ts
        if max_ts.month == 12:
            end = max_ts.replace(year=max_ts.year + 1, month=1, day=1,
                                 hour=0, minute=0, second=0, microsecond=0)
        else:
            end = max_ts.replace(month=max_ts.month + 1, day=1,
                                 hour=0, minute=0, second=0, microsecond=0)
    elif granularity == "quarter":
        q_start = ((min_ts.month - 1) // 3) * 3 + 1
        start = min_ts.replace(month=q_start, day=1, hour=0, minute=0, second=0, microsecond=0)
        q_start_max = ((max_ts.month - 1) // 3) * 3 + 1
        next_q = q_start_max + 3
        if next_q > 12:
            end = max_ts.replace(year=max_ts.year + 1, month=next_q - 12, day=1,
                                 hour=0, minute=0, second=0, microsecond=0)
        else:
            end = max_ts.replace(month=next_q, day=1,
                                 hour=0, minute=0, second=0, microsecond=0)
    elif granularity == "year":
        start = min_ts.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        end = max_ts.replace(year=max_ts.year + 1, month=1, day=1,
                             hour=0, minute=0, second=0, microsecond=0)
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
    snapped_start, snapped_end = snap_range(min_ts, max_ts, filter_granularity)

    log.info(
        "file stats: %s in [%s, %s] → snapped to [%s, %s)",
        filter_column, min_ts, max_ts, snapped_start, snapped_end,
    )

    return ChangeResult(
        action=RefreshAction.INCREMENTAL,
        current_snapshot=current_snap,
        filter_range=(snapped_start, snapped_end),
    )
