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


def get_current_snapshot(cursor, source_table: str) -> int | None:
    """Get latest snapshot_id from source table. Metadata-only."""
    cursor.execute(
        f"SELECT snapshot_id FROM {system_table(source_table, 'snapshots')} "
        f"ORDER BY committed_at DESC LIMIT 1"
    )
    row = cursor.fetchone()
    return row[0] if row else None


def get_snapshots_since(cursor, source_table: str, last_snap: int) -> list[dict]:
    """Get snapshot_id and operation for all snapshots after last_snap.

    Snapshot IDs are random longs in Iceberg — not sequential. We find
    the committed_at of last_snap and return everything after it.
    """
    snaps_table = system_table(source_table, "snapshots")
    cursor.execute(
        f"SELECT snapshot_id, operation FROM {snaps_table} "
        f"WHERE committed_at > ("
        f"  SELECT committed_at FROM {snaps_table} WHERE snapshot_id = {last_snap}"
        f") ORDER BY committed_at"
    )
    return [{"snapshot_id": row[0], "operation": row[1]} for row in cursor.fetchall()]


NON_APPEND_OPS = frozenset({"overwrite", "delete", "replace"})


def get_new_files_column_range(
    cursor, source_table: str, snapshot_ids: list[int], filter_column: str,
) -> tuple[str, str] | None:
    """Read min/max of filter_column across files added in given snapshots.

    Uses $all_entries.readable_metrics — metadata-only, no data scan.
    Returns (min_value_str, max_value_str) or None if no data files found.
    """
    snap_list = ", ".join(str(s) for s in snapshot_ids)
    cursor.execute(
        f"SELECT readable_metrics "
        f"FROM {system_table(source_table, 'all_entries')} "
        f"WHERE snapshot_id IN ({snap_list}) "
        f"AND status = 1"  # ADDED data files
    )

    overall_min = None
    overall_max = None

    for (metrics_raw,) in cursor.fetchall():
        if metrics_raw is None:
            continue
        metrics = metrics_raw if isinstance(metrics_raw, dict) else json.loads(metrics_raw)
        col_metrics = metrics.get(filter_column)
        if col_metrics is None:
            continue
        lb = col_metrics.get("lower_bound")
        ub = col_metrics.get("upper_bound")
        if lb is not None:
            if overall_min is None or str(lb) < str(overall_min):
                overall_min = lb
        if ub is not None:
            if overall_max is None or str(ub) > str(overall_max):
                overall_max = ub

    if overall_min is None or overall_max is None:
        return None
    return (str(overall_min), str(overall_max))


def _parse_ts(value: str) -> datetime:
    """Parse an Iceberg timestamp string to a Python datetime."""
    # Iceberg readable_metrics returns ISO-ish strings like:
    #   2026-04-08T10:00:41.385604+00:00
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
    # Last resort: just parse date
    log.warning("date-only parse fallback for %r — no time component matched", value)
    return datetime.strptime(value[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)


def snap_range(
    min_ts: datetime, max_ts: datetime, granularity: str,
) -> tuple[datetime, datetime]:
    """Snap a timestamp range outward to complete GROUP BY bucket boundaries.

    Returns (start, end) where start is the floor of the bucket containing
    min_ts, and end is the ceiling (exclusive) of the bucket containing max_ts.
    """
    tz = min_ts.tzinfo or timezone.utc

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


def detect_changes(
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
    current_snap = get_current_snapshot(cursor, source_table)
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

    # Get intermediate snapshots
    snapshots = get_snapshots_since(cursor, source_table, last_snapshot)
    if not snapshots:
        log.debug("%s: no new snapshots since %d", source_table, last_snapshot)
        return ChangeResult(action=RefreshAction.NO_CHANGE, current_snapshot=current_snap)

    ops = [s["operation"] for s in snapshots]
    log.debug("%s: %d new snapshots, operations: %s", source_table, len(snapshots), ops)

    # Non-append operations → full refresh
    if any(op in NON_APPEND_OPS for op in ops):
        log.info("%s: non-append operations %s → full refresh", source_table, ops)
        return ChangeResult(action=RefreshAction.FULL_REFRESH, current_snapshot=current_snap)

    # Read column range from new files
    snap_ids = [s["snapshot_id"] for s in snapshots]
    col_range = get_new_files_column_range(cursor, source_table, snap_ids, filter_column)
    if col_range is None:
        # New snapshots but no data files (e.g. compaction-only)
        log.debug("%s: new snapshots but no data files (compaction?)", source_table)
        return ChangeResult(action=RefreshAction.NO_CHANGE, current_snapshot=current_snap)

    min_val, max_val = col_range
    min_ts = _parse_ts(min_val)
    max_ts = _parse_ts(max_val)
    snapped_start, snapped_end = snap_range(min_ts, max_ts, filter_granularity)

    log.info(
        "file stats: %s in [%s, %s] → snapped to [%s, %s)",
        filter_column, min_val, max_val, snapped_start, snapped_end,
    )

    return ChangeResult(
        action=RefreshAction.INCREMENTAL,
        current_snapshot=current_snap,
        filter_range=(snapped_start, snapped_end),
    )
