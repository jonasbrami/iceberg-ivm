"""State storage via Iceberg table extra_properties.

Stores mv.last_source_snapshot — the last processed source snapshot ID.
"""
from __future__ import annotations

import logging

from trino_mv_orchestrator.detector import system_table

log = logging.getLogger(__name__)

SNAPSHOT_KEY = "mv.last_source_snapshot"


async def read_last_snapshot(cursor, target_table: str) -> int | None:
    """Read last processed source snapshot ID from target table properties."""
    await cursor.execute(
        f"SELECT value FROM {system_table(target_table, 'properties')} "
        f"WHERE key = '{SNAPSHOT_KEY}'"
    )
    row = await cursor.fetchone()
    return int(row[0]) if row else None


async def write_last_snapshot(cursor, target_table: str, snapshot_id: int) -> None:
    """Write last processed source snapshot ID into target table properties."""
    await cursor.execute(
        f"ALTER TABLE {target_table} SET PROPERTIES "
        f"extra_properties = MAP("
        f"ARRAY['{SNAPSHOT_KEY}'], "
        f"ARRAY['{snapshot_id}']"
        f")"
    )
    log.info("wrote last_snapshot=%d to %s", snapshot_id, target_table)
