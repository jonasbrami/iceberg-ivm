"""State storage via Iceberg table extra_properties.

Stores mv.last_source_snapshot — the last processed source snapshot ID.
"""
from __future__ import annotations

SNAPSHOT_KEY = "mv.last_source_snapshot"


def _properties_table(table: str) -> str:
    """Build reference to the $properties system table."""
    parts = table.rsplit(".", 1)
    if len(parts) == 1:
        return f'"{parts[0]}$properties"'
    return f'{parts[0]}."{parts[1]}$properties"'


def read_last_snapshot(cursor, target_table: str) -> int | None:
    """Read last processed source snapshot ID from target table properties."""
    cursor.execute(
        f"SELECT value FROM {_properties_table(target_table)} "
        f"WHERE key = '{SNAPSHOT_KEY}'"
    )
    row = cursor.fetchone()
    return int(row[0]) if row else None


def write_last_snapshot(cursor, target_table: str, snapshot_id: int) -> None:
    """Write last processed source snapshot ID into target table properties."""
    cursor.execute(
        f"ALTER TABLE {target_table} SET PROPERTIES "
        f"extra_properties = MAP("
        f"ARRAY['{SNAPSHOT_KEY}'], "
        f"ARRAY['{snapshot_id}']"
        f")"
    )
