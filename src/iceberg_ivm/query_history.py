"""Persistent ring buffer for refresh QueryInfo rows, backed by SQLite.

The UI surfaces the last few MERGE / INSERT / DELETE queries per view so an
operator can jump to the Trino UI. Before this module that buffer lived on
``ViewStatus`` in memory and was wiped on restart. Here we keep the same
shape (newest-first, capped per view) but back it with a SQLite file so
history survives process restarts.

SQLite's single-writer model is fine for our write rate (a handful of
inserts per refresh, seconds apart). ``aiosqlite`` runs each call on a
dedicated background thread so the event loop stays unblocked.
"""
from __future__ import annotations

import logging
from pathlib import Path

import aiosqlite

from iceberg_ivm.executor import QueryInfo

log = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS query_history (
    view            TEXT    NOT NULL,
    query_id        TEXT    NOT NULL,
    info_uri        TEXT    NOT NULL,
    stage           TEXT    NOT NULL,
    started_at      REAL    NOT NULL,
    elapsed_ms      REAL    NOT NULL,
    processed_rows  INTEGER NOT NULL DEFAULT 0,
    processed_bytes INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_query_history_view_started
    ON query_history (view, started_at DESC);

-- Persists the last wall-clock time each maintenance op ran, per view, plus
-- summary counters and last-error so the UI / scheduler can recover their
-- pre-restart state. Separate from query_history because that table is a
-- 50-row ring buffer per view — at a 60s refresh cadence maintenance rows
-- would be evicted in under an hour and we'd re-run every op on restart.
CREATE TABLE IF NOT EXISTS maintenance_state (
    view           TEXT    NOT NULL,
    op             TEXT    NOT NULL,
    last_run       REAL    NOT NULL,
    last_duration  REAL,
    last_error     TEXT,
    total_runs     INTEGER NOT NULL DEFAULT 0,
    total_errors   INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (view, op)
);

-- Per-view ViewStatus snapshot. Persisted on every mutation in the refresh
-- path so the UI shows the last-known state immediately after a restart
-- (otherwise total_refreshes / chunks_done / last_refresh all reset to
-- their initial values until the first post-restart tick — see issue #40).
-- ``recent_queries`` and ``maintenance`` live in their own tables and are
-- intentionally NOT mirrored here.
CREATE TABLE IF NOT EXISTS view_status (
    view                  TEXT    PRIMARY KEY,
    last_refresh          REAL,
    last_duration         REAL,
    last_action           TEXT    NOT NULL DEFAULT 'pending',
    last_range            TEXT,
    last_error            TEXT,
    total_refreshes       INTEGER NOT NULL DEFAULT 0,
    total_errors          INTEGER NOT NULL DEFAULT 0,
    chunks_done           INTEGER NOT NULL DEFAULT 0,
    chunks_total          INTEGER,
    last_source_snapshot  INTEGER
);
"""


# Columns stored in ``view_status`` excluding the PK. Used by upsert to build
# the SET clause and by get_view_status to project rows back into a dict.
_VIEW_STATUS_COLS: tuple[str, ...] = (
    "last_refresh",
    "last_duration",
    "last_action",
    "last_range",
    "last_error",
    "total_refreshes",
    "total_errors",
    "chunks_done",
    "chunks_total",
)


# Columns stored in ``maintenance_state`` excluding (view, op). ``last_run``
# is required (NOT NULL) — reuse it both for the upsert SET clause and the
# read-side projection.
_MAINTENANCE_COLS: tuple[str, ...] = (
    "last_run",
    "last_duration",
    "last_error",
    "total_runs",
    "total_errors",
)


class QueryHistory:
    """Async, per-view bounded history of refresh queries.

    One connection is held for the life of the process. SQLite serialises
    writes internally, so a single connection is enough even with multiple
    view workers.
    """

    def __init__(self, db_path: str | Path, limit: int) -> None:
        self.db_path = str(db_path)
        self.limit = limit
        self._db: aiosqlite.Connection | None = None

    async def open(self) -> None:
        self._db = await aiosqlite.connect(self.db_path)
        await self._db.executescript(_SCHEMA)
        await self._db.commit()
        log.info("query history opened at %s (limit=%d per view)", self.db_path, self.limit)

    async def close(self) -> None:
        if self._db is not None:
            await self._db.close()
            self._db = None

    async def append(self, view: str, queries: list[QueryInfo]) -> None:
        """Insert ``queries`` for ``view`` and trim to ``self.limit``."""
        if not queries:
            return
        await self._db.executemany(
            "INSERT INTO query_history "
            "(view, query_id, info_uri, stage, started_at, elapsed_ms, "
            " processed_rows, processed_bytes) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    view, q.query_id, q.info_uri, q.stage,
                    q.started_at, q.elapsed_ms,
                    q.processed_rows, q.processed_bytes,
                )
                for q in queries
            ],
        )
        # Keep only the newest ``limit`` rows per view. Small table, runs
        # once per refresh — no need to amortise.
        await self._db.execute(
            "DELETE FROM query_history "
            "WHERE view = ? AND rowid NOT IN ("
            "    SELECT rowid FROM query_history "
            "    WHERE view = ? "
            "    ORDER BY started_at DESC LIMIT ?"
            ")",
            (view, view, self.limit),
        )
        await self._db.commit()

    async def recent(self, view: str) -> list[QueryInfo]:
        """Return the newest-first list of at most ``self.limit`` queries."""
        async with self._db.execute(
            "SELECT query_id, info_uri, stage, started_at, elapsed_ms, "
            "       processed_rows, processed_bytes "
            "FROM query_history WHERE view = ? "
            "ORDER BY started_at DESC LIMIT ?",
            (view, self.limit),
        ) as cur:
            rows = await cur.fetchall()
        return [
            QueryInfo(
                query_id=r[0], info_uri=r[1], stage=r[2],
                started_at=r[3], elapsed_ms=r[4],
                processed_rows=r[5], processed_bytes=r[6],
            )
            for r in rows
        ]

    async def delete_view(self, view: str) -> None:
        await self._db.execute("DELETE FROM query_history WHERE view = ?", (view,))
        await self._db.execute("DELETE FROM maintenance_state WHERE view = ?", (view,))
        await self._db.execute("DELETE FROM view_status WHERE view = ?", (view,))
        await self._db.commit()

    # ── view_status ───────────────────────────────────────────────────

    async def upsert_view_status(self, view: str, fields: dict) -> None:
        """Upsert the persisted ViewStatus row for ``view``.

        ``fields`` mirrors the ``ViewStatus`` dataclass fields 1:1 (see
        ``_VIEW_STATUS_COLS``). Unknown keys are ignored so callers can pass
        a full ``dataclasses.asdict(vs)`` without filtering ``recent_queries``
        / ``maintenance`` themselves.
        """
        cols = [c for c in _VIEW_STATUS_COLS if c in fields]
        values = [fields[c] for c in cols]
        col_list = ", ".join(["view", *cols])
        placeholders = ", ".join(["?"] * (len(cols) + 1))
        update_clause = ", ".join(f"{c} = excluded.{c}" for c in cols)
        await self._db.execute(
            f"INSERT INTO view_status ({col_list}) VALUES ({placeholders}) "
            f"ON CONFLICT(view) DO UPDATE SET {update_clause}",
            (view, *values),
        )
        await self._db.commit()

    async def get_view_status(self, view: str) -> dict | None:
        """Return the persisted ViewStatus fields for ``view`` or ``None``."""
        col_list = ", ".join(_VIEW_STATUS_COLS)
        async with self._db.execute(
            f"SELECT {col_list} FROM view_status WHERE view = ?",
            (view,),
        ) as cur:
            row = await cur.fetchone()
        if row is None:
            return None
        return dict(zip(_VIEW_STATUS_COLS, row))

    # ── last_source_snapshot ──────────────────────────────────────────
    # Lives on the view_status row but kept out of _VIEW_STATUS_COLS so the
    # ViewStatus dataclass mirror can't accidentally clobber a fresh bookmark
    # via upsert_view_status. Read once at the top of refresh_view, written
    # once when a refresh (or compaction-only advance) commits.

    async def get_last_source_snapshot(self, view: str) -> int | None:
        async with self._db.execute(
            "SELECT last_source_snapshot FROM view_status WHERE view = ?",
            (view,),
        ) as cur:
            row = await cur.fetchone()
        return row[0] if row and row[0] is not None else None

    async def set_last_source_snapshot(self, view: str, snapshot_id: int) -> None:
        await self._db.execute(
            "INSERT INTO view_status (view, last_source_snapshot) VALUES (?, ?) "
            "ON CONFLICT(view) DO UPDATE SET "
            "last_source_snapshot = excluded.last_source_snapshot",
            (view, snapshot_id),
        )
        await self._db.commit()

    # ── maintenance_state ─────────────────────────────────────────────

    async def upsert_maintenance(self, view: str, op: str, fields: dict) -> None:
        """Upsert the per-op maintenance row for ``(view, op)``.

        ``fields`` mirrors ``MaintenanceOpStatus``. ``last_run`` is required
        (NOT NULL); other columns default to NULL / 0 if omitted.
        """
        if "last_run" not in fields or fields["last_run"] is None:
            raise ValueError("upsert_maintenance requires a non-null last_run")
        cols = [c for c in _MAINTENANCE_COLS if c in fields]
        values = [fields[c] for c in cols]
        col_list = ", ".join(["view", "op", *cols])
        placeholders = ", ".join(["?"] * (len(cols) + 2))
        update_clause = ", ".join(f"{c} = excluded.{c}" for c in cols)
        await self._db.execute(
            f"INSERT INTO maintenance_state ({col_list}) VALUES ({placeholders}) "
            f"ON CONFLICT(view, op) DO UPDATE SET {update_clause}",
            (view, op, *values),
        )
        await self._db.commit()

    async def all_maintenance(self, view: str) -> dict[str, dict]:
        """Return ``{op: {col: value, ...}}`` for every op recorded against ``view``.

        Shape changed in #40: previously this returned ``dict[str, float]``
        (just ``op → last_run``). Callers that only need ``last_run`` should
        read ``[op]["last_run"]`` from the new dict.
        """
        col_list = ", ".join(_MAINTENANCE_COLS)
        async with self._db.execute(
            f"SELECT op, {col_list} FROM maintenance_state WHERE view = ?",
            (view,),
        ) as cur:
            rows = await cur.fetchall()
        return {r[0]: dict(zip(_MAINTENANCE_COLS, r[1:])) for r in rows}
