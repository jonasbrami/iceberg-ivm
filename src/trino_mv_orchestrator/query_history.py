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

from trino_mv_orchestrator.executor import QueryInfo

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
"""


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
        # ``check_same_thread=False`` because aiosqlite's worker thread is
        # not the thread that created this object.
        self._db = await aiosqlite.connect(self.db_path)
        await self._db.executescript(_SCHEMA)
        await self._db.commit()
        log.info("query history opened at %s (limit=%d per view)",
                 self.db_path, self.limit)

    async def close(self) -> None:
        if self._db is not None:
            await self._db.close()
            self._db = None

    async def append(self, view: str, queries: list[QueryInfo]) -> None:
        """Insert ``queries`` for ``view`` and trim to ``self.limit``."""
        if not queries:
            return
        assert self._db is not None, "QueryHistory.open() not called"
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
        assert self._db is not None, "QueryHistory.open() not called"
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
        assert self._db is not None, "QueryHistory.open() not called"
        await self._db.execute("DELETE FROM query_history WHERE view = ?", (view,))
        await self._db.commit()
