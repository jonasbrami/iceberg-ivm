"""FastAPI server: web UI, REST API, Prometheus metrics, refresh loop."""

from __future__ import annotations

import asyncio
import base64
import dataclasses
import logging
import os
import secrets
import time
from contextlib import AsyncExitStack, asynccontextmanager, suppress
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse

import aiotrino
from aiotrino.auth import BasicAuthentication
from fastapi import Body, Depends, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, Response
from prometheus_client import Counter, Gauge, Histogram, generate_latest
from pydantic import BaseModel, ValidationError

from iceberg_ivm.config import Config, TrinoConfig, ViewConfig, load_config, load_views, save_views
from iceberg_ivm.detector import (
    FullRefreshNeeded,
    RefreshAction,
    SourceNotFound,
    detect_changes,
    incremental_range_since,
    snapshot_committed_at,
    snapshot_exists,
)
from iceberg_ivm.executor import (
    QueryInfo,
    execute_maintenance,
    execute_refresh,
)
from iceberg_ivm.introspect import (
    build_create_table_sql,
    discover_columns,
)
from iceberg_ivm.query_history import QueryHistory
from iceberg_ivm.query_parser import parse_view_query

log = logging.getLogger(__name__)

# ── Prometheus metrics ──

REFRESH_TOTAL = Counter("mv_refresh_total", "Total refresh operations", ["view", "type"])
REFRESH_DURATION = Histogram("mv_refresh_duration_seconds", "Refresh duration", ["view"])
REFRESH_LAST_SUCCESS = Gauge("mv_refresh_last_success_timestamp", "Last successful refresh", ["view"])
REFRESH_ERRORS = Counter("mv_refresh_errors_total", "Refresh errors", ["view"])
CONFIG_RELOADS = Counter("mv_config_reload_total", "Config reload events")
VIEWS_CONFIGURED = Gauge("mv_views_configured", "Number of configured views")

REFRESH_BYTES = Counter("mv_refresh_bytes_processed_total", "Bytes processed during refresh queries", ["view"])
REFRESH_ROWS = Counter("mv_refresh_rows_processed_total", "Rows processed during refresh queries", ["view"])
DETECTION_DURATION = Histogram("mv_detection_duration_seconds", "Change detection duration", ["view"])
SOURCE_SNAPSHOT = Gauge("mv_source_snapshot_id", "Current source snapshot ID", ["view"])
LAG = Gauge("mv_lag_seconds", "Commit time of the source's newest snapshot minus the one the view reflects", ["view"])

# Per-chunk metrics for chunked first-run full refreshes — mv_refresh_total /
# mv_refresh_duration_seconds only tick once when the whole backfill commits
# (often hours later), giving zero in-flight visibility on their own.
CHUNKS_COMPLETED = Counter("mv_chunks_completed_total", "Completed chunks in a chunked full refresh", ["view"])
CHUNK_DURATION = Histogram("mv_chunk_duration_seconds", "Per-chunk merge duration in a chunked full refresh", ["view"])
CHUNK_ROWS = Counter("mv_chunk_rows_written_total", "Rows written per chunk in a chunked full refresh", ["view"])


# ── Application state ──

RECENT_QUERY_LIMIT = 50


@dataclass(slots=True)
class MaintenanceOpStatus:
    """Per-op runtime status for one view (e.g. optimize, expire_snapshots).

    ``last_run`` is wall-clock epoch seconds — persisted to SQLite via
    ``QueryHistory.upsert_maintenance`` so scheduling survives process
    restarts (a 7d retention interval is meaningless if every restart
    resets it to ``None`` and re-runs immediately).
    """

    last_run: float | None = None
    last_duration: float | None = None
    last_error: str | None = None
    total_runs: int = 0
    total_errors: int = 0


@dataclass(slots=True)
class ViewStatus:
    name: str
    last_refresh: float | None = None
    last_duration: float | None = None
    last_action: str = "pending"
    last_range: str | None = None
    last_error: str | None = None
    total_refreshes: int = 0
    total_errors: int = 0
    # Chunked-backfill progress. ``chunks_total`` is populated while a chunked
    # refresh is in-flight and cleared when it commits cleanly. Kept orthogonal
    # to ``total_refreshes`` (still "one refresh event ↔ one committed refresh")
    # so existing consumers of the counter are not broken.
    chunks_done: int = 0
    chunks_total: int | None = None
    # Informational, not an error (e.g. "waiting for <source> to exist").
    note: str | None = None
    # Freshness, as epoch seconds: the last successful check of the source,
    # the commit time of the source's newest snapshot, and the commit time of
    # the snapshot the view reflects. Equal last two = up to date.
    last_checked: float | None = None
    source_committed_at: float | None = None
    data_as_of: float | None = None
    # Ring buffer of the last few refresh queries (MERGE / INSERT / DELETE).
    # In-memory only; cleared on process restart.
    recent_queries: list[QueryInfo] = field(default_factory=list)
    # Per-maintenance-op status, keyed by op name (see config.MAINTENANCE_OPS).
    # ``last_run`` within each entry is hydrated from SQLite on startup.
    maintenance: dict[str, MaintenanceOpStatus] = field(default_factory=dict)


@dataclass
class ViewRuntime:
    """Per-view concurrency primitives.

    Kept separate from ``ViewStatus`` so ``ViewStatus`` stays JSON-serialisable
    (``dataclasses.asdict`` can't walk ``asyncio.Event`` / ``Condition``).
    """

    wake: asyncio.Event = field(default_factory=asyncio.Event)
    refresh_cond: asyncio.Condition = field(default_factory=asyncio.Condition)
    refresh_seq: int = 0
    refresh_in_flight: bool = False
    rebuild_requested: bool = False  # applied by the worker at its next tick


@dataclass
class AppState:
    config_path: Path = field(default_factory=lambda: Path("config.yaml"))
    views_path: Path = field(default_factory=lambda: Path("views.yaml"))
    config: Config | None = None
    config_mtime: float = 0
    views_mtime: float = 0
    view_statuses: dict[str, ViewStatus] = field(default_factory=dict)
    view_runtimes: dict[str, ViewRuntime] = field(default_factory=dict)
    # Live worker tasks, keyed by view name. Owned by the supervisor; exposed
    # on AppState so delete_view can cancel its worker synchronously rather
    # than waiting up to one supervisor tick (which races with the worker
    # writing rows for an already-deleted view).
    workers: dict[str, asyncio.Task[None]] = field(default_factory=dict)
    history: QueryHistory | None = None
    stop_event: asyncio.Event = field(default_factory=asyncio.Event)


# ── Dependency injection ──


def get_app_state(request: Request) -> AppState:
    return request.app.state.s


# ── Core logic ──


class _Connection:
    """aiotrino connection whose ``close()`` also cancels queries still running.

    aiotrino's own ``close()`` only drops the HTTP session, which would leave a
    timed-out or cancelled MERGE running (and committing) inside Trino.
    """

    def __init__(self, conn: aiotrino.dbapi.Connection) -> None:
        self._conn = conn
        self._cursors: list = []

    async def cursor(self):
        cursor = await self._conn.cursor()
        self._cursors.append(cursor)
        return cursor

    async def close(self) -> None:
        for cursor in self._cursors:
            with suppress(Exception):
                await cursor.cancel()  # no-op for finished queries
        await self._conn.close()


def get_trino_connection(s: AppState) -> _Connection:
    cfg = s.config
    parsed = urlparse(cfg.trino.url)
    scheme = parsed.scheme or "http"
    if not parsed.hostname:
        raise ValueError(f"TRINO_URL does not contain a host: {cfg.trino.url!r}")
    kwargs = {
        "host": parsed.hostname,
        "port": parsed.port or (443 if scheme == "https" else 80),
        "http_scheme": scheme,
        "catalog": cfg.trino.catalog,
        "schema": cfg.trino.schema,
        "user": cfg.trino.user,
        # UTC sessions make Trino's date_trunc on TIMESTAMP WITH TIME ZONE agree
        # with the bucket math in detector.py (see docs/how-it-works.md, "Time zones").
        "timezone": "UTC",
    }
    if cfg.trino.password:
        kwargs["auth"] = BasicAuthentication(cfg.trino.user, cfg.trino.password)
    return _Connection(aiotrino.dbapi.connect(**kwargs))


def reload_config(s: AppState) -> None:
    try:
        config_mtime = s.config_path.stat().st_mtime
    except FileNotFoundError:
        log.warning("config file not found: %s", s.config_path)
        return
    views_mtime = s.views_path.stat().st_mtime if s.views_path.exists() else 0
    if config_mtime <= s.config_mtime and views_mtime <= s.views_mtime:
        return
    try:
        new_cfg = load_config(s.config_path)
        new_views = load_views(s.views_path)
        s.config = Config(trino=new_cfg.trino, views=new_views, server=new_cfg.server)
        VIEWS_CONFIGURED.set(len(new_views))
        CONFIG_RELOADS.inc()
        log.info("config reloaded: %d views", len(new_views))
        for v in new_views:
            s.view_statuses.setdefault(v.name, ViewStatus(name=v.name))
    except Exception:
        log.exception("failed to reload config")
    finally:
        # Always advance mtimes — on failure the file is broken; retrying it
        # every tick spams logs and serves no purpose. The next legitimate
        # edit bumps the mtime and triggers a fresh attempt.
        s.config_mtime = config_mtime
        s.views_mtime = views_mtime


def _view_status_persist_fields(vs: ViewStatus) -> dict:
    """Project ``vs`` to the dict shape stored in ``view_status``.

    ``recent_queries`` and ``maintenance`` are persisted separately
    (their own tables) and intentionally excluded so we never
    accidentally cross the boundary.
    """
    return {
        "last_refresh": vs.last_refresh,
        "last_duration": vs.last_duration,
        "last_action": vs.last_action,
        "last_range": vs.last_range,
        "last_error": vs.last_error,
        "total_refreshes": vs.total_refreshes,
        "total_errors": vs.total_errors,
        "chunks_done": vs.chunks_done,
        "chunks_total": vs.chunks_total,
        "last_checked": vs.last_checked,
        "source_committed_at": vs.source_committed_at,
        "data_as_of": vs.data_as_of,
    }


async def _persist_view_status(s: AppState, view_name: str, vs: ViewStatus) -> None:
    """Mirror ``vs`` to ``view_status`` if a history is attached.

    Called from every refresh/maintenance mutation site — write rate is
    negligible (a few inserts per refresh, seconds apart) so we don't
    batch. Silently no-ops in tests that don't attach a QueryHistory.
    """
    if vs.source_committed_at is not None and vs.data_as_of is not None:
        LAG.labels(view=view_name).set(max(0.0, vs.source_committed_at - vs.data_as_of))
    if s.history is None:
        return
    await s.history.upsert_view_status(view_name, _view_status_persist_fields(vs))


def _maintenance_persist_fields(ms: MaintenanceOpStatus) -> dict:
    """Project ``ms`` to the dict shape stored in ``maintenance_state``."""
    return {
        "last_run": ms.last_run,
        "last_duration": ms.last_duration,
        "last_error": ms.last_error,
        "total_runs": ms.total_runs,
        "total_errors": ms.total_errors,
    }


async def hydrate_view_state(s: AppState) -> None:
    """Populate ``ViewStatus`` from the persisted DB.

    Called once at startup (after ``history`` is opened) and whenever a
    fresh ``ViewStatus`` is seeded for a reloaded view. Cheap — one
    indexed query per view, capped to ``RECENT_QUERY_LIMIT``.

    Three sources are merged:

    * ``query_history``     → ``vs.recent_queries``
    * ``view_status``       → scalar status fields (counters, last_*)
    * ``maintenance_state`` → ``vs.maintenance[op]``
    """
    if s.history is None or s.config is None:
        return
    for v in s.config.views:
        vs = s.view_statuses.setdefault(v.name, ViewStatus(name=v.name))
        if not vs.recent_queries:
            vs.recent_queries = await s.history.recent(v.name)
        # Status scalars: only hydrate on a fresh ViewStatus (default values)
        # to avoid clobbering an already-running view's in-memory counters.
        # The fixed sentinel here is total_refreshes==0 and last_refresh is None.
        persisted = await s.history.get_view_status(v.name)
        if persisted is not None and vs.total_refreshes == 0 and vs.last_refresh is None:
            for k, val in persisted.items():
                # last_action defaults to "pending"; respect persisted value
                # only if it's there. setattr handles all known fields safely.
                setattr(vs, k, val)
        if not vs.maintenance:
            for op, fields in (await s.history.all_maintenance(v.name)).items():
                vs.maintenance[op] = MaintenanceOpStatus(**fields)


async def maintain_view(
    s: AppState,
    view: ViewConfig,
    conn,
    target_table: str,
) -> None:
    """Run any due Iceberg maintenance ops after a refresh.

    Serialising with refresh avoids Iceberg commit conflicts.
    ``maintenance_interval_seconds == 0`` disables maintenance entirely;
    each per-op boolean toggles that op individually. Per-op last-run
    times are persisted so restarts resume the schedule.

    A fresh cursor is opened per op so that an op which raises (and
    leaves the cursor in an undefined Trino state) can't cascade into
    misreported failures on the next op.
    """
    interval = view.maintenance_interval_seconds
    if interval <= 0:
        return
    ops = [
        (
            "optimize",
            view.optimize,
            {"file_size_threshold": view.optimize_file_size_threshold} if view.optimize_file_size_threshold else {},
        ),
        ("expire_snapshots", view.expire_snapshots, {"retention_threshold": view.expire_snapshots_retention}),
        ("remove_orphan_files", view.remove_orphan_files, {"retention_threshold": view.remove_orphan_files_retention}),
    ]
    vs = s.view_statuses.setdefault(view.name, ViewStatus(name=view.name))
    now = time.time()
    for op, enabled, params in ops:
        if not enabled:
            continue
        ms = vs.maintenance.setdefault(op, MaintenanceOpStatus())
        if ms.last_run is not None and now - ms.last_run < interval:
            continue
        try:
            cursor = await conn.cursor()
            qi = await execute_maintenance(cursor, target_table, op, params)
            ms.last_run = time.time()
            ms.last_duration = qi.elapsed_ms / 1000.0
            ms.last_error = None
            ms.total_runs += 1
            await _record_query(s, view.name, vs, qi)
            if s.history is not None:
                await s.history.upsert_maintenance(
                    view.name,
                    op,
                    _maintenance_persist_fields(ms),
                )
        except Exception as e:
            ms.last_error = str(e)
            ms.total_errors += 1
            log.exception("%s: maintenance %s failed", view.name, op)
            # Persist the failure too so total_errors / last_error survive
            # restart. Skipped if last_run is still None (initial failure
            # before any successful run) — the table requires a non-null
            # last_run and there's no useful timestamp to record yet.
            if s.history is not None and ms.last_run is not None:
                await s.history.upsert_maintenance(
                    view.name,
                    op,
                    _maintenance_persist_fields(ms),
                )


async def _record_query(s: AppState, view_name: str, vs: ViewStatus, q: QueryInfo) -> None:
    """Persist ``q`` to history + refresh ``vs.recent_queries``.

    When history is open (always in prod, sometimes not in tests) SQLite is
    the source of truth; otherwise we fall back to the in-memory ring buffer.
    """
    if s.history is not None:
        await s.history.append(view_name, [q])
        vs.recent_queries = await s.history.recent(view_name)
    else:
        vs.recent_queries = ([q, *vs.recent_queries])[:RECENT_QUERY_LIMIT]


def _epoch(value) -> float | None:
    return value.timestamp() if value is not None else None


async def _skip_tick(s: AppState, view: ViewConfig, conn, vs: ViewStatus, *, advance_to: int | None) -> None:
    """Record a tick with nothing to merge: move the bookmark to ``advance_to``
    (None: leave it), clear any in-flight run and any error from an earlier
    failed tick, then run due maintenance."""
    vs.last_action = "skip"
    vs.last_error = None
    vs.chunks_total = None
    vs.chunks_done = 0
    REFRESH_TOTAL.labels(view=view.name, type="skip").inc()
    if s.history is not None:
        if advance_to is not None:
            await s.history.set_last_source_snapshot(view.name, advance_to)
        await s.history.clear_current_work(view.name)
    await _persist_view_status(s, view.name, vs)
    await maintain_view(s, view, conn, view.target_table)


async def refresh_view(s: AppState, view: ViewConfig) -> None:
    """One tick for one view; errors are recorded on its status, never raised."""
    conn = get_trino_connection(s)
    vs = s.view_statuses.setdefault(view.name, ViewStatus(name=view.name))
    try:
        async with asyncio.timeout(view.query_timeout_seconds):  # None → no timeout
            await _refresh(s, view, conn, vs)
    except SourceNotFound as e:
        vs.last_action = "waiting"
        vs.last_error = None
        vs.note = str(e)
        await _persist_view_status(s, view.name, vs)
    except TimeoutError:
        vs.last_error = f"refresh exceeded query_timeout_seconds={view.query_timeout_seconds}"
        vs.total_errors += 1
        REFRESH_ERRORS.labels(view=view.name).inc()
        log.warning("%s: refresh timed out after %ds", view.name, view.query_timeout_seconds)
        await _persist_view_status(s, view.name, vs)
    except Exception as e:
        vs.last_error = str(e)
        vs.total_errors += 1
        REFRESH_ERRORS.labels(view=view.name).inc()
        log.exception("%s: refresh failed", view.name)
        await _persist_view_status(s, view.name, vs)
    finally:
        # Shielded so a cancelled tick still cancels its Trino queries and closes.
        try:
            await asyncio.shield(conn.close())
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("%s: closing the Trino connection failed", view.name)


async def _refresh(s: AppState, view: ViewConfig, conn, vs: ViewStatus) -> None:
    """Bring ``view`` up to date with its source.

    A run pins the source snapshot it targets and records it (plus the last
    committed chunk) as ``current_work`` before merging. The bookmark moves to
    that snapshot only when the run completes, so an interrupted run resumes
    with the same pin and window. A missing bookmark means "full refresh".
    """
    vs.note = None
    parsed = parse_view_query(view.query)
    cursor = await conn.cursor()
    history = s.history
    rt = s.view_runtimes.get(view.name)
    if rt is not None and rt.rebuild_requested:
        rt.rebuild_requested = False
        if history:
            await history.set_last_source_snapshot(view.name, None)
            await history.clear_current_work(view.name)
        log.info("%s: rebuild requested → full refresh", view.name)
    bookmark = await history.get_last_source_snapshot(view.name) if history else None
    pinned, resume_from = await history.get_current_work(view.name) if history else (None, None)
    window = None

    if pinned is not None:
        try:
            if not await snapshot_exists(cursor, parsed.source_table, pinned):
                raise FullRefreshNeeded(f"pinned snapshot {pinned} expired")
            if bookmark is not None:
                window = await incremental_range_since(
                    cursor, parsed.source_table, parsed.filter_column, parsed.granularity, bookmark, pinned
                )
                if window is None:
                    vs.data_as_of = _epoch(await snapshot_committed_at(cursor, parsed.source_table, pinned))
                    await _skip_tick(s, view, conn, vs, advance_to=pinned)
                    return
            log.info("%s: resuming run pinned at snapshot %d from %s", view.name, pinned, resume_from)
        except FullRefreshNeeded as e:
            log.warning("%s: discarding interrupted run: %s", view.name, e)
            if history:
                await history.clear_current_work(view.name)
            pinned = resume_from = window = None

    pinned_at = None  # commit time of the pinned snapshot, when known
    if pinned is None:
        t0 = time.monotonic()
        result = await detect_changes(cursor, parsed.source_table, parsed.filter_column, parsed.granularity, bookmark)
        DETECTION_DURATION.labels(view=view.name).observe(time.monotonic() - t0)
        if result.current_snapshot is not None:
            SOURCE_SNAPSHOT.labels(view=view.name).set(result.current_snapshot)
        vs.last_checked = time.time()
        vs.source_committed_at = _epoch(result.committed_at)
        if result.action == RefreshAction.NO_CHANGE:
            vs.data_as_of = vs.source_committed_at  # nothing to merge: the view is current
            advance = result.current_snapshot if result.current_snapshot != bookmark else None
            await _skip_tick(s, view, conn, vs, advance_to=advance)
            return
        pinned, window, pinned_at = result.current_snapshot, result.filter_range, result.committed_at
        if history:
            if result.action == RefreshAction.FULL_REFRESH and bookmark is not None:
                # Forget the bookmark so a restart mid-run resumes it as a full refresh.
                await history.set_last_source_snapshot(view.name, None)
            await history.set_current_work(view.name, pinned, None)
        log.info("%s: %s at snapshot %d", view.name, result.action.name, pinned)

    columns = await discover_columns(cursor, view.query)
    await cursor.execute(build_create_table_sql(view.target_table, columns, view.target_partitioning))
    value_columns = [c.name for c in columns if c.name not in parsed.merge_keys]

    kind = "full" if window is None else "incremental"
    vs.last_action = f"chunked_{kind}" if view.full_refresh_chunk else kind
    vs.chunks_done = 0
    vs.chunks_total = None
    total_elapsed = 0.0
    total_rows = 0
    total_bytes = 0

    async for q in execute_refresh(
        cursor, view, parsed, value_columns, snapshot=pinned, window=window, resume_from=resume_from
    ):
        total_elapsed += q.elapsed_ms / 1000.0
        total_rows += q.processed_rows
        total_bytes += q.processed_bytes
        vs.last_refresh = time.time()
        vs.last_duration = q.elapsed_ms / 1000.0
        vs.last_error = None
        vs.last_range = f"[{q.range_start}, {q.range_end})"
        chunked = q.chunks_total > 1
        vs.chunks_done, vs.chunks_total = (q.chunks_done, q.chunks_total) if chunked else (0, None)
        await _record_query(s, view.name, vs, q)
        await _persist_view_status(s, view.name, vs)
        if chunked:
            # Only after the chunk's MERGE committed: a restart resumes after it.
            if history and q.range_end is not None:
                await history.set_current_work(view.name, pinned, q.range_end)
            CHUNKS_COMPLETED.labels(view=view.name).inc()
            CHUNK_DURATION.labels(view=view.name).observe(q.elapsed_ms / 1000.0)
            CHUNK_ROWS.labels(view=view.name).inc(q.processed_rows)
        if s.stop_event.is_set():
            log.info("%s: interrupted after chunk %d/%d; will resume", view.name, q.chunks_done, q.chunks_total)
            return

    # The view now reflects the source as of `pinned`; newer commits are next tick's.
    REFRESH_TOTAL.labels(view=view.name, type=kind).inc()
    if history:
        await history.set_last_source_snapshot(view.name, pinned)
        await history.clear_current_work(view.name)
    vs.total_refreshes += 1
    vs.chunks_total = None  # keep chunks_done so the UI can show "7/7"
    if pinned_at is None:
        pinned_at = await snapshot_committed_at(cursor, parsed.source_table, pinned)
    vs.data_as_of = _epoch(pinned_at)
    REFRESH_DURATION.labels(view=view.name).observe(total_elapsed)
    REFRESH_LAST_SUCCESS.labels(view=view.name).set(vs.last_refresh or time.time())
    REFRESH_BYTES.labels(view=view.name).inc(total_bytes)
    REFRESH_ROWS.labels(view=view.name).inc(total_rows)
    await _persist_view_status(s, view.name, vs)
    await maintain_view(s, view, conn, view.target_table)


async def view_worker(s: AppState, name: str) -> None:
    """Sole caller of ``refresh_view`` for this view.

    Asyncio is single-threaded: because this task is the only one that awaits
    ``refresh_view(view)`` for ``name``, refreshes are sequential by
    construction — no lock needed. Manual triggers set ``runtime.wake`` to
    request a refresh; bursts of triggers coalesce into one follow-up pass.
    """
    rt = s.view_runtimes.setdefault(name, ViewRuntime())
    try:
        while not s.stop_event.is_set():
            view = next(
                (v for v in (s.config.views if s.config else []) if v.name == name),
                None,
            )
            if view is None:
                log.info("%s: view removed, worker exiting", name)
                return

            rt.refresh_in_flight = True
            try:
                await refresh_view(s, view)
            except Exception:
                # refresh_view already logs + records its own exceptions; this is a
                # last-resort guard so the worker never dies on an unexpected error.
                log.exception("%s: worker caught unexpected error", name)
            finally:
                # Reset in_flight unconditionally — CancelledError (BaseException)
                # skips except-Exception and would otherwise leave it True for
                # any respawned worker reusing this ViewRuntime.
                rt.refresh_in_flight = False
            # seq/notify are intentionally outside the finally: on CancelledError
            # a parked trigger_refresh waiter is woken by the outer finally's
            # notify_all and bails via the s.config.views check (delete path)
            # or the socket drop (shutdown). Bumping seq from within a cancel
            # unwind would also require shielding an async lock acquire to be
            # safe against re-cancellation — not worth the complexity for paths
            # that already terminate the waiter another way.
            rt.refresh_seq += 1
            async with rt.refresh_cond:
                rt.refresh_cond.notify_all()

            with suppress(TimeoutError):
                async with asyncio.timeout(view.refresh_interval_seconds):
                    await rt.wake.wait()
            rt.wake.clear()
    finally:
        # Wake any trigger_refresh waiter that might still be parked on the
        # condition (e.g. view was deleted mid-wait); the waiter re-checks
        # s.config.views and bails with 410 rather than hanging forever.
        async with rt.refresh_cond:
            rt.refresh_cond.notify_all()


async def supervisor(s: AppState) -> None:
    """Reload config periodically and keep one worker task per configured view.

    ``s.workers`` is the shared worker map: the supervisor owns its lifecycle,
    but ``delete_view`` reaches in to cancel a worker synchronously rather
    than waiting for the next supervisor tick.
    """
    reload_config(s)

    def sync_workers() -> None:
        current = {v.name for v in (s.config.views if s.config else [])}
        for name in list(s.workers):
            t = s.workers[name]
            if name not in current or t.done():
                if t.done() and not t.cancelled():
                    exc = t.exception()
                    if exc is not None:
                        log.exception(
                            "%s: worker task exited with unhandled exception",
                            name,
                            exc_info=exc,
                        )
                t.cancel()
                s.workers.pop(name)
        for name in current:
            if name not in s.workers:
                log.info("spawning worker for %r", name)
                s.workers[name] = asyncio.create_task(view_worker(s, name))

    try:
        sync_workers()
        while not s.stop_event.is_set():
            reload_interval = s.config.server.config_reload_interval_seconds if s.config else 30
            # Wait on the stop event with a timeout — woken either by
            # shutdown (clean exit) or the timeout (config reload tick).
            with suppress(TimeoutError):
                async with asyncio.timeout(reload_interval):
                    await s.stop_event.wait()
            if s.stop_event.is_set():
                break
            reload_config(s)
            sync_workers()
    finally:
        for t in s.workers.values():
            t.cancel()
        await asyncio.gather(*s.workers.values(), return_exceptions=True)
        s.workers.clear()


# ── FastAPI lifespan ──


def resolve_state_db_path(
    views_path: Path,
    config_path: Path,
    configured: str,
) -> Path:
    """Resolve ``state_db_path`` from config to a concrete filesystem path.

    Absolute values are used as-is. For relative values, prefer the
    ``views.yaml`` directory as the anchor — in the Dockerfile that file
    is bind-mounted from the host, so it's persistent by construction
    (whereas ``config.yaml`` is often mounted file-only into ``/app``,
    which lives in the container's writable layer and gets wiped on
    image bumps). Falls back to the config file's
    directory when the views directory doesn't exist or isn't writable
    (e.g. test fixtures pointing at a not-yet-created path).
    """
    db_path = Path(configured)
    if db_path.is_absolute():
        return db_path
    views_dir = views_path.parent
    if views_dir.exists() and os.access(views_dir, os.W_OK):
        return views_dir / db_path
    return config_path.parent / db_path


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Allow tests to pre-seed state (with stop_event set to skip loop).
    if hasattr(app.state, "s"):
        s = app.state.s
        log.info("using pre-seeded app state")
    else:
        s = AppState(
            config_path=getattr(app.state, "config_path", Path("config.yaml")),
            views_path=getattr(app.state, "views_path", Path("views.yaml")),
        )
        reload_config(s)
        app.state.s = s

    # Unwind order matters: stop_event.set must run before the TaskGroup
    # drains so the supervisor exits cleanly. Stack registration is LIFO,
    # hence: history.close first, TaskGroup second, stop_event last.
    async with AsyncExitStack() as stack:
        if s.history is None and s.config is not None:
            db_path = resolve_state_db_path(
                s.views_path,
                s.config_path,
                s.config.server.state_db_path,
            )
            if str(db_path).startswith("/app/"):
                log.warning(
                    "state_db_path resolved under /app/ (%s) — in Docker this is "
                    "the container's writable layer and will be wiped on image "
                    "bumps. Mount a host directory for views.yaml or set an "
                    "absolute server.state_db_path on a persistent volume.",
                    db_path,
                )
            db_path.parent.mkdir(parents=True, exist_ok=True)
            s.history = QueryHistory(db_path, RECENT_QUERY_LIMIT)
            await s.history.open()
            # Register close immediately so a failure in hydrate_view_state
            # (next line) still releases the SQLite connection.
            stack.push_async_callback(s.history.close)
            await hydrate_view_state(s)

        if not os.environ.get("IVM_PASSWORD"):
            log.warning("IVM_PASSWORD is not set: the UI and API accept anyone who can reach this port")
        log.info(
            "starting supervisor — %d views configured",
            len(s.config.views) if s.config else 0,
        )
        tg = await stack.enter_async_context(asyncio.TaskGroup())

        def _signal_shutdown() -> None:
            log.info("shutting down supervisor")
            s.stop_event.set()

        # Registered after the TaskGroup so it runs *before* the group's
        # __aexit__ on unwind — supervisor sees stop_event set before we
        # wait on it to drain.
        stack.callback(_signal_shutdown)
        tg.create_task(supervisor(s))
        yield


app = FastAPI(title="iceberg-ivm", lifespan=lifespan)

_OPEN_PATHS = frozenset({"/health", "/metrics"})


@app.middleware("http")
async def basic_auth(request: Request, call_next):
    """When ``IVM_PASSWORD`` is set, require it (HTTP Basic, any user name)
    for the UI and API. ``/health`` and ``/metrics`` stay open for probes."""
    password = os.environ.get("IVM_PASSWORD")
    if password and request.url.path not in _OPEN_PATHS:
        scheme, _, encoded = request.headers.get("authorization", "").partition(" ")
        try:
            supplied = base64.b64decode(encoded).decode().partition(":")[2] if scheme.lower() == "basic" else ""
        except (ValueError, UnicodeDecodeError):
            supplied = ""
        if not secrets.compare_digest(supplied.encode(), password.encode()):
            return Response(status_code=401, headers={"WWW-Authenticate": 'Basic realm="iceberg-ivm"'})
    return await call_next(request)


# ── API models + form schema, all derived from ViewConfig ──

# Per-field presentation metadata: label + anything non-inferrable from the
# dataclass annotation/default. Not in the map → derived defaults (label from
# Title Case, required=False, type inferred from annotation).
_GRAN_OPTIONS = [{"value": "", "label": "— none (single-shot) —"}] + [
    {"value": g, "label": g} for g in ("hour", "day", "week", "month", "quarter", "year")
]
_FIELD_META: dict[str, dict] = {
    "name": {
        "placeholder": "defaults to target table",
        "disabled_on_edit": True,
        "help": (
            "optional label used to identify this view in the API and UI. "
            "Leave blank to default to the target table FQDN."
        ),
    },
    "query": {
        "required": True,
        "type": "text",
        "rows": 10,
        "disabled_on_edit": True,
        "placeholder": (
            "SELECT symbol,\n       date_trunc('minute', ts) AS minute,\n"
            "       min_by(price, ts) AS open,\n       max(price)        AS high,\n"
            "       min(price)        AS low,\n       max_by(price, ts) AS close\n"
            "FROM iceberg.market_data.trades\nGROUP BY symbol, date_trunc('minute', ts)"
        ),
        "help": (
            "exactly what you would write after CREATE MATERIALIZED VIEW … AS. "
            "source table, filter column, granularity and merge keys are "
            "derived automatically from the query."
        ),
    },
    "target_table": {"required": True, "group": "target", "placeholder": "iceberg.analytics.my_view"},
    "target_partitioning": {"group": "target", "placeholder": "ARRAY['day(minute)']", "help": "unpartitioned if blank"},
    "refresh_interval_seconds": {"min": 1, "suffix": "seconds", "label": "Refresh Interval"},
    "query_timeout_seconds": {
        # Set explicitly: ``_build_form_schema``'s inference resolves
        # ``int | None`` to ``types.UnionType``, not ``int``, and would
        # otherwise render this as a text input.
        "type": "number",
        "min": 1,
        "suffix": "seconds",
        "label": "Query Timeout",
        "help": "Optional. Max wall-clock seconds for a single refresh tick. Leave blank for no timeout.",
    },
    "full_refresh_chunk": {
        "type": "select",
        "group": "target",
        "options": _GRAN_OPTIONS,
        "label": "Full Refresh Chunk Size",
        "help": (
            "If set, the first-run backfill is split into chunks of this size "
            "and each chunk is committed independently. Must be coarser-or-equal "
            "to the view's own date_trunc granularity. Sub-second views "
            "(second / millisecond) must pick minute or coarser."
        ),
    },
    "maintenance_interval_seconds": {
        "min": 0,
        "suffix": "seconds",
        "group": "maintenance",
        "label": "Maintenance Interval",
        "help": "Shared interval for every enabled op. 0 disables maintenance entirely.",
    },
    "optimize": {
        "group": "maintenance",
        "label": "Optimize",
        "help": "Run ALTER TABLE ... EXECUTE optimize on each tick.",
    },
    "optimize_file_size_threshold": {
        "group": "maintenance",
        "label": "Optimize File Size Threshold",
        "placeholder": "e.g. 128MB (default: Trino's 100MB)",
    },
    "expire_snapshots": {
        "group": "maintenance",
        "label": "Expire Snapshots",
        "help": "Run ALTER TABLE ... EXECUTE expire_snapshots on each tick.",
    },
    "expire_snapshots_retention": {
        "group": "maintenance",
        "label": "Expire Snapshots Retention",
        "help": "Trino duration (e.g. '7d'). Must be ≥ catalog's min-retention.",
    },
    "remove_orphan_files": {
        "group": "maintenance",
        "label": "Remove Orphan Files",
        "help": "Run ALTER TABLE ... EXECUTE remove_orphan_files on each tick.",
    },
    "remove_orphan_files_retention": {
        "group": "maintenance",
        "label": "Remove Orphan Files Retention",
        "help": "Trino duration (e.g. '7d'). Must be ≥ catalog's min-retention.",
    },
}


# Resolve string annotations to real types (PEP 563 / `from __future__ import annotations`).
def _build_form_schema() -> list[dict]:
    schema = []
    for name, f in ViewConfig.model_fields.items():
        meta = _FIELD_META.get(name, {})
        default_type = "boolean" if f.annotation is bool else "number" if f.annotation is int else "string"
        entry = {
            "name": name,
            "label": meta.get("label", name.replace("_", " ").title()),
            "type": meta.get("type", default_type),
            "required": meta.get("required", False),
            "disabled_on_edit": meta.get("disabled_on_edit", False),
        }
        if not f.is_required():
            entry["default"] = f.default
        for k in ("placeholder", "help", "group", "options", "rows", "min", "suffix"):
            if k in meta:
                entry[k] = meta[k]
        schema.append(entry)
    return schema


VIEW_FORM_SCHEMA: list[dict] = _build_form_schema()


def _validate_view(data: dict) -> ViewConfig:
    """``ViewConfig`` from a request body, or a 422 with readable messages."""
    try:
        return ViewConfig.model_validate(data)
    except ValidationError as e:
        messages = []
        for err in e.errors():
            msg = err["msg"].removeprefix("Value error, ")
            where = ".".join(str(p) for p in err["loc"])
            messages.append(f"{where}: {msg}" if where else msg)
        raise HTTPException(422, "; ".join(messages)) from None


class ViewResponse(BaseModel):
    """View as returned by the API.

    ``source_table``, ``filter_column``, ``merge_keys`` are derived from the
    query AST and added for the UI's benefit; they are not accepted on POST.
    """

    model_config = {"extra": "allow"}
    source_table: str
    filter_column: str
    merge_keys: tuple[str, ...]
    status: dict | None = None


def rewrite_info_uri(info_uri: str, internal_url: str, public_url: str) -> str:
    """Return ``info_uri`` with its ``internal_url`` prefix replaced by ``public_url``.

    Trino derives the per-query ``infoUri`` it returns from the *client's*
    request URL, so when the orchestrator and the user's browser talk to
    Trino over different hostnames (e.g. ``trino:8080`` inside a docker
    network vs ``localhost:28080`` from the host), the deep-link in the UI's
    recent-queries panel is unreachable. Rewriting at the response boundary
    fixes the link without coupling the orchestrator's egress URL to its
    user-facing URL.

    No-ops when ``info_uri`` is empty, when the URLs match (single-host
    deployments), or when ``info_uri`` does not start with ``internal_url``
    (don't munge arbitrary URLs).
    """
    if not info_uri or internal_url == public_url:
        return info_uri
    internal = internal_url.rstrip("/")
    public = public_url.rstrip("/")
    # Require a path boundary so an internal URL of `http://trino:8080`
    # doesn't accidentally match `http://trino:8080xyz/…`. Trino's real
    # info_uris always carry a path, but the helper is part of the module
    # API and a stricter check is cheap insurance.
    if info_uri == internal or info_uri.startswith(internal + "/"):
        return public + info_uri[len(internal) :]
    return info_uri


def _view_to_response(v: ViewConfig, vs: ViewStatus | None, trino: TrinoConfig | None = None) -> ViewResponse:
    parsed = parse_view_query(v.query)
    data = v.model_dump()
    status_dict: dict | None = dataclasses.asdict(vs) if vs else None
    if status_dict and trino is not None and trino.public_url and trino.public_url != trino.url:
        for q in status_dict.get("recent_queries") or ():
            q["info_uri"] = rewrite_info_uri(q.get("info_uri") or "", trino.url, trino.public_url)
    return ViewResponse(
        source_table=parsed.source_table,
        filter_column=parsed.filter_column,
        merge_keys=parsed.merge_keys,
        status=status_dict,
        **data,
    )


# ── Endpoints ──


@app.get("/api/views/schema")
def view_schema():
    """Return form field metadata so the UI can render dynamically."""
    return VIEW_FORM_SCHEMA


@app.get("/health")
def health(s: AppState = Depends(get_app_state)):
    return {"status": "ok", "views": len(s.config.views) if s.config else 0}


@app.get("/metrics")
def metrics():
    return PlainTextResponse(
        generate_latest().decode(),
        media_type="text/plain; version=0.0.4",
    )


@app.get("/api/views")
def list_views(s: AppState = Depends(get_app_state)) -> list[ViewResponse]:
    if not s.config:
        return []
    trino = s.config.trino
    return [_view_to_response(v, s.view_statuses.get(v.name), trino) for v in s.config.views]


class ParseRequest(BaseModel):
    query: str


class ParseResponse(BaseModel):
    source_table: str
    filter_column: str
    granularity: str
    merge_keys: tuple[str, ...]


@app.post("/api/views/parse")
def parse_query(body: ParseRequest) -> ParseResponse:
    """Parse a view query and return the derived attributes.

    Used by the UI to live-validate the query as the operator types.
    Returns 422 with a human-readable detail on any parse violation.
    """
    try:
        p = parse_view_query(body.query)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc
    return ParseResponse(
        source_table=p.source_table,
        filter_column=p.filter_column,
        granularity=p.granularity,
        merge_keys=p.merge_keys,
    )


def _save(s: AppState, views: list[ViewConfig]) -> None:
    save_views(views, s.views_path)
    s.config = Config(trino=s.config.trino, views=views, server=s.config.server)
    s.views_mtime = s.views_path.stat().st_mtime
    VIEWS_CONFIGURED.set(len(views))


@app.post("/api/views", status_code=201)
def create_view(body: dict = Body(...), s: AppState = Depends(get_app_state)) -> ViewResponse:
    if not s.config:
        raise HTTPException(500, "config not loaded")
    view = _validate_view(body)
    if any(v.name == view.name for v in s.config.views):
        raise HTTPException(409, f"view '{view.name}' already exists")
    _save(s, [*s.config.views, view])
    s.view_statuses[view.name] = ViewStatus(name=view.name)
    log.info("created view %r via API", view.name)
    return _view_to_response(view, s.view_statuses[view.name], s.config.trino)


@app.put("/api/views/{name}")
def update_view(name: str, body: dict = Body(...), s: AppState = Depends(get_app_state)) -> ViewResponse:
    """Change a view's settings. Its name and query are its identity: to
    change either, delete the view and create a new one."""
    if not s.config:
        raise HTTPException(500, "config not loaded")
    existing = next((v for v in s.config.views if v.name == name), None)
    if not existing:
        raise HTTPException(404, f"view '{name}' not found")
    if body.get("name") not in (None, "", name):
        raise HTTPException(422, "name cannot be changed; delete and recreate the view instead")
    view = _validate_view({**body, "name": name})
    if view.query.strip() != existing.query.strip():
        raise HTTPException(422, "query cannot be changed; delete and recreate the view instead")
    _save(s, [view if v.name == name else v for v in s.config.views])
    log.info("updated view %r via API", name)
    return _view_to_response(view, s.view_statuses.get(name), s.config.trino)


@app.delete("/api/views/{name}", status_code=204)
async def delete_view(name: str, s: AppState = Depends(get_app_state)):
    if not s.config:
        raise HTTPException(500, "config not loaded")
    if not any(v.name == name for v in s.config.views):
        raise HTTPException(404, f"view '{name}' not found")
    _save(s, [v for v in s.config.views if v.name != name])
    # Cancel and await the worker before clearing runtime state and history.
    # The supervisor's sync_workers would do this on its next tick (~1s
    # later), during which the worker could keep writing view_status and
    # MERGE rows for an already-deleted view. Synchronous cancellation
    # closes that window.
    task = s.workers.pop(name, None)
    if task is not None and not task.done():
        task.cancel()
        with suppress(asyncio.CancelledError, Exception):
            await task
    s.view_statuses.pop(name, None)
    s.view_runtimes.pop(name, None)
    if s.history is not None:
        await s.history.delete_view(name)
    log.info("deleted view %r via API", name)


@app.post("/api/views/{name}/refresh")
async def trigger_refresh(name: str, s: AppState = Depends(get_app_state)):
    if not s.config:
        raise HTTPException(500, "config not loaded")
    view = next((v for v in s.config.views if v.name == name), None)
    if not view:
        raise HTTPException(404, f"view '{name}' not found")

    rt = s.view_runtimes.setdefault(name, ViewRuntime())
    log.info("manual refresh triggered for %r", name)

    # We want the caller to see the status of a refresh that observed their
    # trigger. If a refresh is already in flight, it started before our wake
    # and can't reflect it — wait for the *next* completion after that.
    seq_before = rt.refresh_seq
    target = seq_before + (2 if rt.refresh_in_flight else 1)
    rt.wake.set()
    async with rt.refresh_cond:
        while rt.refresh_seq < target:
            # If the view was deleted while we waited, the worker's finally
            # notify_all will wake us; bail rather than re-entering wait().
            if not any(v.name == name for v in s.config.views):
                raise HTTPException(410, f"view '{name}' deleted")
            await rt.refresh_cond.wait()

    vs = s.view_statuses.get(name)
    return {
        "status": "ok",
        "last_action": vs.last_action if vs else None,
        "last_error": vs.last_error if vs else None,
    }


@app.post("/api/views/{name}/rebuild")
async def rebuild_view(name: str, s: AppState = Depends(get_app_state)):
    """Recompute the whole view from its source now. The target ends up
    exactly equal to the query; it stays readable throughout."""
    if not s.config or not any(v.name == name for v in s.config.views):
        raise HTTPException(404, f"view '{name}' not found")
    s.view_runtimes.setdefault(name, ViewRuntime()).rebuild_requested = True
    return await trigger_refresh(name, s)


@app.get("/", response_class=HTMLResponse)
def ui():
    return HTMLResponse((Path(__file__).parent / "static" / "index.html").read_text())
