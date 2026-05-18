"""FastAPI server: web UI, REST API, Prometheus metrics, refresh loop."""

from __future__ import annotations

import asyncio
import dataclasses
import logging
import os
import time
import typing
from contextlib import AsyncExitStack, asynccontextmanager, suppress
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse

import aiotrino
from aiotrino.auth import BasicAuthentication
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, PlainTextResponse
from prometheus_client import Counter, Gauge, Histogram, generate_latest
from pydantic import BaseModel, create_model, field_validator

from iceberg_ivm.config import (
    Config,
    TrinoConfig,
    ViewConfig,
    load_config,
    load_views,
    save_views,
    validate_chunk_compatibility,
    validate_maintenance_config,
    validate_qualified_name,
    validate_view_name,
)
from iceberg_ivm.detector import RefreshAction, detect_changes
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


def get_trino_connection(s: AppState) -> aiotrino.dbapi.Connection:
    cfg = s.config
    parsed = urlparse(cfg.trino.url)
    scheme = parsed.scheme or "http"
    host = parsed.hostname
    port = parsed.port or (443 if scheme == "https" else 80)
    if not host:
        raise ValueError(f"TRINO_URL does not contain a host: {cfg.trino.url!r}")
    log.debug("connecting to Trino at %s as %s", cfg.trino.url, cfg.trino.user)
    # Pin every session to UTC so Trino's `date_trunc` on TIMESTAMP WITH
    # TIME ZONE columns agrees with the Python-side expand_to_bucket_bounds math. See
    # DESIGN.md "Timezone assumption" for the full rationale.
    kwargs = {
        "host": host,
        "port": port,
        "http_scheme": scheme,
        "catalog": cfg.trino.catalog,
        "schema": cfg.trino.schema,
        "user": cfg.trino.user,
        "timezone": "UTC",
    }
    if cfg.trino.password:
        kwargs["auth"] = BasicAuthentication(cfg.trino.user, cfg.trino.password)
    return aiotrino.dbapi.connect(**kwargs)


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
    }


async def _persist_view_status(s: AppState, view_name: str, vs: ViewStatus) -> None:
    """Mirror ``vs`` to ``view_status`` if a history is attached.

    Called from every refresh/maintenance mutation site — write rate is
    negligible (a few inserts per refresh, seconds apart) so we don't
    batch. Silently no-ops in tests that don't attach a QueryHistory.
    """
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


async def refresh_view(s: AppState, view: ViewConfig) -> None:
    conn = get_trino_connection(s)
    cursor = await conn.cursor()
    vs = s.view_statuses.setdefault(view.name, ViewStatus(name=view.name))

    try:
        # asyncio.timeout(None) is a documented no-op, so no None-branch needed.
        async with asyncio.timeout(view.query_timeout_seconds):
            parsed = parse_view_query(view.query)
            target_table = view.target_table

            # Create target on first run (unpartitioned by default; see #22).
            columns = await discover_columns(cursor, view.query)
            await cursor.execute(
                build_create_table_sql(
                    target_table,
                    columns,
                    view.target_partitioning,
                )
            )
            value_columns = [c.name for c in columns if c.name not in parsed.merge_keys]

            last_snap = await s.history.get_last_source_snapshot(view.name) if s.history is not None else None

            t0 = time.monotonic()
            result = await detect_changes(
                cursor,
                parsed.source_table,
                parsed.filter_column,
                parsed.granularity,
                last_snap,
            )
            DETECTION_DURATION.labels(view=view.name).observe(time.monotonic() - t0)
            log.info("%s: detection → %s (%.3fs)", view.name, result.action.name, time.monotonic() - t0)
            if result.current_snapshot is not None:
                SOURCE_SNAPSHOT.labels(view=view.name).set(result.current_snapshot)

            if result.action == RefreshAction.NO_CHANGE:
                vs.last_action = "skip"
                # Clear any lingering in-flight chunked-backfill progress that may
                # have been hydrated from view_status after a mid-backfill restart.
                # If the source has caught up while iceberg-ivm was down, the
                # next tick is NO_CHANGE — but the persisted chunks_total would
                # still claim a backfill is in flight. The committed bookmark
                # (last_source_snapshot) is the source of truth here.
                vs.chunks_total = None
                vs.chunks_done = 0
                REFRESH_TOTAL.labels(view=view.name, type="skip").inc()
                # Advance state past empty-append / compaction-only snapshots so
                # we don't re-detect them every cycle.
                if (
                    result.current_snapshot is not None
                    and result.current_snapshot != last_snap
                    and s.history is not None
                ):
                    await s.history.set_last_source_snapshot(view.name, result.current_snapshot)
                await _persist_view_status(s, view.name, vs)
                await maintain_view(s, view, conn, target_table)
                return

            # Set vs.last_action up-front so /api/views reflects what's running,
            # not what last finished — matters for multi-hour chunked backfills.
            chunked = result.action == RefreshAction.FULL_REFRESH and view.full_refresh_chunk
            if result.action == RefreshAction.FULL_REFRESH:
                vs.last_action = "chunked_full" if chunked else "full"
                vs.chunks_done = 0
                vs.chunks_total = None
            else:
                vs.last_action = "incremental"

            incremental_range = result.filter_range if result.action == RefreshAction.INCREMENTAL else None
            total_elapsed = 0.0
            total_rows = 0
            total_bytes = 0
            total_queries = 0

            async for q in execute_refresh(
                cursor,
                view,
                target_table,
                parsed,
                value_columns,
                incremental_range=incremental_range,
            ):
                total_elapsed += q.elapsed_ms / 1000.0
                total_rows += q.processed_rows
                total_bytes += q.processed_bytes
                total_queries += 1
                vs.last_refresh = time.time()
                vs.last_duration = q.elapsed_ms / 1000.0
                vs.last_error = None
                vs.last_range = f"[{q.range_start}, {q.range_end})"
                # Keep chunks_done aligned with chunks_total: a non-chunked
                # single-shot still arrives with q.chunks_done=1, q.chunks_total=1,
                # and persisting "1/None" creates phantom progress in the UI.
                if q.chunks_total > 1:
                    vs.chunks_done = q.chunks_done
                    vs.chunks_total = q.chunks_total
                else:
                    vs.chunks_done = 0
                    vs.chunks_total = None
                await _record_query(s, view.name, vs, q)
                await _persist_view_status(s, view.name, vs)
                if q.chunks_total > 1:
                    CHUNKS_COMPLETED.labels(view=view.name).inc()
                    CHUNK_DURATION.labels(view=view.name).observe(q.elapsed_ms / 1000.0)
                    CHUNK_ROWS.labels(view=view.name).inc(q.processed_rows)
                if s.stop_event.is_set():
                    # Graceful shutdown mid-backfill — leave last_source_snapshot
                    # unset so the next tick resumes from target metadata.
                    log.info("%s: refresh interrupted after chunk %d/%d", view.name, q.chunks_done, q.chunks_total)
                    return

            # Non-chunked paths (full or incremental) and completed chunked backfills
            # commit the source snapshot bookmark. Empty chunked runs (source empty,
            # or fully caught up) also advance state — total_queries == 0 is fine.
            REFRESH_TOTAL.labels(view=view.name, type="incremental" if incremental_range else "full").inc()
            if s.history is not None:
                await s.history.set_last_source_snapshot(view.name, result.current_snapshot)
            vs.total_refreshes += 1
            # Clear the "in-flight" marker on clean completion. Leave chunks_done
            # at its last value so the UI can show "12/12 done" alongside total
            # counters until the next tick overwrites it.
            vs.chunks_total = None
            REFRESH_DURATION.labels(view=view.name).observe(total_elapsed)
            REFRESH_LAST_SUCCESS.labels(view=view.name).set(vs.last_refresh or time.time())
            REFRESH_BYTES.labels(view=view.name).inc(total_bytes)
            REFRESH_ROWS.labels(view=view.name).inc(total_rows)
            await _persist_view_status(s, view.name, vs)
            await maintain_view(s, view, conn, target_table)

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
        # Shield the close so a task cancellation mid-execute doesn't cancel
        # the close itself and leak the Trino connection. Re-raise CancelledError
        # so the task actually finishes cancelling.
        try:
            await asyncio.shield(conn.close())
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("%s: conn.close failed", view.name)


async def view_worker(s: AppState, name: str) -> None:
    """Sole caller of ``refresh_view`` for this view.

    Asyncio is single-threaded: because this task is the only one that awaits
    ``refresh_view(view)`` for ``name``, refreshes are sequential by
    construction — no lock needed. Manual triggers set ``runtime.wake`` to
    request a refresh; bursts of triggers coalesce into a single follow-up
    pass, which removes the refresh-storm behaviour described in #24.
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
    image bumps — see issue #39). Falls back to the config file's
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
                    "absolute server.state_db_path on a persistent volume "
                    "(see issue #39).",
                    db_path,
                )
            db_path.parent.mkdir(parents=True, exist_ok=True)
            s.history = QueryHistory(db_path, RECENT_QUERY_LIMIT)
            await s.history.open()
            # Register close immediately so a failure in hydrate_view_state
            # (next line) still releases the SQLite connection.
            stack.push_async_callback(s.history.close)
            await hydrate_view_state(s)

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
_VIEW_TYPES: dict[str, type] = typing.get_type_hints(ViewConfig)


def _build_form_schema() -> list[dict]:
    schema = []
    for f in dataclasses.fields(ViewConfig):
        meta = _FIELD_META.get(f.name, {})
        t = _VIEW_TYPES[f.name]
        default_type = "boolean" if t is bool else "number" if t is int else "string"
        entry = {
            "name": f.name,
            "label": meta.get("label", f.name.replace("_", " ").title()),
            "type": meta.get("type", default_type),
            "required": meta.get("required", False),
            # Always emit a boolean so the UI's `:disabled` binding never
            # receives `undefined` — Alpine renders an `undefined` value as
            # a present `disabled` attribute, which silently locks the field.
            "disabled_on_edit": meta.get("disabled_on_edit", False),
        }
        if f.default is not dataclasses.MISSING:
            entry["default"] = f.default
        for k in ("placeholder", "help", "group", "options", "rows", "min", "suffix"):
            if k in meta:
                entry[k] = meta[k]
        schema.append(entry)
    return schema


VIEW_FORM_SCHEMA: list[dict] = _build_form_schema()


def _build_view_create_model() -> type[BaseModel]:
    """Pydantic ViewCreate model auto-derived from ViewConfig fields.

    ``name`` is optional at the API boundary — both ``""`` and ``null``
    round-trip and are substituted with ``target_table`` inside the create
    handler. The ``str | None`` typing keeps any reasonable client (UI
    JS that emits ``null`` for blank optional fields, curl, etc.) from
    needing to know our defaulting rule.
    """
    fields_spec: dict[str, tuple] = {}
    for f in dataclasses.fields(ViewConfig):
        if f.name == "name":
            field_type: object = str | None
            default = None
        else:
            field_type = _VIEW_TYPES[f.name]
            default = ... if f.default is dataclasses.MISSING else f.default
        fields_spec[f.name] = (field_type, default)

    def _check_name(cls, v):
        if v:
            validate_view_name(v, "name")
        return v

    def _check_target_table(cls, v):
        validate_qualified_name(v, "target_table")
        return v

    validators = {
        "_v_name": field_validator("name")(_check_name),
        "_v_target_table": field_validator("target_table")(_check_target_table),
    }
    return create_model("ViewCreate", __validators__=validators, **fields_spec)


ViewCreate = _build_view_create_model()


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
    data = {f.name: getattr(v, f.name) for f in dataclasses.fields(ViewConfig)}
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


@app.post("/api/views", status_code=201)
def create_view(
    body: ViewCreate,
    s: AppState = Depends(get_app_state),
) -> ViewResponse:
    if not s.config:
        raise HTTPException(500, "config not loaded")

    # Default name → target_table FQDN. Lets API/UI callers omit a redundant
    # label when they're happy to identify the view by where it writes.
    name = body.name or body.target_table
    try:
        validate_view_name(name, "name")
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc

    if any(v.name == name for v in s.config.views):
        raise HTTPException(409, f"view '{name}' already exists")

    # Validate the query — raises on any violation.  Rejected queries never
    # make it into saved state.
    try:
        parse_view_query(body.query)
        validate_chunk_compatibility(body.full_refresh_chunk, body.query)
        validate_maintenance_config(body.model_dump())
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc

    # Normalize the UI's empty-string sentinel ("single-shot") to None so it
    # round-trips through YAML the same way YAML-loaded views do.
    payload = body.model_dump()
    payload["name"] = name
    if not payload.get("full_refresh_chunk"):
        payload["full_refresh_chunk"] = None
    if not payload.get("optimize_file_size_threshold"):
        payload["optimize_file_size_threshold"] = None
    new_view = ViewConfig(**payload)
    new_views = list(s.config.views) + [new_view]
    new_cfg = Config(trino=s.config.trino, views=new_views, server=s.config.server)
    save_views(new_views, s.views_path)
    s.config = new_cfg
    s.views_mtime = s.views_path.stat().st_mtime
    s.view_statuses[name] = ViewStatus(name=name)
    VIEWS_CONFIGURED.set(len(new_views))
    log.info("created view %r via API", name)

    return _view_to_response(new_view, s.view_statuses[name], s.config.trino)


@app.put("/api/views/{name}")
def update_view(
    name: str,
    body: ViewCreate,
    s: AppState = Depends(get_app_state),
) -> ViewResponse:
    """Update mutable fields of an existing view.

    ``name`` and ``query`` are the view's identity — changing the query would
    silently orphan already-materialized rows (the target table gets recreated
    on the next refresh), so both are rejected here. To change either, delete
    the view and recreate it.
    """
    if not s.config:
        raise HTTPException(500, "config not loaded")
    existing = next((v for v in s.config.views if v.name == name), None)
    if not existing:
        raise HTTPException(404, f"view '{name}' not found")
    if body.name and body.name != name:
        raise HTTPException(422, "name cannot be changed; delete and recreate the view instead")
    if body.query.strip() != existing.query.strip():
        raise HTTPException(422, "query cannot be changed; delete and recreate the view instead")

    try:
        validate_chunk_compatibility(body.full_refresh_chunk, body.query)
        validate_maintenance_config(body.model_dump())
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc

    payload = body.model_dump()
    payload["name"] = name
    if not payload.get("full_refresh_chunk"):
        payload["full_refresh_chunk"] = None
    if not payload.get("optimize_file_size_threshold"):
        payload["optimize_file_size_threshold"] = None
    updated = ViewConfig(**payload)
    new_views = [updated if v.name == name else v for v in s.config.views]
    new_cfg = Config(trino=s.config.trino, views=new_views, server=s.config.server)
    save_views(new_views, s.views_path)
    s.config = new_cfg
    s.views_mtime = s.views_path.stat().st_mtime
    log.info("updated view %r via API", name)

    return _view_to_response(updated, s.view_statuses.get(name), s.config.trino)


@app.delete("/api/views/{name}", status_code=204)
async def delete_view(name: str, s: AppState = Depends(get_app_state)):
    if not s.config:
        raise HTTPException(500, "config not loaded")
    if not any(v.name == name for v in s.config.views):
        raise HTTPException(404, f"view '{name}' not found")
    new_views = [v for v in s.config.views if v.name != name]
    new_cfg = Config(trino=s.config.trino, views=new_views, server=s.config.server)
    save_views(new_views, s.views_path)
    s.config = new_cfg
    s.views_mtime = s.views_path.stat().st_mtime
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
    VIEWS_CONFIGURED.set(len(new_views))
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


@app.get("/", response_class=HTMLResponse)
def ui():
    return HTMLResponse((Path(__file__).parent / "static" / "index.html").read_text())
