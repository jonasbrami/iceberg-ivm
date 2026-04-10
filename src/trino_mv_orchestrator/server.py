"""FastAPI server: web UI, REST API, Prometheus metrics, refresh loop."""
from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path

import trino
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, PlainTextResponse
from prometheus_client import Counter, Gauge, Histogram, generate_latest
from pydantic import BaseModel

from trino_mv_orchestrator.config import Config, ViewConfig, load_config, save_config
from trino_mv_orchestrator.detector import RefreshAction, detect_changes
from trino_mv_orchestrator.executor import execute_full_refresh, execute_incremental_refresh
from trino_mv_orchestrator.introspect import (
    build_create_table_sql,
    discover_columns,
    discover_source_partitioning,
)
from trino_mv_orchestrator.state import read_last_snapshot, write_last_snapshot

log = logging.getLogger(__name__)

# ── Prometheus metrics ──

REFRESH_TOTAL = Counter("mv_refresh_total", "Total refresh operations", ["view", "type"])
REFRESH_DURATION = Histogram("mv_refresh_duration_seconds", "Refresh duration", ["view"])
REFRESH_LAST_SUCCESS = Gauge("mv_refresh_last_success_timestamp", "Last successful refresh", ["view"])
REFRESH_ERRORS = Counter("mv_refresh_errors_total", "Refresh errors", ["view"])
CONFIG_RELOADS = Counter("mv_config_reload_total", "Config reload events")
VIEWS_CONFIGURED = Gauge("mv_views_configured", "Number of configured views")


@dataclass
class ViewStatus:
    name: str
    last_refresh: float | None = None
    last_duration: float | None = None
    last_action: str = "pending"
    last_range: str | None = None
    last_error: str | None = None
    total_refreshes: int = 0
    total_errors: int = 0


@dataclass
class AppState:
    config_path: Path = field(default_factory=lambda: Path("config.yaml"))
    config: Config | None = None
    config_mtime: float = 0
    view_statuses: dict[str, ViewStatus] = field(default_factory=dict)
    _stop: bool = False


state = AppState()


def get_trino_connection() -> trino.dbapi.Connection:
    cfg = state.config
    return trino.dbapi.connect(
        host=cfg.trino.host, port=cfg.trino.port,
        catalog=cfg.trino.catalog, schema=cfg.trino.schema,
        user=cfg.trino.user,
    )


def resolve_target_table(view: ViewConfig, cfg: Config) -> str:
    return view.target_table or f"{cfg.trino.catalog}.{cfg.trino.schema}.{view.name}"


def reload_config() -> bool:
    try:
        mtime = state.config_path.stat().st_mtime
    except FileNotFoundError:
        return False
    if mtime <= state.config_mtime:
        return False
    try:
        new_cfg = load_config(state.config_path)
        state.config = new_cfg
        state.config_mtime = mtime
        VIEWS_CONFIGURED.set(len(new_cfg.views))
        CONFIG_RELOADS.inc()
        log.info("config reloaded: %d views", len(new_cfg.views))
        for v in new_cfg.views:
            if v.name not in state.view_statuses:
                state.view_statuses[v.name] = ViewStatus(name=v.name)
        return True
    except Exception:
        log.exception("failed to reload config")
        return False


def refresh_view(view: ViewConfig) -> None:
    conn = get_trino_connection()
    cursor = conn.cursor()
    vs = state.view_statuses.setdefault(view.name, ViewStatus(name=view.name))

    try:
        target_table = resolve_target_table(view, state.config)

        # Auto-discover columns and create target
        columns = discover_columns(cursor, view.query)
        target_partitioning = view.target_partitioning or discover_source_partitioning(cursor, view.source_table)
        create_sql = build_create_table_sql(target_table, columns, target_partitioning)
        cursor.execute(create_sql)

        value_columns = [c.name for c in columns if c.name not in view.merge_keys]

        # Read state
        last_snap = read_last_snapshot(cursor, target_table)

        # Detect changes via file-level column stats
        result = detect_changes(
            cursor, view.source_table,
            view.filter_column, view.filter_granularity,
            last_snap,
        )

        if result.action == RefreshAction.NO_CHANGE:
            log.info("%s: no changes, skipping", view.name)
            vs.last_action = "skip"
            REFRESH_TOTAL.labels(view=view.name, type="skip").inc()
            return

        if result.action == RefreshAction.FULL_REFRESH:
            elapsed = execute_full_refresh(cursor, view, target_table)
            vs.last_action = "full"
            vs.last_range = None
            REFRESH_TOTAL.labels(view=view.name, type="full").inc()
        else:
            elapsed = execute_incremental_refresh(
                cursor, view, target_table, value_columns, result.filter_range,
            )
            vs.last_action = "incremental"
            vs.last_range = f"[{result.filter_range[0]}, {result.filter_range[1]})"
            REFRESH_TOTAL.labels(view=view.name, type="incremental").inc()

        write_last_snapshot(cursor, target_table, result.current_snapshot)

        vs.last_refresh = time.time()
        vs.last_duration = elapsed
        vs.last_error = None
        vs.total_refreshes += 1
        REFRESH_DURATION.labels(view=view.name).observe(elapsed)
        REFRESH_LAST_SUCCESS.labels(view=view.name).set(vs.last_refresh)

    except Exception as e:
        vs.last_error = str(e)
        vs.total_errors += 1
        REFRESH_ERRORS.labels(view=view.name).inc()
        log.exception("%s: refresh failed", view.name)
    finally:
        conn.close()


async def refresh_loop():
    reload_config()
    last_refresh_times: dict[str, float] = {}
    while not state._stop:
        reload_config()
        if state.config:
            for view in state.config.views:
                now = time.time()
                last = last_refresh_times.get(view.name, 0)
                if now - last >= view.refresh_interval_seconds:
                    await asyncio.get_event_loop().run_in_executor(None, refresh_view, view)
                    last_refresh_times[view.name] = time.time()
        await asyncio.sleep(1)


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(refresh_loop())
    yield
    state._stop = True
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(title="trino-mv-orchestrator", lifespan=lifespan)


# ── API ──

class ViewCreate(BaseModel):
    name: str
    source_table: str
    query: str
    merge_keys: list[str]
    filter_column: str
    filter_granularity: str = "day"
    target_table: str | None = None
    target_partitioning: str | None = None
    refresh_interval_seconds: int = 60


class ViewResponse(BaseModel):
    name: str
    source_table: str
    query: str
    merge_keys: list[str]
    filter_column: str
    filter_granularity: str
    target_table: str | None = None
    target_partitioning: str | None = None
    refresh_interval_seconds: int = 60
    status: dict | None = None


@app.get("/health")
def health():
    return {"status": "ok", "views": len(state.config.views) if state.config else 0}


@app.get("/metrics")
def metrics():
    return PlainTextResponse(generate_latest().decode(), media_type="text/plain; version=0.0.4")


@app.get("/api/views")
def list_views() -> list[ViewResponse]:
    if not state.config:
        return []
    result = []
    for v in state.config.views:
        vs = state.view_statuses.get(v.name)
        result.append(ViewResponse(
            name=v.name, source_table=v.source_table, query=v.query,
            merge_keys=v.merge_keys, filter_column=v.filter_column,
            filter_granularity=v.filter_granularity,
            target_table=v.target_table, target_partitioning=v.target_partitioning,
            refresh_interval_seconds=v.refresh_interval_seconds,
            status={
                "last_refresh": vs.last_refresh, "last_duration": vs.last_duration,
                "last_action": vs.last_action, "last_range": vs.last_range,
                "last_error": vs.last_error, "total_refreshes": vs.total_refreshes,
                "total_errors": vs.total_errors,
            } if vs else None,
        ))
    return result


@app.post("/api/views", status_code=201)
def create_view(body: ViewCreate) -> ViewResponse:
    if not state.config:
        raise HTTPException(500, "config not loaded")
    if any(v.name == body.name for v in state.config.views):
        raise HTTPException(409, f"view '{body.name}' already exists")

    new_view = ViewConfig(
        name=body.name, source_table=body.source_table, query=body.query,
        merge_keys=body.merge_keys, filter_column=body.filter_column,
        filter_granularity=body.filter_granularity,
        target_table=body.target_table, target_partitioning=body.target_partitioning,
        refresh_interval_seconds=body.refresh_interval_seconds,
    )
    new_views = list(state.config.views) + [new_view]
    new_cfg = Config(trino=state.config.trino, views=new_views, server=state.config.server)
    save_config(new_cfg, state.config_path)
    state.config = new_cfg
    state.config_mtime = state.config_path.stat().st_mtime
    state.view_statuses[body.name] = ViewStatus(name=body.name)
    VIEWS_CONFIGURED.set(len(new_views))

    return ViewResponse(
        name=body.name, source_table=body.source_table, query=body.query,
        merge_keys=body.merge_keys, filter_column=body.filter_column,
        filter_granularity=body.filter_granularity,
        target_table=body.target_table, target_partitioning=body.target_partitioning,
        refresh_interval_seconds=body.refresh_interval_seconds,
    )


@app.delete("/api/views/{name}", status_code=204)
def delete_view(name: str):
    if not state.config:
        raise HTTPException(500, "config not loaded")
    if not any(v.name == name for v in state.config.views):
        raise HTTPException(404, f"view '{name}' not found")
    new_views = [v for v in state.config.views if v.name != name]
    new_cfg = Config(trino=state.config.trino, views=new_views, server=state.config.server)
    save_config(new_cfg, state.config_path)
    state.config = new_cfg
    state.config_mtime = state.config_path.stat().st_mtime
    state.view_statuses.pop(name, None)
    VIEWS_CONFIGURED.set(len(new_views))


@app.post("/api/views/{name}/refresh")
def trigger_refresh(name: str):
    if not state.config:
        raise HTTPException(500, "config not loaded")
    view = next((v for v in state.config.views if v.name == name), None)
    if not view:
        raise HTTPException(404, f"view '{name}' not found")
    refresh_view(view)
    vs = state.view_statuses.get(name)
    return {"status": "ok", "last_action": vs.last_action if vs else None, "last_error": vs.last_error if vs else None}


@app.get("/", response_class=HTMLResponse)
def ui():
    return HTMLResponse((Path(__file__).parent / "static" / "index.html").read_text())
