"""FastAPI server: web UI, REST API, Prometheus metrics, refresh loop."""
from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path

import trino
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, PlainTextResponse
from prometheus_client import Counter, Gauge, Histogram, generate_latest
from pydantic import BaseModel, field_validator

from trino_mv_orchestrator.config import (
    VALID_GRANULARITIES,
    Config,
    ViewConfig,
    _validate_identifier,
    _validate_qualified_name,
    infer_granularity,
    load_config,
    save_config,
)
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

# Enhanced metrics
REFRESH_BYTES = Counter(
    "mv_refresh_bytes_processed_total",
    "Bytes processed during refresh queries",
    ["view", "catalog", "schema", "table"],
)
REFRESH_ROWS = Counter(
    "mv_refresh_rows_processed_total",
    "Rows processed during refresh queries",
    ["view", "catalog", "schema", "table"],
)
DETECTION_DURATION = Histogram(
    "mv_detection_duration_seconds",
    "Change detection duration",
    ["view"],
)
SOURCE_SNAPSHOT = Gauge(
    "mv_source_snapshot_id",
    "Current source snapshot ID",
    ["view"],
)


def _parse_table_labels(table: str) -> dict[str, str]:
    """Split a qualified table name into Prometheus label dict."""
    parts = table.split(".")
    if len(parts) == 3:
        return {"catalog": parts[0], "schema": parts[1], "table": parts[2]}
    if len(parts) == 2:
        return {"catalog": "", "schema": parts[0], "table": parts[1]}
    return {"catalog": "", "schema": "", "table": table}


# ── Application state ──

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


# ── Config path bootstrap (set by CLI before uvicorn starts) ──

_config_path: Path = Path("config.yaml")


def set_config_path(path: Path) -> None:
    global _config_path
    _config_path = path


# ── Dependency injection ──

def get_app_state(request: Request) -> AppState:
    return request.app.state.s


# ── Core logic ──

def get_trino_connection(s: AppState) -> trino.dbapi.Connection:
    cfg = s.config
    log.debug("connecting to Trino at %s:%s", cfg.trino.host, cfg.trino.port)
    return trino.dbapi.connect(
        host=cfg.trino.host, port=cfg.trino.port,
        catalog=cfg.trino.catalog, schema=cfg.trino.schema,
        user=cfg.trino.user,
    )


def resolve_target_table(view: ViewConfig, cfg: Config) -> str:
    return view.target_table or f"{cfg.trino.catalog}.{cfg.trino.schema}.{view.name}"


def reload_config(s: AppState) -> bool:
    try:
        mtime = s.config_path.stat().st_mtime
    except FileNotFoundError:
        log.warning("config file not found: %s", s.config_path)
        return False
    if mtime <= s.config_mtime:
        return False
    try:
        new_cfg = load_config(s.config_path)
        s.config = new_cfg
        s.config_mtime = mtime
        VIEWS_CONFIGURED.set(len(new_cfg.views))
        CONFIG_RELOADS.inc()
        log.info("config reloaded from %s: %d views", s.config_path, len(new_cfg.views))
        for v in new_cfg.views:
            if v.name not in s.view_statuses:
                s.view_statuses[v.name] = ViewStatus(name=v.name)
        return True
    except Exception:
        log.exception("failed to reload config from %s", s.config_path)
        return False


def refresh_view(s: AppState, view: ViewConfig) -> None:
    conn = get_trino_connection(s)
    cursor = conn.cursor()
    vs = s.view_statuses.setdefault(view.name, ViewStatus(name=view.name))

    try:
        target_table = resolve_target_table(view, s.config)
        source_labels = _parse_table_labels(view.source_table)

        # Auto-discover columns and create target
        columns = discover_columns(cursor, view.query)
        target_partitioning = view.target_partitioning or discover_source_partitioning(cursor, view.source_table)
        create_sql = build_create_table_sql(target_table, columns, target_partitioning)
        cursor.execute(create_sql)

        value_columns = [c.name for c in columns if c.name not in view.merge_keys]

        # Read state
        last_snap = read_last_snapshot(cursor, target_table)

        # Detect changes via file-level column stats
        detect_start = time.monotonic()
        result = detect_changes(
            cursor, view.source_table,
            view.filter_column, view.filter_granularity,
            last_snap,
        )
        detect_elapsed = time.monotonic() - detect_start
        DETECTION_DURATION.labels(view=view.name).observe(detect_elapsed)
        log.info(
            "%s: change detection took %.3fs → %s",
            view.name, detect_elapsed, result.action.name,
        )

        if result.current_snapshot is not None:
            SOURCE_SNAPSHOT.labels(view=view.name).set(result.current_snapshot)

        if result.action == RefreshAction.NO_CHANGE:
            vs.last_action = "skip"
            REFRESH_TOTAL.labels(view=view.name, type="skip").inc()
            return

        if result.action == RefreshAction.FULL_REFRESH:
            refresh_result = execute_full_refresh(cursor, view, target_table)
            vs.last_action = "full"
            vs.last_range = None
            REFRESH_TOTAL.labels(view=view.name, type="full").inc()
        else:
            refresh_result = execute_incremental_refresh(
                cursor, view, target_table, value_columns, result.filter_range,
            )
            vs.last_action = "incremental"
            vs.last_range = f"[{result.filter_range[0]}, {result.filter_range[1]})"
            REFRESH_TOTAL.labels(view=view.name, type="incremental").inc()

        write_last_snapshot(cursor, target_table, result.current_snapshot)

        vs.last_refresh = time.time()
        vs.last_duration = refresh_result.elapsed
        vs.last_error = None
        vs.total_refreshes += 1
        REFRESH_DURATION.labels(view=view.name).observe(refresh_result.elapsed)
        REFRESH_LAST_SUCCESS.labels(view=view.name).set(vs.last_refresh)

        # Record bytes/rows with source table labels
        lbl = {"view": view.name, **source_labels}
        REFRESH_BYTES.labels(**lbl).inc(refresh_result.processed_bytes)
        REFRESH_ROWS.labels(**lbl).inc(refresh_result.processed_rows)

    except Exception as e:
        vs.last_error = str(e)
        vs.total_errors += 1
        REFRESH_ERRORS.labels(view=view.name).inc()
        log.exception("%s: refresh failed", view.name)
    finally:
        conn.close()


async def refresh_loop(s: AppState) -> None:
    reload_config(s)
    last_refresh_times: dict[str, float] = {}
    last_config_reload: float = time.time()

    while not s._stop:
        now = time.time()

        # Reload config at configured interval
        reload_interval = s.config.server.config_reload_interval_seconds if s.config else 30
        if now - last_config_reload >= reload_interval:
            reload_config(s)
            last_config_reload = now

        if s.config:
            for view in s.config.views:
                last = last_refresh_times.get(view.name, 0)
                if now - last >= view.refresh_interval_seconds:
                    log.debug("%s: scheduling refresh (%.0fs since last)", view.name, now - last)
                    await asyncio.get_event_loop().run_in_executor(
                        None, refresh_view, s, view,
                    )
                    last_refresh_times[view.name] = time.time()

        await asyncio.sleep(1)


# ── FastAPI lifespan ──

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Allow tests to pre-seed state (with _stop=True to skip loop)
    if hasattr(app.state, "s"):
        s = app.state.s
        log.info("using pre-seeded app state")
    else:
        s = AppState(config_path=_config_path)
        reload_config(s)
        app.state.s = s

    log.info(
        "starting refresh loop — %d views configured",
        len(s.config.views) if s.config else 0,
    )
    task = asyncio.create_task(refresh_loop(s))
    yield
    log.info("shutting down refresh loop")
    s._stop = True
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(title="trino-mv-orchestrator", lifespan=lifespan)


# ── API models ──

class ViewCreate(BaseModel):
    name: str
    source_table: str
    query: str
    merge_keys: tuple[str, ...]
    filter_column: str
    filter_granularity: str | None = None
    target_table: str | None = None
    target_partitioning: str | None = None
    refresh_interval_seconds: int = 60

    @field_validator("name", "filter_column")
    @classmethod
    def validate_identifier(cls, v: str) -> str:
        _validate_identifier(v, "field")
        return v

    @field_validator("source_table")
    @classmethod
    def validate_source_table(cls, v: str) -> str:
        _validate_qualified_name(v, "source_table")
        return v

    @field_validator("merge_keys")
    @classmethod
    def validate_merge_keys(cls, v: tuple[str, ...]) -> tuple[str, ...]:
        for key in v:
            _validate_identifier(key, "merge_keys")
        return v

    @field_validator("target_table")
    @classmethod
    def validate_target_table(cls, v: str | None) -> str | None:
        if v is not None:
            _validate_qualified_name(v, "target_table")
        return v


class ViewResponse(BaseModel):
    name: str
    source_table: str
    query: str
    merge_keys: tuple[str, ...]
    filter_column: str
    filter_granularity: str
    target_table: str | None = None
    target_partitioning: str | None = None
    refresh_interval_seconds: int = 60
    status: dict | None = None


# ── Endpoints ──

@app.get("/health")
def health(s: AppState = Depends(get_app_state)):
    return {"status": "ok", "views": len(s.config.views) if s.config else 0}


@app.get("/metrics")
def metrics():
    return PlainTextResponse(
        generate_latest().decode(), media_type="text/plain; version=0.0.4",
    )


@app.get("/api/views")
def list_views(s: AppState = Depends(get_app_state)) -> list[ViewResponse]:
    if not s.config:
        return []
    result = []
    for v in s.config.views:
        vs = s.view_statuses.get(v.name)
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
def create_view(
    body: ViewCreate, s: AppState = Depends(get_app_state),
) -> ViewResponse:
    if not s.config:
        raise HTTPException(500, "config not loaded")
    if any(v.name == body.name for v in s.config.views):
        raise HTTPException(409, f"view '{body.name}' already exists")

    resolved_granularity = body.filter_granularity
    if resolved_granularity is None:
        resolved_granularity = infer_granularity(body.query)
        if resolved_granularity is None:
            raise HTTPException(
                422,
                f"filter_granularity not specified and could not be inferred from "
                f"query. Set it explicitly to one of: {', '.join(VALID_GRANULARITIES)}",
            )
    elif resolved_granularity not in VALID_GRANULARITIES:
        raise HTTPException(
            422, f"filter_granularity must be one of {VALID_GRANULARITIES}",
        )

    new_view = ViewConfig(
        name=body.name, source_table=body.source_table, query=body.query,
        merge_keys=body.merge_keys, filter_column=body.filter_column,
        filter_granularity=resolved_granularity,
        target_table=body.target_table, target_partitioning=body.target_partitioning,
        refresh_interval_seconds=body.refresh_interval_seconds,
    )
    new_views = list(s.config.views) + [new_view]
    new_cfg = Config(trino=s.config.trino, views=new_views, server=s.config.server)
    save_config(new_cfg, s.config_path)
    s.config = new_cfg
    s.config_mtime = s.config_path.stat().st_mtime
    s.view_statuses[body.name] = ViewStatus(name=body.name)
    VIEWS_CONFIGURED.set(len(new_views))
    log.info("created view %r via API", body.name)

    return ViewResponse(
        name=body.name, source_table=body.source_table, query=body.query,
        merge_keys=body.merge_keys, filter_column=body.filter_column,
        filter_granularity=resolved_granularity,
        target_table=body.target_table, target_partitioning=body.target_partitioning,
        refresh_interval_seconds=body.refresh_interval_seconds,
    )


@app.delete("/api/views/{name}", status_code=204)
def delete_view(name: str, s: AppState = Depends(get_app_state)):
    if not s.config:
        raise HTTPException(500, "config not loaded")
    if not any(v.name == name for v in s.config.views):
        raise HTTPException(404, f"view '{name}' not found")
    new_views = [v for v in s.config.views if v.name != name]
    new_cfg = Config(trino=s.config.trino, views=new_views, server=s.config.server)
    save_config(new_cfg, s.config_path)
    s.config = new_cfg
    s.config_mtime = s.config_path.stat().st_mtime
    s.view_statuses.pop(name, None)
    VIEWS_CONFIGURED.set(len(new_views))
    log.info("deleted view %r via API", name)


@app.post("/api/views/{name}/refresh")
async def trigger_refresh(name: str, s: AppState = Depends(get_app_state)):
    if not s.config:
        raise HTTPException(500, "config not loaded")
    view = next((v for v in s.config.views if v.name == name), None)
    if not view:
        raise HTTPException(404, f"view '{name}' not found")
    log.info("manual refresh triggered for %r", name)
    await asyncio.get_event_loop().run_in_executor(None, refresh_view, s, view)
    vs = s.view_statuses.get(name)
    return {
        "status": "ok",
        "last_action": vs.last_action if vs else None,
        "last_error": vs.last_error if vs else None,
    }


@app.get("/", response_class=HTMLResponse)
def ui():
    return HTMLResponse((Path(__file__).parent / "static" / "index.html").read_text())
