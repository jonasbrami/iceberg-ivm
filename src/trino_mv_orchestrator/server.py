"""FastAPI server: web UI, REST API, Prometheus metrics, refresh loop."""
from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
import dataclasses
from dataclasses import dataclass, field
from pathlib import Path

from urllib.parse import urlparse

import aiotrino
from aiotrino.auth import BasicAuthentication
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, PlainTextResponse
from prometheus_client import Counter, Gauge, Histogram, generate_latest
from pydantic import BaseModel, field_validator

from trino_mv_orchestrator.config import (
    Config,
    ViewConfig,
    _validate_identifier,
    _validate_qualified_name,
    load_config,
    load_views,
    save_views,
)
from trino_mv_orchestrator.detector import RefreshAction, detect_changes
from trino_mv_orchestrator.executor import (
    QueryInfo,
    execute_full_refresh,
    execute_incremental_refresh,
)
from trino_mv_orchestrator.introspect import (
    build_create_table_sql,
    discover_columns,
    discover_source_partitioning,
)
from trino_mv_orchestrator.query_parser import parse_view_query
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
    """Split a qualified table name into a Prometheus label dict."""
    parts = (["", ""] + table.split("."))[-3:]
    return dict(zip(("catalog", "schema", "table"), parts))


# ── Application state ──

RECENT_QUERY_LIMIT = 50


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
    # Ring buffer of the last few refresh queries (MERGE / INSERT / DELETE).
    # In-memory only; cleared on process restart.
    recent_queries: list[QueryInfo] = field(default_factory=list)


@dataclass
class AppState:
    config_path: Path = field(default_factory=lambda: Path("config.yaml"))
    views_path: Path = field(default_factory=lambda: Path("views.yaml"))
    config: Config | None = None
    config_mtime: float = 0
    views_mtime: float = 0
    view_statuses: dict[str, ViewStatus] = field(default_factory=dict)
    _stop: bool = False


# ── Path bootstrap (set by CLI before uvicorn starts) ──

_config_path: Path = Path("config.yaml")
_views_path: Path = Path("views.yaml")


def set_config_path(path: Path) -> None:
    global _config_path
    _config_path = path


def set_views_path(path: Path) -> None:
    global _views_path
    _views_path = path


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
    kwargs = dict(
        host=host, port=port,
        http_scheme=scheme,
        catalog=cfg.trino.catalog, schema=cfg.trino.schema,
        user=cfg.trino.user,
        timezone="UTC",
    )
    if cfg.trino.password:
        kwargs["auth"] = BasicAuthentication(cfg.trino.user, cfg.trino.password)
    return aiotrino.dbapi.connect(**kwargs)


def resolve_target_table(view: ViewConfig, cfg: Config) -> str:
    return view.target_table or f"{cfg.trino.catalog}.{cfg.trino.schema}.{view.name}"


def reload_config(s: AppState) -> bool:
    try:
        config_mtime = s.config_path.stat().st_mtime
    except FileNotFoundError:
        log.warning("config file not found: %s", s.config_path)
        return False
    views_mtime = s.views_path.stat().st_mtime if s.views_path.exists() else 0
    if config_mtime <= s.config_mtime and views_mtime <= s.views_mtime:
        return False
    try:
        new_cfg = load_config(s.config_path)
        new_views = load_views(s.views_path)
        s.config = Config(trino=new_cfg.trino, views=new_views, server=new_cfg.server)
        s.config_mtime = config_mtime
        s.views_mtime = views_mtime
        VIEWS_CONFIGURED.set(len(new_views))
        CONFIG_RELOADS.inc()
        log.info(
            "config reloaded from %s + %s: %d views",
            s.config_path, s.views_path, len(new_views),
        )
        for v in new_views:
            if v.name not in s.view_statuses:
                s.view_statuses[v.name] = ViewStatus(name=v.name)
        return True
    except Exception:
        log.exception("failed to reload config")
        return False


async def refresh_view(s: AppState, view: ViewConfig) -> None:
    conn = get_trino_connection(s)
    cursor = await conn.cursor()
    vs = s.view_statuses.setdefault(view.name, ViewStatus(name=view.name))

    try:
        # Derive source_table, filter_column, granularity, merge_keys
        # from the query AST.  Cheap: sqlparse is O(μs) on a one-screen query.
        parsed = parse_view_query(view.query)
        target_table = resolve_target_table(view, s.config)
        source_labels = _parse_table_labels(parsed.source_table)

        # Auto-discover columns and create target
        columns = await discover_columns(cursor, view.query)
        target_partitioning = (
            view.target_partitioning
            or await discover_source_partitioning(cursor, parsed.source_table)
        )
        create_sql = build_create_table_sql(target_table, columns, target_partitioning)
        await cursor.execute(create_sql)

        value_columns = [c.name for c in columns if c.name not in parsed.merge_keys]

        # Read state
        last_snap = await read_last_snapshot(cursor, target_table)

        # Detect changes via file-level column stats
        detect_start = time.monotonic()
        result = await detect_changes(
            cursor, parsed.source_table,
            parsed.filter_column, parsed.granularity,
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
            # Advance state past empty-append or compaction-only
            # snapshots so we don't re-detect them every cycle. The
            # unchanged-snapshot case (current_snapshot == last_snap)
            # is a true no-op and doesn't need a write.
            if (
                result.current_snapshot is not None
                and result.current_snapshot != last_snap
            ):
                await write_last_snapshot(cursor, target_table, result.current_snapshot)
            return

        if result.action == RefreshAction.FULL_REFRESH:
            refresh_result = await execute_full_refresh(cursor, view, target_table)
            vs.last_action = "full"
            vs.last_range = None
            REFRESH_TOTAL.labels(view=view.name, type="full").inc()
        else:
            refresh_result = await execute_incremental_refresh(
                cursor, view, target_table,
                parsed.filter_column, parsed.merge_keys,
                value_columns, result.filter_range,
            )
            vs.last_action = "incremental"
            vs.last_range = f"[{result.filter_range[0]}, {result.filter_range[1]})"
            REFRESH_TOTAL.labels(view=view.name, type="incremental").inc()

        await write_last_snapshot(cursor, target_table, result.current_snapshot)

        vs.last_refresh = time.time()
        vs.last_duration = refresh_result.elapsed
        vs.last_error = None
        vs.total_refreshes += 1
        # Ring-buffer the refresh queries (MERGE / INSERT / DELETE) for the UI.
        # Newest first, capped to RECENT_QUERY_LIMIT.
        vs.recent_queries = (refresh_result.queries + vs.recent_queries)[:RECENT_QUERY_LIMIT]
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
        await conn.close()


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
                    await refresh_view(s, view)
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
        s = AppState(config_path=_config_path, views_path=_views_path)
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
    query: str
    target_table: str | None = None
    target_partitioning: str | None = None
    refresh_interval_seconds: int = 60

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        _validate_identifier(v, "name")
        return v

    @field_validator("target_table")
    @classmethod
    def validate_target_table(cls, v: str | None) -> str | None:
        if v is not None:
            _validate_qualified_name(v, "target_table")
        return v


class ViewResponse(BaseModel):
    """View as returned by the API.

    ``source_table``, ``filter_column``, and ``merge_keys`` are *derived* from
    the query AST — they appear in the response so the UI can render a source →
    target card, but they are not accepted on ``POST``.
    """
    name: str
    query: str
    source_table: str
    filter_column: str
    merge_keys: tuple[str, ...]
    target_table: str | None = None
    target_partitioning: str | None = None
    refresh_interval_seconds: int = 60
    status: dict | None = None


def _view_to_response(v: ViewConfig, vs: ViewStatus | None) -> ViewResponse:
    parsed = parse_view_query(v.query)
    return ViewResponse(
        name=v.name, query=v.query,
        source_table=parsed.source_table,
        filter_column=parsed.filter_column,
        merge_keys=parsed.merge_keys,
        target_table=v.target_table, target_partitioning=v.target_partitioning,
        refresh_interval_seconds=v.refresh_interval_seconds,
        status=dataclasses.asdict(vs) if vs else None,
    )


# ── Form schema (drives the UI dynamically) ──

VIEW_FORM_SCHEMA: list[dict] = [
    {"name": "name", "label": "Name", "type": "string", "required": True,
     "placeholder": "ohlcv_1m", "disabled_on_edit": True},
    {"name": "query", "label": "Query", "type": "text", "required": True,
     "placeholder": (
         "SELECT symbol,\n"
         "       date_trunc('minute', ts) AS minute,\n"
         "       min_by(price, ts) AS open,\n"
         "       max(price)        AS high,\n"
         "       min(price)        AS low,\n"
         "       max_by(price, ts) AS close\n"
         "FROM iceberg.market_data.trades\n"
         "GROUP BY symbol, date_trunc('minute', ts)"
     ),
     "help": (
         "exactly what you would write after CREATE MATERIALIZED VIEW … AS. "
         "source table, filter column, granularity and merge keys are "
         "derived automatically from the query."
     ),
     "rows": 10},
    {"name": "target_table", "label": "Target Table", "type": "string", "required": False,
     "placeholder": "auto-generated", "group": "target"},
    {"name": "target_partitioning", "label": "Partitioning", "type": "string", "required": False,
     "placeholder": "inherits from source", "group": "target"},
    {"name": "refresh_interval_seconds", "label": "Refresh Interval", "type": "number",
     "required": False, "default": 60, "min": 1, "suffix": "seconds"},
]


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
        generate_latest().decode(), media_type="text/plain; version=0.0.4",
    )


@app.get("/api/views")
def list_views(s: AppState = Depends(get_app_state)) -> list[ViewResponse]:
    if not s.config:
        return []
    return [_view_to_response(v, s.view_statuses.get(v.name)) for v in s.config.views]


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
        raise HTTPException(422, str(exc))
    return ParseResponse(
        source_table=p.source_table,
        filter_column=p.filter_column,
        granularity=p.granularity,
        merge_keys=p.merge_keys,
    )


@app.post("/api/views", status_code=201)
def create_view(
    body: ViewCreate, s: AppState = Depends(get_app_state),
) -> ViewResponse:
    if not s.config:
        raise HTTPException(500, "config not loaded")
    if any(v.name == body.name for v in s.config.views):
        raise HTTPException(409, f"view '{body.name}' already exists")

    # Validate the query — raises on any violation.  Rejected queries never
    # make it into saved state.
    try:
        parse_view_query(body.query)
    except ValueError as exc:
        raise HTTPException(422, str(exc))

    new_view = ViewConfig(**body.model_dump())
    new_views = list(s.config.views) + [new_view]
    new_cfg = Config(trino=s.config.trino, views=new_views, server=s.config.server)
    save_views(new_views, s.views_path)
    s.config = new_cfg
    s.views_mtime = s.views_path.stat().st_mtime
    s.view_statuses[body.name] = ViewStatus(name=body.name)
    VIEWS_CONFIGURED.set(len(new_views))
    log.info("created view %r via API", body.name)

    return _view_to_response(new_view, s.view_statuses[body.name])


@app.delete("/api/views/{name}", status_code=204)
def delete_view(name: str, s: AppState = Depends(get_app_state)):
    if not s.config:
        raise HTTPException(500, "config not loaded")
    if not any(v.name == name for v in s.config.views):
        raise HTTPException(404, f"view '{name}' not found")
    new_views = [v for v in s.config.views if v.name != name]
    new_cfg = Config(trino=s.config.trino, views=new_views, server=s.config.server)
    save_views(new_views, s.views_path)
    s.config = new_cfg
    s.views_mtime = s.views_path.stat().st_mtime
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
    await refresh_view(s, view)
    vs = s.view_statuses.get(name)
    return {
        "status": "ok",
        "last_action": vs.last_action if vs else None,
        "last_error": vs.last_error if vs else None,
    }


@app.get("/", response_class=HTMLResponse)
def ui():
    return HTMLResponse((Path(__file__).parent / "static" / "index.html").read_text())
