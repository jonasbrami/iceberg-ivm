"""Configuration loading and validation."""
from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from trino_mv_orchestrator.query_parser import (
    IDENTIFIER_RE,
    QUALIFIED_NAME_RE,
    VALID_GRANULARITIES,
    parse_view_query,
)

log = logging.getLogger(__name__)

# full_refresh_chunk must be coarser-or-equal to the view's own granularity
# *and* cleanly contain its buckets. Weeks do not divide months, and months
# do not divide weeks, so the relation is a partial order, not a total one.
_CHUNK_COMPATIBILITY: dict[str, frozenset[str]] = {
    "minute":  frozenset({"minute", "hour", "day", "week", "month", "quarter", "year"}),
    "hour":    frozenset({"hour", "day", "week", "month", "quarter", "year"}),
    "day":     frozenset({"day", "week", "month", "quarter", "year"}),
    "week":    frozenset({"week"}),
    "month":   frozenset({"month", "quarter", "year"}),
    "quarter": frozenset({"quarter", "year"}),
    "year":    frozenset({"year"}),
}

# Trino credentials are *only* read from these environment variables.
# No defaults, no YAML overrides — keeping secrets out of the repo and
# making per-environment deployment a matter of setting env vars.
#
# TRINO_PASSWORD is optional: if unset, the orchestrator connects
# without BasicAuth (for clusters that allow anonymous access — e.g.
# the local dev compose stack).
_TRINO_ENV_REQUIRED = ("TRINO_URL", "TRINO_USER")

def validate_identifier(value: str, field_name: str) -> None:
    if not IDENTIFIER_RE.match(value):
        raise ValueError(f"{field_name}: {value!r} is not a valid SQL identifier")


def validate_qualified_name(value: str, field_name: str) -> None:
    if not QUALIFIED_NAME_RE.match(value):
        raise ValueError(
            f"{field_name}: {value!r} is not a valid qualified table name"
        )


# Iceberg maintenance operations we run via ``ALTER TABLE ... EXECUTE <op>``.
# Order matters — ``server.maintain_view`` iterates this list so ops run in the
# same order on every pass.
MAINTENANCE_OPS: tuple[str, ...] = (
    "optimize",
    "expire_snapshots",
    "remove_orphan_files",
)

# Trino duration grammar (``'7d'``, ``'30m'``, …) — used by Iceberg retention
# params. Accepts s/m/h/d; weeks are ``Nd`` (Trino does not take 'w').
_DURATION_RE = re.compile(r"^\d+[smhd]$")

# Iceberg ``file_size_threshold`` must be a DataSize literal.
_DATASIZE_RE = re.compile(r"^\d+(B|KB|MB|GB|TB)$")


@dataclass(frozen=True)
class ViewConfig:
    name: str
    query: str
    target_table: str | None = None
    target_partitioning: str | None = None
    refresh_interval_seconds: int = 60
    # Granularity string ("day", "month", …) controlling the size of each
    # chunk in the first-run chunked backfill. ``None`` → legacy single-shot
    # full refresh. Validated against the view's own date_trunc granularity
    # at load time.
    full_refresh_chunk: str | None = None

    # Iceberg maintenance. Each ``_interval_seconds`` is the minimum gap
    # between runs (0 = disabled). Retention/threshold values are strings
    # passed straight through to Trino's named-arg syntax; format-validated
    # at load time so bad values never reach the executor.
    optimize_interval_seconds: int = 0
    optimize_file_size_threshold: str | None = None     # None → Trino default (100MB)
    expire_snapshots_interval_seconds: int = 0
    expire_snapshots_retention: str = "7d"
    remove_orphan_files_interval_seconds: int = 0
    remove_orphan_files_retention: str = "7d"


@dataclass(frozen=True)
class ServerConfig:
    port: int = 8000
    config_reload_interval_seconds: int = 30
    # SQLite file that persists the UI's "recent queries" ring buffer
    # across restarts. Resolved relative to the config file's directory
    # when non-absolute, so the default colocates the DB with config.yaml
    # without surprising deployments that mount the config dir as a volume.
    state_db_path: str = "state.db"


@dataclass(frozen=True)
class TrinoConfig:
    url: str                # full coordinator URL, e.g. "http://trino:8080" (from TRINO_URL)
    user: str               # from TRINO_USER
    password: str | None    # from TRINO_PASSWORD; None → connect anonymously
    catalog: str            # from YAML (trino.catalog)
    schema: str             # from YAML (trino.schema)


@dataclass(frozen=True)
class Config:
    trino: TrinoConfig
    views: list[ViewConfig] = field(default_factory=list)
    server: ServerConfig = field(default_factory=ServerConfig)


def validate_chunk_compatibility(chunk: str | None, query: str) -> None:
    """Validate a ``full_refresh_chunk`` against a view's query.

    No-op when ``chunk`` is ``None`` or empty string (the "single-shot"
    marker used by the HTTP layer). Raises ``ValueError`` on any of:

    - unknown granularity
    - granularity that doesn't cleanly contain the view's own buckets
    - view query missing a direct ``date_trunc(...) AS <alias>`` projection
    """
    if not chunk:
        return
    parsed = parse_view_query(query)
    if chunk not in VALID_GRANULARITIES:
        raise ValueError(
            f"full_refresh_chunk: {chunk!r} is not a valid granularity; "
            f"expected one of {sorted(VALID_GRANULARITIES)}"
        )
    allowed = _CHUNK_COMPATIBILITY[parsed.granularity]
    if chunk not in allowed:
        raise ValueError(
            f"full_refresh_chunk: {chunk!r} is not compatible with the "
            f"view's date_trunc granularity {parsed.granularity!r}; "
            f"allowed values: {sorted(allowed)}"
        )
    if parsed.bucket_alias is None:
        raise ValueError(
            "full_refresh_chunk requires date_trunc("
            f"{parsed.granularity!r}, {parsed.filter_column}) to appear "
            "as a direct projection with an alias (the target needs a "
            "column to read as the resume point)"
        )


def validate_maintenance_config(raw: dict | ViewConfig) -> None:
    """Validate maintenance intervals + param strings.

    Accepts either a raw YAML/API dict or a constructed ``ViewConfig`` — same
    field names in both, so attribute access via ``getattr`` covers it.
    """
    g = (raw.get if isinstance(raw, dict) else lambda k, d=None: getattr(raw, k, d))
    for op in MAINTENANCE_OPS:
        iv = g(f"{op}_interval_seconds", 0) or 0
        if iv < 0:
            raise ValueError(
                f"{op}_interval_seconds must be >= 0 (got {iv!r}); "
                f"use 0 to disable the op"
            )
    for field_name in ("expire_snapshots_retention", "remove_orphan_files_retention"):
        v = g(field_name, None)
        if v and not _DURATION_RE.match(str(v)):
            raise ValueError(
                f"{field_name}: {v!r} is not a valid Trino duration; "
                f"expected e.g. '7d', '24h', '60m', '30s'"
            )
    thr = g("optimize_file_size_threshold", None)
    if thr and not _DATASIZE_RE.match(str(thr)):
        raise ValueError(
            f"optimize_file_size_threshold: {thr!r} is not a valid data size; "
            f"expected e.g. '128MB', '1GB'"
        )


def _parse_view(raw: dict) -> ViewConfig:
    missing = {"name", "query"} - raw.keys()
    if missing:
        raise ValueError(f"view missing required fields: {sorted(missing)}")

    validate_identifier(raw["name"], "name")
    if raw.get("target_table"):
        validate_qualified_name(raw["target_table"], "target_table")

    # Full query validation — source_table, filter_column, granularity,
    # merge_keys are all derived here at load time.  Raises on any violation.
    parse_view_query(raw["query"])

    chunk = raw.get("full_refresh_chunk")
    validate_chunk_compatibility(chunk, raw["query"])
    validate_maintenance_config(raw)

    return ViewConfig(
        name=raw["name"],
        query=raw["query"],
        target_table=raw.get("target_table"),
        target_partitioning=raw.get("target_partitioning"),
        refresh_interval_seconds=raw.get("refresh_interval_seconds", 60),
        full_refresh_chunk=chunk,
        optimize_interval_seconds=raw.get("optimize_interval_seconds", 0),
        optimize_file_size_threshold=raw.get("optimize_file_size_threshold"),
        expire_snapshots_interval_seconds=raw.get("expire_snapshots_interval_seconds", 0),
        expire_snapshots_retention=raw.get("expire_snapshots_retention", "7d"),
        remove_orphan_files_interval_seconds=raw.get("remove_orphan_files_interval_seconds", 0),
        remove_orphan_files_retention=raw.get("remove_orphan_files_retention", "7d"),
    )


def load_config(path: str | Path) -> Config:
    """Load static configuration (trino + server) from a YAML file.

    Trino credentials (URL, user, password) are read *only* from the
    ``TRINO_URL`` / ``TRINO_USER`` / ``TRINO_PASSWORD`` environment
    variables — never from YAML — so secrets stay out of the repo and
    deployments can inject per-environment values. Any host/port/user in
    the YAML's ``trino:`` section is ignored.

    Views are managed separately via ``load_views`` / ``save_views``.
    """
    raw = yaml.safe_load(Path(path).read_text())
    if "trino" not in raw:
        raise ValueError("config missing 'trino' section")

    trino_raw = raw["trino"]
    missing_yaml = {"catalog", "schema"} - trino_raw.keys()
    if missing_yaml:
        raise ValueError(
            f"trino config missing required YAML fields: {sorted(missing_yaml)}"
        )

    missing_env = [v for v in _TRINO_ENV_REQUIRED if not os.environ.get(v)]
    if missing_env:
        raise ValueError(
            f"trino credentials missing from environment: {missing_env}. "
            f"Set {', '.join(_TRINO_ENV_REQUIRED)} before starting the orchestrator."
        )

    trino = TrinoConfig(
        url=os.environ["TRINO_URL"],
        user=os.environ["TRINO_USER"],
        password=os.environ.get("TRINO_PASSWORD") or None,
        catalog=trino_raw["catalog"],
        schema=trino_raw["schema"],
    )

    server_raw = raw.get("server", {})
    server = ServerConfig(
        port=server_raw.get("port", 8000),
        config_reload_interval_seconds=server_raw.get("config_reload_interval_seconds", 30),
        state_db_path=server_raw.get("state_db_path", "state.db"),
    )

    cfg = Config(trino=trino, views=[], server=server)
    log.info(
        "loaded static config from %s (trino=%s as %s / %s.%s)",
        path, trino.url, trino.user, trino.catalog, trino.schema,
    )
    return cfg


def load_views(path: str | Path) -> list[ViewConfig]:
    """Load views from a separate YAML file.

    Returns an empty list if the file does not exist (e.g. fresh Docker volume).
    """
    p = Path(path)
    if not p.exists():
        log.info("views file not found: %s — starting with no views", p)
        return []
    raw = yaml.safe_load(p.read_text())
    if not raw:
        return []
    views_raw = raw.get("views", raw) if isinstance(raw, dict) else raw
    if not isinstance(views_raw, list):
        raise ValueError(f"views file {path} must contain a list or a 'views' key")
    views = [_parse_view(v) for v in views_raw]
    names = [v.name for v in views]
    if len(names) != len(set(names)):
        raise ValueError("duplicate view names detected")
    log.info("loaded %d view(s) from %s", len(views), p)
    return views


def save_views(views: list[ViewConfig], path: str | Path) -> None:
    """Save views list to a YAML file."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    data: dict = {"views": []}
    for v in views:
        vd: dict = {
            "name": v.name,
            "query": v.query,
            "refresh_interval_seconds": v.refresh_interval_seconds,
        }
        if v.target_table:
            vd["target_table"] = v.target_table
        if v.target_partitioning:
            vd["target_partitioning"] = v.target_partitioning
        if v.full_refresh_chunk:
            vd["full_refresh_chunk"] = v.full_refresh_chunk
        # Only emit maintenance fields that differ from the defaults, so the
        # common "no maintenance configured" view stays a one-liner in YAML.
        if v.optimize_interval_seconds:
            vd["optimize_interval_seconds"] = v.optimize_interval_seconds
        if v.optimize_file_size_threshold:
            vd["optimize_file_size_threshold"] = v.optimize_file_size_threshold
        if v.expire_snapshots_interval_seconds:
            vd["expire_snapshots_interval_seconds"] = v.expire_snapshots_interval_seconds
            if v.expire_snapshots_retention != "7d":
                vd["expire_snapshots_retention"] = v.expire_snapshots_retention
        if v.remove_orphan_files_interval_seconds:
            vd["remove_orphan_files_interval_seconds"] = v.remove_orphan_files_interval_seconds
            if v.remove_orphan_files_retention != "7d":
                vd["remove_orphan_files_retention"] = v.remove_orphan_files_retention
        data["views"].append(vd)
    p.write_text(yaml.dump(data, default_flow_style=False, sort_keys=False))
    log.info("saved %d view(s) to %s", len(views), p)
