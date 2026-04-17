"""Configuration loading and validation."""
from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from trino_mv_orchestrator.query_parser import parse_view_query

log = logging.getLogger(__name__)

# Trino credentials are *only* read from these environment variables.
# No defaults, no YAML overrides — keeping secrets out of the repo and
# making per-environment deployment a matter of setting env vars.
#
# TRINO_PASSWORD is optional: if unset, the orchestrator connects
# without BasicAuth (for clusters that allow anonymous access — e.g.
# the local dev compose stack).
_TRINO_ENV_REQUIRED = ("TRINO_URL", "TRINO_USER")

_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_QUALIFIED_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$")


def _validate_identifier(value: str, field_name: str) -> None:
    if not _IDENTIFIER_RE.match(value):
        raise ValueError(f"{field_name}: {value!r} is not a valid SQL identifier")


def _validate_qualified_name(value: str, field_name: str) -> None:
    if not _QUALIFIED_NAME_RE.match(value):
        raise ValueError(
            f"{field_name}: {value!r} is not a valid qualified table name"
        )


@dataclass(frozen=True)
class ViewConfig:
    name: str
    query: str
    target_table: str | None = None
    target_partitioning: str | None = None
    refresh_interval_seconds: int = 60


@dataclass(frozen=True)
class ServerConfig:
    port: int = 8000
    config_reload_interval_seconds: int = 30


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


def _parse_view(raw: dict) -> ViewConfig:
    missing = {"name", "query"} - raw.keys()
    if missing:
        raise ValueError(f"view missing required fields: {sorted(missing)}")

    _validate_identifier(raw["name"], "name")
    if raw.get("target_table"):
        _validate_qualified_name(raw["target_table"], "target_table")

    # Full query validation — source_table, filter_column, granularity,
    # merge_keys are all derived here at load time.  Raises on any violation.
    parse_view_query(raw["query"])

    return ViewConfig(
        name=raw["name"],
        query=raw["query"],
        target_table=raw.get("target_table"),
        target_partitioning=raw.get("target_partitioning"),
        refresh_interval_seconds=raw.get("refresh_interval_seconds", 60),
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
        data["views"].append(vd)
    p.write_text(yaml.dump(data, default_flow_style=False, sort_keys=False))
    log.info("saved %d view(s) to %s", len(views), p)
