"""Configuration: the static ``config.yaml``, Trino credentials from the
environment, and view definitions (``views.yaml`` or the API).

``ViewConfig`` is the single definition of a view. The same model validates
``views.yaml``, API requests, and what ``save_views`` writes back.
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from iceberg_ivm.query_parser import QUALIFIED_NAME_RE, parse_view_query

log = logging.getLogger(__name__)

# A chunk must hold whole buckets of the view's granularity. Weeks and months
# don't divide each other, and sub-minute chunks would mean millions of commits.
_GRAN_ORDER = ("millisecond", "second", "minute", "hour", "day", "week", "month", "quarter", "year")
CHUNK_GRANULARITIES = ("minute", "hour", "day", "week", "month", "quarter", "year")
CHUNKS_FOR: dict[str, tuple[str, ...]] = {
    g: tuple(c for c in _GRAN_ORDER[i:] if c in CHUNK_GRANULARITIES and (c != "week" or i <= _GRAN_ORDER.index("week")))
    for i, g in enumerate(_GRAN_ORDER)
}
CHUNKS_FOR["week"] = ("week",)

_DURATION_RE = re.compile(r"^\d+[smhd]$")  # Trino duration
_DATASIZE_RE = re.compile(r"^\d+(B|KB|MB|GB|TB)$")  # Trino data size

PositiveInt = Annotated[int, Field(gt=0, strict=True)]


class ViewConfig(BaseModel):
    """One materialized view. Only ``query`` and ``target_table`` are required."""

    model_config = ConfigDict(frozen=True)

    name: str = ""  # defaults to target_table
    query: str
    target_table: str
    target_partitioning: str | None = None  # e.g. "ARRAY['day(minute)']"
    refresh_interval_seconds: PositiveInt = 60
    query_timeout_seconds: PositiveInt | None = None  # per refresh tick
    full_refresh_chunk: str | None = None  # backfill in chunks of this unit
    # Iceberg maintenance on the target; 0 disables it.
    maintenance_interval_seconds: Annotated[int, Field(ge=0)] = 0
    optimize: bool = True
    optimize_file_size_threshold: str | None = None  # None → Trino's default
    expire_snapshots: bool = True
    expire_snapshots_retention: str = "7d"
    remove_orphan_files: bool = True
    remove_orphan_files_retention: str = "7d"

    @model_validator(mode="before")
    @classmethod
    def _default_name(cls, data):
        if isinstance(data, dict) and not data.get("name"):
            data = {**data, "name": data.get("target_table")}
        return data

    @field_validator("target_partitioning", "full_refresh_chunk", "optimize_file_size_threshold", mode="before")
    @classmethod
    def _blank_is_none(cls, value):
        return value or None

    @field_validator("name", "target_table")
    @classmethod
    def _qualified_name(cls, value: str) -> str:
        if not QUALIFIED_NAME_RE.match(value):
            raise ValueError(f"{value!r} must be a SQL identifier or a dotted name like catalog.schema.table")
        return value

    @field_validator("expire_snapshots_retention", "remove_orphan_files_retention")
    @classmethod
    def _duration(cls, value: str) -> str:
        if not _DURATION_RE.match(value):
            raise ValueError(f"{value!r} is not a Trino duration like '7d' or '24h'")
        return value

    @field_validator("optimize_file_size_threshold")
    @classmethod
    def _data_size(cls, value: str | None) -> str | None:
        if value is not None and not _DATASIZE_RE.match(value):
            raise ValueError(f"{value!r} is not a data size like '128MB'")
        return value

    @model_validator(mode="after")
    def _query_and_chunk(self):
        parsed = parse_view_query(self.query)
        chunk = self.full_refresh_chunk
        if chunk is not None and chunk not in CHUNKS_FOR[parsed.granularity]:
            raise ValueError(
                f"full_refresh_chunk {chunk!r} doesn't hold whole {parsed.granularity} buckets; "
                f"use one of {list(CHUNKS_FOR[parsed.granularity])}"
            )
        return self


@dataclass(frozen=True)
class ServerConfig:
    port: int = 8000
    config_reload_interval_seconds: int = 30
    # Relative paths resolve next to views.yaml (see server.resolve_state_db_path).
    state_db_path: str = "state.db"


@dataclass(frozen=True)
class TrinoConfig:
    url: str  # TRINO_URL
    user: str  # TRINO_USER
    password: str | None  # TRINO_PASSWORD; None → no authentication
    catalog: str
    schema: str
    # Browser-reachable Trino URL for the UI's query links (TRINO_PUBLIC_URL);
    # differs from `url` when the service and the browser see different hosts.
    public_url: str = ""


@dataclass(frozen=True)
class Config:
    trino: TrinoConfig
    views: list[ViewConfig] = field(default_factory=list)
    server: ServerConfig = field(default_factory=ServerConfig)


def load_config(path: str | Path) -> Config:
    """Load ``config.yaml``. Trino credentials come only from the environment
    (``TRINO_URL``, ``TRINO_USER``, optional ``TRINO_PASSWORD``), never YAML."""
    raw = yaml.safe_load(Path(path).read_text()) or {}
    if "trino" not in raw:
        raise ValueError("config missing 'trino' section")
    missing_yaml = {"catalog", "schema"} - raw["trino"].keys()
    if missing_yaml:
        raise ValueError(f"trino config missing required YAML fields: {sorted(missing_yaml)}")
    missing_env = [v for v in ("TRINO_URL", "TRINO_USER") if not os.environ.get(v)]
    if missing_env:
        raise ValueError(f"trino credentials missing from environment: {missing_env}")

    url = os.environ["TRINO_URL"]
    trino = TrinoConfig(
        url=url,
        user=os.environ["TRINO_USER"],
        password=os.environ.get("TRINO_PASSWORD") or None,
        catalog=raw["trino"]["catalog"],
        schema=raw["trino"]["schema"],
        public_url=os.environ.get("TRINO_PUBLIC_URL") or url,
    )
    server_raw = raw.get("server") or {}
    server = ServerConfig(
        **{k: server_raw[k] for k in ("port", "config_reload_interval_seconds", "state_db_path") if k in server_raw}
    )
    log.info("loaded %s (trino=%s as %s, %s.%s)", path, trino.url, trino.user, trino.catalog, trino.schema)
    return Config(trino=trino, views=[], server=server)


def load_views(path: str | Path) -> list[ViewConfig]:
    """Load views from YAML (a list, or a mapping with a ``views`` list).
    A missing or empty file means no views."""
    p = Path(path)
    if not p.exists():
        log.info("views file not found: %s — starting with no views", p)
        return []
    raw = yaml.safe_load(p.read_text())
    if not raw:
        return []
    items = raw.get("views", raw) if isinstance(raw, dict) else raw
    if not isinstance(items, list):
        raise ValueError(f"views file {path} must contain a list or a 'views' key")
    views = [ViewConfig.model_validate(item) for item in items]
    names = [v.name for v in views]
    if len(names) != len(set(names)):
        raise ValueError("duplicate view names detected")
    log.info("loaded %d view(s) from %s", len(views), p)
    return views


def save_views(views: list[ViewConfig], path: str | Path) -> None:
    """Write views to YAML, omitting settings left at their defaults."""
    always = {"name", "query", "target_table", "refresh_interval_seconds"}
    out = [
        {k: val for k, val in v.model_dump().items() if k in always or val != ViewConfig.model_fields[k].default}
        for v in views
    ]
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(yaml.dump({"views": out}, default_flow_style=False, sort_keys=False))
    log.info("saved %d view(s) to %s", len(views), p)
