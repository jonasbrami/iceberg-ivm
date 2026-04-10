"""Configuration loading and validation."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml

VALID_GRANULARITIES = ("minute", "hour", "day", "week", "month")


@dataclass(frozen=True)
class ViewConfig:
    name: str
    source_table: str
    query: str
    merge_keys: list[str]
    filter_column: str
    filter_granularity: str = "day"
    target_table: str | None = None
    target_partitioning: str | None = None
    refresh_interval_seconds: int = 60


@dataclass(frozen=True)
class ServerConfig:
    port: int = 8000
    config_reload_interval_seconds: int = 30


@dataclass(frozen=True)
class TrinoConfig:
    host: str
    port: int
    catalog: str
    schema: str
    user: str


@dataclass(frozen=True)
class Config:
    trino: TrinoConfig
    views: list[ViewConfig] = field(default_factory=list)
    server: ServerConfig = field(default_factory=ServerConfig)


def _parse_view(raw: dict) -> ViewConfig:
    for key in ("name", "source_table", "query", "merge_keys", "filter_column"):
        if key not in raw:
            raise ValueError(f"view missing required field: {key}")

    if "{range_filter}" not in raw["query"]:
        raise ValueError(
            f"view '{raw['name']}': query must contain {{range_filter}} placeholder"
        )

    granularity = raw.get("filter_granularity", "day")
    if granularity not in VALID_GRANULARITIES:
        raise ValueError(
            f"filter_granularity must be one of {VALID_GRANULARITIES}, got: {granularity}"
        )

    return ViewConfig(
        name=raw["name"],
        source_table=raw["source_table"],
        query=raw["query"],
        merge_keys=list(raw["merge_keys"]),
        filter_column=raw["filter_column"],
        filter_granularity=granularity,
        target_table=raw.get("target_table"),
        target_partitioning=raw.get("target_partitioning"),
        refresh_interval_seconds=raw.get("refresh_interval_seconds", 60),
    )


def load_config(path: str | Path) -> Config:
    """Load and validate configuration from a YAML file."""
    raw = yaml.safe_load(Path(path).read_text())
    if "trino" not in raw:
        raise ValueError("config missing 'trino' section")
    if "views" not in raw or not raw["views"]:
        raise ValueError("config missing 'views' section or views list is empty")

    trino_raw = raw["trino"]
    for key in ("host", "port", "catalog", "schema", "user"):
        if key not in trino_raw:
            raise ValueError(f"trino config missing required field: {key}")

    trino = TrinoConfig(
        host=trino_raw["host"],
        port=int(trino_raw["port"]),
        catalog=trino_raw["catalog"],
        schema=trino_raw["schema"],
        user=trino_raw["user"],
    )

    views = [_parse_view(v) for v in raw["views"]]
    names = [v.name for v in views]
    if len(names) != len(set(names)):
        raise ValueError("duplicate view names detected")

    server_raw = raw.get("server", {})
    server = ServerConfig(
        port=server_raw.get("port", 8000),
        config_reload_interval_seconds=server_raw.get("config_reload_interval_seconds", 30),
    )

    return Config(trino=trino, views=views, server=server)


def save_config(cfg: Config, path: str | Path) -> None:
    """Save configuration to a YAML file."""
    data = {
        "server": {
            "port": cfg.server.port,
            "config_reload_interval_seconds": cfg.server.config_reload_interval_seconds,
        },
        "trino": {
            "host": cfg.trino.host,
            "port": cfg.trino.port,
            "catalog": cfg.trino.catalog,
            "schema": cfg.trino.schema,
            "user": cfg.trino.user,
        },
        "views": [],
    }
    for v in cfg.views:
        vd: dict = {
            "name": v.name,
            "source_table": v.source_table,
            "query": v.query,
            "merge_keys": v.merge_keys,
            "filter_column": v.filter_column,
            "filter_granularity": v.filter_granularity,
            "refresh_interval_seconds": v.refresh_interval_seconds,
        }
        if v.target_table:
            vd["target_table"] = v.target_table
        if v.target_partitioning:
            vd["target_partitioning"] = v.target_partitioning
        data["views"].append(vd)

    Path(path).write_text(yaml.dump(data, default_flow_style=False, sort_keys=False))
