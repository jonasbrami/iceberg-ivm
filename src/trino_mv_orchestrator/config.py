"""Configuration loading and validation."""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

log = logging.getLogger(__name__)

VALID_GRANULARITIES = ("minute", "hour", "day", "week", "month", "quarter", "year")

_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_QUALIFIED_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$")

# Regex to extract granularity from date_trunc('X', ...)
_DATE_TRUNC_RE = re.compile(r"date_trunc\s*\(\s*'(\w+)'\s*,", re.IGNORECASE)

# Detects date_trunc used inside arithmetic (e.g. date_trunc(...) - INTERVAL ...)
_COMPLEX_EXPR_RE = re.compile(
    r"date_trunc\s*\(\s*'\w+'\s*,[^)]*\)\s*[-+*/%]"
    r"|[-+*/%]\s*date_trunc\s*\(\s*'\w+'\s*,",
    re.IGNORECASE,
)


def infer_granularity(query: str) -> str:
    """Infer filter_granularity from ``date_trunc('X', ...)`` in the query.

    Returns the granularity string if exactly one valid granularity is found
    in a simple ``date_trunc`` call (not part of arithmetic).  Raises
    ``ValueError`` when inference is not possible.
    """
    if _COMPLEX_EXPR_RE.search(query):
        raise ValueError(
            "complex date_trunc expressions are not supported; "
            "use date_trunc('X', col) directly in GROUP BY"
        )

    matches = _DATE_TRUNC_RE.findall(query)
    if not matches:
        raise ValueError(
            "query must contain a date_trunc('X', col) expression "
            "for automatic granularity inference"
        )

    granularities = {m.lower() for m in matches}
    valid = granularities & set(VALID_GRANULARITIES)
    if len(valid) != 1:
        raise ValueError(
            f"could not infer a single granularity from query; "
            f"found: {granularities}"
        )

    return valid.pop()


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
    source_table: str
    query: str
    merge_keys: tuple[str, ...]
    filter_column: str
    filter_granularity: str = ""  # always set by infer_granularity()
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

    _validate_identifier(raw["name"], "name")
    _validate_qualified_name(raw["source_table"], "source_table")
    _validate_identifier(raw["filter_column"], "filter_column")
    for key in raw["merge_keys"]:
        _validate_identifier(str(key), "merge_keys")
    if raw.get("target_table"):
        _validate_qualified_name(raw["target_table"], "target_table")

    if "{range_filter}" not in raw["query"]:
        raise ValueError(
            f"view '{raw['name']}': query must contain {{range_filter}} placeholder"
        )

    granularity = infer_granularity(raw["query"])

    return ViewConfig(
        name=raw["name"],
        source_table=raw["source_table"],
        query=raw["query"],
        merge_keys=tuple(raw["merge_keys"]),
        filter_column=raw["filter_column"],
        filter_granularity=granularity,
        target_table=raw.get("target_table"),
        target_partitioning=raw.get("target_partitioning"),
        refresh_interval_seconds=raw.get("refresh_interval_seconds", 60),
    )


def load_config(path: str | Path) -> Config:
    """Load static configuration (trino + server) from a YAML file.

    Views are managed separately via load_views / save_views.
    """
    raw = yaml.safe_load(Path(path).read_text())
    if "trino" not in raw:
        raise ValueError("config missing 'trino' section")

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

    server_raw = raw.get("server", {})
    server = ServerConfig(
        port=server_raw.get("port", 8000),
        config_reload_interval_seconds=server_raw.get("config_reload_interval_seconds", 30),
    )

    cfg = Config(trino=trino, views=[], server=server)
    log.info(
        "loaded static config from %s (trino=%s:%d/%s)",
        path, trino.host, trino.port, trino.catalog,
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
            "source_table": v.source_table,
            "query": v.query,
            "merge_keys": list(v.merge_keys),
            "filter_column": v.filter_column,
            "refresh_interval_seconds": v.refresh_interval_seconds,
        }
        if v.target_table:
            vd["target_table"] = v.target_table
        if v.target_partitioning:
            vd["target_partitioning"] = v.target_partitioning
        data["views"].append(vd)
    p.write_text(yaml.dump(data, default_flow_style=False, sort_keys=False))
    log.info("saved %d view(s) to %s", len(views), p)
