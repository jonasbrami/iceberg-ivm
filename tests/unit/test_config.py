"""Tests for config loading and validation.

Query-parsing tests live in test_query_parser.py; these cover YAML loading,
identifier validation, and save/load round-tripping.
"""
import textwrap
from pathlib import Path

import pytest

from trino_mv_orchestrator.config import ViewConfig, load_config, load_views, save_views


def write_config(tmp_path: Path, content: str) -> Path:
    p = tmp_path / "config.yaml"
    p.write_text(textwrap.dedent(content))
    return p


def write_views(tmp_path: Path, content: str) -> Path:
    p = tmp_path / "views.yaml"
    p.write_text(textwrap.dedent(content))
    return p


STATIC_CONFIG = """\
trino:
  host: localhost
  port: 8080
  catalog: iceberg
  schema: analytics
  user: test
"""

VALID_VIEWS = """\
views:
  - name: ohlcv_1m
    query: |
      SELECT symbol,
             date_trunc('minute', ts) AS minute,
             sum(qty) AS volume
      FROM iceberg.market_data.trades
      GROUP BY 1, 2
    refresh_interval_seconds: 30
"""


# ── load_config (static config only) ──

def test_load_valid(tmp_path):
    cfg = load_config(write_config(tmp_path, STATIC_CONFIG))
    assert cfg.trino.host == "localhost"
    assert cfg.views == []


def test_missing_trino(tmp_path):
    with pytest.raises(ValueError, match="missing 'trino'"):
        load_config(write_config(tmp_path, "server:\n  port: 8000\n"))


def test_defaults(tmp_path):
    cfg = load_config(write_config(tmp_path, STATIC_CONFIG))
    assert cfg.views == []
    assert cfg.server.port == 8000


# ── load_views ──

def test_load_views_valid(tmp_path):
    views = load_views(write_views(tmp_path, VALID_VIEWS))
    assert len(views) == 1
    v = views[0]
    assert v.name == "ohlcv_1m"
    assert "date_trunc" in v.query
    assert v.refresh_interval_seconds == 30


def test_load_views_missing_file(tmp_path):
    assert load_views(tmp_path / "nonexistent.yaml") == []


def test_view_defaults(tmp_path):
    minimal = (
        "views:\n"
        "  - name: v\n"
        "    query: \"SELECT date_trunc('day', ts) AS d FROM t GROUP BY 1\"\n"
    )
    views = load_views(write_views(tmp_path, minimal))
    assert views[0].refresh_interval_seconds == 60


def test_missing_query(tmp_path):
    bad = VALID_VIEWS.replace("    query: |\n      SELECT symbol,\n             date_trunc('minute', ts) AS minute,\n             sum(qty) AS volume\n      FROM iceberg.market_data.trades\n      GROUP BY 1, 2\n", "")
    with pytest.raises(ValueError, match="missing required fields"):
        load_views(write_views(tmp_path, bad))


def test_missing_name(tmp_path):
    bad = VALID_VIEWS.replace("  - name: ohlcv_1m\n", "  - \n")
    with pytest.raises(ValueError, match="missing required fields"):
        load_views(write_views(tmp_path, bad))


def test_invalid_view_name(tmp_path):
    bad = VALID_VIEWS.replace("name: ohlcv_1m", "name: drop-table")
    with pytest.raises(ValueError, match="valid SQL identifier"):
        load_views(write_views(tmp_path, bad))


def test_invalid_target_table(tmp_path):
    bad = VALID_VIEWS.replace(
        "    refresh_interval_seconds: 30\n",
        "    target_table: x;DROP\n",
    )
    with pytest.raises(ValueError, match="valid qualified table name"):
        load_views(write_views(tmp_path, bad))


def test_legacy_range_filter_placeholder_rejected(tmp_path):
    """{range_filter} is no longer supported — old views.yaml files must be migrated."""
    legacy = (
        "views:\n  - name: v\n"
        "    query: \"SELECT date_trunc('day', ts) AS d FROM t WHERE {range_filter} GROUP BY 1\"\n"
    )
    with pytest.raises(ValueError, match="range_filter"):
        load_views(write_views(tmp_path, legacy))


def test_duplicate_view_names_rejected(tmp_path):
    views_yaml = (
        "views:\n"
        "  - name: v\n"
        "    query: \"SELECT date_trunc('day', ts) AS d FROM t GROUP BY 1\"\n"
        "  - name: v\n"
        "    query: \"SELECT date_trunc('day', ts) AS d FROM t GROUP BY 1\"\n"
    )
    with pytest.raises(ValueError, match="duplicate"):
        load_views(write_views(tmp_path, views_yaml))


# ── save_views / load_views round-trip ──

def test_save_views_and_reload(tmp_path):
    views = [ViewConfig(
        name="ohlcv_1m",
        query="SELECT date_trunc('day', ts) AS d FROM t GROUP BY 1",
        refresh_interval_seconds=30,
    )]
    views_path = tmp_path / "views.yaml"
    save_views(views, views_path)
    loaded = load_views(views_path)
    assert loaded[0].name == views[0].name
    assert loaded[0].query.strip() == views[0].query.strip()
    assert loaded[0].refresh_interval_seconds == 30


def test_save_views_creates_parent_dirs(tmp_path):
    views_path = tmp_path / "data" / "views.yaml"
    save_views([], views_path)
    assert views_path.exists()
