"""Tests for config loading and validation."""
import textwrap
from pathlib import Path

import pytest

from trino_mv_orchestrator.config import ViewConfig, infer_granularity, load_config, load_views, save_views


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
    source_table: iceberg.market_data.trades
    filter_column: ts
    query: "SELECT date_trunc('day', ts) AS d FROM t WHERE {range_filter} GROUP BY 1"
    merge_keys: [symbol, minute]
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
    assert v.filter_column == "ts"
    assert v.merge_keys == ("symbol", "minute")
    assert isinstance(v.merge_keys, tuple)


def test_load_views_missing_file(tmp_path):
    assert load_views(tmp_path / "nonexistent.yaml") == []


def test_view_defaults(tmp_path):
    minimal = (
        "views:\n  - name: v\n    source_table: t\n    filter_column: ts\n"
        "    query: \"SELECT date_trunc('day', ts) AS d FROM t WHERE {range_filter} GROUP BY 1\"\n"
        "    merge_keys: [a]\n"
    )
    views = load_views(write_views(tmp_path, minimal))
    assert views[0].refresh_interval_seconds == 60


def test_missing_filter_column(tmp_path):
    bad = VALID_VIEWS.replace("    filter_column: ts\n", "")
    with pytest.raises(ValueError, match="filter_column"):
        load_views(write_views(tmp_path, bad))


def test_missing_placeholder(tmp_path):
    bad = VALID_VIEWS.replace("{range_filter}", "1=1")
    with pytest.raises(ValueError, match="range_filter"):
        load_views(write_views(tmp_path, bad))


def test_invalid_view_name(tmp_path):
    bad = VALID_VIEWS.replace("name: ohlcv_1m", "name: drop-table")
    with pytest.raises(ValueError, match="valid SQL identifier"):
        load_views(write_views(tmp_path, bad))


def test_invalid_source_table(tmp_path):
    bad = VALID_VIEWS.replace("source_table: iceberg.market_data.trades", "source_table: x;DROP")
    with pytest.raises(ValueError, match="valid qualified table name"):
        load_views(write_views(tmp_path, bad))


def test_invalid_filter_column(tmp_path):
    bad = VALID_VIEWS.replace("filter_column: ts", "filter_column: ts OR")
    with pytest.raises(ValueError, match="valid SQL identifier"):
        load_views(write_views(tmp_path, bad))


def test_invalid_merge_key(tmp_path):
    bad = VALID_VIEWS.replace("merge_keys: [symbol, minute]", "merge_keys: [symbol, 1bad]")
    with pytest.raises(ValueError, match="valid SQL identifier"):
        load_views(write_views(tmp_path, bad))


def test_valid_qualified_table_names(tmp_path):
    views = load_views(write_views(tmp_path, VALID_VIEWS))
    assert views[0].source_table == "iceberg.market_data.trades"


# ── save_views / load_views round-trip ──

def test_save_views_and_reload(tmp_path):
    views = [ViewConfig(
        name="ohlcv_1m",
        source_table="iceberg.market_data.trades",
        query="SELECT date_trunc('day', ts) AS d FROM t WHERE {range_filter} GROUP BY 1",
        merge_keys=("symbol", "minute"),
        filter_column="ts",
        refresh_interval_seconds=30,
    )]
    views_path = tmp_path / "views.yaml"
    save_views(views, views_path)
    loaded = load_views(views_path)
    assert loaded[0].filter_column == views[0].filter_column
    assert loaded[0].merge_keys == views[0].merge_keys


def test_save_views_creates_parent_dirs(tmp_path):
    views_path = tmp_path / "data" / "views.yaml"
    save_views([], views_path)
    assert views_path.exists()


# ── infer_granularity ──

def test_infer_minute():
    assert infer_granularity("SELECT date_trunc('minute', ts) FROM t") == "minute"

def test_infer_hour():
    assert infer_granularity("SELECT date_trunc('hour', ts) FROM t") == "hour"

def test_infer_day():
    assert infer_granularity("SELECT date_trunc('day', ts) FROM t") == "day"

def test_infer_week():
    assert infer_granularity("SELECT date_trunc('week', ts) FROM t") == "week"

def test_infer_month():
    assert infer_granularity("SELECT date_trunc('month', ts) FROM t") == "month"

def test_infer_quarter():
    assert infer_granularity("SELECT date_trunc('quarter', ts) FROM t") == "quarter"

def test_infer_year():
    assert infer_granularity("SELECT date_trunc('year', ts) FROM t") == "year"

def test_infer_case_insensitive():
    assert infer_granularity("SELECT DATE_TRUNC('Hour', ts) FROM t") == "hour"

def test_infer_no_date_trunc_raises():
    with pytest.raises(ValueError, match="must contain a date_trunc"):
        infer_granularity("SELECT ts FROM t")

def test_infer_complex_expr_raises():
    """date_trunc used in arithmetic should be rejected."""
    q = ("SELECT date_trunc('minute', minute) "
         "- (extract(minute FROM minute) % 5) * INTERVAL '1' MINUTE AS bar FROM t")
    with pytest.raises(ValueError, match="complex date_trunc"):
        infer_granularity(q)

def test_infer_invalid_granularity_raises():
    with pytest.raises(ValueError, match="could not infer a single granularity"):
        infer_granularity("SELECT date_trunc('second', ts) FROM t")

def test_infer_multiple_same():
    q = "SELECT date_trunc('hour', ts), date_trunc('hour', other) FROM t"
    assert infer_granularity(q) == "hour"

def test_infer_multiple_different_raises():
    q = "SELECT date_trunc('hour', ts), date_trunc('day', ts) FROM t"
    with pytest.raises(ValueError, match="could not infer a single granularity"):
        infer_granularity(q)


# ── views-level query validation ──
# The loader calls infer_granularity() on the query to validate it, but the
# result is not stored. These tests verify that valid queries load and invalid
# queries are rejected.

def test_load_views_accepts_minute(tmp_path):
    views_yaml = (
        "views:\n  - name: v\n    source_table: t\n    filter_column: ts\n"
        "    query: \"SELECT date_trunc('minute', ts) AS m FROM t WHERE {range_filter} GROUP BY 1\"\n"
        "    merge_keys: [m]\n"
    )
    assert len(load_views(write_views(tmp_path, views_yaml))) == 1


def test_load_views_accepts_quarter(tmp_path):
    views_yaml = (
        "views:\n  - name: v\n    source_table: t\n    filter_column: ts\n"
        "    query: \"SELECT date_trunc('quarter', ts) AS q FROM t WHERE {range_filter} GROUP BY 1\"\n"
        "    merge_keys: [q]\n"
    )
    assert len(load_views(write_views(tmp_path, views_yaml))) == 1


def test_load_views_accepts_year(tmp_path):
    views_yaml = (
        "views:\n  - name: v\n    source_table: t\n    filter_column: ts\n"
        "    query: \"SELECT date_trunc('year', ts) AS y FROM t WHERE {range_filter} GROUP BY 1\"\n"
        "    merge_keys: [y]\n"
    )
    assert len(load_views(write_views(tmp_path, views_yaml))) == 1


def test_load_views_fails_when_cannot_infer(tmp_path):
    views_yaml = (
        "views:\n  - name: v\n    source_table: t\n    filter_column: ts\n"
        "    query: \"SELECT ts FROM t WHERE {range_filter}\"\n"
        "    merge_keys: [ts]\n"
    )
    with pytest.raises(ValueError, match="must contain a date_trunc"):
        load_views(write_views(tmp_path, views_yaml))


def test_load_views_fails_on_complex_expr(tmp_path):
    views_yaml = (
        "views:\n  - name: v\n    source_table: t\n    filter_column: ts\n"
        "    query: \"SELECT date_trunc('minute', ts) - INTERVAL '5' MINUTE AS x FROM t WHERE {range_filter} GROUP BY 1\"\n"
        "    merge_keys: [x]\n"
    )
    with pytest.raises(ValueError, match="complex date_trunc"):
        load_views(write_views(tmp_path, views_yaml))
