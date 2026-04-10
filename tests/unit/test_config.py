"""Tests for config loading and validation."""
import textwrap
from pathlib import Path

import pytest

from trino_mv_orchestrator.config import infer_granularity, load_config, save_config

def write_yaml(tmp_path: Path, content: str) -> Path:
    p = tmp_path / "config.yaml"
    p.write_text(textwrap.dedent(content))
    return p

VALID_CONFIG = """\
trino:
  host: localhost
  port: 8080
  catalog: iceberg
  schema: analytics
  user: test

views:
  - name: ohlcv_1m
    source_table: iceberg.market_data.trades
    filter_column: ts
    filter_granularity: day
    query: "SELECT * FROM t WHERE {range_filter} GROUP BY 1"
    merge_keys: [symbol, minute]
    refresh_interval_seconds: 30
"""

def test_load_valid(tmp_path):
    cfg = load_config(write_yaml(tmp_path, VALID_CONFIG))
    assert cfg.trino.host == "localhost"
    assert len(cfg.views) == 1
    v = cfg.views[0]
    assert v.filter_column == "ts"
    assert v.filter_granularity == "day"
    assert v.merge_keys == ("symbol", "minute")
    assert isinstance(v.merge_keys, tuple)

def test_missing_trino(tmp_path):
    with pytest.raises(ValueError, match="missing 'trino'"):
        load_config(write_yaml(tmp_path, "views:\n  - name: x\n    source_table: t\n    filter_column: ts\n    query: \"q {range_filter}\"\n    merge_keys: [a]\n"))

def test_missing_views(tmp_path):
    with pytest.raises(ValueError, match="missing 'views'"):
        load_config(write_yaml(tmp_path, "trino:\n  host: x\n  port: 1\n  catalog: c\n  schema: s\n  user: u\n"))

def test_missing_filter_column(tmp_path):
    bad = VALID_CONFIG.replace("    filter_column: ts\n", "")
    with pytest.raises(ValueError, match="filter_column"):
        load_config(write_yaml(tmp_path, bad))

def test_invalid_granularity(tmp_path):
    bad = VALID_CONFIG.replace("filter_granularity: day", "filter_granularity: century")
    with pytest.raises(ValueError, match="filter_granularity"):
        load_config(write_yaml(tmp_path, bad))

def test_missing_placeholder(tmp_path):
    bad = VALID_CONFIG.replace("{range_filter}", "1=1")
    with pytest.raises(ValueError, match="range_filter"):
        load_config(write_yaml(tmp_path, bad))

def test_defaults(tmp_path):
    minimal = "trino:\n  host: x\n  port: 1\n  catalog: c\n  schema: s\n  user: u\nviews:\n  - name: v\n    source_table: t\n    filter_column: ts\n    filter_granularity: day\n    query: \"q {range_filter}\"\n    merge_keys: [a]\n"
    cfg = load_config(write_yaml(tmp_path, minimal))
    assert cfg.views[0].filter_granularity == "day"
    assert cfg.views[0].refresh_interval_seconds == 60
    assert cfg.server.port == 8000

def test_invalid_view_name(tmp_path):
    bad = VALID_CONFIG.replace("name: ohlcv_1m", "name: drop-table")
    with pytest.raises(ValueError, match="valid SQL identifier"):
        load_config(write_yaml(tmp_path, bad))

def test_invalid_source_table(tmp_path):
    bad = VALID_CONFIG.replace("source_table: iceberg.market_data.trades", "source_table: x;DROP")
    with pytest.raises(ValueError, match="valid qualified table name"):
        load_config(write_yaml(tmp_path, bad))

def test_invalid_filter_column(tmp_path):
    bad = VALID_CONFIG.replace("filter_column: ts", "filter_column: ts OR")
    with pytest.raises(ValueError, match="valid SQL identifier"):
        load_config(write_yaml(tmp_path, bad))

def test_invalid_merge_key(tmp_path):
    bad = VALID_CONFIG.replace("merge_keys: [symbol, minute]", "merge_keys: [symbol, 1bad]")
    with pytest.raises(ValueError, match="valid SQL identifier"):
        load_config(write_yaml(tmp_path, bad))

def test_valid_qualified_table_names(tmp_path):
    """Qualified names with dots should be accepted."""
    cfg = load_config(write_yaml(tmp_path, VALID_CONFIG))
    assert cfg.views[0].source_table == "iceberg.market_data.trades"

def test_save_and_reload(tmp_path):
    cfg = load_config(write_yaml(tmp_path, VALID_CONFIG))
    out = tmp_path / "out.yaml"
    save_config(cfg, out)
    cfg2 = load_config(out)
    assert cfg2.views[0].filter_column == cfg.views[0].filter_column
    assert cfg2.views[0].filter_granularity == cfg.views[0].filter_granularity


# ── infer_granularity ──

def test_infer_minute():
    assert infer_granularity("SELECT date_trunc('minute', ts) FROM t") == "minute"

def test_infer_week():
    assert infer_granularity("SELECT date_trunc('week', ts) FROM t") == "week"

def test_infer_case_insensitive():
    assert infer_granularity("SELECT DATE_TRUNC('Hour', ts) FROM t") == "hour"

def test_infer_no_date_trunc():
    assert infer_granularity("SELECT ts FROM t") is None

def test_infer_complex_expr_returns_none():
    """date_trunc used in arithmetic (5-min bars) should not be inferred."""
    q = ("SELECT date_trunc('minute', minute) "
         "- (extract(minute FROM minute) % 5) * INTERVAL '1' MINUTE AS bar FROM t")
    assert infer_granularity(q) is None

def test_infer_invalid_granularity_ignored():
    assert infer_granularity("SELECT date_trunc('second', ts) FROM t") is None

def test_infer_multiple_same():
    q = "SELECT date_trunc('hour', ts), date_trunc('hour', other) FROM t"
    assert infer_granularity(q) == "hour"

def test_infer_multiple_different_returns_none():
    q = "SELECT date_trunc('hour', ts), date_trunc('day', ts) FROM t"
    assert infer_granularity(q) is None


# ── config-level inference ──

TRINO_BLOCK = "trino:\n  host: x\n  port: 1\n  catalog: c\n  schema: s\n  user: u\n"

def test_load_infers_granularity(tmp_path):
    yaml = TRINO_BLOCK + (
        "views:\n  - name: v\n    source_table: t\n    filter_column: ts\n"
        "    query: \"SELECT date_trunc('minute', ts) AS m FROM t WHERE {range_filter} GROUP BY 1\"\n"
        "    merge_keys: [m]\n"
    )
    cfg = load_config(write_yaml(tmp_path, yaml))
    assert cfg.views[0].filter_granularity == "minute"

def test_load_explicit_overrides_inference(tmp_path):
    yaml = TRINO_BLOCK + (
        "views:\n  - name: v\n    source_table: t\n    filter_column: ts\n"
        "    filter_granularity: day\n"
        "    query: \"SELECT date_trunc('minute', ts) AS m FROM t WHERE {range_filter} GROUP BY 1\"\n"
        "    merge_keys: [m]\n"
    )
    cfg = load_config(write_yaml(tmp_path, yaml))
    assert cfg.views[0].filter_granularity == "day"

def test_load_fails_when_cannot_infer(tmp_path):
    yaml = TRINO_BLOCK + (
        "views:\n  - name: v\n    source_table: t\n    filter_column: ts\n"
        "    query: \"SELECT ts FROM t WHERE {range_filter}\"\n"
        "    merge_keys: [ts]\n"
    )
    with pytest.raises(ValueError, match="could not be inferred"):
        load_config(write_yaml(tmp_path, yaml))
