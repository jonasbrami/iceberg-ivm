"""Tests for config loading and validation."""
import textwrap
from pathlib import Path

import pytest

from trino_mv_orchestrator.config import load_config, save_config

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
    assert v.merge_keys == ["symbol", "minute"]

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
    minimal = "trino:\n  host: x\n  port: 1\n  catalog: c\n  schema: s\n  user: u\nviews:\n  - name: v\n    source_table: t\n    filter_column: ts\n    query: \"q {range_filter}\"\n    merge_keys: [a]\n"
    cfg = load_config(write_yaml(tmp_path, minimal))
    assert cfg.views[0].filter_granularity == "day"
    assert cfg.views[0].refresh_interval_seconds == 60
    assert cfg.server.port == 8000

def test_save_and_reload(tmp_path):
    cfg = load_config(write_yaml(tmp_path, VALID_CONFIG))
    out = tmp_path / "out.yaml"
    save_config(cfg, out)
    cfg2 = load_config(out)
    assert cfg2.views[0].filter_column == cfg.views[0].filter_column
    assert cfg2.views[0].filter_granularity == cfg.views[0].filter_granularity
