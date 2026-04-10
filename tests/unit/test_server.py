"""Tests for the FastAPI server endpoints."""
import textwrap
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from trino_mv_orchestrator.config import Config, load_config, load_views
from trino_mv_orchestrator.server import AppState, ViewStatus, app, get_app_state


STATIC_CONFIG_YAML = textwrap.dedent("""\
    trino:
      host: localhost
      port: 8080
      catalog: iceberg
      schema: analytics
      user: test
""")

VIEWS_YAML = textwrap.dedent("""\
    views:
      - name: test_view
        source_table: iceberg.db.trades
        filter_column: ts
        filter_granularity: day
        query: "SELECT a, b FROM t WHERE {range_filter} GROUP BY 1"
        merge_keys: [a]
""")


@pytest.fixture(autouse=True)
def setup_state(tmp_path):
    """Pre-seed AppState on app.state so lifespan skips init and refresh loop exits immediately."""
    cfg_path = tmp_path / "config.yaml"
    views_path = tmp_path / "views.yaml"
    cfg_path.write_text(STATIC_CONFIG_YAML)
    views_path.write_text(VIEWS_YAML)
    static_cfg = load_config(cfg_path)
    views = load_views(views_path)
    s = AppState(config_path=cfg_path, views_path=views_path)
    s.config = Config(trino=static_cfg.trino, views=views, server=static_cfg.server)
    s.config_mtime = cfg_path.stat().st_mtime
    s.views_mtime = views_path.stat().st_mtime
    s.view_statuses = {
        "test_view": ViewStatus(name="test_view", last_action="skip", total_refreshes=3),
    }
    s._stop = True  # Prevents refresh loop from running
    app.state.s = s
    yield s
    if hasattr(app.state, "s"):
        del app.state.s


@pytest.fixture()
def client():
    return TestClient(app, raise_server_exceptions=False)


# ── Health & metrics ──

def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["views"] == 1


def test_metrics(client):
    assert "mv_views_configured" in client.get("/metrics").text


# ── CRUD ──

def test_list_views(client):
    views = client.get("/api/views").json()
    assert len(views) == 1
    assert views[0]["filter_column"] == "ts"
    assert views[0]["status"]["total_refreshes"] == 3


def test_create_view(client, setup_state):
    r = client.post("/api/views", json={
        "name": "new_view",
        "source_table": "iceberg.db.t",
        "filter_column": "ts",
        "filter_granularity": "day",
        "query": "SELECT x FROM t WHERE {range_filter}",
        "merge_keys": ["x"],
    })
    assert r.status_code == 201
    assert len(setup_state.config.views) == 2


def test_create_view_invalid_name(client):
    """SQL injection via view name should be rejected."""
    r = client.post("/api/views", json={
        "name": "bad-name",
        "source_table": "iceberg.db.t",
        "filter_column": "ts",
        "filter_granularity": "day",
        "query": "SELECT x FROM t WHERE {range_filter}",
        "merge_keys": ["x"],
    })
    assert r.status_code == 422


def test_create_duplicate(client):
    r = client.post("/api/views", json={
        "name": "test_view",
        "source_table": "t",
        "filter_column": "ts",
        "filter_granularity": "day",
        "query": "q {range_filter}",
        "merge_keys": ["a"],
    })
    assert r.status_code == 409


def test_delete_view(client, setup_state):
    assert client.delete("/api/views/test_view").status_code == 204
    assert len(setup_state.config.views) == 0


def test_delete_not_found(client):
    assert client.delete("/api/views/nope").status_code == 404


# ── Trigger refresh ──

@patch("trino_mv_orchestrator.server.refresh_view", new_callable=AsyncMock)
def test_trigger_refresh(mock_refresh, client):
    r = client.post("/api/views/test_view/refresh")
    assert r.status_code == 200
    mock_refresh.assert_called_once()


def test_trigger_refresh_not_found(client):
    r = client.post("/api/views/nope/refresh")
    assert r.status_code == 404


# ── UI ──

def test_ui(client):
    r = client.get("/")
    assert r.status_code == 200
    assert "Materialized Views" in r.text


# ── Config reload ──

def test_reload_config_on_mtime_change(setup_state, tmp_path):
    from trino_mv_orchestrator.server import reload_config

    assert len(setup_state.config.views) == 1

    new_views_yaml = VIEWS_YAML + (
        "  - name: second_view\n"
        "    source_table: iceberg.db.other\n"
        "    filter_column: ts\n"
        "    filter_granularity: day\n"
        '    query: "SELECT x FROM t2 WHERE {range_filter}"\n'
        "    merge_keys: [x]\n"
    )
    setup_state.views_path.write_text(new_views_yaml)

    reloaded = reload_config(setup_state)
    assert reloaded is True
    assert len(setup_state.config.views) == 2
    assert "second_view" in setup_state.view_statuses


def test_reload_config_no_change(setup_state):
    from trino_mv_orchestrator.server import reload_config

    reloaded = reload_config(setup_state)
    assert reloaded is False


# ── Metrics presence ──

def test_new_metrics_defined():
    """Verify the enhanced metrics are importable."""
    from trino_mv_orchestrator.server import (
        DETECTION_DURATION,
        REFRESH_BYTES,
        REFRESH_ROWS,
        SOURCE_SNAPSHOT,
    )
    assert REFRESH_BYTES is not None
    assert REFRESH_ROWS is not None
    assert DETECTION_DURATION is not None
    assert SOURCE_SNAPSHOT is not None


# ── Granularity inference via API ──

def test_create_view_infers_granularity(client, setup_state):
    r = client.post("/api/views", json={
        "name": "auto_view",
        "source_table": "iceberg.db.t",
        "filter_column": "ts",
        "query": "SELECT date_trunc('hour', ts) AS h FROM t WHERE {range_filter} GROUP BY 1",
        "merge_keys": ["h"],
    })
    assert r.status_code == 201
    assert r.json()["filter_granularity"] == "hour"


def test_create_view_explicit_overrides(client, setup_state):
    r = client.post("/api/views", json={
        "name": "explicit_view",
        "source_table": "iceberg.db.t",
        "filter_column": "ts",
        "filter_granularity": "day",
        "query": "SELECT date_trunc('hour', ts) AS h FROM t WHERE {range_filter} GROUP BY 1",
        "merge_keys": ["h"],
    })
    assert r.status_code == 201
    assert r.json()["filter_granularity"] == "day"


def test_create_view_fails_when_cannot_infer(client, setup_state):
    r = client.post("/api/views", json={
        "name": "fail_view",
        "source_table": "iceberg.db.t",
        "filter_column": "ts",
        "query": "SELECT ts FROM t WHERE {range_filter}",
        "merge_keys": ["ts"],
    })
    assert r.status_code == 422
