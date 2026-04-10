"""Tests for the FastAPI server endpoints."""
import textwrap
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from trino_mv_orchestrator.config import load_config
from trino_mv_orchestrator.server import app, state, ViewStatus


@pytest.fixture(autouse=True)
def reset_state(tmp_path):
    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text(textwrap.dedent("""\
        trino:
          host: localhost
          port: 8080
          catalog: iceberg
          schema: analytics
          user: test
        views:
          - name: test_view
            source_table: iceberg.db.trades
            filter_column: ts
            query: "SELECT a, b FROM t WHERE {range_filter} GROUP BY 1"
            merge_keys: [a]
    """))
    state.config_path = cfg_path
    state.config = load_config(cfg_path)
    state.config_mtime = cfg_path.stat().st_mtime
    state.view_statuses = {"test_view": ViewStatus(name="test_view", last_action="skip", total_refreshes=3)}
    state._stop = False
    yield
    state.config = None
    state.view_statuses = {}


client = TestClient(app, raise_server_exceptions=False)


def test_health():
    assert client.get("/health").json()["views"] == 1

def test_metrics():
    assert "mv_views_configured" in client.get("/metrics").text

def test_list_views():
    views = client.get("/api/views").json()
    assert len(views) == 1
    assert views[0]["filter_column"] == "ts"

def test_create_view():
    r = client.post("/api/views", json={
        "name": "new", "source_table": "t", "filter_column": "ts",
        "query": "SELECT x FROM t WHERE {range_filter}", "merge_keys": ["x"],
    })
    assert r.status_code == 201
    assert len(state.config.views) == 2

def test_create_duplicate():
    assert client.post("/api/views", json={
        "name": "test_view", "source_table": "t", "filter_column": "ts",
        "query": "q {range_filter}", "merge_keys": ["a"],
    }).status_code == 409

def test_delete_view():
    assert client.delete("/api/views/test_view").status_code == 204
    assert len(state.config.views) == 0

def test_delete_not_found():
    assert client.delete("/api/views/nope").status_code == 404

@patch("trino_mv_orchestrator.server.refresh_view")
def test_trigger_refresh(mock):
    assert client.post("/api/views/test_view/refresh").status_code == 200
    mock.assert_called_once()

def test_ui():
    assert "Materialized Views" in client.get("/").text
