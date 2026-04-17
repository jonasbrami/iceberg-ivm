"""Tests for the FastAPI server endpoints."""
import textwrap
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from trino_mv_orchestrator.config import Config, load_config, load_views
from trino_mv_orchestrator.server import AppState, ViewStatus, app, get_app_state


STATIC_CONFIG_YAML = textwrap.dedent("""\
    trino:
      catalog: iceberg
      schema: analytics
""")


# Credentials come from env vars only — set them for every test so
# load_config doesn't refuse to initialize.
@pytest.fixture(autouse=True)
def trino_env(monkeypatch):
    monkeypatch.setenv("TRINO_URL", "http://localhost:8080")
    monkeypatch.setenv("TRINO_USER", "test")
    monkeypatch.setenv("TRINO_PASSWORD", "hunter2")

VIEWS_YAML = textwrap.dedent("""\
    views:
      - name: test_view
        query: |
          SELECT date_trunc('day', ts) AS d, a
          FROM iceberg.db.trades
          GROUP BY 1, 2
""")


@pytest.fixture(autouse=True)
def setup_state(tmp_path, trino_env):
    """Pre-seed AppState on app.state so lifespan skips init and refresh loop exits immediately.

    Depends on `trino_env` so the credential env vars are set before
    load_config runs.
    """
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


def test_view_schema(client):
    """Schema endpoint drives the dynamic UI — must expose only create-view fields.

    source_table / filter_column / merge_keys are derived from the query and
    must NOT appear in the create form.
    """
    r = client.get("/api/views/schema")
    assert r.status_code == 200
    schema = r.json()
    assert isinstance(schema, list)
    names = {f["name"] for f in schema}
    assert names == {
        "name", "query",
        "target_table", "target_partitioning", "refresh_interval_seconds",
    }
    for f in schema:
        assert "label" in f and "type" in f and "required" in f
    # Derived fields must not leak into the create form
    assert "source_table" not in names
    assert "filter_column" not in names
    assert "merge_keys" not in names


# ── CRUD ──

def test_list_views(client):
    views = client.get("/api/views").json()
    assert len(views) == 1
    # Derived fields still appear in the response so the UI can render them
    assert views[0]["source_table"] == "iceberg.db.trades"
    assert views[0]["filter_column"] == "ts"
    assert views[0]["merge_keys"] == ["d", "a"]
    assert views[0]["status"]["total_refreshes"] == 3


def test_create_view(client, setup_state):
    r = client.post("/api/views", json={
        "name": "new_view",
        "query": (
            "SELECT date_trunc('day', ts) AS d FROM iceberg.db.t GROUP BY 1"
        ),
    })
    assert r.status_code == 201
    body = r.json()
    assert body["source_table"] == "iceberg.db.t"
    assert body["filter_column"] == "ts"
    assert body["merge_keys"] == ["d"]
    assert len(setup_state.config.views) == 2


def test_create_view_invalid_name(client):
    """SQL injection via view name should be rejected."""
    r = client.post("/api/views", json={
        "name": "bad-name",
        "query": "SELECT date_trunc('day', ts) AS d FROM t GROUP BY 1",
    })
    assert r.status_code == 422


def test_create_duplicate(client):
    r = client.post("/api/views", json={
        "name": "test_view",
        "query": "SELECT date_trunc('day', ts) AS d FROM t GROUP BY 1",
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
        "    query: |\n"
        "      SELECT date_trunc('hour', ts) AS h FROM iceberg.db.other GROUP BY 1\n"
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


# ── Trino connection ──

def test_get_trino_connection_pins_timezone_to_utc(setup_state):
    """All Trino sessions must be pinned to UTC.

    `date_trunc('day' | 'week' | 'month', ts)` on a TIMESTAMP WITH TIME
    ZONE column operates in the session timezone; if the session isn't
    UTC, Trino's bucket boundaries will disagree with the Python-side
    `expand_to_bucket_bounds` math and the MERGE will recompute partial buckets with
    wrong aggregates. Pinning to UTC makes the two sides agree by
    construction.
    """
    from trino_mv_orchestrator.server import get_trino_connection

    captured = {}
    def fake_connect(**kwargs):
        captured.update(kwargs)
        return object()

    with patch("trino_mv_orchestrator.server.aiotrino.dbapi.connect",
               side_effect=fake_connect):
        get_trino_connection(setup_state)

    assert captured.get("timezone") == "UTC", (
        f"Trino connection was opened without timezone=UTC; got {captured}"
    )


def test_get_trino_connection_uses_env_credentials(setup_state):
    """The connection opens with host/port/scheme parsed from TRINO_URL
    and uses BasicAuthentication from TRINO_USER / TRINO_PASSWORD."""
    from trino_mv_orchestrator.server import get_trino_connection
    from aiotrino.auth import BasicAuthentication

    captured = {}
    def fake_connect(**kwargs):
        captured.update(kwargs)
        return object()

    with patch("trino_mv_orchestrator.server.aiotrino.dbapi.connect",
               side_effect=fake_connect):
        get_trino_connection(setup_state)

    assert captured["host"] == "localhost"
    assert captured["port"] == 8080
    assert captured["http_scheme"] == "http"
    assert captured["user"] == "test"
    assert isinstance(captured["auth"], BasicAuthentication)


def test_get_trino_connection_parses_https_url(setup_state, monkeypatch):
    """https:// URL produces http_scheme='https' and the right port."""
    from trino_mv_orchestrator.server import get_trino_connection
    monkeypatch.setenv("TRINO_URL", "https://trino.prod.internal:8443")
    setup_state.config = load_config(setup_state.config_path)  # reload with new env

    captured = {}
    def fake_connect(**kwargs):
        captured.update(kwargs)
        return object()

    with patch("trino_mv_orchestrator.server.aiotrino.dbapi.connect",
               side_effect=fake_connect):
        get_trino_connection(setup_state)

    assert captured["host"] == "trino.prod.internal"
    assert captured["port"] == 8443
    assert captured["http_scheme"] == "https"


def test_get_trino_connection_omits_auth_when_no_password(setup_state, monkeypatch):
    """When TRINO_PASSWORD is unset the connection opens without auth."""
    from trino_mv_orchestrator.server import get_trino_connection
    monkeypatch.delenv("TRINO_PASSWORD")
    setup_state.config = load_config(setup_state.config_path)  # reload

    captured = {}
    def fake_connect(**kwargs):
        captured.update(kwargs)
        return object()

    with patch("trino_mv_orchestrator.server.aiotrino.dbapi.connect",
               side_effect=fake_connect):
        get_trino_connection(setup_state)

    assert "auth" not in captured
    assert captured["user"] == "test"


# ── State advance on NO_CHANGE ──

async def test_refresh_view_advances_state_on_empty_append_no_change(setup_state):
    """When the detector reports NO_CHANGE but current_snapshot has
    advanced (empty-append or compaction-only snapshots), state must
    still be written — otherwise the view re-detects the same
    snapshots forever.
    """
    from trino_mv_orchestrator import server as server_mod
    from trino_mv_orchestrator.detector import ChangeResult, RefreshAction
    from trino_mv_orchestrator.introspect import ColumnInfo

    view = setup_state.config.views[0]
    write_calls: list[int] = []

    class FakeConn:
        async def cursor(self): return FakeCursor()
        async def close(self): pass

    class FakeCursor:
        stats = {}
        async def execute(self, sql): pass
        async def fetchone(self): return None
        async def fetchall(self): return []

    async def fake_write(cursor, target, snap_id):
        write_calls.append(snap_id)

    async def fake_read(cursor, target): return 100

    async def fake_detect(*args, **kwargs):
        return ChangeResult(action=RefreshAction.NO_CHANGE, current_snapshot=200)

    async def fake_discover_columns(cursor, query):
        return [ColumnInfo(name="d", type="DATE"), ColumnInfo(name="a", type="VARCHAR")]

    with patch.object(server_mod, "get_trino_connection", lambda s: FakeConn()), \
         patch.object(server_mod, "discover_columns", fake_discover_columns), \
         patch.object(server_mod, "read_last_snapshot", fake_read), \
         patch.object(server_mod, "write_last_snapshot", fake_write), \
         patch.object(server_mod, "detect_changes", fake_detect):
        await server_mod.refresh_view(setup_state, view)

    assert write_calls == [200], (
        f"expected write_last_snapshot(200), got {write_calls}. "
        f"view status: {setup_state.view_statuses[view.name]!r}"
    )


async def test_refresh_view_appends_recent_queries(setup_state, client):
    """A successful refresh must surface the MERGE / INSERT query IDs on
    the view's status so the UI can link to the Trino UI.
    """
    from trino_mv_orchestrator import server as server_mod
    from trino_mv_orchestrator.detector import ChangeResult, RefreshAction
    from trino_mv_orchestrator.executor import RefreshResult, QueryInfo
    from trino_mv_orchestrator.introspect import ColumnInfo

    view = setup_state.config.views[0]

    class FakeConn:
        async def cursor(self): return FakeCursor()
        async def close(self): pass

    class FakeCursor:
        stats = {}
        async def execute(self, sql): pass
        async def fetchone(self): return None
        async def fetchall(self): return []

    async def fake_write(cursor, target, snap_id): pass
    async def fake_read(cursor, target): return None

    async def fake_detect(*args, **kwargs):
        return ChangeResult(action=RefreshAction.FULL_REFRESH, current_snapshot=1)

    async def fake_discover_columns(cursor, query):
        return [ColumnInfo(name="d", type="DATE"), ColumnInfo(name="a", type="VARCHAR")]

    async def fake_full(cursor, view, target):
        return RefreshResult(
            elapsed=0.5,
            processed_rows=10,
            processed_bytes=2048,
            queries=[
                QueryInfo(
                    query_id="20260417_000000_00001_xyz",
                    info_uri="http://trino/ui/query.html?20260417_000000_00001_xyz",
                    stage="full_delete", started_at=1.0, elapsed_ms=120.0,
                ),
                QueryInfo(
                    query_id="20260417_000000_00002_xyz",
                    info_uri="http://trino/ui/query.html?20260417_000000_00002_xyz",
                    stage="full_insert", started_at=2.0, elapsed_ms=380.0,
                    processed_rows=10, processed_bytes=2048,
                ),
            ],
        )

    with patch.object(server_mod, "get_trino_connection", lambda s: FakeConn()), \
         patch.object(server_mod, "discover_columns", fake_discover_columns), \
         patch.object(server_mod, "read_last_snapshot", fake_read), \
         patch.object(server_mod, "write_last_snapshot", fake_write), \
         patch.object(server_mod, "detect_changes", fake_detect), \
         patch.object(server_mod, "execute_full_refresh", fake_full):
        await server_mod.refresh_view(setup_state, view)

    # Now the view status should carry the captured queries.
    vs = setup_state.view_statuses[view.name]
    assert len(vs.recent_queries) == 2
    stages = [q.stage for q in vs.recent_queries]
    assert "full_insert" in stages and "full_delete" in stages

    # And the API exposes them.
    body = client.get("/api/views").json()[0]
    status_queries = body["status"]["recent_queries"]
    assert len(status_queries) == 2
    assert status_queries[0]["info_uri"].endswith("20260417_000000_00001_xyz") or \
           status_queries[1]["info_uri"].endswith("20260417_000000_00001_xyz")


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


# ── Query validation via API ──

def test_create_view_fails_when_no_date_trunc(client, setup_state):
    r = client.post("/api/views", json={
        "name": "fail_view",
        "query": "SELECT ts FROM t GROUP BY 1",
    })
    assert r.status_code == 422
    assert "date_trunc" in r.json()["detail"]


def test_create_view_fails_on_arithmetic(client, setup_state):
    r = client.post("/api/views", json={
        "name": "arith_view",
        "query": (
            "SELECT date_trunc('minute', ts) - INTERVAL '5' MINUTE AS x "
            "FROM t GROUP BY 1"
        ),
    })
    assert r.status_code == 422
    assert "arithmetic" in r.json()["detail"]


# ── /api/views/parse — live validation for the UI ──

def test_parse_query_returns_parsed_fields(client):
    r = client.post("/api/views/parse", json={
        "query": (
            "SELECT symbol, date_trunc('week', ts) AS week, sum(q) AS v "
            "FROM iceberg.md.trades GROUP BY 1, 2"
        ),
    })
    assert r.status_code == 200
    body = r.json()
    assert body["source_table"] == "iceberg.md.trades"
    assert body["filter_column"] == "ts"
    assert body["granularity"] == "week"
    assert body["merge_keys"] == ["symbol", "week"]


def test_parse_query_rejects_invalid(client):
    r = client.post("/api/views/parse", json={
        "query": "SELECT date_trunc('day', ts) - INTERVAL '1' DAY AS x FROM t GROUP BY 1",
    })
    assert r.status_code == 422
    assert "arithmetic" in r.json()["detail"]


def test_parse_query_rejects_legacy_placeholder(client):
    r = client.post("/api/views/parse", json={
        "query": (
            "SELECT date_trunc('day', ts) AS d FROM t WHERE {range_filter} GROUP BY 1"
        ),
    })
    assert r.status_code == 422
    assert "range_filter" in r.json()["detail"]


def test_create_view_rejects_legacy_range_filter(client, setup_state):
    r = client.post("/api/views", json={
        "name": "legacy_view",
        "query": (
            "SELECT date_trunc('day', ts) AS d FROM t WHERE {range_filter} GROUP BY 1"
        ),
    })
    assert r.status_code == 422
    assert "range_filter" in r.json()["detail"]
