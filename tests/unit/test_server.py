"""Tests for the FastAPI server endpoints."""
import asyncio
import textwrap
from unittest.mock import patch

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

def test_trigger_refresh_not_found(client):
    r = client.post("/api/views/nope/refresh")
    assert r.status_code == 404


async def test_trigger_refresh_signals_worker_and_returns_status(setup_state):
    """POST /refresh sets the worker's wake event, waits for the worker to
    complete one refresh cycle, and returns the resulting status.
    """
    from trino_mv_orchestrator import server as server_mod

    view = setup_state.config.views[0]
    setup_state._stop = False

    async def fake_refresh_view(s, v):
        vs = s.view_statuses.setdefault(v.name, server_mod.ViewStatus(name=v.name))
        vs.last_action = "full"

    with patch.object(server_mod, "refresh_view", fake_refresh_view):
        worker = asyncio.create_task(server_mod.view_worker(setup_state, view.name))
        try:
            result = await server_mod.trigger_refresh(view.name, setup_state)
        finally:
            setup_state._stop = True
            worker.cancel()
            try:
                await worker
            except asyncio.CancelledError:
                pass

    assert result["status"] == "ok"
    assert result["last_action"] == "full"


async def test_concurrent_triggers_coalesce_into_single_followup(setup_state):
    """Repro for #23 / #24.

    With a per-view worker as the sole caller of ``refresh_view``, a burst of
    concurrent triggers (manual POSTs, interval ticks, …) coalesces into *one*
    follow-up refresh after the in-flight one, regardless of burst size. This
    is strictly stronger than "no overlap": a lock-based fix would serialize
    every trigger into its own refresh — ``refresh_count`` would grow with
    the burst. Coalescing keeps it at 2 (in-flight + single coalesced pass).
    """
    from trino_mv_orchestrator import server as server_mod
    from trino_mv_orchestrator.detector import ChangeResult, RefreshAction
    from trino_mv_orchestrator.executor import RefreshResult
    from trino_mv_orchestrator.introspect import ColumnInfo

    view = setup_state.config.views[0]
    setup_state._stop = False

    class FakeConn:
        async def cursor(self): return FakeCursor()
        async def close(self): pass

    class FakeCursor:
        stats = {}
        async def execute(self, sql): pass
        async def fetchone(self): return None
        async def fetchall(self): return []

    in_flight = 0
    max_in_flight = 0
    refresh_count = 0

    async def slow_full(cursor, view, target):
        nonlocal in_flight, max_in_flight, refresh_count
        in_flight += 1
        max_in_flight = max(max_in_flight, in_flight)
        # Yield the event loop so a second caller could interleave if the
        # worker weren't serializing refreshes.
        for _ in range(5):
            await asyncio.sleep(0)
        in_flight -= 1
        refresh_count += 1
        return RefreshResult(
            elapsed=0.0, processed_rows=0, processed_bytes=0, queries=[],
        )

    async def fake_detect(*a, **k):
        return ChangeResult(action=RefreshAction.FULL_REFRESH, current_snapshot=1)

    async def fake_discover_columns(c, q):
        return [ColumnInfo(name="d", type="DATE"), ColumnInfo(name="a", type="VARCHAR")]

    async def fake_read(c, t): return None
    async def fake_write(c, t, snap): pass

    with patch.object(server_mod, "get_trino_connection", lambda s: FakeConn()), \
         patch.object(server_mod, "discover_columns", fake_discover_columns), \
         patch.object(server_mod, "read_last_snapshot", fake_read), \
         patch.object(server_mod, "write_last_snapshot", fake_write), \
         patch.object(server_mod, "detect_changes", fake_detect), \
         patch.object(server_mod, "execute_full_refresh", slow_full):
        worker = asyncio.create_task(server_mod.view_worker(setup_state, view.name))
        try:
            # Fire 10 concurrent triggers — they all hit the worker while its
            # first refresh is in flight.
            await asyncio.gather(*[
                server_mod.trigger_refresh(view.name, setup_state) for _ in range(10)
            ])
        finally:
            setup_state._stop = True
            worker.cancel()
            try:
                await worker
            except asyncio.CancelledError:
                pass

    assert max_in_flight == 1, (
        f"worker ran refresh_view bodies in parallel "
        f"(max_in_flight={max_in_flight}); see issue #24"
    )
    assert refresh_count == 2, (
        f"10 concurrent triggers produced {refresh_count} refreshes; "
        f"expected exactly 2 (initial in-flight + one coalesced follow-up). "
        f"A lock-based fix would produce ~11."
    )


async def test_trigger_bails_when_view_deleted_during_wait(setup_state):
    """If the view is deleted while a trigger is parked on the condition,
    the worker's shutdown-notify wakes the waiter and it returns 410 —
    no hang.
    """
    from fastapi import HTTPException
    from trino_mv_orchestrator import server as server_mod
    from trino_mv_orchestrator.config import Config

    view = setup_state.config.views[0]
    setup_state._stop = False

    # A refresh that blocks until cancelled — simulates a long-running INSERT.
    async def hanging_refresh_view(s, v):
        await asyncio.Event().wait()

    with patch.object(server_mod, "refresh_view", hanging_refresh_view):
        worker = asyncio.create_task(server_mod.view_worker(setup_state, view.name))
        trigger = asyncio.create_task(
            server_mod.trigger_refresh(view.name, setup_state)
        )
        # Let the worker enter refresh_view and the trigger reach its wait.
        for _ in range(5):
            await asyncio.sleep(0)

        # Remove the view from config and cancel the worker, mirroring what
        # delete_view + supervisor would do in production.
        setup_state.config = Config(
            trino=setup_state.config.trino,
            views=[],
            server=setup_state.config.server,
        )
        worker.cancel()
        try:
            await worker
        except asyncio.CancelledError:
            pass

        with pytest.raises(HTTPException) as exc:
            await trigger
        assert exc.value.status_code == 410


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


# ── refresh_view: chunked full refresh dispatch + interrupt ──


def _chunked_view_config():
    from trino_mv_orchestrator.config import ViewConfig
    return ViewConfig(
        name="test_view",
        query=(
            "SELECT date_trunc('day', ts) AS d, a "
            "FROM iceberg.db.trades GROUP BY 1, 2"
        ),
        target_table="iceberg.out.mv",
        full_refresh_chunk="day",
    )


def _install_chunked_view(s):
    """Swap setup_state's view for one with full_refresh_chunk='day'."""
    from trino_mv_orchestrator.config import Config
    view = _chunked_view_config()
    s.config = Config(trino=s.config.trino, views=[view], server=s.config.server)
    return view


class _FakeConn:
    async def cursor(self): return _FakeCursor()
    async def close(self): pass


class _FakeCursor:
    stats = {}
    async def execute(self, sql): pass
    async def fetchone(self): return None
    async def fetchall(self): return []


async def test_refresh_view_dispatches_to_chunked_when_configured(setup_state):
    """When ``view.full_refresh_chunk`` is set, ``refresh_view`` must call
    ``execute_chunked_full_refresh`` (not ``execute_full_refresh``),
    surface ``last_action = "chunked_full"``, and still write
    ``last_source_snapshot`` on completion."""
    from trino_mv_orchestrator import server as server_mod
    from trino_mv_orchestrator.detector import ChangeResult, RefreshAction
    from trino_mv_orchestrator.executor import RefreshResult
    from trino_mv_orchestrator.introspect import ColumnInfo

    view = _install_chunked_view(setup_state)
    chunked_calls: list[str] = []
    full_calls: list[str] = []
    write_calls: list[int] = []

    async def fake_detect(*a, **kw):
        return ChangeResult(action=RefreshAction.FULL_REFRESH, current_snapshot=42)

    async def fake_discover(cursor, query):
        return [ColumnInfo(name="d", type="DATE"), ColumnInfo(name="a", type="VARCHAR")]

    async def fake_chunked(cursor, v, target, parsed, value_columns, **kwargs):
        chunked_calls.append(v.name)
        # chunk_granularity must be forwarded from the view config
        assert kwargs["chunk_granularity"] == "day"
        assert callable(kwargs["should_stop"])
        assert callable(kwargs["on_chunk"])
        return RefreshResult(elapsed=0.1, processed_rows=1)

    async def fake_full(cursor, v, target):
        full_calls.append(v.name)
        return RefreshResult(elapsed=0.1)

    async def fake_write(cursor, target, snap_id):
        write_calls.append(snap_id)

    async def fake_read(cursor, target): return None

    with patch.object(server_mod, "get_trino_connection", lambda s: _FakeConn()), \
         patch.object(server_mod, "discover_columns", fake_discover), \
         patch.object(server_mod, "read_last_snapshot", fake_read), \
         patch.object(server_mod, "write_last_snapshot", fake_write), \
         patch.object(server_mod, "detect_changes", fake_detect), \
         patch.object(server_mod, "execute_chunked_full_refresh", fake_chunked), \
         patch.object(server_mod, "execute_full_refresh", fake_full):
        await server_mod.refresh_view(setup_state, view)

    assert chunked_calls == ["test_view"]
    assert full_calls == []   # single-shot path NOT taken
    assert write_calls == [42]  # last_source_snapshot still written on completion
    vs = setup_state.view_statuses[view.name]
    assert vs.last_action == "chunked_full"
    assert vs.last_error is None


async def test_refresh_view_interrupt_skips_last_snapshot_write(setup_state):
    """When the chunked refresh returns ``interrupted=True``, ``refresh_view``
    must skip ``write_last_snapshot`` (so the next tick resumes from target
    metadata) and must NOT increment ``total_refreshes`` — an interrupt
    is a partial-progress event, not a successful refresh.
    Partial stats (last_refresh, last_duration, recent_queries) are still
    surfaced so the UI shows the work that did complete."""
    from trino_mv_orchestrator import server as server_mod
    from trino_mv_orchestrator.detector import ChangeResult, RefreshAction
    from trino_mv_orchestrator.executor import QueryInfo, RefreshResult
    from trino_mv_orchestrator.introspect import ColumnInfo

    view = _install_chunked_view(setup_state)
    setup_state.view_statuses["test_view"] = ViewStatus(name="test_view", total_refreshes=0)

    write_calls: list[int] = []

    async def fake_detect(*a, **kw):
        return ChangeResult(action=RefreshAction.FULL_REFRESH, current_snapshot=99)

    async def fake_discover(cursor, query):
        return [ColumnInfo(name="d", type="DATE"), ColumnInfo(name="a", type="VARCHAR")]

    async def fake_chunked(cursor, v, target, parsed, value_columns, **kwargs):
        return RefreshResult(
            elapsed=0.25,
            processed_rows=5,
            processed_bytes=128,
            queries=[QueryInfo(
                query_id="q1", info_uri="http://trino/q1",
                stage="chunk_merge", started_at=1.0, elapsed_ms=100.0,
                processed_rows=5, processed_bytes=128,
            )],
            interrupted=True,
        )

    async def fake_write(cursor, target, snap_id):
        write_calls.append(snap_id)

    async def fake_read(cursor, target): return None

    with patch.object(server_mod, "get_trino_connection", lambda s: _FakeConn()), \
         patch.object(server_mod, "discover_columns", fake_discover), \
         patch.object(server_mod, "read_last_snapshot", fake_read), \
         patch.object(server_mod, "write_last_snapshot", fake_write), \
         patch.object(server_mod, "detect_changes", fake_detect), \
         patch.object(server_mod, "execute_chunked_full_refresh", fake_chunked):
        await server_mod.refresh_view(setup_state, view)

    assert write_calls == []   # no last_source_snapshot write on interrupt
    vs = setup_state.view_statuses[view.name]
    assert vs.last_action == "chunked_full"
    assert vs.total_refreshes == 0    # interrupt is not a successful refresh
    # Partial stats surfaced
    assert len(vs.recent_queries) == 1
    assert vs.recent_queries[0].stage == "chunk_merge"
    assert vs.last_duration == 0.25
    assert vs.last_refresh is not None
