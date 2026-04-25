"""Tests for the FastAPI server endpoints."""
import asyncio
import textwrap
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from trino_mv_orchestrator.config import Config, load_config, load_views
from trino_mv_orchestrator.query_history import QueryHistory
from trino_mv_orchestrator.server import RECENT_QUERY_LIMIT, AppState, ViewStatus, app, get_app_state


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
        target_table: iceberg.analytics.test_view
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
        "full_refresh_chunk",
        "optimize_interval_seconds", "optimize_file_size_threshold",
        "expire_snapshots_interval_seconds", "expire_snapshots_retention",
        "remove_orphan_files_interval_seconds", "remove_orphan_files_retention",
    }
    for f in schema:
        assert "label" in f and "type" in f and "required" in f
    # Derived fields must not leak into the create form
    assert "source_table" not in names
    assert "filter_column" not in names
    assert "merge_keys" not in names


def test_view_schema_maintenance_fields_grouped(client):
    """All six maintenance fields share the 'maintenance' group so the UI
    can render them as a dedicated section."""
    schema = client.get("/api/views/schema").json()
    maint = [f for f in schema if f.get("group") == "maintenance"]
    assert {f["name"] for f in maint} == {
        "optimize_interval_seconds", "optimize_file_size_threshold",
        "expire_snapshots_interval_seconds", "expire_snapshots_retention",
        "remove_orphan_files_interval_seconds", "remove_orphan_files_retention",
    }


def test_view_schema_full_refresh_chunk_is_select_with_granularity_options(client):
    """The UI renders full_refresh_chunk as a dropdown — the schema must expose
    ``type: select`` and a granularity allow-list that matches the allow-list
    validated server-side by ``validate_chunk_compatibility``."""
    schema = client.get("/api/views/schema").json()
    field = next(f for f in schema if f["name"] == "full_refresh_chunk")
    assert field["type"] == "select"
    assert field["required"] is False
    option_values = [o["value"] for o in field["options"]]
    # Empty-string option is the "single-shot" sentinel
    assert "" in option_values
    # Each other value must be a valid granularity
    for v in option_values:
        if v == "":
            continue
        assert v in {"hour", "day", "week", "month", "quarter", "year"}


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
        "target_table": "iceberg.analytics.new_view",
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
        "target_table": "iceberg.analytics.bad-name",
        "query": "SELECT date_trunc('day', ts) AS d FROM t GROUP BY 1",
    })
    assert r.status_code == 422


def test_create_duplicate(client):
    r = client.post("/api/views", json={
        "name": "test_view",
        "target_table": "iceberg.analytics.test_view",
        "query": "SELECT date_trunc('day', ts) AS d FROM t GROUP BY 1",
    })
    assert r.status_code == 409


def test_delete_view(client, setup_state):
    assert client.delete("/api/views/test_view").status_code == 204
    assert len(setup_state.config.views) == 0


def test_delete_not_found(client):
    assert client.delete("/api/views/nope").status_code == 404


# ── PUT /api/views/{name} — immutable query/name guard ──

_EXISTING_QUERY = (
    "SELECT date_trunc('day', ts) AS d, a\n"
    "FROM iceberg.db.trades\n"
    "GROUP BY 1, 2\n"
)


def test_update_view_accepts_mutable_field_change(client, setup_state):
    """refresh_interval_seconds is mutable — PUT with the same query must succeed."""
    r = client.put("/api/views/test_view", json={
        "name": "test_view",
        "target_table": "iceberg.analytics.test_view",
        "query": _EXISTING_QUERY,
        "refresh_interval_seconds": 300,
    })
    assert r.status_code == 200, r.text
    assert r.json()["refresh_interval_seconds"] == 300
    updated = next(v for v in setup_state.config.views if v.name == "test_view")
    assert updated.refresh_interval_seconds == 300


def test_update_view_rejects_query_change(client, setup_state):
    """Changing the query would silently orphan materialized rows — must 422."""
    r = client.put("/api/views/test_view", json={
        "name": "test_view",
        "target_table": "iceberg.analytics.test_view",
        "query": "SELECT date_trunc('hour', ts) AS d FROM iceberg.db.trades GROUP BY 1",
    })
    assert r.status_code == 422
    assert "query cannot be changed" in r.text
    # State untouched
    unchanged = next(v for v in setup_state.config.views if v.name == "test_view")
    assert unchanged.query.strip() == _EXISTING_QUERY.strip()


def test_update_view_rejects_name_change(client):
    r = client.put("/api/views/test_view", json={
        "name": "renamed",
        "target_table": "iceberg.analytics.renamed",
        "query": _EXISTING_QUERY,
    })
    assert r.status_code == 422
    assert "name cannot be changed" in r.text


def test_update_view_not_found(client):
    r = client.put("/api/views/nope", json={
        "name": "nope",
        "target_table": "iceberg.analytics.nope",
        "query": _EXISTING_QUERY,
    })
    assert r.status_code == 404


def test_update_view_whitespace_only_query_diff_is_accepted(client):
    """A trailing-newline difference is not a semantic query change."""
    r = client.put("/api/views/test_view", json={
        "name": "test_view",
        "target_table": "iceberg.analytics.test_view",
        "query": _EXISTING_QUERY.strip(),
        "refresh_interval_seconds": 90,
    })
    assert r.status_code == 200, r.text


def test_view_schema_query_field_is_disabled_on_edit(client):
    """The UI must lock the query field when editing — query changes are
    semantically a delete+recreate, not an update."""
    schema = client.get("/api/views/schema").json()
    query_field = next(f for f in schema if f["name"] == "query")
    assert query_field.get("disabled_on_edit") is True


# ── full_refresh_chunk via the REST API (#32) ──

_FULL_REFRESH_QUERY = (
    "SELECT date_trunc('day', ts) AS d, a "
    "FROM iceberg.db.t GROUP BY 1, 2"
)


def test_create_view_accepts_full_refresh_chunk(client, setup_state):
    """POST must accept full_refresh_chunk, persist it, and echo it back."""
    r = client.post("/api/views", json={
        "name": "chunked_view",
        "target_table": "iceberg.analytics.chunked_view",
        "query": _FULL_REFRESH_QUERY,
        "full_refresh_chunk": "day",
    })
    assert r.status_code == 201, r.text
    assert r.json()["full_refresh_chunk"] == "day"
    # Round-trips through ViewConfig (the source of truth for the executor)
    new = next(v for v in setup_state.config.views if v.name == "chunked_view")
    assert new.full_refresh_chunk == "day"
    # GET /api/views surfaces the field too (so the UI can display it)
    listed = next(v for v in client.get("/api/views").json() if v["name"] == "chunked_view")
    assert listed["full_refresh_chunk"] == "day"


def test_create_view_accepts_week_chunk_on_week_view(client, setup_state):
    """Week-granularity views accept week chunks (the strictest row in the
    compatibility matrix: week divides nothing but itself)."""
    r = client.post("/api/views", json={
        "name": "weekly",
        "target_table": "iceberg.analytics.weekly",
        "query": (
            "SELECT date_trunc('week', ts) AS w, a "
            "FROM iceberg.db.t GROUP BY 1, 2"
        ),
        "full_refresh_chunk": "week",
    })
    assert r.status_code == 201, r.text


def test_create_view_rejects_incompatible_chunk(client):
    """Month-granularity view + week chunk is a known-bad combo (weeks do not
    cleanly contain months). The API must reject with 422, not silently accept."""
    r = client.post("/api/views", json={
        "name": "bad_chunk",
        "target_table": "iceberg.analytics.bad_chunk",
        "query": (
            "SELECT date_trunc('month', ts) AS m, a "
            "FROM iceberg.db.t GROUP BY 1, 2"
        ),
        "full_refresh_chunk": "week",
    })
    assert r.status_code == 422
    assert "full_refresh_chunk" in r.text


def test_create_view_rejects_unknown_granularity(client):
    """Freeform strings must not slip past the API — they'd crash the executor
    downstream where walk_buckets assumes a valid granularity."""
    r = client.post("/api/views", json={
        "name": "bad_granularity",
        "target_table": "iceberg.analytics.bad_granularity",
        "query": _FULL_REFRESH_QUERY,
        "full_refresh_chunk": "fortnight",
    })
    assert r.status_code == 422


def test_create_view_empty_string_chunk_treated_as_none(client, setup_state):
    """The UI's select sends "" for the "single-shot" option. The API must
    treat that as equivalent to omitting the field — the stored ViewConfig
    must have None, not "" (otherwise the YAML round-trip and the executor's
    ``if view.full_refresh_chunk:`` check would disagree)."""
    r = client.post("/api/views", json={
        "name": "single_shot",
        "target_table": "iceberg.analytics.single_shot",
        "query": _FULL_REFRESH_QUERY,
        "full_refresh_chunk": "",
    })
    assert r.status_code == 201
    new = next(v for v in setup_state.config.views if v.name == "single_shot")
    assert new.full_refresh_chunk is None


def test_create_view_omits_chunk_defaults_to_none(client, setup_state):
    """Omitting full_refresh_chunk (the common case, pre-#32 clients) is still
    accepted and behaves like a single-shot refresh."""
    r = client.post("/api/views", json={
        "name": "no_chunk",
        "target_table": "iceberg.analytics.no_chunk",
        "query": _FULL_REFRESH_QUERY,
    })
    assert r.status_code == 201
    new = next(v for v in setup_state.config.views if v.name == "no_chunk")
    assert new.full_refresh_chunk is None


# ── Iceberg maintenance via the REST API ──


def test_create_view_accepts_maintenance_fields(client, setup_state):
    r = client.post("/api/views", json={
        "name": "maintained",
        "target_table": "iceberg.analytics.maintained",
        "query": _FULL_REFRESH_QUERY,
        "optimize_interval_seconds": 3600,
        "optimize_file_size_threshold": "128MB",
        "expire_snapshots_interval_seconds": 86400,
        "expire_snapshots_retention": "14d",
        "remove_orphan_files_interval_seconds": 604800,
        "remove_orphan_files_retention": "30d",
    })
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["optimize_interval_seconds"] == 3600
    assert body["optimize_file_size_threshold"] == "128MB"
    assert body["expire_snapshots_retention"] == "14d"
    new = next(v for v in setup_state.config.views if v.name == "maintained")
    assert new.optimize_interval_seconds == 3600
    assert new.optimize_file_size_threshold == "128MB"


def test_create_view_rejects_bad_retention(client):
    r = client.post("/api/views", json={
        "name": "bad_ret",
        "target_table": "iceberg.analytics.bad_ret",
        "query": _FULL_REFRESH_QUERY,
        "expire_snapshots_interval_seconds": 3600,
        "expire_snapshots_retention": "forever",
    })
    assert r.status_code == 422
    assert "duration" in r.text


def test_create_view_rejects_negative_interval(client):
    r = client.post("/api/views", json={
        "name": "bad_iv",
        "target_table": "iceberg.analytics.bad_iv",
        "query": _FULL_REFRESH_QUERY,
        "optimize_interval_seconds": -10,
    })
    assert r.status_code == 422


def test_update_view_applies_maintenance_change(client, setup_state):
    """Maintenance fields are mutable via PUT."""
    r = client.put("/api/views/test_view", json={
        "name": "test_view",
        "target_table": "iceberg.analytics.test_view",
        "query": _EXISTING_QUERY,
        "optimize_interval_seconds": 7200,
    })
    assert r.status_code == 200, r.text
    updated = next(v for v in setup_state.config.views if v.name == "test_view")
    assert updated.optimize_interval_seconds == 7200


def test_create_view_empty_file_size_threshold_treated_as_none(client, setup_state):
    """Empty string from the UI input must round-trip as None in ViewConfig."""
    r = client.post("/api/views", json={
        "name": "no_thr",
        "target_table": "iceberg.analytics.no_thr",
        "query": _FULL_REFRESH_QUERY,
        "optimize_file_size_threshold": "",
    })
    assert r.status_code == 201
    new = next(v for v in setup_state.config.views if v.name == "no_thr")
    assert new.optimize_file_size_threshold is None


async def test_refresh_view_runs_maintenance(setup_state):
    """Piggyback: after a successful refresh, due maintenance ops must run
    on the same cursor and be surfaced in ViewStatus.maintenance."""
    from trino_mv_orchestrator import server as server_mod
    from trino_mv_orchestrator.config import Config, ViewConfig
    from trino_mv_orchestrator.detector import ChangeResult, RefreshAction
    from trino_mv_orchestrator.executor import QueryInfo
    from trino_mv_orchestrator.introspect import ColumnInfo

    v = ViewConfig(
        name="test_view",
        query=_EXISTING_QUERY,
        target_table="iceberg.analytics.test_view",
        optimize_interval_seconds=1,
        expire_snapshots_interval_seconds=1,
        expire_snapshots_retention="7d",
    )
    setup_state.config = Config(
        trino=setup_state.config.trino, views=[v],
        server=setup_state.config.server,
    )
    setup_state.view_statuses["test_view"] = ViewStatus(name="test_view")
    setup_state._stop = False  # fixture seeds True to keep the supervisor quiet

    async def fake_detect(*a, **kw):
        return ChangeResult(action=RefreshAction.FULL_REFRESH, current_snapshot=1)

    async def fake_discover(cursor, query):
        return [ColumnInfo(name="d", type="DATE"), ColumnInfo(name="a", type="VARCHAR")]

    async def fake_execute_refresh(*a, **kw):
        yield QueryInfo(
            query_id="q_merge", info_uri="http://trino/q_merge",
            stage="merge", started_at=1.0, elapsed_ms=100.0,
            processed_rows=1, processed_bytes=10,
            chunks_done=1, chunks_total=1,
        )

    async def fake_write(cursor, target, snap_id): pass
    async def fake_read(cursor, target): return None

    maintenance_calls: list[tuple[str, dict]] = []

    async def fake_maintenance(cursor, target, op, params):
        maintenance_calls.append((op, params))
        return QueryInfo(
            query_id=f"q_{op}", info_uri=f"http://trino/q_{op}",
            stage=f"maintenance_{op}", started_at=1.0, elapsed_ms=5.0,
        )

    with patch.object(server_mod, "get_trino_connection", lambda s: _FakeConn()), \
         patch.object(server_mod, "discover_columns", fake_discover), \
         patch.object(server_mod, "read_last_snapshot", fake_read), \
         patch.object(server_mod, "write_last_snapshot", fake_write), \
         patch.object(server_mod, "detect_changes", fake_detect), \
         patch.object(server_mod, "execute_refresh", fake_execute_refresh), \
         patch.object(server_mod, "execute_maintenance", fake_maintenance):
        await server_mod.refresh_view(setup_state, v)

    # Both configured ops ran exactly once, in declared order, with the
    # right parameter shape.
    assert [op for op, _ in maintenance_calls] == ["optimize", "expire_snapshots"]
    assert dict(maintenance_calls) == {
        "optimize": {},
        "expire_snapshots": {"retention_threshold": "7d"},
    }
    vs = setup_state.view_statuses["test_view"]
    assert vs.maintenance["optimize"].last_run is not None
    assert vs.maintenance["optimize"].total_runs == 1
    assert vs.maintenance["expire_snapshots"].total_runs == 1
    # The maintenance queries appear alongside refresh queries in recent_queries
    stages = {q.stage for q in vs.recent_queries}
    assert "maintenance_optimize" in stages
    assert "maintenance_expire_snapshots" in stages


async def test_maintain_view_respects_interval(setup_state):
    """When last_run is within the interval, the op is skipped — no SQL runs."""
    from trino_mv_orchestrator import server as server_mod
    from trino_mv_orchestrator.config import ViewConfig

    v = ViewConfig(
        name="test_view",
        query=_EXISTING_QUERY,
        target_table="iceberg.analytics.test_view",
        optimize_interval_seconds=3600,  # 1h
    )
    vs = ViewStatus(name="test_view")
    # Just ran 10s ago — 1h interval should gate it off.
    vs.maintenance["optimize"] = server_mod.MaintenanceOpStatus(last_run=__import__("time").time() - 10)
    setup_state.view_statuses["test_view"] = vs

    calls: list[str] = []

    async def fake_maintenance(cursor, target, op, params):
        calls.append(op)
        raise AssertionError("should not have run")

    with patch.object(server_mod, "execute_maintenance", fake_maintenance):
        await server_mod.maintain_view(setup_state, v, cursor=None, target_table="t")

    assert calls == []


async def test_maintain_view_persists_last_run_to_history(setup_state, tmp_path):
    """Scheduling must survive restart — last_run is written to maintenance_state."""
    from trino_mv_orchestrator import server as server_mod
    from trino_mv_orchestrator.config import ViewConfig
    from trino_mv_orchestrator.executor import QueryInfo
    from trino_mv_orchestrator.query_history import QueryHistory

    v = ViewConfig(
        name="test_view",
        query=_EXISTING_QUERY,
        target_table="iceberg.analytics.test_view",
        optimize_interval_seconds=60,
    )
    setup_state.view_statuses["test_view"] = ViewStatus(name="test_view")
    h = QueryHistory(tmp_path / "state.db", limit=RECENT_QUERY_LIMIT)
    await h.open()
    setup_state.history = h
    try:
        async def fake_maintenance(cursor, target, op, params):
            return QueryInfo(
                query_id="q1", info_uri="http://trino/q1",
                stage=f"maintenance_{op}", started_at=1.0, elapsed_ms=5.0,
            )

        with patch.object(server_mod, "execute_maintenance", fake_maintenance):
            await server_mod.maintain_view(setup_state, v, cursor=None, target_table="t")

        persisted = await h.all_maintenance("test_view")
        assert "optimize" in persisted
        assert persisted["optimize"]["last_run"] > 0
    finally:
        await h.close()


async def test_hydrate_rehydrates_maintenance_last_run(setup_state, tmp_path):
    """On startup, maintenance state is read from SQLite so intervals survive restarts."""
    from trino_mv_orchestrator import server as server_mod
    from trino_mv_orchestrator.query_history import QueryHistory

    h = QueryHistory(tmp_path / "state.db", limit=RECENT_QUERY_LIMIT)
    await h.open()
    setup_state.history = h
    try:
        await h.record_maintenance("test_view", "optimize", 12345.0)
        # Fresh ViewStatus, empty maintenance dict — hydrate must populate it.
        setup_state.view_statuses["test_view"] = ViewStatus(name="test_view")
        await server_mod.hydrate_recent_queries(setup_state)
        vs = setup_state.view_statuses["test_view"]
        assert vs.maintenance["optimize"].last_run == 12345.0
    finally:
        await h.close()


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
    from trino_mv_orchestrator.executor import QueryInfo
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

    async def slow_refresh(*a, **kw):
        nonlocal in_flight, max_in_flight, refresh_count
        in_flight += 1
        max_in_flight = max(max_in_flight, in_flight)
        # Yield the event loop so a second caller could interleave if the
        # worker weren't serializing refreshes.
        for _ in range(5):
            await asyncio.sleep(0)
        in_flight -= 1
        refresh_count += 1
        yield QueryInfo(
            query_id="q", info_uri="http://trino/q", stage="merge",
            started_at=1.0, elapsed_ms=1.0, chunks_done=1, chunks_total=1,
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
         patch.object(server_mod, "execute_refresh", slow_refresh):
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
        "    target_table: iceberg.analytics.second_view\n"
    )
    setup_state.views_path.write_text(new_views_yaml)

    reload_config(setup_state)
    assert len(setup_state.config.views) == 2
    assert "second_view" in setup_state.view_statuses


def test_reload_config_no_change(setup_state):
    from trino_mv_orchestrator.server import reload_config

    before = setup_state.config
    reload_config(setup_state)
    assert setup_state.config is before  # unchanged


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
    from trino_mv_orchestrator.executor import QueryInfo
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

    async def fake_execute_refresh(*a, **kw):
        yield QueryInfo(
            query_id="20260417_000000_00001_xyz",
            info_uri="http://trino/ui/query.html?20260417_000000_00001_xyz",
            stage="merge", started_at=1.0, elapsed_ms=380.0,
            processed_rows=10, processed_bytes=2048,
            chunks_done=1, chunks_total=1,
        )

    with patch.object(server_mod, "get_trino_connection", lambda s: FakeConn()), \
         patch.object(server_mod, "discover_columns", fake_discover_columns), \
         patch.object(server_mod, "read_last_snapshot", fake_read), \
         patch.object(server_mod, "write_last_snapshot", fake_write), \
         patch.object(server_mod, "detect_changes", fake_detect), \
         patch.object(server_mod, "execute_refresh", fake_execute_refresh):
        await server_mod.refresh_view(setup_state, view)

    vs = setup_state.view_statuses[view.name]
    assert len(vs.recent_queries) == 1
    assert vs.recent_queries[0].stage == "merge"

    body = client.get("/api/views").json()[0]
    status_queries = body["status"]["recent_queries"]
    assert len(status_queries) == 1
    assert status_queries[0]["info_uri"].endswith("20260417_000000_00001_xyz")


async def test_refresh_persists_queries_and_hydrates_on_restart(
    setup_state, tmp_path,
):
    """End-to-end: attaching a QueryHistory to AppState must cause refresh_view
    to persist QueryInfo rows, and a fresh ViewStatus with an empty
    recent_queries must re-hydrate from the DB via hydrate_recent_queries."""
    from trino_mv_orchestrator import server as server_mod
    from trino_mv_orchestrator.detector import ChangeResult, RefreshAction
    from trino_mv_orchestrator.executor import QueryInfo
    from trino_mv_orchestrator.introspect import ColumnInfo

    view = setup_state.config.views[0]

    h = QueryHistory(tmp_path / "state.db", limit=RECENT_QUERY_LIMIT)
    await h.open()
    setup_state.history = h

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
    async def fake_detect(*a, **kw):
        return ChangeResult(action=RefreshAction.FULL_REFRESH, current_snapshot=1)
    async def fake_discover(cursor, query):
        return [ColumnInfo(name="d", type="DATE"), ColumnInfo(name="a", type="VARCHAR")]
    async def fake_execute_refresh(*a, **kw):
        yield QueryInfo(
            query_id="persisted_qid", info_uri="http://trino/persisted_qid",
            stage="merge", started_at=10.0, elapsed_ms=250.0,
            processed_rows=1, processed_bytes=64, chunks_done=1, chunks_total=1,
        )

    try:
        with patch.object(server_mod, "get_trino_connection", lambda s: FakeConn()), \
             patch.object(server_mod, "discover_columns", fake_discover), \
             patch.object(server_mod, "read_last_snapshot", fake_read), \
             patch.object(server_mod, "write_last_snapshot", fake_write), \
             patch.object(server_mod, "detect_changes", fake_detect), \
             patch.object(server_mod, "execute_refresh", fake_execute_refresh):
            await server_mod.refresh_view(setup_state, view)

        vs = setup_state.view_statuses[view.name]
        assert [q.query_id for q in vs.recent_queries] == ["persisted_qid"]

        # Simulate a restart: ViewStatus is fresh, DB survives.
        setup_state.view_statuses[view.name] = ViewStatus(name=view.name)
        await server_mod.hydrate_recent_queries(setup_state)
        rehydrated = setup_state.view_statuses[view.name].recent_queries
        assert [q.query_id for q in rehydrated] == ["persisted_qid"]
    finally:
        await h.close()


async def test_delete_view_purges_history(setup_state, tmp_path, client):
    from trino_mv_orchestrator.executor import QueryInfo

    h = QueryHistory(tmp_path / "state.db", limit=RECENT_QUERY_LIMIT)
    await h.open()
    setup_state.history = h
    try:
        await h.append("test_view", [QueryInfo(
            query_id="x", info_uri="http://trino/x", stage="merge",
            started_at=1.0, elapsed_ms=1.0,
        )])
        await h.upsert_view_status("test_view", {"total_refreshes": 9})
        assert len(await h.recent("test_view")) == 1
        assert await h.get_view_status("test_view") is not None

        r = client.delete("/api/views/test_view")
        assert r.status_code == 204
        assert await h.recent("test_view") == []
        assert await h.get_view_status("test_view") is None
    finally:
        await h.close()


# ── Issue #40: ViewStatus counters survive restart ──


async def test_refresh_persists_view_status_counters(setup_state, tmp_path):
    """After a refresh, ``view_status`` must hold the new counters so a
    restart re-hydrates them (the bug from issue #40 was that ``total_refreshes``,
    ``last_refresh``, ``chunks_done`` etc. all reset to zero on every restart)."""
    from trino_mv_orchestrator import server as server_mod
    from trino_mv_orchestrator.detector import ChangeResult, RefreshAction
    from trino_mv_orchestrator.executor import QueryInfo
    from trino_mv_orchestrator.introspect import ColumnInfo

    view = setup_state.config.views[0]
    setup_state._stop = False  # fixture defaults to True; refresh_view bails on stop

    h = QueryHistory(tmp_path / "state.db", limit=RECENT_QUERY_LIMIT)
    await h.open()
    setup_state.history = h

    class FakeConn:
        async def cursor(self): return FakeCursor()
        async def close(self): pass

    class FakeCursor:
        stats = {}
        async def execute(self, sql): pass
        async def fetchone(self): return None
        async def fetchall(self): return []

    async def fake_write(c, t, snap_id): pass
    async def fake_read(c, t): return None
    async def fake_detect(*a, **kw):
        return ChangeResult(action=RefreshAction.FULL_REFRESH, current_snapshot=1)
    async def fake_discover(c, q):
        return [ColumnInfo(name="d", type="DATE"), ColumnInfo(name="a", type="VARCHAR")]
    async def fake_execute_refresh(*a, **kw):
        yield QueryInfo(
            query_id="q1", info_uri="http://trino/q1",
            stage="merge", started_at=10.0, elapsed_ms=250.0,
            processed_rows=1, processed_bytes=64, chunks_done=1, chunks_total=1,
        )

    try:
        with patch.object(server_mod, "get_trino_connection", lambda s: FakeConn()), \
             patch.object(server_mod, "discover_columns", fake_discover), \
             patch.object(server_mod, "read_last_snapshot", fake_read), \
             patch.object(server_mod, "write_last_snapshot", fake_write), \
             patch.object(server_mod, "detect_changes", fake_detect), \
             patch.object(server_mod, "execute_refresh", fake_execute_refresh):
            await server_mod.refresh_view(setup_state, view)

        persisted = await h.get_view_status(view.name)
        assert persisted is not None
        assert persisted["total_refreshes"] == 4   # fixture seeded 3 + this run
        assert persisted["last_action"] == "full"
        assert persisted["last_refresh"] is not None
        assert persisted["last_duration"] == pytest.approx(0.25)
        assert persisted["chunks_total"] is None
    finally:
        await h.close()


async def test_hydrate_view_state_restores_persisted_counters(setup_state, tmp_path):
    """A fresh ViewStatus (counters at zero) must adopt the persisted
    snapshot — that's the user-visible fix for issue #40."""
    from trino_mv_orchestrator import server as server_mod

    h = QueryHistory(tmp_path / "state.db", limit=RECENT_QUERY_LIMIT)
    await h.open()
    setup_state.history = h
    try:
        await h.upsert_view_status("test_view", {
            "last_refresh": 1776916823.87,
            "last_duration": 2.76,
            "last_action": "skip",
            "last_range": "[2026-04-23 03:50:00, 2026-04-23 04:01:00)",
            "last_error": None,
            "total_refreshes": 99,
            "total_errors": 0,
            "chunks_done": 68,
            "chunks_total": 68,
        })

        # Simulate a restart: ViewStatus is fresh, DB survives.
        setup_state.view_statuses["test_view"] = ViewStatus(name="test_view")
        await server_mod.hydrate_view_state(setup_state)

        vs = setup_state.view_statuses["test_view"]
        assert vs.total_refreshes == 99
        assert vs.last_refresh == 1776916823.87
        assert vs.last_duration == 2.76
        assert vs.last_action == "skip"
        assert vs.last_range == "[2026-04-23 03:50:00, 2026-04-23 04:01:00)"
        assert vs.chunks_done == 68
        assert vs.chunks_total == 68
    finally:
        await h.close()


async def test_refresh_then_restart_round_trips_total_refreshes(setup_state, tmp_path):
    """End-to-end: refresh once, simulate restart (drop ViewStatus, re-hydrate),
    and ``total_refreshes`` survives. This is the headline fix for issue #40."""
    from trino_mv_orchestrator import server as server_mod
    from trino_mv_orchestrator.detector import ChangeResult, RefreshAction
    from trino_mv_orchestrator.executor import QueryInfo
    from trino_mv_orchestrator.introspect import ColumnInfo

    view = setup_state.config.views[0]
    setup_state.view_statuses[view.name] = ViewStatus(name=view.name)
    setup_state._stop = False  # fixture defaults to True; refresh_view bails on stop

    h = QueryHistory(tmp_path / "state.db", limit=RECENT_QUERY_LIMIT)
    await h.open()
    setup_state.history = h

    class FakeConn:
        async def cursor(self): return FakeCursor()
        async def close(self): pass

    class FakeCursor:
        stats = {}
        async def execute(self, sql): pass
        async def fetchone(self): return None
        async def fetchall(self): return []

    async def fake_write(c, t, snap_id): pass
    async def fake_read(c, t): return None
    async def fake_detect(*a, **kw):
        return ChangeResult(action=RefreshAction.FULL_REFRESH, current_snapshot=1)
    async def fake_discover(c, q):
        return [ColumnInfo(name="d", type="DATE"), ColumnInfo(name="a", type="VARCHAR")]
    async def fake_execute_refresh(*a, **kw):
        yield QueryInfo(
            query_id="q1", info_uri="http://trino/q1",
            stage="merge", started_at=1.0, elapsed_ms=10.0,
            processed_rows=1, processed_bytes=8, chunks_done=1, chunks_total=1,
        )

    try:
        with patch.object(server_mod, "get_trino_connection", lambda s: FakeConn()), \
             patch.object(server_mod, "discover_columns", fake_discover), \
             patch.object(server_mod, "read_last_snapshot", fake_read), \
             patch.object(server_mod, "write_last_snapshot", fake_write), \
             patch.object(server_mod, "detect_changes", fake_detect), \
             patch.object(server_mod, "execute_refresh", fake_execute_refresh):
            await server_mod.refresh_view(setup_state, view)

        assert setup_state.view_statuses[view.name].total_refreshes == 1

        # Simulate a process restart: drop the in-memory status, re-hydrate.
        setup_state.view_statuses[view.name] = ViewStatus(name=view.name)
        await server_mod.hydrate_view_state(setup_state)
        assert setup_state.view_statuses[view.name].total_refreshes == 1
        assert setup_state.view_statuses[view.name].last_action == "full"
    finally:
        await h.close()


async def test_refresh_failure_persists_last_error(setup_state, tmp_path):
    """A refresh that raises must persist ``last_error`` / ``total_errors``
    so the UI doesn't lose the failure on restart (issue #40 explicitly
    flags this as desirable)."""
    from trino_mv_orchestrator import server as server_mod
    from trino_mv_orchestrator.detector import ChangeResult, RefreshAction
    from trino_mv_orchestrator.introspect import ColumnInfo

    view = setup_state.config.views[0]
    setup_state.view_statuses[view.name] = ViewStatus(name=view.name)
    h = QueryHistory(tmp_path / "state.db", limit=RECENT_QUERY_LIMIT)
    await h.open()
    setup_state.history = h

    class FakeConn:
        async def cursor(self): return FakeCursor()
        async def close(self): pass

    class FakeCursor:
        stats = {}
        async def execute(self, sql): pass
        async def fetchone(self): return None
        async def fetchall(self): return []

    async def fake_read(c, t): return None
    async def fake_detect(*a, **kw):
        return ChangeResult(action=RefreshAction.FULL_REFRESH, current_snapshot=1)
    async def fake_discover(c, q):
        return [ColumnInfo(name="d", type="DATE"), ColumnInfo(name="a", type="VARCHAR")]
    async def fake_execute_refresh(*a, **kw):
        raise RuntimeError("trino exploded")
        yield  # pragma: no cover (make it a generator)

    try:
        with patch.object(server_mod, "get_trino_connection", lambda s: FakeConn()), \
             patch.object(server_mod, "discover_columns", fake_discover), \
             patch.object(server_mod, "read_last_snapshot", fake_read), \
             patch.object(server_mod, "detect_changes", fake_detect), \
             patch.object(server_mod, "execute_refresh", fake_execute_refresh):
            await server_mod.refresh_view(setup_state, view)

        persisted = await h.get_view_status(view.name)
        assert persisted is not None
        assert persisted["last_error"] == "trino exploded"
        assert persisted["total_errors"] == 1
    finally:
        await h.close()


async def test_post_restart_no_change_clears_chunks_total(setup_state, tmp_path):
    """A NO_CHANGE tick after a restart that hydrated mid-backfill state
    must clear ``chunks_total``. Otherwise the UI keeps showing a phantom
    backfill in flight after the source caught up."""
    from trino_mv_orchestrator import server as server_mod
    from trino_mv_orchestrator.detector import ChangeResult, RefreshAction
    from trino_mv_orchestrator.introspect import ColumnInfo

    view = setup_state.config.views[0]
    h = QueryHistory(tmp_path / "state.db", limit=RECENT_QUERY_LIMIT)
    await h.open()
    setup_state.history = h
    try:
        # Pretend the previous process was killed mid-backfill.
        await h.upsert_view_status(view.name, {
            "last_action": "chunked_full",
            "chunks_done": 5,
            "chunks_total": 10,
            "total_refreshes": 0,
        })
        setup_state.view_statuses[view.name] = ViewStatus(name=view.name)
        await server_mod.hydrate_view_state(setup_state)
        assert setup_state.view_statuses[view.name].chunks_total == 10

        class FakeConn:
            async def cursor(self): return FakeCursor()
            async def close(self): pass

        class FakeCursor:
            stats = {}
            async def execute(self, sql): pass
            async def fetchone(self): return None
            async def fetchall(self): return []

        async def fake_read(c, t): return 1
        async def fake_detect(*a, **kw):
            return ChangeResult(action=RefreshAction.NO_CHANGE, current_snapshot=1)
        async def fake_discover(c, q):
            return [ColumnInfo(name="d", type="DATE"), ColumnInfo(name="a", type="VARCHAR")]

        with patch.object(server_mod, "get_trino_connection", lambda s: FakeConn()), \
             patch.object(server_mod, "discover_columns", fake_discover), \
             patch.object(server_mod, "read_last_snapshot", fake_read), \
             patch.object(server_mod, "detect_changes", fake_detect):
            await server_mod.refresh_view(setup_state, view)

        vs = setup_state.view_statuses[view.name]
        assert vs.last_action == "skip"
        assert vs.chunks_total is None, (
            "stale chunks_total from a mid-backfill restart was not cleared"
        )
    finally:
        await h.close()


async def test_maintenance_persists_full_field_dict(setup_state, tmp_path):
    """``maintain_view`` must persist every MaintenanceOpStatus field, not
    just last_run — so total_runs / last_duration etc. survive restart."""
    from trino_mv_orchestrator import server as server_mod
    from trino_mv_orchestrator.config import ViewConfig
    from trino_mv_orchestrator.executor import QueryInfo

    v = ViewConfig(
        name="test_view",
        query=_EXISTING_QUERY,
        target_table="iceberg.analytics.test_view",
        optimize_interval_seconds=60,
    )
    setup_state.view_statuses["test_view"] = ViewStatus(name="test_view")
    h = QueryHistory(tmp_path / "state.db", limit=RECENT_QUERY_LIMIT)
    await h.open()
    setup_state.history = h
    try:
        async def fake_maintenance(cursor, target, op, params):
            return QueryInfo(
                query_id="q1", info_uri="http://trino/q1",
                stage=f"maintenance_{op}", started_at=1.0, elapsed_ms=42.0,
            )

        with patch.object(server_mod, "execute_maintenance", fake_maintenance):
            await server_mod.maintain_view(setup_state, v, cursor=None, target_table="t")

        persisted = await h.all_maintenance("test_view")
        assert "optimize" in persisted
        op = persisted["optimize"]
        assert op["last_run"] > 0
        assert op["last_duration"] == pytest.approx(0.042)
        assert op["total_runs"] == 1
        assert op["total_errors"] == 0
        assert op["last_error"] is None
    finally:
        await h.close()


async def test_hydrate_restores_full_maintenance_state(setup_state, tmp_path):
    """Hydration must rebuild every MaintenanceOpStatus field, not just last_run."""
    from trino_mv_orchestrator import server as server_mod

    h = QueryHistory(tmp_path / "state.db", limit=RECENT_QUERY_LIMIT)
    await h.open()
    setup_state.history = h
    try:
        await h.upsert_maintenance("test_view", "optimize", {
            "last_run": 12345.0,
            "last_duration": 5.0,
            "last_error": "some failure",
            "total_runs": 4,
            "total_errors": 1,
        })
        setup_state.view_statuses["test_view"] = ViewStatus(name="test_view")
        await server_mod.hydrate_view_state(setup_state)
        ms = setup_state.view_statuses["test_view"].maintenance["optimize"]
        assert ms.last_run == 12345.0
        assert ms.last_duration == 5.0
        assert ms.last_error == "some failure"
        assert ms.total_runs == 4
        assert ms.total_errors == 1
    finally:
        await h.close()


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
        "target_table": "iceberg.analytics.fail_view",
        "query": "SELECT ts FROM t GROUP BY 1",
    })
    assert r.status_code == 422
    assert "date_trunc" in r.json()["detail"]


def test_create_view_fails_on_arithmetic(client, setup_state):
    r = client.post("/api/views", json={
        "name": "arith_view",
        "target_table": "iceberg.analytics.arith_view",
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
        "target_table": "iceberg.analytics.legacy_view",
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


async def test_refresh_view_chunked_backfill_completes(setup_state):
    """When ``full_refresh_chunk`` is set and detection says FULL_REFRESH, the
    worker must surface ``last_action="chunked_full"``, iterate chunks from
    the executor, and write ``last_source_snapshot`` on clean completion."""
    from datetime import datetime, timezone

    from trino_mv_orchestrator import server as server_mod
    from trino_mv_orchestrator.detector import ChangeResult, RefreshAction
    from trino_mv_orchestrator.executor import QueryInfo
    from trino_mv_orchestrator.introspect import ColumnInfo

    view = _install_chunked_view(setup_state)
    setup_state.view_statuses["test_view"] = ViewStatus(name="test_view", total_refreshes=0)
    setup_state._stop = False
    write_calls: list[int] = []

    async def fake_detect(*a, **kw):
        return ChangeResult(action=RefreshAction.FULL_REFRESH, current_snapshot=42)

    async def fake_discover(cursor, query):
        return [ColumnInfo(name="d", type="DATE"), ColumnInfo(name="a", type="VARCHAR")]

    async def fake_execute_refresh(*a, **kw):
        for i in range(1, 4):
            yield QueryInfo(
                query_id=f"q{i}", info_uri=f"http://trino/q{i}",
                stage="chunk_merge", started_at=float(i),
                elapsed_ms=100.0 * i, processed_rows=i, processed_bytes=10 * i,
                range_start=datetime(2026, 4, 7 + i, tzinfo=timezone.utc),
                range_end=datetime(2026, 4, 8 + i, tzinfo=timezone.utc),
                chunks_done=i, chunks_total=3,
            )

    async def fake_write(cursor, target, snap_id):
        write_calls.append(snap_id)

    async def fake_read(cursor, target): return None

    with patch.object(server_mod, "get_trino_connection", lambda s: _FakeConn()), \
         patch.object(server_mod, "discover_columns", fake_discover), \
         patch.object(server_mod, "read_last_snapshot", fake_read), \
         patch.object(server_mod, "write_last_snapshot", fake_write), \
         patch.object(server_mod, "detect_changes", fake_detect), \
         patch.object(server_mod, "execute_refresh", fake_execute_refresh):
        await server_mod.refresh_view(setup_state, view)

    assert write_calls == [42]  # snapshot written on clean completion
    vs = setup_state.view_statuses[view.name]
    assert vs.last_action == "chunked_full"
    assert vs.chunks_done == 3
    assert vs.chunks_total is None  # cleared on clean completion
    assert vs.total_refreshes == 1
    # Three chunks appended newest-first via the history ring buffer.
    assert [q.query_id for q in vs.recent_queries] == ["q3", "q2", "q1"]


async def test_refresh_view_interrupt_skips_last_snapshot_write(setup_state):
    """When ``s._stop`` trips between chunks, ``refresh_view`` breaks out of
    the async-for without writing ``last_source_snapshot`` (next tick resumes
    from target metadata) and without incrementing ``total_refreshes``.
    Partial chunks already appended to recent_queries are preserved."""
    from datetime import datetime, timezone

    from trino_mv_orchestrator import server as server_mod
    from trino_mv_orchestrator.detector import ChangeResult, RefreshAction
    from trino_mv_orchestrator.executor import QueryInfo
    from trino_mv_orchestrator.introspect import ColumnInfo

    view = _install_chunked_view(setup_state)
    setup_state.view_statuses["test_view"] = ViewStatus(name="test_view", total_refreshes=0)
    write_calls: list[int] = []

    async def fake_detect(*a, **kw):
        return ChangeResult(action=RefreshAction.FULL_REFRESH, current_snapshot=99)

    async def fake_discover(cursor, query):
        return [ColumnInfo(name="d", type="DATE"), ColumnInfo(name="a", type="VARCHAR")]

    async def fake_execute_refresh(*a, **kw):
        # Yield one chunk, then the refresh_view sets _stop before the next.
        yield QueryInfo(
            query_id="q1", info_uri="http://trino/q1",
            stage="chunk_merge", started_at=1.0, elapsed_ms=250.0,
            processed_rows=5, processed_bytes=128,
            range_start=datetime(2026, 4, 8, tzinfo=timezone.utc),
            range_end=datetime(2026, 4, 9, tzinfo=timezone.utc),
            chunks_done=1, chunks_total=3,
        )
        setup_state._stop = True
        yield QueryInfo(
            query_id="q2", info_uri="http://trino/q2", stage="chunk_merge",
            started_at=2.0, elapsed_ms=100.0, chunks_done=2, chunks_total=3,
        )

    async def fake_write(cursor, target, snap_id):
        write_calls.append(snap_id)

    async def fake_read(cursor, target): return None

    with patch.object(server_mod, "get_trino_connection", lambda s: _FakeConn()), \
         patch.object(server_mod, "discover_columns", fake_discover), \
         patch.object(server_mod, "read_last_snapshot", fake_read), \
         patch.object(server_mod, "write_last_snapshot", fake_write), \
         patch.object(server_mod, "detect_changes", fake_detect), \
         patch.object(server_mod, "execute_refresh", fake_execute_refresh):
        await server_mod.refresh_view(setup_state, view)

    assert write_calls == []   # interrupt: no snapshot bookmark write
    vs = setup_state.view_statuses[view.name]
    assert vs.last_action == "chunked_full"
    assert vs.total_refreshes == 0
    # First chunk made it into recent_queries before the stop flag tripped.
    assert any(q.query_id == "q1" for q in vs.recent_queries)
    assert vs.last_duration == pytest.approx(0.25)
    assert vs.chunks_done == 1
    assert vs.chunks_total == 3


def test_chunk_metrics_defined():
    """Per-chunk Prometheus metrics are registered so operators can build
    chunked-backfill dashboards without scraping stdout."""
    from trino_mv_orchestrator import server as server_mod
    assert hasattr(server_mod, "CHUNKS_COMPLETED")
    assert hasattr(server_mod, "CHUNK_DURATION")
    assert hasattr(server_mod, "CHUNK_ROWS")
    # Show up on the /metrics endpoint (they're lazy but prometheus_client
    # registers them eagerly).
    from fastapi.testclient import TestClient
    text = TestClient(server_mod.app, raise_server_exceptions=False).get("/metrics").text
    assert "mv_chunks_completed_total" in text
    assert "mv_chunk_duration_seconds" in text
    assert "mv_chunk_rows_written_total" in text
