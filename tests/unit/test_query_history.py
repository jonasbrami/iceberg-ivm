"""Tests for the SQLite-backed QueryHistory ring buffer."""
from __future__ import annotations

import pytest

from trino_mv_orchestrator.executor import QueryInfo
from trino_mv_orchestrator.query_history import QueryHistory


def _q(qid: str, started_at: float, stage: str = "merge") -> QueryInfo:
    return QueryInfo(
        query_id=qid,
        info_uri=f"http://trino/ui/query.html?{qid}",
        stage=stage,
        started_at=started_at,
        elapsed_ms=100.0,
        processed_rows=1,
        processed_bytes=64,
    )


@pytest.fixture
async def history(tmp_path):
    h = QueryHistory(tmp_path / "state.db", limit=3)
    await h.open()
    yield h
    await h.close()


async def test_append_and_recent_round_trip(history):
    """A QueryInfo inserted via append must come back intact from recent."""
    q = _q("q1", started_at=1.0)
    await history.append("v", [q])

    got = await history.recent("v")
    assert len(got) == 1
    assert got[0] == q


async def test_recent_ordered_newest_first(history):
    await history.append("v", [_q("q1", 1.0), _q("q2", 2.0), _q("q3", 3.0)])

    ids = [q.query_id for q in await history.recent("v")]
    assert ids == ["q3", "q2", "q1"]


async def test_limit_trims_oldest(history):
    """Ring buffer: once limit is exceeded, oldest rows are dropped."""
    for i in range(5):
        await history.append("v", [_q(f"q{i}", float(i))])

    ids = [q.query_id for q in await history.recent("v")]
    assert ids == ["q4", "q3", "q2"]


async def test_views_are_isolated(history):
    await history.append("a", [_q("a1", 1.0)])
    await history.append("b", [_q("b1", 2.0)])

    assert [q.query_id for q in await history.recent("a")] == ["a1"]
    assert [q.query_id for q in await history.recent("b")] == ["b1"]


async def test_delete_view_drops_only_that_view(history):
    await history.append("a", [_q("a1", 1.0), _q("a2", 2.0)])
    await history.append("b", [_q("b1", 3.0)])

    await history.delete_view("a")

    assert await history.recent("a") == []
    assert [q.query_id for q in await history.recent("b")] == ["b1"]


async def test_history_survives_reopen(tmp_path):
    """Close the connection and reopen against the same file — rows persist."""
    path = tmp_path / "state.db"

    h1 = QueryHistory(path, limit=5)
    await h1.open()
    await h1.append("v", [_q("q1", 1.0), _q("q2", 2.0)])
    await h1.close()

    h2 = QueryHistory(path, limit=5)
    await h2.open()
    try:
        ids = [q.query_id for q in await h2.recent("v")]
        assert ids == ["q2", "q1"]
    finally:
        await h2.close()


async def test_append_empty_list_is_noop(history):
    await history.append("v", [])
    assert await history.recent("v") == []


# ── maintenance_state ──


async def test_record_and_read_maintenance(history):
    await history.upsert_maintenance("v", "optimize", {"last_run": 1234.5})
    persisted = await history.all_maintenance("v")
    assert persisted["optimize"]["last_run"] == 1234.5


async def test_upsert_maintenance_overwrites(history):
    """A second record for the same (view, op) overwrites the prior value."""
    await history.upsert_maintenance("v", "optimize", {"last_run": 1.0})
    await history.upsert_maintenance("v", "optimize", {"last_run": 9999.0})
    persisted = await history.all_maintenance("v")
    assert persisted["optimize"]["last_run"] == 9999.0


async def test_all_maintenance_isolated_per_view(history):
    await history.upsert_maintenance("a", "optimize", {"last_run": 1.0})
    await history.upsert_maintenance("b", "optimize", {"last_run": 2.0})
    await history.upsert_maintenance("a", "expire_snapshots", {"last_run": 3.0})
    a = await history.all_maintenance("a")
    b = await history.all_maintenance("b")
    assert {op: row["last_run"] for op, row in a.items()} == {
        "optimize": 1.0, "expire_snapshots": 3.0,
    }
    assert {op: row["last_run"] for op, row in b.items()} == {"optimize": 2.0}


async def test_delete_view_purges_maintenance_state(history):
    await history.upsert_maintenance("a", "optimize", {"last_run": 1.0})
    await history.upsert_maintenance("b", "optimize", {"last_run": 2.0})
    await history.delete_view("a")
    assert await history.all_maintenance("a") == {}
    persisted_b = await history.all_maintenance("b")
    assert persisted_b["optimize"]["last_run"] == 2.0


async def test_maintenance_state_survives_reopen(tmp_path):
    path = tmp_path / "state.db"
    h1 = QueryHistory(path, limit=5)
    await h1.open()
    await h1.upsert_maintenance("v", "optimize", {"last_run": 42.0})
    await h1.close()

    h2 = QueryHistory(path, limit=5)
    await h2.open()
    try:
        persisted = await h2.all_maintenance("v")
        assert persisted["optimize"]["last_run"] == 42.0
    finally:
        await h2.close()


async def test_all_maintenance_returns_full_field_dict(history):
    """``all_maintenance`` exposes every persisted column, not just last_run.

    Surface for issue #40 — UI / hydration need total_runs / total_errors
    / last_error / last_duration too.
    """
    await history.upsert_maintenance("v", "optimize", {
        "last_run": 1234.5,
        "last_duration": 12.5,
        "last_error": None,
        "total_runs": 7,
        "total_errors": 1,
    })
    persisted = await history.all_maintenance("v")
    assert persisted["optimize"] == {
        "last_run": 1234.5,
        "last_duration": 12.5,
        "last_error": None,
        "total_runs": 7,
        "total_errors": 1,
    }


async def test_upsert_maintenance_round_trip_all_fields(history):
    """Upsert + read round-trips every column, including the failure-path fields."""
    await history.upsert_maintenance("v", "expire_snapshots", {
        "last_run": 100.0,
        "last_duration": 0.0,
        "last_error": "boom",
        "total_runs": 3,
        "total_errors": 2,
    })
    persisted = await history.all_maintenance("v")
    assert persisted["expire_snapshots"]["last_error"] == "boom"
    assert persisted["expire_snapshots"]["total_errors"] == 2
    # And it's actually upsert, not insert-only:
    await history.upsert_maintenance("v", "expire_snapshots", {
        "last_run": 200.0,
        "last_duration": 1.5,
        "last_error": None,
        "total_runs": 4,
        "total_errors": 2,
    })
    persisted = await history.all_maintenance("v")
    assert persisted["expire_snapshots"]["last_run"] == 200.0
    assert persisted["expire_snapshots"]["last_error"] is None


async def test_upsert_maintenance_requires_last_run(history):
    """``last_run`` is NOT NULL — calling without one is a programming error."""
    with pytest.raises(ValueError):
        await history.upsert_maintenance("v", "optimize", {"last_duration": 1.0})


# ── view_status ──


async def test_get_view_status_returns_none_for_unknown_view(history):
    assert await history.get_view_status("never_seen") is None


async def test_upsert_and_get_view_status_round_trip(history):
    fields = {
        "last_refresh": 1234.5,
        "last_duration": 2.5,
        "last_action": "incremental",
        "last_range": "[2026-04-23, 2026-04-24)",
        "last_error": None,
        "total_refreshes": 99,
        "total_errors": 1,
        "chunks_done": 68,
        "chunks_total": None,
    }
    await history.upsert_view_status("v", fields)
    got = await history.get_view_status("v")
    assert got == fields


async def test_upsert_view_status_overwrites_prior(history):
    """A second upsert for the same view replaces the earlier value."""
    await history.upsert_view_status("v", {"total_refreshes": 1, "last_action": "full"})
    await history.upsert_view_status("v", {"total_refreshes": 2, "last_action": "skip"})
    got = await history.get_view_status("v")
    assert got["total_refreshes"] == 2
    assert got["last_action"] == "skip"


async def test_upsert_view_status_ignores_unknown_keys(history):
    """``recent_queries`` / ``maintenance`` live in their own tables — passing
    them through dataclasses.asdict() must not break the upsert."""
    await history.upsert_view_status("v", {
        "total_refreshes": 5,
        "recent_queries": [],         # not a column
        "maintenance": {},            # not a column
        "name": "v",                  # not a column
    })
    got = await history.get_view_status("v")
    assert got["total_refreshes"] == 5


async def test_view_status_survives_reopen(tmp_path):
    path = tmp_path / "state.db"
    h1 = QueryHistory(path, limit=5)
    await h1.open()
    await h1.upsert_view_status("v", {
        "total_refreshes": 99, "last_refresh": 1.0, "chunks_done": 42,
    })
    await h1.close()

    h2 = QueryHistory(path, limit=5)
    await h2.open()
    try:
        got = await h2.get_view_status("v")
        assert got["total_refreshes"] == 99
        assert got["chunks_done"] == 42
    finally:
        await h2.close()


async def test_delete_view_purges_view_status(history):
    await history.upsert_view_status("a", {"total_refreshes": 1})
    await history.upsert_view_status("b", {"total_refreshes": 2})
    await history.delete_view("a")
    assert await history.get_view_status("a") is None
    assert (await history.get_view_status("b"))["total_refreshes"] == 2


