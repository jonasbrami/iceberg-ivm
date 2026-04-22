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
