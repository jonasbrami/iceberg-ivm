"""Tests for config loading and validation.

Query-parsing tests live in test_query_parser.py; these cover YAML loading,
identifier validation, and save/load round-tripping.
"""

import textwrap
from pathlib import Path

import pytest

from iceberg_ivm.config import (
    ViewConfig,
    load_config,
    load_views,
    save_views,
    validate_maintenance_config,
)


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
  catalog: iceberg
  schema: analytics
"""


# All load_config tests use this fixture to populate the credential env
# vars; load_config refuses to start without them.
@pytest.fixture(autouse=True)
def trino_env(monkeypatch):
    monkeypatch.setenv("TRINO_URL", "http://localhost:8080")
    monkeypatch.setenv("TRINO_USER", "test")
    monkeypatch.setenv("TRINO_PASSWORD", "hunter2")


VALID_VIEWS = """\
views:
  - name: ohlcv_1m
    query: |
      SELECT symbol,
             date_trunc('minute', ts) AS minute,
             sum(qty) AS volume
      FROM iceberg.market_data.trades
      GROUP BY 1, 2
    target_table: iceberg.analytics.ohlcv_1m
    refresh_interval_seconds: 30
"""


# ── load_config (static config only) ──


def test_load_valid(tmp_path):
    cfg = load_config(write_config(tmp_path, STATIC_CONFIG))
    assert cfg.trino.url == "http://localhost:8080"
    assert cfg.trino.user == "test"
    assert cfg.trino.password == "hunter2"
    assert cfg.trino.catalog == "iceberg"
    assert cfg.trino.schema == "analytics"
    assert cfg.views == []


def test_missing_trino(tmp_path):
    with pytest.raises(ValueError, match="missing 'trino'"):
        load_config(write_config(tmp_path, "server:\n  port: 8000\n"))


def test_defaults(tmp_path):
    cfg = load_config(write_config(tmp_path, STATIC_CONFIG))
    assert cfg.views == []
    assert cfg.server.port == 8000


@pytest.mark.parametrize("var", ["TRINO_URL", "TRINO_USER"])
def test_missing_required_env_var_raises(tmp_path, monkeypatch, var):
    """TRINO_URL and TRINO_USER are required — no defaults."""
    monkeypatch.delenv(var)
    with pytest.raises(ValueError, match=var):
        load_config(write_config(tmp_path, STATIC_CONFIG))


def test_missing_password_is_allowed(tmp_path, monkeypatch):
    """TRINO_PASSWORD is optional — missing => no auth / anonymous
    connection (for clusters that don't require basic auth, e.g.
    the local dev compose stack)."""
    monkeypatch.delenv("TRINO_PASSWORD")
    cfg = load_config(write_config(tmp_path, STATIC_CONFIG))
    assert cfg.trino.password is None
    assert cfg.trino.user == "test"


def test_empty_password_treated_as_missing(tmp_path, monkeypatch):
    """TRINO_PASSWORD='' (empty string) collapses to None so it
    doesn't produce BasicAuthentication with an empty password."""
    monkeypatch.setenv("TRINO_PASSWORD", "")
    cfg = load_config(write_config(tmp_path, STATIC_CONFIG))
    assert cfg.trino.password is None


def test_yaml_host_port_user_are_not_accepted(tmp_path):
    """Host/port/user in YAML must not silently override the env vars;
    credentials only come from env.  Any leftover field in the YAML is
    simply ignored, but the loader does not read it."""
    yaml = "trino:\n  host: wrong-host\n  port: 9999\n  user: wrong-user\n  catalog: iceberg\n  schema: analytics\n"
    cfg = load_config(write_config(tmp_path, yaml))
    # The env var wins; YAML host/port/user are ignored
    assert cfg.trino.url == "http://localhost:8080"
    assert cfg.trino.user == "test"


# ── load_views ──


def test_load_views_valid(tmp_path):
    views = load_views(write_views(tmp_path, VALID_VIEWS))
    assert len(views) == 1
    v = views[0]
    assert v.name == "ohlcv_1m"
    assert "date_trunc" in v.query
    assert v.refresh_interval_seconds == 30


def test_load_views_missing_file(tmp_path):
    assert load_views(tmp_path / "nonexistent.yaml") == []


def test_view_defaults(tmp_path):
    minimal = (
        "views:\n"
        "  - name: v\n"
        "    query: \"SELECT date_trunc('day', ts) AS d FROM t GROUP BY 1\"\n"
        "    target_table: iceberg.analytics.v\n"
    )
    views = load_views(write_views(tmp_path, minimal))
    assert views[0].refresh_interval_seconds == 60


def test_missing_query(tmp_path):
    bad = VALID_VIEWS.replace(
        "    query: |\n      SELECT symbol,\n             date_trunc('minute', ts) AS minute,\n             sum(qty) AS volume\n      FROM iceberg.market_data.trades\n      GROUP BY 1, 2\n",
        "",
    )
    with pytest.raises(ValueError, match="missing required fields"):
        load_views(write_views(tmp_path, bad))


def test_missing_name(tmp_path):
    bad = VALID_VIEWS.replace("  - name: ohlcv_1m\n", "  - \n")
    with pytest.raises(ValueError, match="missing required fields"):
        load_views(write_views(tmp_path, bad))


def test_invalid_view_name(tmp_path):
    bad = VALID_VIEWS.replace("name: ohlcv_1m", "name: drop-table")
    with pytest.raises(ValueError, match="valid view name"):
        load_views(write_views(tmp_path, bad))


def test_view_name_can_be_qualified_fqdn(tmp_path):
    """A name with dots (e.g. matching the target_table FQDN) is accepted —
    enables the API/UI default of name = target_table."""
    fqdn = VALID_VIEWS.replace("name: ohlcv_1m", "name: iceberg.analytics.ohlcv_1m")
    views = load_views(write_views(tmp_path, fqdn))
    assert views[0].name == "iceberg.analytics.ohlcv_1m"


def test_invalid_target_table(tmp_path):
    bad = VALID_VIEWS.replace(
        "    refresh_interval_seconds: 30\n",
        "    target_table: x;DROP\n",
    )
    with pytest.raises(ValueError, match="valid qualified table name"):
        load_views(write_views(tmp_path, bad))


def test_legacy_range_filter_placeholder_rejected(tmp_path):
    """{range_filter} is no longer supported — old views.yaml files must be migrated."""
    legacy = (
        "views:\n  - name: v\n"
        "    query: \"SELECT date_trunc('day', ts) AS d FROM t WHERE {range_filter} GROUP BY 1\"\n"
        "    target_table: iceberg.analytics.v\n"
    )
    with pytest.raises(ValueError, match="range_filter"):
        load_views(write_views(tmp_path, legacy))


def test_duplicate_view_names_rejected(tmp_path):
    views_yaml = (
        "views:\n"
        "  - name: v\n"
        "    query: \"SELECT date_trunc('day', ts) AS d FROM t GROUP BY 1\"\n"
        "    target_table: iceberg.analytics.v\n"
        "  - name: v\n"
        "    query: \"SELECT date_trunc('day', ts) AS d FROM t GROUP BY 1\"\n"
        "    target_table: iceberg.analytics.v2\n"
    )
    with pytest.raises(ValueError, match="duplicate"):
        load_views(write_views(tmp_path, views_yaml))


# ── save_views / load_views round-trip ──


def test_save_views_and_reload(tmp_path):
    views = [
        ViewConfig(
            name="ohlcv_1m",
            query="SELECT date_trunc('day', ts) AS d FROM t GROUP BY 1",
            target_table="iceberg.analytics.ohlcv_1m",
            refresh_interval_seconds=30,
        )
    ]
    views_path = tmp_path / "views.yaml"
    save_views(views, views_path)
    loaded = load_views(views_path)
    assert loaded[0].name == views[0].name
    assert loaded[0].query.strip() == views[0].query.strip()
    assert loaded[0].refresh_interval_seconds == 30


def test_save_views_creates_parent_dirs(tmp_path):
    views_path = tmp_path / "data" / "views.yaml"
    save_views([], views_path)
    assert views_path.exists()


# ── full_refresh_chunk ──


def _views_yaml_with_chunk(view_granularity: str, chunk: str | None) -> str:
    """Build a minimal views.yaml with `date_trunc(view_granularity, ts)` and
    optional full_refresh_chunk."""
    lines = [
        "views:",
        "  - name: v",
        f"    query: \"SELECT date_trunc('{view_granularity}', ts) AS d FROM t GROUP BY 1\"",
        "    target_table: iceberg.analytics.v",
    ]
    if chunk is not None:
        lines.append(f"    full_refresh_chunk: {chunk}")
    return "\n".join(lines) + "\n"


def test_full_refresh_chunk_defaults_to_none(tmp_path):
    views = load_views(write_views(tmp_path, _views_yaml_with_chunk("day", None)))
    assert views[0].full_refresh_chunk is None


def test_full_refresh_chunk_valid(tmp_path):
    views = load_views(write_views(tmp_path, _views_yaml_with_chunk("minute", "day")))
    assert views[0].full_refresh_chunk == "day"


def test_full_refresh_chunk_rejects_invalid_granularity(tmp_path):
    with pytest.raises(ValueError, match="not a valid chunk granularity"):
        load_views(write_views(tmp_path, _views_yaml_with_chunk("day", "fortnight")))


@pytest.mark.parametrize(
    "view_g, chunk_g",
    [
        # chunk finer than view — would split GROUP BY buckets
        ("day", "hour"),
        ("hour", "minute"),
        ("month", "day"),
        # week does not divide month, month does not divide week
        ("week", "month"),
        ("month", "week"),
        ("week", "quarter"),
        ("quarter", "week"),
    ],
)
def test_full_refresh_chunk_rejects_incompatible(tmp_path, view_g, chunk_g):
    with pytest.raises(ValueError, match="not compatible"):
        load_views(write_views(tmp_path, _views_yaml_with_chunk(view_g, chunk_g)))


@pytest.mark.parametrize(
    "view_g, chunk_g",
    [
        # sub-second values are valid view granularities but never valid chunk
        # granularities — chunked backfills measured in seconds or milliseconds
        # would create absurd commit counts.
        ("second", "second"),
        ("second", "millisecond"),
        ("millisecond", "millisecond"),
        ("hour", "second"),
        ("day", "millisecond"),
    ],
)
def test_full_refresh_chunk_rejects_subsecond_chunk(tmp_path, view_g, chunk_g):
    with pytest.raises(ValueError, match="not a valid chunk granularity"):
        load_views(write_views(tmp_path, _views_yaml_with_chunk(view_g, chunk_g)))


@pytest.mark.parametrize(
    "view_g, chunk_g",
    [
        ("minute", "minute"),
        ("minute", "hour"),
        ("minute", "day"),
        ("hour", "day"),
        ("day", "day"),
        ("day", "week"),
        ("day", "month"),
        ("week", "week"),
        ("month", "month"),
        ("month", "quarter"),
        ("month", "year"),
        ("quarter", "year"),
        ("year", "year"),
        # sub-second views can still pick coarser chunk values
        ("second", "minute"),
        ("second", "hour"),
        ("second", "day"),
        ("second", "week"),
        ("millisecond", "minute"),
        ("millisecond", "week"),
        ("millisecond", "year"),
    ],
)
def test_full_refresh_chunk_accepts_compatible(tmp_path, view_g, chunk_g):
    views = load_views(write_views(tmp_path, _views_yaml_with_chunk(view_g, chunk_g)))
    assert views[0].full_refresh_chunk == chunk_g


def test_full_refresh_chunk_round_trip(tmp_path):
    views = [
        ViewConfig(
            name="v",
            query="SELECT date_trunc('minute', ts) AS d FROM t GROUP BY 1",
            target_table="iceberg.analytics.v",
            full_refresh_chunk="day",
        )
    ]
    views_path = tmp_path / "views.yaml"
    save_views(views, views_path)
    loaded = load_views(views_path)
    assert loaded[0].full_refresh_chunk == "day"


def test_save_views_omits_full_refresh_chunk_when_none(tmp_path):
    """Views without chunked refresh must not emit a spurious
    ``full_refresh_chunk`` key in the YAML."""
    views = [
        ViewConfig(
            name="v",
            query="SELECT date_trunc('minute', ts) AS d FROM t GROUP BY 1",
            target_table="iceberg.analytics.v",
        )
    ]
    views_path = tmp_path / "views.yaml"
    save_views(views, views_path)
    yaml_text = views_path.read_text()
    assert "full_refresh_chunk" not in yaml_text


def test_query_timeout_seconds_round_trip(tmp_path):
    """A view with ``query_timeout_seconds`` set must survive save → load."""
    views = [
        ViewConfig(
            name="v",
            query="SELECT date_trunc('day', ts) AS d FROM t GROUP BY 1",
            target_table="iceberg.analytics.v",
            query_timeout_seconds=3600,
        )
    ]
    views_path = tmp_path / "views.yaml"
    save_views(views, views_path)
    loaded = load_views(views_path)
    assert loaded[0].query_timeout_seconds == 3600


def test_query_timeout_seconds_omitted_when_none(tmp_path):
    """The default (``None``) must not emit a spurious key in YAML, mirroring
    how ``full_refresh_chunk`` is handled."""
    views = [
        ViewConfig(
            name="v",
            query="SELECT date_trunc('day', ts) AS d FROM t GROUP BY 1",
            target_table="iceberg.analytics.v",
        )
    ]
    views_path = tmp_path / "views.yaml"
    save_views(views, views_path)
    assert "query_timeout_seconds" not in views_path.read_text()


@pytest.mark.parametrize("bad", [0, -1, "30s", True, False])
def test_query_timeout_seconds_rejects_non_positive_int(tmp_path, bad):
    """Validation: only a positive integer (or omission) is accepted. ``0`` is
    a footgun (``asyncio.timeout(0)`` cancels immediately) so we reject it
    rather than silently treating it as "no timeout".

    ``True`` / ``False`` are flagged separately because ``isinstance(True, int)``
    is ``True`` — a YAML ``query_timeout_seconds: true`` would otherwise pass as
    a 1-second timeout that nukes every refresh."""
    raw = (
        "views:\n  - name: v\n"
        "    query: \"SELECT date_trunc('day', ts) AS d FROM t GROUP BY 1\"\n"
        "    target_table: iceberg.analytics.v\n"
        f"    query_timeout_seconds: {bad!r}\n"
    )
    with pytest.raises(ValueError, match="query_timeout_seconds"):
        load_views(write_views(tmp_path, raw))


def test_full_refresh_chunk_rejects_view_without_direct_bucket_projection(tmp_path):
    """``full_refresh_chunk`` requires a direct ``date_trunc(g, col) AS <alias>``
    projection so the target has a column the executor can read as the
    resume point. A wrapped expression is rejected only when chunking is
    enabled."""
    wrapped = (
        "views:\n  - name: v\n"
        "    query: \"SELECT from_iso8601_date(CAST(date_trunc('day', ts) AS varchar)) AS d FROM t GROUP BY 1\"\n"
        "    target_table: iceberg.analytics.v\n"
        "    full_refresh_chunk: day\n"
    )
    with pytest.raises(ValueError, match="direct projection"):
        load_views(write_views(tmp_path, wrapped))


def test_wrapped_date_trunc_accepted_without_chunk(tmp_path):
    """A view whose date_trunc is wrapped is accepted when chunking is OFF
    (it just can't use the chunked-backfill path)."""
    wrapped = (
        "views:\n  - name: v\n"
        "    query: \"SELECT from_iso8601_date(CAST(date_trunc('day', ts) AS varchar)) AS d FROM t GROUP BY 1\"\n"
        "    target_table: iceberg.analytics.v\n"
    )
    views = load_views(write_views(tmp_path, wrapped))
    assert views[0].full_refresh_chunk is None


# ── Iceberg maintenance config ──


_MAINTENANCE_YAML = """\
views:
  - name: v
    query: "SELECT date_trunc('day', ts) AS d FROM t GROUP BY 1"
    target_table: iceberg.analytics.v
    maintenance_interval_seconds: 3600
    optimize: true
    optimize_file_size_threshold: 128MB
    expire_snapshots: false
    expire_snapshots_retention: 14d
    remove_orphan_files: true
    remove_orphan_files_retention: 30d
"""


def test_maintenance_defaults_to_disabled_with_ops_enabled(tmp_path):
    """Views without maintenance fields load with the shared interval at 0
    (kill switch off) but every per-op toggle defaulted to True — so opting
    in only requires setting maintenance_interval_seconds > 0."""
    minimal = (
        "views:\n"
        "  - name: v\n"
        "    query: \"SELECT date_trunc('day', ts) AS d FROM t GROUP BY 1\"\n"
        "    target_table: iceberg.analytics.v\n"
    )
    v = load_views(write_views(tmp_path, minimal))[0]
    assert v.maintenance_interval_seconds == 0
    assert v.optimize is True
    assert v.optimize_file_size_threshold is None
    assert v.expire_snapshots is True
    assert v.expire_snapshots_retention == "7d"
    assert v.remove_orphan_files is True
    assert v.remove_orphan_files_retention == "7d"


def test_maintenance_fields_loaded(tmp_path):
    v = load_views(write_views(tmp_path, _MAINTENANCE_YAML))[0]
    assert v.maintenance_interval_seconds == 3600
    assert v.optimize is True
    assert v.optimize_file_size_threshold == "128MB"
    assert v.expire_snapshots is False
    assert v.expire_snapshots_retention == "14d"
    assert v.remove_orphan_files is True
    assert v.remove_orphan_files_retention == "30d"


def test_maintenance_round_trip(tmp_path):
    views = [
        ViewConfig(
            name="v",
            query="SELECT date_trunc('day', ts) AS d FROM t GROUP BY 1",
            target_table="iceberg.analytics.v",
            maintenance_interval_seconds=3600,
            optimize=False,
            optimize_file_size_threshold="128MB",
            expire_snapshots_retention="14d",
        )
    ]
    p = tmp_path / "views.yaml"
    save_views(views, p)
    loaded = load_views(p)[0]
    assert loaded.maintenance_interval_seconds == 3600
    assert loaded.optimize is False
    assert loaded.optimize_file_size_threshold == "128MB"
    assert loaded.expire_snapshots_retention == "14d"


def test_save_views_omits_maintenance_defaults(tmp_path):
    """Default values (interval=0, all booleans True, default retentions)
    don't appear in YAML — the common empty-maintenance view stays a short diff."""
    views = [
        ViewConfig(
            name="v",
            query="SELECT date_trunc('day', ts) AS d FROM t GROUP BY 1",
            target_table="iceberg.analytics.v",
        )
    ]
    p = tmp_path / "views.yaml"
    save_views(views, p)
    text = p.read_text()
    assert "maintenance_interval_seconds" not in text
    assert "optimize" not in text  # default True
    assert "expire_snapshots" not in text  # default True
    assert "remove_orphan_files" not in text
    assert "optimize_file_size_threshold" not in text


def test_save_views_emits_disabled_op_toggle(tmp_path):
    """A False boolean must round-trip through YAML even though it's falsy —
    bug-prone area since the old save filter dropped any falsy value."""
    views = [
        ViewConfig(
            name="v",
            query="SELECT date_trunc('day', ts) AS d FROM t GROUP BY 1",
            target_table="iceberg.analytics.v",
            maintenance_interval_seconds=3600,
            optimize=False,
        )
    ]
    p = tmp_path / "views.yaml"
    save_views(views, p)
    text = p.read_text()
    assert "optimize: false" in text
    loaded = load_views(p)[0]
    assert loaded.optimize is False


def test_maintenance_rejects_negative_interval():
    with pytest.raises(ValueError, match=">= 0"):
        validate_maintenance_config({"maintenance_interval_seconds": -1})


@pytest.mark.parametrize(
    "field,value",
    [
        ("expire_snapshots_retention", "1 day"),
        ("expire_snapshots_retention", "7"),
        ("expire_snapshots_retention", "1w"),  # weeks not accepted by Trino
        ("remove_orphan_files_retention", "forever"),
    ],
)
def test_maintenance_rejects_bad_retention(field, value):
    with pytest.raises(ValueError, match="valid Trino duration"):
        validate_maintenance_config({field: value})


@pytest.mark.parametrize("value", ["128", "128m", "128 MB", "big"])
def test_maintenance_rejects_bad_file_size_threshold(value):
    with pytest.raises(ValueError, match="valid data size"):
        validate_maintenance_config({"optimize_file_size_threshold": value})


def test_maintenance_accepts_valid():
    """Sanity: a fully-shaped maintenance dict passes validation."""
    validate_maintenance_config(
        {
            "maintenance_interval_seconds": 3600,
            "optimize": True,
            "optimize_file_size_threshold": "128MB",
            "expire_snapshots": True,
            "expire_snapshots_retention": "7d",
            "remove_orphan_files": True,
            "remove_orphan_files_retention": "30d",
        }
    )
