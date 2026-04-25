"""Tests for ``resolve_state_db_path`` (issue #39).

The default Docker deployment bind-mounts ``config.yaml`` as a single file
into ``/app/`` (the container's writable layer) but bind-mounts the views
file from a host directory. Anchoring the relative ``state_db_path`` on
the config file's parent therefore wipes the SQLite store on every image
bump. The resolver must prefer the views file's directory and only fall
back to the config file's directory when the views directory is missing
or not writable.
"""
from __future__ import annotations

import os
import stat
from pathlib import Path

import pytest

from trino_mv_orchestrator.server import resolve_state_db_path


def test_relative_anchors_on_views_dir(tmp_path: Path) -> None:
    """The default ``state.db`` lands next to ``views.yaml``, not next to
    ``config.yaml`` — this is the behaviour the Docker compose default
    relies on for state to survive image bumps."""
    config_dir = tmp_path / "etc"
    views_dir = tmp_path / "data"
    config_dir.mkdir()
    views_dir.mkdir()
    config_path = config_dir / "config.yaml"
    views_path = views_dir / "views.yaml"
    config_path.write_text("")
    views_path.write_text("")

    resolved = resolve_state_db_path(views_path, config_path, "state.db")

    assert resolved == views_dir / "state.db"
    # And explicitly NOT under the config dir — that's the regression.
    assert config_dir not in resolved.parents


def test_absolute_path_used_as_is(tmp_path: Path) -> None:
    """An explicit absolute path is honoured verbatim."""
    config_path = tmp_path / "config.yaml"
    views_path = tmp_path / "views.yaml"
    abs_path = tmp_path / "elsewhere" / "state.db"

    resolved = resolve_state_db_path(views_path, config_path, str(abs_path))

    assert resolved == abs_path


def test_falls_back_to_config_dir_when_views_dir_missing(tmp_path: Path) -> None:
    """When the views file's directory does not exist (e.g. a test fixture
    pointing at a not-yet-created path), the resolver falls back to the
    config file's directory rather than raising."""
    config_dir = tmp_path / "etc"
    config_dir.mkdir()
    config_path = config_dir / "config.yaml"
    config_path.write_text("")
    # Deliberately a path whose parent does NOT exist.
    views_path = tmp_path / "missing_dir" / "views.yaml"

    resolved = resolve_state_db_path(views_path, config_path, "state.db")

    assert resolved == config_dir / "state.db"


def test_falls_back_to_config_dir_when_views_dir_not_writable(tmp_path: Path) -> None:
    """If the views directory exists but isn't writable, fall back to the
    config dir. Without this guard a read-only mount of the views dir
    would blow up at SQLite open time instead of using a sane default."""
    if os.geteuid() == 0:
        pytest.skip("root bypasses POSIX write bits — can't simulate read-only dir")
    config_dir = tmp_path / "etc"
    views_dir = tmp_path / "ro_views"
    config_dir.mkdir()
    views_dir.mkdir()
    config_path = config_dir / "config.yaml"
    views_path = views_dir / "views.yaml"
    config_path.write_text("")
    views_path.write_text("")

    original_mode = views_dir.stat().st_mode
    views_dir.chmod(stat.S_IRUSR | stat.S_IXUSR)  # r-x: readable but not writable
    try:
        resolved = resolve_state_db_path(views_path, config_path, "state.db")
    finally:
        views_dir.chmod(original_mode)

    assert resolved == config_dir / "state.db"
