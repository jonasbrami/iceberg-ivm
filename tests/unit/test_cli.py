"""Tests for the CLI entry point."""
import textwrap
from unittest.mock import patch

import pytest


CONFIG_YAML = textwrap.dedent("""\
    trino:
      host: localhost
      port: 8080
      catalog: iceberg
      schema: analytics
      user: test
""")


class TestCliMain:
    def test_default_config_path(self, tmp_path, monkeypatch):
        cfg_path = tmp_path / "config.yaml"
        cfg_path.write_text(CONFIG_YAML)
        monkeypatch.chdir(tmp_path)

        with patch("trino_mv_orchestrator.cli.argparse.ArgumentParser.parse_args") as mock_args, \
             patch("uvicorn.run") as mock_uvicorn:
            mock_args.return_value = type("Args", (), {"config": str(cfg_path), "views": str(tmp_path / "views.yaml"), "verbose": False})()

            from trino_mv_orchestrator.cli import main
            main()

            mock_uvicorn.assert_called_once()
            call_kwargs = mock_uvicorn.call_args
            assert call_kwargs[1]["port"] == 8000

    def test_custom_config_path(self, tmp_path, monkeypatch):
        custom = tmp_path / "custom.yaml"
        monkeypatch.chdir(tmp_path)

        custom.write_text(CONFIG_YAML + "server:\n  port: 9999\n")

        with patch("trino_mv_orchestrator.cli.argparse.ArgumentParser.parse_args") as mock_args, \
             patch("uvicorn.run") as mock_uvicorn:
            mock_args.return_value = type("Args", (), {"config": str(custom), "views": str(tmp_path / "views.yaml"), "verbose": False})()

            from trino_mv_orchestrator.cli import main
            main()

            call_kwargs = mock_uvicorn.call_args
            assert call_kwargs[1]["port"] == 9999

    def test_verbose_sets_debug(self, tmp_path, monkeypatch):
        cfg_path = tmp_path / "config.yaml"
        cfg_path.write_text(CONFIG_YAML)
        monkeypatch.chdir(tmp_path)

        with patch("trino_mv_orchestrator.cli.argparse.ArgumentParser.parse_args") as mock_args, \
             patch("uvicorn.run"), \
             patch("logging.basicConfig") as mock_logging:
            mock_args.return_value = type("Args", (), {"config": str(cfg_path), "views": str(tmp_path / "views.yaml"), "verbose": True})()

            from trino_mv_orchestrator.cli import main
            main()

            mock_logging.assert_called_once()
            import logging
            assert mock_logging.call_args[1]["level"] == logging.DEBUG
