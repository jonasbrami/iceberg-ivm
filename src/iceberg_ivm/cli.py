"""CLI entry point for iceberg-ivm."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import uvicorn

from iceberg_ivm.config import load_config
from iceberg_ivm.server import app


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="iceberg-ivm",
        description="Incremental view maintenance for Iceberg tables on Trino",
    )
    parser.add_argument(
        "-c",
        "--config",
        default="config.yaml",
        help="path to config file (default: config.yaml)",
    )
    parser.add_argument(
        "--views",
        default="views.yaml",
        help="path to views file (default: views.yaml)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="enable debug logging",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-5s %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    config_path = Path(args.config)
    app.state.config_path = config_path
    app.state.views_path = Path(args.views)

    # Pre-load config to get the port; lifespan does the real init.
    cfg = load_config(config_path)
    uvicorn.run(app, host="0.0.0.0", port=cfg.server.port, log_level="info")


if __name__ == "__main__":
    main()
