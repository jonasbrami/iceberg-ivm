"""CLI entry point for trino-mv-orchestrator."""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

import uvicorn

from trino_mv_orchestrator.config import load_config
from trino_mv_orchestrator.server import app, set_config_path, set_views_path


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="trino-mv-orchestrator",
        description="Metadata-driven incremental MV orchestrator for Trino/Iceberg",
    )
    parser.add_argument(
        "-c", "--config",
        default="config.yaml",
        help="path to config file (default: config.yaml)",
    )
    parser.add_argument(
        "--views",
        default="views.yaml",
        help="path to views file (default: views.yaml)",
    )
    parser.add_argument(
        "-v", "--verbose",
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
    set_config_path(config_path)
    set_views_path(Path(args.views))

    # Pre-load config to get the port, then let lifespan do the real init
    cfg = load_config(config_path)
    log = logging.getLogger(__name__)
    log.info("starting trino-mv-orchestrator on port %d", cfg.server.port)

    uvicorn.run(app, host="0.0.0.0", port=cfg.server.port, log_level="info")


if __name__ == "__main__":
    main()
