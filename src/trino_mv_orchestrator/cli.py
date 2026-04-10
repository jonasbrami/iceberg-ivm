"""CLI entry point for trino-mv-orchestrator."""
from __future__ import annotations

import argparse
import logging


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
        "-v", "--verbose",
        action="store_true",
        help="enable debug logging",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-5s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    from pathlib import Path

    from trino_mv_orchestrator.config import load_config
    from trino_mv_orchestrator.server import app, state

    state.config_path = Path(args.config)
    cfg = load_config(state.config_path)

    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=cfg.server.port, log_level="info")


if __name__ == "__main__":
    main()
