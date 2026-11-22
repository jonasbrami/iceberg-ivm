# Contributing

Thanks for helping. This page covers setting up, testing, and releasing. For how the refresh logic works, read [docs/how-it-works.md](docs/how-it-works.md) first.

## Set up

You need [uv](https://docs.astral.sh/uv/) and Docker. uv installs Python 3.12+ for you.

```bash
git clone https://github.com/jonasbrami/iceberg-ivm && cd iceberg-ivm
uv sync
uv run pre-commit install --hook-type pre-commit --hook-type pre-push
```

The pre-commit hook runs ruff (lint with autofix, then format). The pre-push hook runs the unit tests.

## Run it locally

The quickest way to see your changes is the quickstart stack, built from your working tree:

```bash
cd quickstart
docker compose -f docker-compose.yml -f docker-compose.local.yml up -d --build
open http://localhost:8001
```

Or run the service directly against any Trino (here, the quickstart's):

```bash
cp config.yaml.example config.yaml
export TRINO_URL=http://localhost:28080 TRINO_USER=dev
uv run iceberg-ivm -c config.yaml --views views.yaml -v
```

## Tests

### Unit tests

```bash
uv run pytest -m "not integration"
```

They take a few seconds and run in parallel (pytest-xdist). They fake Trino, so they need no Docker.

### Integration tests

These run every refresh path against a real Trino with an Iceberg catalog:

```bash
docker compose -f tests/docker-compose.yml up -d --wait trino
uv run pytest -m integration
docker compose -f tests/docker-compose.yml down -v
```

The stack is Trino (port 18080) plus Postgres for the Iceberg JDBC catalog. Trino keeps the warehouse on its local filesystem, so there is no object store to set up. Point the tests elsewhere with `TRINO_HOST` and `TRINO_PORT`.

| Suite | What it checks |
|---|---|
| `test_correctness.py` | after each refresh, the target equals the query run directly: NULL keys, `HAVING` drop-outs, source deletes, recreated views |
| `test_refresh.py` | first run, incremental runs, skips, chunked full refresh, interrupt and resume |
| `test_cross_partition_groupby.py` | weekly and monthly buckets over a daily-partitioned source, late data |
| `test_chained_mv.py` | a view whose source is another view's target, over many cycles |
| `test_chunked_resume_scenarios.py` | interrupted backfills: resume, no window drift, no snapshot mixing, expired pins |
| `test_streaming_refresh.py` | many ingest cycles, late rows and compaction, compared with an in-memory oracle |

A change to detection or refresh logic should come with an integration test that compares the target with the query run directly, like those in `test_correctness.py`.

### Lint and format

```bash
uv run ruff check .
uv run ruff format .
```

CI runs lint and unit tests on every push. It runs the integration suite on pull requests to `master` and on manual dispatch.

## Project layout

```text
src/iceberg_ivm/
  cli.py            command line: load config, start the server
  config.py         ViewConfig (one model for views.yaml and the API), config.yaml, Trino env vars
  query_parser.py   validate a view query; rewrite it for one range and snapshot (sqlglot)
  detector.py       change detection from Iceberg metadata; bucket math
  executor.py       build and run the replace-range MERGE, chunking, maintenance ops
  introspect.py     output columns via DESCRIBE OUTPUT; CREATE TABLE for the target
  query_history.py  state.db: bookmarks, in-flight runs, query history, maintenance schedule
  server.py         FastAPI app: REST API, UI, Prometheus metrics, one worker per view
  static/           the web UI: index.html (no build step) and a pinned copy of Alpine.js
tests/
  unit/             fast tests with fake Trino cursors
  integration/      real Trino via tests/docker-compose.yml
quickstart/         one-command demo stack
docs/               how-it-works.md, screenshots and the logo
```

## Releases

Versions come from git tags (hatch-vcs). To release:

```bash
git tag v0.9.0 && git push origin v0.9.0
```

CI runs the tests, then publishes `ghcr.io/jonasbrami/iceberg-ivm` tagged `0.9.0`, `0.9`, `latest` and `sha-<commit>`.

## Pull requests

- Keep each pull request to one change, with tests.
- Explain *why* in the description; the diff shows *what*.
- Comments in code explain the code as it is. History belongs in commit messages.
