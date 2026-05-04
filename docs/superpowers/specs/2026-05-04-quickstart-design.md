# Quickstart self-contained example — design

## Goal

Provide a one-command, fully self-contained quickstart of `iceberg-ivm` with
Trino + MinIO + Postgres + sample data + pre-loaded materialized views, so a
new visitor can `cd quickstart && docker compose up` and immediately see a
populated UI.

Add a clickable link from the README and a screenshot showing many MVs
defined.

## Non-goals

- Not replacing `tests/docker-compose.yml`. That stack stays for development
  and the integration test suite.
- Not a production deployment. Single-instance, ephemeral volumes are fine.

## Layout

```
quickstart/
  README.md                       — short "what this does" + run instructions
  docker-compose.yml              — full stack
  config.yaml                     — server + trino catalog/schema
  views.yaml                      — 8 demonstrative views, pre-loaded
  trino-config/
    iceberg.properties            — JDBC catalog over MinIO (copied from tests/)
    init-iceberg-catalog.sql      — bootstrap iceberg_tables/iceberg_namespace_properties
  seed/
    Dockerfile                    — slim python image with trino client
    seed.py                       — schemas + trades table + ~30 days of trades
docs/
  screenshots/
    ui-overview.png               — UI showing all 8 cards
```

## Compose services

| Service | Image | Role |
|---|---|---|
| `minio` | `minio/minio:latest` | object store for the warehouse |
| `minio-init` | `minio/mc:latest` | one-shot: create `warehouse` bucket |
| `postgres` | `postgres:16-alpine` | Iceberg JDBC catalog |
| `trino` | `jonasbrami/trino-arrow:479-03d1b24` | query engine (matches `tests/`) |
| `seed` | locally-built (`./seed/Dockerfile`) | one-shot: create schemas + trades + insert ~30 days |
| `iceberg-ivm` | `jonasbrami/iceberg-ivm:0.2.1` (published) | the orchestrator |

**Dependency chain:**
- `minio-init` → `postgres` → `trino` (healthcheck) → `seed` (runs to completion) → `iceberg-ivm`

`iceberg-ivm` waits for `seed` to complete successfully so the `trades` table
exists before the first refresh tick. This avoids first-run "table not found"
errors in the UI's recent-queries log.

**Why:** the UI screenshot needs to look successful — every card showing a
green refresh count and a recent successful query. If `iceberg-ivm` starts
before seed runs, every view's first refresh fails and the cards show errors
until the seed completes (up to a few minutes).

## Pre-loaded views (`views.yaml`)

Eight views, chosen to demonstrate range and produce a varied,
informative grid for the screenshot:

1. **`ohlcv_1m`** — minute bars from `trades` (canonical example).
2. **`ohlcv_1h`** — hour bars chained from `ohlcv_1m` (shows MV-on-MV).
3. **`ohlcv_1d`** — day bars chained from `ohlcv_1h` (3-deep chain).
4. **`weekly_volume`** — weekly bars direct from `trades` (cross-partition demo).
5. **`monthly_volume`** — monthly bars from `trades`.
6. **`hourly_trade_count`** — `count(*)` by hour, no other columns.
7. **`symbol_daily_high`** — uses `max_by` (non-decomposable; full-recompute story).
8. **`large_trades_hourly`** — pre-filtered with a `WHERE` clause showing
   the `WHERE` is preserved when iceberg-ivm AND-injects the time predicate.

Each view sets a short `refresh_interval_seconds` (15-30s) so by the time
the screenshot is taken every card has run at least one refresh.

Reserved-word caveat: in `ohlcv_1h` / `ohlcv_1d` the upstream MV's time
column must be a non-reserved name (e.g. `bucket` or `minute_ts`), not
`minute` / `hour`. The README already calls this out; the views.yaml will
follow the rule.

## Seed script

`seed/seed.py` creates `iceberg.market_data.trades` (the same shape as
`tests/seed_data.py`) and inserts roughly 30 days of synthetic trades for
3 symbols, batched. Why 30 days: enough data that monthly/weekly views
have multiple buckets and the screenshot tells a story.

The seed is idempotent (DROP TABLE IF EXISTS at the top) but only runs once
per `docker compose up` — the service exits 0 on success and `iceberg-ivm`
gates on its completion.

## Network / port plan

| Port | Service |
|---|---|
| `8000` | `iceberg-ivm` UI |
| `18080` | Trino UI (matches `tests/`) |
| `9001` | MinIO console (optional) |

No clashes with anything in `tests/` so a developer can run both stacks
on different terminals (volumes are namespaced).

## Verification

After `docker compose up -d`:

1. Wait until `iceberg-ivm` is healthy (`curl localhost:8000/health` returns
   `{"status":"ok","views":8}`).
2. `curl localhost:8000/api/views | jq '.[] | {name, status, last_action}'` —
   every view should show a non-error status and a non-zero refresh count
   within ~1 minute.
3. Take a screenshot of `http://localhost:8000` showing all 8 cards and
   save it to `docs/screenshots/ui-overview.png`.

## README changes

1. Add an inline image reference to `docs/screenshots/ui-overview.png` near
   the top so the project page leads with what the UI actually looks like.
2. Add a prominent clickable link `[Quick start (Docker)](./quickstart)` in
   the "Run it (TL;DR)" section, pointing at the new directory.
3. Replace the existing "Running against a local Trino stack" subsection
   with a short pointer at `quickstart/` (the longer `tests/` instructions
   remain implicitly available for contributors).

## Risks and mitigations

| Risk | Mitigation |
|---|---|
| `jonasbrami/iceberg-ivm:0.2.1` doesn't pick up future fixes | Use a tag, not `latest`; bump on release. |
| Slow first run (Trino start ~30-60s) | Healthchecks + `depends_on` so the user just waits — single `up -d` works. |
| Seed timing out (large insert) | Batch inserts of 200 rows at a time, same pattern as `tests/seed_data.py`. |
| Reserved-word collision in chained views | Use `bucket` / `minute_ts` aliases; verified against the parser at load time. |
