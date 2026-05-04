# Quick start — fully self-contained iceberg-ivm

A one-command demo stack: Trino + MinIO + Postgres (Iceberg JDBC catalog) +
iceberg-ivm, with sample trades data and 8 pre-loaded materialized views.

## Run

```bash
docker compose up -d
```

That's it. The compose file:

1. Brings up MinIO, Postgres, and Trino.
2. Runs a one-shot **seed** container that creates `iceberg.market_data.trades`
   and inserts ~30 days of synthetic trades for 3 symbols.
3. Once seeding completes, starts **iceberg-ivm** with 8 views pre-loaded.

After ~1-2 minutes (initial Trino start + seed) the UI is fully populated.

## URLs

| What | URL |
|---|---|
| iceberg-ivm UI | http://localhost:8001 |
| Trino UI | http://localhost:28080 |

> Quickstart binds the UI on **8001** (not the usual 8000) so it doesn't clash
> with a locally-running `iceberg-ivm` on the default port. Edit
> `docker-compose.yml` if you want to change it.
| MinIO console | http://localhost:19001 (`minioadmin` / `minioadmin`) |

## What you'll see

Eight materialized views, all refreshing automatically:

| View | Shape |
|---|---|
| `ohlcv_1m` | Minute OHLCV bars from `trades` |
| `ohlcv_1h` | Hour OHLCV bars from `trades` |
| `ohlcv_1d` | Day OHLCV bars from `trades` |
| `weekly_volume` | Weekly bars direct from `trades` (cross-partition demo) |
| `monthly_volume` | Monthly bars from `trades` |
| `hourly_trade_count` | Plain `count(*)` by hour |
| `symbol_daily_high` | Uses `max_by` (non-decomposable aggregate) |
| `large_trades_hourly` | Pre-filtered with a `WHERE` clause |

## Tear down

```bash
docker compose down -v   # -v also removes the warehouse + state volumes
```

## State persistence

The iceberg-ivm SQLite state DB is on a named volume (`ivm-state`). Restarts
preserve recent-query history; `docker compose down -v` wipes it.

## Trying it for real

Edit `views.yaml` while the stack is running — iceberg-ivm hot-reloads it
within `config_reload_interval_seconds` (30s). Or use the **New View**
button in the UI to add views interactively; the API will write back to
`views.yaml` for you.
