# Quickstart

A complete local stack in one command: **Trino**, an **Iceberg** catalog (Postgres), sample trade data, and **iceberg-ivm** with 8 views already defined.

```bash
cd quickstart
docker compose up -d
```

Give it a minute or two (Trino starts, the seed job writes 30 days of trades), then open:

| | |
|---|---|
| **iceberg-ivm** | http://localhost:8001 |
| **Trino UI** | http://localhost:28080 |

Both are bound to `127.0.0.1` only. The UI uses port 8001 so it doesn't clash with an iceberg-ivm you may already run on 8000.

## The 8 views

| View | What it shows off |
|---|---|
| `ohlcv_1m` | Minute OHLCV bars, using `min_by` / `max_by` |
| `ohlcv_1h` | **Chained**: hourly bars rolled up from `ohlcv_1m`'s table |
| `ohlcv_1d` | Daily bars |
| `weekly_volume` | Weekly buckets over a daily-partitioned source |
| `monthly_volume` | Monthly buckets |
| `hourly_trade_count` | A plain `count(*)` |
| `symbol_daily_high` | `max_by`, which can't be computed from partial aggregates |
| `large_trades_hourly` | A `WHERE` filter inside the view |

## Make it change

Insert a trade in Trino and watch the affected minute, hour, day, week and month buckets refresh. Nothing else is recomputed.

```bash
docker compose exec trino trino --execute "
  INSERT INTO iceberg.market_data.trades
  VALUES ('AAPL', current_timestamp, 199.5, 300)"
```

Edit `views.yaml` while the stack runs (it reloads within 30 s), or add a view from the UI's **New view** button.

## Stop

```bash
docker compose down      # keep data for next time
docker compose down -v   # also delete the warehouse, catalog and state
```

## Run your local checkout

To exercise your working tree instead of the published image:

```bash
docker compose -f docker-compose.yml -f docker-compose.local.yml up -d --build
```

Only the `iceberg-ivm` service is rebuilt. Requires Docker Compose v2.24+.
