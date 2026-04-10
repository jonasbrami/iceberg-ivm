# trino-mv-orchestrator

Metadata-driven incremental materialized view orchestrator for Trino/Iceberg.

Maintains materialized views backed by Iceberg tables, refreshed incrementally
using only Iceberg file-level metadata for change detection. When source data
changes, only the affected time range is recomputed from complete source data,
guaranteeing correct aggregations. Refreshes are atomic via `MERGE INTO`.

> This project was designed and implemented through a conversation between a
> human prompter and Claude Code. See [DESIGN.md](DESIGN.md) for the full
> design rationale and conversation context.

## How it works

```mermaid
sequenceDiagram
    participant O as Orchestrator
    participant T as Trino
    participant SRC as Source Table<br/>(Iceberg metadata)
    participant TGT as Target MV<br/>(Iceberg table)

    O->>T: SELECT snapshot_id FROM source.$snapshots
    T-->>O: current_snapshot = 200

    Note over O: last_processed = 150 → changed

    O->>T: SELECT readable_metrics<br/>FROM source.$all_entries<br/>WHERE snapshot_id IN (new)
    T->>SRC: read manifest files (no data scan)
    T-->>O: ts ∈ [Apr 9 10:00, Apr 9 15:30]

    Note over O: snap_range("day")<br/>→ [Apr 9, Apr 10)

    O->>T: MERGE INTO target<br/>USING (SELECT ... WHERE ts >= Apr 9 AND ts < Apr 10)<br/>ON keys WHEN MATCHED UPDATE / NOT MATCHED INSERT
    T->>SRC: read data files (partition-pruned)
    T->>TGT: atomic Iceberg commit
    T-->>O: done

    O->>T: ALTER TABLE target SET PROPERTIES {last_snapshot: 200}
```

1. **Detect** -- query `$snapshots` to check if source changed (<50ms)
2. **Measure** -- read `$all_entries` for new files' column-level min/max bounds (metadata only)
3. **Snap** -- expand the time range to complete GROUP BY bucket boundaries (pure Python)
4. **Refresh** -- `MERGE INTO` with a plain column range filter (Trino pushes down to partition pruning)
5. **Persist** -- store snapshot ID in target table's Iceberg properties

## Quick start

```bash
uv sync
uv run trino-mv-orchestrator -c config.yaml
# Web UI:  http://localhost:8000
# Metrics: http://localhost:8000/metrics
```

### Trino prerequisite

```properties
# etc/catalog/iceberg.properties
iceberg.allowed-extra-properties=mv.last_source_snapshot
```

### Minimal view definition

```yaml
trino:
  host: localhost
  port: 8080
  catalog: iceberg
  schema: analytics
  user: orchestrator

views:
  - name: ohlcv_1m
    source_table: iceberg.market_data.trades
    filter_column: ts
    # filter_granularity inferred as "minute" from date_trunc('minute', ts)
    query: |
      SELECT
        symbol,
        date_trunc('minute', ts) AS minute,
        min_by(price, ts) AS open, max(price) AS high,
        min(price) AS low, max_by(price, ts) AS close,
        sum(quantity) AS volume, count(*) AS trade_count
      FROM iceberg.market_data.trades
      WHERE {range_filter}
      GROUP BY 1, 2
    merge_keys: [symbol, minute]
```

`filter_granularity` is automatically inferred from `date_trunc('minute', ts)` in
the query. See [Granularity inference](#granularity-inference) for details and
when you need to set it explicitly.

The orchestrator auto-discovers column types (`DESCRIBE OUTPUT`), creates the
target table, and starts refreshing. Views can also be managed from the web UI.

### Configuration reference

| Field | Required | Description |
|---|---|---|
| `name` | yes | Unique view name |
| `source_table` | yes | Fully qualified Iceberg source table |
| `filter_column` | yes | Column to read min/max stats for (must have Iceberg column stats) |
| `filter_granularity` | no | `minute`, `hour`, `day`, `week`, or `month`. Auto-inferred from `date_trunc` in query when omitted. Required for complex expressions (see below). |
| `query` | yes | SELECT with `{range_filter}` placeholder in WHERE clause |
| `merge_keys` | yes | Columns forming the MERGE ON clause (must be unique in output) |
| `target_table` | no | Defaults to `{catalog}.{schema}.{name}` |
| `target_partitioning` | no | Defaults to source table's partitioning |
| `refresh_interval_seconds` | no | Defaults to 60 |

### API

| Endpoint | Method | Description |
|---|---|---|
| `/` | GET | Web UI |
| `/api/views` | GET | List all views with status |
| `/api/views` | POST | Create a new view |
| `/api/views/{name}` | DELETE | Remove a view |
| `/api/views/{name}/refresh` | POST | Trigger manual refresh |
| `/metrics` | GET | Prometheus metrics |
| `/health` | GET | Health check |

### Prometheus metrics

| Metric | Type | Labels |
|---|---|---|
| `mv_refresh_total` | counter | view, type(full/incremental/skip) |
| `mv_refresh_duration_seconds` | histogram | view |
| `mv_refresh_last_success_timestamp` | gauge | view |
| `mv_refresh_errors_total` | counter | view |
| `mv_config_reload_total` | counter | |
| `mv_views_configured` | gauge | |

## Cross-partition GROUP BY

The tool correctly handles GROUP BY expressions coarser than the source
partition granularity (e.g. weekly bars from a daily-partitioned table).

```mermaid
graph LR
    subgraph "Source: trades (partitioned by day)"
        D1["day=Apr 6 (Mon)"]
        D2["day=Apr 7 (Tue)"]
        D3["day=Apr 8 (Wed) ← NEW"]
    end

    subgraph Orchestrator
        FS["File stats: ts ∈ [Apr 8 10:00, Apr 8 15:30]"]
        SR["snap_range('week'): [Apr 6, Apr 13)"]
        FS --> SR
    end

    subgraph "MERGE reads ALL 3 days"
        Q["WHERE ts >= Apr 6<br/>AND ts < Apr 13"]
    end

    subgraph "Target: weekly bars"
        W["week=Apr 6<br/>vol=350 (Mon+Tue+Wed)"]
    end

    D3 -->|"readable_metrics"| FS
    SR --> Q
    D1 --> Q
    D2 --> Q
    D3 --> Q
    Q --> W

    style D3 fill:#2d6a4f,stroke:#40916c
    style W fill:#2d6a4f,stroke:#40916c
```

The `filter_granularity: week` setting snaps the file-stats range to complete
week boundaries, so the MERGE query reads Mon+Tue+Wed and produces a correct
weekly bar.

## Granularity inference

When `filter_granularity` is omitted, the orchestrator parses
`date_trunc('X', ...)` from the query and uses `X` as the granularity:

```
date_trunc('minute', ts) AS minute   →  inferred: minute
date_trunc('hour', ts) AS hour       →  inferred: hour
date_trunc('week', ts) AS week       →  inferred: week
```

Inference is **not attempted** when `date_trunc` appears inside arithmetic
expressions. For example, 5-minute bars use `date_trunc` as a helper but the
real bucket is 5 minutes:

```sql
-- Inference skipped: date_trunc is part of arithmetic
date_trunc('minute', minute)
  - (extract(minute FROM minute) % 5) * INTERVAL '1' MINUTE AS bar
```

In these cases, set `filter_granularity` explicitly. The value must be at least
as coarse as the real GROUP BY bucket to avoid data loss:

- **Too fine** (e.g. `minute` for weekly GROUP BY) → **data loss**
- **Too coarse** (e.g. `month` for minute GROUP BY) → correct but wasteful

## Limitations

### Query shape

The query must be a `SELECT ... GROUP BY` over a **single source table** with
a `{range_filter}` in the WHERE clause.

### Not supported

- **Joins** -- change detection tracks one source table only
- **Non-time GROUP BY** -- `GROUP BY symbol` with no time component degrades
  to near-full-refresh on every change
- **Source deletes/overwrites** -- detected via `$snapshots`, triggers full
  refresh (correct but expensive)
- **Missing column stats** -- if the source writer disables Iceberg column
  statistics, the detector can't determine the affected range

### Assumptions

- **Append-only sources** (trades, logs, events)
- **Iceberg v2** (required for MERGE)
- Source files have column-level min/max statistics (default in Parquet)

## Tests

```bash
# Unit tests only
uv run pytest tests/unit/ -v

# Full suite (requires docker compose)
cd tests && docker compose up -d
cd .. && uv run pytest tests/ -v
cd tests && docker compose down -v
```

## Project structure

```
src/trino_mv_orchestrator/
    config.py        -- YAML config loading, saving, validation
    detector.py      -- $snapshots + $all_entries file stats + snap_range()
    executor.py      -- MERGE SQL generation + execution
    introspect.py    -- DESCRIBE OUTPUT, EXPLAIN IO, SHOW CREATE TABLE
    state.py         -- Read/write last_source_snapshot via extra_properties
    server.py        -- FastAPI: web UI, REST API, Prometheus, refresh loop
    cli.py           -- Entry point, starts uvicorn
    static/
        index.html   -- Web UI (Tailwind CSS + Alpine.js)
tests/
    unit/            -- 43 tests (mock cursors, FastAPI test client)
    integration/     -- 10 e2e tests (Trino + Iceberg + MinIO via docker compose)
```
