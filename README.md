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

### `date_trunc` and `snap_range`: forward and inverse

The entire incremental refresh correctness depends on one thing: given the
min/max timestamps from new files, compute a filter range that covers **every
complete GROUP BY bucket** touched by that data. This is done by inverting
the `date_trunc` function used in the query's GROUP BY.

```mermaid
flowchart TB
    subgraph forward["<b>Forward: date_trunc('hour', ts)</b> — many timestamps → one bucket"]
        direction LR
        t1["09:15:42"] & t2["09:47:03"] -->|date_trunc| b1["<b>09:00</b>"]
        t3["10:22:18"] & t4["10:55:31"] -->|date_trunc| b2["<b>10:00</b>"]
    end

    subgraph inverse["<b>Inverse: snap_range('hour')</b> — file stats → complete bucket boundaries"]
        direction LR
        stats["file stats<br/><b>min=09:15, max=10:55</b>"]
        snap["snap_range"]
        result["filter range<br/><b>[09:00, 11:00)</b>"]
        stats --> snap --> result
    end

    forward -..->|"snap_range reverses date_trunc"| inverse

    style forward fill:#1a1a2e,stroke:#16213e,color:#e0e0e0
    style inverse fill:#0f3460,stroke:#16213e,color:#e0e0e0
    style b1 fill:#2d6a4f,stroke:#40916c,color:#fff
    style b2 fill:#2d6a4f,stroke:#40916c,color:#fff
    style result fill:#2d6a4f,stroke:#40916c,color:#fff
```

`date_trunc` is a **many-to-one** function: it maps every timestamp within a
bucket to the same boundary value. `snap_range` is its inverse: it expands a
raw timestamp range outward to the nearest bucket boundaries so the filter
captures all rows that belong to any touched bucket.

```mermaid
flowchart LR
    subgraph timeline["Timeline"]
        direction LR
        h08["08:00"]
        h09["09:00"]
        h10["10:00"]
        h11["11:00"]
        h12["12:00"]
    end

    subgraph filestats["New files"]
        fs["min = 09:15<br/>max = 10:55"]
    end

    subgraph snapresult["snap_range result"]
        direction TB
        floor["<b>floor</b>(09:15) = 09:00"]
        ceil["<b>ceil</b>(10:55) = 11:00"]
    end

    subgraph filter["WHERE ts >= 09:00 AND ts < 11:00"]
        direction TB
        covered["Covers complete buckets<br/><b>[09:00, 10:00)</b> and <b>[10:00, 11:00)</b><br/><br/>All rows in both buckets are<br/>included in the GROUP BY →<br/>MERGE produces correct aggregates"]
    end

    fs --> floor & ceil
    floor --> h09
    ceil --> h11
    h09 & h10 & h11 --> covered

    style filestats fill:#4a1942,stroke:#6b2d5b,color:#e0e0e0
    style snapresult fill:#0f3460,stroke:#16213e,color:#e0e0e0
    style filter fill:#2d6a4f,stroke:#40916c,color:#e0e0e0
```

The two operations mirror each other exactly:

| | `date_trunc('hour', ts)` | `snap_range('hour')` |
|---|---|---|
| **Direction** | timestamp → bucket start | timestamp range → bucket-aligned range |
| **Operation** | floor to `:00:00` | floor min to `:00:00`, ceil max to next `:00:00` |
| **Used in** | `GROUP BY` (query) | `WHERE` filter (orchestrator) |
| **Guarantees** | rows are grouped by hour | filter covers complete hours |

This is why only simple `date_trunc` is allowed: for any `date_trunc('X', col)`,
the inverse is trivially computable by `snap_range('X')`. Complex expressions
(e.g. 5-minute bars via arithmetic) break this — the bucket width can't be
reliably inferred, and the inverse would produce a too-narrow filter that
corrupts aggregates.

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
    query: |
      SELECT
        symbol,
        date_trunc('minute', ts) AS minute,
        min_by(price, ts) AS open, max(price) AS high,
        min(price) AS low, max_by(price, ts) AS close,
        sum(quantity) AS volume, count(*) AS trade_count
      FROM iceberg.market_data.trades
      GROUP BY symbol, date_trunc('minute', ts)
```

The operator writes *exactly* what they would put after
`CREATE MATERIALIZED VIEW … AS`. The orchestrator parses the query and derives
`source_table`, `filter_column`, `filter_granularity`, and `merge_keys` from it.
At refresh time the time-range `WHERE` predicate is AST-injected automatically —
there is no `{range_filter}` placeholder.

The orchestrator also auto-discovers column types (`DESCRIBE OUTPUT`), creates
the target table, and starts refreshing. Views can also be managed from the web
UI.

### Configuration reference

| Field | Required | Description |
|---|---|---|
| `name` | yes | Unique view name |
| `query` | yes | The full SELECT — exactly what you would write after `CREATE MATERIALIZED VIEW … AS`. `source_table`, `filter_column`, `filter_granularity`, and `merge_keys` are derived from this |
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

The inferred `filter_granularity` (`week`) snaps the file-stats range to complete
week boundaries, so the MERGE query reads Mon+Tue+Wed and produces a correct
weekly bar.

## Query parsing

At config-load time the orchestrator parses every view query with an AST-based
parser (`sqlparse`). It derives:

- `source_table` — from the FROM clause
- `filter_column` — the bare column inside `date_trunc('X', col)`
- `filter_granularity` — the `'X'` literal (one of `minute`, `hour`, `day`,
  `week`, `month`, `quarter`, `year`)
- `merge_keys` — resolved from the GROUP BY list against the projection
  (positional `GROUP BY 1, 2` refs are handled too)

At refresh time the orchestrator AST-injects the time-range `WHERE` predicate
directly into the query:

```sql
-- operator writes:
SELECT symbol, date_trunc('week', ts) AS week, sum(qty) AS volume
FROM iceberg.md.trades
WHERE color = 'red'
GROUP BY 1, 2

-- orchestrator runs:
MERGE INTO iceberg.md.trades_weekly AS t USING (
  SELECT symbol, date_trunc('week', ts) AS week, sum(qty) AS volume
  FROM iceberg.md.trades
  WHERE color = 'red'
    AND ts >= TIMESTAMP '2026-04-06 00:00:00.000000 UTC'
    AND ts < TIMESTAMP '2026-04-13 00:00:00.000000 UTC'
  GROUP BY 1, 2
) AS s ON t.symbol = s.symbol AND t.week = s.week …
```

## Limitations

### Query shape

The query must be a `SELECT … GROUP BY` over a **single source table**. The
parser enforces this at load time and rejects anything else with a clear error.

### Not supported

- **Joins.** The query must reference exactly one source table. Change
  detection only inspects the parsed source table; if a query joined a second
  table, updates to that other table would never trigger a refresh and the MV
  would silently go stale. Parser rejects queries containing `JOIN`.
- **Set operations** (`UNION` / `INTERSECT` / `EXCEPT`) — rejected at parse.
- **CTEs** (`WITH …`) — rejected at parse.
- **Subqueries in FROM** — rejected at parse.
- **Queries without `GROUP BY`** — rejected at parse. The correctness model
  is built around `date_trunc('X', col)` defining aggregation buckets.
- **`date_trunc` wrapped in arithmetic** (e.g. 5-minute bars via
  `date_trunc(...) - INTERVAL ...`) — rejected at parse because the bucket
  width cannot be reliably inferred.
- **Projection columns without an alias on computed expressions** — rejected
  so the target-table columns have stable names.
- **Source deletes/overwrites** — detected via `$snapshots`, raises
  `UnexpectedOperationError` (the project assumes append-only sources).
- **Missing column stats** — if the source writer disables Iceberg column
  statistics, the detector can't determine the affected range and raises
  `MissingFilterColumnError`.

### Assumptions

- **Append-only sources** (trades, logs, events). Only Iceberg `append` and
  `replace` (compaction) snapshot operations are allowed. `replace` is
  skipped — files were rewritten but no data changed. Any other operation
  (`overwrite`, `delete`) fails loudly.
- **UTC session timezone.** The orchestrator pins every Trino session to
  `UTC` so that `date_trunc('day' | 'week' | …, ts)` on `TIMESTAMP WITH
  TIME ZONE` columns aligns with the Python-side `snap_range` bucket math.
  Without this pin, a non-UTC session would produce bucket boundaries
  that disagree with the computed filter range and silently corrupt
  incremental aggregates. See [DESIGN.md](DESIGN.md#timezone-assumption).
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
