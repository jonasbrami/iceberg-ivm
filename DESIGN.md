# Design Document

Implementation details, design decisions, and conversation context for agents
and contributors continuing this work.

## Origin

This project was built through a conversation between Jonas Brami (working on
a trading system called konbanwa) and Claude Code (Anthropic, April 2026).

The conversation started with: *"How does Trino manage materialized views on
Iceberg? Is it incremental? Typical case: computing OHLCV minute bars from a
trades table."*

## What we found in Trino's source code

We explored the Trino codebase (`plugin/trino-iceberg/`, `core/trino-main/`):

- Trino supports incremental MV refresh on Iceberg, but only for
  **scan+filter+project** MVs. The gate is `IncrementalRefreshVisitor.java:31`
  which whitelists `TableScanNode`, `FilterNode`, `ProjectNode`. Any
  `AggregationNode` (i.e. any `GROUP BY`) forces `FULL` refresh.
- Full refresh does `deleteFromRowFilter(alwaysTrue)` + re-insert -- rewrites
  the entire storage table every time.
- For an OHLCV minute-bar MV with `GROUP BY symbol, date_trunc('minute', ts)`,
  every `REFRESH MATERIALIZED VIEW` rescans the entire trades table.

## Design evolution

The design went through several iterations during the conversation:

### v1: Partition-based diff

Initial approach: diff `$partitions` between snapshots to find changed
partitions, then recompute those partitions.

**Problem discovered**: `$partitions FOR VERSION AS OF <snapshot_id>` is not
supported via Trino SQL (the Java API accepts a snapshot ID parameter, but the
SQL planner doesn't route `FOR VERSION AS OF` to system tables).

**Workaround**: stored partition stats (record_count, file_count) alongside
the snapshot ID in the target table's `extra_properties`.

### v2: Partition-based diff with stored stats

Stored `{partition_value: [record_count, file_count]}` as JSON in
`mv.partition_stats` table property. Compared current `$partitions` against
stored stats to find changes.

**Problem discovered by the prompter**: when the GROUP BY granularity is
coarser than the source partition granularity (e.g. weekly bars from daily
partitions), the partition-based filter only reads the changed partition.
A weekly bar computed from only Wednesday's data loses Monday+Tuesday.

This was reproduced in an integration test
(`test_cross_partition_groupby.py`) before fixing.

### v3: File-stats-based detection (current)

Uses `$all_entries.readable_metrics` to read per-column min/max bounds from
newly added files. Snaps the range to GROUP BY bucket boundaries in Python.
Produces a plain column range filter that Trino pushes down to Iceberg
partition pruning.

This approach:
- Does not depend on partition scheme at all
- Correctly handles cross-partition GROUP BY
- Produces pushdown-friendly filters (verified via `EXPLAIN (TYPE IO)`)

## Iceberg metadata tables used

### `$snapshots`

```sql
SELECT snapshot_id, operation, committed_at
FROM source."$snapshots"
ORDER BY committed_at DESC LIMIT 1
```

Returns: snapshot_id (random bigint, NOT sequential), operation (append,
overwrite, delete), committed_at (timestamp). Used to detect if source changed
and whether any non-append operations occurred.

**Important**: snapshot IDs are random -- cannot use `WHERE snapshot_id > X`
to find newer snapshots. Must compare by `committed_at`.

### `$all_entries`

```sql
SELECT readable_metrics
FROM source."$all_entries"
WHERE snapshot_id IN (...) AND status = 1
```

Returns: per-file metadata including `readable_metrics` JSON with per-column
min/max bounds. `status = 1` means ADDED data file. Used to determine the
time range of new data without scanning data files.

`readable_metrics` example:
```json
{
  "ts": {"lower_bound": "2026-04-08T10:00:41+00:00", "upper_bound": "2026-04-08T15:59:50+00:00"},
  "price": {"lower_bound": 138.5, "upper_bound": 260.0},
  "symbol": {"lower_bound": "AAPL", "upper_bound": "TSLA"}
}
```

### `$properties`

```sql
SELECT value FROM target."$properties" WHERE key = 'mv.last_source_snapshot'
```

Used to read/write orchestrator state. Writes via:
```sql
ALTER TABLE target SET PROPERTIES
  extra_properties = MAP(ARRAY['mv.last_source_snapshot'], ARRAY['12345'])
```

Requires `iceberg.allowed-extra-properties` to be configured in the catalog.
`ALTER TABLE SET PROPERTIES extra_properties` **merges** keys (does not
clobber existing properties) -- confirmed via `IcebergMetadata.java:2660`:
`extraProperties.forEach(updateProperties::set)`.

## Key design decisions

### Timezone assumption

Every Trino session the orchestrator opens is pinned to `UTC` via the
aiotrino `timezone` connection parameter. This is a hard requirement,
not a convenience — without it the detector and the user's GROUP BY can
disagree on bucket boundaries and silently corrupt incremental
aggregates.

**Why it matters.** The detector computes the snapped time range in
Python from `readable_metrics` (which Trino returns as ISO-8601 strings
with UTC offsets) and ships a plain
`ts >= TIMESTAMP 'A' AND ts < TIMESTAMP 'B'` filter to the MERGE. The
filter selects rows by instant, which is unambiguous. The trap is how
those rows are grouped: for a `TIMESTAMP WITH TIME ZONE` column,
`date_trunc('day' | 'week' | 'month' | …, ts)` operates in the **session
timezone**, not UTC. If the session tz is not UTC, the bucket Trino
assigns to a row differs from the bucket Python computed.

Concrete walk-through with session tz = `America/New_York` (UTC−5),
granularity `day`, a new row at `2026-01-15 02:00:00 UTC`:

- Python's `snap_range` floors to `2026-01-15 00:00 UTC` and ceils to
  `2026-01-16 00:00 UTC`. The MERGE filter is
  `[2026-01-15 00:00 UTC, 2026-01-16 00:00 UTC)`.
- The MERGE reads all rows in that 24-hour UTC window, then the
  `GROUP BY date_trunc('day', ts)` collapses them into **NY-day**
  buckets: `[00:00 UTC, 05:00 UTC)` belong to NY day **2026-01-14**
  (which we never intended to touch), and `[05:00 UTC, 24:00 UTC)`
  belong to NY day **2026-01-15** (missing its final 5 hours,
  `[24:00 UTC, 29:00 UTC)`).
- Both buckets get aggregated from a partial slice, and
  `WHEN MATCHED THEN UPDATE` overwrites previously-correct values with
  wrong ones. Silent corruption.

The same offset-straddling failure mode applies at week/month/quarter/
year — anywhere a "midnight" boundary differs between UTC and session
tz.

**Fix.** Pin `timezone='UTC'` on every `aiotrino.dbapi.connect(...)` in
`server.get_trino_connection`. `date_trunc` on tz-aware columns then
operates in UTC, aligning with `snap_range` by construction.

### Append-only source assumption

The orchestrator assumes source tables are append-only. The only
legitimate Iceberg snapshot operations are:

- `append` — real new data, drives incremental refresh.
- `replace` — compaction rewrote files; no data changed. The detector
  advances state past the snapshot but does not run a refresh. Files
  added by `replace` snapshots are deliberately excluded from the
  `$all_entries` min/max scan (they're just rewritten rows and would
  uselessly expand the range).

Anything else (`overwrite`, `delete`, or an unknown future op name)
raises `UnexpectedOperationError` and surfaces in the view status. The
old behavior — treating every non-append as a full refresh — was wrong
under this assumption because compactions triggered needless full
rewrites.

### Why not dbt?

Discussed during the conversation. dbt's `incremental_strategy='merge'`
generates MERGE SQL, but the user must write the incremental filter logic.
dbt's `{% if is_incremental() %} WHERE ts >= ... {% endif %}` has the same
partial-bucket problem we identified. dbt doesn't solve the hard problem
(correct partition-aligned recompute based on metadata).

### Why MERGE, not DELETE+INSERT?

Trino does NOT support multi-statement transactions on Iceberg. Each DML
auto-commits immediately (`START TRANSACTION` / `COMMIT` / `ROLLBACK` are
syntactic but don't provide atomicity -- verified in integration tests).

MERGE is a single Iceberg commit = atomic. For append-only sources, MERGE
correctly handles both updates (recomputed rows) and inserts (new rows).

### Why file-level stats, not partition diff?

Partition-based detection breaks when GROUP BY spans multiple partitions.
File-level column stats from `readable_metrics` give the exact value range
of new data regardless of partition scheme. Combined with `snap_range()`,
this produces the minimum correct time range for any GROUP BY granularity.

### Why snap_range in Python, not a Trino subquery?

We tested whether Trino pushes down `date_trunc('week', ts) IN (...)`
predicates to Iceberg partition pruning. It does NOT -- `EXPLAIN (TYPE IO)`
shows the predicate becomes a post-scan filter, causing a full table scan.

A plain column range (`ts >= A AND ts < B`) IS pushed down. So we compute
bucket boundaries in Python and produce a plain range filter.

### Why `extra_properties` for state, not a separate table?

State lives with the target table. Drop the table = state gone = full
backfill on recreate. No external coordination. The `extra_properties`
mechanism merges keys (confirmed in source code), so the orchestrator's
property doesn't clobber other properties.

The MERGE and state write are two separate Iceberg commits (Trino has no
multi-statement transactions). Crash between them = redundant refresh on
next cycle. No data loss or corruption.

### Auto-discovery via DESCRIBE OUTPUT

The prompter suggested using `EXPLAIN` to extract column types. We found
that `PREPARE stmt FROM <query>` + `DESCRIBE OUTPUT stmt` gives exact
column names and Trino types without executing the query. This avoids the
need for the user to specify value_columns or column types.

`EXPLAIN (TYPE IO, FORMAT JSON)` provides a structured list of all source
tables referenced by a query -- used to auto-discover source tables.

## What was explored but not built

### Trino engine-level incremental aggregate refresh

We analyzed what it would take to add this to Trino itself:

- Extend `IncrementalRefreshVisitor` to accept `AggregationNode`
  (currently only `TableScanNode`, `FilterNode`, `ProjectNode`)
- Add a new `ConnectorMetadata.applyIncrementalRefreshFilter()` SPI method
- Iceberg connector computes affected partitions via
  `newIncrementalAppendScan().fromSnapshotExclusive()`, returns a
  `TupleDomain` filter
- Engine injects that filter onto the source `TableScanNode` in the MV plan
- Iceberg `finishRefreshMaterializedView` does partition-scoped
  `deleteFromRowFilter` + append

Estimated: 4-6 weeks for MVP + 1-2 months upstream review. The external
orchestrator achieves the same result in ~500 lines of Python.

### Iceberg identifier-field-ids for merge_keys

Iceberg v2 supports `identifier-field-ids` in the schema (unenforced primary
key). These are stored in the metadata JSON but NOT exposed via Trino SQL.
We explored reading them from S3 directly but concluded that `merge_keys`
as a user-provided config field is simpler and more reliable.

### `$partitions FOR VERSION AS OF`

The Iceberg connector's `PartitionsTable.java` accepts an `Optional<Long>
snapshotId` in its constructor, but the Trino SQL planner does not route
`FOR VERSION AS OF` to system tables. Tested on both `trinodb/trino:479`
and the custom `jonasbrami/trino-arrow` build -- same result.

## Implementation details

### snap_range()

`detector.py:snap_range(min_ts, max_ts, granularity)` expands a timestamp
range outward to complete GROUP BY bucket boundaries:

- `minute`: floor to second=0, ceil to next minute
- `hour`: floor to minute=0, ceil to next hour
- `day`: floor to midnight, ceil to next midnight
- `week`: floor to Monday 00:00, ceil to next Monday
- `month`: floor to 1st of month, ceil to 1st of next month

Week uses ISO week (Monday = start). Month handles year boundaries.

### MERGE SQL generation

`executor.py:build_merge_sql()` generates:

```sql
MERGE INTO target AS t
USING (
  <user query with {range_filter} replaced by range>
) AS s
ON t.key1 = s.key1 AND t.key2 = s.key2
WHEN MATCHED THEN UPDATE SET val1 = s.val1, val2 = s.val2, ...
WHEN NOT MATCHED THEN INSERT (key1, key2, val1, val2, ...) VALUES (...)
```

`value_columns` = all query output columns minus `merge_keys`. Discovered
automatically via `DESCRIBE OUTPUT`.

### Snapshot ID comparison

Iceberg snapshot IDs are random int64 values, NOT sequential. The detector
uses `committed_at` timestamp comparison to find snapshots after the last
processed one:

```sql
SELECT snapshot_id, operation FROM $snapshots
WHERE committed_at > (
  SELECT committed_at FROM $snapshots WHERE snapshot_id = <last_processed>
)
```

### Timestamp parsing

`readable_metrics` returns timestamps as ISO strings with varying formats:
`2026-04-08T10:00:41.385604+00:00`, `2026-04-08T10:00:41+00:00`, etc.
The `_parse_ts()` function tries multiple `strptime` formats and falls back
to date-only parsing.

## Test infrastructure

### Docker compose

`tests/docker-compose.yml` runs:
- MinIO (S3-compatible object storage for Iceberg data files)
- PostgreSQL (Iceberg JDBC catalog backend)
- Trino (`jonasbrami/trino-arrow:479-03d1b24` with Iceberg connector)

PostgreSQL is initialized with `init-iceberg-catalog.sql` that creates the
JDBC catalog tables (`iceberg_tables`, `iceberg_namespace_properties`).

### Test organization

Unit tests use mock cursors that return pre-configured results. Integration
tests run against real Trino. `pytest-xdist` parallelizes unit tests across
workers; integration tests are serialized via `xdist_group("integration")`
to avoid table conflicts.

### Key test cases

- **Full refresh correctness**: insert data, full refresh, verify OHLCV values
- **Incremental same-day update**: add trades to existing day, verify MERGE
  updates bars correctly with all data (old + new)
- **Incremental new day**: add data for new day, verify only that day is
  recomputed (existing bars untouched)
- **Cross-partition weekly**: Mon+Tue data, full refresh, add Wed, verify
  weekly bar includes all 3 days (vol=350, not 50)
- **Cross-partition monthly**: Apr 1+15 data, add Apr 20, verify monthly
  bar includes all 3 trades
- **New data in different week**: verify previous week's bars untouched
- **No-op skip**: no new snapshots = no queries beyond `$snapshots`
- **State roundtrip**: write/read `last_source_snapshot` via `$properties`
