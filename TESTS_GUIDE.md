# Test Suite Guide

What every test verifies, grouped by file and class. Read alongside the source it covers — this guide is *what* and *why*, the test code is *how*.

---

## Layout

```
tests/
├── unit/           303 fast tests, mocked Trino cursor, no I/O
│   ├── test_cli.py
│   ├── test_config.py
│   ├── test_detector.py
│   ├── test_executor.py
│   ├── test_introspect.py
│   ├── test_server.py
│   └── test_state.py
└── integration/    ~10 tests, real Trino + Iceberg + MinIO via docker compose
    ├── test_refresh.py
    └── test_cross_partition_groupby.py
```

Both layers are **necessary**: unit tests pin the algorithm and SQL shapes, integration tests prove that Trino actually behaves the way the unit tests assume.

---

## Unit Tests

### `test_cli.py` — argument parsing

Covers `cli.py:main()`. Three tests, all use `monkeypatch` to replace `uvicorn.run`.

| Test | What it checks |
|---|---|
| `test_default_config_path` | When no `-c` flag, defaults to `config.yaml` and the server starts on the configured port |
| `test_custom_config_path` | `-c custom.yaml` is honoured and the file is loaded |
| `test_verbose_sets_debug` | `-v` flips uvicorn log level to `debug` |

### `test_config.py` — YAML loading + identifier validation

Covers `config.py`. Query-shape validation lives in `test_query_parser.py`; this file focuses on the YAML envelope (identifier checks, defaults, round-trip).

**Loading & defaults**
- `test_load_valid` — happy-path config round-trip
- `test_missing_trino` — `trino:` section is required
- `test_defaults` — server port and reload interval defaults applied
- `test_load_views_valid` — view list loads with only `name` + `query`
- `test_load_views_missing_file` — clear error if `views.yaml` doesn't exist
- `test_view_defaults` — `refresh_interval_seconds` defaults to 60

**Validation — identifier shape**
- `test_missing_query` / `test_missing_name` — required fields
- `test_invalid_view_name` — non-identifier view names rejected (would break SQL generation)
- `test_invalid_target_table` — `catalog.schema.table` shape enforced on the optional override
- `test_legacy_range_filter_placeholder_rejected` — any `{range_filter}` in a loaded query is rejected with a migration-pointing error
- `test_duplicate_view_names_rejected` — two views with the same name rejected

**Round-trip persistence**
- `test_save_views_and_reload` — `save_views` then `load_views` preserves every field
- `test_save_views_creates_parent_dirs` — writing to a fresh path makes the directory

### `test_query_parser.py` — AST-based query parsing

Covers `query_parser.py`. This is where the orchestrator now decides whether a view query is supported.

**Happy path** — every shape the parser extracts correctly:
- `test_full_shape` — end-to-end ParsedView from a typical SELECT
- `test_every_granularity` — parametric over all seven: minute/hour/day/week/month/quarter/year
- `test_qualified_table_name` — `cat.sch.tbl` survives round-trip
- `test_group_by_expression_matches_projection` — `GROUP BY date_trunc(...)` resolves to the aliased projection entry
- `test_group_by_positional` — `GROUP BY 1, 2` resolves via projection index
- `test_bare_column_alias_is_its_own_name` — `GROUP BY d` referencing an aliased projection entry

**Rejections** — every shape the parser must refuse:
- `test_no_date_trunc` — required for granularity inference
- `test_date_trunc_in_arithmetic` / `test_date_trunc_multiplied` — structural Operation-parent check
- `test_multiple_granularities` / `test_date_trunc_on_different_columns` — only one filter column/granularity per view
- `test_date_trunc_inside_string_literal_is_not_matched` / `test_date_trunc_inside_line_comment_is_not_matched` / `test_date_trunc_inside_block_comment_is_not_matched` — where the regex-based predecessor would false-match
- `test_join_rejected` / `test_union_rejected` / `test_with_cte_rejected` / `test_subquery_in_from_rejected` — unsupported query shapes
- `test_no_group_by_rejected` — correctness model requires it
- `test_legacy_range_filter_placeholder_rejected` — explicit migration error for old views
- `test_projection_function_without_alias_rejected` — every computed column needs `AS name`
- `test_multi_statement_rejected` / `test_empty_query_rejected` / `test_invalid_granularity_rejected` — sundry hygiene

**`inject_range_filter`** — the refresh-time WHERE injection:
- `test_appends_onto_existing_where` — AND-joins onto the operator's WHERE
- `test_inserts_new_where_when_absent` — adds one when the query had none
- `test_places_before_having` / `_order_by` / `_limit` — WHERE lands in the right structural position
- `test_naive_datetime_omits_utc_suffix` / `test_tz_aware_non_utc_converted_to_utc` — timezone handling
- `test_result_parses_as_valid_query` — round-trip: inject, re-parse, same ParsedView

### `test_detector.py` — change detection

Covers `detector.py`. This is where the orchestrator's correctness is mostly proven.

#### `TestSnapRange` — handcrafted boundary cases

12 tests, one per scenario, asserting the exact `(start, end)` pair returned for known inputs:

| Test | Scenario |
|---|---|
| `test_minute` | min/max within the same minute snaps to `[minute, next_minute)` |
| `test_hour` | snaps to full hour, possibly spanning to the next hour |
| `test_day` | snaps to midnight UTC boundaries |
| `test_week` | week starts on Monday; `Wed → Mon..Mon` |
| `test_week_spanning_two_weeks` | range crossing Sunday→Monday spans both weeks |
| `test_month` | snaps to first-of-month |
| `test_month_spanning_year_boundary` | Dec → Jan handled correctly |
| `test_quarter` | Q1=Jan/Q2=Apr/Q3=Jul/Q4=Oct boundaries |
| `test_quarter_spanning_two` | Mar→Apr spans Q1 and Q2 |
| `test_quarter_year_boundary` | Q4→Q1 of next year |
| `test_year` | snaps to Jan 1 boundaries |
| `test_year_spanning` | Dec→next-Jan spans two years |

#### `TestSnapRangeInversesDateTrunc` — property-based

Three properties, each parameterised over **every granularity × every sample timestamp pair**. This is the strongest correctness guarantee in the codebase: it asserts that `snap_range` is the **mathematical inverse** of `date_trunc`.

- `test_boundaries_are_bucket_aligned` — `date_trunc(start) == start` and `date_trunc(end) == end`. (start/end land exactly on bucket boundaries)
- `test_touched_buckets_are_fully_covered` — every bucket containing any source row is fully inside `[start, end)`. (no partial bucket = no wrong aggregate)
- `test_range_is_tight` — `start == date_trunc(min_ts)`. (we don't expand further than needed)

If you ever change `snap_range`, these are the tests to keep green.

#### `TestParseTs` — timestamp string parsing

- `test_iso_with_tz` / `test_iso_no_tz` — both common shapes parse to a datetime
- `test_raises_on_unparseable` — fail loudly on garbage (the old code silently fell back to date-only, shifting the range up to 24h)
- `test_raises_on_nanosecond_precision` — 9-digit fractional seconds exceed `strptime`'s `%f` budget; old code dropped the time component, new code raises

#### `TestGetCurrentSnapshot`

- `test_returns_id` — extracts snapshot id from `$snapshots`
- `test_returns_none` — empty source → no snapshot

#### `TestGetSnapshotsSince`

- `test_raises_on_missing_last_snap` — if `last_snap` is no longer in `$snapshots` (Iceberg expired it), raises `ExpiredSnapshotError` instead of returning `[]` (the old behavior would freeze the view forever)
- `test_sql_uses_snapshot_id_tiebreak` — the generated SQL uses `(committed_at, snapshot_id)` for the tiebreak so two snapshots committed in the same millisecond aren't dropped

#### `TestGetNewFilesColumnRange` — extracting min/max from `$all_entries`

- `test_computes_range` — happy path: parses `readable_metrics` JSON, returns `(min_dt, max_dt)`
- `test_no_data_files` — empty entries → `None`
- `test_raises_when_filter_column_absent_from_metrics` — `MissingFilterColumnError` (configuration error, must not silently freeze)
- `test_min_max_ignores_lex_order` — **the lexicographic-vs-chronological bug**. Two files whose ISO strings sort one way but whose actual instants sort the other. Asserts the result is computed on instants, not strings.

#### `TestDetectChanges` — the top-level decision function

- `test_no_change_same_snapshot` — current == last → `NO_CHANGE`
- `test_full_refresh_first_run` — `last is None` → `FULL_REFRESH`
- `test_incremental_with_range` — new append snapshot → `INCREMENTAL` with snapped range
- `test_incremental_week_granularity` — same as above but with `'week'` granularity (Apr 8 Wed → Mon..Mon range)
- `test_no_data_files_in_new_snapshots` — append snapshot with no entries → `NO_CHANGE`
- `test_compaction_only_no_change_advances_state` — only `replace` ops since last snap → `NO_CHANGE` with the **advanced** snapshot id, and **no** `$all_entries` query (the file-stats lookup is skipped)
- `test_mixed_append_and_replace_uses_only_append_snapshots` — when an append and a compaction are both new, the file-stats query is scoped to the append snapshot only (`IN (50)`, not `IN (50, 51)`) — compaction-rewritten files would uselessly inflate the range
- `test_unexpected_operation_raises` — `overwrite` / `delete` / unknown op → `UnexpectedOperationError`. Enforces the project's append-only assumption.

### `test_executor.py` — SQL generation + execution

Covers `executor.py`.

#### `TestBuildRangeFilter`

- `test_basic` — produces `ts >= TIMESTAMP '...' AND ts < TIMESTAMP '...'`
- `test_pushdown_friendly` — the filter contains no `date_trunc` or `CAST` calls (so Trino can push it down to partition pruning)
- `test_converts_non_utc_to_utc` — **the timezone bug**. A datetime in `+02:00` must be converted to its UTC instant before being formatted as a `TIMESTAMP '... UTC'` literal. The old code formatted the wall-clock value and tacked on `" UTC"`, silently shifting the instant.

#### `TestBuildMergeSql`

- `test_structure` — generated MERGE has `MERGE INTO target AS t`, the right `ON` clause from merge keys, both `WHEN MATCHED UPDATE` and `WHEN NOT MATCHED INSERT`, and the full injected source query.

#### `TestFormatTs`

- `test_microsecond_precision` — `format_ts` emits 6-digit microseconds (matches Trino `TIMESTAMP(6)` literal precision).

#### `TestRefreshResult`

- `test_dataclass_fields` / `test_defaults` — sanity on the result shape returned by execute_*.

#### `TestExecuteFullRefresh` / `TestExecuteIncrementalRefresh`

- `test_returns_refresh_result_with_stats` — pulls `processedRows` / `processedBytes` off the Trino cursor stats and returns them in `RefreshResult`
- `test_returns_zero_stats_when_missing` — missing stats default to 0 (not crash)

### `test_introspect.py` — auto-discovery

Covers `introspect.py`.

#### `TestDiscoverSourcePartitioning` (parses `SHOW CREATE TABLE` output)

- `test_partitioned_table` — single-column partitioning extracted as `ARRAY['day(ts)']`
- `test_multi_column_partitioning` — `ARRAY['day(ts)', 'bucket(16, id)']` preserved verbatim
- `test_not_partitioned` — returns `None` (not an error)
- `test_whitespace_variations` — tolerant of irregular spacing in the DDL

#### `TestDiscoverColumns` (PREPARE + DESCRIBE OUTPUT)

- `test_basic` — runs PREPARE/DESCRIBE/DEALLOCATE, returns `[ColumnInfo(name, type), ...]`, passes the query verbatim (no placeholder substitution)

#### `TestDiscoverSourceTables` (parses EXPLAIN JSON)

- `test_single_source` — extracts `catalog.schema.table` from EXPLAIN's `inputTableColumnInfos`
- `test_multiple_sources_deduped` — returns sorted, deduplicated list

#### `TestBuildCreateTableSql`

- `test_with_column_info` / `test_with_tuples` — both `[ColumnInfo, ...]` and `[(name, type), ...]` accepted
- `test_with_partitioning` — when partitioning is supplied, it shows up in the WITH clause

### `test_server.py` — FastAPI app + refresh orchestration

Covers `server.py`.

**Plain endpoints**
- `test_health` — `/health` returns 200 with the view count
- `test_metrics` — `/metrics` returns Prometheus text format
- `test_view_schema` — `/api/views/schema` returns the dynamic form schema for the UI
- `test_ui` — `/` serves `index.html`

**Views CRUD**
- `test_list_views` — GET `/api/views` lists configured views with status
- `test_create_view` — POST creates a view, persists to `views.yaml`
- `test_create_view_invalid_name` — bad identifier rejected with 400
- `test_create_duplicate` — duplicate name rejected with 409
- `test_create_view_accepts_valid_query` — granularity inference happens at the API too
- `test_create_view_fails_when_cannot_infer` / `_fails_on_complex_expr` — bad queries rejected at create time
- `test_delete_view` / `test_delete_not_found` — DELETE works and 404s appropriately
- `test_trigger_refresh` / `test_trigger_refresh_not_found` — POST `/api/views/{name}/refresh` calls `refresh_view` once

**Config reload**
- `test_reload_config_on_mtime_change` — when `views.yaml` mtime changes, config is reloaded and new views appear in state
- `test_reload_config_no_change` — unchanged mtime → reload skipped

**Trino session pinning**
- `test_get_trino_connection_pins_timezone_to_utc` — **the session-tz bug**. Asserts that every `aiotrino.dbapi.connect(...)` call passes `timezone="UTC"`. Without this, `date_trunc` on `TIMESTAMP WITH TIME ZONE` columns uses the session's tz, while the Python `snap_range` math runs in UTC — they disagree and partial buckets get recomputed with wrong aggregates.

**State advance on NO_CHANGE**
- `test_refresh_view_advances_state_on_empty_append_no_change` — when `detect_changes` returns `NO_CHANGE` but `current_snapshot` has moved (compaction or empty-append), `write_last_snapshot` is still called. Otherwise the view re-detects the same snapshots forever.

**Metrics presence**
- `test_new_metrics_defined` — guards against accidentally removing `REFRESH_BYTES`, `REFRESH_ROWS`, `DETECTION_DURATION`, `SOURCE_SNAPSHOT`.

### `test_state.py` — snapshot persistence

Covers `state.py`.

- `test_state_uses_detector_system_table` / `test_unqualified_table` — both modules share one `system_table()` helper (no duplication of the `"name$properties"` quoting rule)
- `TestReadLastSnapshot.test_returns_id` — reads from `target."$properties"`, parses int
- `TestReadLastSnapshot.test_returns_none` — empty result → `None`
- `TestWriteLastSnapshot.test_writes_alter` — emits `ALTER TABLE … SET PROPERTIES` containing the snapshot key and value

---

## Integration Tests

Both files are decorated with `pytest.mark.integration` and `pytest.mark.xdist_group("integration")` — they share one Trino + Iceberg + MinIO docker stack and serialize execution to avoid table-name collisions. Run them with:

```
docker compose -f tests/docker-compose.yml up -d
pytest tests/integration
```

### `test_refresh.py` — end-to-end happy paths

Source is `iceberg.test_schema.trades` partitioned by `day(ts)`. Target is a 1-minute OHLCV view.

#### `TestIntrospection`
- `test_discover_columns` — `discover_columns(query)` returns 8 columns matching the SELECT list

#### `TestFullRefresh`
- `test_first_run` — Insert 4 trades (3 same minute + 1 next minute, across 2 days). Detector returns `FULL_REFRESH`. After execute, target has 3 minute bars; the busy minute has the right `high`, `volume`, `trade_count`.

#### `TestIncrementalRefresh`
- `test_new_day` — Full refresh, persist snapshot. Insert a trade on a new day. Detector returns `INCREMENTAL` with a `filter_range`. After execute, target has 2 bars total. Verifies that data from the previous day isn't lost.
- `test_same_day_update` — Full refresh on minute X. Insert a second trade in **the same minute**. Detector returns `INCREMENTAL`. After execute, the existing row is `MERGE`d (not duplicated) and `high`/`volume`/`trade_count` reflect both trades.

#### `TestState`
- `test_roundtrip` — `write_last_snapshot(99999)` followed by `read_last_snapshot()` returns `99999` (asserts that Iceberg `extra_properties` actually persists across statements)

#### `TestNoChangeSkip`
- `test_skip` — Two `detect_changes` calls in a row with the same `last_snapshot` — the second returns `NO_CHANGE`

### `test_cross_partition_groupby.py` — the correctness flagship

These tests are the **canonical proof** that `snap_range` works in the real world. They use coarse aggregations (week/month) on a daily-partitioned source — exactly the case where a naive filter would corrupt aggregates.

#### `TestWeeklyBarsCrossPartition`
- `test_incremental_refresh_preserves_all_days` — the headline test:
  1. Insert Mon + Tue trades (same week). Full refresh → weekly bar volume = 300.
  2. Insert Wed trade. Detect_changes → `INCREMENTAL` with `filter_range = [Mon, next-Mon)`.
  3. Run incremental refresh.
  4. Assert weekly bar volume = **350** (not 50 — i.e. Mon+Tue weren't dropped).
  
  **Without `snap_range`**, this test fails because the MERGE would only see Wed's trade and rebuild the bar with volume=50.

- `test_new_data_in_next_week` — Insert week-1 data, full refresh. Insert week-2 data, incremental refresh. Assert filter range covers only week 2 (`[Apr 13, Apr 20)`) and week 1's bar is untouched.

#### `TestMonthlyBarsCrossPartition`
- `test_incremental_refresh_reads_full_month` — same idea at month granularity:
  1. Insert Apr 1 + Apr 15. Full refresh → monthly bar volume = 30.
  2. Insert Apr 20. Incremental refresh.
  3. Assert filter range = `[Apr 1, May 1)` and final monthly volume = 35.

---

## Reading these tests as documentation

Three classes worth opening before changing anything:

1. **`TestSnapRangeInversesDateTrunc`** (`test_detector.py:218`) — the property tests that pin the math. If you touch `snap_range`, run these first.
2. **`TestDetectChanges`** (`test_detector.py:412`) — the truth table for the orchestrator's decision function. Eight cases that together describe every legitimate state transition.
3. **`TestWeeklyBarsCrossPartition`** (`test_cross_partition_groupby.py:74`) — what correctness looks like end-to-end. If a refactor breaks this, you've broken the product.
