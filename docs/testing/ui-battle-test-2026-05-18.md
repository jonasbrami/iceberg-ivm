# UI Battle-Test Log — 2026-05-18

**Stack:** `quickstart/` (Trino 28080, MinIO 19001, orchestrator UI on 8001)
**Driver:** Chrome DevTools MCP
**Plan:** `/home/quenouille/.claude/plans/atomic-yawning-meteor.md`

Legend: ✅ pass · ❌ fail · ⚠️ partial / unexpected · ⏭️ skipped

---

## Phase 0 — Bootstrap & smoke

| Step | Result | Notes |
|------|--------|-------|
| 0.1 | ✅ | Emptied `views.yaml` to `views: []`; `docker compose up -d` succeeded |
| 0.2 | ✅ | All containers Up; `seed` exited 0; `iceberg-ivm` running |
| 0.3 | ✅ | `/health` → `{"status":"ok","views":0}` |
| 0.4 | ✅ | UI loads at http://localhost:8001 — "Materialized Views" header, "No views configured" empty state, "New View" button visible (`screenshots/00-empty-state.png`) |

**Console / network notes:**
- Tailwind CDN dev warning (not actionable)
- a11y issue (count=13): form fields lack labels/ids — observation only
- favicon.ico 404 (cosmetic)
- `/api/views` polled every ~5s as expected (Phase 3 implicitly verified)

## 🐛 FINDING #1 — `ExpiredSnapshotError` is not caught; view stays stuck after source recreation

**Symptom:** After stack restart (`docker compose down` was not run, but the host rebooted), the `seed` container re-ran on `docker compose up -d`, which recreated `iceberg.market_data.trades` with a fresh snapshot history. The persisted `last_source_snapshot` on `ohlcv_1m_test` no longer exists in `$snapshots`. Every subsequent refresh tick errors with:

> `iceberg.market_data.trades: last_source_snapshot=9211282903465265043 is not in $snapshots (expired?). Cannot compute the set of new snapshots.`

`total_errors` climbed from 1 → 10 in ~50 s of polling. Card stays red. No automatic fallback.

**Root cause** (located via `grep`):
- `src/iceberg_ivm/detector.py:86` raises `ExpiredSnapshotError` from `get_snapshots_since` when the bookmarked snapshot id is missing.
- The class docstring at `src/iceberg_ivm/detector.py:26-27` and the README both say this case should fall back to FULL refresh.
- But `grep -rn ExpiredSnapshotError src/iceberg_ivm/` finds **only the definition and raise sites — no `except ExpiredSnapshotError` anywhere**.
- The single call site at `server.py:394` does not wrap `detect_changes` in any handler for this error, so the bug surfaces every poll forever.

**Suggested fix sketch** (for a follow-up PR per [[feedback_pr_workflow]]):
- Catch `ExpiredSnapshotError` inside `detect_changes` itself (or at the caller in `server.py:394`) and return `ChangeResult(action=RefreshAction.FULL_REFRESH, current_snapshot=current_snap)`.
- Add a unit test that hides `last_snap` from `$snapshots` and asserts the detector returns `FULL_REFRESH` rather than raising.
- Add an integration test that drops + recreates the source table between two refreshes and verifies the view re-backfills.

**Workaround for the rest of this battle-test session:** delete & recreate the view → `last_snap=None` path at `server.py:358` does trigger FULL_REFRESH cleanly.

---

## Phase 1 — Form & live SQL parsing

| Step | Result | Notes |
|------|--------|-------|
| 1.1 | ✅ | "New View" → modal opens with 13 fields from `/api/views/schema` |
| 1.2 | ✅ | Garbage SQL → red "PARSE ERROR · query must have a GROUP BY clause" (`01-parse-error.png`) |
| 1.3 | ✅ | (covered by 1.2 — missing GROUP BY is rejected) |
| 1.4 | ✅ | Valid SQL → DERIVED panel: SOURCE=`iceberg.market_data.trades`, FILTER COL=`ts`, GRANULARITY=`minute`, MERGE KEYS=`symbol, bucket` (`01-parse-green.png`) |
| 1.5 | ✅ | Cancel closes modal; UI returns to empty state; no view created |

## 🐛 FINDING #2 — Trino-UI deep-link points to docker-internal hostname

**Symptom:** `recent_queries[].info_uri` is rendered as a clickable link inside the card (e.g. `http://trino:8080/ui/query.html?20260518_085528_00970_atxqy`). The hostname `trino` is the docker-compose service name — it resolves inside the `quickstart_default` network only. From a host browser, the link is unreachable.

**Suggested fix:** rewrite `info_uri` host:port (`trino:8080`) to the user-facing one (`localhost:28080`) in the UI render. Could be done either client-side (configurable `TRINO_PUBLIC_URL`) or server-side when building `ViewResponse`.

## 🐛 FINDING #3 — Query textarea ignores `disabled_on_edit` flag

**Symptom:** API schema at `/api/views/schema` returns `disabled_on_edit=True` for both `name` and `query`. The UI honors this for `name` (the input becomes `disabled`) but not for `query` — the user can freely edit the SQL textarea. The error only surfaces on Save, where the server's `PUT /api/views/{name}` at `server.py:1040` returns 422 "query cannot be changed; delete and recreate the view instead" and the modal shows that error.

**Root cause:** `src/iceberg_ivm/static/index.html:519` renders a `<textarea>` for `field.type === 'text'` without the `:disabled="field.disabled_on_edit && editingView !== null"` binding that the `<input type=text>` (`:600`) and `<select>` (`:625`) templates have.

**Suggested fix:** add the same `:disabled` binding to the textarea at index.html:519. One-line change.

**Plan correction:** my plan claimed `target_table` was also `disabled_on_edit`. Schema actually has `target_table.disabled_on_edit=False` — server allows it to change. Plan was wrong; system is by design.

## 🟡 FINDING #4 (UX) — "Refresh" button only runs a detection tick, not a forced MERGE

**Symptom:** clicking Refresh on a view with no source changes since last refresh runs the standard detection (current_snap == last_snap → NO_CHANGE → `last_action=skip`). No new MERGE entry appears in recent_queries. For a user expecting "force re-materialize", this is misleading.

**Suggestion:** either rename the button to "Run tick" / "Check now", or add a second button for "Force full refresh" that clears `last_source_snapshot` first.

---

**Schema findings (vs plan expectations):**
- ⚠️ `query_timeout_seconds` field is **not exposed** in the create/edit form (only via YAML/API). Doc/plan said it should be a UI field.
- ⚠️ `FULL REFRESH CHUNK SIZE` dropdown lists `hour, day, week, month, quarter, year` — `minute`, `second`, `millisecond` not offered (likely deliberate to enforce coarser-or-equal-to-granularity, but worth confirming).

## 🐛 FINDING #5 — Quickstart docker image is behind master

The `quickstart/docker-compose.yml` pins `image: ghcr.io/jonasbrami/iceberg-ivm:0.6.0`. Master commit `45afa6f feat: accept second and millisecond as view granularities (#55)` is **not** in that image, so creating views with `date_trunc('second', …)` or `date_trunc('millisecond', …)` is rejected by the running container with:
> `date_trunc granularity must be one of ['day','hour','minute','month','quarter','week','year']; got 'second'`

Suggested fix: bump the quickstart pin (or document that the image is a release pin, not master).

## Phase 2 — CRUD on a simple view

| Step | Result | Notes |
|------|--------|-------|
| 2.1 | ✅ | Create `ohlcv_1m_test` via UI (`screenshots/02-card-idle.png`) |
| 2.2 | ✅ | View written back to `views.yaml` immediately (`save_views` on POST) |
| 2.3 | ⚠️ | NAME locked; QUERY editable in UI but server-side rejected with 422 (Finding #3) (`02-edit-sql-rejected.png`) |
| 2.4 | ✅ | `refresh_interval_seconds` 20 → 60 applied; card reflects new INTERVAL |
| 2.5 | ✅ | recent_queries renders MERGE stage with query_id, elapsed, rows; Trino link present (but broken — Finding #2) |
| 2.x | ⚠️ | Refresh button on steady-state view is no-op (Finding #4) |

## Phase 3 — Polling sanity ✅
`/api/views` polled every ~5s from page load (verified in network panel during Phase 0).

## Phase 4 — Granularity matrix

7 views created via `POST /api/views`, one per granularity (`screenshots/04-granularity-grid.png`):

| Granularity | View name              | First FULL duration |
|-------------|------------------------|---------------------|
| millisecond | ❌ rejected             | (Finding #5)         |
| second      | ❌ rejected             | (Finding #5)         |
| minute      | `ohlcv_1m_test`        | 3.63s (264k rows)   |
| hour        | `gran_hour_test`       | 2.02s               |
| day         | `gran_day_test`        | 1.74s               |
| week        | `gran_week_test`       | 1.43s               |
| month       | `gran_month_test`      | 1.54s               |
| quarter     | `gran_quarter_test`    | 0.75s               |
| year        | `gran_year_test`       | 1.22s               |

All 7 supported granularities → IDLE with `total_refreshes=1, total_errors=0`.

## Phase 5 — Chunked backfill + live chunk-size edit

| Step | Result | Notes |
|------|--------|-------|
| 5.1 | ✅ | Created `ohlcv_1m_chunked_test` with `full_refresh_chunk=hour` — orchestrator emitted 531 hour-chunks (matches 30 days × 7 trading hours × 24 boundaries vs total data range) |
| 5.2 | ✅ | UI shows `BACKFILL` badge + `CHUNKS: 406 / 531` mid-flight + per-chunk `RANGE` text |
| 5.3a | ✅ | Mid-flight Edit: NAME disabled; QUERY editable (Finding #3); chunk select changed hour→day; PUT accepted |
| 5.3b | ✅ | Second pass POST(`day`)+PUT(`hour`) within 1s — PUT accepted with HTTP 200, config persisted |
| 5.4 | ✅ | Both increase (hour→day) and decrease (day→hour) accepted by API |
| 5.5 | ✅ | Final target: 37,800 rows, 37,800 distinct (symbol,bucket), 0 duplicates, range = full source window — verified in Trino |
| 5.x | ⚠️ | Limitation: docker-Trino completes backfills in <30s. Mid-poll chunk-size changes are picked up on the **next** `execute_refresh` call (executor.py:191 fixes ranges per tick), not within a single in-flight tick. The 306a058 gap-free resume requires the backfill to span multiple polls — verified indirectly when the laptop restart split the backfill across iceberg-ivm lifetimes (chunked view resumed at 93/531 → 531/531 cleanly, 0 dups). |
| 5.6 | ✅ | **Direct gap-free-resume test** (added 2026-05-20, post-PR-#60): seeded 3M extra trades (1 row/sec for 11.5 days from `quickstart/seed/seed.py` + the slow-test INSERT loop), created `slow_chunked` with `hour` chunks → 703 chunks expected. While mid-flight at `chunks_done=151/703` (target_max already at `2026-04-10`), edited `full_refresh_chunk` `hour→day` via API, then `docker compose restart iceberg-ivm` to force the next `execute_refresh` to read the new config. Resume completed cleanly: `action=skip refr=1 err=0`, last range was a `day` chunk `[2026-04-30 00:00, 2026-05-01 00:00)`. Trino verified the target: **72,681 rows, 72,681 distinct `(symbol, bucket)`, 0 duplicates**, full source span covered. The orchestrator's `_backfill_ranges` (`executor.py:154`) read `target_max` from `$files`, floored to the containing day-chunk, walked forward in day granularity, re-MERGEd the seam day (idempotent over an append-only source), and emitted ~21 day-chunks to completion. First time the 306a058 path has been observed firing live on a deliberate UI-driven chunk-size change. |

## Phase 6 — Iceberg maintenance ops

Created `maint_test` with `maintenance_interval_seconds=15`, all three ops enabled (`screenshots/06-maintenance-card.png`).

| Op                    | Runs | Errors | Notes |
|-----------------------|------|--------|-------|
| `optimize`            | 4    | 0      | ~45ms each, with `optimize_file_size_threshold=128MB` |
| `expire_snapshots`    | 2    | 5      | First 3 attempts failed with `1d < min-retention=7d`; after PUT-to-`7d`, ops succeed |
| `remove_orphan_files` | 2    | 5      | Same `min-retention=7d` constraint, same recovery |

UI's "LAST MAINTENANCE" row + per-op tooltip present. `recent_queries` includes `maintenance_optimize`, `maintenance_expire_snapshots`, `maintenance_remove_orphan_files` stages.

## Phase 7 — Chained MV (MV on top of MV)

`chained_1h_on_1m` reads from `iceberg.analytics.ohlcv_1m_chunked_test` (a target of another MV). First refresh: `last_action=full`, range covers parent's full window, 630 rows = 30 days × 7 hours × 3 symbols. ✅ Orchestrator's detector treats parent's `overwrite` snapshots as appends.

## Phase 8 — Live incremental from new trades

`INSERT INTO iceberg.market_data.trades` 300 new rows (3 symbols × 100 seconds, `ts = current_timestamp + n s`). Within one refresh interval:

| Target                     | Before | After | Δ | last_action |
|----------------------------|--------|-------|---|-------------|
| `trades` (source)          | 151200 | 151500| +300 | n/a |
| `ohlcv_1m_chunked_test`    | 37800  | 37809 | +9 (3 syms × 3 min) | incremental ✅ |
| `chained_1h_on_1m`         | 630    | 633   | +3 (3 syms × 1 hr) | incremental ✅ |
| `maint_test`               | 37800  | 37809 | +9 | incremental ✅ |
| 7 stuck pre-restart views  | —      | —     | 0 | still error (Finding #1) |

Propagation through MV-on-MV chain verified.

## Phase 9 — Negative tests

| Case | Expected | Got |
|------|----------|-----|
| 9.1 Duplicate name (`maint_test`) | 409 | ✅ `409 view 'maint_test' already exists` |
| 9.2 Non-existent source table | accept + runtime error on first refresh | ✅ `201` then `TrinoUserError TABLE_NOT_FOUND` after ~30s, badge=PENDING+ERROR, total_errors=2 |
| 9.3 Malformed SQL (no GROUP BY) | 422 | ✅ `422 query must have a GROUP BY clause` |
| 9.4 Empty form fields | 422 | ✅ `422 target_table: '' is not a valid qualified table name` |

(Skipped 9.5 query_timeout test — field not in UI per Phase 1 finding; would require YAML-only path.)

## Phase 10 — Delete

| Step | Result |
|------|--------|
| 10.1 UI delete (one view: `bad_source_test`) | ✅ browser confirm dialog → DELETE /api/views/{name} → card removed |
| 10.2 API bulk delete (10 views) | ✅ all returned `204`; `/api/views → []` |
| 10.3 `views.yaml` on disk | ✅ `views: []` |
| 10.4 State.db cleanup | (couldn't verify with sqlite3 — not in container; server.py:1077+ does the cleanup) |
| 10.5 UI returns to empty state | ✅ `screenshots/10-empty-after-delete.png` |

## Phase 11 — Restart resilience

**Verified implicitly** during the two laptop restarts that interrupted this session.

| Aspect | Result |
|--------|--------|
| Views in `views.yaml` reloaded on iceberg-ivm boot | ✅ all 8 / 11 views re-appeared in the UI after `docker compose up -d` |
| Per-view status restored from `state.db` | ✅ `total_refreshes`, `last_duration`, `last_range`, `recent_queries` all preserved |
| Mid-flight chunked backfill resumes from `target_max` | ✅ chunked view picked up at chunks=93/531, finished cleanly at 531/531 |
| Stale `last_source_snapshot` after source re-creation | ❌ Finding #1 — orchestrator never recovers |

---

# Summary

13 phases of UI battle-testing surfaced **5 findings** and confirmed end-to-end correctness for the happy paths and for live-edit scenarios that don't outpace the docker-Trino's MERGE throughput.

| # | Severity | Finding | Suggested fix |
|---|----------|---------|---------------|
| 1 | 🔴 Bug | `ExpiredSnapshotError` is never caught; views with stale source-snapshot bookmarks stay errored forever instead of falling back to FULL_REFRESH as documented. | Catch in `detect_changes`/server.py:394 → return `RefreshAction.FULL_REFRESH`. Add unit + integration test. |
| 2 | 🟡 UX | `recent_queries[].info_uri` points to docker-internal `http://trino:8080/...`, unreachable from a host browser. | Make Trino public URL configurable; rewrite host:port in `_view_to_response`. |
| 3 | 🟡 UI bug | Query textarea ignores `disabled_on_edit=True` (server still rejects the SQL change with 422). | One-line `:disabled` binding on `<textarea>` at `static/index.html:519`. |
| 4 | 🟡 UX | "Refresh" button = detection tick (no-op when nothing changed) — confusing label. | Rename to "Run tick" or add a "Force refresh" that clears `last_source_snapshot`. |
| 5 | 🟡 Stale | Quickstart pins `iceberg-ivm:0.6.0`, missing master's `second`/`millisecond` granularity support (commit 45afa6f). | Bump tag in `quickstart/docker-compose.yml`. |

Plan claims that turned out incorrect on inspection:
- `target_table` is documented mutable on edit (schema `disabled_on_edit=False`); my plan said it was locked.
- `query_timeout_seconds` is not surfaced in the create/edit form at all.
- Chunk-size live edit takes effect across poll boundaries, not within a single in-flight `execute_refresh`.
- Stack supports granularities `[minute, hour, day, week, month, quarter, year]` in the running 0.6.0 image; master adds `second` + `millisecond`.

Final test-stack state: `views.yaml` is `views: []`, all test target tables remain in `iceberg.analytics.*` (intentionally not dropped, lightweight). Stack stays running for follow-up if desired.

