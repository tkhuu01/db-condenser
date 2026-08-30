# AGENTS.md

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks, use judgment.

## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

---

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to overcomplication,
and clarifying questions come before implementation rather than after mistakes.

# Project Guidelines

## Quick Reference

- **Language:** Python 3.10+
- **Package manager:** uv (Astral)
- **Entry point:** `src/db_condenser/direct_subset.py` → CLI command `subset`
- **Config:** `config.json` (see `src/db_condenser/config.json.example_all` for all options)

## Commands

```bash
# Install dependencies
uv sync --frozen

# Run the subsetter
uv run subset            # uses config.json
uv run subset -v         # verbose mode (query timing)
uv run subset --no-constraints  # skip FK constraint restoration

# Lint & format
uv run ruff format --check .
uv run ruff check --select I .   # import ordering
uv run ruff check .

# Fix lint issues
uv run ruff format .
uv run ruff check --fix .

# Run tests (requires PostgreSQL running on localhost:5432, user/pass: test/test)
uv run pytest tests/ -v
```

## Architecture

### Data Flow
1. `direct_subset.py` — CLI entry, parses args, orchestrates pipeline
2. `config_reader.py` — Loads config.json into dataclasses with validation
3. `psql_database_creator.py` / `mysql_database_creator.py` — Schema copy via pg_dump/mysqldump
4. `subset.py` — Core "middle-out" algorithm:
   - Direct targeting (initial_targets with WHERE or percent sampling)
   - Greedy upstream subsetting (tables with FKs to already-imported rows)
   - Pass-through tables (full copy, concurrent with 4 threads)
   - Downstream subsetting (referenced rows needed for FK integrity)
   - Disconnected tables (full copy or empty schema)
5. `result_tabulator.py` — Summary report of row counts

### Key Modules
- `subset_utils.py` — Query building, graph analysis (UnionFind, topo helpers), column selection
- `database_helper.py` — Factory that returns DB-specific helper module
- `db_connect.py` — Connection wrappers (PsqlConnection/MySqlConnection) with LoggingCursor
- `data_masking.py` — PII masking functions (null_out, mask_numbers, mask_characters, mask_email)
- `topo_orderer.py` — Topological sorting for table dependency order

### Upstream/Downstream Strategies

The upstream and downstream subsetting steps each have two code paths, selected by the `use_temp_tables` config flag:

- **`use_temp_tables: false` (default)** — Uses PostgreSQL `unnest()` to pass ID arrays as query parameters. IDs are held in Python memory in 100k-row batches. Only requires read access on the source DB.
- **`use_temp_tables: true`** — Streams IDs from the destination into temporary tables on the source DB, then does a server-side JOIN. Near-zero Python memory for ID lookups. Requires write access on the source (for `CREATE TEMPORARY TABLE`). Temp table columns are `varchar`, so the JOIN casts them to the source column's real datatype (e.g. `col0::int4`).

Shared helpers in `subset.py`:
- `__stream_ids_to_source_temp(dest_query, columns)` — streams ID rows into a source temp table
- `__build_temp_table_join(source_table, id_temp, columns, datatypes, select_expr)` — builds a `SELECT ... JOIN` with type casts

### Row Transfer Strategy

Row transfer from source to destination uses `copy_rows` in `psql_database_helper.py`, selected by the `use_copy_protocol` config flag:

- **`use_copy_protocol: true` (default)** — `copy_rows_copy_protocol` pipes block-level `COPY ... TO STDOUT` straight into `COPY ... FROM STDIN` via a staging table (for `ON CONFLICT` dedup). Significantly faster (5-10x for bulk inserts). JSON values pass through as raw COPY text. Generated columns are excluded via the COPY column list; identity values are preserved via `OVERRIDING SYSTEM VALUE` on the staging insert.
- **`use_copy_protocol: false`** — `copy_rows` uses `executemany` with per-row INSERT statements. Handles JSON columns via `psycopg.Json` wrapping and identity columns via `OVERRIDING SYSTEM VALUE`. Fallback for cases where COPY is unavailable.

The copy function is selected once in `Subset.__init__` and stored as `self.__copy_rows`, used by all 8 call sites.

### Parallel Read

When `parallel_read_workers` > 1, reads are parallelized in three places:

- **Direct targets and pass-through tables** are split by ctid page ranges (below)
- **Upstream/downstream tables that sit alone in their stratum** fan their ID batches out across the source connection pool (`__parallel_id_batches`), instead of processing batches sequentially
- **Table-level concurrency** (multi-table strata, fallback pass-through copies) uses `parallel_read_workers` threads instead of the default 4

ctid page-range splitting:

- Queries `pg_class.relpages` to get total heap pages (instant, no table scan)
- Divides pages evenly among N workers; each worker reads its page range via TID Range Scan
- Each worker: `WHERE ctid >= '(start,0)'::tid AND ctid < '(end,0)'::tid AND (<user_where>)`
- Each worker gets its own source + destination connection; `ON CONFLICT DO NOTHING` deduplicates
- Falls back to single-threaded if the table has fewer pages than `workers * 10`
- Works for any PK type (UUID, text, composite, or no PK at all) — splits by physical storage, not key values

Designed for read-only replicas where parallel reads are safe (AccessShareLock only). Requires PostgreSQL 14+ for TID Range Scan support.

### Unified Source Snapshot

On PostgreSQL, the main source connection exports a snapshot at startup (`pg_export_snapshot()`) and every other source connection — the parallel-read pool and all per-table worker connections — imports it via `SET TRANSACTION SNAPSHOT`, so the entire run reads the source as of one instant regardless of concurrent writes. The main connection's transaction stays open for the whole run to keep the snapshot importable (source-side work is never committed; temp-table contents are session-visible). Works on hot standbys (PostgreSQL 10+).

Config: `"parallel_read_workers": 4` (default `1` = sequential, current behavior)

### Pre-filters

Named queries executed once at subset start, cached in memory, and applied as `AND <column> = ANY(<values>)` to initial targets. Designed for slow/remote data sources (e.g., FDW tables) where re-executing the filter per worker or per target would be expensive.

- Defined in top-level `pre_filters` array (name, query, column)
- Referenced from `initial_targets` via optional `pre_filter` field (by name)
- Multiple targets can share the same pre-filter — query runs once regardless
- Works on read-only replicas (no writes to source)
- Practical limit: ~2M cached values (~140MB memory); beyond that, consider a materialized view

### Incremental Re-Runs (Top-Up and Grow)

When `destination_mode` is `"topup"` or `"grow"` (PostgreSQL only; default is `"recreate"`), the destination is treated as an existing subset and the run adds to it instead of re-transferring everything:

- Destination FK constraints are dropped for the duration of the run (middle-out load order inserts referencing rows before referenced ones) and restored at the end; definitions are retained in `_condenser.fk_backup` and backed up to `SQL/incremental_fk_backup.sql`. PKs and unique indexes stay for conflict handling. Restoration failures fail the command.
- Incremental inserts upsert using the resolved incremental identity from `_conflict_clause`: `ON CONFLICT (<identity>) DO UPDATE`, guarded by `IS DISTINCT FROM` so unchanged rows write nothing. The identity may be the primary key or an eligible unique key. Re-read rows therefore refresh in place (history `enddate`s, soft-delete flags). The COPY-protocol staging insert dedups on the resolved identity (`DISTINCT ON`) first, since one upsert statement cannot affect a row twice.
- Within each incremental copy, refreshes of existing rows are applied before new-row inserts. Both transfer paths stage into the session staging table and share the `apply_staged` refresh-then-insert statements (the single-statement staging insert orders via `ORDER BY EXISTS(...) DESC`; the executemany path stages its fetchmany batches first so pairs spanning batch boundaries are covered). This lets deactivate-and-replace patterns load under a live partial unique index (e.g. one active history row per entity): the stale row is switched off before its replacement arrives. The guarantee covers rows that share an ID batch — which deactivate-and-replace pairs do, since they share the parent entity. It cannot cover a stale row whose source counterpart was hard-deleted (nothing refreshes it); that still errors loudly and requires a recreate run. In incremental runs, ctid-parallel splitting of tables with a secondary unique index or exclusion constraint (detected at `prep_incremental`, see `has_secondary_unique`) runs in two phases: every worker stages its page range and applies refreshes, all workers meet at a barrier, then inserts run (`stage_rows`/`apply_staged`) — same guarantee as a sequential copy at full worker count. Identity-only tables keep the single-pass split. The two-phase path needs `use_copy_protocol` and an identity-tracked delta; otherwise those tables fall back to a sequential copy.
- Every insert records resolved incremental identities into a per-table delta table in the reserved `_condenser` schema (dropped only after a successful run and FK restoration) via `_wrap_insert_with_delta`: `WITH ins AS (INSERT ... RETURNING <identity>, (xmax = 0) AS _inserted) INSERT INTO <delta> ...`. The `_inserted` flag distinguishes genuine inserts from upsert refreshes. Failed runs retain the journal so the next incremental run with the same configuration can resume; a destination advisory lock rejects concurrent incremental runs.
- Upstream subsetting in `"topup"` joins children against delta parent IDs (`_inserted` only, so refreshed old parents don't unfreeze their children) instead of full destination tables — a re-run costs O(new rows). Multi-FK tables run one pass per constraint: that constraint joins its delta, the others join the full ID sets (preserves AND semantics). Tables whose parent deltas have no inserted rows are skipped entirely. In `"grow"`, upstream reads full destination parent ID sets instead, so new children of already-imported parents are picked up — a run costs O(existing subset).
- Downstream subsetting (both modes) joins each child scan against the child's delta — including upsert-refreshed rows, since an update can repoint an FK column at a parent not yet present — so the missing-parent scan also costs O(new rows). Children with empty deltas are skipped; a parent whose children all have empty deltas is skipped entirely.
- Top-up semantics: new initial-target matches and their descendants arrive; already-imported entities stay frozen (new children of old parents are not picked up). Grow semantics: frozen entities are unfrozen — new children/descendants of resident rows arrive too. Hard deletes never propagate in either mode.
- `_resolve_incremental_keys` raises an error when an incremental table has neither a primary key nor an eligible unique key; it does not fall back to non-incremental behavior.
- Incremental preflight rejects a `GENERATED ALWAYS AS IDENTITY` column outside the resolved identity because PostgreSQL cannot update it to the source's explicit value.

### Database Support
- **PostgreSQL:** Full support including sequence reset, named cursors, JSON casting
- **MySQL:** Functional but limited (no sequence reset, no constraint re-application, no cross-db FKs)

## Design Principles

- **Scalability:** Solutions must work efficiently from small tables to massive tables (500M to 1B rows). Avoid approaches that load full result sets into memory, prefer streaming/batching when you think the solution is optimal, and consider query plan impact at scale.
- When streaming/batching are not optimal then think of other means, like storing information in the SQL folder.
- When planning an optimal solution, it should first consider read only replicas so that production master instances are not impacted.
- When planning an optimal solution, it should not only consider PKs as a number.

## Code Style

- Formatter/linter: Ruff (format + isort + lint)
- No type stubs; uses standard type hints
- Dataclasses for config objects with `__post_init__` validation
- Global singleton `config` via `initialize()` / `get_config()`
- Factory pattern for DB-specific implementations
- Batch sizes: 100k rows (PostgreSQL upstream), 1k rows (MySQL)

## Testing

- Integration tests in `tests/psql/test_integration.py`
- Requires PostgreSQL 18 service (see docker-compose.yml or CI config)
- Test DB seeded from `tests/psql/seed.sql`, config in `tests/psql/test_config.json`
- Fixture `subsetter_dbs` is parameterized: every test runs three times (`[unnest]`, `[temp_tables]`, and `[copy_protocol]`)
- Fixtures `rerun_dbs`, `incremental_dbs`, and `grow_dbs` cover re-runs into an existing destination (idempotency, top-up, and grow semantics), also across all three variants; `incremental_composite_dbs` covers the composite-PK downstream delta join
- Each variant gets its own databases (e.g. `condenser_test_source`, `condenser_test_source_temp`, `condenser_test_source_copy`)
- CI runs on GitHub Actions: ruff lint + pytest with Postgres 18 service container
