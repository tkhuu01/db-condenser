# Config

Configuration must exist in `config.json`. Run `subset --example-config` to
print a comprehensive example with all options (redirect it to get started:
`subset --example-config > config.json`). Most of the configuration is
straightforward:
source and destination DB connection details and subsetting settings.
There are three fields that deserve some additional attention.

The first is `initial_targets`. This is where you tell the subsetter to begin
the subset. You can specify any number of tables as an initial target, and
provide either a percent goal (e.g. 5% of the `users` table) or a WHERE clause.

Next is `dependency_breaks`. The subsetting tool cannot operate on databases
with cycles in their foreign key relationships. (Example: Table `events`
references `users`, which references `company`, which references `events` — a
cycle exists if you think about the foreign keys as a directed graph.) If your
database has a foreign key cycle (and many do), this field lets you tell the
subsetter to ignore certain foreign keys, essentially removing the cycle.
You'll have to know a bit about your database to use this field effectively.
The tool will warn you if you have a cycle that you haven't broken.

The last is `fk_augmentation`. Databases frequently have foreign keys that are
not codified as constraints on the database — these are implicit foreign keys.
For a subsetter to create useful subsets it needs to know about these implicit
constraints. This field lets you add foreign keys to the subsetter that the DB
doesn't have listed as a constraint.

Below we describe all configuration parameters. Run `subset --example-config`
for the exact format.

## Required

`db_type`: The type of the database to subset. Valid values are `"postgres"` or
`"mysql"`. MySQL support targets the 8.4 LTS series or newer.

`source_db_connection_info`: Source database connection details. A JSON object
with the fields `user_name`, `host`, `db_name`, `port`, `ssl_mode` (optional),
and `password` (optional). If `password` is omitted, you will be prompted for
a password. Any string field can reference an environment variable using
`${VAR_NAME}` syntax (e.g., `"password": "${DB_SOURCE_PASSWORD}"`). Values
without `${...}` are used as-is.

`destination_db_connection_info`: Destination database connection details. Same
fields and environment variable support as `source_db_connection_info`. If you
do not pass the `-y` flag, a confirmation prompt will appear unless the
destination is localhost or 127.0.0.1.

`initial_targets`: JSON array of JSON objects. Each object must contain a
`table` field (the target table) and exactly one of `where` or `percent`.
The `where` field specifies a WHERE clause for row selection. The `percent`
field indicates a percentage of the target table; it is equivalent to
`"where": "random() < <percent>/100.0"`.

## Table selection

`passthrough_tables`: Tables that will be copied to the destination database in
whole. The value is a JSON array of strings, of the form `"<schema>.<table>"`
for Postgres and `"<database>.<table>"` for MySQL.

`excluded_tables`: Tables that will be excluded from the subset. The table will
exist in the output, but contain no rows. The value is a JSON array of strings,
of the form `"<schema>.<table>"` for Postgres and `"<database>.<table>"` for
MySQL.

`keep_disconnected_tables`: If `true`, tables that the subset target(s) don't
reach when following foreign keys will be copied 100% over. If `false`
(default), their schema will be copied but the table contents will be empty.
The tables and foreign keys form a graph (tables are nodes, foreign keys are
directed edges); disconnected tables are those in components that don't contain
any targets.

`max_rows_per_table`: A limit applied to all tables being copied. Useful if you
have very large tables that you want a sampling from. Set to `"ALL"` for
unlimited (recommended for most use cases). Default is no limit.

## Foreign key configuration

`dependency_breaks`: An array of JSON objects with `fk_table` and
`target_table` fields specifying table relationships to ignore in order to
break cycles. Optionally include `preserve_fk_opportunistically: true` to
still preserve the foreign key relationship where possible without creating
cycles.

`fk_augmentation`: Additional foreign keys that, while not represented as
constraints in the database, are logically present in the data. Foreign keys
listed here are unioned with the foreign keys discovered from database
constraints. Each entry is a JSON object with `fk_table`, `fk_columns`,
`target_table`, and `target_columns`. The column arrays must be the same
length.

When a candidate child row has several relationships to tables already in the
subset, every non-NULL relationship must point at a selected row (AND
semantics). A nullable `MATCH SIMPLE` relationship is neutral when any of its
columns is NULL, but at least one relationship must actually match for the
child to be selected. This permits common audit shapes such as an event owned
by a selected entity with an optional NULL actor. `dependency_breaks` are
excluded from this membership test, and their columns are nulled on every copy
path unless `preserve_fk_opportunistically` is enabled.

## Filtering

`upstream_filters`: Additional filtering applied to tables during upstream
subsetting. Upstream subsetting happens when a row is imported and the
subsetter greedily grabs rows from other tables that reference it via foreign
keys. If you don't want such greedy behavior on certain tables, you can impose
additional filters. Each entry is a JSON object with a `condition` field and
exactly one of `table` (filter applies to a specific table) or `column`
(filter applies to any table with that column). This is an advanced feature.

## Performance

`use_temp_tables`: If `true`, temporary ID tables will be created in the source
database so that IDs are not stored in Python memory when batching 100k rows.
This enables server-side JOINs, making subsetting more memory-efficient.
Requires write access on the source database (for `CREATE TEMPORARY TABLE`).
Default is `false`.

`use_copy_protocol`: If `true`, uses PostgreSQL's `COPY ... FROM STDIN`
protocol for row transfer instead of per-row INSERT statements. Significantly
faster (5-10x for bulk inserts). Postgres only. Default is `true`.

`parallel_read_workers`: Number of parallel connections used to read direct
target tables from the source. Splits work by physical page ranges (ctid),
so it works for any table regardless of primary key type. Designed for
read-only replicas. Requires PostgreSQL 14+. Default is `1` (sequential).

## Pre-filters

`pre_filters`: Named queries that execute once at the start of subsetting and
cache their results. Use this when an initial target needs to be filtered by a
slow or remote source (e.g., a foreign data wrapper table) that you don't want
re-executed per parallel worker. Each entry is a JSON object with `name` (a
reference key), `query` (the SQL to execute on the source), and `column` (the
target table column to filter against).

Initial targets reference a pre-filter by name via the optional `pre_filter`
field. The cached results are applied as `AND <column> = ANY(<cached values>)`
to the target's query.

## Incremental subsetting

`destination_mode`: One of `"recreate"` (default), `"topup"`, or `"grow"`.

With `"recreate"`, the destination schema is dropped and recreated from the
source on every run. This is the only mode that yields a clean point-in-time
snapshot (and the only way to drop rows deleted in the source).

With `"topup"` (Postgres only), the destination is treated as an existing
subset and the run adds to it — for example, to add rows from different
initial targets across multiple runs. Already-imported entities stay frozen:
new source children of previously imported rows are not picked up. Re-runs
cost O(new rows).

With `"grow"`, the run adds new direct targets and picks up new
children/descendants of already-imported rows, so the subset keeps tracking
source growth. This re-reads the children of every resident parent
(deduplicated on insert), so a run costs O(existing subset) rather than O(new
rows). PostgreSQL supports the full incremental contract described below.

MySQL 8.4 currently supports a bounded `"grow"` mode when every table in the
connected run scope has a primary key, source and destination column definitions
and primary keys match, and the destination tables have no secondary unique
indexes or enabled triggers. Re-read rows are refreshed and generated columns
are recomputed. The run uses transient delta tables, takes a destination advisory
lock, verifies foreign-key integrity before finishing, and can be retried
idempotently. MySQL `"topup"`, `incremental_keys`, secondary unique indexes, and
durable failure journals are not yet supported and fail during configuration or
preflight.

The remaining incremental details in this section describe PostgreSQL.

In both incremental modes, any row the run re-reads is refreshed in place
(upsert on its incremental identity): changed columns — soft-delete flags, history
`enddate`s, statuses — heal on re-read. Refreshes are applied before new
rows are inserted, so deactivate-and-replace patterns (a history table with
a "one active row per entity" partial unique index) load correctly with the
index live. Rows the run never re-reads keep their old values, and rows
hard-deleted in the source are never removed — if a hard-deleted row blocks
a unique index that a replacement row needs, the run fails with a unique
violation and a `"recreate"` run is the fix.
On PostgreSQL, the incremental identity defaults to the table's primary key.
`incremental_keys` applies only to tables without a primary key and cannot
override a table's primary key. A table with no primary key may use a unique
index when it is valid, immediate, non-partial, non-expression, and all of its
key columns are `NOT NULL` and non-generated.
If exactly one such unique key exists it is inferred. If several exist, select
one explicitly with `incremental_keys`:

```json
"incremental_keys": [
  {
    "table": "sales.customer_status_history",
    "columns": ["history_id"]
  }
]
```

The selected columns must be a stable row identity; changing an identity value
is treated as inserting a different row. Other unique indexes remain secondary
constraints. Partial indexes such as “one active row per customer” are never
eligible identities. Tables without any safe identity, ambiguous tables without
an explicit selection, and deferrable identity constraints fail before data is
transferred. A deferrable unique constraint matching the same columns as an
otherwise eligible identity is also rejected because PostgreSQL cannot use
that column set as an `ON CONFLICT` arbiter.
Tables with a `GENERATED ALWAYS AS IDENTITY` column outside the resolved
incremental identity are also rejected because PostgreSQL cannot update that
column to the source's explicit value.

Incremental runs reject enabled destination user triggers, table inheritance,
declarative partitioning, and PostgreSQL 18 temporal `PERIOD`/`WITHOUT
OVERLAPS` constraints rather than loading them with unsafe semantics. Use
`fk_augmentation` to describe history ownership that is logical but not backed
by a physical foreign key.

The destination is protected by an advisory lock. On PostgreSQL, a failed run
retains its `_condenser` delta journal and FK definitions, and the same effective
configuration resumes it on the next run. A different configuration or
identity selection is rejected until the original run is resumed or the
destination is recreated. `SQL/incremental_fk_backup.sql` is also written as a
manual recovery copy. This check includes target predicates,
`fk_augmentation`, and `incremental_keys`, so do not partially edit a config
while recovering a failed run.

After a *successful* run the journal is removed, because changing target
predicates is a normal top-up workflow. Keep the structural parts of the
config (`fk_augmentation`, `incremental_keys`, exclusions, passthrough tables,
dependency breaks, and filters) under version control and carry them forward
as a complete set. db-condenser cannot infer a logical relationship that was
removed from `fk_augmentation`; omitting it on a later successful run can leave
new logical history rows out of the subset. Adding or correcting structural
relationships may require one `"recreate"` run, since previously imported rows
are not re-checked under the old graph.

For audit/history tables owned by an entity, use `"grow"` when new history for
already-imported entities must continue to arrive. `"topup"` intentionally
freezes those entities and only follows history belonging to newly inserted
direct targets. Also set `destination_mode` explicitly: omitting it defaults to
`"recreate"`, which rebuilds the destination.

## Post-processing

`pre_constraint_sql`: An array of SQL commands issued on the destination
database after subsetting is complete, but before database constraints have
been applied. Useful to clean up data that would otherwise violate constraints.
Prefer `post_subset_sql` for general-purpose queries.

`post_subset_sql`: An array of SQL commands issued on the destination database
after subsetting is complete and after database constraints have been applied.
Useful for additional ad-hoc tasks after subsetting.
