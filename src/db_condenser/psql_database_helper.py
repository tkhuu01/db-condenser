import hashlib
import json
import os
import uuid
from dataclasses import asdict

from psycopg import sql
from psycopg.types.json import Json, set_json_loads

from db_condenser.config_reader import DestinationMode, get_config
from db_condenser.db_connect import PsqlConnection
from db_condenser.subset_utils import (
    columns_joined,
    columns_tupled,
    compute_batch_size,
    fully_qualified_table,
    quoter,
    schema_name,
    table_name,
)

set_json_loads(lambda s: s)

# Table shapes never change during a run (schema is created before subsetting;
# constraints added after don't alter columns), so metadata lookups are cached
# per database. Saves a catalog round trip per copy_rows call, which the
# streamed upstream/downstream paths invoke once per ID batch.
_metadata_cache: dict = {}


def _conn_cache_key(conn):
    # host+port included so same-named source and destination databases on
    # different servers don't share cache entries
    info = conn.connection.info
    return (info.host, info.port, info.dbname)


# Incremental state: when destination_mode is "topup" or "grow", each
# destination table gets a delta table in the _condenser schema keyed by its
# primary key or selected unique identity. Upstream subsetting
# (topup only) and downstream subsetting join against these deltas instead of
# full tables, so re-runs cost O(new rows). Incremental inserts also upsert on
# the selected identity so re-read rows refresh in place.
DELTA_SCHEMA = "_condenser"
_incremental_deltas: dict = {}
# destination tables carrying a unique index beyond their selected identity
# (or an exclusion constraint): their incremental copies need refreshes before
# new-row inserts, so ctid-parallel splits run in two phases (stage_rows /
# apply_staged with a barrier) instead of a single pass — page-range
# workers can't order across connections. Populated by prep_incremental.
_secondary_unique_tables: set = set()


def get_batch_size(column_count: int) -> int:
    return compute_batch_size(column_count)


def validate_supported_version(conn):
    pass


def requires_distinct_id_temp_tables() -> bool:
    return False


def build_id_table(rows, columns, datatypes, alias):
    """Render a typed table-valued parameter for a batch of identity rows."""
    unnest_args = ", ".join("%s::{}[]".format(datatypes[col]) for col in columns)
    join_cols = ", ".join("col{}".format(i) for i in range(len(columns)))
    params = [[row[i] for row in rows] for i in range(len(columns))]
    return "unnest({}) AS {}({})".format(unnest_args, alias, join_cols), params


def iter_membership_batches(values):
    yield values


def build_membership_filter(column_sql, values):
    return "{} = ANY(%s)".format(column_sql), [values]


def temp_table_column(temp_table, index, datatype):
    return "{}.col{}::{}".format(fully_qualified_table(temp_table), index, datatype)


def _prefixed_identifier(prefix, qualified_table):
    """Build '<prefix><schema>_<table>', hashing when it would exceed
    Postgres's 63-byte identifier limit (which truncates silently and could
    collide across long table names)."""
    name = prefix + qualified_table.replace(".", "_")
    if len(name) > 63:
        name = prefix + hashlib.md5(qualified_table.encode()).hexdigest()
    return name


def _relation_map(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ns.nspname || '.' || cl.relname, cl.relkind
              FROM pg_class cl
              JOIN pg_namespace ns ON ns.oid = cl.relnamespace
             WHERE ns.nspname NOT IN ('pg_catalog', 'information_schema')
               AND ns.nspname NOT LIKE 'pg\\_%'
            """
        )
        return dict(cur.fetchall())


def _unique_index_metadata(conn):
    q = """
        SELECT ns.nspname || '.' || cl.relname,
               idx.relname,
               i.indisprimary,
               i.indisvalid,
               i.indisready,
               i.indimmediate,
               i.indpred IS NULL,
               i.indexprs IS NULL,
               i.indisexclusion,
               array_agg(att.attname ORDER BY x.ord)
                   FILTER (WHERE x.ord <= i.indnkeyatts),
               bool_and(COALESCE(att.attnotnull, false))
                   FILTER (WHERE x.ord <= i.indnkeyatts),
               bool_and(COALESCE(att.attgenerated = '', false))
                   FILTER (WHERE x.ord <= i.indnkeyatts)
          FROM pg_index i
          JOIN pg_class cl ON cl.oid = i.indrelid
          JOIN pg_class idx ON idx.oid = i.indexrelid
          JOIN pg_namespace ns ON ns.oid = cl.relnamespace
          JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS x(attnum, ord) ON true
          LEFT JOIN pg_attribute att
            ON att.attrelid = cl.oid AND att.attnum = x.attnum
         WHERE i.indisunique OR i.indisprimary
         GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
    """
    with conn.cursor() as cur:
        cur.execute(q)
        rows = cur.fetchall()
    metadata = {}
    for row in rows:
        metadata.setdefault(row[0], []).append(
            {
                "name": row[1],
                "primary": row[2],
                "valid": row[3],
                "ready": row[4],
                "immediate": row[5],
                "non_partial": row[6],
                "non_expression": row[7],
                "exclusion": row[8],
                "columns": list(row[9] or []),
                "not_null": bool(row[10]),
                "not_generated": bool(row[11]),
            }
        )
    return metadata


def get_tables_primary_keys(tables, conn):
    """Map each fully-qualified table to its ordered PK column list ([] if none)."""
    indexes = _unique_index_metadata(conn)
    result = {}
    for table in tables:
        primary = next(
            (index for index in indexes.get(table, []) if index["primary"]), None
        )
        result[table] = primary["columns"] if primary else []
    return result


def _eligible_identity(index):
    return (
        index["valid"]
        and index["ready"]
        and index["immediate"]
        and index["non_partial"]
        and index["non_expression"]
        and not index["exclusion"]
        and index["not_null"]
        and index["not_generated"]
        and bool(index["columns"])
    )


def _reject_deferrable_arbiter(table, indexes, identity, database_label):
    deferrable = next(
        (
            index
            for index in indexes
            if index["valid"]
            and index["ready"]
            and not index["immediate"]
            and index["non_partial"]
            and index["non_expression"]
            and len(index["columns"]) == len(identity)
            and set(index["columns"]) == set(identity)
        ),
        None,
    )
    if deferrable is not None:
        raise ValueError(
            "Incremental identity ({}) on {} also matches deferrable unique index"
            " {} in the {} database; PostgreSQL cannot use that column set as an"
            " ON CONFLICT arbiter".format(
                ", ".join(identity), table, deferrable["name"], database_label
            )
        )


def _resolve_incremental_keys(conn, tables, configured_keys, database_label):
    relations = _relation_map(conn)
    indexes = _unique_index_metadata(conn)
    resolved = {}
    for table in tables:
        if table not in relations:
            raise ValueError(
                "Incremental table {} does not exist in the {} database".format(
                    table, database_label
                )
            )
        if relations[table] != "r":
            raise ValueError(
                "Incremental table {} has unsupported relation kind {!r} in the {}"
                " database".format(table, relations[table], database_label)
            )

        table_indexes = indexes.get(table, [])
        primary = next((index for index in table_indexes if index["primary"]), None)
        configured = configured_keys.get(table)
        if primary is not None:
            if not _eligible_identity(primary):
                raise ValueError(
                    "Primary key on {} cannot be used for incremental refresh; it"
                    " must be valid, immediate, non-expression, non-partial, and"
                    " backed by NOT NULL, non-generated columns".format(table)
                )
            if configured is not None and set(configured) != set(primary["columns"]):
                raise ValueError(
                    "incremental_keys cannot override the primary key on {}".format(
                        table
                    )
                )
            _reject_deferrable_arbiter(
                table, table_indexes, primary["columns"], database_label
            )
            resolved[table] = primary["columns"]
            continue

        eligible_by_columns = {
            tuple(index["columns"]): index
            for index in table_indexes
            if _eligible_identity(index)
        }
        if configured is not None:
            match = next(
                (
                    columns
                    for columns in eligible_by_columns
                    if len(columns) == len(configured)
                    and set(columns) == set(configured)
                ),
                None,
            )
            if match is None:
                raise ValueError(
                    "Configured incremental key for {} is not backed by a valid,"
                    " immediate, non-partial, non-expression unique index whose"
                    " columns are all NOT NULL and non-generated in the {}"
                    " database".format(table, database_label)
                )
            identity = configured
        elif len(eligible_by_columns) == 1:
            identity = list(next(iter(eligible_by_columns)))
        elif not eligible_by_columns:
            raise ValueError(
                "Incremental table {} has no primary key or eligible unique key;"
                " add a primary key or configure a NOT NULL unique identity".format(
                    table
                )
            )
        else:
            candidates = ", ".join(
                "({})".format(", ".join(columns))
                for columns in sorted(eligible_by_columns)
            )
            raise ValueError(
                "Incremental table {} has multiple eligible unique keys: {};"
                " select one with incremental_keys".format(table, candidates)
            )
        _reject_deferrable_arbiter(table, table_indexes, identity, database_label)
        resolved[table] = identity
    return resolved


def _validate_incremental_identity_columns(conn, identity_map, database_label):
    for table, identity in identity_map.items():
        non_key_always_identities = [
            column
            for column, _, _, identity_kind in get_table_datatypes(
                table_name(table), schema_name(table), conn
            )
            if identity_kind == "a" and column not in identity
        ]
        if non_key_always_identities:
            raise ValueError(
                "Incremental table {} has non-key GENERATED ALWAYS AS IDENTITY"
                " column(s) {} in the {} database; PostgreSQL cannot refresh"
                " these columns to their source values".format(
                    table, ", ".join(non_key_always_identities), database_label
                )
            )


def _validate_incremental_schema(conn, database_label, tables, reject_triggers=False):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT pn.nspname || '.' || parent.relname,
                   cn.nspname || '.' || child.relname,
                   child.relispartition
              FROM pg_inherits inh
              JOIN pg_class parent ON parent.oid = inh.inhparent
              JOIN pg_namespace pn ON pn.oid = parent.relnamespace
              JOIN pg_class child ON child.oid = inh.inhrelid
              JOIN pg_namespace cn ON cn.oid = child.relnamespace
             WHERE pn.nspname NOT IN ('pg_catalog', 'information_schema')
               AND pn.nspname NOT LIKE 'pg\\_%%'
               AND (
                   pn.nspname || '.' || parent.relname = ANY(%s)
                   OR cn.nspname || '.' || child.relname = ANY(%s)
               )
            """,
            (tables, tables),
        )
        inherited = cur.fetchall()
        if inherited:
            parent, child, is_partition = inherited[0]
            kind = "partitioning" if is_partition else "table inheritance"
            raise ValueError(
                "Incremental refresh does not support {} in the {} database"
                " ({} -> {})".format(kind, database_label, parent, child)
            )

        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1 FROM pg_attribute
                 WHERE attrelid = 'pg_constraint'::regclass
                   AND attname = 'conperiod'
            )
            """
        )
        if cur.fetchone()[0]:
            cur.execute(
                """
                SELECT ns.nspname || '.' || cl.relname, con.conname
                  FROM pg_constraint con
                  JOIN pg_class cl ON cl.oid = con.conrelid
                  JOIN pg_namespace ns ON ns.oid = cl.relnamespace
                 WHERE con.conperiod
                   AND ns.nspname NOT IN ('pg_catalog', 'information_schema')
                   AND ns.nspname || '.' || cl.relname = ANY(%s)
                """,
                (tables,),
            )
            temporal = cur.fetchone()
            if temporal:
                raise ValueError(
                    "PostgreSQL temporal constraint {} on {} is not supported by"
                    " the PostgreSQL 14+ incremental refresh contract".format(
                        temporal[1], temporal[0]
                    )
                )

        if reject_triggers:
            cur.execute(
                """
                SELECT ns.nspname || '.' || cl.relname, trg.tgname
                  FROM pg_trigger trg
                  JOIN pg_class cl ON cl.oid = trg.tgrelid
                  JOIN pg_namespace ns ON ns.oid = cl.relnamespace
                 WHERE NOT trg.tgisinternal
                   AND trg.tgenabled <> 'D'
                   AND (trg.tgtype & 60) <> 0
                   AND ns.nspname NOT IN (
                       'pg_catalog', 'information_schema', '_condenser'
                   )
                   AND ns.nspname || '.' || cl.relname = ANY(%s)
                 LIMIT 1
                """,
                (tables,),
            )
            trigger = cur.fetchone()
            if trigger:
                raise ValueError(
                    "Incremental refresh cannot safely write {} while destination"
                    " trigger {} is enabled".format(trigger[0], trigger[1])
                )


def _incremental_config_hash(identity_map):
    raw = asdict(get_config())
    for name in ("source_db_connection_info", "destination_db_connection_info"):
        raw[name].pop("password", None)
    for name in ("excluded_tables", "passthrough_tables"):
        raw[name] = sorted(set(raw[name]))
    for name in ("dependency_breaks", "fk_augmentation", "incremental_keys"):
        raw[name] = sorted(raw[name], key=lambda item: json.dumps(item, sort_keys=True))
    raw["resolved_incremental_keys"] = {
        table: identity_map[table] for table in sorted(identity_map)
    }
    payload = json.dumps(raw, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()


def acquire_incremental_lock(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT pg_try_advisory_lock("
            "hashtextextended(current_database() || ':db-condenser:incremental', 0))"
        )
        acquired = cur.fetchone()[0]
    if not acquired:
        raise RuntimeError(
            "Another incremental db-condenser run is active for this destination"
        )


def release_incremental_lock(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT pg_advisory_unlock("
            "hashtextextended(current_database() || ':db-condenser:incremental', 0))"
        )


def _prepare_incremental_state(conn, identity_map):
    config_hash = _incremental_config_hash(identity_map)
    with conn.cursor() as cur:
        cur.execute("SELECT to_regnamespace(%s)", (DELTA_SCHEMA,))
        schema_exists = cur.fetchone()[0] is not None
        if not schema_exists:
            cur.execute('CREATE SCHEMA "{}"'.format(DELTA_SCHEMA))
            cur.execute(
                'CREATE TABLE "{}"."run_state" (config_hash text NOT NULL)'.format(
                    DELTA_SCHEMA
                )
            )
            cur.execute(
                'INSERT INTO "{}"."run_state" VALUES (%s)'.format(DELTA_SCHEMA),
                (config_hash,),
            )
            cur.execute(
                'CREATE TABLE "{}"."fk_backup" ('
                "schema_name text NOT NULL, table_name text NOT NULL, "
                "constraint_name text NOT NULL, definition text NOT NULL, "
                "PRIMARY KEY (schema_name, table_name, constraint_name))".format(
                    DELTA_SCHEMA
                )
            )
            return False

        cur.execute(
            "SELECT to_regclass(%s)",
            ('"{}"."run_state"'.format(DELTA_SCHEMA),),
        )
        if cur.fetchone()[0] is None:
            raise RuntimeError(
                "Reserved schema _condenser exists without incremental run metadata;"
                " move or remove it before retrying"
            )
        cur.execute('SELECT config_hash FROM "{}"."run_state"'.format(DELTA_SCHEMA))
        rows = cur.fetchall()
        if len(rows) != 1 or rows[0][0] != config_hash:
            raise RuntimeError(
                "Retained incremental journal was created with a different"
                " configuration or row identity; resume with the original"
                " configuration or perform a recreate run"
            )
    return True


def _prepare_delta_table(conn, table, key, resume):
    name = _prefixed_identifier("new_ids_", table)
    qualified = '"{}"."{}"'.format(DELTA_SCHEMA, name)
    if resume:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass(%s)", (qualified,))
            if cur.fetchone()[0] is None:
                raise RuntimeError(
                    "Retained incremental journal is missing the delta for {}".format(
                        table
                    )
                )
            cur.execute(
                "SELECT attname FROM pg_attribute"
                " WHERE attrelid = %s::regclass AND attnum > 0 AND NOT attisdropped"
                " ORDER BY attnum",
                (qualified,),
            )
            columns = [row[0] for row in cur.fetchall()]
        if columns != key + ["_inserted"]:
            raise RuntimeError(
                "Retained incremental delta for {} has incompatible columns".format(
                    table
                )
            )
        return qualified

    with conn.cursor() as cur:
        cur.execute(
            "CREATE TABLE {} AS SELECT {}, false AS _inserted"
            " FROM {} WITH NO DATA".format(
                qualified, columns_joined(key), fully_qualified_table(table)
            )
        )
        cur.execute(
            "ALTER TABLE {} ADD PRIMARY KEY ({})".format(qualified, columns_joined(key))
        )
        cur.execute(
            'ALTER TABLE {} ALTER COLUMN "_inserted" SET NOT NULL'.format(qualified)
        )
    return qualified


def prep_incremental(source_conn, destination_conn, tables):
    _incremental_deltas.clear()
    _secondary_unique_tables.clear()
    acquire_incremental_lock(destination_conn)
    try:
        _validate_incremental_schema(source_conn, "source", tables)
        _validate_incremental_schema(
            destination_conn, "destination", tables, reject_triggers=True
        )
        configured_keys = {
            table: columns
            for table, columns in get_config().incremental_key_map.items()
            if table in tables
        }
        source_keys = _resolve_incremental_keys(
            source_conn, tables, configured_keys, "source"
        )
        destination_keys = _resolve_incremental_keys(
            destination_conn, tables, configured_keys, "destination"
        )
        mismatched = [
            table
            for table in tables
            if set(source_keys[table]) != set(destination_keys[table])
        ]
        if mismatched:
            raise ValueError(
                "Source and destination incremental identities differ for: "
                + ", ".join(mismatched)
            )
        _validate_incremental_identity_columns(source_conn, source_keys, "source")
        _validate_incremental_identity_columns(
            destination_conn, destination_keys, "destination"
        )

        resume = _prepare_incremental_state(destination_conn, destination_keys)
        destination_indexes = _unique_index_metadata(destination_conn)
        for table in tables:
            identity = destination_keys[table]
            if any(
                not index["primary"]
                and not (
                    _eligible_identity(index)
                    and len(index["columns"]) == len(identity)
                    and set(index["columns"]) == set(identity)
                )
                for index in destination_indexes.get(table, [])
            ):
                _secondary_unique_tables.add(table)
        with destination_conn.cursor() as cur:
            cur.execute(
                """
                SELECT ns.nspname || '.' || cl.relname
                  FROM pg_constraint con
                  JOIN pg_class cl ON cl.oid = con.conrelid
                  JOIN pg_namespace ns ON ns.oid = cl.relnamespace
                 WHERE con.contype = 'x'
                """
            )
            _secondary_unique_tables.update(row[0] for row in cur.fetchall())
        for table in tables:
            key = destination_keys[table]
            qualified = _prepare_delta_table(destination_conn, table, key, resume)
            _incremental_deltas[table] = (qualified, key)
        destination_conn.commit()
    except BaseException:
        destination_conn.connection.rollback()
        release_incremental_lock(destination_conn)
        raise


def unprep_incremental(conn):
    _incremental_deltas.clear()
    _secondary_unique_tables.clear()
    try:
        with conn.cursor() as cur:
            cur.execute('DROP SCHEMA IF EXISTS "{}" CASCADE'.format(DELTA_SCHEMA))
        conn.commit()
    finally:
        release_incremental_lock(conn)


def retain_incremental(conn):
    """Leave the durable journal in place after a failed run, then unlock."""
    _incremental_deltas.clear()
    _secondary_unique_tables.clear()
    release_incremental_lock(conn)


def has_secondary_unique(table):
    """True when the table has uniqueness beyond its incremental identity."""
    return table in _secondary_unique_tables


def drop_fk_constraints(conn):
    """Capture and drop all FK constraints on the destination.

    Incremental runs load into a destination whose constraints are live
    (added at the end of the first run), but middle-out ordering inserts
    upstream rows before the downstream rows they reference. FKs are dropped
    for the duration of the run and restored afterwards; PKs and unique
    indexes stay so ON CONFLICT dedup keeps working. Returns the dropped
    definitions for restore_fk_constraints.
    """
    q = """
        SELECT ns.nspname, cl.relname, con.conname, pg_get_constraintdef(con.oid)
          FROM pg_constraint con
          JOIN pg_class cl ON cl.oid = con.conrelid
          JOIN pg_namespace ns ON ns.oid = cl.relnamespace
         WHERE con.contype = 'f'
           AND ns.nspname NOT IN ('pg_catalog', 'information_schema')
    """
    with conn.cursor() as cur:
        cur.execute(q)
        fks = cur.fetchall()
        for nsp, rel, name, defn in fks:
            cur.execute(
                "SELECT definition FROM"
                ' "{}"."fk_backup" WHERE schema_name = %s AND table_name = %s'
                " AND constraint_name = %s".format(DELTA_SCHEMA),
                (nsp, rel, name),
            )
            retained = cur.fetchone()
            if retained is None:
                cur.execute(
                    'INSERT INTO "{}"."fk_backup" VALUES (%s, %s, %s, %s)'.format(
                        DELTA_SCHEMA
                    ),
                    (nsp, rel, name, defn),
                )
            elif retained[0] != defn:
                raise RuntimeError(
                    "Retained foreign key definition for {}.{} {} differs from"
                    " the current constraint; remove the conflicting constraint"
                    " before retrying".format(nsp, rel, name)
                )
        cur.execute(
            "SELECT schema_name, table_name, constraint_name, definition FROM"
            ' "{}"."fk_backup" ORDER BY schema_name, table_name, constraint_name'.format(
                DELTA_SCHEMA
            )
        )
        all_fks = cur.fetchall()
    conn.commit()

    if all_fks:
        backup_dir = os.path.join(os.getcwd(), "SQL")
        os.makedirs(backup_dir, exist_ok=True)
        backup_path = os.path.join(backup_dir, "incremental_fk_backup.sql")
        with open(backup_path, "w") as fp:
            for nsp, rel, name, defn in all_fks:
                fp.write(
                    'ALTER TABLE "{}"."{}" ADD CONSTRAINT "{}" {};\n'.format(
                        nsp, rel, name, defn
                    )
                )

    with conn.cursor() as cur:
        for nsp, rel, name, _ in fks:
            cur.execute(
                'ALTER TABLE "{}"."{}" DROP CONSTRAINT "{}"'.format(nsp, rel, name)
            )
    conn.commit()
    return all_fks


def restore_fk_constraints(conn, fks):
    try:
        with conn.cursor() as cur:
            for nsp, rel, name, defn in fks:
                cur.execute(
                    """
                    SELECT con.contype, pg_get_constraintdef(con.oid)
                      FROM pg_constraint con
                      JOIN pg_class cl ON cl.oid = con.conrelid
                      JOIN pg_namespace ns ON ns.oid = cl.relnamespace
                     WHERE ns.nspname = %s
                       AND cl.relname = %s
                       AND con.conname = %s
                    """,
                    (nsp, rel, name),
                )
                existing = cur.fetchone()
                if existing is None:
                    cur.execute(
                        'ALTER TABLE "{}"."{}" ADD CONSTRAINT "{}" {}'.format(
                            nsp, rel, name, defn
                        )
                    )
                elif existing != ("f", defn):
                    raise RuntimeError(
                        "Cannot restore foreign key {}.{} {}; an existing"
                        " constraint with that name has a different definition".format(
                            nsp, rel, name
                        )
                    )
        conn.commit()
    except BaseException:
        conn.connection.rollback()
        raise


def delta_for(table):
    """Return (qualified_delta_table, identity_columns) or None."""
    return _incremental_deltas.get(table)


def _wrap_insert_with_delta(insert_query, destination_table):
    delta = _incremental_deltas.get(destination_table)
    if not delta:
        return insert_query
    qualified, identity = delta
    # upserts RETURN updated rows too; xmax = 0 identifies genuine inserts
    # (updates whose values were unchanged are skipped by the upsert guard
    # and don't appear at all)
    return (
        "WITH ins AS ({} RETURNING {}, (xmax = 0) AS _inserted)"
        " INSERT INTO {} AS _delta SELECT {}, _inserted FROM ins"
        " ON CONFLICT ({}) DO UPDATE SET _inserted ="
        " _delta._inserted OR EXCLUDED._inserted".format(
            insert_query,
            columns_joined(identity),
            qualified,
            columns_joined(identity),
            columns_joined(identity),
        )
    )


def _conflict_clause(destination_table, update_columns):
    """ON CONFLICT clause for destination inserts. Incremental runs upsert on
    the selected identity so re-read rows refresh in place; the guard skips
    rows whose values are unchanged. Recreate runs and tables outside the
    incremental traversal (no delta registered) keep DO NOTHING.

    Returns (clause, pk_columns); pk_columns is None unless upserting.
    Upsert clauses reference the insert target via the alias _dest.
    """
    delta = _incremental_deltas.get(destination_table)
    if not delta:
        return " ON CONFLICT DO NOTHING", None
    _, identity = delta
    non_identity = [c for c in update_columns if c not in identity]
    if not non_identity:
        return " ON CONFLICT DO NOTHING", None
    sets = ", ".join('"{0}" = EXCLUDED."{0}"'.format(c) for c in non_identity)
    dest_row = ", ".join('_dest."{}"'.format(c) for c in non_identity)
    excl_row = ", ".join('EXCLUDED."{}"'.format(c) for c in non_identity)
    return (
        " ON CONFLICT ({}) DO UPDATE SET {} WHERE ({}) IS DISTINCT FROM ({})".format(
            columns_joined(identity), sets, dest_row, excl_row
        ),
        identity,
    )


def prep_temp_dbs(_, __):
    # runs once at the start of every subset run: drop metadata cached from
    # any prior run in this process, in case a same-named database was
    # dropped and recreated with a different shape in between
    _metadata_cache.clear()


def unprep_temp_dbs(_, __):
    pass


def turn_off_constraints(connection):
    # can't be done in postgres
    pass


def copy_rows(
    source, destination, query, destination_table, params=None, batch_size=None
):
    datatypes = get_table_datatypes(
        table_name(destination_table), schema_name(destination_table), destination
    )
    if batch_size is None:
        batch_size = compute_batch_size(len(datatypes))

    non_generated_columns = [
        (dt[0], dt[1]) for _, dt in enumerate(datatypes) if not dt[2]
    ]
    updatable_columns = [dt[0] for dt in datatypes if not dt[2] and dt[3] != "a"]
    generated_columns_positions = {i for i, dt in enumerate(datatypes) if dt[2]}
    always_generated_id = any([dt[3] == "a" for dt in datatypes])

    def template_piece(dt):
        if dt == "_json":
            return "%s::json[]"
        elif dt == "_jsonb":
            return "%s::jsonb[]"
        else:
            return "%s"

    template = (
        "(" + ",".join([template_piece(dt[1]) for dt in non_generated_columns]) + ")"
    )
    columns = '("' + '","'.join([dt[0] for dt in non_generated_columns]) + '")'

    json_positions = {
        i for i, dt in enumerate(non_generated_columns) if dt[1] in ("json", "jsonb")
    }

    def _adapt_json(val):
        if val is None:
            return None
        if isinstance(val, bytes):
            val = val.decode("utf-8")
        return Json(val)

    def _adapt_row(row):
        if json_positions:
            return tuple(
                _adapt_json(val) if i in json_positions else val
                for i, val in enumerate(row)
            )
        return row

    cursor_name = "table_cursor_" + str(uuid.uuid4()).replace("-", "")
    cursor = source.cursor(name=cursor_name)
    # using the inner_cursor means we don't log all the noise
    destination_cursor = destination.cursor().inner_cursor
    try:
        cursor.execute(query, params)

        conflict_clause, upsert_pk = _conflict_clause(
            destination_table, updatable_columns
        )
        if upsert_pk is not None:
            # incremental upsert: per-batch ordering cannot cover a
            # deactivate-and-replace pair that straddles a fetchmany
            # boundary, so stage the whole copy into the session staging
            # table first, then apply it with the same refresh-then-insert
            # statements the COPY-protocol path uses
            dest_table = fully_qualified_table(destination_table)
            temp_table = '"{}"'.format(
                _prefixed_identifier("_copy_staging_", destination_table)
            )
            destination_cursor.execute(
                "CREATE TEMPORARY TABLE IF NOT EXISTS {} (LIKE {} INCLUDING DEFAULTS)".format(
                    temp_table, dest_table
                )
            )
            destination_cursor.execute("TRUNCATE {}".format(temp_table))
            insert_query = "INSERT INTO {} {} VALUES {}".format(
                temp_table, columns, template
            )
        else:
            insert_query = "INSERT INTO {} AS _dest {}{} VALUES {}{}".format(
                fully_qualified_table(destination_table),
                columns,
                " OVERRIDING SYSTEM VALUE" if always_generated_id else "",
                template,
                conflict_clause,
            )
            insert_query = _wrap_insert_with_delta(insert_query, destination_table)

        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break

            if generated_columns_positions:
                updated_rows = (
                    _adapt_row(
                        tuple(
                            val
                            for i, val in enumerate(row)
                            if i not in generated_columns_positions
                        )
                    )
                    for row in rows
                )
            else:
                updated_rows = (_adapt_row(row) for row in rows)

            destination_cursor.executemany(insert_query, updated_rows)

        if upsert_pk is not None:
            apply_staged(destination, destination_table, "refresh")
            apply_staged(destination, destination_table, "insert")

    finally:
        destination_cursor.close()
        cursor.close()
        destination.commit()


def _copy_metadata(destination_table, destination):
    """Shared column/naming metadata for the COPY-protocol paths."""
    datatypes = get_table_datatypes(
        table_name(destination_table), schema_name(destination_table), destination
    )
    non_generated_columns = [dt[0] for dt in datatypes if not dt[2]]
    updatable_columns = [dt[0] for dt in datatypes if not dt[2] and dt[3] != "a"]
    column_list = ", ".join('"' + col + '"' for col in non_generated_columns)
    always_generated_id = any(dt[3] == "a" for dt in datatypes)
    dest_table = fully_qualified_table(destination_table)
    # deterministic name so batched calls for the same table reuse one
    # session-local staging table instead of CREATE/DROP catalog churn per call
    temp_table = '"{}"'.format(
        _prefixed_identifier("_copy_staging_", destination_table)
    )
    return (
        updatable_columns,
        column_list,
        always_generated_id,
        dest_table,
        temp_table,
    )


def _pipe_copy(source_cursor, dest_cursor, query, params, column_list, copy_target):
    # Block-level COPY streaming: pipe raw COPY data straight from the source
    # into the destination, avoiding a per-row Python loop. Selecting just the
    # non-generated columns keeps the stream aligned with the target column
    # list (generated columns are excluded; the cap, joins, etc. live in query).
    # COPY FROM inserts supplied values into identity columns natively.
    copy_out = "COPY (SELECT {} FROM ({}) AS _src) TO STDOUT".format(column_list, query)
    copy_in = "COPY {} ({}) FROM STDIN".format(copy_target, column_list)
    # psycopg yields one buffer per row; writing each individually caps
    # throughput on Python loop overhead (~2x), so coalesce into ~1MB
    # chunks before writing
    with source_cursor.copy(copy_out, params) as src_copy:
        with dest_cursor.copy(copy_in) as dest_copy:
            buf = bytearray()
            for data in src_copy:
                buf += data
                if len(buf) >= 1 << 20:
                    dest_copy.write(bytes(buf))
                    buf.clear()
            if buf:
                dest_copy.write(bytes(buf))


def stage_rows(source, destination, query, destination_table, params=None):
    """Two-phase parallel copy, step 1: stream a worker's rows into its
    session-local staging table without applying them. The caller applies
    them later with apply_staged (refresh phase, barrier, insert phase)."""
    _, column_list, _, dest_table, temp_table = _copy_metadata(
        destination_table, destination
    )
    source_cursor = source.cursor().inner_cursor
    dest_cursor = destination.cursor().inner_cursor
    try:
        dest_cursor.execute(
            "CREATE TEMPORARY TABLE IF NOT EXISTS {} (LIKE {} INCLUDING DEFAULTS)".format(
                temp_table, dest_table
            )
        )
        dest_cursor.execute("TRUNCATE {}".format(temp_table))
        _pipe_copy(source_cursor, dest_cursor, query, params, column_list, temp_table)
    finally:
        dest_cursor.close()
        source_cursor.close()
        destination.commit()


def apply_staged(destination, destination_table, phase):
    """Two-phase parallel copy, step 2: apply this session's staged rows.

    phase='refresh' upserts only rows whose identities already exist (any order is
    safe: each refresh moves its own row toward the source's valid state);
    phase='insert' adds the remaining rows and clears the staging table.
    Callers must run every worker's refresh phase to completion (and commit,
    which this function does) before any insert phase starts — that barrier
    is what lets new rows land under a live secondary unique index.
    """
    (
        updatable_columns,
        column_list,
        always_generated_id,
        dest_table,
        temp_table,
    ) = _copy_metadata(destination_table, destination)
    conflict_clause, _ = _conflict_clause(destination_table, updatable_columns)
    # Classify rows by the delta identity, not _conflict_clause's upsert key:
    # the latter is None for identity-only tables (nothing to refresh, so the
    # clause is DO NOTHING), but the phase split still needs the key. The
    # two-phase caller gate guarantees the delta exists.
    _, pk = _incremental_deltas[destination_table]
    pk_match = " AND ".join('_t."{0}" = _s."{0}"'.format(c) for c in pk)
    select_src = (
        "SELECT {} FROM (SELECT DISTINCT ON ({}) {} FROM {}) _s"
        " WHERE {} (SELECT 1 FROM {} _t WHERE {})".format(
            column_list,
            columns_joined(pk),
            column_list,
            temp_table,
            "EXISTS" if phase == "refresh" else "NOT EXISTS",
            dest_table,
            pk_match,
        )
    )
    insert_query = "INSERT INTO {} AS _dest ({}){} {}{}".format(
        dest_table,
        column_list,
        " OVERRIDING SYSTEM VALUE" if always_generated_id else "",
        select_src,
        conflict_clause,
    )
    dest_cursor = destination.cursor().inner_cursor
    try:
        dest_cursor.execute(_wrap_insert_with_delta(insert_query, destination_table))
        if phase == "insert":
            dest_cursor.execute("TRUNCATE {}".format(temp_table))
    finally:
        dest_cursor.close()
        destination.commit()


def copy_rows_copy_protocol(
    source, destination, query, destination_table, params=None, batch_size=None
):
    # batch_size is accepted for interface parity with copy_rows (both are used
    # as self.__copy_rows) but is unused here: the COPY stream self-chunks.
    (
        updatable_columns,
        column_list,
        always_generated_id,
        dest_table,
        temp_table,
    ) = _copy_metadata(destination_table, destination)

    # On a recreate run the destination was just built from the pre-data
    # schema, so no unique indexes exist during load and ON CONFLICT cannot
    # dedup anything: the staging pass would be a pure second write of every
    # row. COPY straight into the target instead. Top-up/grow runs keep
    # staging for dedup/upsert and delta capture.
    direct_copy = get_config().destination_mode == DestinationMode.RECREATE

    source_cursor = source.cursor().inner_cursor
    dest_cursor = destination.cursor().inner_cursor
    try:
        if not direct_copy:
            dest_cursor.execute(
                "CREATE TEMPORARY TABLE IF NOT EXISTS {} (LIKE {} INCLUDING DEFAULTS)".format(
                    temp_table, dest_table
                )
            )
            dest_cursor.execute("TRUNCATE {}".format(temp_table))

        _pipe_copy(
            source_cursor,
            dest_cursor,
            query,
            params,
            column_list,
            dest_table if direct_copy else temp_table,
        )

        if not direct_copy:
            conflict_clause, upsert_pk = _conflict_clause(
                destination_table, updatable_columns
            )
            if upsert_pk is not None:
                # dedupe on the identity (an upsert cannot affect the same
                # row twice), then arbitrate refreshes of existing rows before
                # new-row inserts: a new row may only satisfy a secondary
                # unique index (e.g. one active history row per entity) once
                # the stale row it displaces has been refreshed. The ORDER BY
                # sorts before any row is inserted, so the EXISTS classifies
                # every row against the pre-statement destination state.
                exists_cond = " AND ".join(
                    '_t."{0}" = _s."{0}"'.format(c) for c in upsert_pk
                )
                select_src = (
                    "SELECT {} FROM (SELECT DISTINCT ON ({}) {} FROM {}) _s"
                    " ORDER BY (EXISTS (SELECT 1 FROM {} _t WHERE {})) DESC".format(
                        column_list,
                        columns_joined(upsert_pk),
                        column_list,
                        temp_table,
                        dest_table,
                        exists_cond,
                    )
                )
            else:
                select_src = "SELECT {} FROM {}".format(column_list, temp_table)
            insert_query = "INSERT INTO {} AS _dest ({}){} {}{}".format(
                dest_table,
                column_list,
                " OVERRIDING SYSTEM VALUE" if always_generated_id else "",
                select_src,
                conflict_clause,
            )
            dest_cursor.execute(
                _wrap_insert_with_delta(insert_query, destination_table)
            )
            # release the staging rows' disk immediately; otherwise the last
            # result set per table lingers until the session closes
            dest_cursor.execute("TRUNCATE {}".format(temp_table))
    finally:
        dest_cursor.close()
        source_cursor.close()
        destination.commit()


def source_db_temp_table(target_table):
    return "tonic_subset_" + schema_name(target_table) + "_" + table_name(target_table)


def create_id_temp_table(conn, number_of_columns: int) -> str:
    table_name = "tonic_subset_" + str(uuid.uuid4())
    column_defs = ",\n".join(
        ["    col" + str(aye) + "  varchar" for aye in range(number_of_columns)]
    )
    q = 'CREATE TEMPORARY TABLE "{}" (\n {} \n)'.format(table_name, column_defs)
    with conn.cursor() as cursor:
        cursor.execute(q)
    return table_name


def copy_to_temp_table(conn, query, target_table, pk_columns=None):
    temp_table = fully_qualified_table(source_db_temp_table(target_table))
    with conn.cursor() as cur:
        cur.execute(
            "CREATE TEMPORARY TABLE IF NOT EXISTS "
            + temp_table
            + " AS "
            + query
            + " LIMIT 0"
        )
        if pk_columns:
            query = query + " WHERE {} NOT IN (SELECT {} FROM {})".format(
                columns_tupled(pk_columns), columns_joined(pk_columns), temp_table
            )
        cur.execute("INSERT INTO " + temp_table + " " + query)
        conn.commit()


def clean_temp_table_cells(fk_table, fk_columns, target_table, target_columns, conn):
    fk_alias = "tonic_subset_398dhjr23_fk"
    target_alias = "tonic_subset_398dhjr23_target"

    fk_table = fully_qualified_table(source_db_temp_table(fk_table))
    target_table = fully_qualified_table(source_db_temp_table(target_table))
    assignment_list = ",".join(["{} = NULL".format(quoter(c)) for c in fk_columns])
    column_matching = " AND ".join(
        [
            "{}.{} = {}.{}".format(fk_alias, quoter(fc), target_alias, quoter(tc))
            for fc, tc in zip(fk_columns, target_columns)
        ]
    )
    q = "UPDATE {} {} SET {} WHERE NOT EXISTS (SELECT 1 FROM {} {} WHERE {})".format(
        fk_table, fk_alias, assignment_list, target_table, target_alias, column_matching
    )
    run_query(q, conn)


def get_unredacted_fk_relationships(tables: list[str], conn: PsqlConnection):
    q = """
        SELECT fk_nsp.nspname || '.' || fk_table AS fk_table,
        array_agg(fk_att.attname ORDER BY fk_att.attnum) AS fk_columns,
        tar_nsp.nspname || '.' || target_table AS target_table,
        array_agg(tar_att.attname ORDER BY fk_att.attnum) AS target_columns
    FROM (
        SELECT
            fk.oid AS fk_table_id,
            fk.relnamespace AS fk_schema_id,
            fk.relname AS fk_table,
            unnest(con.conkey) as fk_column_id,

            tar.oid AS target_table_id,
            tar.relnamespace AS target_schema_id,
            tar.relname AS target_table,
            unnest(con.confkey) as target_column_id,

            con.connamespace AS constraint_nsp,
            con.conname AS constraint_name

        FROM pg_constraint con
        JOIN pg_class fk ON con.conrelid = fk.oid
        JOIN pg_class tar ON con.confrelid = tar.oid
        WHERE con.contype = 'f'
    ) sub
    JOIN pg_attribute fk_att
      ON fk_att.attrelid = fk_table_id AND fk_att.attnum = fk_column_id
    JOIN pg_attribute tar_att
      ON tar_att.attrelid = target_table_id AND tar_att.attnum = target_column_id
    JOIN pg_namespace fk_nsp
      ON fk_schema_id = fk_nsp.oid
    JOIN pg_namespace tar_nsp
      ON target_schema_id = tar_nsp.oid
    GROUP BY 1, 3, sub.constraint_nsp, sub.constraint_name;
    """

    config = get_config()
    configured_tables = set(tables)
    augmented_tables = {
        table
        for fka in config.fk_augmentation
        for table in (fka.fk_table, fka.target_table)
    }
    unavailable = sorted(augmented_tables - configured_tables)
    if unavailable:
        raise ValueError(
            "fk_augmentation references unknown or excluded tables: "
            + ", ".join(unavailable)
        )

    if augmented_tables:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ns.nspname || '.' || cl.relname, att.attname
                  FROM pg_attribute att
                  JOIN pg_class cl ON cl.oid = att.attrelid
                  JOIN pg_namespace ns ON ns.oid = cl.relnamespace
                 WHERE ns.nspname || '.' || cl.relname = ANY(%s)
                   AND att.attnum > 0
                   AND NOT att.attisdropped
                """,
                (list(augmented_tables),),
            )
            columns_by_table = {}
            for qualified_table, column in cur.fetchall():
                columns_by_table.setdefault(qualified_table, set()).add(column)
        for fka in config.fk_augmentation:
            for table, columns in (
                (fka.fk_table, fka.fk_columns),
                (fka.target_table, fka.target_columns),
            ):
                missing = sorted(set(columns) - columns_by_table.get(table, set()))
                if missing:
                    raise ValueError(
                        "fk_augmentation references unknown columns on {}: {}".format(
                            table, ", ".join(missing)
                        )
                    )

    relationships = list()

    with conn.cursor() as cur:
        cur.execute(q)
        for row in cur.fetchall():
            d = dict()
            d["fk_table"] = row[0]
            d["fk_columns"] = row[1]
            d["target_table"] = row[2]
            d["target_columns"] = row[3]

            if d["fk_table"] in tables and d["target_table"] in tables:
                relationships.append(d)

    for fka in config.fk_augmentation:
        augment = asdict(fka)
        not_present = True
        for r in relationships:
            not_present = not_present and not all(
                [r[key] == augment[key] for key in r.keys()]
            )
            if not not_present:
                break

        if (
            augment["fk_table"] in tables
            and augment["target_table"] in tables
            and not_present
        ):
            relationships.append(augment)

    return relationships


def run_query(query, conn, commit=True):
    with conn.cursor() as cur:
        cur.execute(query)
        if commit:
            conn.commit()


def update_sequence_numbering(conn: PsqlConnection, tables: list[str]):
    with conn.cursor() as cur:
        for full_table in tables:
            schema_ = schema_name(full_table)
            if schema_ is None:
                schema_ = "public"
            table_ = table_name(full_table)
            col_seq_query = """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s
                  AND table_name   = %s
                  AND (
                        column_default LIKE 'nextval(%%'
                        OR is_identity = 'YES'
                  )
            """
            cur.execute(col_seq_query, (schema_, table_))
            cols = [row[0] for row in cur.fetchall()]
            if not cols:
                continue
            for col in cols:
                seq_update_query = sql.SQL("""
                    SELECT setval(
                        pg_get_serial_sequence({tbl_lit}, {col_lit}),
                        COALESCE(MAX({col_id}), 0) + 1,
                        false
                    )
                    FROM {tbl_id}
                """).format(
                    tbl_lit=sql.Literal(schema_ + "." + table_),
                    col_lit=sql.Literal(col),
                    col_id=sql.Identifier(col),
                    tbl_id=sql.Identifier(schema_, table_),
                )
                cur.execute(seq_update_query)
        conn.commit()


def get_table_count_estimate(table_name, schema, conn):
    with conn.cursor() as cur:
        if schema is None:
            cur.execute(
                """
                SELECT COALESCE((
                    SELECT reltuples::BIGINT
                      FROM pg_class
                     WHERE oid = to_regclass(%s)
                ), 0)
                """,
                (table_name,),
            )
        else:
            cur.execute(
                """
                SELECT COALESCE((
                    SELECT cls.reltuples::BIGINT
                      FROM pg_class cls
                      JOIN pg_namespace nsp
                        ON nsp.oid = cls.relnamespace
                     WHERE nsp.nspname = %s
                       AND cls.relname = %s
                ), 0)
                """,
                (schema, table_name),
            )
        return cur.fetchone()[0]


def get_table_columns(table, schema, conn):
    cache_key = ("columns", _conn_cache_key(conn), schema, table)
    cached = _metadata_cache.get(cache_key)
    if cached is not None:
        return cached
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT attname
              FROM pg_attribute
             WHERE attrelid=\'"{}"."{}"\'::regclass
               AND attnum > 0
               AND NOT attisdropped
             ORDER BY attnum;""".format(schema, table)
        )
        result = [r[0] for r in cur.fetchall()]
    _metadata_cache[cache_key] = result
    return result


def list_all_user_schemas(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT nspname
              FROM pg_catalog.pg_namespace
             WHERE nspname NOT LIKE 'pg\\_%'
               AND nspname != 'information_schema';
            """
        )
        return [r[0] for r in cur.fetchall()]


def list_all_tables(db_connect):
    conn = db_connect.get_db_connection()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT concat(concat(nsp.nspname,'.'),cls.relname)
              FROM pg_class cls
              JOIN pg_namespace nsp
                ON nsp.oid = cls.relnamespace
             WHERE nsp.nspname NOT IN ('information_schema', 'pg_catalog')
               AND cls.relkind = 'r';
        """)
        return [r[0] for r in cur.fetchall()]


def get_table_page_count(table, schema, conn):
    """Return the number of heap pages for a table from pg_class.

    Config-supplied table names may be unqualified (schema=None); those
    resolve via search_path, matching how fully_qualified_table builds the
    data queries.
    """
    regclass = '"{}"."{}"'.format(schema, table) if schema else '"{}"'.format(table)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT relpages FROM pg_class WHERE oid='{}'::regclass".format(regclass)
        )
        row = cur.fetchone()
    return row[0] if row else 0


def get_table_datatypes(table, schema, conn):
    cache_key = ("datatypes", _conn_cache_key(conn), schema, table)
    cached = _metadata_cache.get(cache_key)
    if cached is not None:
        return cached
    if not schema:
        table_clause = "cl.relname = '{}'".format(table)
    else:
        table_clause = "cl.relname = '{}' AND ns.nspname = '{}'".format(table, schema)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                att.attname,
                ty.typname,
                att.attgenerated,
                att.attidentity
              FROM pg_attribute att
              JOIN pg_class cl ON cl.oid = att.attrelid
              JOIN pg_type ty ON ty.oid = att.atttypid
              JOIN pg_namespace ns ON ns.oid = cl.relnamespace
             WHERE {} AND att.attnum > 0 AND
               NOT att.attisdropped
             ORDER BY att.attnum;
        """.format(table_clause)
        )

        result = [(r[0], r[1], r[2], r[3]) for r in cur.fetchall()]
    _metadata_cache[cache_key] = result
    return result


def truncate_table(target_table, conn):
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE {}".format(target_table))
        conn.commit()
