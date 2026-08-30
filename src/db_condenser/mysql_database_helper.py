import hashlib
import re
import uuid
from dataclasses import asdict

from db_condenser.config_reader import get_config
from db_condenser.db_connect import MySqlConnection
from db_condenser.subset_utils import (
    columns_joined,
    columns_tupled,
    fully_qualified_table,
    quoter,
    schema_name,
    table_name,
)

system_schemas_str = ",".join(
    [
        "'" + schema + "'"
        for schema in [
            "information_schema",
            "performance_schema",
            "sys",
            "mysql",
            "innodb",
            "tmp",
        ]
    ]
)
temp_db = "tonic_subset_temp_db_398dhjr23"
_incremental_deltas = {}
_incremental_lock_name = None


def prep_temp_dbs(source_conn, destination_conn):
    pass


def unprep_temp_dbs(source_conn, destination_conn):
    pass


def get_batch_size(column_count: int) -> int:
    # Keep generated parameter lists below MySQL's 65,535 placeholder limit.
    return max(1, min(1000, 60_000 // max(column_count, 1)))


def validate_supported_version(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT VERSION()")
        version = cur.fetchone()[0]
    match = re.match(r"^(\d+)\.(\d+)", version)
    if "mariadb" in version.lower() or match is None:
        raise RuntimeError("Unsupported MySQL server version: {}".format(version))
    if tuple(map(int, match.groups())) < (8, 4):
        raise RuntimeError(
            "MySQL 8.4 LTS or newer is required; server is {}".format(version)
        )


def requires_distinct_id_temp_tables() -> bool:
    # MySQL error 1137 prevents reopening one temporary table under multiple
    # aliases in the same query.
    return True


def build_id_table(rows, columns, datatypes, alias):
    """Render identity rows as a derived table without writing to the source."""
    selects = []
    params = []
    for row_index, row in enumerate(rows):
        if len(row) != len(columns):
            raise ValueError("identity row does not match its column count")
        select_columns = []
        for column_index, value in enumerate(row):
            placeholder = "%s"
            if row_index == 0:
                placeholder += " AS col{}".format(column_index)
            select_columns.append(placeholder)
            params.append(value)
        selects.append("SELECT " + ", ".join(select_columns))
    if not selects:
        empty_columns = ", ".join(
            "NULL AS col{}".format(i) for i in range(len(columns))
        )
        selects.append("SELECT {} WHERE FALSE".format(empty_columns))
    return "({}) AS {}".format(" UNION ALL ".join(selects), alias), params


def iter_membership_batches(values):
    batch_size = get_batch_size(1)
    for start in range(0, len(values), batch_size):
        yield values[start : start + batch_size]


def build_membership_filter(column_sql, values):
    placeholders = ", ".join(["%s"] * len(values))
    return "{} IN ({})".format(column_sql, placeholders), list(values)


def temp_table_column(temp_table, index, datatype):
    # MySQL coerces the temporary TEXT value to the indexed source column's
    # type for equality comparison. Keeping the source expression uncast lets
    # the optimizer continue to use its index.
    return "{}.col{}".format(fully_qualified_table(temp_table), index)


def turn_off_constraints(connection):
    cur = connection.cursor()
    try:
        cur.execute("SET UNIQUE_CHECKS=0, FOREIGN_KEY_CHECKS=0;")
        cur.execute("SELECT @@SESSION.sql_mode")
        sql_modes = [mode for mode in cur.fetchone()[0].split(",") if mode]
        if "NO_AUTO_VALUE_ON_ZERO" not in sql_modes:
            sql_modes.append("NO_AUTO_VALUE_ON_ZERO")
            cur.execute("SET SESSION sql_mode = %s", (",".join(sql_modes),))
    finally:
        cur.close()


def _source_table_for_destination(destination_table):
    config = get_config()
    if schema_name(destination_table) == config.destination_db_connection_info.db_name:
        return "{}.{}".format(
            config.source_db_connection_info.db_name,
            table_name(destination_table),
        )
    return destination_table


def _primary_key_columns(conn, schema, table):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.key_column_usage "
            "WHERE table_schema = %s AND table_name = %s "
            "AND constraint_name = 'PRIMARY' ORDER BY ordinal_position",
            (schema, table),
        )
        return [row[0] for row in cur.fetchall()]


def _column_metadata(conn, schema, table):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name, column_type, is_nullable, column_default, extra, "
            "generation_expression, collation_name "
            "FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
            (schema, table),
        )
        return cur.fetchall()


def _acquire_incremental_lock(conn):
    global _incremental_lock_name
    destination = get_config().destination_db_connection_info.db_name
    digest = hashlib.sha256(destination.encode()).hexdigest()[:32]
    _incremental_lock_name = "db-condenser:grow:" + digest
    with conn.cursor() as cur:
        cur.execute("SELECT GET_LOCK(%s, 0)", (_incremental_lock_name,))
        acquired = cur.fetchone()[0]
    if acquired != 1:
        _incremental_lock_name = None
        raise RuntimeError(
            "Another incremental db-condenser run is active for this destination"
        )


def _release_incremental_lock(conn):
    global _incremental_lock_name
    if _incremental_lock_name is None:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT RELEASE_LOCK(%s)", (_incremental_lock_name,))
            cur.fetchone()
    finally:
        _incremental_lock_name = None


def prep_incremental(source_conn, destination_conn, tables):
    _incremental_deltas.clear()
    _acquire_incremental_lock(destination_conn)
    config = get_config()
    destination_schema = config.destination_db_connection_info.db_name
    validated = []
    try:
        for source_table in tables:
            source_schema = schema_name(source_table)
            name = table_name(source_table)
            source_key = _primary_key_columns(source_conn, source_schema, name)
            destination_key = _primary_key_columns(
                destination_conn, destination_schema, name
            )
            if not source_key or not destination_key:
                raise ValueError(
                    "MySQL grow requires a primary key on {} in both source and "
                    "destination".format(source_table)
                )
            if source_key != destination_key:
                raise ValueError(
                    "Source and destination primary keys differ for {}".format(
                        source_table
                    )
                )
            if _column_metadata(source_conn, source_schema, name) != _column_metadata(
                destination_conn, destination_schema, name
            ):
                raise ValueError(
                    "Source and destination columns differ for {}".format(source_table)
                )
            with destination_conn.cursor() as cur:
                cur.execute(
                    "SELECT index_name FROM information_schema.statistics "
                    "WHERE table_schema = %s AND table_name = %s "
                    "AND non_unique = 0 AND index_name <> 'PRIMARY' LIMIT 1",
                    (destination_schema, name),
                )
                secondary_unique = cur.fetchone()
                cur.execute(
                    "SELECT trigger_name FROM information_schema.triggers "
                    "WHERE trigger_schema = %s AND event_object_table = %s LIMIT 1",
                    (destination_schema, name),
                )
                trigger = cur.fetchone()
            if secondary_unique:
                raise ValueError(
                    "MySQL grow does not yet support secondary unique index {} on {}".format(
                        secondary_unique[0], source_table
                    )
                )
            if trigger:
                raise ValueError(
                    "MySQL grow cannot safely write {} while destination trigger {} "
                    "is enabled".format(source_table, trigger[0])
                )
            validated.append((source_table, name, source_key))

        for source_table, name, key in validated:
            digest = hashlib.sha256(source_table.encode()).hexdigest()[:24]
            delta_table = fully_qualified_table("tonic_grow_delta_" + digest)
            destination_table = fully_qualified_table(
                "{}.{}".format(destination_schema, name)
            )
            with destination_conn.cursor() as cur:
                cur.execute(
                    "CREATE TEMPORARY TABLE {} AS SELECT {}, FALSE AS _inserted "
                    "FROM {} LIMIT 0".format(
                        delta_table, columns_joined(key), destination_table
                    )
                )
                cur.execute(
                    "ALTER TABLE {} MODIFY _inserted BOOLEAN NOT NULL, "
                    "ADD PRIMARY KEY ({})".format(delta_table, columns_joined(key))
                )
            _incremental_deltas[source_table] = (delta_table, key)
        destination_conn.commit()
    except BaseException:
        destination_conn.connection.rollback()
        _incremental_deltas.clear()
        _release_incremental_lock(destination_conn)
        raise


def unprep_incremental(conn):
    try:
        with conn.cursor() as cur:
            for delta_table, _ in _incremental_deltas.values():
                cur.execute("DROP TEMPORARY TABLE IF EXISTS {}".format(delta_table))
        conn.commit()
    finally:
        _incremental_deltas.clear()
        _release_incremental_lock(conn)


def retain_incremental(conn):
    # MySQL grow scans every resident parent again, so a failed run can be
    # retried safely without retaining its transient delta tables.
    _incremental_deltas.clear()
    _release_incremental_lock(conn)


def delta_for(table):
    return _incremental_deltas.get(table)


def has_secondary_unique(table):
    return False


def drop_fk_constraints(conn):
    # FOREIGN_KEY_CHECKS is already disabled for this destination session.
    return []


def restore_fk_constraints(conn, _constraints):
    schema = get_config().destination_db_connection_info.db_name
    with conn.cursor() as cur:
        cur.execute(
            "SELECT constraint_name, table_name, referenced_table_schema, "
            "referenced_table_name, "
            "GROUP_CONCAT(column_name ORDER BY ordinal_position), "
            "GROUP_CONCAT(referenced_column_name ORDER BY ordinal_position) "
            "FROM information_schema.key_column_usage "
            "WHERE constraint_schema = %s AND referenced_table_name IS NOT NULL "
            "GROUP BY constraint_name, table_name, referenced_table_schema, "
            "referenced_table_name",
            (schema,),
        )
        relationships = cur.fetchall()
        for (
            constraint,
            child,
            parent_schema,
            parent,
            child_raw,
            parent_raw,
        ) in relationships:
            child_columns = child_raw.split(",")
            parent_columns = parent_raw.split(",")
            child_table = fully_qualified_table("{}.{}".format(schema, child))
            parent_table = fully_qualified_table("{}.{}".format(parent_schema, parent))
            nonnull = " AND ".join(
                "_fk.{} IS NOT NULL".format(quoter(column)) for column in child_columns
            )
            matches = " AND ".join(
                "_pk.{} = _fk.{}".format(quoter(pk), quoter(fk))
                for fk, pk in zip(child_columns, parent_columns)
            )
            cur.execute(
                "SELECT EXISTS (SELECT 1 FROM {} _fk WHERE {} "
                "AND NOT EXISTS (SELECT 1 FROM {} _pk WHERE {}))".format(
                    child_table, nonnull, parent_table, matches
                )
            )
            if cur.fetchone()[0]:
                raise RuntimeError(
                    "Cannot finish MySQL grow: foreign key {} on {} has orphaned "
                    "rows".format(constraint, child_table)
                )
        cur.execute("SET UNIQUE_CHECKS=1, FOREIGN_KEY_CHECKS=1")
    conn.commit()


def copy_rows(
    source,
    destination,
    query,
    destination_table,
    params=None,
    batch_size=1000,
    row_filter=None,
):
    datatypes = get_table_datatypes(
        table_name(destination_table), schema_name(destination_table), destination
    )
    generated_positions = {i for i, datatype in enumerate(datatypes) if datatype[2]}
    insert_columns = [datatype[0] for datatype in datatypes if not datatype[2]]
    if not insert_columns:
        raise ValueError("Table {} has no insertable columns".format(destination_table))
    column_list = columns_joined(insert_columns)
    template = ",".join(["%s"] * len(insert_columns))
    source_table = _source_table_for_destination(destination_table)
    delta = _incremental_deltas.get(source_table)
    identity = delta[1] if delta else []
    update_columns = [column for column in insert_columns if column not in identity]
    if delta and update_columns:
        updates = ", ".join(
            "{0} = _incoming.{0}".format(quoter(column)) for column in update_columns
        )
        duplicate_clause = " AS _incoming ON DUPLICATE KEY UPDATE " + updates
    else:
        duplicate_column = quoter(identity[0] if identity else insert_columns[0])
        duplicate_clause = " ON DUPLICATE KEY UPDATE {} = {}".format(
            duplicate_column, duplicate_column
        )
    insert_query = "INSERT INTO {} ({}) VALUES ({}){}".format(
        fully_qualified_table(destination_table),
        column_list,
        template,
        duplicate_clause,
    )
    identity_positions = (
        [insert_columns.index(column) for column in identity] if delta else []
    )
    cursor = source.cursor()

    try:
        cursor.execute(query, params)
        while True:
            rows = cursor.fetchmany(batch_size)
            if len(rows) == 0:
                break
            fetched_count = len(rows)

            if row_filter is not None:
                rows = row_filter(rows)

            if generated_positions:
                rows = [
                    tuple(
                        value
                        for index, value in enumerate(row)
                        if index not in generated_positions
                    )
                    for row in rows
                ]
            if rows:
                destination_cursor = destination.cursor()
                try:
                    destination_cursor.executemany(insert_query, rows)
                    if delta:
                        delta_rows = [
                            tuple(row[position] for position in identity_positions)
                            for row in rows
                        ]
                        delta_insert = (
                            "INSERT INTO {} ({}, _inserted) VALUES ({}, TRUE) "
                            "ON DUPLICATE KEY UPDATE _inserted = TRUE"
                        ).format(
                            delta[0],
                            columns_joined(identity),
                            ",".join(["%s"] * len(identity)),
                        )
                        destination_cursor.executemany(delta_insert, delta_rows)
                finally:
                    destination_cursor.close()
                destination.commit()

            if fetched_count < batch_size:
                # necessary because mysql doesn't behave if you fetchmany after the last row
                break
    except Exception as e:
        if (
            hasattr(e, "msg")
            and e.msg.startswith("Table")
            and e.msg.endswith("doesn't exist")
        ):
            raise ValueError(
                "Your database has foreign keys to another database. This is not currently supported."
            )
        else:
            raise e
    finally:
        cursor.close()


def create_id_temp_table(
    conn, number_of_columns, destination_table=None, destination_columns=None
):
    temp_table = "tonic_subset_" + str(uuid.uuid4()).replace("-", "")
    with conn.cursor() as cursor:
        if destination_table is None:
            column_defs = ",\n".join(
                ["    col" + str(aye) + "  text" for aye in range(number_of_columns)]
            )
            cursor.execute(
                "CREATE TEMPORARY TABLE {} (\n {} \n)".format(
                    fully_qualified_table(temp_table), column_defs
                )
            )
        else:
            selected = ",".join(
                "IF(TRUE, {}, NULL) AS {}".format(
                    quoter(column), quoter("col" + str(index))
                )
                for index, column in enumerate(destination_columns)
            )
            id_columns = ["col" + str(index) for index in range(number_of_columns)]
            cursor.execute(
                "CREATE TEMPORARY TABLE {} AS SELECT {} FROM {} LIMIT 0".format(
                    fully_qualified_table(temp_table),
                    selected,
                    fully_qualified_table(destination_table),
                )
            )
            cursor.execute(
                "CREATE INDEX {} ON {} ({})".format(
                    quoter("tonic_subset_ids"),
                    fully_qualified_table(temp_table),
                    ",".join(quoter(column) for column in id_columns),
                )
            )
    return temp_table


def copy_to_temp_table(conn, query, target_table, pk_columns=None):
    cur = conn.cursor()
    temp_table = fully_qualified_table(source_db_temp_table(target_table))
    try:
        cur.execute(
            "CREATE TABLE IF NOT EXISTS " + temp_table + " AS " + query + " LIMIT 0"
        )
        if pk_columns:
            query = query + " WHERE {} NOT IN (SELECT {} FROM {})".format(
                columns_tupled(pk_columns), columns_joined(pk_columns), temp_table
            )
        cur.execute("INSERT INTO " + temp_table + " " + query)
        conn.commit()
    finally:
        cur.close()


def clean_temp_table_cells(fk_table, fk_columns, target_table, target_columns, conn):
    fk_alias = "tonic_subset_398dhjr23_fk"
    target_alias = "tonic_subset_398dhjr23_target"

    fk_table = fully_qualified_table(source_db_temp_table(fk_table))
    target_table = fully_qualified_table(source_db_temp_table(target_table))
    assignment_list = ",".join(
        ["{}.{} = NULL".format(fk_alias, quoter(c)) for c in fk_columns]
    )
    column_matching = " AND ".join(
        [
            "{}.{} = {}.{}".format(fk_alias, quoter(fc), target_alias, quoter(tc))
            for fc, tc in zip(fk_columns, target_columns)
        ]
    )
    target_columns_null = " AND ".join(
        ["{}.{} IS NULL".format(target_alias, quoter(tc)) for tc in target_columns]
        + ["{}.{} IS NOT NULL".format(fk_alias, quoter(c)) for c in fk_columns]
    )
    q = "UPDATE {} {} LEFT JOIN {} {} ON {} SET {} WHERE {}".format(
        fk_table,
        fk_alias,
        target_table,
        target_alias,
        column_matching,
        assignment_list,
        target_columns_null,
    )
    run_query(q, conn)


def source_db_temp_table(target_table):
    return temp_db + "." + schema_name(target_table) + "_" + table_name(target_table)


def get_unredacted_fk_relationships(tables, conn):
    cur = conn.cursor()

    q = """
    SELECT
        concat(table_schema, '.', table_name) AS fk_table,
        group_concat(column_name ORDER BY ordinal_position) AS fk_column,
        concat(referenced_table_schema, '.', referenced_table_name) AS pk_name,
        group_concat(referenced_column_name ORDER BY ordinal_position) AS pk_name
    FROM
        information_schema.key_column_usage
    WHERE
        referenced_table_schema NOT IN ({})
    GROUP BY 1, 3, constraint_schema, constraint_name;
    """.format(system_schemas_str)

    cur.execute(q)

    relationships = list()

    for row in cur.fetchall():
        d = dict()
        d["fk_table"] = row[0]
        d["fk_columns"] = row[1].split(",")
        d["target_table"] = row[2]
        d["target_columns"] = row[3].split(",")

        if d["fk_table"] in tables and d["target_table"] in tables:
            relationships.append(d)
    cur.close()

    config = get_config()
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
    cur = conn.cursor()
    try:
        cur.execute(query)
        if commit:
            conn.commit()
    finally:
        cur.close()


def update_sequence_numbering(conn: MySqlConnection, tables: list[str]):
    pass


def get_table_count_estimate(table_name, schema, conn):
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT table_rows AS count
              FROM information_schema.tables
             WHERE table_schema='{}'
               AND table_name='{}'
            """.format(schema, table_name)
        )
        row = cur.fetchone()
        return row[0] if row is not None and row[0] is not None else 0
    finally:
        cur.close()


def get_table_datatypes(table, schema, conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name, data_type, generation_expression, extra
              FROM information_schema.columns
             WHERE table_schema = '{}'
               AND table_name = '{}'
             ORDER BY ordinal_position
            """.format(schema, table)
        )
        results = []
        for r in cur.fetchall():
            generated = "s" if r[2] else ""
            identity = "a" if "auto_increment" in (r[3] or "") else ""
            results.append((r[0], r[1], generated, identity))
        return results


def get_table_columns(table, schema, conn):
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT column_name
              FROM information_schema.columns
             WHERE table_schema = '{}'
               AND table_name = '{}'
             ORDER BY ordinal_position
             """.format(schema, table)
        )
        return [r[0] for r in cur.fetchall()]
    finally:
        cur.close()


def list_all_tables(db_connect):
    conn = db_connect.get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT concat(concat(table_schema,'.'),table_name)
              FROM information_schema.tables
             WHERE table_schema = '{}' AND table_type = 'BASE TABLE';
             """.format(db_connect.db_name)
        )
        return [r[0] for r in cur.fetchall()]
    finally:
        cur.close()


def truncate_table(target_table, conn):
    cur = conn.cursor()
    try:
        cur.execute("TRUNCATE TABLE {}".format(target_table))
        conn.commit()
    finally:
        cur.close()
