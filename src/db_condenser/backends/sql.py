"""Existing SQL projection, identifier quoting, and destination-name helpers."""

from db_condenser import database_helper
from db_condenser.backends.contracts import Backend
from db_condenser.config_reader import DbType, get_config
from db_condenser.db_connect import MySqlConnection


# this function generally copies all columns as is, but if the table has been selected as
# breaking a dependency cycle, then it will insert NULLs instead of that table's foreign keys
# to the downstream dependency that breaks the cycle
def columns_to_copy(table, relationships, conn, *, backend: Backend | None = None):
    config = get_config()
    target_breaks = set()
    opportunists = config.preserve_fk_opportunistically
    for fk_table, target_table in config.dependency_break_set:
        if fk_table == table and (fk_table, target_table) not in opportunists:
            target_breaks.add(target_table)

    columns_to_null = set()
    for rel in relationships:
        if rel["fk_table"] == table and rel["target_table"] in target_breaks:
            columns_to_null.update(rel["fk_columns"])

    helper = backend if backend is not None else database_helper.get_specific_helper()
    columns = helper.get_table_columns(table_name(table), schema_name(table), conn)
    return ",".join(
        [
            "{}.{}".format(quoter(table_name(table)), quoter(c))
            if c not in columns_to_null
            else "NULL as {}".format(quoter(c))
            for c in columns
        ]
    )


def fully_qualified_table(table):
    if "." in table:
        return quoter(schema_name(table)) + "." + quoter(table_name(table))
    else:
        return quoter(table_name(table))


def schema_name(table):
    return table.split(".")[0] if "." in table else None


def table_name(table):
    split = table.split(".")
    return split[1] if len(split) > 1 else split[0]


def columns_tupled(columns):
    return "(" + ",".join([quoter(c) for c in columns]) + ")"


def columns_joined(columns):
    return ",".join([quoter(c) for c in columns])


def quoter(id):
    config = get_config()
    q = '"' if config.db_type == DbType.POSTGRES else "`"
    return q + id + q


def mysql_db_name_hack(target, conn):
    if not isinstance(conn, MySqlConnection) or "." not in target:
        return target
    else:
        return conn.db_name + "." + table_name(target)
