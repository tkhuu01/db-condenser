"""Downstream SQL selection: fetch missing parents required by child rows.

Moved from Subset without changing SQL, batching, or selection semantics.
The executor supplies the run connections and existing transfer helpers.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from db_condenser.backends.sql import (
    columns_to_copy,
    fully_qualified_table,
    mysql_db_name_hack,
    quoter,
    schema_name,
    table_name,
)
from db_condenser.subset_utils import compute_batch_size, redact_relationships

if TYPE_CHECKING:
    from db_condenser.backends.execution import SqlSelectionExecutor


def _downstream_delta_plan(
    executor: SqlSelectionExecutor, referencing_tables, dest_conn
):
    """Decide the child-scan sources for an incremental downstream step.

    Returns (skip, child_plan):
    - skip=True: every referencing child tracked a delta and all are
      empty, so no row inserted this run can reference a missing parent
    - child_plan: {fk_table: (delta_table, identity_cols) or None}. None
      means scan the child fully; a child with an empty delta is left out
      entirely (nothing new to scan).
      child_plan=None means full (non-incremental) behavior.

    Unlike upstream, children contribute missing-parent IDs independently
    (union semantics), so mixed per-child decisions are safe.
    """
    if not executor.incremental:
        return False, None
    children = {r["fk_table"] for r in referencing_tables}
    plan = {}
    with dest_conn.cursor() as cur:
        for child in children:
            delta = executor.helper.delta_for(child)
            if delta is None:
                plan[child] = None
                continue
            cur.execute("SELECT EXISTS (SELECT 1 FROM {})".format(delta[0]))
            if cur.fetchone()[0]:
                plan[child] = delta
    if not plan:
        return True, None
    return False, plan


def select_downstream(
    executor: SqlSelectionExecutor,
    table,
    relationships,
    source_conn=None,
    dest_conn=None,
    allow_chunk=False,
):
    source_conn = source_conn or executor.source
    dest_conn = dest_conn or executor.destination
    referencing_tables = [
        r for r in redact_relationships(relationships) if r["target_table"] == table
    ]

    if not referencing_tables:
        return

    skip, child_plan = _downstream_delta_plan(executor, referencing_tables, dest_conn)
    if skip:
        return

    relationship_groups = {}
    for relationship in referencing_tables:
        key = tuple(relationship["target_columns"])
        relationship_groups.setdefault(key, []).append(relationship)

    stage_before_insert = executor.incremental and executor.helper.has_secondary_unique(
        table
    )
    staged = False

    def transfer_rows(
        batch_source_conn,
        batch_dest_conn,
        query,
        destination_table,
        params=None,
        batch_size=None,
    ):
        nonlocal staged
        if not stage_before_insert:
            executor.copy_rows(
                batch_source_conn,
                batch_dest_conn,
                query,
                destination_table,
                params,
                batch_size,
            )
            return
        executor.helper.stage_rows(
            batch_source_conn,
            batch_dest_conn,
            query,
            destination_table,
            params,
            append=staged,
        )
        staged = True

    columns_query = columns_to_copy(
        table, relationships, source_conn, backend=executor.backend
    )
    for pk_columns, group in relationship_groups.items():
        temp_table = executor.backend.create_id_temp_table(dest_conn, len(pk_columns))

        for r in group:
            fk_table = r["fk_table"]
            fk_columns = r["fk_columns"]

            if child_plan is not None and fk_table not in child_plan:
                # no rows were inserted into this child this run
                continue
            delta = child_plan.get(fk_table) if child_plan else None

            fk_qualified = fully_qualified_table(
                mysql_db_name_hack(fk_table, dest_conn)
            )
            target_qualified = fully_qualified_table(
                mysql_db_name_hack(table, dest_conn)
            )
            delta_join = ""
            if delta is not None:
                delta_table, child_pk = delta
                delta_join = " JOIN {} _d ON {}".format(
                    delta_table,
                    " AND ".join(
                        "_fk.{} = _d.{}".format(quoter(c), quoter(c)) for c in child_pk
                    ),
                )
            exists_conditions = " AND ".join(
                "_t.{} = _fk.{}".format(quoter(pc), quoter(fc))
                for pc, fc in zip(pk_columns, fk_columns)
            )
            select_q = (
                "SELECT DISTINCT {} FROM {} _fk{}"
                " WHERE NOT EXISTS (SELECT 1 FROM {} _t WHERE {})".format(
                    ",".join("_fk.{}".format(quoter(c)) for c in fk_columns),
                    fk_qualified,
                    delta_join,
                    target_qualified,
                    exists_conditions,
                )
            )
            insert_q = 'INSERT INTO "{}" {}'.format(temp_table, select_q)
            with dest_conn.cursor() as cur:
                cur.execute(insert_q)
            dest_conn.commit()

        if executor.config.use_temp_tables:
            _subset_downstream_temp_tables(
                executor,
                table,
                temp_table,
                pk_columns,
                columns_query,
                source_conn,
                dest_conn,
                transfer_rows,
            )
        else:
            _subset_downstream_unnest(
                executor,
                table,
                temp_table,
                pk_columns,
                columns_query,
                source_conn,
                dest_conn,
                transfer_rows,
                allow_chunk and not stage_before_insert,
            )

    if staged:
        executor.helper.apply_staged(dest_conn, table, "refresh")
        executor.helper.apply_staged(dest_conn, table, "insert")


def _subset_downstream_temp_tables(
    executor: SqlSelectionExecutor,
    table,
    dest_temp_table,
    pk_columns,
    columns_query,
    source_conn,
    dest_conn,
    transfer_rows,
):
    downstream_datatypes = {
        col: typ
        for col, typ, _, _ in executor.backend.get_table_datatypes(
            table_name(table), schema_name(table), source_conn
        )
    }
    dest_query = "SELECT DISTINCT * FROM {}".format(
        fully_qualified_table(dest_temp_table)
    )
    src_id_temp = executor.stream_ids_to_source_temp(
        dest_query, pk_columns, source_conn, dest_conn
    )
    q = executor.build_temp_table_join(
        table, src_id_temp, pk_columns, downstream_datatypes, columns_query
    )
    transfer_rows(
        source_conn,
        dest_conn,
        q,
        mysql_db_name_hack(table, dest_conn),
        batch_size=compute_batch_size(len(downstream_datatypes)),
    )


def _subset_downstream_unnest(
    executor: SqlSelectionExecutor,
    table,
    dest_temp_table,
    pk_columns,
    columns_query,
    source_conn,
    dest_conn,
    transfer_rows,
    allow_chunk=False,
):
    downstream_datatypes = {
        col: typ
        for col, typ, _, _ in executor.backend.get_table_datatypes(
            table_name(table), schema_name(table), source_conn
        )
    }

    def copy_batch(valid_rows, batch_source_conn, batch_dest_conn):
        unnest_args = ", ".join(
            "%s::{}[]".format(downstream_datatypes[col]) for col in pk_columns
        )
        join_cols = ", ".join("col{}".format(i) for i in range(len(pk_columns)))
        join_conditions = " AND ".join(
            "{}.{} = ids.col{}".format(fully_qualified_table(table), quoter(col), i)
            for i, col in enumerate(pk_columns)
        )
        q = (
            "SELECT {cols} FROM {tbl}"
            " JOIN unnest({unnest}) AS ids({join_cols})"
            " ON {conditions}"
        ).format(
            cols=columns_query,
            tbl=fully_qualified_table(table),
            unnest=unnest_args,
            join_cols=join_cols,
            conditions=join_conditions,
        )
        params = [[row[i] for row in valid_rows] for i in range(len(pk_columns))]
        transfer_rows(
            batch_source_conn,
            batch_dest_conn,
            q,
            mysql_db_name_hack(table, batch_dest_conn),
            params,
            batch_size=compute_batch_size(len(downstream_datatypes)),
        )

    cursor_name = "table_cursor_" + str(uuid.uuid4()).replace("-", "")
    cursor = dest_conn.cursor(name=cursor_name, withhold=True)
    try:
        cursor_query = "SELECT DISTINCT * FROM {}".format(
            fully_qualified_table(dest_temp_table)
        )
        cursor.execute(cursor_query)
        batch_size = compute_batch_size(len(pk_columns))
        if allow_chunk and executor.source_pool:
            executor.parallel_id_batches(cursor, batch_size, copy_batch)
            return
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            valid_rows = [row for row in rows if all(c is not None for c in row)]
            if not valid_rows:
                continue
            copy_batch(valid_rows, source_conn, dest_conn)
    finally:
        cursor.close()
