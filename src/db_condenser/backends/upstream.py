"""Upstream SQL selection: match child rows against selected parent keys.

Moved from Subset without changing SQL, batching, or selection semantics.
The executor supplies the run connections and existing transfer helpers.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from db_condenser.backends.sql import (
    columns_joined,
    columns_to_copy,
    fully_qualified_table,
    mysql_db_name_hack,
    quoter,
    schema_name,
    table_name,
)
from db_condenser.subset_utils import (
    compute_batch_size,
    redact_relationships,
    upstream_filter_match,
)

if TYPE_CHECKING:
    from db_condenser.backends.execution import SqlSelectionExecutor


def _upstream_delta_plan(
    executor: SqlSelectionExecutor, relevant_key_constraints, dest_conn
):
    """Decide the upstream ID sources for an incremental (top-up) run.

    Returns (skip, delta_plan):
    - skip=True: no parent gained rows this run, no new child rows possible
    - delta_plan: {parent_table: (delta_table, pk_cols)} for parents with
      rows inserted this run (upserted rows don't count: their children
      were already considered when they first arrived). None means full
      (non-incremental) behavior because this run doesn't delta-restrict
      upstream reads (recreate, or grow which scans all resident parents).
    """
    if not executor.upstream_delta_reads:
        return False, None
    parents = {kc["target_table"] for kc in relevant_key_constraints}
    deltas = {p: executor.helper.delta_for(p) for p in parents}
    if not all(deltas.values()):
        return False, None
    nonempty = {}
    with dest_conn.cursor() as cur:
        for p, (delta_table, _) in deltas.items():
            cur.execute(
                "SELECT EXISTS (SELECT 1 FROM {} WHERE _inserted)".format(delta_table)
            )
            nonempty[p] = cur.fetchone()[0]
    if not any(nonempty.values()):
        return True, None
    return False, {p: deltas[p] for p in parents if nonempty[p]}


def _upstream_ids_query(
    executor: SqlSelectionExecutor, kc_target, target_cols, dest_conn, delta_plan
):
    """Build the destination-side query for a parent's referenced columns.

    With a delta plan entry for the parent, reads only the rows added this
    run (joined to its delta table on row identity); otherwise the full table.
    """
    qualified = fully_qualified_table(mysql_db_name_hack(kc_target, dest_conn))
    delta = delta_plan.get(kc_target) if delta_plan else None
    if delta is None:
        return "SELECT DISTINCT {} FROM {}".format(
            columns_joined(target_cols), qualified
        )
    delta_table, pk_cols = delta
    cols = ",".join("_t.{}".format(quoter(c)) for c in target_cols)
    join_cond = " AND ".join(
        "_t.{} = _d.{}".format(quoter(c), quoter(c)) for c in pk_cols
    )
    return "SELECT DISTINCT {} FROM {} _t JOIN {} _d ON {} AND _d._inserted".format(
        cols, qualified, delta_table, join_cond
    )


def select_upstream(
    executor: SqlSelectionExecutor,
    target,
    processed_tables,
    relationships,
    source_conn,
    dest_conn,
    allow_chunk=False,
):
    redacted_relationships = redact_relationships(relationships)
    relevant_key_constraints = list(
        filter(
            lambda r: r["target_table"] in processed_tables and r["fk_table"] == target,
            redacted_relationships,
        )
    )
    if len(relevant_key_constraints) == 0 or target in processed_tables:
        return False

    skip, delta_plan = _upstream_delta_plan(
        executor, relevant_key_constraints, dest_conn
    )
    if skip:
        return True

    table_columns = executor.backend.get_table_columns(
        table_name(target), schema_name(target), source_conn
    )
    upstream_filters = upstream_filter_match(target, table_columns)
    columns_query = columns_to_copy(
        target, relationships, source_conn, backend=executor.backend
    )

    if executor.config.use_temp_tables:
        _subset_upstream_temp_tables(
            executor,
            target,
            relevant_key_constraints,
            upstream_filters,
            source_conn,
            dest_conn,
            delta_plan,
            columns_query,
        )
    else:
        _subset_upstream_unnest(
            executor,
            target,
            relevant_key_constraints,
            upstream_filters,
            source_conn,
            dest_conn,
            delta_plan,
            allow_chunk,
            columns_query,
        )

    return True


def _subset_upstream_temp_tables(
    executor: SqlSelectionExecutor,
    target,
    relevant_key_constraints,
    upstream_filters,
    source_conn,
    dest_conn,
    delta_plan=None,
    columns_query="*",
):
    fk_datatypes = {
        col: typ
        for col, typ, _, _ in executor.backend.get_table_datatypes(
            table_name(target), schema_name(target), source_conn
        )
    }
    groups = {}
    for kc in relevant_key_constraints:
        key = (kc["target_table"], tuple(kc["target_columns"]))
        groups.setdefault(key, []).append(kc)

    group_temps = {}
    delta_temps = {}
    for kc_target, target_cols in groups:
        dest_query = _upstream_ids_query(
            executor, kc_target, list(target_cols), dest_conn, None
        )
        group_temps[(kc_target, target_cols)] = executor.stream_ids_to_source_temp(
            dest_query, target_cols, source_conn, dest_conn
        )
        if delta_plan and kc_target in delta_plan:
            delta_query = _upstream_ids_query(
                executor, kc_target, list(target_cols), dest_conn, delta_plan
            )
            delta_temps[(kc_target, target_cols)] = executor.stream_ids_to_source_temp(
                delta_query, target_cols, source_conn, dest_conn
            )

    kcs = relevant_key_constraints
    if delta_plan is None:
        passes = [None]
    else:
        # one pass per constraint whose parent gained rows this run: that
        # constraint joins the delta, the others join the full ID sets
        # (AND semantics must hold against everything already imported)
        passes = [
            j
            for j, kc in enumerate(kcs)
            if (kc["target_table"], tuple(kc["target_columns"])) in delta_temps
        ]
        if not passes:
            return

    fqt = fully_qualified_table(target)
    for pass_j in passes:
        joins = ""
        match_conditions = []
        nullable_conditions = []
        for idx, kc in enumerate(kcs):
            key = (kc["target_table"], tuple(kc["target_columns"]))
            if pass_j is not None and pass_j == idx:
                id_temp = delta_temps[key]
            else:
                id_temp = group_temps[key]
            fk_cols = kc["fk_columns"]
            alias = "_ids{}".format(idx)
            join_conditions = " AND ".join(
                "{}.{} = {}.col{}::{}".format(
                    fqt, quoter(col), alias, i, fk_datatypes[col]
                )
                for i, col in enumerate(fk_cols)
            )
            joins += ' LEFT JOIN "{}" AS {} ON {}'.format(
                id_temp, alias, join_conditions
            )
            match_conditions.append("{}.col0 IS NOT NULL".format(alias))
            nullable_conditions.append(
                " OR ".join("{}.{} IS NULL".format(fqt, quoter(col)) for col in fk_cols)
            )

        q = "SELECT {} FROM {}{}".format(columns_query, fqt, joins)
        conditions = [
            "({} OR {})".format(nullable, matched)
            for nullable, matched in zip(nullable_conditions, match_conditions)
        ]
        # NULL foreign keys are neutral (PostgreSQL MATCH SIMPLE), but a
        # row still needs at least one selected parent to enter the
        # subset. In topup, that match specifically must be the parent
        # delta driving this pass.
        if pass_j is None:
            conditions.append("({})".format(" OR ".join(match_conditions)))
        else:
            conditions.append(match_conditions[pass_j])
        conditions.extend("({})".format(condition) for condition in upstream_filters)
        q += " WHERE {}".format(" AND ".join(conditions))
        if executor.config.max_rows_per_table is not None:
            q += " LIMIT {}".format(executor.config.max_rows_per_table)
        executor.copy_rows(
            source_conn,
            dest_conn,
            q,
            target,
            batch_size=compute_batch_size(len(fk_datatypes)),
        )


def _build_upstream_unnest_query(
    executor: SqlSelectionExecutor,
    fqt,
    kc_rows,
    fk_datatypes,
    upstream_filters,
    required_join=None,
    columns_query=None,
):
    """Build the source-side join for (constraint, id_rows) pairs."""
    joins = ""
    all_params = []
    match_conditions = []
    nullable_conditions = []
    for join_idx, (kc, rows) in enumerate(kc_rows):
        fk_cols = kc["fk_columns"]
        unnest_args = ", ".join("%s::{}[]".format(fk_datatypes[col]) for col in fk_cols)
        join_cols = ", ".join("col{}".format(i) for i in range(len(fk_cols)))
        join_conditions = " AND ".join(
            "{}.{} = ids{}.col{}".format(fqt, quoter(col), join_idx, i)
            for i, col in enumerate(fk_cols)
        )
        joins += (
            " LEFT JOIN unnest({unnest}) AS ids{idx}({join_cols}) ON {conds}".format(
                unnest=unnest_args,
                idx=join_idx,
                join_cols=join_cols,
                conds=join_conditions,
            )
        )
        all_params.extend([row[i] for row in rows] for i in range(len(fk_cols)))
        match_conditions.append("ids{}.col0 IS NOT NULL".format(join_idx))
        nullable_conditions.append(
            " OR ".join("{}.{} IS NULL".format(fqt, quoter(col)) for col in fk_cols)
        )

    q = "SELECT {} FROM {}{}".format(columns_query or fqt + ".*", fqt, joins)
    conditions = [
        "({} OR {})".format(nullable, matched)
        for nullable, matched in zip(nullable_conditions, match_conditions)
    ]
    if required_join is None:
        conditions.append("({})".format(" OR ".join(match_conditions)))
    else:
        conditions.append(match_conditions[required_join])
    conditions.extend("({})".format(condition) for condition in upstream_filters)
    q += " WHERE {}".format(" AND ".join(conditions))
    return q, all_params


def _subset_upstream_unnest(
    executor: SqlSelectionExecutor,
    target,
    relevant_key_constraints,
    upstream_filters,
    source_conn,
    dest_conn,
    delta_plan=None,
    allow_chunk=False,
    columns_query="*",
):
    fk_datatypes = {
        col: typ
        for col, typ, _, _ in executor.backend.get_table_datatypes(
            table_name(target), schema_name(target), source_conn
        )
    }

    groups = {}
    for kc in relevant_key_constraints:
        key = (kc["target_table"], tuple(kc["target_columns"]))
        groups.setdefault(key, []).append(kc)

    fqt = fully_qualified_table(target)
    batch_size = compute_batch_size(len(fk_datatypes))

    # a single ID stream can only serve a single constraint: binding the
    # same batch to two constraints (e.g. from/to FKs to one parent) drops
    # AND-pairs whose IDs span two batches. Multi-constraint tables take
    # the multi-group path, which joins each batched constraint against
    # the other constraints' full ID sets.
    streaming_ok = (
        len(relevant_key_constraints) == 1
        and executor.config.max_rows_per_table is None
    )

    if streaming_ok:
        _upstream_unnest_streamed(
            executor,
            target,
            fqt,
            groups,
            fk_datatypes,
            upstream_filters,
            batch_size,
            source_conn,
            dest_conn,
            delta_plan,
            allow_chunk,
            columns_query,
        )
        return

    _upstream_unnest_multi_group(
        executor,
        target,
        fqt,
        groups,
        fk_datatypes,
        upstream_filters,
        batch_size,
        source_conn,
        dest_conn,
        delta_plan,
        columns_query,
    )


def _fetch_dest_rows(executor: SqlSelectionExecutor, query, batch_size, dest_conn):
    cursor_name = "table_cursor_" + str(uuid.uuid4()).replace("-", "")
    dest_cursor = dest_conn.cursor(name=cursor_name, withhold=True)
    try:
        dest_cursor.execute(query)
        rows = []
        while True:
            batch = dest_cursor.fetchmany(batch_size)
            if not batch:
                break
            rows.extend(row for row in batch if all(c is not None for c in row))
    finally:
        dest_cursor.close()
    return rows


def _upstream_unnest_streamed(
    executor: SqlSelectionExecutor,
    target,
    fqt,
    groups,
    fk_datatypes,
    upstream_filters,
    batch_size,
    source_conn,
    dest_conn,
    delta_plan=None,
    allow_chunk=False,
    columns_query="*",
):
    group_key = next(iter(groups))
    kc_target, target_cols = group_key

    query = _upstream_ids_query(
        executor, kc_target, list(target_cols), dest_conn, delta_plan
    )

    def copy_batch(valid_rows, batch_source_conn, batch_dest_conn):
        q, params = _build_upstream_unnest_query(
            executor,
            fqt,
            [(kc, valid_rows) for kc in groups[group_key]],
            fk_datatypes,
            upstream_filters,
            required_join=0,
            columns_query=columns_query,
        )
        executor.copy_rows(
            batch_source_conn,
            batch_dest_conn,
            q,
            target,
            params,
            batch_size=compute_batch_size(len(fk_datatypes)),
        )

    cursor_name = "table_cursor_" + str(uuid.uuid4()).replace("-", "")
    dest_cursor = dest_conn.cursor(name=cursor_name, withhold=True)
    try:
        dest_cursor.execute(query)
        if allow_chunk and executor.source_pool:
            first = dest_cursor.fetchmany(batch_size)
            if not first:
                return
            valid_first = [row for row in first if all(c is not None for c in row)]
            kcs_in_group = groups[group_key]
            fk_cols = kcs_in_group[0]["fk_columns"]
            if len(first) < batch_size and len(kcs_in_group) == 1 and len(fk_cols) == 1:
                # the whole ID set fits one batch, so there are no
                # batches to fan out: split the child table read by
                # ctid ranges instead, with the IDs as a filter
                if not valid_first:
                    return
                col = fk_cols[0]
                conditions = [
                    "{}.{} = ANY(%s::{}[])".format(fqt, quoter(col), fk_datatypes[col])
                ] + list(upstream_filters)
                ids = [row[0] for row in valid_first]
                if executor.copy_table_parallel(
                    target, columns_query, conditions, [ids]
                ):
                    return
                copy_batch(valid_first, source_conn, dest_conn)
                return
            executor.parallel_id_batches(
                dest_cursor, batch_size, copy_batch, initial_rows=valid_first
            )
            return
        while True:
            batch = dest_cursor.fetchmany(batch_size)
            if not batch:
                break
            valid_rows = [row for row in batch if all(c is not None for c in row)]
            if not valid_rows:
                continue
            copy_batch(valid_rows, source_conn, dest_conn)
    finally:
        dest_cursor.close()


def _upstream_unnest_multi_group(
    executor: SqlSelectionExecutor,
    target,
    fqt,
    groups,
    fk_datatypes,
    upstream_filters,
    batch_size,
    source_conn,
    dest_conn,
    delta_plan=None,
    columns_query="*",
):
    kcs = [kc for group in groups.values() for kc in group]

    def group_of(kc):
        return (kc["target_table"], tuple(kc["target_columns"]))

    kcs_per_group = {}
    for kc in kcs:
        kcs_per_group[group_of(kc)] = kcs_per_group.get(group_of(kc), 0) + 1

    delta_rows = {}
    if delta_plan:
        for kc_target, target_cols in groups:
            if kc_target in delta_plan:
                delta_rows[(kc_target, target_cols)] = _fetch_dest_rows(
                    executor,
                    _upstream_ids_query(
                        executor, kc_target, list(target_cols), dest_conn, delta_plan
                    ),
                    batch_size,
                    dest_conn,
                )

    if delta_plan is None:
        passes = [None]
    else:
        # one pass per constraint whose parent gained rows this run: that
        # constraint uses the delta IDs, the others use the full ID sets
        # (AND semantics must hold against everything already imported)
        passes = [j for j, kc in enumerate(kcs) if delta_rows.get(group_of(kc))]
        if not passes:
            return

    # count each group's IDs so the largest set can be streamed through a
    # cursor instead of held in memory. Only a group referenced by a single
    # constraint can stream: a shared group must stay resident so every
    # constraint joins its full set (batching two constraints against the
    # same batch would drop cross-batch pairs).
    full_counts = {}
    with dest_conn.cursor() as cur:
        for kc_target, target_cols in groups:
            q = _upstream_ids_query(
                executor, kc_target, list(target_cols), dest_conn, None
            )
            cur.execute("SELECT COUNT(*) FROM ({}) _ids".format(q))
            full_counts[(kc_target, target_cols)] = cur.fetchone()[0]

    full_rows = {}  # loaded lazily, only for groups that must stay resident

    def resident_rows(group_key):
        if group_key not in full_rows:
            full_rows[group_key] = _fetch_dest_rows(
                executor,
                _upstream_ids_query(
                    executor, group_key[0], list(group_key[1]), dest_conn, None
                ),
                batch_size,
                dest_conn,
            )
        return full_rows[group_key]

    copy_batch = compute_batch_size(len(fk_datatypes))

    def copy_kc_rows(kc_rows, single_shot, required_join):
        q, params = _build_upstream_unnest_query(
            executor,
            fqt,
            kc_rows,
            fk_datatypes,
            upstream_filters,
            required_join=required_join,
            columns_query=columns_query,
        )
        if single_shot and executor.config.max_rows_per_table is not None:
            q += " LIMIT {}".format(executor.config.max_rows_per_table)
        executor.copy_rows(
            source_conn, dest_conn, q, target, params, batch_size=copy_batch
        )

    def copy_neutral_rows(kc_rows, batched_idx, required_join):
        if required_join == batched_idx:
            return
        batched_kc = kc_rows[batched_idx][0]
        remaining = [row for i, row in enumerate(kc_rows) if i != batched_idx]
        if not remaining or not any(rows for _, rows in remaining):
            return
        remapped_required = required_join
        if remapped_required is not None and remapped_required > batched_idx:
            remapped_required -= 1
        nullable = " OR ".join(
            "{}.{} IS NULL".format(fqt, quoter(col)) for col in batched_kc["fk_columns"]
        )
        q, params = _build_upstream_unnest_query(
            executor,
            fqt,
            remaining,
            fk_datatypes,
            list(upstream_filters) + [nullable],
            required_join=remapped_required,
            columns_query=columns_query,
        )
        executor.copy_rows(
            source_conn, dest_conn, q, target, params, batch_size=copy_batch
        )

    for pass_j in passes:
        # groups whose full set this pass doesn't need: the delta
        # constraint's own group, when no other constraint shares it
        stream_candidates = [
            gk
            for gk in groups
            if kcs_per_group[gk] == 1
            and not (pass_j is not None and group_of(kcs[pass_j]) == gk)
        ]
        stream_gk = (
            max(stream_candidates, key=lambda gk: full_counts[gk])
            if stream_candidates
            else None
        )

        def rows_for(j, kc):
            if pass_j is not None and pass_j == j:
                return delta_rows[group_of(kc)]
            return resident_rows(group_of(kc))

        if stream_gk is None:
            # every group is shared (or delta-sourced): all resident,
            # batching the largest set as before
            kc_rows = [(kc, rows_for(j, kc)) for j, kc in enumerate(kcs)]
            if not any(rows for _, rows in kc_rows):
                continue
            largest_idx = max(range(len(kc_rows)), key=lambda i: len(kc_rows[i][1]))
            largest_rows = kc_rows[largest_idx][1]
            if len(largest_rows) <= batch_size:
                copy_kc_rows(kc_rows, single_shot=True, required_join=pass_j)
                continue
            for i in range(0, len(largest_rows), batch_size):
                batch_kc_rows = list(kc_rows)
                batch_kc_rows[largest_idx] = (
                    kc_rows[largest_idx][0],
                    largest_rows[i : i + batch_size],
                )
                copy_kc_rows(
                    batch_kc_rows,
                    single_shot=False,
                    required_join=(pass_j if pass_j is not None else largest_idx),
                )
            copy_neutral_rows(kc_rows, largest_idx, pass_j)
            continue

        # stream the largest single-constraint group; everything else
        # (small groups, deltas) stays resident
        stream_idx = next(j for j, kc in enumerate(kcs) if group_of(kc) == stream_gk)
        cursor_name = "table_cursor_" + str(uuid.uuid4()).replace("-", "")
        dest_cursor = dest_conn.cursor(name=cursor_name, withhold=True)
        try:
            dest_cursor.execute(
                _upstream_ids_query(
                    executor, stream_gk[0], list(stream_gk[1]), dest_conn, None
                )
            )
            first = True
            while True:
                batch = dest_cursor.fetchmany(batch_size)
                if not batch:
                    break
                valid_rows = [row for row in batch if all(c is not None for c in row)]
                single_shot = first and len(batch) < batch_size
                first = False
                if not valid_rows:
                    continue
                kc_rows = [
                    (kc, valid_rows if j == stream_idx else rows_for(j, kc))
                    for j, kc in enumerate(kcs)
                ]
                copy_kc_rows(
                    kc_rows,
                    single_shot=single_shot,
                    required_join=(pass_j if pass_j is not None else stream_idx),
                )
        finally:
            dest_cursor.close()
        kc_rows = [
            (kc, [] if j == stream_idx else rows_for(j, kc)) for j, kc in enumerate(kcs)
        ]
        copy_neutral_rows(kc_rows, stream_idx, pass_j)
