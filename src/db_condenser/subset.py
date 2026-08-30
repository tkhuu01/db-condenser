import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

from db_condenser import database_helper
from db_condenser.config_reader import (
    DbType,
    DestinationMode,
    InitialTarget,
    get_config,
)
from db_condenser.db_connect import DbConnect
from db_condenser.subset_utils import (
    columns_joined,
    columns_to_copy,
    compute_disconnected_tables,
    compute_downstream_strata,
    compute_upstream_strata,
    fully_qualified_table,
    mysql_db_name_hack,
    print_progress,
    quoter,
    redact_relationships,
    schema_name,
    table_name,
    upstream_filter_match,
)
from db_condenser.topo_orderer import get_topological_order_by_tables

"""
A QUICK NOTE ON DEFINITIONS:

Foreign key relationships form a graph. We make sure all subsetting happens on DAGs.
Nodes in the DAG are tables, and FKs point from the table with a FK column to the table
with the PK column. In other words, tables with FKs are upstream of tables with PKs.

Sometimes we'll refer to tables as downstream or 'target' tables, because they are
targeted by foreign keys. We will also use upstream or 'fk' tables, because they
have foreign keys.

Generally speaking, tables downstream of other tables have their membership defined
by the requirements of their upstream tables. And tables upstream can be more flexible
about their membership vis-a-vis the downstream tables (i.e. upstream tables can decide
to include more or less).
"""


class Subset:
    def __init__(
        self,
        source_dbc: DbConnect,
        destination_dbc: DbConnect,
        all_tables: list[str],
    ):
        self.config = get_config()
        self.__all_tables = all_tables
        self.__incremental = self.config.is_incremental
        unknown_incremental_keys = set(self.config.incremental_key_map) - set(
            self.__all_tables
        )
        if self.__incremental and unknown_incremental_keys:
            raise ValueError(
                "incremental_keys references unknown or excluded tables: "
                + ", ".join(sorted(unknown_incremental_keys))
            )

        self.__source_dbc = source_dbc
        self.__destination_dbc = destination_dbc
        self.__db_helper = database_helper.get_specific_helper()
        self.__source_conn = source_dbc.get_db_connection(read_repeatable=True)
        try:
            self.__db_helper.validate_supported_version(self.__source_conn)
        except BaseException:
            self.__source_conn.close()
            raise
        try:
            self.__destination_conn = self.__get_destination_connection()
        except BaseException:
            self.__source_conn.close()
            raise

        if self.config.use_copy_protocol and self.config.db_type == DbType.POSTGRES:
            self.__copy_rows = self.__db_helper.copy_rows_copy_protocol
        else:
            self.__copy_rows = self.__db_helper.copy_rows

        if self.config.use_temp_tables:
            self.__check_source_writable()

        # topup/grow mean the destination already exists: track selected row
        # identities in per-table delta tables and drop/restore FKs around the run
        # topup restricts upstream parent ID reads to this run's deltas
        # (already-imported entities stay frozen); grow reads full destination
        # parent ID sets so new source children of old parents are picked up
        self.__upstream_delta_reads = (
            self.__incremental and self.config.destination_mode == DestinationMode.TOPUP
        )
        self.__dropped_fks = []
        self.__incremental_prepared = False

        # export one snapshot from the main source connection; every other
        # source connection imports it so all reads see the database as of
        # the same instant. The main connection's transaction must stay open
        # for the whole run to keep the snapshot importable.
        self.__snapshot_id = None
        if self.config.db_type == DbType.POSTGRES:
            with self.__source_conn.cursor() as cur:
                cur.execute("SELECT pg_export_snapshot()")
                self.__snapshot_id = cur.fetchone()[0]

        # table-level concurrency follows parallel_read_workers when set,
        # otherwise keeps the historical 4 threads
        if self.config.db_type == DbType.MYSQL:
            # MySQL cannot export an InnoDB snapshot into other sessions.
            # Keep all source reads on the main repeatable-read transaction.
            self.__table_workers = 1
        else:
            self.__table_workers = (
                self.config.parallel_read_workers
                if self.config.parallel_read_workers > 1
                else 4
            )

        self.__source_pool = []
        if (
            self.config.parallel_read_workers > 1
            and self.config.db_type == DbType.POSTGRES
        ):
            for _ in range(self.config.parallel_read_workers):
                self.__source_pool.append(self.__get_source_connection())

    def __get_source_connection(self):
        """Open a source connection pinned to the run's exported snapshot."""
        conn = self.__source_dbc.get_db_connection(read_repeatable=True)
        if self.__snapshot_id is not None:
            with conn.cursor() as cur:
                cur.execute("SET TRANSACTION SNAPSHOT '{}'".format(self.__snapshot_id))
        return conn

    def __get_destination_connection(self):
        conn = self.__destination_dbc.get_db_connection()
        try:
            self.__db_helper.validate_supported_version(conn)
            self.__db_helper.turn_off_constraints(conn)
        except BaseException:
            conn.close()
            raise
        return conn

    def __get_destination_read_connection(self):
        conn = self.__destination_dbc.get_db_connection(read_repeatable=True)
        try:
            self.__db_helper.validate_supported_version(conn)
        except BaseException:
            conn.close()
            raise
        return conn

    def __check_source_writable(self):
        if self.config.db_type == DbType.POSTGRES:
            with self.__source_conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_is_in_recovery(),"
                    " has_database_privilege(current_user, current_database(), 'TEMP')"
                )
                is_replica, has_temp = cur.fetchone()
            if is_replica:
                raise RuntimeError(
                    "use_temp_tables is enabled but the source database is a"
                    " read replica (pg_is_in_recovery() = true)"
                )
            if not has_temp:
                raise RuntimeError(
                    "use_temp_tables is enabled but the source user lacks the"
                    " TEMP privilege on the source database"
                )
        elif self.config.db_type == DbType.MYSQL:
            with self.__source_conn.cursor() as cur:
                cur.execute("SELECT @@global.read_only")
                (read_only,) = cur.fetchone()
            if read_only:
                raise RuntimeError(
                    "use_temp_tables is enabled but the source database is"
                    " read-only (@@global.read_only = 1)"
                )

    def run_middle_out(self):
        passthrough_tables = self.config.passthrough_tables
        relationships = self.__db_helper.get_unredacted_fk_relationships(
            self.__all_tables, self.__source_conn
        )
        disconnected_tables = compute_disconnected_tables(
            self.config.initial_target_tables,
            passthrough_tables,
            self.__all_tables,
            relationships,
        )
        connected_tables = [
            table for table in self.__all_tables if table not in disconnected_tables
        ]
        order = get_topological_order_by_tables(relationships, connected_tables)
        order = list(order)

        # validate pre_filter references
        pf_names = {pf.name for pf in self.config.pre_filters}
        for target in self.config.initial_targets:
            if target.pre_filter and target.pre_filter not in pf_names:
                raise ValueError(
                    "initial target '{}' references pre_filter '{}' which does not exist".format(
                        target.table, target.pre_filter
                    )
                )

        # execute pre_filters once and cache results
        self.__pre_filter_cache = {}
        for pf in self.config.pre_filters:
            with self.__source_conn.cursor() as cur:
                cur.execute(pf.query)
                values = list(set(row[0] for row in cur.fetchall()))
                self.__pre_filter_cache[pf.name] = values
                print(
                    "Pre-filter '{}' cached {} unique values".format(
                        pf.name, len(values)
                    )
                )

        # start by subsetting the direct targets
        print(
            "Beginning direct targets: " + ", ".join(self.config.initial_target_tables)
        )
        start_time = time.time()
        processed_tables = set()
        if (
            self.config.parallel_read_workers > 1
            and self.config.db_type == DbType.POSTGRES
        ):
            for idx, target in enumerate(self.config.initial_targets):
                print_progress(target, idx + 1, len(self.config.initial_targets))
                self.__subset_direct_parallel(target, relationships)
        elif self.__table_workers > 1 and len(self.config.initial_targets) >= 3:
            self.__subset_direct_concurrent(relationships)
        else:
            for idx, target in enumerate(self.config.initial_targets):
                print_progress(target, idx + 1, len(self.config.initial_targets))
                self.__subset_direct(target, relationships)
        for target in self.config.initial_targets:
            processed_tables.add(target.table)
        print("Direct targets completed in {:.1f}s".format(time.time() - start_time))

        # greedily grab rows with foreign keys to rows in the target strata
        upstream_strata = compute_upstream_strata(
            self.config.initial_target_tables, order
        )
        upstream_tables = [t for stratum in upstream_strata for t in stratum]
        print("Beginning upstream subsetting: " + ", ".join(upstream_tables))
        start_time = time.time()
        table_idx = 0
        for stratum in upstream_strata:
            added = self.__process_stratum_upstream(
                stratum,
                processed_tables,
                relationships,
                table_idx,
                len(upstream_tables),
            )
            processed_tables.update(added)
            table_idx += len(stratum)
        print(
            "Upstream subsetting completed in {:.1f}s".format(time.time() - start_time)
        )

        # process pass-through tables concurrently, you need this before subset_downstream,
        # so you can get all required downstream rows
        print("Beginning pass-through tables: " + ", ".join(passthrough_tables))
        start_time = time.time()
        self.__copy_tables_concurrent(passthrough_tables)
        print("Pass-through completed in {:.1f}s".format(time.time() - start_time))

        # use subset_downstream to get all supporting rows according to existing needs
        downstream_strata = compute_downstream_strata(
            passthrough_tables, disconnected_tables, order
        )
        downstream_tables = [t for stratum in downstream_strata for t in stratum]
        print("Beginning downstream subsetting: " + ", ".join(downstream_tables))
        start_time = time.time()
        table_idx = 0
        for stratum in downstream_strata:
            self.__process_stratum_downstream(
                stratum, relationships, table_idx, len(downstream_tables)
            )
            table_idx += len(stratum)
        print(
            "Downstream subsetting completed in {:.1f}s".format(
                time.time() - start_time
            )
        )

        if self.config.keep_disconnected_tables:
            # get all the data for tables in disconnected components (i.e. pass those tables through)
            print("Beginning disconnected tables: " + ", ".join(disconnected_tables))
            start_time = time.time()
            for idx, t in enumerate(disconnected_tables):
                print_progress(t, idx + 1, len(disconnected_tables))
                q = "SELECT {} FROM {}".format(
                    self.__explicit_columns(t, self.__source_conn),
                    fully_qualified_table(t),
                )
                self.__copy_rows(
                    self.__source_conn,
                    self.__destination_conn,
                    q,
                    mysql_db_name_hack(t, self.__destination_conn),
                )
            print(
                "Disconnected tables completed in {:.1f}s".format(
                    time.time() - start_time
                )
            )

    def prep_temp_dbs(self):
        self.__db_helper.prep_temp_dbs(self.__source_conn, self.__destination_conn)
        if self.__incremental:
            relationships = self.__db_helper.get_unredacted_fk_relationships(
                self.__all_tables, self.__source_conn
            )
            disconnected = compute_disconnected_tables(
                self.config.initial_target_tables,
                self.config.passthrough_tables,
                self.__all_tables,
                relationships,
            )
            incremental_tables = self.__all_tables
            if not self.config.keep_disconnected_tables:
                incremental_tables = [
                    table for table in self.__all_tables if table not in disconnected
                ]
            self.__db_helper.prep_incremental(
                self.__source_conn, self.__destination_conn, incremental_tables
            )
            self.__incremental_prepared = True
            # constraints are live on an existing destination, but middle-out
            # load order inserts referencing rows before referenced ones
            self.__dropped_fks = self.__db_helper.drop_fk_constraints(
                self.__destination_conn
            )

    def unprep_temp_dbs(self, succeeded=True):
        self.__db_helper.unprep_temp_dbs(self.__source_conn, self.__destination_conn)
        if self.__incremental_prepared:
            try:
                self.__destination_conn.connection.rollback()
                self.__db_helper.restore_fk_constraints(
                    self.__destination_conn, self.__dropped_fks
                )
                if succeeded:
                    self.__db_helper.unprep_incremental(self.__destination_conn)
                else:
                    self.__db_helper.retain_incremental(self.__destination_conn)
            except BaseException:
                self.__destination_conn.connection.rollback()
                self.__db_helper.retain_incremental(self.__destination_conn)
                raise
            finally:
                self.__incremental_prepared = False

    def close_connections(self):
        self.__source_conn.close()
        self.__destination_conn.close()
        for conn in self.__source_pool:
            conn.close()

    def __process_stratum_upstream(
        self, stratum, processed_tables, relationships, start_idx, total_count
    ):
        added = set()
        if len(stratum) <= 1 or self.__table_workers == 1:
            for t in stratum:
                print_progress(t, start_idx + 1, total_count)
                data_added = self.__subset_upstream(
                    t,
                    processed_tables,
                    relationships,
                    self.__source_conn,
                    self.__destination_conn,
                    # a lone table in its stratum serializes the run, so let
                    # it fan its ID batches out across the source pool
                    allow_chunk=True,
                )
                if data_added:
                    added.add(t)
            return added

        # genuinely large tables (~100MB+) get the whole source pool to
        # themselves, one at a time, chunked internally; everything else
        # shares the table-level thread pool as before
        small_tables = list(stratum)
        big_tables = []
        if self.__source_pool:
            threshold = 12_800  # heap pages, ~100MB
            for t in list(small_tables):
                pages = self.__db_helper.get_table_page_count(
                    table_name(t), schema_name(t), self.__source_conn
                )
                if pages >= threshold:
                    small_tables.remove(t)
                    big_tables.append(t)

        def upstream_worker(table):
            source_conn = self.__get_source_connection()
            dest_conn = self.__get_destination_connection()
            try:
                return self.__subset_upstream(
                    table, processed_tables, relationships, source_conn, dest_conn
                )
            finally:
                source_conn.close()
                dest_conn.close()

        with ThreadPoolExecutor(max_workers=self.__table_workers) as pool:
            futures = {}
            for idx, t in enumerate(small_tables):
                print_progress(t, start_idx + idx + 1, total_count)
                futures[pool.submit(upstream_worker, t)] = t
            for future in as_completed(futures):
                t = futures[future]
                if future.result():
                    added.add(t)

        for j, t in enumerate(big_tables):
            print_progress(t, start_idx + len(small_tables) + j + 1, total_count)
            if self.__subset_upstream(
                t,
                processed_tables,
                relationships,
                self.__source_conn,
                self.__destination_conn,
                allow_chunk=True,
            ):
                added.add(t)
        return added

    def __process_stratum_downstream(
        self, stratum, relationships, start_idx, total_count
    ):
        if len(stratum) <= 1 or self.__table_workers == 1:
            for t in stratum:
                print_progress(t, start_idx + 1, total_count)
                self.subset_downstream(
                    t,
                    relationships,
                    self.__source_conn,
                    self.__destination_conn,
                    allow_chunk=True,
                )
            return

        def downstream_worker(table):
            source_conn = self.__get_source_connection()
            dest_conn = self.__get_destination_connection()
            try:
                self.subset_downstream(table, relationships, source_conn, dest_conn)
            finally:
                source_conn.close()
                dest_conn.close()

        with ThreadPoolExecutor(max_workers=self.__table_workers) as pool:
            futures = {}
            for idx, t in enumerate(stratum):
                print_progress(t, start_idx + idx + 1, total_count)
                futures[pool.submit(downstream_worker, t)] = t
            for future in as_completed(futures):
                future.result()

    def __copy_table(self, table, source_conn, dest_conn):
        q = "SELECT {} FROM {}".format(
            self.__explicit_columns(table, source_conn),
            fully_qualified_table(table),
        )
        if self.config.max_rows_per_table is not None:
            q += " LIMIT {}".format(self.config.max_rows_per_table)
        self.__copy_rows(
            source_conn,
            dest_conn,
            q,
            mysql_db_name_hack(table, dest_conn),
        )

    def __explicit_columns(self, table, source_conn):
        qualified = fully_qualified_table(table)
        columns = self.__db_helper.get_table_columns(
            table_name(table), schema_name(table), source_conn
        )
        return ",".join("{}.{}".format(qualified, quoter(column)) for column in columns)

    def __copy_table_worker(self, table):
        source_conn = self.__get_source_connection()
        dest_conn = self.__get_destination_connection()
        try:
            self.__copy_table(table, source_conn, dest_conn)
        finally:
            source_conn.close()
            dest_conn.close()

    def __copy_tables_concurrent(self, tables):
        if self.__table_workers == 1:
            for idx, table in enumerate(tables):
                print_progress(table, idx + 1, len(tables))
                self.__copy_table(table, self.__source_conn, self.__destination_conn)
            return

        if self.__source_pool and self.config.max_rows_per_table is None:
            # split each table across the pool by ctid page ranges; tables too
            # small to split fall back to a plain single-connection copy
            for idx, t in enumerate(tables):
                print_progress(t, idx + 1, len(tables))
                if not self.__copy_table_ctid_parallel(t):
                    self.__copy_table_worker(t)
            return

        with ThreadPoolExecutor(max_workers=self.__table_workers) as pool:
            futures = {pool.submit(self.__copy_table_worker, t): t for t in tables}
            for idx, future in enumerate(as_completed(futures)):
                table = futures[future]
                print_progress(table, idx + 1, len(tables))
                future.result()  # raises if the worker failed

    def __subset_direct_concurrent(self, relationships):
        targets = self.config.initial_targets

        def direct_worker(target):
            source_conn = self.__get_source_connection()
            dest_conn = self.__get_destination_connection()
            try:
                self.__subset_direct(target, relationships, source_conn, dest_conn)
            finally:
                source_conn.close()
                dest_conn.close()

        with ThreadPoolExecutor(max_workers=self.__table_workers) as pool:
            futures = {pool.submit(direct_worker, t): t for t in targets}
            for idx, future in enumerate(as_completed(futures)):
                target = futures[future]
                print_progress(target, idx + 1, len(targets))
                future.result()

    def __get_pre_filter_info(self, target: InitialTarget):
        """Return (column, values) for a target's pre_filter, or None."""
        if target.pre_filter is None:
            return None
        pf = next(
            (p for p in self.config.pre_filters if p.name == target.pre_filter), None
        )
        if pf is None:
            return None
        values = self.__pre_filter_cache.get(pf.name)
        if not values:
            return None
        return (pf.column, values)

    def __copy_table_ctid_parallel(
        self, table, columns_query="*", extra_conditions=None, params=None
    ):
        """Copy a table split across the source pool by ctid page ranges.

        Returns False when not applicable (no pool, or table too small to be
        worth splitting), leaving the caller to fall back.
        """
        if not self.__source_pool:
            return False
        # incremental upserts rely on refreshes landing before new-row
        # inserts (see _conflict_clause), and page-range workers give no
        # cross-worker ordering. Tables with uniqueness beyond their identity
        # split in two phases instead: every worker stages its rows and
        # applies its refreshes, all workers meet at a barrier, then the
        # inserts run — same guarantee as a sequential copy, full worker
        # count. Needs the staging machinery and an identity-tracked delta;
        # otherwise fall back to the sequential path. Identity-only tables
        # keep the single-pass split: ON CONFLICT arbitrates collisions
        # regardless of order.
        two_phase = False
        if self.__incremental and self.__db_helper.has_secondary_unique(table):
            two_phase = (
                self.config.use_copy_protocol
                and self.__db_helper.delta_for(table) is not None
            )
            if not two_phase:
                return False
        num_workers = len(self.__source_pool)
        page_count = self.__db_helper.get_table_page_count(
            table_name(table), schema_name(table), self.__source_conn
        )
        if page_count < num_workers * 10:
            return False

        fqt = fully_qualified_table(table)
        pages_per_worker = page_count // num_workers
        barrier = threading.Barrier(num_workers) if two_phase else None

        def worker(idx, start_page, end_page):
            source_conn = self.__source_pool[idx]
            dest_conn = self.__get_destination_connection()
            try:
                # relpages can undercount (only refreshed by VACUUM/ANALYZE),
                # so the last worker scans to the actual heap end.
                ctid_filter = "{}.ctid >= '({},0)'::tid".format(fqt, start_page)
                if end_page is not None:
                    ctid_filter += " AND {}.ctid < '({},0)'::tid".format(fqt, end_page)
                conditions = [ctid_filter] + list(extra_conditions or [])
                q = "SELECT {} FROM {} WHERE {}".format(
                    columns_query, fqt, " AND ".join(conditions)
                )
                if two_phase:
                    self.__db_helper.stage_rows(
                        source_conn, dest_conn, q, table, params
                    )
                    self.__db_helper.apply_staged(dest_conn, table, "refresh")
                    barrier.wait()
                    self.__db_helper.apply_staged(dest_conn, table, "insert")
                else:
                    self.__copy_rows(source_conn, dest_conn, q, table, params)
            except BaseException:
                # a worker failing before the barrier would strand the rest
                # at wait(); break the barrier so they fail fast too
                if barrier is not None:
                    barrier.abort()
                raise
            finally:
                dest_conn.close()

        with ThreadPoolExecutor(max_workers=num_workers) as pool:
            futures = []
            for idx in range(num_workers):
                start_page = idx * pages_per_worker
                end_page = (
                    None if idx == num_workers - 1 else (idx + 1) * pages_per_worker
                )
                futures.append(pool.submit(worker, idx, start_page, end_page))
            for future in as_completed(futures):
                future.result()
        return True

    def __subset_direct_parallel(self, target: InitialTarget, relationships):
        """Subset a direct target using parallel ctid page-range splitting."""
        t = target.table
        columns_query = columns_to_copy(t, relationships, self.__source_conn)
        fqt = fully_qualified_table(t)

        conditions = []
        if target.where is not None:
            conditions.append("({})".format(target.where))
        elif target.percent is not None:
            conditions.append("random() < {}".format(float(target.percent) / 100))
        pre_filter_info = self.__get_pre_filter_info(target)
        params = None
        if pre_filter_info:
            column, values = pre_filter_info
            condition, params = self.__db_helper.build_membership_filter(
                "{}.{}".format(fqt, quoter(column)), values
            )
            conditions.append(condition)

        if not self.__copy_table_ctid_parallel(t, columns_query, conditions, params):
            self.__subset_direct(target, relationships)

    def __parallel_id_batches(
        self, dest_cursor, batch_size, copy_batch_fn, initial_rows=None
    ):
        """Fan ID batches from a destination cursor out across the source pool.

        copy_batch_fn(valid_rows, source_conn, dest_conn) runs one batch; the
        cursor is only read from this thread, so batches stay disjoint.
        initial_rows carries a batch the caller already fetched (and filtered).
        """
        dest_conns = [self.__get_destination_connection() for _ in self.__source_pool]
        pending = initial_rows
        try:
            with ThreadPoolExecutor(max_workers=len(self.__source_pool)) as pool:
                exhausted = False
                while not exhausted:
                    futures = []
                    for src_conn, dst_conn in zip(self.__source_pool, dest_conns):
                        if pending is not None:
                            valid_rows = pending
                            pending = None
                        else:
                            rows = dest_cursor.fetchmany(batch_size)
                            if not rows:
                                exhausted = True
                                break
                            valid_rows = [
                                row for row in rows if all(c is not None for c in row)
                            ]
                        if valid_rows:
                            futures.append(
                                pool.submit(
                                    copy_batch_fn, valid_rows, src_conn, dst_conn
                                )
                            )
                    for future in as_completed(futures):
                        future.result()
        finally:
            for conn in dest_conns:
                conn.close()

    def __stream_ids_to_source_temp(
        self, dest_query, columns, source_conn=None, dest_conn=None
    ):
        source_conn = source_conn or self.__source_conn
        dest_conn = dest_conn or self.__destination_conn
        id_temp = self.__db_helper.create_id_temp_table(source_conn, len(columns))
        insert_q = "INSERT INTO {} VALUES ({})".format(
            fully_qualified_table(id_temp), ",".join(["%s"] * len(columns))
        )
        cursor_name = "table_cursor_" + str(uuid.uuid4()).replace("-", "")
        dest_cursor = dest_conn.cursor(name=cursor_name, withhold=True)
        src_insert_cur = source_conn.cursor()
        try:
            dest_cursor.execute(dest_query)
            batch_size = self.__db_helper.get_batch_size(len(columns))
            while True:
                rows = dest_cursor.fetchmany(batch_size)
                if not rows:
                    break
                valid_rows = [row for row in rows if all(c is not None for c in row)]
                if valid_rows:
                    src_insert_cur.executemany(insert_q, valid_rows)
            # no source commit: temp table contents are session-visible, and
            # committing would end the transaction that keeps this
            # connection's (or the run's exported) snapshot alive
        finally:
            src_insert_cur.close()
            dest_cursor.close()
        return id_temp

    def __build_temp_table_join(
        self,
        source_table,
        id_temp,
        join_columns,
        datatypes,
        select_expr=None,
    ):
        """Build a SELECT ... JOIN query against a source temp table.

        join_columns are the columns on source_table to match against the temp table.
        datatypes maps temp table column names to their real types for casting.
        """
        fqt = fully_qualified_table(source_table)
        if select_expr is None:
            select_expr = "{}.*".format(fqt)
        join_conditions = " AND ".join(
            "{}.{} = {}".format(
                fqt,
                quoter(col),
                self.__db_helper.temp_table_column(id_temp, i, datatypes[col]),
            )
            for i, col in enumerate(join_columns)
        )
        return "SELECT {} FROM {} JOIN {} ON {}".format(
            select_expr,
            fqt,
            fully_qualified_table(id_temp),
            join_conditions,
        )

    def __subset_direct(
        self, target: InitialTarget, relationships, source_conn=None, dest_conn=None
    ):
        source_conn = source_conn or self.__source_conn
        dest_conn = dest_conn or self.__destination_conn
        t = target.table
        columns_query = columns_to_copy(t, relationships, source_conn)
        if target.where is not None:
            q = "SELECT {} FROM {} WHERE ({})".format(
                columns_query, fully_qualified_table(t), target.where
            )
        elif target.percent is not None:
            if self.config.db_type == DbType.POSTGRES:
                q = "SELECT {} FROM {} WHERE random() < {}".format(
                    columns_query,
                    fully_qualified_table(t),
                    float(target.percent) / 100,
                )
            else:
                q = "SELECT {} FROM {} WHERE rand() < {}".format(
                    columns_query,
                    fully_qualified_table(t),
                    float(target.percent) / 100,
                )
        else:
            raise ValueError(
                "target table {} had no 'where' or 'percent' term defined, check your configuration.".format(
                    t
                )
            )
        pre_filter_info = self.__get_pre_filter_info(target)
        if pre_filter_info:
            column, values = pre_filter_info
            column_sql = "{}.{}".format(fully_qualified_table(t), quoter(column))
            for value_batch in self.__db_helper.iter_membership_batches(values):
                condition, params = self.__db_helper.build_membership_filter(
                    column_sql, value_batch
                )
                self.__copy_rows(
                    source_conn,
                    dest_conn,
                    q + " AND " + condition,
                    mysql_db_name_hack(t, dest_conn),
                    params,
                )
            return
        self.__copy_rows(
            source_conn,
            dest_conn,
            q,
            mysql_db_name_hack(t, dest_conn),
        )

    def __upstream_delta_plan(self, relevant_key_constraints, dest_conn):
        """Decide the upstream ID sources for an incremental (top-up) run.

        Returns (skip, delta_plan):
        - skip=True: no parent gained rows this run, no new child rows possible
        - delta_plan: {parent_table: (delta_table, pk_cols)} for parents with
          rows inserted this run (upserted rows don't count: their children
          were already considered when they first arrived). None means full
          (non-incremental) behavior because this run doesn't delta-restrict
          upstream reads (recreate, or grow which scans all resident parents).
        """
        if not self.__upstream_delta_reads:
            return False, None
        parents = {kc["target_table"] for kc in relevant_key_constraints}
        deltas = {p: self.__db_helper.delta_for(p) for p in parents}
        if not all(deltas.values()):
            return False, None
        nonempty = {}
        with dest_conn.cursor() as cur:
            for p, (delta_table, _) in deltas.items():
                cur.execute(
                    "SELECT EXISTS (SELECT 1 FROM {} WHERE _inserted)".format(
                        delta_table
                    )
                )
                nonempty[p] = cur.fetchone()[0]
        if not any(nonempty.values()):
            return True, None
        return False, {p: deltas[p] for p in parents if nonempty[p]}

    def __upstream_ids_query(self, kc_target, target_cols, dest_conn, delta_plan):
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

    def __downstream_delta_plan(self, referencing_tables, dest_conn):
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
        if not self.__incremental:
            return False, None
        children = {r["fk_table"] for r in referencing_tables}
        plan = {}
        with dest_conn.cursor() as cur:
            for child in children:
                delta = self.__db_helper.delta_for(child)
                if delta is None:
                    plan[child] = None
                    continue
                cur.execute("SELECT EXISTS (SELECT 1 FROM {})".format(delta[0]))
                if cur.fetchone()[0]:
                    plan[child] = delta
        if not plan:
            return True, None
        return False, plan

    def __subset_upstream(
        self,
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
                lambda r: (
                    r["target_table"] in processed_tables and r["fk_table"] == target
                ),
                redacted_relationships,
            )
        )
        if len(relevant_key_constraints) == 0 or target in processed_tables:
            return False

        skip, delta_plan = self.__upstream_delta_plan(
            relevant_key_constraints, dest_conn
        )
        if skip:
            return True

        table_columns = self.__db_helper.get_table_columns(
            table_name(target), schema_name(target), source_conn
        )
        upstream_filters = upstream_filter_match(target, table_columns)
        columns_query = columns_to_copy(target, relationships, source_conn)

        if self.config.use_temp_tables:
            self.__subset_upstream_temp_tables(
                target,
                relevant_key_constraints,
                upstream_filters,
                source_conn,
                dest_conn,
                delta_plan,
                columns_query,
            )
        else:
            self.__subset_upstream_batched(
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

    def __subset_upstream_temp_tables(
        self,
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
            for col, typ, _, _ in self.__db_helper.get_table_datatypes(
                table_name(target), schema_name(target), source_conn
            )
        }
        groups = {}
        for kc in relevant_key_constraints:
            key = (kc["target_table"], tuple(kc["target_columns"]))
            groups.setdefault(key, []).append(kc)

        constraint_temps = {}
        constraint_delta_temps = {}
        for (kc_target, target_cols), group_constraints in groups.items():
            dest_query = self.__upstream_ids_query(
                kc_target, list(target_cols), dest_conn, None
            )
            temp_count = (
                len(group_constraints)
                if self.__db_helper.requires_distinct_id_temp_tables()
                else 1
            )
            full_temps = [
                self.__stream_ids_to_source_temp(
                    dest_query, target_cols, source_conn, dest_conn
                )
                for _ in range(temp_count)
            ]
            for index, kc in enumerate(group_constraints):
                constraint_temps[id(kc)] = full_temps[index if temp_count > 1 else 0]
            if delta_plan and kc_target in delta_plan:
                delta_query = self.__upstream_ids_query(
                    kc_target, list(target_cols), dest_conn, delta_plan
                )
                delta_temps = [
                    self.__stream_ids_to_source_temp(
                        delta_query, target_cols, source_conn, dest_conn
                    )
                    for _ in range(temp_count)
                ]
                for index, kc in enumerate(group_constraints):
                    constraint_delta_temps[id(kc)] = delta_temps[
                        index if temp_count > 1 else 0
                    ]

        kcs = relevant_key_constraints
        if delta_plan is None:
            passes = [None]
        else:
            # one pass per constraint whose parent gained rows this run: that
            # constraint joins the delta, the others join the full ID sets
            # (AND semantics must hold against everything already imported)
            passes = [j for j, kc in enumerate(kcs) if id(kc) in constraint_delta_temps]
            if not passes:
                return

        fqt = fully_qualified_table(target)
        for pass_j in passes:
            joins = ""
            match_conditions = []
            nullable_conditions = []
            for idx, kc in enumerate(kcs):
                if pass_j is not None and pass_j == idx:
                    id_temp = constraint_delta_temps[id(kc)]
                else:
                    id_temp = constraint_temps[id(kc)]
                fk_cols = kc["fk_columns"]
                alias = "_ids{}".format(idx)
                join_conditions = " AND ".join(
                    "{}.{} = {}".format(
                        fqt,
                        quoter(col),
                        self.__db_helper.temp_table_column(alias, i, fk_datatypes[col]),
                    )
                    for i, col in enumerate(fk_cols)
                )
                joins += " LEFT JOIN {} AS {} ON {}".format(
                    fully_qualified_table(id_temp), alias, join_conditions
                )
                match_conditions.append("{}.col0 IS NOT NULL".format(alias))
                nullable_conditions.append(
                    " OR ".join(
                        "{}.{} IS NULL".format(fqt, quoter(col)) for col in fk_cols
                    )
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
            conditions.extend(
                "({})".format(condition) for condition in upstream_filters
            )
            q += " WHERE {}".format(" AND ".join(conditions))
            if self.config.max_rows_per_table is not None:
                q += " LIMIT {}".format(self.config.max_rows_per_table)
            self.__copy_rows(
                source_conn,
                dest_conn,
                q,
                mysql_db_name_hack(target, dest_conn),
                batch_size=self.__db_helper.get_batch_size(len(fk_datatypes)),
            )

    def __build_upstream_id_query(
        self,
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
            alias = "ids{}".format(join_idx)
            id_table, params = self.__db_helper.build_id_table(
                rows, fk_cols, fk_datatypes, alias
            )
            join_conditions = " AND ".join(
                "{}.{} = {}.col{}".format(fqt, quoter(col), alias, i)
                for i, col in enumerate(fk_cols)
            )
            joins += " LEFT JOIN {} ON {}".format(id_table, join_conditions)
            all_params.extend(params)
            match_conditions.append("{}.col0 IS NOT NULL".format(alias))
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

    def __subset_upstream_batched(
        self,
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
            for col, typ, _, _ in self.__db_helper.get_table_datatypes(
                table_name(target), schema_name(target), source_conn
            )
        }

        groups = {}
        for kc in relevant_key_constraints:
            key = (kc["target_table"], tuple(kc["target_columns"]))
            groups.setdefault(key, []).append(kc)

        fqt = fully_qualified_table(target)
        batch_size = self.__db_helper.get_batch_size(len(fk_datatypes))

        if self.config.db_type == DbType.MYSQL and (
            len(relevant_key_constraints) > 1
            or self.config.max_rows_per_table is not None
        ):
            self.__subset_upstream_mysql_multi_fk(
                target,
                fqt,
                relevant_key_constraints,
                fk_datatypes,
                upstream_filters,
                batch_size,
                source_conn,
                dest_conn,
                delta_plan,
                columns_query,
            )
            return

        # a single ID stream can only serve a single constraint: binding the
        # same batch to two constraints (e.g. from/to FKs to one parent) drops
        # AND-pairs whose IDs span two batches. Multi-constraint tables take
        # the multi-group path, which joins each batched constraint against
        # the other constraints' full ID sets.
        streaming_ok = (
            len(relevant_key_constraints) == 1
            and self.config.max_rows_per_table is None
        )

        if streaming_ok:
            self.__upstream_batched_streamed(
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

        self.__upstream_batched_multi_group(
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

    def __destination_membership(self, constraint, candidate_rows, dest_conn):
        if not candidate_rows:
            return set()
        alias = "_candidate_ids"
        id_table, params = self.__db_helper.build_id_table(
            candidate_rows,
            constraint["target_columns"],
            {},
            alias,
        )
        target = fully_qualified_table(
            mysql_db_name_hack(constraint["target_table"], dest_conn)
        )
        matches = " AND ".join(
            "_target.{} = {}.col{}".format(quoter(column), alias, index)
            for index, column in enumerate(constraint["target_columns"])
        )
        selected = ",".join(
            "{}.col{}".format(alias, index)
            for index in range(len(constraint["target_columns"]))
        )
        query = "SELECT DISTINCT {} FROM {} JOIN {} _target ON {}".format(
            selected,
            id_table,
            target,
            matches,
        )
        with dest_conn.cursor() as cursor:
            cursor.execute(query, params)
            return set(cursor.fetchall())

    def __subset_upstream_mysql_multi_fk(
        self,
        target,
        fqt,
        constraints,
        fk_datatypes,
        upstream_filters,
        batch_size,
        source_conn,
        dest_conn,
        delta_plan,
        columns_query,
    ):
        """Copy a multi-FK child without retaining complete parent ID sets.

        Each constraint drives one bounded source query at a time. Candidate
        child rows are checked against the other destination parent sets using
        only the FK values in that source batch. This preserves AND and nullable
        FK semantics without source-side writes or unbounded generated SQL.
        """
        column_positions = {column: index for index, column in enumerate(fk_datatypes)}
        remaining = self.config.max_rows_per_table
        drivers = range(len(constraints))
        if delta_plan is not None:
            drivers = [
                index
                for index, constraint in enumerate(constraints)
                if constraint["target_table"] in delta_plan
            ]

        id_reader = self.__get_destination_read_connection()
        try:
            for driver_index in drivers:
                if remaining is not None and remaining <= 0:
                    break
                driver = constraints[driver_index]
                ids_query = self.__upstream_ids_query(
                    driver["target_table"],
                    driver["target_columns"],
                    id_reader,
                    delta_plan,
                )
                id_cursor = id_reader.cursor()
                try:
                    id_cursor.execute(ids_query)
                    while True:
                        if remaining is not None and remaining <= 0:
                            break
                        id_rows = id_cursor.fetchmany(batch_size)
                        if not id_rows:
                            break
                        id_rows = [
                            row
                            for row in id_rows
                            if all(value is not None for value in row)
                        ]
                        if not id_rows:
                            continue
                        query, params = self.__build_upstream_id_query(
                            fqt,
                            [(driver, id_rows)],
                            fk_datatypes,
                            upstream_filters,
                            required_join=0,
                            columns_query=columns_query,
                        )

                        def filter_rows(rows):
                            nonlocal remaining
                            valid = list(rows)
                            for index, constraint in enumerate(constraints):
                                if index == driver_index:
                                    continue
                                positions = [
                                    column_positions[column]
                                    for column in constraint["fk_columns"]
                                ]
                                candidates = list(
                                    dict.fromkeys(
                                        tuple(row[position] for position in positions)
                                        for row in valid
                                        if all(
                                            row[position] is not None
                                            for position in positions
                                        )
                                    )
                                )
                                matched = self.__destination_membership(
                                    constraint, candidates, dest_conn
                                )
                                valid = [
                                    row
                                    for row in valid
                                    if any(
                                        row[position] is None for position in positions
                                    )
                                    or tuple(row[position] for position in positions)
                                    in matched
                                ]
                            if remaining is not None:
                                valid = valid[:remaining]
                                remaining -= len(valid)
                            return valid

                        self.__db_helper.copy_rows(
                            source_conn,
                            dest_conn,
                            query,
                            mysql_db_name_hack(target, dest_conn),
                            params,
                            batch_size=self.__db_helper.get_batch_size(
                                len(fk_datatypes)
                            ),
                            row_filter=filter_rows,
                        )
                finally:
                    if id_reader.connection.unread_result:
                        id_reader.connection.consume_results()
                    id_cursor.close()
        finally:
            id_reader.close()

    def __fetch_dest_rows(self, query, batch_size, dest_conn):
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

    def __upstream_batched_streamed(
        self,
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

        id_reader = dest_conn
        owns_id_reader = False
        if self.config.db_type == DbType.MYSQL:
            id_reader = self.__get_destination_read_connection()
            owns_id_reader = True
        query = self.__upstream_ids_query(
            kc_target, list(target_cols), id_reader, delta_plan
        )

        def copy_batch(valid_rows, batch_source_conn, batch_dest_conn):
            q, params = self.__build_upstream_id_query(
                fqt,
                [(kc, valid_rows) for kc in groups[group_key]],
                fk_datatypes,
                upstream_filters,
                required_join=0,
                columns_query=columns_query,
            )
            self.__copy_rows(
                batch_source_conn,
                batch_dest_conn,
                q,
                mysql_db_name_hack(target, batch_dest_conn),
                params,
                batch_size=self.__db_helper.get_batch_size(len(fk_datatypes)),
            )

        cursor_name = "table_cursor_" + str(uuid.uuid4()).replace("-", "")
        dest_cursor = id_reader.cursor(name=cursor_name, withhold=True)
        try:
            dest_cursor.execute(query)
            if allow_chunk and self.__source_pool:
                first = dest_cursor.fetchmany(batch_size)
                if not first:
                    return
                valid_first = [row for row in first if all(c is not None for c in row)]
                kcs_in_group = groups[group_key]
                fk_cols = kcs_in_group[0]["fk_columns"]
                if (
                    len(first) < batch_size
                    and len(kcs_in_group) == 1
                    and len(fk_cols) == 1
                ):
                    # the whole ID set fits one batch, so there are no
                    # batches to fan out: split the child table read by
                    # ctid ranges instead, with the IDs as a filter
                    if not valid_first:
                        return
                    col = fk_cols[0]
                    conditions = [
                        "{}.{} = ANY(%s::{}[])".format(
                            fqt, quoter(col), fk_datatypes[col]
                        )
                    ] + list(upstream_filters)
                    ids = [row[0] for row in valid_first]
                    if self.__copy_table_ctid_parallel(
                        target, columns_query, conditions, [ids]
                    ):
                        return
                    copy_batch(valid_first, source_conn, dest_conn)
                    return
                self.__parallel_id_batches(
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
            if owns_id_reader:
                id_reader.close()
            else:
                dest_cursor.close()

    def __upstream_batched_multi_group(
        self,
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
                    delta_rows[(kc_target, target_cols)] = self.__fetch_dest_rows(
                        self.__upstream_ids_query(
                            kc_target, list(target_cols), dest_conn, delta_plan
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
                q = self.__upstream_ids_query(
                    kc_target, list(target_cols), dest_conn, None
                )
                cur.execute("SELECT COUNT(*) FROM ({}) _ids".format(q))
                full_counts[(kc_target, target_cols)] = cur.fetchone()[0]

        full_rows = {}  # loaded lazily, only for groups that must stay resident

        def resident_rows(group_key):
            if group_key not in full_rows:
                full_rows[group_key] = self.__fetch_dest_rows(
                    self.__upstream_ids_query(
                        group_key[0], list(group_key[1]), dest_conn, None
                    ),
                    batch_size,
                    dest_conn,
                )
            return full_rows[group_key]

        copy_batch = self.__db_helper.get_batch_size(len(fk_datatypes))

        def copy_kc_rows(kc_rows, single_shot, required_join):
            q, params = self.__build_upstream_id_query(
                fqt,
                kc_rows,
                fk_datatypes,
                upstream_filters,
                required_join=required_join,
                columns_query=columns_query,
            )
            if single_shot and self.config.max_rows_per_table is not None:
                q += " LIMIT {}".format(self.config.max_rows_per_table)
            self.__copy_rows(
                source_conn,
                dest_conn,
                q,
                mysql_db_name_hack(target, dest_conn),
                params,
                batch_size=copy_batch,
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
                "{}.{} IS NULL".format(fqt, quoter(col))
                for col in batched_kc["fk_columns"]
            )
            q, params = self.__build_upstream_id_query(
                fqt,
                remaining,
                fk_datatypes,
                list(upstream_filters) + [nullable],
                required_join=remapped_required,
                columns_query=columns_query,
            )
            self.__copy_rows(
                source_conn,
                dest_conn,
                q,
                mysql_db_name_hack(target, dest_conn),
                params,
                batch_size=copy_batch,
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
            stream_idx = next(
                j for j, kc in enumerate(kcs) if group_of(kc) == stream_gk
            )
            cursor_name = "table_cursor_" + str(uuid.uuid4()).replace("-", "")
            dest_cursor = dest_conn.cursor(name=cursor_name, withhold=True)
            try:
                dest_cursor.execute(
                    self.__upstream_ids_query(
                        stream_gk[0], list(stream_gk[1]), dest_conn, None
                    )
                )
                first = True
                while True:
                    batch = dest_cursor.fetchmany(batch_size)
                    if not batch:
                        break
                    valid_rows = [
                        row for row in batch if all(c is not None for c in row)
                    ]
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
                (kc, [] if j == stream_idx else rows_for(j, kc))
                for j, kc in enumerate(kcs)
            ]
            copy_neutral_rows(kc_rows, stream_idx, pass_j)

    def subset_downstream(
        self,
        table,
        relationships,
        source_conn=None,
        dest_conn=None,
        allow_chunk=False,
    ):
        source_conn = source_conn or self.__source_conn
        dest_conn = dest_conn or self.__destination_conn
        referencing_tables = [
            r for r in redact_relationships(relationships) if r["target_table"] == table
        ]

        if len(referencing_tables) > 0:
            pk_columns = referencing_tables[0]["target_columns"]
        else:
            return

        skip, child_plan = self.__downstream_delta_plan(referencing_tables, dest_conn)
        if skip:
            return

        if self.config.db_type == DbType.MYSQL:
            temp_table = self.__db_helper.create_id_temp_table(
                dest_conn,
                len(pk_columns),
                mysql_db_name_hack(table, dest_conn),
                pk_columns,
            )
        else:
            temp_table = self.__db_helper.create_id_temp_table(
                dest_conn, len(pk_columns)
            )

        for r in referencing_tables:
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
            id_columns = ["col{}".format(index) for index in range(len(pk_columns))]
            insert_q = "INSERT INTO {} ({}) {}".format(
                fully_qualified_table(temp_table),
                columns_joined(id_columns),
                select_q,
            )
            with dest_conn.cursor() as cur:
                cur.execute(insert_q)
            dest_conn.commit()

        columns_query = columns_to_copy(table, relationships, source_conn)

        if self.config.use_temp_tables:
            self.__subset_downstream_temp_tables(
                table, temp_table, pk_columns, columns_query, source_conn, dest_conn
            )
        else:
            self.__subset_downstream_batched(
                table,
                temp_table,
                pk_columns,
                columns_query,
                source_conn,
                dest_conn,
                allow_chunk,
            )

    def __subset_downstream_temp_tables(
        self, table, dest_temp_table, pk_columns, columns_query, source_conn, dest_conn
    ):
        downstream_datatypes = {
            col: typ
            for col, typ, _, _ in self.__db_helper.get_table_datatypes(
                table_name(table), schema_name(table), source_conn
            )
        }
        dest_query = "SELECT DISTINCT * FROM {}".format(
            fully_qualified_table(dest_temp_table)
        )
        src_id_temp = self.__stream_ids_to_source_temp(
            dest_query, pk_columns, source_conn, dest_conn
        )
        q = self.__build_temp_table_join(
            table, src_id_temp, pk_columns, downstream_datatypes, columns_query
        )
        self.__copy_rows(
            source_conn,
            dest_conn,
            q,
            mysql_db_name_hack(table, dest_conn),
            batch_size=self.__db_helper.get_batch_size(len(downstream_datatypes)),
        )

    def __subset_downstream_batched(
        self,
        table,
        dest_temp_table,
        pk_columns,
        columns_query,
        source_conn,
        dest_conn,
        allow_chunk=False,
    ):
        downstream_datatypes = {
            col: typ
            for col, typ, _, _ in self.__db_helper.get_table_datatypes(
                table_name(table), schema_name(table), source_conn
            )
        }

        def copy_batch(valid_rows, batch_source_conn, batch_dest_conn):
            id_table, params = self.__db_helper.build_id_table(
                valid_rows, pk_columns, downstream_datatypes, "ids"
            )
            join_conditions = " AND ".join(
                "{}.{} = ids.col{}".format(fully_qualified_table(table), quoter(col), i)
                for i, col in enumerate(pk_columns)
            )
            q = ("SELECT {cols} FROM {tbl} JOIN {id_table} ON {conditions}").format(
                cols=columns_query,
                tbl=fully_qualified_table(table),
                id_table=id_table,
                conditions=join_conditions,
            )
            self.__copy_rows(
                batch_source_conn,
                batch_dest_conn,
                q,
                mysql_db_name_hack(table, batch_dest_conn),
                params,
                batch_size=self.__db_helper.get_batch_size(len(downstream_datatypes)),
            )

        if self.config.db_type == DbType.MYSQL:
            id_columns = ["col{}".format(index) for index in range(len(pk_columns))]
            selected = columns_joined(id_columns)
            nonnull = " AND ".join(
                "{} IS NOT NULL".format(quoter(column)) for column in id_columns
            )
            last_row = None
            batch_size = self.__db_helper.get_batch_size(len(pk_columns))
            while True:
                conditions = [nonnull]
                params = []
                if last_row is not None:
                    if len(id_columns) == 1:
                        conditions.append("{} > %s".format(quoter(id_columns[0])))
                    else:
                        conditions.append(
                            "({}) > ({})".format(
                                selected,
                                ",".join(["%s"] * len(id_columns)),
                            )
                        )
                    params.extend(last_row)
                cursor_query = (
                    "SELECT DISTINCT {} FROM {} WHERE {} ORDER BY {} LIMIT {}"
                ).format(
                    selected,
                    fully_qualified_table(dest_temp_table),
                    " AND ".join(conditions),
                    selected,
                    batch_size,
                )
                with dest_conn.cursor() as cursor:
                    cursor.execute(cursor_query, params)
                    rows = cursor.fetchall()
                if not rows:
                    break
                copy_batch(rows, source_conn, dest_conn)
                last_row = rows[-1]
            return

        cursor_name = "table_cursor_" + str(uuid.uuid4()).replace("-", "")
        cursor = dest_conn.cursor(name=cursor_name, withhold=True)
        try:
            cursor_query = "SELECT DISTINCT * FROM {}".format(
                fully_qualified_table(dest_temp_table)
            )
            cursor.execute(cursor_query)
            batch_size = self.__db_helper.get_batch_size(len(pk_columns))
            if allow_chunk and self.__source_pool:
                self.__parallel_id_batches(cursor, batch_size, copy_batch)
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
