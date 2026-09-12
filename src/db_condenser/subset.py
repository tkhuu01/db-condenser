import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from db_condenser.backends import get_backend
from db_condenser.backends.contracts import Backend, ConnectionFactory
from db_condenser.config_reader import get_config
from db_condenser.subset_utils import (
    compute_disconnected_tables,
    compute_downstream_strata,
    compute_upstream_strata,
    print_progress,
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
        source_dbc: ConnectionFactory,
        destination_dbc: ConnectionFactory,
        all_tables: list[str],
        *,
        backend: Backend | None = None,
    ):
        self.config = get_config()
        self.__all_tables = all_tables
        self.__backend = (
            backend if backend is not None else get_backend(self.config.db_type)
        )
        self.__incremental = self.__backend.uses_incremental(self.config)
        unknown_incremental_keys = set(self.config.incremental_key_map) - set(
            self.__all_tables
        )
        if self.__incremental and unknown_incremental_keys:
            raise ValueError(
                "incremental_keys references unknown or excluded tables: "
                + ", ".join(sorted(unknown_incremental_keys))
            )

        self.__destination_dbc = destination_dbc
        self.__session = self.__backend.open_run(
            source_dbc, destination_dbc, self.config
        )
        self.__source_conn = self.__session.source
        self.__destination_conn = self.__session.destination
        try:
            self.__executor = self.__backend.selection_executor(
                self.__session, destination_dbc, self.config
            )
        except BaseException:
            self.__session.close()
            raise
        # table-level concurrency follows parallel_read_workers when set,
        # otherwise keeps the historical 4 threads
        self.__table_workers = (
            self.config.parallel_read_workers
            if self.config.parallel_read_workers > 1
            else 4
        )

    def __get_source_connection(self):
        return self.__session.open_source_connection()

    def run_middle_out(self):
        passthrough_tables = self.config.passthrough_tables
        relationships = self.__backend.get_unredacted_fk_relationships(
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

        self.__executor.load_pre_filters()

        # start by subsetting the direct targets
        print(
            "Beginning direct targets: " + ", ".join(self.config.initial_target_tables)
        )
        start_time = time.time()
        processed_tables = set()
        if self.__executor.parallel_reads:
            for idx, target in enumerate(self.config.initial_targets):
                print_progress(target, idx + 1, len(self.config.initial_targets))
                self.__executor.select_direct_parallel(target, relationships)
        elif len(self.config.initial_targets) >= 3:
            self.__subset_direct_concurrent(relationships)
        else:
            for idx, target in enumerate(self.config.initial_targets):
                print_progress(target, idx + 1, len(self.config.initial_targets))
                self.__executor.select_direct(target, relationships)
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
                self.__executor.copy_table(t)
            print(
                "Disconnected tables completed in {:.1f}s".format(
                    time.time() - start_time
                )
            )

    def prep_temp_dbs(self):
        self.__session.prepare()
        if self.__incremental:
            relationships = self.__backend.get_unredacted_fk_relationships(
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
            self.__session.prepare_incremental(incremental_tables)

    def unprep_temp_dbs(self, succeeded=True):
        self.__session.finish(succeeded)

    def close_connections(self):
        self.__session.close()

    def __process_stratum_upstream(
        self, stratum, processed_tables, relationships, start_idx, total_count
    ):
        added = set()
        if len(stratum) <= 1:
            for t in stratum:
                print_progress(t, start_idx + 1, total_count)
                data_added = self.__executor.select_upstream(
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
        if self.__executor.parallel_reads:
            for t in list(small_tables):
                if self.__executor.prefers_parallel(t):
                    small_tables.remove(t)
                    big_tables.append(t)

        def upstream_worker(table):
            source_conn = self.__get_source_connection()
            dest_conn = self.__destination_dbc.get_db_connection()
            try:
                return self.__executor.select_upstream(
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
            if self.__executor.select_upstream(
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
        if len(stratum) <= 1:
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
            dest_conn = self.__destination_dbc.get_db_connection()
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

    def __copy_table_worker(self, table):
        source_conn = self.__get_source_connection()
        dest_conn = self.__destination_dbc.get_db_connection()
        # no-op on Postgres; preserves prior MySQL behavior where this path
        # used the shared dest connection that had constraints disabled
        self.__backend.turn_off_constraints(dest_conn)
        try:
            self.__executor.copy_table(
                table, source_conn, dest_conn, limit=self.config.max_rows_per_table
            )
        finally:
            source_conn.close()
            dest_conn.close()

    def __copy_tables_concurrent(self, tables):
        if self.__executor.parallel_reads and self.config.max_rows_per_table is None:
            # split each table across the pool by ctid page ranges; tables too
            # small to split fall back to a plain single-connection copy
            for idx, t in enumerate(tables):
                print_progress(t, idx + 1, len(tables))
                if not self.__executor.copy_table_parallel(t):
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
            dest_conn = self.__destination_dbc.get_db_connection()
            # no-op on Postgres; preserves prior MySQL behavior where this path
            # used the shared dest connection that had constraints disabled
            self.__backend.turn_off_constraints(dest_conn)
            try:
                self.__executor.select_direct(
                    target, relationships, source_conn, dest_conn
                )
            finally:
                source_conn.close()
                dest_conn.close()

        with ThreadPoolExecutor(max_workers=self.__table_workers) as pool:
            futures = {pool.submit(direct_worker, t): t for t in targets}
            for idx, future in enumerate(as_completed(futures)):
                target = futures[future]
                print_progress(target, idx + 1, len(targets))
                future.result()

    def subset_downstream(
        self, table, relationships, source_conn=None, dest_conn=None, allow_chunk=False
    ):
        return self.__executor.select_downstream(
            table, relationships, source_conn, dest_conn, allow_chunk
        )
