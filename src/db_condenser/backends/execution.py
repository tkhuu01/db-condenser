"""SQL selection for one run, separate from graph traversal and scheduling.

Both shipped adapters reuse this executor to preserve their existing behavior.
PostgreSQL-specific relationship SQL intentionally remains on MySQL's legacy
path too: its known failures are characterized by tests, not fixed here.
Connections are borrowed from the run session; worker ownership is unchanged.
Legacy helpers still read global configuration; this extraction does not add
support for simultaneous independent runs with different configurations.
"""

import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from types import ModuleType

from db_condenser.backends import downstream, upstream
from db_condenser.backends.contracts import Backend, ConnectionFactory, RunSession
from db_condenser.backends.sql import (
    columns_to_copy,
    fully_qualified_table,
    mysql_db_name_hack,
    quoter,
    schema_name,
    table_name,
)
from db_condenser.config_reader import Config, DbType, DestinationMode, InitialTarget
from db_condenser.subset_utils import compute_batch_size


class SqlSelectionExecutor:
    def __init__(
        self,
        backend: Backend,
        helper: ModuleType,
        session: RunSession,
        destination_factory: ConnectionFactory,
        config: Config,
    ):
        self.backend = backend
        self.helper = helper
        self.config = config
        self.source = session.source
        self.destination = session.destination
        self.source_pool = session.source_pool
        self.destination_factory = destination_factory
        self.copy_rows = backend.copy_rows
        self.incremental = backend.uses_incremental(config)
        self.upstream_delta_reads = (
            self.incremental and config.destination_mode == DestinationMode.TOPUP
        )

    @property
    def parallel_reads(self):
        return bool(self.source_pool)

    def prefers_parallel(self, table):
        # Same 12,800 heap-page (~100MB) threshold used by table scheduling.
        return (
            self.helper.get_table_page_count(
                table_name(table), schema_name(table), self.source
            )
            >= 12_800
        )

    def load_pre_filters(self):
        self._pre_filter_cache = {}
        for pf in self.config.pre_filters:
            with self.source.cursor() as cur:
                cur.execute(pf.query)
                values = list(set(row[0] for row in cur.fetchall()))
                self._pre_filter_cache[pf.name] = values
                print(
                    "Pre-filter '{}' cached {} unique values".format(
                        pf.name, len(values)
                    )
                )

    def copy_table(self, table, source_conn=None, dest_conn=None, *, limit=None):
        source_conn = source_conn or self.source
        dest_conn = dest_conn or self.destination
        q = "SELECT * FROM {}".format(fully_qualified_table(table))
        if limit is not None:
            q += " LIMIT {}".format(limit)
        self.copy_rows(source_conn, dest_conn, q, mysql_db_name_hack(table, dest_conn))

    def select_upstream(
        self,
        target,
        processed_tables,
        relationships,
        source_conn,
        dest_conn,
        allow_chunk=False,
    ):
        return upstream.select_upstream(
            self,
            target,
            processed_tables,
            relationships,
            source_conn,
            dest_conn,
            allow_chunk,
        )

    def select_downstream(
        self, table, relationships, source_conn=None, dest_conn=None, allow_chunk=False
    ):
        return downstream.select_downstream(
            self, table, relationships, source_conn, dest_conn, allow_chunk
        )

    def _get_pre_filter_info(self, target: InitialTarget):
        """Return (column, values) for a target's pre_filter, or None."""
        if target.pre_filter is None:
            return None
        pf = next(
            (p for p in self.config.pre_filters if p.name == target.pre_filter), None
        )
        if pf is None:
            return None
        values = self._pre_filter_cache.get(pf.name)
        if not values:
            return None
        return (pf.column, values)

    def copy_table_parallel(
        self, table, columns_query="*", extra_conditions=None, params=None
    ):
        """Copy a table split across the source pool by ctid page ranges.

        Returns False when not applicable (no pool, or table too small to be
        worth splitting), leaving the caller to fall back.
        """
        if not self.source_pool:
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
        if self.incremental and self.helper.has_secondary_unique(table):
            two_phase = self.helper.delta_for(table) is not None
            if not two_phase:
                return False
        num_workers = len(self.source_pool)
        page_count = self.helper.get_table_page_count(
            table_name(table), schema_name(table), self.source
        )
        if page_count < num_workers * 10:
            return False

        fqt = fully_qualified_table(table)
        pages_per_worker = page_count // num_workers
        barrier = threading.Barrier(num_workers) if two_phase else None

        def worker(idx, start_page, end_page):
            source_conn = self.source_pool[idx]
            dest_conn = self.destination_factory.get_db_connection()
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
                    self.helper.stage_rows(source_conn, dest_conn, q, table, params)
                    self.helper.apply_staged(dest_conn, table, "refresh")
                    barrier.wait()
                    self.helper.apply_staged(dest_conn, table, "insert")
                else:
                    self.copy_rows(source_conn, dest_conn, q, table, params)
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

    def select_direct_parallel(self, target: InitialTarget, relationships):
        """Subset a direct target using parallel ctid page-range splitting."""
        t = target.table
        columns_query = columns_to_copy(
            t, relationships, self.source, backend=self.backend
        )
        fqt = fully_qualified_table(t)

        conditions = []
        if target.where is not None:
            conditions.append("({})".format(target.where))
        elif target.percent is not None:
            conditions.append("random() < {}".format(float(target.percent) / 100))
        pre_filter_info = self._get_pre_filter_info(target)
        params = None
        if pre_filter_info:
            conditions.append('{}."{}" = ANY(%s)'.format(fqt, pre_filter_info[0]))
            params = [pre_filter_info[1]]

        if not self.copy_table_parallel(t, columns_query, conditions, params):
            self.select_direct(target, relationships)

    def parallel_id_batches(
        self, dest_cursor, batch_size, copy_batch_fn, initial_rows=None
    ):
        """Fan ID batches from a destination cursor out across the source pool.

        copy_batch_fn(valid_rows, source_conn, dest_conn) runs one batch; the
        cursor is only read from this thread, so batches stay disjoint.
        initial_rows carries a batch the caller already fetched (and filtered).
        """
        dest_conns = [
            self.destination_factory.get_db_connection() for _ in self.source_pool
        ]
        pending = initial_rows
        try:
            with ThreadPoolExecutor(max_workers=len(self.source_pool)) as pool:
                exhausted = False
                while not exhausted:
                    futures = []
                    for src_conn, dst_conn in zip(self.source_pool, dest_conns):
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

    def stream_ids_to_source_temp(
        self, dest_query, columns, source_conn=None, dest_conn=None
    ):
        source_conn = source_conn or self.source
        dest_conn = dest_conn or self.destination
        id_temp = self.backend.create_id_temp_table(source_conn, len(columns))
        insert_q = 'INSERT INTO "{}" VALUES ({})'.format(
            id_temp, ",".join(["%s"] * len(columns))
        )
        cursor_name = "table_cursor_" + str(uuid.uuid4()).replace("-", "")
        dest_cursor = dest_conn.cursor(name=cursor_name, withhold=True)
        src_insert_cur = source_conn.cursor()
        try:
            dest_cursor.execute(dest_query)
            batch_size = compute_batch_size(len(columns))
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

    def build_temp_table_join(
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
            '{}.{} = "{}".col{}::{}'.format(
                fqt, quoter(col), id_temp, i, datatypes[col]
            )
            for i, col in enumerate(join_columns)
        )
        return 'SELECT {} FROM {} JOIN "{}" ON {}'.format(
            select_expr, fqt, id_temp, join_conditions
        )

    def select_direct(
        self, target: InitialTarget, relationships, source_conn=None, dest_conn=None
    ):
        source_conn = source_conn or self.source
        dest_conn = dest_conn or self.destination
        t = target.table
        columns_query = columns_to_copy(
            t, relationships, source_conn, backend=self.backend
        )
        if target.where is not None:
            q = "SELECT {} FROM {} WHERE {}".format(
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
        pre_filter_info = self._get_pre_filter_info(target)
        params = None
        if pre_filter_info:
            q += ' AND {}."{}" = ANY(%s)'.format(
                fully_qualified_table(t), pre_filter_info[0]
            )
            params = [pre_filter_info[1]]
        self.copy_rows(
            source_conn,
            dest_conn,
            q,
            mysql_db_name_hack(t, dest_conn),
            params,
        )
