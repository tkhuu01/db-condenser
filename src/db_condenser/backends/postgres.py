from db_condenser import psql_database_helper
from db_condenser.backends.contracts import (
    BackendCapabilities,
    ConnectionFactory,
    SchemaManager,
)
from db_condenser.backends.legacy import LegacyOperations
from db_condenser.backends.session import BaseRunSession


class PostgresBackend(LegacyOperations):
    capabilities = BackendCapabilities(
        read_only_source=True,
        relationship_selection=True,
        incremental=True,
        shared_snapshot=True,
        parallel_reads=True,
        sequence_reset=True,
    )

    def __init__(self):
        super().__init__(psql_database_helper)

    def open_run(self, source, destination, config):
        return PostgresRunSession(self, source, destination, config)

    def schema_manager(
        self, source: ConnectionFactory, destination: ConnectionFactory
    ) -> SchemaManager:
        from db_condenser.psql_database_creator import PsqlDatabaseCreator

        return PsqlDatabaseCreator(source, destination, False)


class PostgresRunSession(BaseRunSession):
    def _initialize(self):
        self._dropped_fks = []
        self._incremental_prepared = False
        if self.config.use_temp_tables:
            with self.source.cursor() as cur:
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

        # Keep this transaction open until close so every worker can import it.
        with self.source.cursor() as cur:
            cur.execute("SELECT pg_export_snapshot()")
            self._snapshot_id = cur.fetchone()[0]
        if self.config.parallel_read_workers > 1:
            for _ in range(self.config.parallel_read_workers):
                connection = self.open_source_connection()
                self._connections.append(connection)
                self.source_pool.append(connection)

    def open_source_connection(self):
        connection = super().open_source_connection()
        try:
            with connection.cursor() as cur:
                cur.execute("SET TRANSACTION SNAPSHOT '{}'".format(self._snapshot_id))
        except BaseException:
            connection.close()
            raise
        return connection

    def prepare_incremental(self, tables):
        psql_database_helper.prep_incremental(self.source, self.destination, tables)
        self._incremental_prepared = True
        self._dropped_fks = psql_database_helper.drop_fk_constraints(
            self.destination, tables
        )

    def finish(self, succeeded):
        super().finish(succeeded)
        if self._incremental_prepared:
            try:
                self.destination.connection.rollback()
                psql_database_helper.restore_fk_constraints(
                    self.destination, self._dropped_fks
                )
                if succeeded:
                    psql_database_helper.unprep_incremental(self.destination)
                else:
                    psql_database_helper.retain_incremental(self.destination)
            except BaseException:
                self.destination.connection.rollback()
                psql_database_helper.retain_incremental(self.destination)
                raise
            finally:
                self._incremental_prepared = False
