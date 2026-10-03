"""Explicit reuse of existing helpers and SQL selection by shipped adapters."""

from types import ModuleType

from db_condenser.backends.contracts import (
    ColumnInfo,
    Connection,
    ConnectionFactory,
    QueryParameters,
    Relationship,
    RunSession,
    SelectionExecutor,
)
from db_condenser.config_reader import Config, DbType


class LegacyOperations:
    def __init__(self, helper: ModuleType):
        self._helper = helper

    def uses_incremental(self, config: Config) -> bool:
        return config.is_incremental and config.db_type == DbType.POSTGRES

    def selection_executor(
        self, session: RunSession, destination: ConnectionFactory, config: Config
    ) -> SelectionExecutor:
        from db_condenser.backends.execution import SqlSelectionExecutor

        return SqlSelectionExecutor(self, self._helper, session, destination, config)

    def list_all_tables(self, source: ConnectionFactory) -> list[str]:
        return self._helper.list_all_tables(source)

    def get_unredacted_fk_relationships(
        self, tables: list[str], connection: Connection
    ) -> list[Relationship]:
        return self._helper.get_unredacted_fk_relationships(tables, connection)

    def get_table_columns(
        self, table: str, schema: str | None, connection: Connection
    ) -> list[str]:
        return self._helper.get_table_columns(table, schema, connection)

    def get_table_datatypes(
        self, table: str, schema: str | None, connection: Connection
    ) -> list[ColumnInfo]:
        return self._helper.get_table_datatypes(table, schema, connection)

    def get_table_count_estimate(
        self, table: str, schema: str | None, connection: Connection
    ) -> int:
        return self._helper.get_table_count_estimate(table, schema, connection)

    def copy_rows(
        self,
        source: Connection,
        destination: Connection,
        query: str,
        destination_table: str,
        params: QueryParameters = None,
        batch_size: int | None = None,
    ) -> None:
        # None means "use the backend default". Passing it through would
        # replace MySQL's 1,000-row default with an invalid fetchmany(None).
        if batch_size is None:
            return self._helper.copy_rows(
                source, destination, query, destination_table, params
            )
        return self._helper.copy_rows(
            source, destination, query, destination_table, params, batch_size=batch_size
        )

    def run_query(
        self, query: str, connection: Connection, commit: bool = True
    ) -> None:
        return self._helper.run_query(query, connection, commit=commit)

    def update_sequence_numbering(
        self, connection: Connection, tables: list[str]
    ) -> None:
        return self._helper.update_sequence_numbering(connection, tables)

    def turn_off_constraints(self, connection: Connection) -> None:
        return self._helper.turn_off_constraints(connection)

    def prep_temp_dbs(self, source: Connection, destination: Connection) -> None:
        return self._helper.prep_temp_dbs(source, destination)

    def unprep_temp_dbs(self, source: Connection, destination: Connection) -> None:
        return self._helper.unprep_temp_dbs(source, destination)

    def create_id_temp_table(
        self, connection: Connection, number_of_columns: int
    ) -> str:
        return self._helper.create_id_temp_table(connection, number_of_columns)
