"""The shared backend boundary; no database driver imports."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol, TypedDict, runtime_checkable

from db_condenser.config_reader import Config, InitialTarget


class Relationship(TypedDict):
    """Ordered FK/reference columns; composite pairs must never be flattened.

    Keep the existing dictionary representation so graph code needs no
    conversion or duplicate relationship model during this extraction.
    """

    fk_table: str
    fk_columns: list[str]
    target_table: str
    target_columns: list[str]


# Existing catalog result: name, database type, generated flag, identity flag.
ColumnInfo = tuple[str, str, str, str]
QueryParameters = Sequence[Any] | Mapping[str, Any] | None


class Connection(Protocol):
    """Borrowed connection. Adapters pass existing wrappers through unchanged."""

    def cursor(self, name: str | None = None, withhold: bool = False) -> Any: ...
    def commit(self) -> None: ...
    def close(self) -> None: ...


class ConnectionFactory(Protocol):
    """Existing DbConnect surface, including schema-tool connection options."""

    db_name: str
    host: str
    port: int
    user: str
    password: str | None
    ssl_mode: str | None

    def get_db_connection(self, read_repeatable: bool = False) -> Connection: ...


@runtime_checkable
class SchemaManager(Protocol):
    """Explicit destructive lifecycle, implemented by the existing creators.

    Construction and methods retain the legacy creators' connection and file
    ownership. No automatic teardown, transaction, or cleanup is added here.
    """

    def teardown(self) -> None: ...
    def create(self) -> None: ...
    def add_constraints(self) -> None: ...


class RunSession(Protocol):
    """Owns main connections and pooled readers for a single run.

    Additional worker connections returned by open_source_connection are
    caller-owned. finish restores run state; close always releases connections.
    """

    source: Connection
    destination: Connection
    source_pool: list[Connection]

    def open_source_connection(self) -> Connection: ...
    def prepare(self) -> None: ...
    def prepare_incremental(self, tables: list[str]) -> None: ...
    def finish(self, succeeded: bool) -> None: ...
    def close(self) -> None: ...


class SelectionExecutor(Protocol):
    """Run-bound selection operations; traversal never builds or executes SQL.

    Main/pool connections are borrowed from RunSession. Optional worker
    connections are borrowed from the caller. Operations preserve existing
    transfer commits, batching, and error propagation; they do not own cleanup
    of the run. parallel_reads describes the available run pool, not a new
    strategy switch. prefers_parallel retains the backend's size threshold.
    """

    @property
    def parallel_reads(self) -> bool: ...

    def prefers_parallel(self, table: str) -> bool: ...
    def load_pre_filters(self) -> None: ...
    def copy_table(
        self,
        table: str,
        source_conn: Connection | None = None,
        dest_conn: Connection | None = None,
        *,
        limit: int | None = None,
    ) -> None: ...
    def copy_table_parallel(self, table: str) -> bool: ...
    def select_direct(
        self,
        target: InitialTarget,
        relationships: list[Relationship],
        source_conn: Connection | None = None,
        dest_conn: Connection | None = None,
    ) -> None: ...
    def select_direct_parallel(
        self,
        target: InitialTarget,
        relationships: list[Relationship],
    ) -> None: ...
    def select_upstream(
        self,
        target: str,
        processed_tables: set[str],
        relationships: list[Relationship],
        source_conn: Connection,
        dest_conn: Connection,
        allow_chunk: bool = False,
    ) -> bool: ...
    def select_downstream(
        self,
        table: str,
        relationships: list[Relationship],
        source_conn: Connection | None = None,
        dest_conn: Connection | None = None,
        allow_chunk: bool = False,
    ) -> None: ...


@dataclass(frozen=True)
class BackendCapabilities:
    """Descriptive support, not runtime routing or permission checks.

    read_only_source applies to the default path, without source temp tables.
    relationship_selection means end-to-end traversal, not FK discovery.
    parallel_reads means within-table parallel reads, not table-level threads.
    MySQL's currently failing relationship paths must not be advertised as
    supported or silently skipped just because this flag is false.
    """

    read_only_source: bool
    relationship_selection: bool
    incremental: bool
    shared_snapshot: bool
    parallel_reads: bool
    sequence_reset: bool


@runtime_checkable
class Backend(Protocol):
    """Operations currently common to the two database implementations.

    Supplied connections remain caller-owned; adapters never close them or
    add commits/rollbacks. Metadata reads leave source transactions open.
    copy_rows streams through the existing transfer helper, commits destination
    batches as before, and never commits the source. run_query commits only
    when requested. Driver/helper exceptions propagate without translation.

    Scratch setup/cleanup retain backend-specific effects: PostgreSQL clears
    metadata caches; MySQL creates/drops its source/destination scratch database.
    Run sessions coordinate snapshots, incremental journals, and FK restoration
    through the existing backend-specific SQL helpers.
    """

    @property
    def capabilities(self) -> BackendCapabilities: ...

    def uses_incremental(self, config: Config) -> bool: ...

    def selection_executor(
        self,
        session: RunSession,
        destination: ConnectionFactory,
        config: Config,
    ) -> SelectionExecutor: ...

    def open_run(
        self,
        source: ConnectionFactory,
        destination: ConnectionFactory,
        config: Config,
    ) -> RunSession: ...

    def schema_manager(
        self, source: ConnectionFactory, destination: ConnectionFactory
    ) -> SchemaManager: ...
    def list_all_tables(self, source: ConnectionFactory) -> list[str]: ...
    def get_unredacted_fk_relationships(
        self, tables: list[str], connection: Connection
    ) -> list[Relationship]: ...
    def get_table_columns(
        self, table: str, schema: str | None, connection: Connection
    ) -> list[str]: ...
    def get_table_datatypes(
        self, table: str, schema: str | None, connection: Connection
    ) -> list[ColumnInfo]: ...
    def get_table_count_estimate(
        self, table: str, schema: str | None, connection: Connection
    ) -> int: ...
    def copy_rows(
        self,
        source: Connection,
        destination: Connection,
        query: str,
        destination_table: str,
        params: QueryParameters = None,
        batch_size: int | None = None,
    ) -> None: ...
    def run_query(
        self, query: str, connection: Connection, commit: bool = True
    ) -> None: ...
    def update_sequence_numbering(
        self, connection: Connection, tables: list[str]
    ) -> None: ...
    def turn_off_constraints(self, connection: Connection) -> None: ...
    def prep_temp_dbs(self, source: Connection, destination: Connection) -> None: ...
    def unprep_temp_dbs(self, source: Connection, destination: Connection) -> None: ...
    def create_id_temp_table(
        self, connection: Connection, number_of_columns: int
    ) -> str: ...
