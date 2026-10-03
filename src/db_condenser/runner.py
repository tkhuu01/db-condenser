"""The shared application workflow for Python consumers and the CLI."""

import time
from collections.abc import Iterator
from contextlib import closing, contextmanager
from pathlib import Path

from db_condenser import config_reader, result_tabulator
from db_condenser.backends import get_backend
from db_condenser.config_reader import Config, DbType, DestinationMode
from db_condenser.db_connect import DbConnect
from db_condenser.subset import Subset
from db_condenser.subset_utils import print_progress


@contextmanager
def _subset_run(subsetter: Subset) -> Iterator[None]:
    """Own selection and cleanup, including any finishing work in the body."""
    succeeded = False
    try:
        subsetter._prepare()
        subsetter._run_middle_out()
        yield
        succeeded = True
    finally:
        try:
            subsetter._finalize(succeeded)
        finally:
            subsetter._close()


def run_subset(
    config: Config | str | Path,
    *,
    verbose: bool = False,
    no_constraints: bool = False,
    report: bool = True,
) -> None:
    """Run the complete subset workflow and release its connections.

    Configuration may be a file path or a Config. Destination recreation is
    destructive and follows destination_mode; confirmation belongs to the CLI.
    no_constraints skips only recreate constraint installation. report controls
    the final count summary, not progress output. Errors propagate to the caller.

    Sequential calls are supported. Legacy global state means concurrent or
    nested independent runs in the same process are not supported.
    """
    if not isinstance(config, Config):
        config = config_reader.load_config(config)
    previous_config = config_reader.config
    config_reader.config = config
    try:
        _run(config, verbose=verbose, no_constraints=no_constraints, report=report)
    finally:
        config_reader.config = previous_config


def _run(config: Config, *, verbose: bool, no_constraints: bool, report: bool):
    source = DbConnect(config.db_type, config.source_db_connection_info, verbose)
    destination = DbConnect(
        config.db_type, config.destination_db_connection_info, verbose
    )
    backend = get_backend(config.db_type)
    database = backend.schema_manager(source, destination)
    if config.destination_mode == DestinationMode.RECREATE:
        database.teardown()
        database.create()

    tables = [
        table
        for table in backend.list_all_tables(source)
        if table not in config.excluded_tables
    ]
    subsetter = Subset(source, destination, tables, backend=backend)
    total_start_time = time.time()
    with _subset_run(subsetter):
        print("Beginning pre-constraint SQL calls")
        start_time = time.time()
        for idx, statement in enumerate(config.pre_constraint_sql):
            print_progress(statement, idx + 1, len(config.pre_constraint_sql))
            with closing(destination.get_db_connection()) as connection:
                backend.run_query(statement, connection)
        print(
            "Pre-constraint SQL completed in {:.1f}s".format(time.time() - start_time)
        )

        print("Adding database constraints")
        if not no_constraints and config.destination_mode == DestinationMode.RECREATE:
            database.add_constraints()

        print("Beginning post-subset SQL calls")
        start_time = time.time()
        for idx, statement in enumerate(config.post_subset_sql):
            print_progress(statement, idx + 1, len(config.post_subset_sql))
            with closing(destination.get_db_connection()) as connection:
                backend.run_query(statement, connection)
        print("Post-subset SQL completed in {:.1f}s".format(time.time() - start_time))

        print("Resetting sequence numbering")
        with closing(destination.get_db_connection()) as connection:
            if config.db_type == DbType.POSTGRES:
                backend.update_sequence_numbering(
                    connection, [table for table in tables if "pgbench" not in table]
                )

        total_elapsed = time.time() - total_start_time
        if report:
            result_tabulator.tabulate(
                source, destination, tables, total_elapsed, backend=backend
            )
