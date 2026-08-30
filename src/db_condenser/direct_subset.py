import argparse
import sys
import time
from importlib import resources

from db_condenser import config_reader, database_helper, result_tabulator
from db_condenser.config_reader import DbConnectInfo, DbType, DestinationMode
from db_condenser.db_connect import DbConnect, MySqlConnection, PsqlConnection
from db_condenser.mysql_database_creator import MySqlDatabaseCreator
from db_condenser.psql_database_creator import PsqlDatabaseCreator
from db_condenser.subset import Subset
from db_condenser.subset_utils import print_progress


def db_creator(
    db_type: DbType, source: DbConnect, dest: DbConnect
) -> PsqlDatabaseCreator | MySqlDatabaseCreator:
    if db_type == DbType.POSTGRES:
        return PsqlDatabaseCreator(source, dest, False)
    elif db_type == DbType.MYSQL:
        return MySqlDatabaseCreator(source, dest)


def _parse_args():
    parser = argparse.ArgumentParser(description="Database Condenser")
    parser.add_argument(
        "-y", "--yes", action="store_true", help="Skip destination confirmation prompt"
    )
    parser.add_argument(
        "--no-constraints", action="store_true", help="Skip adding constraints"
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Log every query with timing"
    )
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Specify a custom JSON config file name",
    )
    parser.add_argument(
        "--help-config",
        action="store_true",
        help="Print the full configuration reference and exit",
    )
    parser.add_argument(
        "--example-config",
        action="store_true",
        help="Print an example config.json with all options and exit",
    )
    return parser.parse_args()


def _print_packaged_file(name: str):
    print(resources.files("db_condenser").joinpath(name).read_text(), end="")


def _confirm_destination(dest_info: DbConnectInfo):
    print(
        f"\nDestination: {dest_info.host}:{dest_info.port}/{dest_info.db_name}"
        f" (user: {dest_info.user_name})"
    )
    response = input("Proceed with subsetting into this destination? [y/N] ")
    if response.lower() not in ("y", "yes"):
        print("Aborted.")
        sys.exit(1)


def main():
    args = _parse_args()

    if args.help_config:
        _print_packaged_file("CONFIG.md")
        return
    if args.example_config:
        _print_packaged_file("config.json.example_all")
        return

    config_file = args.config or "config.json"
    try:
        config_reader.initialize(config_file)
    except FileNotFoundError:
        print(
            f"Config file '{config_file}' not found.\n"
            "Run 'subset --help-config' for the configuration reference, or\n"
            "'subset --example-config > config.json' to start from a template.",
            file=sys.stderr,
        )
        sys.exit(1)

    config = config_reader.get_config()

    db_type = config.db_type
    source_dbc = DbConnect(
        db_type, config.source_db_connection_info, verbose=args.verbose
    )

    dest_info = config.destination_db_connection_info
    if not args.yes and dest_info.host not in ("localhost", "127.0.0.1"):
        _confirm_destination(dest_info)

    destination_dbc = DbConnect(db_type, dest_info, verbose=args.verbose)

    db_helper = database_helper.get_specific_helper()
    if db_type == DbType.MYSQL:
        source_conn = source_dbc.get_db_connection()
        try:
            db_helper.validate_supported_version(source_conn)
        finally:
            source_conn.close()
        destination_conn = destination_dbc.get_server_connection()
        try:
            db_helper.validate_supported_version(destination_conn)
        finally:
            destination_conn.close()

    database = db_creator(db_type, source_dbc, destination_dbc)

    if config.destination_mode == DestinationMode.RECREATE:
        database.teardown()
        database.create()

    # Get list of tables to operate on
    all_tables = db_helper.list_all_tables(source_dbc)
    all_tables = [x for x in all_tables if x not in config.excluded_tables]

    subsetter = Subset(source_dbc, destination_dbc, all_tables)

    total_start_time = time.time()
    succeeded = False
    try:
        subsetter.prep_temp_dbs()
        subsetter.run_middle_out()

        print("Beginning pre-constraint SQL calls")
        start_time = time.time()
        for idx, sql in enumerate(config.pre_constraint_sql):
            print_progress(sql, idx + 1, len(config.pre_constraint_sql))
            db_helper.run_query(sql, destination_dbc.get_db_connection())
        print(
            "Pre-constraint SQL completed in {:.1f}s".format(time.time() - start_time)
        )

        print("Adding database constraints")
        if (
            not args.no_constraints
            and config.destination_mode == DestinationMode.RECREATE
        ):
            database.add_constraints()

        print("Beginning post-subset SQL calls")
        start_time = time.time()
        for idx, sql in enumerate(config.post_subset_sql):
            print_progress(sql, idx + 1, len(config.post_subset_sql))
            db_helper.run_query(sql, destination_dbc.get_db_connection())
        print("Post-subset SQL completed in {:.1f}s".format(time.time() - start_time))

        print("Resetting sequence numbering")
        all_tables_no_pg = [table for table in all_tables if "pgbench" not in table]
        dest_conn = destination_dbc.get_db_connection()
        if db_type == DbType.POSTGRES:
            assert isinstance(dest_conn, PsqlConnection)
            db_helper.update_sequence_numbering(dest_conn, all_tables_no_pg)
        elif db_type == DbType.MYSQL:
            # TODO update sequencing for mysql
            assert isinstance(dest_conn, MySqlConnection)
            # db_helper.update_sequence_numbering(
            #    dest_conn, all_tables_no_pg
            # )

        total_elapsed = time.time() - total_start_time
        result_tabulator.tabulate(
            source_dbc, destination_dbc, all_tables, total_elapsed
        )
        if (
            db_type == DbType.MYSQL
            and config.destination_mode == DestinationMode.RECREATE
        ):
            database.enable_events()
        succeeded = True
    except KeyboardInterrupt:
        print("\nInterrupted — closing connections...")
        raise
    finally:
        try:
            subsetter.unprep_temp_dbs(succeeded=succeeded)
        finally:
            subsetter.close_connections()


if __name__ == "__main__":
    main()
