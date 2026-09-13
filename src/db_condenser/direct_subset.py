import argparse
import sys
from importlib import resources

from db_condenser import config_reader, run_subset
from db_condenser.config_reader import DbConnectInfo


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
        config = config_reader.load_config(config_file)
    except FileNotFoundError:
        print(
            f"Config file '{config_file}' not found.\n"
            "Run 'subset --help-config' for the configuration reference, or\n"
            "'subset --example-config > config.json' to start from a template.",
            file=sys.stderr,
        )
        sys.exit(1)

    dest_info = config.destination_db_connection_info
    if not args.yes and dest_info.host not in ("localhost", "127.0.0.1"):
        _confirm_destination(dest_info)

    try:
        run_subset(config, verbose=args.verbose, no_constraints=args.no_constraints)
    except KeyboardInterrupt:
        print("\nInterrupted — closing connections...")
        raise


if __name__ == "__main__":
    main()
