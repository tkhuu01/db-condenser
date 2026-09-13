import os
import subprocess
from contextlib import closing
from urllib.parse import urlencode

from db_condenser import database_helper

_SCHEMA_DUMP_OPTIONS = (
    "--schema-only",
    "--no-owner",
    "--no-privileges",
    "--no-comments",
    "--no-tablespaces",
    "--no-publications",
    "--no-subscriptions",
    "--no-security-labels",
)
_FILTERED_COMMAND_PREFIXES = ("COMMENT ON CONSTRAINT", "COMMENT ON EXTENSION")


class PsqlDatabaseCreator:
    def __init__(self, source_dbc, destination_dbc, use_existing_dump=False):
        self.destination_dbc = destination_dbc
        self.source_dbc = source_dbc

        self.use_existing_dump = use_existing_dump

        self.output_path = os.path.join(os.getcwd(), "SQL")
        if not os.path.isdir(self.output_path):
            os.mkdir(self.output_path)

        self.add_constraint_output_path = os.path.join(
            os.getcwd(), "SQL", "add_constraint_output.txt"
        )
        self.add_constraint_error_path = os.path.join(
            os.getcwd(), "SQL", "add_constraint_error.txt"
        )

        if os.path.exists(self.add_constraint_output_path):
            os.remove(self.add_constraint_output_path)
        if os.path.exists(self.add_constraint_error_path):
            os.remove(self.add_constraint_error_path)

        self.create_output_path = os.path.join(os.getcwd(), "SQL", "create_output.txt")
        self.create_error_path = os.path.join(os.getcwd(), "SQL", "create_error.txt")

        if os.path.exists(self.create_output_path):
            os.remove(self.create_output_path)
        if os.path.exists(self.create_error_path):
            os.remove(self.create_error_path)

    def create(self):
        if self.use_existing_dump:
            return

        pre_data_sql = self._dump_schema("pre-data")
        self.run_psql(self._filter_commands(pre_data_sql))

    def teardown(self):
        helper = database_helper.get_specific_helper()
        with closing(self.source_dbc.get_db_connection()) as connection:
            user_schemas = helper.list_all_user_schemas(connection)

        if len(user_schemas) == 0:
            raise Exception("Couldn't find any non system schemas.")

        drop_statements = [
            'DROP SCHEMA IF EXISTS "{}" CASCADE'.format(helper.DELTA_SCHEMA)
        ] + [
            'DROP SCHEMA IF EXISTS "{}" CASCADE'.format(s)
            for s in user_schemas
            if s not in ("public", helper.DELTA_SCHEMA)
        ]

        q = ";".join(drop_statements)
        q += ";DROP SCHEMA IF EXISTS public CASCADE;CREATE SCHEMA IF NOT EXISTS public;"

        self.run_query(q)

    def add_constraints(self):
        if self.use_existing_dump:
            return

        self.run_psql(self._dump_schema("post-data"))

    def _dump_schema(self, section):
        result = self._run_command(
            "pg_dump",
            [
                _connection_argument(self.source_dbc),
                *_SCHEMA_DUMP_OPTIONS,
                "--section={}".format(section),
            ],
            "Capturing {} schema failed".format(section),
            stdout=subprocess.PIPE,
        )
        return result.stdout.decode("utf-8")

    @staticmethod
    def _filter_commands(commands):
        filtered_commands = []
        for line in commands.split("\n"):
            stripped_line = line.rstrip()
            if not stripped_line.startswith(_FILTERED_COMMAND_PREFIXES):
                filtered_commands.append(stripped_line)
        return "\n".join(filtered_commands)

    def run_query(self, query):
        self._run_command(
            "psql",
            [_connection_argument(self.destination_dbc), "-c {0}".format(query)],
            'Running query: "{}" failed'.format(query),
            stdout=subprocess.DEVNULL,
        )

    def run_psql(self, queries):
        self._run_command(
            "psql",
            [_connection_argument(self.destination_dbc)],
            "Creating schema failed",
            input=queries.encode("utf-8"),
            stdout=subprocess.DEVNULL,
        )

    @staticmethod
    def _run_command(executable, args, error_message, **kwargs):
        result = subprocess.run(
            [_postgres_executable(executable), *args],
            stderr=subprocess.PIPE,
            **kwargs,
        )
        if result.returncode != 0 or contains_errors(result.stderr):
            raise Exception("{}. Details:\n{}".format(error_message, result.stderr))
        return result


def _connection_argument(connect):
    return "--dbname=postgresql://{0}@{2}:{3}/{4}?{1}".format(
        connect.user,
        urlencode({"password": connect.password}),
        connect.host,
        connect.port,
        connect.db_name,
    )


def _postgres_executable(name):
    pg_bin_path = get_pg_bin_path()
    return os.path.join(pg_bin_path, name) if pg_bin_path else name


def get_pg_bin_path():
    pg_dump_path = os.environ.get("POSTGRES_PATH", "")
    try:
        result = subprocess.run(
            [os.path.join(pg_dump_path, "pg_dump"), "--help"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except OSError as error:
        raise _missing_postgres_utilities_error() from error
    if result.returncode != 0:
        raise _missing_postgres_utilities_error()
    return pg_dump_path


def _missing_postgres_utilities_error():
    return Exception(
        "Couldn't find Postgres utilities, consider specifying POSTGRES_PATH "
        "environment variable if Postgres isn't in your PATH."
    )


def contains_errors(stderr):
    msgs = stderr.decode("utf-8")
    return any(msg.strip().startswith("ERROR") for msg in msgs.split("\n"))
