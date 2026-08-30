import os
import re
import subprocess

_SCHEMA_DUMP_OPTIONS = (
    "--no-data",
    "--routines",
    "--events",
    "--triggers",
    "--no-tablespaces",
    "--set-gtid-purged=OFF",
)

_EVENT_DECLARATION = re.compile(
    rb"^/\*!50106 CREATE\*/[^\r\n]*?/\*!50106 EVENT `", re.MULTILINE
)
_EVENT_STATUS = re.compile(
    rb"^(?P<prefix>/\*!50106 CREATE\*/[^\r\n]*?/\*!50106 EVENT "
    rb"`(?P<name>(?:``|[^`])+)`[^\r\n]*? ON COMPLETION (?:NOT )?PRESERVE )"
    rb"(?P<status>ENABLE|DISABLE(?: ON (?:REPLICA|SLAVE))?)\b",
    re.MULTILINE,
)


def _disable_enabled_events(schema):
    event_count = len(_EVENT_DECLARATION.findall(schema))
    enabled_events = []

    def disable(match):
        if match.group("status") != b"ENABLE":
            return match.group(0)
        enabled_events.append(match.group("name").replace(b"``", b"`").decode("utf-8"))
        return match.group("prefix") + b"DISABLE"

    schema, parsed_count = _EVENT_STATUS.subn(disable, schema)
    if parsed_count != event_count:
        raise RuntimeError(
            "Could not safely disable all MySQL event definitions in the schema dump"
        )
    return schema, enabled_events


class MySqlDatabaseCreator:
    def __init__(self, source_connect, destination_connect):
        self.source_dbc = source_connect
        self.destination_dbc = destination_connect
        self._events_to_enable = []

    def create(self):
        schema = self._run_command(
            "mysqldump",
            [
                *_SCHEMA_DUMP_OPTIONS,
                *connection_args(self.source_dbc),
                self.source_dbc.db_name,
            ],
            "Capturing schema failed",
            stdout=subprocess.PIPE,
        ).stdout
        schema, self._events_to_enable = _disable_enabled_events(schema)

        self.run_query_on_destination(
            "CREATE DATABASE {}".format(_quote_identifier(self.destination_dbc.db_name))
        )
        self._run_command(
            "mysql",
            [
                *connection_args(self.destination_dbc),
                "--database={}".format(self.destination_dbc.db_name),
            ],
            "Creating destination schema failed",
            input=schema,
            stdout=subprocess.DEVNULL,
        )

    def enable_events(self):
        if not self._events_to_enable:
            return
        statements = ";".join(
            "ALTER EVENT {}.{} ENABLE".format(
                _quote_identifier(self.destination_dbc.db_name),
                _quote_identifier(event),
            )
            for event in self._events_to_enable
        )
        self.run_query_on_destination(statements)
        self._events_to_enable = []

    def teardown(self):
        self.run_query_on_destination(
            "DROP DATABASE IF EXISTS {}".format(
                _quote_identifier(self.destination_dbc.db_name)
            )
        )

    def add_constraints(self):
        # mysqldump includes keys and constraints in CREATE TABLE statements.
        pass

    def run_query_on_destination(self, command):
        self._run_command(
            "mysql",
            [*connection_args(self.destination_dbc), "--execute={}".format(command)],
            'Running query: "{}" failed'.format(command),
            stdout=subprocess.DEVNULL,
        )

    @staticmethod
    def _run_command(executable, args, error_message, **kwargs):
        try:
            result = subprocess.run(
                [_mysql_executable(executable), *args],
                stderr=subprocess.PIPE,
                **kwargs,
            )
        except OSError as error:
            raise Exception(
                "{}. Could not execute {}: {}".format(error_message, executable, error)
            ) from error
        if result.returncode != 0:
            raise Exception(
                "{}. Details:\n{}".format(
                    error_message, result.stderr.decode("utf-8", errors="replace")
                )
            )
        return result


def get_mysql_bin_path():
    mysql_bin_path = os.environ.get("MYSQL_PATH", "")
    for executable in ("mysqldump", "mysql"):
        try:
            result = subprocess.run(
                [os.path.join(mysql_bin_path, executable), "--help"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except OSError as error:
            raise _missing_mysql_utilities_error() from error
        if result.returncode != 0:
            raise _missing_mysql_utilities_error()
    return mysql_bin_path


def connection_args(connect):
    return [
        "--protocol=TCP",
        "--host={}".format(connect.host),
        "--port={}".format(connect.port),
        "--user={}".format(connect.user),
        "--password={}".format(connect.password),
    ]


def _mysql_executable(name):
    mysql_bin_path = get_mysql_bin_path()
    return os.path.join(mysql_bin_path, name) if mysql_bin_path else name


def _quote_identifier(identifier):
    return "`{}`".format(identifier.replace("`", "``"))


def _missing_mysql_utilities_error():
    return Exception(
        "Couldn't find MySQL utilities, consider specifying MYSQL_PATH "
        "if mysql and mysqldump aren't in your PATH."
    )
