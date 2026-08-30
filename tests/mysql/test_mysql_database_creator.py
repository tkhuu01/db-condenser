import os
import subprocess
from types import SimpleNamespace

import pytest

from db_condenser import mysql_database_creator
from db_condenser.mysql_database_creator import MySqlDatabaseCreator


def _db_connect(user, password, host, port, db_name):
    return SimpleNamespace(
        user=user,
        password=password,
        host=host,
        port=port,
        db_name=db_name,
    )


@pytest.fixture
def creator():
    source = _db_connect("source-user", "source password", "source-db", 3307, "app")
    destination = _db_connect(
        "dest-user", "dest password", "destination-db", 3308, "subset"
    )
    return MySqlDatabaseCreator(source, destination)


def test_create_dumps_complete_schema_and_imports_it(creator, monkeypatch):
    calls = []
    schema = b"CREATE TABLE example (id BIGINT PRIMARY KEY);\n"

    def run(command, **kwargs):
        calls.append((command, kwargs))
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=schema if command[0].endswith("mysqldump") else None,
            stderr=b"",
        )

    monkeypatch.setattr(
        mysql_database_creator, "get_mysql_bin_path", lambda: "/opt/mysql/bin"
    )
    monkeypatch.setattr(mysql_database_creator.subprocess, "run", run)

    creator.create()

    dump_command, dump_kwargs = calls[0]
    assert dump_command == [
        "/opt/mysql/bin/mysqldump",
        *mysql_database_creator._SCHEMA_DUMP_OPTIONS,
        "--protocol=TCP",
        "--host=source-db",
        "--port=3307",
        "--user=source-user",
        "--password=source password",
        "app",
    ]
    assert dump_kwargs == {"stderr": subprocess.PIPE, "stdout": subprocess.PIPE}

    create_command, create_kwargs = calls[1]
    assert create_command == [
        "/opt/mysql/bin/mysql",
        "--protocol=TCP",
        "--host=destination-db",
        "--port=3308",
        "--user=dest-user",
        "--password=dest password",
        "--execute=CREATE DATABASE `subset`",
    ]
    assert create_kwargs == {
        "stderr": subprocess.PIPE,
        "stdout": subprocess.DEVNULL,
    }

    import_command, import_kwargs = calls[2]
    assert import_command[-1] == "--database=subset"
    assert import_kwargs == {
        "stderr": subprocess.PIPE,
        "input": schema,
        "stdout": subprocess.DEVNULL,
    }


def test_create_keeps_enabled_events_disabled_until_explicitly_enabled(
    creator, monkeypatch
):
    calls = []
    schema = (
        b"/*!50106 CREATE*/ /*!50117 DEFINER=`root`@`%`*/ "
        b"/*!50106 EVENT `enabled``event` ON SCHEDULE EVERY 1 DAY "
        b"ON COMPLETION NOT PRESERVE ENABLE COMMENT 'ENABLE here' "
        b"DO SELECT 'ENABLE in body' */ ;;\n"
        b"/*!50106 CREATE*/ /*!50117 DEFINER=`root`@`%`*/ "
        b"/*!50106 EVENT `disabled_event` ON SCHEDULE EVERY 1 DAY "
        b"ON COMPLETION PRESERVE DISABLE DO SELECT 2 */ ;;\n"
    )

    def run(command, **kwargs):
        calls.append((command, kwargs))
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=schema if command[0].endswith("mysqldump") else None,
            stderr=b"",
        )

    monkeypatch.setattr(
        mysql_database_creator, "get_mysql_bin_path", lambda: "/opt/mysql/bin"
    )
    monkeypatch.setattr(mysql_database_creator.subprocess, "run", run)

    creator.create()

    imported_schema = calls[2][1]["input"]
    assert b"`enabled``event` ON SCHEDULE EVERY 1 DAY " in imported_schema
    assert (
        b"ON COMPLETION NOT PRESERVE DISABLE COMMENT 'ENABLE here'" in imported_schema
    )
    assert b"ON COMPLETION PRESERVE DISABLE DO SELECT 2" in imported_schema

    creator.enable_events()

    assert calls[3][0][-1] == ("--execute=ALTER EVENT `subset`.`enabled``event` ENABLE")


def test_create_rejects_unrecognized_event_dump_before_import(creator, monkeypatch):
    calls = []
    schema = (
        b"/*!50106 CREATE*/ /*!50117 DEFINER=`root`@`%`*/ "
        b"/*!50106 EVENT `unsafe_event` IN AN UNKNOWN FORMAT ENABLE DO SELECT 1 */ ;;"
    )

    def run(command, **kwargs):
        calls.append((command, kwargs))
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=schema if command[0].endswith("mysqldump") else None,
            stderr=b"",
        )

    monkeypatch.setattr(mysql_database_creator, "get_mysql_bin_path", lambda: "")
    monkeypatch.setattr(mysql_database_creator.subprocess, "run", run)

    with pytest.raises(Exception, match="event definitions"):
        creator.create()

    assert len(calls) == 1


def test_teardown_quotes_destination_database(creator, monkeypatch):
    commands = []
    creator.destination_dbc.db_name = "subset`quoted"

    monkeypatch.setattr(
        creator,
        "_run_command",
        lambda executable, args, error_message, **kwargs: commands.append(
            (executable, args)
        ),
    )

    creator.teardown()

    assert commands[0][1][-1] == ("--execute=DROP DATABASE IF EXISTS `subset``quoted`")


def test_command_failure_preserves_stderr_context(creator, monkeypatch):
    monkeypatch.setattr(mysql_database_creator, "get_mysql_bin_path", lambda: "")
    monkeypatch.setattr(
        mysql_database_creator.subprocess,
        "run",
        lambda command, **kwargs: subprocess.CompletedProcess(
            command, 1, stderr=b"Access denied for user"
        ),
    )

    with pytest.raises(Exception, match="Access denied for user"):
        creator.run_query_on_destination("CREATE DATABASE subset")


def test_get_mysql_bin_path_checks_both_executables(monkeypatch, tmp_path):
    mysql_path = str(tmp_path / "mysql tools")
    calls = []

    def run(command, **kwargs):
        calls.append((command, kwargs))
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setenv("MYSQL_PATH", mysql_path)
    monkeypatch.setattr(mysql_database_creator.subprocess, "run", run)

    assert mysql_database_creator.get_mysql_bin_path() == mysql_path
    assert [call[0] for call in calls] == [
        [os.path.join(mysql_path, "mysqldump"), "--help"],
        [os.path.join(mysql_path, "mysql"), "--help"],
    ]
    assert all(
        kwargs == {"stdout": subprocess.DEVNULL, "stderr": subprocess.DEVNULL}
        for _, kwargs in calls
    )
