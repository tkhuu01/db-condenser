import os
import subprocess
from types import SimpleNamespace

import pytest

from db_condenser import psql_database_creator
from db_condenser.psql_database_creator import PsqlDatabaseCreator


def _db_connect(user, password, host, port, db_name):
    return SimpleNamespace(
        user=user,
        password=password,
        host=host,
        port=port,
        db_name=db_name,
        get_db_connection=lambda: object(),
    )


@pytest.fixture
def creator(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    source = _db_connect("source-user", "source password", "source-db", 5432, "app")
    destination = _db_connect(
        "dest-user", "dest password", "destination-db", 6432, "subset"
    )
    return PsqlDatabaseCreator(source, destination)


def test_create_uses_shared_dump_options_and_filters_comments(creator, monkeypatch):
    calls = []

    def run(command, **kwargs):
        calls.append((command, kwargs))
        if command[0].endswith("pg_dump"):
            return subprocess.CompletedProcess(
                command,
                0,
                stdout=(
                    b"CREATE TABLE public.example (id integer);   \n"
                    b"COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL';\n"
                    b"COMMENT ON CONSTRAINT example_fk ON public.example IS 'FK';\n"
                ),
                stderr=b"",
            )
        return subprocess.CompletedProcess(command, 0, stderr=b"")

    monkeypatch.setattr(
        psql_database_creator, "get_pg_bin_path", lambda: "/opt/postgres/bin"
    )
    monkeypatch.setattr(psql_database_creator.subprocess, "run", run)

    creator.create()

    dump_command, dump_kwargs = calls[0]
    assert dump_command == [
        "/opt/postgres/bin/pg_dump",
        psql_database_creator._connection_argument(creator.source_dbc),
        *psql_database_creator._SCHEMA_DUMP_OPTIONS,
        "--section=pre-data",
    ]
    assert dump_kwargs == {
        "stderr": subprocess.PIPE,
        "stdout": subprocess.PIPE,
    }

    psql_command, psql_kwargs = calls[1]
    assert psql_command == [
        "/opt/postgres/bin/psql",
        psql_database_creator._connection_argument(creator.destination_dbc),
    ]
    assert psql_kwargs == {
        "stderr": subprocess.PIPE,
        "input": b"CREATE TABLE public.example (id integer);\n",
        "stdout": subprocess.DEVNULL,
    }


def test_add_constraints_restores_unfiltered_post_data(creator, monkeypatch):
    calls = []
    post_data = (
        b"ALTER TABLE public.example ADD CONSTRAINT example_pk PRIMARY KEY (id);\n"
    )

    def run(command, **kwargs):
        calls.append((command, kwargs))
        if command[0].endswith("pg_dump"):
            return subprocess.CompletedProcess(command, 0, stdout=post_data, stderr=b"")
        return subprocess.CompletedProcess(command, 0, stderr=b"")

    monkeypatch.setattr(psql_database_creator, "get_pg_bin_path", lambda: "")
    monkeypatch.setattr(psql_database_creator.subprocess, "run", run)

    creator.add_constraints()

    assert calls[0][0][-1] == "--section=post-data"
    assert calls[1][1]["input"] == post_data


def test_existing_dump_mode_skips_schema_commands(creator, monkeypatch):
    creator.use_existing_dump = True

    def unexpected_run(*args, **kwargs):
        pytest.fail("schema commands should not run in existing-dump mode")

    monkeypatch.setattr(psql_database_creator.subprocess, "run", unexpected_run)

    creator.create()
    creator.add_constraints()


def test_run_query_preserves_error_context(creator, monkeypatch):
    query = "DROP SCHEMA public CASCADE"

    monkeypatch.setattr(psql_database_creator, "get_pg_bin_path", lambda: "")
    monkeypatch.setattr(
        psql_database_creator.subprocess,
        "run",
        lambda command, **kwargs: subprocess.CompletedProcess(
            command, 1, stderr=b"ERROR: permission denied"
        ),
    )

    with pytest.raises(Exception) as error:
        creator.run_query(query)
    assert query in str(error.value)
    assert "permission denied" in str(error.value)


def test_get_pg_bin_path_checks_executable_without_changing_directory(
    tmp_path, monkeypatch
):
    postgres_path = str(tmp_path / "postgres tools")
    start_directory = os.getcwd()
    calls = []

    def run(command, **kwargs):
        calls.append((command, kwargs))
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setenv("POSTGRES_PATH", postgres_path)
    monkeypatch.setattr(psql_database_creator.subprocess, "run", run)

    assert psql_database_creator.get_pg_bin_path() == postgres_path
    assert calls == [
        (
            [os.path.join(postgres_path, "pg_dump"), "--help"],
            {"stdout": subprocess.DEVNULL, "stderr": subprocess.DEVNULL},
        )
    ]
    assert os.getcwd() == start_directory
