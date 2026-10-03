"""Opt-in, synthetic PostgreSQL baseline, not a timing assertion.

Run: pytest tests/performance --run-performance -s
BASELINE_ROWS defaults to 100,000 total source rows (maximum 1,000,000).
BASELINE_REPEATS defaults to 3. BASELINE_PAYLOAD_BYTES defaults to 256.
Each measured run uses a fresh process. JSON reports include Python peak RSS,
explicit execute/COPY call counts, and actual outgoing COPY rows/bytes (not
total wire traffic). Fixture setup, validation, and schema tools are not timed.
Server-cursor FETCH commands and protocol messages are not execute-call counts.
For XML measurements, add --junitxml=baseline.xml -o junit_family=xunit1.
"""

import json
import os
import subprocess
import sys
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

import psycopg
import pytest
from psycopg import sql

from db_condenser import config_reader
from db_condenser.db_connect import DbConnect
from db_condenser.runner import _subset_run
from db_condenser.subset import Subset


def connection_info():
    return dict(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        user=os.environ.get("POSTGRES_USER", "test"),
        password=os.environ.get("POSTGRES_PASSWORD", "test"),
    )


def worker(source_name, destination_name, mode):
    # resource is POSIX-only; keep normal test collection portable.
    import resource

    info = connection_info()
    connect = {"user_name": info.pop("user"), **info}
    config_reader.config = config_reader._raw_dict_to_config(
        {
            "db_type": "postgres",
            "source_db_connection_info": {**connect, "db_name": source_name},
            "destination_db_connection_info": {**connect, "db_name": destination_name},
            "initial_targets": [{"table": "public.parent", "where": "selected"}],
            "destination_mode": mode,
        }
    )
    counts = {
        "source_execute_calls": 0,
        "destination_execute_calls": 0,
        "copy_calls": 0,
        "source_copy_rows": 0,
        "source_copy_bytes": 0,
    }

    def instrument_execute(original):
        def execute(cursor, *args, **kwargs):
            role = (
                "source"
                if cursor.connection.info.dbname == source_name
                else "destination"
            )
            counts[role + "_execute_calls"] += 1
            return original(cursor, *args, **kwargs)

        return execute

    for cursor_class in (psycopg.Cursor, psycopg.ServerCursor):
        cursor_class.execute = instrument_execute(cursor_class.execute)
    original_copy = psycopg.Cursor.copy

    @contextmanager
    def measured_copy(cursor, statement, *args, **kwargs):
        counts["copy_calls"] += 1
        with original_copy(cursor, statement, *args, **kwargs) as stream:
            if (
                cursor.connection.info.dbname == source_name
                and "TO STDOUT" in statement
            ):

                def rows():
                    for data in stream:
                        counts["source_copy_rows"] += 1
                        counts["source_copy_bytes"] += len(data)
                        yield data

                yield rows()
            else:
                yield stream

    psycopg.Cursor.copy = measured_copy
    config = config_reader.get_config()
    started = time.perf_counter()
    subset = Subset(
        DbConnect(config.db_type, config.source_db_connection_info),
        DbConnect(config.db_type, config.destination_db_connection_info),
        ["public.parent", "public.child"],
    )
    with _subset_run(subset):
        pass
    if sys.platform.startswith("linux"):
        # ru_maxrss can inherit the pytest parent's high-water mark at fork;
        # /proc reports this worker's post-exec address space instead.
        rss_bytes = next(
            int(line.split()[1]) * 1024
            for line in Path("/proc/self/status").read_text().splitlines()
            if line.startswith("VmHWM:")
        )
    else:
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        rss_bytes = rss if sys.platform == "darwin" else rss * 1024
    print(
        json.dumps(
            {
                "mode": mode,
                "seconds": time.perf_counter() - started,
                "peak_python_rss_bytes": rss_bytes,
                **counts,
            }
        )
    )


def copy_fixture(source, destination, table, condition):
    with source.cursor() as src, destination.cursor() as dst:
        with src.copy(
            f"COPY (SELECT * FROM {table} WHERE {condition}) TO STDOUT"
        ) as outgoing:
            with dst.copy(f"COPY {table} FROM STDIN") as incoming:
                for data in outgoing:
                    incoming.write(data)
    destination.commit()


def assert_exact_rows(source, destination, table, expected_query):
    with (
        source.cursor(name="expected_" + table) as src,
        destination.cursor(name="actual_" + table) as dst,
    ):
        src.execute(expected_query)
        dst.execute(f"SELECT * FROM {table} ORDER BY id")
        while True:
            expected, actual = src.fetchmany(1000), dst.fetchmany(1000)
            assert actual == expected
            if not expected:
                break
    destination.commit()
    source.commit()


@pytest.mark.parametrize("mode", ["recreate", "topup", "grow"])
def test_postgres_baseline(mode, tmp_path, record_property):
    if sys.platform == "win32":
        pytest.skip("peak RSS measurement requires POSIX resource")
    total = int(os.environ.get("BASELINE_ROWS", "100000"))
    repeats = int(os.environ.get("BASELINE_REPEATS", "3"))
    payload = int(os.environ.get("BASELINE_PAYLOAD_BYTES", "256"))
    if (
        not 1000 <= total <= 1_000_000
        or not 1 <= repeats <= 5
        or not 0 <= payload <= 1024
    ):
        pytest.fail(
            "baseline requires 1000..1M source rows, 1..5 repeats, 0..1024 payload bytes"
        )
    parents = total // 10
    children = total - parents - 1
    selected = parents // 10
    resident = selected // 2
    names = ["condenser_perf_" + uuid.uuid4().hex for _ in range(2)]
    created = []
    info = connection_info()
    results = []
    try:
        with psycopg.connect(**info, dbname="postgres", autocommit=True) as admin:
            for name in names:
                admin.execute(
                    sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name))
                )
                created.append(name)
        with (
            psycopg.connect(**info, dbname=names[0]) as source,
            psycopg.connect(**info, dbname=names[1]) as destination,
        ):
            for conn in (source, destination):
                conn.execute("""
                    CREATE TABLE parent (id bigint PRIMARY KEY, selected boolean NOT NULL);
                    CREATE TABLE child (id bigint PRIMARY KEY, left_id bigint REFERENCES parent(id),
                        right_id bigint REFERENCES parent(id), payload text);
                    CREATE INDEX ON child(left_id); CREATE INDEX ON child(right_id);
                """)
                conn.commit()
            source.execute(
                f"INSERT INTO parent SELECT i, i <= {selected} FROM generate_series(1,{parents}) i"
            )
            source.execute(f"""
                INSERT INTO child SELECT i, i % {parents} + 1,
                    (i + {resident // 2}) % {parents} + 1, repeat('x',{payload})
                FROM generate_series(1,{children}) i;
                ANALYZE;
            """)
            source.execute(
                sql.SQL(
                    "ALTER DATABASE {} SET default_transaction_read_only=on"
                ).format(sql.Identifier(names[0]))
            )
            source.commit()
            version = source.execute("SHOW server_version").fetchone()[0]
            for repeat in range(repeats):
                destination.execute("TRUNCATE child, parent")
                destination.commit()
                source.execute(f"DELETE FROM child WHERE id = {children + 1}")
                source.execute(
                    "UPDATE child SET payload = repeat('x',%s) WHERE id=1", (payload,)
                )
                source.commit()
                if mode != "recreate":
                    copy_fixture(source, destination, "parent", f"id <= {resident}")
                    copy_fixture(
                        source,
                        destination,
                        "child",
                        f"left_id <= {resident} AND right_id <= {resident}",
                    )
                # A new child of old parents is frozen by topup, but grow must
                # import it and refresh the changed payload of an existing row.
                source.execute(
                    f"INSERT INTO child VALUES ({children + 1},1,2,'new child')"
                )
                source.execute("UPDATE child SET payload='updated' WHERE id=1")
                source.commit()
                destination.execute("ANALYZE")
                destination.commit()
                result = subprocess.run(
                    [
                        sys.executable,
                        str(Path(__file__).resolve()),
                        "--worker",
                        *names,
                        mode,
                    ],
                    cwd=tmp_path,
                    text=True,
                    capture_output=True,
                    timeout=600,
                )
                assert result.returncode == 0, result.stdout + result.stderr
                measurement = json.loads(result.stdout.splitlines()[-1])
                expected_payload = (
                    f"CASE WHEN id=1 THEN repeat('x',{payload}) ELSE payload END"
                    if mode == "topup"
                    else "payload"
                )
                exclude_new = f" AND id <= {children}" if mode == "topup" else ""
                assert_exact_rows(
                    source,
                    destination,
                    "parent",
                    "SELECT * FROM parent WHERE selected ORDER BY id",
                )
                assert_exact_rows(
                    source,
                    destination,
                    "child",
                    f"SELECT id,left_id,right_id,{expected_payload} FROM child WHERE left_id <= {selected} AND right_id <= {selected}{exclude_new} ORDER BY id",
                )
                measurement.update(
                    repeat=repeat + 1,
                    source_rows=total,
                    payload_bytes=payload,
                    postgres_version=version,
                )
                results.append(measurement)
                print(json.dumps(measurement), flush=True)
        record_property("baseline_measurements", json.dumps(results))
    finally:
        with psycopg.connect(**info, dbname="postgres", autocommit=True) as admin:
            for name in created:
                admin.execute(
                    sql.SQL("DROP DATABASE {} WITH (FORCE)").format(
                        sql.Identifier(name)
                    )
                )


if __name__ == "__main__":
    worker(*sys.argv[2:])
