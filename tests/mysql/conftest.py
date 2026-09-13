"""Opt-in MySQL fixtures; use a disposable server with CREATE DATABASE access."""

import os
import uuid

import mysql.connector
import pytest

from db_condenser import config_reader, mysql_database_helper
from db_condenser.db_connect import DbConnect
from db_condenser.runner import _subset_run
from db_condenser.subset import Subset


@pytest.fixture
def mysql_case(monkeypatch):
    if not os.environ.get("MYSQL_HOST"):
        pytest.skip("set MYSQL_HOST to a disposable MySQL service")
    info = dict(
        host=os.environ["MYSQL_HOST"],
        port=int(os.environ.get("MYSQL_PORT", "3306")),
        user=os.environ.get("MYSQL_USER", "root"),
        password=os.environ.get("MYSQL_PASSWORD", "test"),
    )
    prefix = "condenser_baseline_" + uuid.uuid4().hex
    names = [prefix + suffix for suffix in ("_src", "_dst", "_scratch")]
    admin = mysql.connector.connect(**info, autocommit=True)
    created, connections = [], []
    try:
        with admin.cursor() as cur:
            for name in names[:2]:
                cur.execute(f"CREATE DATABASE `{name}`")
                created.append(name)
        for name in names[:2]:
            conn = mysql.connector.connect(**info, database=name, autocommit=True)
            connections.append(conn)
            with conn.cursor() as cur:
                cur.execute(
                    "CREATE TABLE parent (id bigint PRIMARY KEY, selected boolean NOT NULL)"
                )
                cur.execute("""
                    CREATE TABLE child (id bigint PRIMARY KEY, parent_id bigint,
                        payload text, FOREIGN KEY (parent_id) REFERENCES parent(id))
                """)
        with connections[0].cursor() as cur:
            cur.execute("INSERT INTO parent VALUES (1,true),(2,false)")
            cur.execute("INSERT INTO child VALUES (10,1,'accepted'),(20,2,'rejected')")
        connect = {
            "user_name": info["user"],
            **{k: v for k, v in info.items() if k != "user"},
        }
        config = config_reader._raw_dict_to_config(
            {
                "db_type": "mysql",
                "source_db_connection_info": {**connect, "db_name": names[0]},
                "destination_db_connection_info": {**connect, "db_name": names[1]},
                "initial_targets": [
                    {"table": names[0] + ".parent", "where": "selected"}
                ],
            }
        )
        monkeypatch.setattr(config_reader, "config", config)
        # The legacy backend uses a fixed scratch database. Isolate only its
        # name so this test can never drop another run's scratch data.
        monkeypatch.setattr(mysql_database_helper, "temp_db", names[2])
        yield connections, config
    finally:
        for conn in connections:
            conn.close()
        with admin.cursor() as cur:
            for name in [names[2], *reversed(created)]:
                cur.execute(f"DROP DATABASE IF EXISTS `{name}`")
        admin.close()


@pytest.fixture
def mysql_run(mysql_case):
    return lambda **kwargs: run_case(mysql_case[1], **kwargs)


def run_case(config, *, relationships=False):
    source = DbConnect(config.db_type, config.source_db_connection_info)
    destination = DbConnect(config.db_type, config.destination_db_connection_info)
    tables = [source.db_name + ".parent"]
    if relationships:
        tables.append(source.db_name + ".child")
    subset = Subset(source, destination, tables)
    with _subset_run(subset):
        pass
