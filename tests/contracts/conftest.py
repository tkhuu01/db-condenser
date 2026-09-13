"""Disposable databases; backend differences belong in setup, not assertions."""

import os
import uuid
from types import SimpleNamespace

import mysql.connector
import psycopg
import pytest

from db_condenser import config_reader
from db_condenser.backends import get_backend
from db_condenser.config_reader import DbType
from db_condenser.db_connect import DbConnect


@pytest.fixture(
    params=[
        pytest.param(DbType.POSTGRES, marks=pytest.mark.postgres, id="postgres"),
        pytest.param(DbType.MYSQL, marks=pytest.mark.mysql, id="mysql"),
    ]
)
def backend_case(request, monkeypatch):
    db_type = request.param
    postgres = db_type == DbType.POSTGRES
    if not postgres and not os.environ.get("MYSQL_HOST"):
        pytest.skip("set MYSQL_HOST to a disposable MySQL service")
    prefix = "POSTGRES" if postgres else "MYSQL"
    info = {
        "host": os.environ.get(prefix + "_HOST", "localhost"),
        "port": int(os.environ.get(prefix + "_PORT", "5432" if postgres else "3306")),
        "user": os.environ.get(prefix + "_USER", "test" if postgres else "root"),
        "password": os.environ.get(prefix + "_PASSWORD", "test"),
    }
    admin = (
        psycopg.connect(**info, dbname="postgres", autocommit=True)
        if postgres
        else mysql.connector.connect(**info, autocommit=True)
    )
    # Only these UUID-owned databases may be dropped. Never use configured DB names.
    base = "condenser_contract_" + uuid.uuid4().hex
    names = [base + suffix for suffix in ("_src", "_dst")]
    quote = '"' if postgres else "`"
    created, connections = [], []
    try:
        with admin.cursor() as cur:
            for name in names:
                cur.execute(f"CREATE DATABASE {quote}{name}{quote}")
                created.append(name)
        raw_info = {
            "user_name": info["user"],
            **{key: value for key, value in info.items() if key != "user"},
        }
        config = config_reader._raw_dict_to_config(
            {
                "db_type": db_type.value,
                "source_db_connection_info": {**raw_info, "db_name": names[0]},
                "destination_db_connection_info": {**raw_info, "db_name": names[1]},
                "initial_targets": [],
            }
        )
        monkeypatch.setattr(config_reader, "config", config)

        def connect(connection_info):
            conn = DbConnect(db_type, connection_info).get_db_connection()
            connections.append(conn)
            return conn

        source = connect(config.source_db_connection_info)
        destination = connect(config.destination_db_connection_info)
        for conn in (source, destination):
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE parent (tenant integer, code varchar(64),
                        PRIMARY KEY (tenant, code))
                """)
                cur.execute("""
                    CREATE TABLE child (id integer PRIMARY KEY, parent_tenant integer,
                        parent_code varchar(64), payload varchar(64),
                        FOREIGN KEY (parent_tenant, parent_code) REFERENCES parent(tenant, code))
                """)
                cur.execute("INSERT INTO parent VALUES (1,'alpha'),(2,'beta')")
            conn.commit()
        with source.cursor() as cur:
            cur.execute(
                "INSERT INTO child VALUES (10,1,'alpha','accepted'),(20,2,'beta','rejected')"
            )
        source.commit()
        yield SimpleNamespace(
            backend=get_backend(db_type),
            source=source,
            destination=destination,
            source_schema="public" if postgres else names[0],
            destination_schema="public" if postgres else names[1],
            source_factory=SimpleNamespace(
                db_name=names[0],
                get_db_connection=lambda: DbConnect(
                    db_type, config.source_db_connection_info
                ).get_db_connection(),
            ),
            observe_destination=lambda: connect(config.destination_db_connection_info),
        )
    finally:
        for conn in reversed(connections):
            conn.close()
        with admin.cursor() as cur:
            for name in reversed(created):
                cur.execute(f"DROP DATABASE {quote}{name}{quote}")
        admin.close()
