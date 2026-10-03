"""Identical assertions for the behavior currently shared by both backends.

Backend-specific features and known relationship traversal failures stay in
their existing integration suites; this is not a claim of feature parity.
"""

import pytest

from db_condenser.config_reader import DbType, InitialTarget, get_config
from db_condenser.db_connect import DbConnect

pytestmark = pytest.mark.integration


def rows(connection):
    with connection.cursor() as cur:
        cur.execute("SELECT * FROM child ORDER BY id")
        return cur.fetchall()


def test_metadata_preserves_composite_relationship_order(backend_case):
    case = backend_case
    tables = [case.source_schema + "." + name for name in ("parent", "child")]
    assert set(case.backend.list_all_tables(case.source_factory)) == set(tables)
    assert case.backend.get_unredacted_fk_relationships(tables, case.source) == [
        {
            "fk_table": tables[1],
            "fk_columns": ["parent_tenant", "parent_code"],
            "target_table": tables[0],
            "target_columns": ["tenant", "code"],
        }
    ]
    columns = ["id", "parent_tenant", "parent_code", "payload"]
    assert (
        case.backend.get_table_columns("child", case.source_schema, case.source)
        == columns
    )
    datatypes = case.backend.get_table_datatypes(
        "child", case.source_schema, case.source
    )
    assert [column[0] for column in datatypes] == columns
    assert all(isinstance(column, tuple) and len(column) == 4 for column in datatypes)
    assert all(column[1] and column[2:] == ("", "") for column in datatypes)
    assert isinstance(
        case.backend.get_table_count_estimate("child", case.source_schema, case.source),
        int,
    )
    assert len(rows(case.source)) == 2  # borrowed connection still usable


@pytest.mark.parametrize(
    "batch_size", [None, 1], ids=["default_batch", "single_row_batch"]
)
def test_copy_filters_and_commits_destination_not_source(backend_case, batch_size):
    case = backend_case
    with case.source.cursor() as cur:
        cur.execute("INSERT INTO child VALUES (30,1,'alpha','uncommitted')")
    case.backend.copy_rows(
        case.source,
        case.destination,
        "SELECT * FROM child WHERE parent_tenant = %s ORDER BY id",
        case.destination_schema + ".child",
        params=(1,),
        batch_size=batch_size,
    )
    case.source.connection.rollback()
    assert rows(case.source) == [
        (10, 1, "alpha", "accepted"),
        (20, 2, "beta", "rejected"),
    ]
    expected = [(10, 1, "alpha", "accepted"), (30, 1, "alpha", "uncommitted")]
    assert rows(case.destination) == expected
    assert (
        rows(case.observe_destination()) == expected
    )  # commit is visible outside the session
    case.backend.copy_rows(
        case.source,
        case.destination,
        "SELECT * FROM child WHERE id = %s",
        case.destination_schema + ".child",
        params=(-1,),
    )
    assert rows(case.destination) == expected  # empty selection is harmless


def test_hook_respects_commit_flag(backend_case):
    case = backend_case
    case.backend.run_query(
        "INSERT INTO child VALUES (40,1,'alpha','rolled back')",
        case.destination,
        commit=False,
    )
    case.destination.connection.rollback()
    assert rows(case.destination) == []
    case.backend.run_query(
        "INSERT INTO child VALUES (50,1,'alpha','committed')", case.destination
    )
    assert rows(case.observe_destination()) == [(50, 1, "alpha", "committed")]


def test_selection_executor_filters_at_source(backend_case):
    case = backend_case
    config = get_config()
    source = DbConnect(config.db_type, config.source_db_connection_info)
    destination = DbConnect(config.db_type, config.destination_db_connection_info)
    session = case.backend.open_run(source, destination, config)
    try:
        executor = case.backend.selection_executor(session, destination, config)
        executor.load_pre_filters()
        executor.select_direct(
            InitialTarget(
                table=case.source_schema + ".child", where="parent_tenant = 1"
            ),
            [],
        )
        assert rows(case.observe_destination()) == [(10, 1, "alpha", "accepted")]
        assert rows(session.source) == [
            (10, 1, "alpha", "accepted"),
            (20, 2, "beta", "rejected"),
        ]
    finally:
        session.close()


def test_postgres_run_readers_keep_snapshot_after_committed_source_write(backend_case):
    config = get_config()
    if config.db_type != DbType.POSTGRES:
        pytest.skip("shared snapshots are PostgreSQL-specific")
    config.parallel_read_workers = 2
    case = backend_case
    session = case.backend.open_run(
        DbConnect(config.db_type, config.source_db_connection_info),
        DbConnect(config.db_type, config.destination_db_connection_info),
        config,
    )
    try:
        before = rows(session.source)
        with case.source.cursor() as cur:
            cur.execute("INSERT INTO child VALUES (30,1,'alpha','committed later')")
        case.source.commit()
        assert len(rows(case.source)) == 3
        assert rows(session.source) == before
        for reader in session.source_pool:
            assert rows(reader) == before
        # A worker opened after the write must still import the original view.
        worker = session.open_source_connection()
        try:
            assert rows(worker) == before
        finally:
            worker.close()
    finally:
        session.close()
