"""Run ownership, shared snapshots, and incremental failure retention."""

from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, call

import pytest

from db_condenser import psql_database_helper
from db_condenser.backends import get_backend
from db_condenser.config_reader import DbType


def connection(rows=None):
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value.fetchone.return_value = rows
    return conn


def open_run(monkeypatch, db_type=DbType.POSTGRES, *, workers=1, temp=False):
    backend = get_backend(db_type)
    monkeypatch.setattr(backend, "turn_off_constraints", Mock())
    monkeypatch.setattr(backend, "prep_temp_dbs", Mock())
    monkeypatch.setattr(backend, "unprep_temp_dbs", Mock())
    source = Mock()
    source.get_db_connection.side_effect = lambda **kwargs: connection(("snapshot",))
    destination = Mock()
    destination.get_db_connection.return_value = connection()
    config = SimpleNamespace(parallel_read_workers=workers, use_temp_tables=temp)
    return backend.open_run(source, destination, config)


def test_postgres_workers_share_snapshot_without_source_commit(monkeypatch):
    session = open_run(monkeypatch, workers=3)
    assert len(session.source_pool) == 3
    session.source.cursor.return_value.__enter__.return_value.execute.assert_called_once_with(
        "SELECT pg_export_snapshot()"
    )
    worker = session.open_source_connection()
    for conn in [*session.source_pool, worker]:
        conn.cursor.return_value.__enter__.return_value.execute.assert_called_once_with(
            "SET TRANSACTION SNAPSHOT 'snapshot'"
        )
        conn.commit.assert_not_called()
    session.prepare()
    session.finish(True)
    session.source.commit.assert_not_called()
    session.close()
    session.close()  # release owned connections only once
    for conn in [session.source, session.destination, *session.source_pool]:
        conn.close.assert_called_once_with()
    worker.close.assert_not_called()  # additional workers belong to the caller
    worker.close()


def test_mysql_session_does_not_export_snapshot_or_create_reader_pool(monkeypatch):
    session = open_run(monkeypatch, DbType.MYSQL, workers=4)
    assert session.source_pool == []
    session.source.cursor.assert_not_called()
    session.prepare()
    session.finish(False)
    session.backend.prep_temp_dbs.assert_called_once_with(
        session.source, session.destination
    )
    session.backend.unprep_temp_dbs.assert_called_once_with(
        session.source, session.destination
    )
    session.close()


@pytest.mark.parametrize(
    "db_type, result, message",
    [
        (DbType.POSTGRES, (True, True), "read replica"),
        (DbType.POSTGRES, (False, False), "TEMP privilege"),
        (DbType.MYSQL, (True,), "read-only"),
    ],
)
def test_unwritable_source_closes_startup_connections(
    monkeypatch, db_type, result, message
):
    backend = get_backend(db_type)
    monkeypatch.setattr(backend, "turn_off_constraints", Mock())
    source, destination = Mock(), Mock()
    source.get_db_connection.return_value = connection(result)
    destination.get_db_connection.return_value = connection()
    with pytest.raises(RuntimeError, match=message):
        backend.open_run(
            source,
            destination,
            SimpleNamespace(use_temp_tables=True, parallel_read_workers=1),
        )
    source.get_db_connection.return_value.close.assert_called_once_with()
    destination.get_db_connection.return_value.close.assert_called_once_with()


def test_failed_destination_open_closes_source(monkeypatch):
    backend = get_backend(DbType.POSTGRES)
    source, destination = Mock(), Mock()
    failure = RuntimeError("destination unavailable")
    destination.get_db_connection.side_effect = failure
    with pytest.raises(RuntimeError) as exc:
        backend.open_run(source, destination, SimpleNamespace())
    assert exc.value is failure
    source.get_db_connection.return_value.close.assert_called_once_with()


def test_failed_pool_snapshot_import_closes_all_opened_connections(monkeypatch):
    backend = get_backend(DbType.POSTGRES)
    monkeypatch.setattr(backend, "turn_off_constraints", Mock())
    source, destination = Mock(), Mock()
    main, first, failed = connection(("snapshot",)), connection(), connection()
    source.get_db_connection.side_effect = [main, first, failed]
    failure = RuntimeError("snapshot import failed")
    failed.cursor.return_value.__enter__.return_value.execute.side_effect = failure
    with pytest.raises(RuntimeError) as exc:
        backend.open_run(
            source,
            destination,
            SimpleNamespace(use_temp_tables=False, parallel_read_workers=2),
        )
    assert exc.value is failure
    for conn in [main, first, failed, destination.get_db_connection.return_value]:
        conn.close.assert_called_once_with()


@pytest.fixture
def incremental_helpers(monkeypatch):
    helpers = Mock()
    for name in (
        "prep_incremental",
        "drop_fk_constraints",
        "restore_fk_constraints",
        "unprep_incremental",
        "retain_incremental",
    ):
        monkeypatch.setattr(psql_database_helper, name, getattr(helpers, name))
    helpers.drop_fk_constraints.return_value = [("public", "child", "fk", "definition")]
    return helpers


@pytest.mark.parametrize("succeeded", [True, False])
def test_incremental_restores_fks_before_dropping_or_retaining_journal(
    monkeypatch, incremental_helpers, succeeded
):
    session = open_run(monkeypatch)
    helpers = incremental_helpers
    helpers.attach_mock(session.destination.connection.rollback, "rollback")
    session.prepare_incremental(["public.parent"])
    session.finish(succeeded)
    assert helpers.mock_calls == [
        call.prep_incremental(session.source, session.destination, ["public.parent"]),
        call.drop_fk_constraints(session.destination, ["public.parent"]),
        call.rollback(),
        call.restore_fk_constraints(
            session.destination, helpers.drop_fk_constraints.return_value
        ),
        (
            call.unprep_incremental(session.destination)
            if succeeded
            else call.retain_incremental(session.destination)
        ),
    ]
    session.finish(succeeded)
    helpers.restore_fk_constraints.assert_called_once()
    session.close()


@pytest.mark.parametrize(
    "failure", [RuntimeError("FK restoration failed"), KeyboardInterrupt()]
)
def test_failed_restoration_retains_journal_and_propagates(
    monkeypatch, incremental_helpers, failure
):
    session = open_run(monkeypatch)
    helpers = incremental_helpers
    helpers.restore_fk_constraints.side_effect = failure
    session.prepare_incremental(["public.parent"])
    with pytest.raises(type(failure)) as exc:
        session.finish(True)
    assert exc.value is failure
    helpers.unprep_incremental.assert_not_called()
    helpers.retain_incremental.assert_called_once_with(session.destination)
    assert session.destination.connection.rollback.call_count == 2
    session.close()


def test_failed_fk_drop_still_finishes_prepared_journal(
    monkeypatch, incremental_helpers
):
    session = open_run(monkeypatch)
    helpers = incremental_helpers
    helpers.drop_fk_constraints.side_effect = RuntimeError("drop failed")
    with pytest.raises(RuntimeError, match="drop failed"):
        session.prepare_incremental(["public.parent"])
    session.finish(False)
    helpers.retain_incremental.assert_called_once_with(session.destination)
    helpers.unprep_incremental.assert_not_called()
    session.close()


def test_failed_incremental_preparation_does_not_restore_or_drop_journal(
    monkeypatch, incremental_helpers
):
    session = open_run(monkeypatch)
    helpers = incremental_helpers
    helpers.prep_incremental.side_effect = RuntimeError("preflight failed")
    with pytest.raises(RuntimeError, match="preflight failed"):
        session.prepare_incremental(["public.parent"])
    session.finish(False)
    helpers.drop_fk_constraints.assert_not_called()
    helpers.restore_fk_constraints.assert_not_called()
    helpers.unprep_incremental.assert_not_called()
    helpers.retain_incremental.assert_not_called()
    session.close()


def test_close_attempts_remaining_connections_after_driver_error(monkeypatch):
    session = open_run(monkeypatch, workers=2)
    session.source.close.side_effect = RuntimeError("close failed")
    with pytest.raises(RuntimeError, match="close failed"):
        session.close()
    for conn in [session.destination, *session.source_pool]:
        conn.close.assert_called_once_with()
