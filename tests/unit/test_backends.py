"""One delegation contract for both adapters; no database connections involved."""

from dataclasses import FrozenInstanceError, asdict
from types import SimpleNamespace
from unittest.mock import Mock, create_autospec

import pytest

from db_condenser import config_reader, database_helper
from db_condenser.backends import get_backend
from db_condenser.backends.contracts import Backend, SchemaManager
from db_condenser.backends.execution import SqlSelectionExecutor
from db_condenser.backends.mysql import MySqlRunSession
from db_condenser.config_reader import DbType
from db_condenser.subset import Subset


@pytest.fixture(params=list(DbType), ids=lambda value: value.value)
def adapter(request, monkeypatch):
    # Selection is explicit even before global configuration is initialized.
    backend = get_backend(request.param)
    monkeypatch.setattr(config_reader, "config", SimpleNamespace(db_type=request.param))
    return backend, database_helper.get_specific_helper()


def test_backend_contract_and_capabilities(adapter):
    backend, helper = adapter
    assert isinstance(backend, Backend)
    expected = helper.__name__.endswith("psql_database_helper")
    assert all(value is expected for value in asdict(backend.capabilities).values())
    with pytest.raises(FrozenInstanceError):
        backend.capabilities.incremental = not expected


@pytest.mark.parametrize(
    "operation,args,kwargs",
    [
        ("list_all_tables", (Mock(),), {}),
        ("get_unredacted_fk_relationships", (["app.parent"], Mock()), {}),
        ("get_table_columns", ("parent", "app", Mock()), {}),
        ("get_table_datatypes", ("parent", "app", Mock()), {}),
        ("get_table_count_estimate", ("parent", "app", Mock()), {}),
        ("run_query", ("SELECT 1", Mock()), {"commit": False}),
        ("update_sequence_numbering", (Mock(), ["app.parent"]), {}),
        ("turn_off_constraints", (Mock(),), {}),
        ("prep_temp_dbs", (Mock(), Mock()), {}),
        ("unprep_temp_dbs", (Mock(), Mock()), {}),
        ("create_id_temp_table", (Mock(), 2), {}),
    ],
)
def test_operations_delegate_without_transforming_or_owning_connections(
    adapter, monkeypatch, operation, args, kwargs
):
    backend, helper = adapter
    delegated = Mock()
    # Patching after construction must still work for existing failure tests.
    monkeypatch.setattr(helper, operation, delegated)
    assert getattr(backend, operation)(*args, **kwargs) is delegated.return_value
    delegated.assert_called_once_with(*args, **kwargs)
    for arg in args:
        if isinstance(arg, Mock):
            assert arg.mock_calls == []
    failure = RuntimeError("original helper failure")
    delegated.side_effect = failure
    with pytest.raises(RuntimeError) as exc:
        getattr(backend, operation)(*args, **kwargs)
    assert exc.value is failure


@pytest.mark.parametrize("batch_size", [None, 17])
def test_copy_preserves_parameters_and_backend_batch_default(
    adapter, monkeypatch, batch_size
):
    backend, helper = adapter
    delegated = Mock()
    monkeypatch.setattr(helper, "copy_rows", delegated)
    source, destination = Mock(), Mock()
    params = ([1, 2], ["composite", "keys"])
    args = (source, destination, "SELECT * FROM parent", "parent", params)
    backend.copy_rows(*args, batch_size=batch_size)
    expected_kwargs = {} if batch_size is None else {"batch_size": batch_size}
    delegated.assert_called_once_with(*args, **expected_kwargs)
    assert delegated.call_args.args[4] is params
    assert source.mock_calls == destination.mock_calls == []
    failure = RuntimeError("copy failed")
    delegated.side_effect = failure
    with pytest.raises(RuntimeError) as exc:
        backend.copy_rows(*args)
    assert exc.value is failure


def test_schema_factory_reuses_existing_creator(adapter, monkeypatch):
    backend, _ = adapter
    postgres = backend.capabilities.incremental
    module = "psql" if postgres else "mysql"
    creator = "PsqlDatabaseCreator" if postgres else "MySqlDatabaseCreator"
    manager = create_autospec(SchemaManager, instance=True, spec_set=True)
    factory = Mock(return_value=manager)
    monkeypatch.setattr(f"db_condenser.{module}_database_creator.{creator}", factory)
    source, destination = Mock(), Mock()
    assert backend.schema_manager(source, destination) is manager
    factory.assert_called_once_with(source, destination, *([False] if postgres else []))
    assert isinstance(manager, SchemaManager)
    assert manager.mock_calls == []  # factory must not start destructive lifecycle


def test_unknown_backend_is_rejected():
    with pytest.raises(ValueError, match="unknown db_type"):
        get_backend("unsupported")


@pytest.mark.parametrize("fail_copy", [False, True], ids=["success", "failure"])
def test_real_traversal_accepts_a_recording_backend(monkeypatch, fail_copy):
    info = {
        "host": "unused",
        "port": 3306,
        "db_name": "app",
        "user_name": "unused",
        "password": "unused",
    }
    config = config_reader._raw_dict_to_config(
        {
            "db_type": "mysql",
            "source_db_connection_info": info,
            "destination_db_connection_info": info,
            "initial_targets": [{"table": "app.parent", "where": "id = 1"}],
        }
    )
    monkeypatch.setattr(config_reader, "config", config)
    backend = create_autospec(Backend, instance=True, spec_set=True)
    backend.uses_incremental.return_value = False
    backend.selection_executor.side_effect = lambda session, destination, config: (
        SqlSelectionExecutor(backend, Mock(), session, destination, config)
    )
    backend.open_run.side_effect = lambda source, destination, config: MySqlRunSession(
        backend, source, destination, config
    )
    backend.get_unredacted_fk_relationships.return_value = []
    backend.get_table_columns.return_value = ["id", "payload"]
    source, destination = Mock(), Mock()
    source_conn = source.get_db_connection.return_value
    destination_conn = destination.get_db_connection.return_value
    subset = Subset(source, destination, ["app.parent"], backend=backend)
    failure = RuntimeError("transfer interrupted")
    if fail_copy:
        backend.copy_rows.side_effect = failure
    succeeded = False
    try:
        subset.prep_temp_dbs()
        if fail_copy:
            with pytest.raises(RuntimeError) as exc:
                subset.run_middle_out()
            assert exc.value is failure
        else:
            subset.run_middle_out()
            succeeded = True
    finally:
        subset.unprep_temp_dbs(succeeded)
        subset.close_connections()
    backend.turn_off_constraints.assert_called_once_with(destination_conn)
    backend.prep_temp_dbs.assert_called_once_with(source_conn, destination_conn)
    backend.unprep_temp_dbs.assert_called_once_with(source_conn, destination_conn)
    backend.get_unredacted_fk_relationships.assert_called_once_with(
        ["app.parent"], source_conn
    )
    backend.get_table_columns.assert_called_once_with("parent", "app", source_conn)
    backend.copy_rows.assert_called_once()
    args = backend.copy_rows.call_args.args
    assert args[:2] == (source_conn, destination_conn)
    assert args[2] == (
        "SELECT `parent`.`id`,`parent`.`payload` FROM `app`.`parent` WHERE id = 1"
    )
    assert args[3] == "app.parent"
    source_conn.cursor.assert_not_called()
    destination_conn.cursor.assert_not_called()
    source_conn.close.assert_called_once()
    destination_conn.close.assert_called_once()
