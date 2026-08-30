from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from db_condenser import config_reader, db_connect, direct_subset
from db_condenser.config_reader import Config, DbConnectInfo, DbType, InitialTarget


def test_mysql_server_connection_does_not_select_destination_database(monkeypatch):
    connect = Mock(return_value=Mock())
    monkeypatch.setattr(db_connect.mysql.connector, "connect", connect)
    connection_info = DbConnectInfo(
        user_name="test",
        password="test",
        host="destination",
        port=3306,
        db_name="missing_database",
    )

    dbc = db_connect.DbConnect(DbType.MYSQL, connection_info)
    connection = dbc.get_server_connection()

    connect.assert_called_once_with(
        host="destination",
        port=3306,
        user="test",
        password="test",
    )
    connection.close()


def test_recreate_validates_mysql_source_before_teardown(monkeypatch):
    connection_info = DbConnectInfo(
        user_name="test",
        password="test",
        host="localhost",
        port=3306,
        db_name="app",
    )
    config = Config(
        db_type=DbType.MYSQL,
        initial_targets=[InitialTarget(table="app.customers", where="TRUE")],
        source_db_connection_info=connection_info,
        destination_db_connection_info=connection_info,
    )
    args = SimpleNamespace(
        help_config=False,
        example_config=False,
        config=None,
        verbose=False,
        yes=True,
        no_constraints=False,
    )
    source_connection = Mock()
    source_dbc = Mock()
    source_dbc.get_db_connection.return_value = source_connection
    destination_dbc = Mock()
    database = Mock()
    helper = Mock()
    helper.validate_supported_version.side_effect = RuntimeError(
        "MySQL 8.4 LTS or newer is required"
    )
    helper.list_all_tables.return_value = []

    monkeypatch.setattr(direct_subset, "_parse_args", lambda: args)
    monkeypatch.setattr(config_reader, "initialize", lambda _file: None)
    monkeypatch.setattr(config_reader, "get_config", lambda: config)
    monkeypatch.setattr(
        direct_subset,
        "DbConnect",
        Mock(side_effect=[source_dbc, destination_dbc]),
    )
    monkeypatch.setattr(direct_subset, "db_creator", Mock(return_value=database))
    monkeypatch.setattr(
        direct_subset.database_helper, "get_specific_helper", lambda: helper
    )
    monkeypatch.setattr(
        direct_subset,
        "Subset",
        Mock(side_effect=RuntimeError("MySQL 8.4 LTS or newer is required")),
    )

    with pytest.raises(RuntimeError, match="MySQL 8.4"):
        direct_subset.main()

    database.teardown.assert_not_called()
    source_connection.close.assert_called_once_with()


def test_recreate_validates_mysql_destination_server_before_teardown(monkeypatch):
    connection_info = DbConnectInfo(
        user_name="test",
        password="test",
        host="localhost",
        port=3306,
        db_name="app",
    )
    config = Config(
        db_type=DbType.MYSQL,
        initial_targets=[InitialTarget(table="app.customers", where="TRUE")],
        source_db_connection_info=connection_info,
        destination_db_connection_info=connection_info,
    )
    args = SimpleNamespace(
        help_config=False,
        example_config=False,
        config=None,
        verbose=False,
        yes=True,
        no_constraints=False,
    )
    source_connection = Mock()
    destination_connection = Mock()
    source_dbc = Mock()
    source_dbc.get_db_connection.return_value = source_connection
    destination_dbc = Mock()
    destination_dbc.get_server_connection.return_value = destination_connection
    database = Mock()
    helper = Mock()
    helper.validate_supported_version.side_effect = [
        None,
        RuntimeError("MySQL 8.4 LTS or newer is required"),
    ]
    helper.list_all_tables.return_value = []

    monkeypatch.setattr(direct_subset, "_parse_args", lambda: args)
    monkeypatch.setattr(config_reader, "initialize", lambda _file: None)
    monkeypatch.setattr(config_reader, "get_config", lambda: config)
    monkeypatch.setattr(
        direct_subset,
        "DbConnect",
        Mock(side_effect=[source_dbc, destination_dbc]),
    )
    monkeypatch.setattr(direct_subset, "db_creator", Mock(return_value=database))
    monkeypatch.setattr(
        direct_subset.database_helper, "get_specific_helper", lambda: helper
    )
    monkeypatch.setattr(
        direct_subset,
        "Subset",
        Mock(side_effect=RuntimeError("MySQL 8.4 LTS or newer is required")),
    )

    with pytest.raises(RuntimeError, match="MySQL 8.4"):
        direct_subset.main()

    database.teardown.assert_not_called()
    source_connection.close.assert_called_once_with()
    destination_connection.close.assert_called_once_with()


@pytest.mark.parametrize("run_fails", [False, True])
def test_mysql_events_are_enabled_only_after_a_successful_run(monkeypatch, run_fails):
    connection_info = DbConnectInfo(
        user_name="test",
        password="test",
        host="localhost",
        port=3306,
        db_name="app",
    )
    config = Config(
        db_type=DbType.MYSQL,
        initial_targets=[InitialTarget(table="app.customers", where="TRUE")],
        source_db_connection_info=connection_info,
        destination_db_connection_info=connection_info,
    )
    args = SimpleNamespace(
        help_config=False,
        example_config=False,
        config=None,
        verbose=False,
        yes=True,
        no_constraints=False,
    )
    source_dbc = Mock()
    destination_dbc = Mock()
    database = Mock()
    helper = Mock()
    helper.list_all_tables.return_value = []
    subsetter = Mock()
    if run_fails:
        subsetter.run_middle_out.side_effect = RuntimeError("copy failed")

    monkeypatch.setattr(direct_subset, "_parse_args", lambda: args)
    monkeypatch.setattr(config_reader, "initialize", lambda _file: None)
    monkeypatch.setattr(config_reader, "get_config", lambda: config)
    monkeypatch.setattr(
        direct_subset,
        "DbConnect",
        Mock(side_effect=[source_dbc, destination_dbc]),
    )
    monkeypatch.setattr(direct_subset, "db_creator", Mock(return_value=database))
    monkeypatch.setattr(
        direct_subset.database_helper, "get_specific_helper", lambda: helper
    )
    monkeypatch.setattr(direct_subset, "Subset", Mock(return_value=subsetter))
    monkeypatch.setattr(direct_subset, "MySqlConnection", object)
    monkeypatch.setattr(direct_subset.result_tabulator, "tabulate", Mock())

    if run_fails:
        with pytest.raises(RuntimeError, match="copy failed"):
            direct_subset.main()
        database.enable_events.assert_not_called()
    else:
        direct_subset.main()
        database.enable_events.assert_called_once_with()
