"""Connections opened outside the selection session still have an owner."""

from unittest.mock import MagicMock, Mock

import pytest

from db_condenser import (
    database_helper,
    mysql_database_helper,
    psql_database_helper,
    result_tabulator,
)
from db_condenser.psql_database_creator import PsqlDatabaseCreator


@pytest.mark.parametrize("helper", [psql_database_helper, mysql_database_helper])
@pytest.mark.parametrize("failure", [False, True])
def test_table_discovery_closes_its_connection(helper, failure):
    factory = Mock()
    connection = factory.get_db_connection.return_value = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchall.return_value = [("public.parent",)]
    if failure:
        cursor.execute.side_effect = RuntimeError("query failure")
        with pytest.raises(RuntimeError, match="query failure"):
            helper.list_all_tables(factory)
    else:
        assert helper.list_all_tables(factory) == ["public.parent"]
    connection.close.assert_called_once_with()


def test_table_discovery_closes_when_cursor_creation_fails():
    for helper in (psql_database_helper, mysql_database_helper):
        factory = Mock()
        factory.get_db_connection.return_value.cursor.side_effect = RuntimeError(
            "cursor"
        )
        with pytest.raises(RuntimeError, match="cursor"):
            helper.list_all_tables(factory)
        factory.get_db_connection.return_value.close.assert_called_once_with()


@pytest.mark.parametrize("failure", [False, True])
def test_schema_inspection_closes_before_destructive_commands(
    tmp_path, monkeypatch, failure
):
    monkeypatch.chdir(tmp_path)
    source, destination, helper = Mock(), Mock(), Mock()
    monkeypatch.setattr(database_helper, "get_specific_helper", lambda: helper)
    creator = PsqlDatabaseCreator(source, destination)
    source.get_db_connection.assert_not_called()
    helper.list_all_user_schemas.return_value = ["public"]
    helper.DELTA_SCHEMA = "_condenser"
    connection = source.get_db_connection.return_value

    def teardown(query):
        connection.close.assert_called_once_with()

    monkeypatch.setattr(creator, "run_query", teardown)
    if failure:
        helper.list_all_user_schemas.side_effect = RuntimeError("schema inspection")
        with pytest.raises(RuntimeError, match="schema inspection"):
            creator.teardown()
    else:
        creator.teardown()
    connection.close.assert_called_once_with()


def test_reporting_closes_source_if_destination_connect_fails():
    source, destination = Mock(), Mock()
    destination.get_db_connection.side_effect = RuntimeError("destination")
    with pytest.raises(RuntimeError, match="destination"):
        result_tabulator.tabulate(source, destination, [], backend=Mock())
    source.get_db_connection.return_value.close.assert_called_once_with()


def test_reporting_attempts_both_closes_if_one_close_fails():
    source, destination = Mock(), Mock()
    destination.get_db_connection.return_value.close.side_effect = RuntimeError("close")
    with pytest.raises(RuntimeError, match="close"):
        result_tabulator.tabulate(source, destination, [], backend=Mock())
    source.get_db_connection.return_value.close.assert_called_once_with()
