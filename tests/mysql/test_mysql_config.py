import pytest

from db_condenser import config_reader


def _raw_config():
    connection = {
        "user_name": "test",
        "password": "test",
        "host": "localhost",
        "db_name": "app",
        "port": 3306,
    }
    return {
        "db_type": "mysql",
        "source_db_connection_info": connection,
        "destination_db_connection_info": connection,
        "initial_targets": [{"table": "app.customers", "where": "TRUE"}],
    }


def test_mysql_grow_parses_but_topup_is_rejected():
    raw = _raw_config()

    raw["destination_mode"] = "grow"
    assert (
        config_reader._raw_dict_to_config(raw).destination_mode
        == config_reader.DestinationMode.GROW
    )

    raw["destination_mode"] = "topup"
    with pytest.raises(ValueError, match="topup"):
        config_reader._raw_dict_to_config(raw)


def test_mysql_rejects_incremental_keys():
    raw = _raw_config()
    raw["incremental_keys"] = [{"table": "app.history", "columns": ["history_id"]}]

    with pytest.raises(ValueError, match="only supported on PostgreSQL"):
        config_reader._raw_dict_to_config(raw)
