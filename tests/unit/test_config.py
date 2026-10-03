"""Configuration validation extracted unchanged from the database suite."""

import json
from pathlib import Path

import pytest

from db_condenser import config_reader

CONFIG_JSON = Path(__file__).parents[1] / "psql" / "test_config.json"


def test_load_config_does_not_mutate_global(monkeypatch):
    existing = object()
    monkeypatch.setattr(config_reader, "config", existing)

    loaded = config_reader.load_config(CONFIG_JSON)

    assert loaded.db_type == config_reader.DbType.POSTGRES
    assert config_reader.config is existing


def test_skip_schema_setup_alias_maps_to_destination_mode():
    with open(CONFIG_JSON, "r") as fp:
        raw = json.load(fp)
    raw.pop("destination_mode", None)

    raw["skip_schema_setup"] = True
    cfg = config_reader._raw_dict_to_config(raw)
    assert cfg.destination_mode == config_reader.DestinationMode.TOPUP

    raw["skip_schema_setup"] = False
    cfg = config_reader._raw_dict_to_config(raw)
    assert cfg.destination_mode == config_reader.DestinationMode.RECREATE

    del raw["skip_schema_setup"]
    cfg = config_reader._raw_dict_to_config(raw)
    assert cfg.destination_mode == config_reader.DestinationMode.RECREATE


def test_grow_mode_parses_and_is_incremental():
    with open(CONFIG_JSON, "r") as fp:
        raw = json.load(fp)

    raw["destination_mode"] = "grow"
    cfg = config_reader._raw_dict_to_config(raw)
    assert cfg.destination_mode == config_reader.DestinationMode.GROW
    assert cfg.is_incremental

    raw["destination_mode"] = "topup"
    assert config_reader._raw_dict_to_config(raw).is_incremental

    raw["destination_mode"] = "recreate"
    assert not config_reader._raw_dict_to_config(raw).is_incremental


def test_grow_mode_rejected_on_mysql():
    with open(CONFIG_JSON, "r") as fp:
        raw = json.load(fp)
    raw["db_type"] = "mysql"

    raw["destination_mode"] = "grow"
    with pytest.raises(ValueError, match="grow"):
        config_reader._raw_dict_to_config(raw)

    # topup keeps its historical degraded-but-allowed behavior on MySQL
    raw["destination_mode"] = "topup"
    config_reader._raw_dict_to_config(raw)


def test_incremental_keys_parse_and_validate():
    with open(CONFIG_JSON, "r") as fp:
        raw = json.load(fp)
    raw["incremental_keys"] = [
        {"table": "sales.history", "columns": ["history_id", "version"]}
    ]

    cfg = config_reader._raw_dict_to_config(raw)
    assert cfg.incremental_key_map == {"sales.history": ["history_id", "version"]}

    raw["incremental_keys"].append({"table": "sales.history", "columns": ["other_id"]})
    with pytest.raises(ValueError, match="at most one"):
        config_reader._raw_dict_to_config(raw)

    raw["incremental_keys"] = [{"table": "sales.history", "columns": []}]
    with pytest.raises(ValueError, match="non-empty string list"):
        config_reader._raw_dict_to_config(raw)

    raw["incremental_keys"] = [{"table": "sales.history", "columns": ["history_id"]}]
    raw["db_type"] = "mysql"
    with pytest.raises(ValueError, match="only supported on PostgreSQL"):
        config_reader._raw_dict_to_config(raw)


@pytest.mark.parametrize(
    ("kwargs", "error"),
    [
        (
            {
                "fk_table": "",
                "fk_columns": ["customer_id"],
                "target_table": "sales.customers",
                "target_columns": ["id"],
            },
            "fk_table must be a non-empty string",
        ),
        (
            {
                "fk_table": "sales.history",
                "fk_columns": [],
                "target_table": "sales.customers",
                "target_columns": [],
            },
            "fk_columns must be a non-empty string list",
        ),
        (
            {
                "fk_table": "sales.history",
                "fk_columns": ["customer_id", "customer_id"],
                "target_table": "sales.customers",
                "target_columns": ["id", "id"],
            },
            "fk_columns must not contain duplicate columns",
        ),
    ],
)
def test_fk_augmentation_shape_is_validated(kwargs, error):
    with pytest.raises(ValueError, match=error):
        config_reader.FkAugmentation(**kwargs)
