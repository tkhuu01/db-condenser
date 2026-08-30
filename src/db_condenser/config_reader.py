import json
import os
import re
import sys
from dataclasses import dataclass, field
from enum import Enum
from typing import Literal


@dataclass
class PreFilter:
    name: str
    query: str
    column: str

    def __post_init__(self):
        if not isinstance(self.name, str) or not self.name.strip():
            raise ValueError("PreFilter 'name' must be a non-empty string")
        if not isinstance(self.query, str) or not self.query.strip():
            raise ValueError("PreFilter 'query' must be a non-empty string")
        if not isinstance(self.column, str) or not self.column.strip():
            raise ValueError("PreFilter 'column' must be a non-empty string")


@dataclass
class InitialTarget:
    table: str
    percent: float | None = None
    where: str | None = None
    pre_filter: str | None = None

    def __post_init__(self):
        # Exactly one of where/percent must be set
        if (self.where is None) == (self.percent is None):
            raise ValueError(
                "Initial Target must specify exactly one of 'where' or 'percent'"
            )


class DbType(str, Enum):
    POSTGRES = "postgres"
    MYSQL = "mysql"


class DestinationMode(str, Enum):
    # tear down the destination schema and rebuild from scratch
    RECREATE = "recreate"
    # destination already exists: keep schema and data, add only new rows;
    # already-imported entities stay frozen (new children of old rows are
    # not picked up)
    TOPUP = "topup"
    # like topup, but also picks up new children/descendants of
    # already-imported rows by re-reading full destination parent ID sets
    GROW = "grow"


@dataclass
class DbConnectInfo:
    user_name: str
    host: str
    db_name: str
    port: int
    ssl_mode: str | None = None
    # No password will prompt user
    password: str | None = None


@dataclass
class UpstreamFilter:
    condition: str
    table: str | None = None
    column: str | None = None

    def __post_init__(self):
        # Exactly one of table/column must be set
        if (self.table is None) == (self.column is None):
            raise ValueError(
                "Upstream filters must specify exactly one of 'table' or 'column'"
            )


@dataclass
class DependencyBreak:
    fk_table: str
    target_table: str
    preserve_fk_opportunistically: bool = False


@dataclass
class FkAugmentation:
    fk_table: str
    fk_columns: list[str]
    target_table: str
    target_columns: list[str]

    def __post_init__(self):
        for name, value in (
            ("fk_table", self.fk_table),
            ("target_table", self.target_table),
        ):
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be a non-empty string")
        if not isinstance(self.fk_columns, list) or not isinstance(
            self.target_columns, list
        ):
            raise ValueError("fk_columns and target_columns must be lists")
        if len(self.fk_columns) != len(self.target_columns):
            raise ValueError("fk_columns and target_columns must be the same length")
        for name, columns in (
            ("fk_columns", self.fk_columns),
            ("target_columns", self.target_columns),
        ):
            if (
                not isinstance(columns, list)
                or not columns
                or any(
                    not isinstance(column, str) or not column.strip()
                    for column in columns
                )
            ):
                raise ValueError(f"{name} must be a non-empty string list")
            if len(columns) != len(set(columns)):
                raise ValueError(f"{name} must not contain duplicate columns")


@dataclass
class IncrementalKey:
    table: str
    columns: list[str]

    def __post_init__(self):
        if not isinstance(self.table, str) or not self.table.strip():
            raise ValueError("Incremental key 'table' must be a non-empty string")
        if (
            not isinstance(self.columns, list)
            or not self.columns
            or any(
                not isinstance(column, str) or not column.strip()
                for column in self.columns
            )
        ):
            raise ValueError(
                "Incremental key 'columns' must be a non-empty string list"
            )
        if len(self.columns) != len(set(self.columns)):
            raise ValueError("Incremental key columns must not contain duplicates")


@dataclass
class Config:
    db_type: DbType
    initial_targets: list[InitialTarget]
    source_db_connection_info: DbConnectInfo
    destination_db_connection_info: DbConnectInfo
    keep_disconnected_tables: bool = False
    upstream_filters: list[UpstreamFilter] = field(default_factory=list)
    excluded_tables: list[str] = field(default_factory=list)
    passthrough_tables: list[str] = field(default_factory=list)
    dependency_breaks: list[DependencyBreak] = field(default_factory=list)
    fk_augmentation: list[FkAugmentation] = field(default_factory=list)
    incremental_keys: list[IncrementalKey] = field(default_factory=list)
    max_rows_per_table: int | Literal["ALL"] | None = None
    use_temp_tables: bool = False
    use_copy_protocol: bool = True
    destination_mode: DestinationMode = DestinationMode.RECREATE
    parallel_read_workers: int = 1
    pre_filters: list[PreFilter] = field(default_factory=list)
    pre_constraint_sql: list[str] = field(default_factory=list)
    post_subset_sql: list[str] = field(default_factory=list)

    def __post_init__(self):
        if (
            not isinstance(self.parallel_read_workers, int)
            or self.parallel_read_workers < 1
        ):
            raise ValueError("parallel_read_workers must be an integer >= 1")
        if (
            self.db_type == DbType.MYSQL
            and self.destination_mode == DestinationMode.TOPUP
        ):
            raise ValueError(
                'destination_mode "topup" is not yet supported on MySQL; '
                'use "grow" for primary-key tables'
            )
        if self.incremental_keys and self.db_type != DbType.POSTGRES:
            raise ValueError("incremental_keys are only supported on PostgreSQL")
        key_tables = [key.table for key in self.incremental_keys]
        if len(key_tables) != len(set(key_tables)):
            raise ValueError(
                "incremental_keys must contain at most one entry per table"
            )

    @property
    def is_incremental(self) -> bool:
        return self.destination_mode in (DestinationMode.TOPUP, DestinationMode.GROW)

    @property
    def dependency_break_set(self) -> set[tuple[str, str]]:
        return {(b.fk_table, b.target_table) for b in self.dependency_breaks}

    @property
    def preserve_fk_opportunistically(self) -> set[tuple[str, str]]:
        return {
            (b.fk_table, b.target_table)
            for b in self.dependency_breaks
            if b.preserve_fk_opportunistically
        }

    @property
    def initial_target_tables(self) -> list[str]:
        return [target.table for target in self.initial_targets]

    @property
    def incremental_key_map(self) -> dict[str, list[str]]:
        return {key.table: key.columns for key in self.incremental_keys}


config: Config | None = None


_ENV_PATTERN = re.compile(r"\$\{([^}]+)\}")


def _resolve_env(value):
    if not isinstance(value, str):
        return value
    match = _ENV_PATTERN.fullmatch(value)
    if match:
        var = match.group(1)
        if var not in os.environ:
            raise ValueError("Environment variable {} is not set".format(var))
        return os.environ[var]
    return value


def _resolve_env_dict(d: dict) -> dict:
    return {k: _resolve_env(v) for k, v in d.items()}


def _raw_dict_to_config(raw_config: dict) -> Config:
    initial_targets = []
    db_type = DbType(raw_config["db_type"].lower())

    initial_targets = [
        InitialTarget(**target) for target in raw_config["initial_targets"]
    ]

    source_raw = _resolve_env_dict(raw_config["source_db_connection_info"])
    dest_raw = _resolve_env_dict(raw_config["destination_db_connection_info"])
    if "port" in source_raw:
        source_raw["port"] = int(source_raw["port"])
    if "port" in dest_raw:
        dest_raw["port"] = int(dest_raw["port"])
    source_db = DbConnectInfo(**source_raw)
    dest_db = DbConnectInfo(**dest_raw)

    upstream_filters = [
        UpstreamFilter(**table) for table in raw_config.get("upstream_filters", [])
    ]

    excluded_tables = [table for table in raw_config.get("excluded_tables", [])]
    passthrough_tables = list(dict.fromkeys(raw_config.get("passthrough_tables", [])))
    dependency_breaks = [
        DependencyBreak(**relation)
        for relation in raw_config.get("dependency_breaks", [])
    ]
    fk_augmentation = []
    for fka in raw_config.get("fk_augmentation", []):
        if "fk_schema" in fka:
            fka = {
                "fk_table": fka["fk_schema"] + "." + fka["fk_table"],
                "fk_columns": fka["fk_columns"],
                "target_table": fka["target_schema"] + "." + fka["target_table"],
                "target_columns": fka["target_columns"],
            }
        fk_augmentation.append(FkAugmentation(**fka))
    incremental_keys = [
        IncrementalKey(**key) for key in raw_config.get("incremental_keys", [])
    ]

    pre_constraint_sql = [sql for sql in raw_config.get("pre_constraint_sql", [])]
    post_subset_sql = [sql for sql in raw_config.get("post_subset_sql", [])]
    max_rows_per_table = raw_config.get("max_rows_per_table", None)
    use_temp_tables = bool(raw_config.get("use_temp_tables", False))
    use_copy_protocol = bool(raw_config.get("use_copy_protocol", True))
    destination_mode = DestinationMode(
        (raw_config.get("destination_mode") or "recreate").lower()
    )
    parallel_read_workers = int(raw_config.get("parallel_read_workers", 1))
    pre_filters = [PreFilter(**pf) for pf in raw_config.get("pre_filters", [])]
    return Config(
        db_type=db_type,
        initial_targets=initial_targets,
        source_db_connection_info=source_db,
        destination_db_connection_info=dest_db,
        keep_disconnected_tables=bool(
            raw_config.get("keep_disconnected_tables", False)
        ),
        upstream_filters=upstream_filters,
        excluded_tables=excluded_tables,
        passthrough_tables=passthrough_tables,
        dependency_breaks=dependency_breaks,
        fk_augmentation=fk_augmentation,
        incremental_keys=incremental_keys,
        max_rows_per_table=max_rows_per_table,
        use_temp_tables=use_temp_tables,
        use_copy_protocol=use_copy_protocol,
        destination_mode=destination_mode,
        parallel_read_workers=parallel_read_workers,
        pre_filters=pre_filters,
        pre_constraint_sql=pre_constraint_sql,
        post_subset_sql=post_subset_sql,
    )


def initialize(file_name: str):
    global config
    if config:
        print("WARNING: Attempted to initialize configuration twice.", file=sys.stderr)

    with open(file_name, "r") as fp:
        raw_config = json.load(fp)

    config = _raw_dict_to_config(raw_config)


def get_config() -> Config:
    if config is None:
        raise RuntimeError("Config not initialized — call initialize() first")
    return config


def reset_config():
    global config
    config = None
