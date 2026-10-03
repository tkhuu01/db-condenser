"""Seeded property-style tests for relationship-closure correctness.

The generated schemas combine patterns used by SaaS applications and common
audit/versioning libraries: tenant-owned records, nullable actor references,
append-only JSON events, version rows, and two links to the same parent table.
Every failure prints a reproducible seed.
"""

import os
import random
import uuid
from dataclasses import dataclass, field

import psycopg
import pytest
from psycopg.types.json import Json

from db_condenser import config_reader
from db_condenser.db_connect import DbConnect
from db_condenser.runner import _subset_run
from db_condenser.subset import Subset

DB_USER = os.environ.get("POSTGRES_USER", "test")
DB_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "test")
DB_HOST = os.environ.get("POSTGRES_HOST", "localhost")
DB_PORT = os.environ.get("POSTGRES_PORT", "5432")

DEFAULT_SEEDS = [104730, 130363, 155921, 196614, 216091, 262147, 299994, 350377, 399989]
if configured_seeds := os.environ.get("DB_CONDENSER_RANDOM_SEEDS"):
    RANDOM_SEEDS = [int(seed.strip()) for seed in configured_seeds.split(",")]
else:
    RANDOM_SEEDS = DEFAULT_SEEDS
ROWS_PER_TABLE = int(os.environ.get("DB_CONDENSER_RANDOM_ROWS_PER_TABLE", "0"))
GROWTH_ROWS_PER_TABLE = int(
    os.environ.get("DB_CONDENSER_RANDOM_GROWTH_ROWS_PER_TABLE", "1")
)
PARALLEL_READ_WORKERS = int(os.environ.get("DB_CONDENSER_RANDOM_PARALLEL_WORKERS", "1"))
FORCED_BATCH_SIZE = int(os.environ.get("DB_CONDENSER_RANDOM_BATCH_SIZE", "0"))


@dataclass
class TableSpec:
    name: str
    key_columns: list[tuple[str, str]]
    primary_key: bool = True
    rows: list[dict] = field(default_factory=list)

    @property
    def qualified(self):
        return "randomized." + self.name

    @property
    def key_names(self):
        return [column for column, _ in self.key_columns]


@dataclass(frozen=True)
class Relationship:
    child: str
    child_columns: tuple[str, ...]
    parent: str
    parent_columns: tuple[str, ...]
    physical: bool = True
    nullable: bool = False


@dataclass
class GeneratedCase:
    seed: int
    tables: list[TableSpec]
    relationships: list[Relationship]
    dependency_breaks: set[tuple[str, str]]

    @property
    def table_map(self):
        return {table.name: table for table in self.tables}


def _key_columns(role, kind):
    singular = role.removesuffix("s")
    if kind == "uuid":
        return [(singular + "_id", "uuid")]
    if kind == "composite":
        return [("tenant_id", "integer"), (singular + "_no", "integer")]
    return [(singular + "_id", "bigint")]


def _key_value(seed, table_index, table, row_index):
    values = []
    for column, datatype in table.key_columns:
        if datatype == "uuid":
            value = uuid.uuid5(
                uuid.NAMESPACE_OID,
                f"db-condenser:{seed}:{table.name}:{row_index}",
            )
        elif column == "tenant_id":
            value = (row_index % 3) + 1
        else:
            value = table_index * 10_000 + row_index + 1
        values.append(value)
    return tuple(values)


def _relationship(child, prefix, parent, tables, *, physical=True, nullable=False):
    parent_table = tables[parent]
    child_columns = tuple(prefix + "_" + name for name in parent_table.key_names)
    return Relationship(
        child=child,
        child_columns=child_columns,
        parent=parent,
        parent_columns=tuple(parent_table.key_names),
        physical=physical,
        nullable=nullable,
    )


def _generate_case(seed):
    rng = random.Random(seed)
    roles = [
        "organizations",
        "users",
        "projects",
        "issues",
        "comments",
        "attachments",
        "approvals",
        "audit_events",
        "issue_versions",
        "issue_links",
    ]
    kinds = (["bigint", "uuid", "composite"] * 4)[: len(roles)]
    rng.shuffle(kinds)
    tables = {
        role: TableSpec(
            role,
            _key_columns(role, kinds[index]),
            # PaperTrail-like audit tables frequently use a unique ID without
            # declaring it as the primary key.
            primary_key=role != "audit_events",
        )
        for index, role in enumerate(roles)
    }

    relationships = [
        _relationship("users", "organization", "organizations", tables),
        _relationship("projects", "organization", "organizations", tables),
        _relationship("projects", "owner", "users", tables),
        _relationship("issues", "project", "projects", tables),
        _relationship("issues", "reporter", "users", tables, nullable=True),
        _relationship("comments", "issue", "issues", tables),
        _relationship("comments", "author", "users", tables, nullable=True),
        _relationship("attachments", "comment", "comments", tables),
        _relationship("attachments", "uploader", "users", tables, nullable=True),
        _relationship("approvals", "issue", "issues", tables),
        _relationship("approvals", "approver", "users", tables),
        # GitLab/PaperTrail-style polymorphic ownership often isn't a physical
        # FK. Model it as a logical composite relationship.
        _relationship("audit_events", "entity", "issues", tables, physical=False),
        _relationship("audit_events", "actor", "users", tables, nullable=True),
        _relationship("issue_versions", "issue", "issues", tables),
        _relationship("issue_versions", "history_user", "users", tables, nullable=True),
        # A realistic transfer/supersession graph: two constraints to the same
        # parent table must use AND semantics.
        _relationship("issue_links", "from_issue", "issues", tables),
        _relationship("issue_links", "to_issue", "issues", tables),
    ]
    if rng.choice([True, False]):
        relationships.append(_relationship("approvals", "project", "projects", tables))
    if rng.choice([True, False]):
        relationships.append(
            _relationship(
                "audit_events",
                "target_project",
                "projects",
                tables,
                physical=False,
                nullable=True,
            )
        )

    # The attachment still arrives through its comment, while the optional
    # uploader edge is broken and its copied columns must be nulled.
    dependency_breaks = {("attachments", "users")}

    table_list = [tables[role] for role in roles]
    incoming = {role: [] for role in roles}
    for relationship in relationships:
        incoming[relationship.child].append(relationship)

    for table_index, table in enumerate(table_list):
        row_count = ROWS_PER_TABLE or rng.randint(6, 10)
        for row_index in range(row_count):
            row = dict(
                zip(
                    table.key_names,
                    _key_value(seed, table_index, table, row_index),
                )
            )
            row["label"] = f"{table.name}-{row_index}"
            row["payload"] = {
                "seed": seed,
                "row": row_index,
                "kind": table.name,
            }
            if table.name == "organizations":
                row["selected"] = row_index < max(2, row_count // 2)

            for relationship in incoming[table.name]:
                parent = tables[relationship.parent]
                # The first two rows form a guaranteed selected deep path.
                # Later rows explore selected/unselected combinations.
                parent_index = 0 if row_index < 2 else rng.randrange(len(parent.rows))
                parent_row = parent.rows[parent_index]
                parent_key = tuple(
                    parent_row[column] for column in relationship.parent_columns
                )
                make_null = relationship.nullable and (
                    row_index == 1 or (row_index > 1 and rng.random() < 0.28)
                )
                for column_index, (child_column, value) in enumerate(
                    zip(relationship.child_columns, parent_key)
                ):
                    if make_null:
                        # Exercise both wholly-null and MATCH SIMPLE partially
                        # null composite foreign keys.
                        row[child_column] = (
                            None if row_index % 3 or column_index == 0 else value
                        )
                    else:
                        row[child_column] = value
            table.rows.append(row)

    return GeneratedCase(seed, table_list, relationships, dependency_breaks)


def _column_types(case, table):
    columns = dict(table.key_columns)
    columns.update({"label": "text", "payload": "jsonb"})
    if table.name == "organizations":
        columns["selected"] = "boolean"
    tables = case.table_map
    for relationship in case.relationships:
        if relationship.child != table.name:
            continue
        parent_types = dict(tables[relationship.parent].key_columns)
        for child_column, parent_column in zip(
            relationship.child_columns, relationship.parent_columns
        ):
            columns[child_column] = parent_types[parent_column]
    return columns


def _create_case_schema(conn, case, with_rows):
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA randomized")
        for table in case.tables:
            columns = _column_types(case, table)
            declarations = []
            for name, datatype in columns.items():
                not_null = " NOT NULL" if name in table.key_names else ""
                declarations.append(f'"{name}" {datatype}{not_null}')
            key_columns = ", ".join(f'"{name}"' for name in table.key_names)
            key_kind = "PRIMARY KEY" if table.primary_key else "UNIQUE"
            declarations.append(f"{key_kind} ({key_columns})")
            for index, relationship in enumerate(case.relationships):
                if relationship.child != table.name or not relationship.physical:
                    continue
                child_columns = ", ".join(
                    f'"{name}"' for name in relationship.child_columns
                )
                parent_columns = ", ".join(
                    f'"{name}"' for name in relationship.parent_columns
                )
                declarations.append(
                    f'CONSTRAINT "fk_{table.name}_{index}" FOREIGN KEY'
                    f" ({child_columns}) REFERENCES"
                    f' randomized."{relationship.parent}" ({parent_columns})'
                )
            cur.execute(
                f'CREATE TABLE randomized."{table.name}" ({", ".join(declarations)})'
            )

            if not with_rows:
                continue
            _insert_rows(cur, case, table, table.rows)
            if PARALLEL_READ_WORKERS > 1:
                # Fresh test tables otherwise report no pages, preventing the
                # production CTID range splitter from exercising parallel reads.
                cur.execute(f'ANALYZE randomized."{table.name}"')
    conn.commit()


def _insert_rows(cur, case, table, rows):
    columns = _column_types(case, table)
    column_names = list(columns)
    insert = (
        f'INSERT INTO randomized."{table.name}" ('
        + ", ".join(f'"{name}"' for name in column_names)
        + ") VALUES ("
        + ", ".join(["%s"] * len(column_names))
        + ")"
    )
    values = [
        tuple(
            Json(row[column]) if columns[column] == "jsonb" else row[column]
            for column in column_names
        )
        for row in rows
    ]
    cur.executemany(insert, values)


def _append_growth_rows(conn, case):
    """Add children owned by already-resident row zero."""
    tables = case.table_map
    incoming = {table.name: [] for table in case.tables}
    for relationship in case.relationships:
        incoming[relationship.child].append(relationship)

    with conn.cursor() as cur:
        for growth_index in range(GROWTH_ROWS_PER_TABLE):
            for table_index, table in enumerate(case.tables[1:], start=1):
                row_index = len(table.rows)
                row = dict(
                    zip(
                        table.key_names,
                        _key_value(case.seed, table_index, table, row_index),
                    )
                )
                row["label"] = f"{table.name}-growth-{row_index}"
                row["payload"] = {
                    "seed": case.seed,
                    "row": row_index,
                    "kind": table.name,
                    "growth": True,
                    "growth_index": growth_index,
                }
                for relationship in incoming[table.name]:
                    parent = tables[relationship.parent]
                    parent_row = parent.rows[0]
                    parent_key = tuple(
                        parent_row[column] for column in relationship.parent_columns
                    )
                    broken = (relationship.child, relationship.parent) in (
                        case.dependency_breaks
                    )
                    make_null = (
                        relationship.nullable
                        and not broken
                        and (case.seed + table_index + growth_index) % 2 == 0
                    )
                    for child_column, value in zip(
                        relationship.child_columns, parent_key
                    ):
                        row[child_column] = None if make_null else value
                table.rows.append(row)
                _insert_rows(cur, case, table, [row])
    conn.commit()


def _key(table, row):
    return tuple(row[column] for column in table.key_names)


def _reference_closure(case):
    """Independent in-memory implementation of middle-out membership.

    A row is eligible upstream when at least one non-broken relationship
    references an already-selected parent and every other applicable
    relationship either references a selected parent or is NULL (PostgreSQL
    MATCH SIMPLE semantics). Downstream then supplies every referenced parent.
    """
    tables = case.table_map
    selected = {table.name: set() for table in case.tables}
    selected["organizations"] = {
        _key(tables["organizations"], row)
        for row in tables["organizations"].rows
        if row["selected"]
    }
    processed = {"organizations"}

    for table in case.tables[1:]:
        relationships = [
            relationship
            for relationship in case.relationships
            if relationship.child == table.name
            and relationship.parent in processed
            and (relationship.child, relationship.parent) not in case.dependency_breaks
        ]
        if not relationships:
            continue
        for row in table.rows:
            outcomes = []
            for relationship in relationships:
                fk = tuple(row[column] for column in relationship.child_columns)
                is_null = any(value is None for value in fk)
                matches = not is_null and fk in selected[relationship.parent]
                outcomes.append((is_null, matches))
            if any(matches for _, matches in outcomes) and all(
                is_null or matches for is_null, matches in outcomes
            ):
                selected[table.name].add(_key(table, row))
        processed.add(table.name)

    for parent in reversed(case.tables):
        parent_rows = {_key(parent, row): row for row in parent.rows}
        for relationship in case.relationships:
            if (
                relationship.parent != parent.name
                or (relationship.child, relationship.parent) in case.dependency_breaks
            ):
                continue
            child = tables[relationship.child]
            selected_child_rows = [
                row for row in child.rows if _key(child, row) in selected[child.name]
            ]
            for row in selected_child_rows:
                fk = tuple(row[column] for column in relationship.child_columns)
                if any(value is None for value in fk):
                    continue
                if fk in parent_rows:
                    selected[parent.name].add(fk)
    return selected


def _connection_info(database):
    return {
        "user_name": DB_USER,
        "password": DB_PASSWORD,
        "host": DB_HOST,
        "db_name": database,
        "port": int(DB_PORT),
    }


def _admin(database="postgres", autocommit=True):
    return psycopg.connect(
        dbname=database,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        autocommit=autocommit,
    )


def _run_case(case, source_database, destination_database, mode="grow"):
    raw_config = {
        "db_type": "postgres",
        "source_db_connection_info": _connection_info(source_database),
        "destination_db_connection_info": _connection_info(destination_database),
        "initial_targets": [{"table": "randomized.organizations", "where": "selected"}],
        "destination_mode": mode,
        "keep_disconnected_tables": False,
        "use_temp_tables": case.seed % 2 == 0,
        "parallel_read_workers": PARALLEL_READ_WORKERS,
        "dependency_breaks": [
            {
                "fk_table": "randomized." + child,
                "target_table": "randomized." + parent,
            }
            for child, parent in sorted(case.dependency_breaks)
        ],
        "fk_augmentation": [
            {
                "fk_table": "randomized." + relationship.child,
                "fk_columns": list(relationship.child_columns),
                "target_table": "randomized." + relationship.parent,
                "target_columns": list(relationship.parent_columns),
            }
            for relationship in case.relationships
            if not relationship.physical
        ],
    }
    config_reader.reset_config()
    config_reader.config = config_reader._raw_dict_to_config(raw_config)
    config = config_reader.get_config()
    source_dbc = DbConnect(config.db_type, config.source_db_connection_info)
    destination_dbc = DbConnect(config.db_type, config.destination_db_connection_info)
    all_tables = [table.qualified for table in case.tables]
    subsetter = Subset(source_dbc, destination_dbc, all_tables)
    with _subset_run(subsetter):
        pass


def _actual_keys(conn, table):
    columns = ", ".join(f'"{column}"' for column in table.key_names)
    with conn.cursor() as cur:
        cur.execute(f'SELECT {columns} FROM randomized."{table.name}"')
        return {tuple(row) for row in cur.fetchall()}


def _assert_membership(conn, case, expected, phase):
    for table in case.tables:
        assert _actual_keys(conn, table) == expected[table.name], (
            f"seed={case.seed}, phase={phase}, table={table.name}"
        )


@pytest.mark.parametrize("seed", RANDOM_SEEDS)
def test_seeded_random_relationship_closure_matches_reference(seed, monkeypatch):
    if FORCED_BATCH_SIZE:
        import db_condenser.psql_database_helper as helper_module
        import db_condenser.subset as subset_module

        monkeypatch.setattr(
            subset_module, "compute_batch_size", lambda _: FORCED_BATCH_SIZE
        )
        monkeypatch.setattr(
            helper_module, "compute_batch_size", lambda _: FORCED_BATCH_SIZE
        )
    case = _generate_case(seed)
    source_database = f"condenser_random_source_{seed}"
    destination_database = f"condenser_random_dest_{seed}"
    admin = _admin()
    try:
        with admin.cursor() as cur:
            for database in (source_database, destination_database):
                cur.execute(f'DROP DATABASE IF EXISTS "{database}"')
                cur.execute(f'CREATE DATABASE "{database}"')

        source = _admin(source_database, autocommit=False)
        destination = _admin(destination_database, autocommit=False)
        try:
            _create_case_schema(source, case, with_rows=True)
            _create_case_schema(destination, case, with_rows=False)
        finally:
            source.close()
            destination.close()

        initial_expected = _reference_closure(case)
        initial_mode = "topup" if seed % 2 else "grow"
        _run_case(case, source_database, destination_database, mode=initial_mode)

        destination = _admin(destination_database, autocommit=False)
        try:
            _assert_membership(destination, case, initial_expected, "initial")
        finally:
            destination.close()

        source = _admin(source_database, autocommit=False)
        try:
            _append_growth_rows(source, case)
        finally:
            source.close()
        growth_expected = _reference_closure(case)

        # Every appended row hangs from an already-resident parent, which is
        # specifically the relationship expansion that distinguishes grow.
        _run_case(case, source_database, destination_database)
        # A mutation-free third run must be idempotent across every graph.
        _run_case(case, source_database, destination_database)

        destination = _admin(destination_database, autocommit=False)
        try:
            _assert_membership(destination, case, growth_expected, "growth")

            with destination.cursor() as cur:
                cur.execute("SELECT to_regnamespace('_condenser') IS NULL")
                assert cur.fetchone()[0], f"seed={seed}: journal not cleaned"
                cur.execute(
                    "SELECT bool_and(convalidated) FROM pg_constraint"
                    " WHERE contype = 'f' AND connamespace = 'randomized'::regnamespace"
                )
                assert cur.fetchone()[0] is True, f"seed={seed}: invalid FK"

                # Broken dependency columns are intentionally nulled, and the
                # selected deep path guarantees at least one attachment row.
                broken = next(
                    relationship
                    for relationship in case.relationships
                    if relationship.child == "attachments"
                    and relationship.parent == "users"
                )
                cur.execute(
                    'SELECT COUNT(*) FROM randomized."attachments" WHERE '
                    + " OR ".join(
                        f'"{column}" IS NOT NULL' for column in broken.child_columns
                    )
                )
                assert cur.fetchone()[0] == 0, (
                    f"seed={seed}: dependency break retained values"
                )
        finally:
            destination.close()
    finally:
        with admin.cursor() as cur:
            for database in (source_database, destination_database):
                cur.execute(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity"
                    " WHERE datname = %s AND pid <> pg_backend_pid()",
                    (database,),
                )
                cur.execute(f'DROP DATABASE IF EXISTS "{database}"')
        admin.close()
        config_reader.reset_config()
