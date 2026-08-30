import json
import os
from pathlib import Path

import psycopg
import pytest
from psycopg.types.json import Json

from db_condenser import config_reader, database_helper, result_tabulator
from db_condenser.db_connect import DbConnect, PsqlConnection
from db_condenser.direct_subset import db_creator
from db_condenser.subset import Subset

TEST_DIR = Path(__file__).parent
SEED_SQL = TEST_DIR / "seed.sql"
CONFIG_JSON = TEST_DIR / "test_config.json"

DB_USER = os.environ.get("POSTGRES_USER", "test")
DB_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "test")
DB_HOST = os.environ.get("POSTGRES_HOST", "localhost")
DB_PORT = os.environ.get("POSTGRES_PORT", "5432")

SOURCE_DB = "condenser_test_source"
DEST_DB = "condenser_test_dest"


def _admin_conn(dbname="postgres"):
    return psycopg.connect(
        dbname=dbname,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        autocommit=True,
    )


def _query_one(conn, sql):
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchone()[0]


def _drop_test_databases(*databases):
    admin = _admin_conn()
    with admin.cursor() as cur:
        for database in databases:
            cur.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = %s AND pid <> pg_backend_pid()",
                (database,),
            )
            cur.execute(f"DROP DATABASE IF EXISTS {database}")
    admin.close()


def _run_subsetter(
    use_temp_tables: bool,
    use_copy_protocol: bool = False,
    mode: str = "recreate",
    suffix_override: str | None = None,
    parallel_read_workers: int = 1,
    config_overrides: dict | None = None,
    setup_sql: list[str] | None = None,
    reset_databases: bool = True,
) -> tuple[str, str]:
    if suffix_override is not None:
        suffix = suffix_override
    else:
        suffix = "_temp" if use_temp_tables else "_copy" if use_copy_protocol else ""
    source_db = SOURCE_DB + suffix
    dest_db = DEST_DB + suffix

    fresh = mode == "recreate"
    if fresh and reset_databases:
        admin = _admin_conn()
        with admin.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS {source_db}")
            cur.execute(f"DROP DATABASE IF EXISTS {dest_db}")
            cur.execute(f"CREATE DATABASE {source_db}")
            cur.execute(f"CREATE DATABASE {dest_db}")
        admin.close()

        source_admin = _admin_conn(source_db)
        with source_admin.cursor() as cur:
            cur.execute(SEED_SQL.read_text())
            for stmt in setup_sql or []:
                cur.execute(stmt)
        source_admin.close()

    with open(CONFIG_JSON, "r") as fp:
        raw_config = json.load(fp)
    raw_config["source_db_connection_info"]["db_name"] = source_db
    raw_config["destination_db_connection_info"]["db_name"] = dest_db
    raw_config["use_temp_tables"] = use_temp_tables
    raw_config["use_copy_protocol"] = use_copy_protocol
    raw_config["destination_mode"] = mode
    raw_config["parallel_read_workers"] = parallel_read_workers
    if config_overrides:
        raw_config.update(config_overrides)

    config_reader.reset_config()
    config_reader.config = config_reader._raw_dict_to_config(raw_config)

    config = config_reader.get_config()
    db_type = config.db_type
    source_dbc = DbConnect(db_type, config.source_db_connection_info)
    destination_dbc = DbConnect(db_type, config.destination_db_connection_info)

    database = db_creator(db_type, source_dbc, destination_dbc)
    if fresh:
        database.teardown()
        database.create()

    db_helper = database_helper.get_specific_helper()
    all_tables = db_helper.list_all_tables(source_dbc)
    all_tables = [x for x in all_tables if x not in config.excluded_tables]

    subsetter = Subset(source_dbc, destination_dbc, all_tables)
    succeeded = False
    try:
        subsetter.prep_temp_dbs()
        subsetter.run_middle_out()

        for sql_stmt in config.pre_constraint_sql:
            db_helper.run_query(sql_stmt, destination_dbc.get_db_connection())

        if fresh:
            database.add_constraints()

        for sql_stmt in config.post_subset_sql:
            db_helper.run_query(sql_stmt, destination_dbc.get_db_connection())

        all_tables_no_pg = [t for t in all_tables if "pgbench" not in t]
        dest_conn = destination_dbc.get_db_connection()
        assert isinstance(dest_conn, PsqlConnection)
        db_helper.update_sequence_numbering(dest_conn, all_tables_no_pg)
        succeeded = True
    finally:
        try:
            subsetter.unprep_temp_dbs(succeeded=succeeded)
        finally:
            subsetter.close_connections()

    return source_db, dest_db


@pytest.fixture(
    scope="module",
    params=[
        {"use_temp_tables": False, "use_copy_protocol": False},
        {"use_temp_tables": True, "use_copy_protocol": False},
        {"use_temp_tables": False, "use_copy_protocol": True},
    ],
    ids=["unnest", "temp_tables", "copy_protocol"],
)
def subsetter_dbs(request):
    source_db, dest_db = _run_subsetter(**request.param)

    source = psycopg.connect(
        dbname=source_db,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )
    dest = psycopg.connect(
        dbname=dest_db,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )

    yield source, dest

    source.close()
    dest.close()

    admin = _admin_conn()
    with admin.cursor() as cur:
        for db in (source_db, dest_db):
            cur.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = %s AND pid <> pg_backend_pid()",
                (db,),
            )
            cur.execute(f"DROP DATABASE IF EXISTS {db}")
    admin.close()


def test_passthrough_table_fully_copied(subsetter_dbs):
    source, dest = subsetter_dbs
    source_count = _query_one(source, "SELECT COUNT(*) FROM public.regions")
    dest_count = _query_one(dest, "SELECT COUNT(*) FROM public.regions")
    assert dest_count == source_count == 5


def test_disconnected_table_copied(subsetter_dbs):
    source, dest = subsetter_dbs
    source_count = _query_one(source, "SELECT COUNT(*) FROM public.feature_flags")
    dest_count = _query_one(dest, "SELECT COUNT(*) FROM public.feature_flags")
    assert dest_count == source_count == 3


def test_initial_target_filtered(subsetter_dbs):
    source, dest = subsetter_dbs
    source_count = _query_one(source, "SELECT COUNT(*) FROM sales.customers")
    dest_count = _query_one(dest, "SELECT COUNT(*) FROM sales.customers")
    # Direct target is 5, but downstream may pull in more to satisfy FKs
    assert dest_count < source_count
    assert dest_count >= 5


def test_dependency_break_nullifies_fk(subsetter_dbs):
    _, dest = subsetter_dbs
    non_null = _query_one(
        dest,
        "SELECT COUNT(*) FROM sales.customers WHERE favorite_order_id IS NOT NULL",
    )
    assert non_null == 0


def test_upstream_orders_follow_customers(subsetter_dbs):
    _, dest = subsetter_dbs
    orphans = _query_one(
        dest,
        """
        SELECT COUNT(*) FROM sales.orders o
        WHERE NOT EXISTS (
            SELECT 1 FROM sales.customers c WHERE c.id = o.customer_id
        )
        """,
    )
    assert orphans == 0
    # Should have some orders (not empty)
    order_count = _query_one(dest, "SELECT COUNT(*) FROM sales.orders")
    assert order_count > 0


def test_upstream_order_lines_follow_orders(subsetter_dbs):
    _, dest = subsetter_dbs
    orphans = _query_one(
        dest,
        """
        SELECT COUNT(*) FROM sales.order_lines ol
        WHERE NOT EXISTS (
            SELECT 1 FROM sales.orders o WHERE o.id = ol.order_id
        )
        """,
    )
    assert orphans == 0
    line_count = _query_one(dest, "SELECT COUNT(*) FROM sales.order_lines")
    assert line_count > 0


def test_downstream_products_pulled_in(subsetter_dbs):
    _, dest = subsetter_dbs
    orphans = _query_one(
        dest,
        """
        SELECT COUNT(*) FROM sales.order_lines ol
        WHERE NOT EXISTS (
            SELECT 1 FROM inventory.products p WHERE p.id = ol.product_id
        )
        """,
    )
    assert orphans == 0
    product_count = _query_one(dest, "SELECT COUNT(*) FROM inventory.products")
    assert 0 < product_count <= 20


def test_downstream_warehouses_pulled_in(subsetter_dbs):
    _, dest = subsetter_dbs
    orphans = _query_one(
        dest,
        """
        SELECT COUNT(*) FROM sales.orders o
        WHERE o.warehouse_id IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM inventory.warehouses w WHERE w.id = o.warehouse_id
        )
        """,
    )
    assert orphans == 0
    wh_count = _query_one(dest, "SELECT COUNT(*) FROM inventory.warehouses")
    assert 0 < wh_count <= 5


def test_upstream_multi_fk_no_orphans(subsetter_dbs):
    """order_transfers has two FKs to orders (from_order_id, to_order_id).
    Verify no orphaned references after subsetting."""
    _, dest = subsetter_dbs
    orphans = _query_one(
        dest,
        """
        SELECT COUNT(*) FROM sales.order_transfers t
        WHERE NOT EXISTS (
            SELECT 1 FROM sales.orders o WHERE o.id = t.from_order_id
        )
        OR NOT EXISTS (
            SELECT 1 FROM sales.orders o WHERE o.id = t.to_order_id
        )
        """,
    )
    assert orphans == 0


def test_upstream_multi_fk_and_semantics(subsetter_dbs):
    """Only transfers where BOTH from_order and to_order are imported should
    be included (AND semantics, not OR)."""
    _, dest = subsetter_dbs
    count = _query_one(dest, "SELECT COUNT(*) FROM sales.order_transfers")
    assert count == 3


def test_upstream_order_lines_multi_fk_no_orphans(subsetter_dbs):
    """order_lines has FKs to orders and products (different target tables).
    Verifies grouped ID collection keeps separate groups distinct."""
    _, dest = subsetter_dbs
    order_orphans = _query_one(
        dest,
        """
        SELECT COUNT(*) FROM sales.order_lines ol
        WHERE NOT EXISTS (
            SELECT 1 FROM sales.orders o WHERE o.id = ol.order_id
        )
        """,
    )
    product_orphans = _query_one(
        dest,
        """
        SELECT COUNT(*) FROM sales.order_lines ol
        WHERE NOT EXISTS (
            SELECT 1 FROM inventory.products p WHERE p.id = ol.product_id
        )
        """,
    )
    assert order_orphans == 0
    assert product_orphans == 0


def test_stock_levels_fk_integrity(subsetter_dbs):
    """stock_levels has a composite PK and FKs to both warehouses and products.
    Verifies downstream subsetting pulls in all referenced rows."""
    _, dest = subsetter_dbs
    warehouse_orphans = _query_one(
        dest,
        """
        SELECT COUNT(*) FROM inventory.stock_levels sl
        WHERE NOT EXISTS (
            SELECT 1 FROM inventory.warehouses w WHERE w.id = sl.warehouse_id
        )
        """,
    )
    product_orphans = _query_one(
        dest,
        """
        SELECT COUNT(*) FROM inventory.stock_levels sl
        WHERE NOT EXISTS (
            SELECT 1 FROM inventory.products p WHERE p.id = sl.product_id
        )
        """,
    )
    assert warehouse_orphans == 0
    assert product_orphans == 0


def test_destination_has_fewer_rows(subsetter_dbs):
    source, dest = subsetter_dbs
    for table in ["sales.customers", "sales.orders", "sales.order_lines"]:
        src = _query_one(source, f"SELECT COUNT(*) FROM {table}")
        dst = _query_one(dest, f"SELECT COUNT(*) FROM {table}")
        assert dst < src, (
            f"{table}: dest ({dst}) should have fewer rows than source ({src})"
        )


def test_sequences_reset(subsetter_dbs):
    _, dest = subsetter_dbs
    tables_with_serials = [
        ("sales", "customers"),
        ("sales", "orders"),
        ("sales", "order_lines"),
        ("sales", "order_transfers"),
        ("inventory", "products"),
        ("inventory", "warehouses"),
    ]
    for schema, table in tables_with_serials:
        seq_name = _query_one(
            dest,
            f"SELECT pg_get_serial_sequence('{schema}.{table}', 'id')",
        )
        if seq_name is None:
            continue
        seq_val = _query_one(dest, f"SELECT last_value FROM {seq_name}")
        max_id = _query_one(dest, f"SELECT COALESCE(MAX(id), 0) FROM {schema}.{table}")
        assert seq_val >= max_id, (
            f"{schema}.{table}: sequence value ({seq_val}) should be >= max id ({max_id})"
        )


@pytest.fixture(
    scope="module",
    params=[
        {
            "use_temp_tables": False,
            "use_copy_protocol": False,
            "suffix_override": "_rerun",
        },
        {
            "use_temp_tables": True,
            "use_copy_protocol": False,
            "suffix_override": "_rerun_temp_tables",
        },
        {
            "use_temp_tables": False,
            "use_copy_protocol": True,
            "suffix_override": "_rerun_copy",
        },
    ],
    ids=["unnest_rerun", "temp_tables_rerun", "copy_protocol_rerun"],
)
def rerun_dbs(request):
    """Run the subsetter twice on the same destination in topup mode."""
    source_db, dest_db = _run_subsetter(**request.param)
    _run_subsetter(**request.param, mode="topup")

    dest = psycopg.connect(
        dbname=dest_db,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )
    yield dest
    dest.close()

    admin = _admin_conn()
    with admin.cursor() as cur:
        for db in (source_db, dest_db):
            cur.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = %s AND pid <> pg_backend_pid()",
                (db,),
            )
            cur.execute(f"DROP DATABASE IF EXISTS {db}")
    admin.close()


def test_rerun_no_duplicate_rows(rerun_dbs):
    dest = rerun_dbs
    for table in ["sales.customers", "sales.orders", "sales.order_lines"]:
        total = _query_one(dest, f"SELECT COUNT(*) FROM {table}")
        distinct = _query_one(dest, f"SELECT COUNT(DISTINCT id) FROM {table}")
        assert total == distinct, f"{table}: found duplicate rows after rerun"


def test_rerun_fk_integrity(rerun_dbs):
    dest = rerun_dbs
    orphans = _query_one(
        dest,
        """
        SELECT COUNT(*) FROM sales.orders o
        WHERE NOT EXISTS (
            SELECT 1 FROM sales.customers c WHERE c.id = o.customer_id
        )
        """,
    )
    assert orphans == 0


@pytest.fixture(
    scope="module",
    params=[
        {"use_copy_protocol": False, "suffix_override": "_parallel"},
        {"use_copy_protocol": True, "suffix_override": "_parallel_copy"},
    ],
    ids=["parallel_unnest", "parallel_copy_protocol"],
)
def parallel_dbs(request):
    """Run subsetter with parallel ctid page-range splitting."""
    source_db, dest_db = _run_subsetter(
        use_temp_tables=False,
        use_copy_protocol=request.param["use_copy_protocol"],
        parallel_read_workers=4,
        suffix_override=request.param["suffix_override"],
    )

    source = psycopg.connect(
        dbname=source_db,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )
    dest = psycopg.connect(
        dbname=dest_db,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )

    yield source, dest

    source.close()
    dest.close()

    admin = _admin_conn()
    with admin.cursor() as cur:
        for db in (source_db, dest_db):
            cur.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = %s AND pid <> pg_backend_pid()",
                (db,),
            )
            cur.execute(f"DROP DATABASE IF EXISTS {db}")
    admin.close()


def test_parallel_target_filtered(parallel_dbs):
    source, dest = parallel_dbs
    source_count = _query_one(source, "SELECT COUNT(*) FROM sales.customers")
    dest_count = _query_one(dest, "SELECT COUNT(*) FROM sales.customers")
    assert dest_count < source_count
    assert dest_count >= 5


def test_parallel_no_duplicate_rows(parallel_dbs):
    _, dest = parallel_dbs
    for table in ["sales.customers", "sales.orders", "sales.order_lines"]:
        total = _query_one(dest, f"SELECT COUNT(*) FROM {table}")
        distinct = _query_one(dest, f"SELECT COUNT(DISTINCT id) FROM {table}")
        assert total == distinct, f"{table}: duplicate rows with parallel reads"


def test_parallel_fk_integrity(parallel_dbs):
    _, dest = parallel_dbs
    orphans = _query_one(
        dest,
        """
        SELECT COUNT(*) FROM sales.orders o
        WHERE NOT EXISTS (
            SELECT 1 FROM sales.customers c WHERE c.id = o.customer_id
        )
        """,
    )
    assert orphans == 0


@pytest.fixture(scope="module")
def pre_filter_dbs():
    """Run subsetter with a pre_filter to simulate FDW-based filtering."""
    source_db = SOURCE_DB + "_prefilter"
    dest_db = DEST_DB + "_prefilter"

    admin = _admin_conn()
    with admin.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS {source_db}")
        cur.execute(f"DROP DATABASE IF EXISTS {dest_db}")
        cur.execute(f"CREATE DATABASE {source_db}")
        cur.execute(f"CREATE DATABASE {dest_db}")
    admin.close()

    source_admin = _admin_conn(source_db)
    with source_admin.cursor() as cur:
        cur.execute(SEED_SQL.read_text())
    source_admin.close()

    with open(CONFIG_JSON, "r") as fp:
        raw_config = json.load(fp)
    raw_config["source_db_connection_info"]["db_name"] = source_db
    raw_config["destination_db_connection_info"]["db_name"] = dest_db
    raw_config["initial_targets"] = [
        {"table": "sales.customers", "where": "1=1", "pre_filter": "region_ids"}
    ]
    raw_config["pre_filters"] = [
        {
            "name": "region_ids",
            "query": "SELECT id FROM sales.customers WHERE region = 'US-W'",
            "column": "id",
        }
    ]
    raw_config["parallel_read_workers"] = 4

    config_reader.reset_config()
    config_reader.config = config_reader._raw_dict_to_config(raw_config)

    config = config_reader.get_config()
    db_type = config.db_type
    source_dbc = DbConnect(db_type, config.source_db_connection_info)
    destination_dbc = DbConnect(db_type, config.destination_db_connection_info)

    database = db_creator(db_type, source_dbc, destination_dbc)
    database.teardown()
    database.create()

    db_helper = database_helper.get_specific_helper()
    all_tables = db_helper.list_all_tables(source_dbc)
    all_tables = [x for x in all_tables if x not in config.excluded_tables]

    subsetter = Subset(source_dbc, destination_dbc, all_tables)
    try:
        subsetter.prep_temp_dbs()
        subsetter.run_middle_out()
    finally:
        subsetter.unprep_temp_dbs()
        subsetter.close_connections()

    dest = psycopg.connect(
        dbname=dest_db,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )

    yield dest

    dest.close()

    admin = _admin_conn()
    with admin.cursor() as cur:
        for db in (source_db, dest_db):
            cur.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = %s AND pid <> pg_backend_pid()",
                (db,),
            )
            cur.execute(f"DROP DATABASE IF EXISTS {db}")
    admin.close()


def test_pre_filter_limits_rows(pre_filter_dbs):
    dest = pre_filter_dbs
    count = _query_one(dest, "SELECT COUNT(*) FROM sales.customers")
    # Only US-W customers: Alice(1), Eve(5), Iris(9) = 3
    assert count == 3
    regions = _query_one(
        dest,
        "SELECT COUNT(DISTINCT region) FROM sales.customers",
    )
    assert regions == 1


# ============================================================
# INCREMENTAL (TOP-UP) RE-RUNS
# ============================================================


@pytest.fixture(
    scope="module",
    params=[
        {
            "use_temp_tables": False,
            "use_copy_protocol": False,
            "suffix_override": "_incr",
        },
        {
            "use_temp_tables": True,
            "use_copy_protocol": False,
            "suffix_override": "_incr_tt",
        },
        {
            "use_temp_tables": False,
            "use_copy_protocol": True,
            "suffix_override": "_incr_cp",
        },
    ],
    ids=["unnest_incremental", "temp_tables_incremental", "copy_protocol_incremental"],
)
def incremental_dbs(request):
    """Run once, add rows to the source, then re-run in topup mode.

    New source rows:
    - customer 11 (Kate, 2025) matches the target WHERE -> should arrive
    - order 31 belongs to Kate -> should arrive (children of new parents)
    - order 32 belongs to customer 6 (imported in run 1) -> should NOT
      arrive (top-up semantics: existing entities stay frozen)
    - order_lines for each follow their order
    - product 21 is referenced only by order 31's line -> downstream pull
    - transfer 31->16 pairs a new order with a run-1 order -> should arrive
      (multi-FK AND semantics against delta + full sets)
    - transfer 32->16 references a never-imported order -> should NOT arrive
    - product 22 is referenced only by rows a topup run never re-reads (an
      old line repointed source-side, and a new line of a frozen order) ->
      the delta-restricted downstream scan must NOT pull it
    - customer 6's name and a passthrough region's tax_rate change
      source-side -> topup re-reads both, upsert refreshes them in place
      (and, per the frozen tests, the refresh must not enter the deltas)
    """
    source_db, dest_db = _run_subsetter(**request.param)

    src = psycopg.connect(
        dbname=source_db,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )
    with src.cursor() as cur:
        cur.execute(
            "INSERT INTO sales.customers (name, email, region, created_at)"
            " VALUES ('Kate', 'kate@example.com', 'US-W', '2025-09-01')"
        )
        cur.execute(
            "INSERT INTO inventory.products (name, price, metadata)"
            " VALUES ('Widget Z', 19.99, NULL)"
        )
        cur.execute(
            "INSERT INTO sales.orders (customer_id, warehouse_id, ordered_at)"
            " VALUES (11, 1, '2025-09-02'), (6, 2, '2025-09-03')"
        )
        cur.execute(
            "INSERT INTO sales.order_lines (order_id, product_id, quantity, unit_price)"
            " VALUES (31, 21, 1, 19.99), (32, 1, 1, 9.99)"
        )
        cur.execute(
            "INSERT INTO sales.order_transfers (from_order_id, to_order_id, reason)"
            " VALUES (31, 16, 'new to old'), (32, 16, 'not imported')"
        )
        cur.execute(
            "INSERT INTO inventory.products (name, price, metadata)"
            " VALUES ('Widget AA', 5.00, NULL)"
        )
        cur.execute(
            "UPDATE sales.order_lines SET product_id = 22 WHERE id ="
            " (SELECT MIN(id) FROM sales.order_lines WHERE order_id = 16)"
        )
        cur.execute(
            "INSERT INTO sales.order_lines (order_id, product_id, quantity, unit_price)"
            " VALUES (16, 22, 1, 5.00)"
        )
        cur.execute("UPDATE sales.customers SET name = 'Frank Updated' WHERE id = 6")
        cur.execute("UPDATE public.regions SET tax_rate = 0.123 WHERE code = 'US-W'")
    src.commit()
    src.close()

    _run_subsetter(**request.param, mode="topup")

    dest = psycopg.connect(
        dbname=dest_db,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )
    yield dest
    dest.close()

    admin = _admin_conn()
    with admin.cursor() as cur:
        for db in (source_db, dest_db):
            cur.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = %s AND pid <> pg_backend_pid()",
                (db,),
            )
            cur.execute(f"DROP DATABASE IF EXISTS {db}")
    admin.close()


def test_incremental_new_rows_arrive(incremental_dbs):
    """New target rows, their descendants, and new downstream references."""
    dest = incremental_dbs
    assert _query_one(dest, "SELECT COUNT(*) FROM sales.customers WHERE id = 11") == 1
    # run 1 imported customers 6-10; Kate makes 6 total
    assert _query_one(dest, "SELECT COUNT(*) FROM sales.customers") == 6
    assert _query_one(dest, "SELECT COUNT(*) FROM sales.orders WHERE id = 31") == 1
    assert (
        _query_one(dest, "SELECT COUNT(*) FROM sales.order_lines WHERE order_id = 31")
        == 1
    )
    assert (
        _query_one(dest, "SELECT COUNT(*) FROM inventory.products WHERE id = 21") == 1
    )


def test_incremental_existing_entities_frozen(incremental_dbs):
    """Top-up semantics: new children of already-imported parents stay out."""
    dest = incremental_dbs
    # order 32 belongs to an already-imported customer: top-up leaves it out
    assert _query_one(dest, "SELECT COUNT(*) FROM sales.orders WHERE id = 32") == 0
    assert (
        _query_one(dest, "SELECT COUNT(*) FROM sales.order_lines WHERE order_id = 32")
        == 0
    )
    assert _query_one(dest, "SELECT COUNT(*) FROM sales.orders") == 16


def test_incremental_multi_fk_new_to_old(incremental_dbs):
    """Multi-FK AND semantics: new order paired with a run-1 order arrives."""
    dest = incremental_dbs
    assert (
        _query_one(
            dest,
            "SELECT COUNT(*) FROM sales.order_transfers"
            " WHERE from_order_id = 31 AND to_order_id = 16",
        )
        == 1
    )
    assert (
        _query_one(
            dest,
            "SELECT COUNT(*) FROM sales.order_transfers WHERE from_order_id = 32",
        )
        == 0
    )
    # run 1's three transfers plus the new one
    assert _query_one(dest, "SELECT COUNT(*) FROM sales.order_transfers") == 4


def test_incremental_downstream_scan_restricted(incremental_dbs):
    """Product 22 is referenced only by rows topup never re-reads: an old
    child line repointed source-side and a new child of a frozen order. A
    downstream scan that read the source, or the full destination child
    table's source counterparts, would pull it; the delta-restricted scan
    must not."""
    dest = incremental_dbs
    assert (
        _query_one(dest, "SELECT COUNT(*) FROM inventory.products WHERE id = 22") == 0
    )
    assert (
        _query_one(dest, "SELECT COUNT(*) FROM sales.order_lines WHERE product_id = 22")
        == 0
    )


def test_incremental_upsert_refreshes_reread_rows(incremental_dbs):
    """Rows a topup run re-reads (direct-target matches, passthrough tables)
    refresh in place instead of keeping the run-1 values."""
    dest = incremental_dbs
    assert (
        _query_one(dest, "SELECT name FROM sales.customers WHERE id = 6")
        == "Frank Updated"
    )
    assert (
        float(
            _query_one(dest, "SELECT tax_rate FROM public.regions WHERE code = 'US-W'")
        )
        == 0.123
    )


def test_incremental_integrity_and_cleanup(incremental_dbs):
    """No duplicates, no FK orphans, and the delta schema is dropped."""
    dest = incremental_dbs
    for table in ["sales.customers", "sales.orders", "sales.order_lines"]:
        total = _query_one(dest, f"SELECT COUNT(*) FROM {table}")
        distinct = _query_one(dest, f"SELECT COUNT(DISTINCT id) FROM {table}")
        assert total == distinct, f"{table}: duplicate rows after incremental run"
    orphans = _query_one(
        dest,
        """
        SELECT COUNT(*) FROM sales.orders o
        WHERE NOT EXISTS (
            SELECT 1 FROM sales.customers c WHERE c.id = o.customer_id
        )
        """,
    )
    assert orphans == 0
    line_orphans = _query_one(
        dest,
        """
        SELECT COUNT(*) FROM sales.order_lines ol
        WHERE NOT EXISTS (SELECT 1 FROM sales.orders o WHERE o.id = ol.order_id)
           OR NOT EXISTS (SELECT 1 FROM inventory.products p WHERE p.id = ol.product_id)
        """,
    )
    assert line_orphans == 0
    leftover = _query_one(
        dest,
        "SELECT COUNT(*) FROM pg_namespace WHERE nspname = '_condenser'",
    )
    assert leftover == 0


def test_incremental_report_handles_new_disconnected_source_table(capsys):
    """A source-only table skipped by topup must not break final reporting."""
    param = {
        "use_temp_tables": False,
        "use_copy_protocol": False,
        "suffix_override": "_incr_report",
        "config_overrides": {"keep_disconnected_tables": False},
    }
    source_db = SOURCE_DB + "_incr_report"
    dest_db = DEST_DB + "_incr_report"
    try:
        source_db, dest_db = _run_subsetter(**param)

        source = psycopg.connect(
            dbname=source_db,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
        )
        with source.cursor() as cur:
            cur.execute("CREATE TABLE public.later_unconfigured (note text)")
            cur.execute("INSERT INTO public.later_unconfigured VALUES ('source only')")
        source.commit()
        source.close()

        _run_subsetter(**param, mode="topup")

        config = config_reader.get_config()
        source_dbc = DbConnect(config.db_type, config.source_db_connection_info)
        destination_dbc = DbConnect(
            config.db_type, config.destination_db_connection_info
        )
        db_helper = database_helper.get_specific_helper()
        all_tables = db_helper.list_all_tables(source_dbc)

        result_tabulator.tabulate(source_dbc, destination_dbc, all_tables)

        assert "public.later_unconfigured" in capsys.readouterr().out
        dest = psycopg.connect(
            dbname=dest_db,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
        )
        assert _query_one(
            dest, "SELECT to_regclass('public.later_unconfigured') IS NULL"
        )
        dest.close()
    finally:
        _drop_test_databases(source_db, dest_db)


def test_destination_mode_defaults_to_recreate():
    with open(CONFIG_JSON, "r") as fp:
        raw = json.load(fp)
    raw.pop("destination_mode", None)

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


@pytest.mark.parametrize(
    ("suffix", "augmentation", "error"),
    [
        (
            "_fka_unknown_table",
            {
                "fk_table": "sales.misspelled_history",
                "fk_columns": ["customer_id"],
                "target_table": "sales.customers",
                "target_columns": ["id"],
            },
            "unknown or excluded tables: sales.misspelled_history",
        ),
        (
            "_fka_unknown_column",
            {
                "fk_table": "sales.customer_status_history",
                "fk_columns": ["misspelled_customer_id"],
                "target_table": "sales.customers",
                "target_columns": ["id"],
            },
            "unknown columns on sales.customer_status_history",
        ),
    ],
)
def test_fk_augmentation_rejects_stale_config_before_transfer(
    suffix, augmentation, error
):
    source_db = SOURCE_DB + suffix
    dest_db = DEST_DB + suffix
    try:
        with pytest.raises(ValueError, match=error):
            _run_subsetter(
                use_temp_tables=False,
                use_copy_protocol=True,
                suffix_override=suffix,
                config_overrides={"fk_augmentation": [augmentation]},
            )
    finally:
        _drop_test_databases(source_db, dest_db)


def test_incremental_config_hash_canonicalizes_unordered_lists():
    with open(CONFIG_JSON, "r") as fp:
        raw = json.load(fp)
    raw["passthrough_tables"] = [
        "public.regions",
        "public.feature_flags",
        "public.regions",
    ]
    raw["excluded_tables"] = ["sales.order_lines", "sales.order_transfers"]
    raw["dependency_breaks"].append(
        {"fk_table": "sales.orders", "target_table": "sales.customers"}
    )
    raw["fk_augmentation"] = [
        {
            "fk_table": "sales.unique_history",
            "fk_columns": ["customer_id"],
            "target_table": "sales.customers",
            "target_columns": ["id"],
        },
        {
            "fk_table": "sales.orders",
            "fk_columns": ["warehouse_id"],
            "target_table": "inventory.warehouses",
            "target_columns": ["id"],
        },
    ]
    raw["incremental_keys"] = [
        {"table": "sales.unique_history", "columns": ["history_id"]},
        {"table": "sales.other_history", "columns": ["owner_id", "version"]},
    ]

    try:
        config_reader.config = config_reader._raw_dict_to_config(raw)
        assert config_reader.config.passthrough_tables == [
            "public.regions",
            "public.feature_flags",
        ]
        helper = database_helper.get_specific_helper()
        identity_map = {"sales.customers": ["id"]}
        first_hash = helper._incremental_config_hash(identity_map)

        raw["passthrough_tables"] = [
            "public.feature_flags",
            "public.regions",
            "public.feature_flags",
        ]
        for name in (
            "excluded_tables",
            "dependency_breaks",
            "fk_augmentation",
            "incremental_keys",
        ):
            raw[name].reverse()
        config_reader.config = config_reader._raw_dict_to_config(raw)

        assert helper._incremental_config_hash(identity_map) == first_hash
    finally:
        config_reader.reset_config()


# ============================================================
# UNIQUE-KEY INCREMENTAL IDENTITY (NO PRIMARY KEY)
# ============================================================


UNIQUE_HISTORY_SETUP = [
    "CREATE TABLE sales.unique_history ("
    " history_id INT NOT NULL,"
    " customer_id INT NOT NULL REFERENCES sales.customers(id),"
    " version_number INT NOT NULL,"
    " status TEXT NOT NULL,"
    " active BOOLEAN NOT NULL,"
    " UNIQUE (history_id),"
    " UNIQUE (customer_id, version_number))",
    "CREATE UNIQUE INDEX unique_history_active"
    " ON sales.unique_history (customer_id) WHERE active",
    "INSERT INTO sales.unique_history VALUES"
    " (600, 6, 1, 'silver', true),"
    " (700, 7, 1, 'bronze', true)",
]


@pytest.fixture(
    scope="module",
    params=[
        {
            "use_temp_tables": False,
            "use_copy_protocol": False,
            "suffix_override": "_unique_grow",
            "mode": "grow",
        },
        {
            "use_temp_tables": True,
            "use_copy_protocol": False,
            "suffix_override": "_unique_tt",
            "mode": "grow",
        },
        {
            "use_temp_tables": False,
            "use_copy_protocol": True,
            "suffix_override": "_unique_cp",
            "mode": "grow",
        },
        {
            "use_temp_tables": False,
            "use_copy_protocol": True,
            "suffix_override": "_unique_topup",
            "mode": "topup",
        },
    ],
    ids=["unnest_grow", "temp_tables_grow", "copy_grow", "copy_topup"],
)
def unique_identity_dbs(request):
    param = dict(request.param)
    mode = param.pop("mode")
    config_overrides = {
        "incremental_keys": [
            {"table": "sales.unique_history", "columns": ["history_id"]}
        ]
    }
    if mode == "topup":
        config_overrides["initial_targets"] = [
            {"table": "sales.customers", "where": "created_at >= '2025-01-01'"},
            {"table": "sales.unique_history", "where": "customer_id = 6"},
        ]
    param["config_overrides"] = config_overrides
    param["setup_sql"] = UNIQUE_HISTORY_SETUP
    source_db, dest_db = _run_subsetter(**param)

    source = psycopg.connect(
        dbname=source_db,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )
    with source.cursor() as cur:
        cur.execute(
            "UPDATE sales.unique_history SET active = false WHERE history_id = 600"
        )
        cur.execute("INSERT INTO sales.unique_history VALUES (601, 6, 2, 'gold', true)")
        cur.execute(
            "UPDATE sales.unique_history SET status = 'silver-retired'"
            " WHERE history_id = 600"
        )
    source.commit()
    source.close()

    _run_subsetter(**param, mode=mode)

    dest = psycopg.connect(
        dbname=dest_db,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )
    yield dest
    dest.close()

    admin = _admin_conn()
    with admin.cursor() as cur:
        for db in (source_db, dest_db):
            cur.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = %s AND pid <> pg_backend_pid()",
                (db,),
            )
            cur.execute(f"DROP DATABASE IF EXISTS {db}")
    admin.close()


def test_unique_identity_refreshes_and_inserts_history(unique_identity_dbs):
    dest = unique_identity_dbs
    assert (
        _query_one(
            dest,
            "SELECT COUNT(*) FROM sales.unique_history WHERE customer_id = 6",
        )
        == 2
    )
    assert (
        _query_one(
            dest,
            "SELECT status FROM sales.unique_history WHERE customer_id = 6 AND active",
        )
        == "gold"
    )
    assert (
        _query_one(
            dest,
            "SELECT COUNT(*) FROM sales.unique_history"
            " WHERE history_id = 600 AND status = 'silver-retired' AND NOT active",
        )
        == 1
    )


def test_multiple_unique_keys_require_explicit_identity():
    param = {
        "use_temp_tables": False,
        "use_copy_protocol": True,
        "suffix_override": "_unique_ambiguous",
        "setup_sql": UNIQUE_HISTORY_SETUP,
    }
    source_db = SOURCE_DB + "_unique_ambiguous"
    dest_db = DEST_DB + "_unique_ambiguous"
    try:
        source_db, dest_db = _run_subsetter(**param)
        with pytest.raises(ValueError, match="multiple eligible unique keys"):
            _run_subsetter(**param, mode="grow")
    finally:
        _drop_test_databases(source_db, dest_db)


def test_single_unique_key_is_inferred_and_generated_columns_refresh():
    admin = _admin_conn()
    version = _query_one(admin, "SHOW server_version_num")
    admin.close()
    generated_kind = "VIRTUAL" if int(version) >= 180000 else "STORED"
    param = {
        "use_temp_tables": False,
        "use_copy_protocol": True,
        "suffix_override": "_unique_inferred",
        "setup_sql": [
            "CREATE TABLE sales.inferred_history ("
            " history_id INT NOT NULL UNIQUE,"
            " customer_id INT NOT NULL REFERENCES sales.customers(id),"
            " payload TEXT NOT NULL)",
            "INSERT INTO sales.inferred_history VALUES (600, 6, 'old')",
            "CREATE TABLE sales.generated_values ("
            " id INT PRIMARY KEY,"
            " customer_id INT NOT NULL REFERENCES sales.customers(id),"
            " raw_value INT NOT NULL,"
            " doubled INT GENERATED ALWAYS AS (raw_value * 2) " + generated_kind + ")",
            "INSERT INTO sales.generated_values (id, customer_id, raw_value)"
            " VALUES (1, 6, 3)",
        ],
    }
    source_db = SOURCE_DB + "_unique_inferred"
    dest_db = DEST_DB + "_unique_inferred"
    try:
        source_db, dest_db = _run_subsetter(**param)
        source = psycopg.connect(
            dbname=source_db,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
        )
        with source.cursor() as cur:
            cur.execute(
                "UPDATE sales.inferred_history SET payload = 'refreshed'"
                " WHERE history_id = 600"
            )
            cur.execute("INSERT INTO sales.inferred_history VALUES (601, 6, 'new')")
            cur.execute("UPDATE sales.generated_values SET raw_value = 4 WHERE id = 1")
        source.commit()
        source.close()

        _run_subsetter(**param, mode="grow")
        dest = psycopg.connect(
            dbname=dest_db,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
        )
        assert (
            _query_one(
                dest,
                "SELECT COUNT(*) FROM sales.inferred_history"
                " WHERE payload IN ('refreshed', 'new')",
            )
            == 2
        )
        assert (
            _query_one(dest, "SELECT doubled FROM sales.generated_values WHERE id = 1")
            == 8
        )
        dest.close()
    finally:
        _drop_test_databases(source_db, dest_db)


@pytest.mark.parametrize(
    ("suffix", "setup_sql", "error"),
    [
        (
            "_no_identity",
            [
                "CREATE TABLE sales.no_identity_history ("
                " history_id INT UNIQUE,"
                " customer_id INT NOT NULL REFERENCES sales.customers(id))",
                "INSERT INTO sales.no_identity_history VALUES (600, 6)",
            ],
            "no primary key or eligible unique key",
        ),
        (
            "_deferrable_identity",
            [
                "CREATE TABLE sales.deferrable_history ("
                " history_id INT PRIMARY KEY DEFERRABLE,"
                " customer_id INT NOT NULL REFERENCES sales.customers(id))",
                "INSERT INTO sales.deferrable_history VALUES (600, 6)",
            ],
            "Primary key.*cannot be used",
        ),
        (
            "_duplicate_deferrable_arbiter",
            [
                "CREATE TABLE sales.duplicate_arbiter_history ("
                " history_id INT NOT NULL,"
                " customer_id INT NOT NULL REFERENCES sales.customers(id),"
                " CONSTRAINT duplicate_arbiter_immediate UNIQUE (history_id),"
                " CONSTRAINT duplicate_arbiter_deferred UNIQUE (history_id)"
                " DEFERRABLE INITIALLY IMMEDIATE)",
                "INSERT INTO sales.duplicate_arbiter_history VALUES (600, 6)",
            ],
            "also matches deferrable unique index",
        ),
        (
            "_non_key_always_identity",
            [
                "CREATE TABLE sales.non_key_always_identity ("
                " generated_id INT GENERATED ALWAYS AS IDENTITY,"
                " natural_key TEXT NOT NULL UNIQUE,"
                " customer_id INT NOT NULL REFERENCES sales.customers(id))",
                "INSERT INTO sales.non_key_always_identity"
                " (natural_key, customer_id) VALUES ('existing', 6)",
            ],
            "non-key GENERATED ALWAYS AS IDENTITY",
        ),
        (
            "_partitioned",
            [
                "CREATE TABLE public.partitioned_events ("
                " id INT NOT NULL, occurred_on DATE NOT NULL,"
                " PRIMARY KEY (id, occurred_on))"
                " PARTITION BY RANGE (occurred_on)",
                "CREATE TABLE public.partitioned_events_2025"
                " PARTITION OF public.partitioned_events"
                " FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')",
                "INSERT INTO public.partitioned_events VALUES (1, '2025-06-01')",
            ],
            "partitioning",
        ),
        (
            "_inherited",
            [
                "CREATE TABLE public.inherited_events (id INT PRIMARY KEY)",
                "CREATE TABLE public.inherited_events_archive ()"
                " INHERITS (public.inherited_events)",
                "INSERT INTO public.inherited_events_archive VALUES (1)",
            ],
            "table inheritance",
        ),
        (
            "_triggered",
            [
                "CREATE FUNCTION sales.incremental_trigger_probe()"
                " RETURNS trigger LANGUAGE plpgsql AS $$"
                " BEGIN RETURN NEW; END $$",
                "CREATE TRIGGER incremental_trigger_probe"
                " BEFORE INSERT OR UPDATE ON sales.customers"
                " FOR EACH ROW EXECUTE FUNCTION sales.incremental_trigger_probe()",
            ],
            "destination trigger",
        ),
    ],
)
def test_incremental_preflight_rejects_unsafe_schemas(suffix, setup_sql, error):
    param = {
        "use_temp_tables": False,
        "use_copy_protocol": True,
        "suffix_override": suffix,
        "setup_sql": setup_sql,
    }
    source_db = SOURCE_DB + suffix
    dest_db = DEST_DB + suffix
    try:
        source_db, dest_db = _run_subsetter(**param)
        with pytest.raises(ValueError, match=error):
            _run_subsetter(**param, mode="grow")
    finally:
        _drop_test_databases(source_db, dest_db)


def test_incremental_preflight_ignores_tables_outside_run_scope():
    admin = _admin_conn()
    version = int(_query_one(admin, "SHOW server_version_num"))
    admin.close()
    setup_sql = [
        "CREATE TABLE public.unrelated_events (id INT PRIMARY KEY)",
        "CREATE TABLE public.unrelated_events_archive ()"
        " INHERITS (public.unrelated_events)",
        "CREATE TABLE sales.excluded_triggered ("
        " id INT PRIMARY KEY,"
        " customer_id INT NOT NULL REFERENCES sales.customers(id))",
        "CREATE FUNCTION sales.excluded_trigger_probe()"
        " RETURNS trigger LANGUAGE plpgsql AS $$"
        " BEGIN RETURN NEW; END $$",
        "CREATE TRIGGER excluded_trigger_probe"
        " BEFORE INSERT OR UPDATE ON sales.excluded_triggered"
        " FOR EACH ROW EXECUTE FUNCTION sales.excluded_trigger_probe()",
    ]
    if version >= 180000:
        setup_sql.extend(
            [
                "CREATE EXTENSION btree_gist",
                "CREATE TABLE public.unrelated_temporal ("
                " entity_id INT, valid_at daterange,"
                " PRIMARY KEY (entity_id, valid_at WITHOUT OVERLAPS))",
            ]
        )

    param = {
        "use_temp_tables": False,
        "use_copy_protocol": True,
        "suffix_override": "_unrelated_unsafe",
        "setup_sql": setup_sql,
        "config_overrides": {
            "keep_disconnected_tables": False,
            "excluded_tables": ["sales.excluded_triggered"],
        },
    }
    source_db = SOURCE_DB + "_unrelated_unsafe"
    dest_db = DEST_DB + "_unrelated_unsafe"
    try:
        source_db, dest_db = _run_subsetter(**param)
        _run_subsetter(**param, mode="grow")
    finally:
        _drop_test_databases(source_db, dest_db)


def test_postgres_18_temporal_identity_is_rejected():
    admin = _admin_conn()
    version = int(_query_one(admin, "SHOW server_version_num"))
    admin.close()
    if version < 180000:
        pytest.skip("WITHOUT OVERLAPS requires PostgreSQL 18")

    param = {
        "use_temp_tables": False,
        "use_copy_protocol": True,
        "suffix_override": "_temporal_identity",
        "setup_sql": [
            "CREATE EXTENSION btree_gist",
            "CREATE TABLE public.temporal_identity ("
            " entity_id INT, valid_at daterange, payload TEXT NOT NULL,"
            " PRIMARY KEY (entity_id, valid_at WITHOUT OVERLAPS))",
            "INSERT INTO public.temporal_identity VALUES"
            " (1, '[2025-01-01,2025-02-01)', 'value')",
        ],
    }
    source_db = SOURCE_DB + "_temporal_identity"
    dest_db = DEST_DB + "_temporal_identity"
    try:
        source_db, dest_db = _run_subsetter(**param)
        with pytest.raises(ValueError, match="temporal constraint"):
            _run_subsetter(**param, mode="grow")
    finally:
        _drop_test_databases(source_db, dest_db)


def test_failed_topup_resumes_retained_journal(monkeypatch):
    param = {
        "use_temp_tables": False,
        "use_copy_protocol": True,
        "suffix_override": "_resume_topup",
    }
    source_db = SOURCE_DB + "_resume_topup"
    dest_db = DEST_DB + "_resume_topup"
    try:
        source_db, dest_db = _run_subsetter(**param)
        source = psycopg.connect(
            dbname=source_db,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
        )
        with source.cursor() as cur:
            cur.execute(
                "INSERT INTO sales.customers (name, email, region, created_at)"
                " VALUES ('Resume', 'resume@example.com', 'US-W', '2025-10-01')"
            )
        source.commit()
        source.close()

        helper = database_helper.get_specific_helper()
        real_update_sequences = helper.update_sequence_numbering

        def fail_after_transfer(*args, **kwargs):
            raise RuntimeError("late test failure")

        monkeypatch.setattr(helper, "update_sequence_numbering", fail_after_transfer)
        with pytest.raises(RuntimeError, match="late test failure"):
            _run_subsetter(**param, mode="topup")
        monkeypatch.setattr(helper, "update_sequence_numbering", real_update_sequences)

        dest = psycopg.connect(
            dbname=dest_db,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
        )
        assert _query_one(dest, "SELECT to_regnamespace('_condenser') IS NOT NULL")
        assert (
            _query_one(
                dest,
                "SELECT COUNT(*) FROM pg_constraint WHERE contype = 'f'",
            )
            > 0
        )
        dest.close()

        with pytest.raises(RuntimeError, match="different configuration"):
            _run_subsetter(**param, mode="topup", parallel_read_workers=2)

        _run_subsetter(**param, mode="topup")
        dest = psycopg.connect(
            dbname=dest_db,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
        )
        assert _query_one(dest, "SELECT to_regnamespace('_condenser') IS NULL")
        assert (
            _query_one(
                dest,
                "SELECT COUNT(*) FROM sales.customers"
                " WHERE email = 'resume@example.com'",
            )
            == 1
        )
        dest.close()
    finally:
        _drop_test_databases(source_db, dest_db)


def test_recreate_clears_retained_incremental_journal(monkeypatch):
    param = {
        "use_temp_tables": False,
        "use_copy_protocol": True,
        "suffix_override": "_recreate_journal",
    }
    source_db = SOURCE_DB + "_recreate_journal"
    dest_db = DEST_DB + "_recreate_journal"
    try:
        source_db, dest_db = _run_subsetter(**param)
        helper = database_helper.get_specific_helper()
        real_update_sequences = helper.update_sequence_numbering

        def fail_after_transfer(*args, **kwargs):
            raise RuntimeError("late test failure")

        monkeypatch.setattr(helper, "update_sequence_numbering", fail_after_transfer)
        with pytest.raises(RuntimeError, match="late test failure"):
            _run_subsetter(**param, mode="grow")
        monkeypatch.setattr(helper, "update_sequence_numbering", real_update_sequences)

        _run_subsetter(**param, mode="recreate", reset_databases=False)
        dest = psycopg.connect(
            dbname=dest_db,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
        )
        assert _query_one(dest, "SELECT to_regnamespace('_condenser') IS NULL")
        dest.close()
    finally:
        _drop_test_databases(source_db, dest_db)


def test_fk_restore_failure_retains_journal(monkeypatch):
    param = {
        "use_temp_tables": False,
        "use_copy_protocol": True,
        "suffix_override": "_resume_fk",
    }
    source_db = SOURCE_DB + "_resume_fk"
    dest_db = DEST_DB + "_resume_fk"
    try:
        source_db, dest_db = _run_subsetter(**param)
        helper = database_helper.get_specific_helper()
        real_restore = helper.restore_fk_constraints

        def fail_restore(*args, **kwargs):
            raise RuntimeError("restore test failure")

        monkeypatch.setattr(helper, "restore_fk_constraints", fail_restore)
        with pytest.raises(RuntimeError, match="restore test failure"):
            _run_subsetter(**param, mode="grow")
        monkeypatch.setattr(helper, "restore_fk_constraints", real_restore)

        dest = psycopg.connect(
            dbname=dest_db,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
        )
        assert _query_one(dest, "SELECT to_regnamespace('_condenser') IS NOT NULL")
        dest.close()

        _run_subsetter(**param, mode="grow")
        dest = psycopg.connect(
            dbname=dest_db,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
        )
        assert _query_one(dest, "SELECT to_regnamespace('_condenser') IS NULL")
        assert (
            _query_one(
                dest,
                "SELECT COUNT(*) FROM pg_constraint WHERE contype = 'f'",
            )
            > 0
        )
        dest.close()
    finally:
        _drop_test_databases(source_db, dest_db)


def test_fk_restore_rejects_same_name_with_different_definition():
    param = {
        "use_temp_tables": False,
        "use_copy_protocol": True,
        "suffix_override": "_fk_name_collision",
    }
    source_db = SOURCE_DB + "_fk_name_collision"
    dest_db = DEST_DB + "_fk_name_collision"
    try:
        source_db, dest_db = _run_subsetter(**param)
        collision = (
            'ALTER TABLE sales.orders ADD CONSTRAINT "orders_customer_id_fkey"'
            " FOREIGN KEY (warehouse_id) REFERENCES inventory.warehouses(id)"
        )
        with pytest.raises(RuntimeError, match="different definition"):
            _run_subsetter(
                **param,
                mode="grow",
                config_overrides={"post_subset_sql": [collision]},
            )

        with pytest.raises(RuntimeError, match="Retained foreign key definition"):
            _run_subsetter(
                **param,
                mode="grow",
                config_overrides={"post_subset_sql": [collision]},
            )

        dest = psycopg.connect(
            dbname=dest_db,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
        )
        assert _query_one(dest, "SELECT to_regnamespace('_condenser') IS NOT NULL")
        assert _query_one(
            dest,
            "SELECT definition LIKE 'FOREIGN KEY (customer_id)%'"
            " FROM _condenser.fk_backup WHERE schema_name = 'sales'"
            " AND table_name = 'orders'"
            " AND constraint_name = 'orders_customer_id_fkey'",
        )
        dest.close()
    finally:
        _drop_test_databases(source_db, dest_db)


# ============================================================
# GROW MODE RE-RUNS
# ============================================================


def _apply_grow_mutations(source_db):
    """Source-side changes applied between run 1 and the grow re-run (see
    the grow_dbs docstring for the expected semantics of each)."""
    src = psycopg.connect(
        dbname=source_db,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )
    with src.cursor() as cur:
        cur.execute(
            "INSERT INTO sales.customers (name, email, region, created_at)"
            " VALUES ('Kate', 'kate@example.com', 'US-W', '2025-09-01')"
        )
        cur.execute(
            "INSERT INTO inventory.products (name, price, metadata)"
            " VALUES ('Widget Z', 19.99, NULL), ('Widget AA', 5.00, NULL)"
        )
        cur.execute(
            "INSERT INTO inventory.warehouses (name, location)"
            " VALUES ('Annex', 'Boise')"
        )
        cur.execute(
            "INSERT INTO sales.orders (customer_id, warehouse_id, ordered_at)"
            " VALUES (11, 1, '2025-09-02'), (6, 2, '2025-09-03')"
        )
        cur.execute(
            "INSERT INTO sales.order_lines (order_id, product_id, quantity, unit_price)"
            " VALUES (31, 21, 1, 19.99), (32, 22, 1, 5.00), (16, 21, 2, 19.99)"
        )
        cur.execute(
            "INSERT INTO sales.order_transfers (from_order_id, to_order_id, reason)"
            " VALUES (31, 16, 'new to old'), (32, 16, 'grown pair')"
        )
        cur.execute("UPDATE sales.orders SET ordered_at = '2025-02-28' WHERE id = 16")
        cur.execute("UPDATE sales.orders SET warehouse_id = 6 WHERE id = 17")
        cur.execute(
            "UPDATE sales.customer_status_history SET active = false"
            " WHERE customer_id = 6 AND active"
        )
        cur.execute(
            "INSERT INTO sales.customer_status_history (customer_id, status, active)"
            " VALUES (6, 'gold', true)"
        )
        # rewrite the retired row wholesale (delete + re-insert, same id) so
        # its heap tuple physically follows the new gold row: every read
        # path then fetches gold first, which only loads cleanly because
        # refreshes are applied before inserts
        cur.execute(
            "DELETE FROM sales.customer_status_history"
            " WHERE customer_id = 6 AND status = 'silver'"
        )
        cur.execute(
            "INSERT INTO sales.customer_status_history"
            " (id, customer_id, status, active)"
            " VALUES (7, 6, 'silver-retired', false)"
        )
    src.commit()
    src.close()


@pytest.fixture(
    scope="module",
    params=[
        {
            "use_temp_tables": False,
            "use_copy_protocol": False,
            "suffix_override": "_grow",
        },
        {
            "use_temp_tables": True,
            "use_copy_protocol": False,
            "suffix_override": "_grow_tt",
        },
        {
            "use_temp_tables": False,
            "use_copy_protocol": True,
            "suffix_override": "_grow_cp",
        },
    ],
    ids=["unnest_grow", "temp_tables_grow", "copy_protocol_grow"],
)
def grow_dbs(request):
    """Run once, mutate the source, re-run in grow mode, then grow again.

    Grow semantics: everything topup does, plus new children/descendants of
    already-imported rows, plus in-place refresh of re-read rows.

    Source mutations between run 1 and run 2:
    - customer 11 (Kate) matches the target WHERE -> arrives (as in topup)
    - order 31 belongs to Kate -> arrives
    - order 32 belongs to customer 6 (imported in run 1) -> ARRIVES in grow
      (unlike topup), with its line and downstream product 22
    - order_line (16, 21) is a new child of run-1 order 16 -> arrives, with
      downstream product 21
    - transfers 31->16 and 32->16 both satisfy AND semantics -> arrive
    - order 16's ordered_at changes -> re-read row refreshes in place
    - order 17's warehouse_id repoints to new warehouse 6 -> the upsert
      changes an FK column on a re-read row, and downstream must backfill
      warehouse 6 (updated rows enter the delta as _inserted = false)
    - customer 6's active status row is deactivated and replaced ('silver'
      retired, 'gold' inserted), then the retired row is touched again so
      its refresh physically follows the new row in scan order -> the run
      succeeds only because refreshes are applied before new-row inserts
      under the live partial unique index (one active row per customer)

    The third run has no mutations and must change nothing (idempotency).
    """
    source_db, dest_db = _run_subsetter(**request.param)

    _apply_grow_mutations(source_db)

    _run_subsetter(**request.param, mode="grow")
    _run_subsetter(**request.param, mode="grow")

    dest = psycopg.connect(
        dbname=dest_db,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )
    yield dest
    dest.close()

    admin = _admin_conn()
    with admin.cursor() as cur:
        for db in (source_db, dest_db):
            cur.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = %s AND pid <> pg_backend_pid()",
                (db,),
            )
            cur.execute(f"DROP DATABASE IF EXISTS {db}")
    admin.close()


def test_grow_new_children_of_old_parents_arrive(grow_dbs):
    """The defining difference from topup: order 32 (new child of run-1
    customer 6) is picked up."""
    dest = grow_dbs
    assert _query_one(dest, "SELECT COUNT(*) FROM sales.orders WHERE id = 32") == 1
    # run 1 imported orders 16-30; grow adds 31 and 32
    assert _query_one(dest, "SELECT COUNT(*) FROM sales.orders") == 17


def test_grow_descendant_closure(grow_dbs):
    """New rows pulled by grow bring their descendants and downstream refs."""
    dest = grow_dbs
    assert (
        _query_one(dest, "SELECT COUNT(*) FROM sales.order_lines WHERE order_id = 32")
        == 1
    )
    assert (
        _query_one(dest, "SELECT COUNT(*) FROM inventory.products WHERE id = 22") == 1
    )
    assert (
        _query_one(
            dest,
            "SELECT COUNT(*) FROM sales.order_lines"
            " WHERE order_id = 16 AND product_id = 21",
        )
        == 1
    )
    assert (
        _query_one(dest, "SELECT COUNT(*) FROM inventory.products WHERE id = 21") == 1
    )


def test_grow_upsert_refreshes_and_backfills_fk(grow_dbs):
    """Re-read rows refresh in place; an FK column changed by the upsert gets
    its new parent backfilled by the downstream pass."""
    dest = grow_dbs
    assert (
        _query_one(
            dest, "SELECT ordered_at::date::text FROM sales.orders WHERE id = 16"
        )
        == "2025-02-28"
    )
    assert _query_one(dest, "SELECT warehouse_id FROM sales.orders WHERE id = 17") == 6
    assert (
        _query_one(dest, "SELECT COUNT(*) FROM inventory.warehouses WHERE id = 6") == 1
    )


def test_grow_history_deactivate_and_replace(grow_dbs):
    """The active-flag pattern: prod retired customer 6's 'silver' row and
    inserted 'gold'. The partial unique index (one active row per customer)
    stays live during the run, and the retired row's refresh physically
    follows the new row in scan order — the run only succeeds because
    refreshes are applied before new-row inserts."""
    dest = grow_dbs
    assert (
        _query_one(
            dest,
            "SELECT COUNT(*) FROM sales.customer_status_history"
            " WHERE customer_id = 6 AND active",
        )
        == 1
    )
    assert (
        _query_one(
            dest,
            "SELECT status FROM sales.customer_status_history"
            " WHERE customer_id = 6 AND active",
        )
        == "gold"
    )
    assert (
        _query_one(
            dest,
            "SELECT COUNT(*) FROM sales.customer_status_history"
            " WHERE customer_id = 6 AND status = 'silver-retired' AND NOT active",
        )
        == 1
    )


def test_grow_multi_fk_pairs(grow_dbs):
    dest = grow_dbs
    assert (
        _query_one(
            dest,
            "SELECT COUNT(*) FROM sales.order_transfers"
            " WHERE (from_order_id, to_order_id) IN ((31, 16), (32, 16))",
        )
        == 2
    )
    # run 1's three transfers plus the two new ones
    assert _query_one(dest, "SELECT COUNT(*) FROM sales.order_transfers") == 5


def test_grow_scope_still_bounded(grow_dbs):
    """Grow does not widen the initial-target filter: pre-2025 customers and
    their orders stay out."""
    dest = grow_dbs
    assert _query_one(dest, "SELECT COUNT(*) FROM sales.customers") == 6
    assert _query_one(dest, "SELECT COUNT(*) FROM sales.customers WHERE id <= 5") == 0
    assert _query_one(dest, "SELECT COUNT(*) FROM sales.orders WHERE id <= 15") == 0


def test_grow_idempotent_and_cleanup(grow_dbs):
    """The mutation-free third grow run added nothing; no duplicates, no FK
    orphans, delta schema dropped."""
    dest = grow_dbs
    for table in ["sales.customers", "sales.orders", "sales.order_lines"]:
        total = _query_one(dest, f"SELECT COUNT(*) FROM {table}")
        distinct = _query_one(dest, f"SELECT COUNT(DISTINCT id) FROM {table}")
        assert total == distinct, f"{table}: duplicate rows after grow reruns"
    orphans = _query_one(
        dest,
        """
        SELECT COUNT(*) FROM sales.order_lines ol
        WHERE NOT EXISTS (SELECT 1 FROM sales.orders o WHERE o.id = ol.order_id)
           OR NOT EXISTS (SELECT 1 FROM inventory.products p WHERE p.id = ol.product_id)
        """,
    )
    assert orphans == 0
    warehouse_orphans = _query_one(
        dest,
        """
        SELECT COUNT(*) FROM sales.orders o
        WHERE o.warehouse_id IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM inventory.warehouses w WHERE w.id = o.warehouse_id
          )
        """,
    )
    assert warehouse_orphans == 0
    leftover = _query_one(
        dest,
        "SELECT COUNT(*) FROM pg_namespace WHERE nspname = '_condenser'",
    )
    assert leftover == 0


@pytest.fixture(scope="module")
def grow_parallel_dbs():
    """Grow re-run with parallel_read_workers=4 and a customers table bulked
    past the ctid page threshold. customers carries a secondary unique index
    (email), so its incremental split runs the two-phase path: staged rows,
    parallel refresh phase, barrier, parallel insert phase. The history
    table's deactivate-and-replace flows through the ordered batch path as
    in grow_dbs."""
    param = {
        "use_temp_tables": False,
        "use_copy_protocol": True,
        "suffix_override": "_grow_par",
        "parallel_read_workers": 4,
        "setup_sql": [
            "INSERT INTO sales.customers (id, name, email, region, created_at)"
            " SELECT i, 'Bulk ' || i, 'bulk' || i || '@example.com', 'US-W',"
            " '2025-05-01' FROM generate_series(1000, 20999) AS i",
            # all of this table's columns are PK members, and it carries a
            # secondary unique index: the two-phase split must classify rows
            # by the delta's PK (there is no upsert_pk for it). No FK — a
            # passthrough table must not also be upstream-subsetted, or a
            # recreate run would copy it twice with no index to dedup on.
            "CREATE TABLE sales.customer_tags ("
            " customer_id INT NOT NULL,"
            " tag VARCHAR NOT NULL,"
            " PRIMARY KEY (customer_id, tag))",
            "CREATE UNIQUE INDEX idx_customer_tags_reverse"
            " ON sales.customer_tags (tag, customer_id)",
            "INSERT INTO sales.customer_tags"
            " SELECT i, 'tag-' || (i % 7) FROM generate_series(1000, 20999) AS i",
        ],
        "config_overrides": {
            "passthrough_tables": ["public.regions", "sales.customer_tags"]
        },
    }
    source_db, dest_db = _run_subsetter(**param)

    _apply_grow_mutations(source_db)
    src = psycopg.connect(
        dbname=source_db,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )
    with src.cursor() as cur:
        # refreshes through the two-phase split...
        cur.execute(
            "UPDATE sales.customers SET name = name || ' updated'"
            " WHERE id BETWEEN 1000 AND 1099"
        )
        # ...and bulk inserts through its insert phase
        cur.execute(
            "INSERT INTO sales.customers (id, name, email, region, created_at)"
            " SELECT i, 'Bulk ' || i, 'bulk' || i || '@example.com', 'US-W',"
            " '2025-05-01' FROM generate_series(30000, 34999) AS i"
        )
        cur.execute(
            "INSERT INTO sales.customer_tags"
            " SELECT i, 'tag-new' FROM generate_series(30000, 34999) AS i"
        )
        # the ctid split reads pg_class.relpages, which only VACUUM/ANALYZE
        # refresh — without this the freshly bulked tables report 0 pages
        # and the split silently falls back to sequential
        cur.execute("ANALYZE sales.customers")
        cur.execute("ANALYZE sales.customer_tags")
    src.commit()
    src.close()

    _run_subsetter(**param, mode="grow")

    dest = psycopg.connect(
        dbname=dest_db,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )
    yield dest
    dest.close()

    admin = _admin_conn()
    with admin.cursor() as cur:
        for db in (source_db, dest_db):
            cur.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = %s AND pid <> pg_backend_pid()",
                (db,),
            )
            cur.execute(f"DROP DATABASE IF EXISTS {db}")
    admin.close()


def test_grow_parallel_matches_sequential_semantics(grow_parallel_dbs):
    dest = grow_parallel_dbs
    assert _query_one(dest, "SELECT COUNT(*) FROM sales.orders WHERE id = 32") == 1
    assert _query_one(dest, "SELECT COUNT(*) FROM sales.orders") == 17
    assert (
        _query_one(
            dest,
            "SELECT status FROM sales.customer_status_history"
            " WHERE customer_id = 6 AND active",
        )
        == "gold"
    )
    for table in ["sales.customers", "sales.orders", "sales.order_lines"]:
        total = _query_one(dest, f"SELECT COUNT(*) FROM {table}")
        distinct = _query_one(dest, f"SELECT COUNT(DISTINCT id) FROM {table}")
        assert total == distinct, f"{table}: duplicate rows after parallel grow"


def test_grow_parallel_two_phase_split(grow_parallel_dbs):
    """The bulked customers table exceeds the page threshold, so its grow
    re-read runs the two-phase ctid split: refreshes land in the parallel
    refresh phase, new rows in the insert phase."""
    dest = grow_parallel_dbs
    # run 1: customers 6-10 + 20000 bulk; grow adds Kate + 5000 bulk
    assert _query_one(dest, "SELECT COUNT(*) FROM sales.customers") == 25006
    assert (
        _query_one(dest, "SELECT name FROM sales.customers WHERE id = 1000")
        == "Bulk 1000 updated"
    )
    assert (
        _query_one(
            dest,
            "SELECT COUNT(*) FROM sales.customers WHERE id BETWEEN 30000 AND 34999",
        )
        == 5000
    )
    assert (
        _query_one(dest, "SELECT COUNT(DISTINCT email) FROM sales.customers") == 25006
    )
    # the all-PK-column passthrough table went through the same two-phase
    # split (refresh phase is a no-op for it; insert phase adds the new rows)
    assert _query_one(dest, "SELECT COUNT(*) FROM sales.customer_tags") == 25000
    assert (
        _query_one(
            dest,
            "SELECT COUNT(DISTINCT (customer_id, tag)) FROM sales.customer_tags",
        )
        == 25000
    )


@pytest.fixture(scope="module")
def grow_batch_boundary_dbs():
    """Deactivate-and-replace pair straddling a fetchmany boundary.

    With compute_batch_size patched to 2, one copy of customer 6's history
    rows spans multiple executemany batches, and the retired row's refresh
    can land in a later batch than the new active row's insert. The
    executemany path must stage the whole copy and apply refreshes before
    inserts (like the COPY path) for this grow run to succeed under the
    live partial unique index."""
    import db_condenser.psql_database_helper as helper_mod

    param = {
        "use_temp_tables": False,
        "use_copy_protocol": False,
        "suffix_override": "_grow_bb",
    }
    source_db, dest_db = _run_subsetter(**param)

    src = psycopg.connect(
        dbname=source_db,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )
    with src.cursor() as cur:
        cur.execute(
            "UPDATE sales.customer_status_history SET active = false"
            " WHERE customer_id = 6 AND active"
        )
        cur.execute(
            "INSERT INTO sales.customer_status_history (customer_id, status, active)"
            " VALUES (6, 'gold', true)"
        )
        # rewrite the retired row so its heap tuple follows the new row:
        # scan order then yields gold before the refresh that unblocks it
        cur.execute(
            "DELETE FROM sales.customer_status_history"
            " WHERE customer_id = 6 AND status = 'silver'"
        )
        cur.execute(
            "INSERT INTO sales.customer_status_history"
            " (id, customer_id, status, active)"
            " VALUES (7, 6, 'silver-retired', false)"
        )
    src.commit()
    src.close()

    real_helper_batch = helper_mod.compute_batch_size
    helper_mod.compute_batch_size = lambda column_count: 2
    try:
        _run_subsetter(**param, mode="grow")
    finally:
        helper_mod.compute_batch_size = real_helper_batch

    dest = psycopg.connect(
        dbname=dest_db,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )
    yield dest
    dest.close()

    admin = _admin_conn()
    with admin.cursor() as cur:
        for db in (source_db, dest_db):
            cur.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = %s AND pid <> pg_backend_pid()",
                (db,),
            )
            cur.execute(f"DROP DATABASE IF EXISTS {db}")
    admin.close()


def test_grow_history_pair_across_batch_boundary(grow_batch_boundary_dbs):
    dest = grow_batch_boundary_dbs
    assert (
        _query_one(
            dest,
            "SELECT COUNT(*) FROM sales.customer_status_history"
            " WHERE customer_id = 6 AND active",
        )
        == 1
    )
    assert (
        _query_one(
            dest,
            "SELECT status FROM sales.customer_status_history"
            " WHERE customer_id = 6 AND active",
        )
        == "gold"
    )
    assert (
        _query_one(
            dest,
            "SELECT COUNT(*) FROM sales.customer_status_history"
            " WHERE customer_id = 6 AND status = 'silver-retired' AND NOT active",
        )
        == 1
    )


# ============================================================
# HISTORY / AUDIT TABLE SHAPES
# ============================================================


AUDIT_HISTORY_SETUP = [
    "CREATE SCHEMA auth",
    "CREATE SCHEMA audit",
    "CREATE TABLE auth.users ( id UUID PRIMARY KEY, display_name TEXT NOT NULL)",
    "CREATE TABLE audit.accounts ("
    " tenant_id INT NOT NULL, account_id INT NOT NULL, name TEXT NOT NULL,"
    " PRIMARY KEY (tenant_id, account_id))",
    "CREATE TABLE audit.account_versions ("
    " tenant_id INT NOT NULL, account_id INT NOT NULL, version INT NOT NULL,"
    " valid_from TIMESTAMPTZ NOT NULL, valid_to TIMESTAMPTZ,"
    " active BOOLEAN NOT NULL, state JSONB NOT NULL, actor_id UUID,"
    " PRIMARY KEY (tenant_id, account_id, version),"
    " FOREIGN KEY (tenant_id, account_id)"
    " REFERENCES audit.accounts (tenant_id, account_id),"
    " FOREIGN KEY (actor_id) REFERENCES auth.users (id))",
    "CREATE UNIQUE INDEX account_versions_one_active"
    " ON audit.account_versions (tenant_id, account_id) WHERE active",
    "CREATE TABLE audit.version_comments ("
    " id BIGINT PRIMARY KEY, tenant_id INT NOT NULL, account_id INT NOT NULL,"
    " version INT NOT NULL, body TEXT NOT NULL,"
    " FOREIGN KEY (tenant_id, account_id, version)"
    " REFERENCES audit.account_versions (tenant_id, account_id, version))",
    "CREATE TABLE audit.account_events ("
    " event_id UUID PRIMARY KEY, tenant_id INT NOT NULL, account_id INT NOT NULL,"
    " occurred_at TIMESTAMPTZ NOT NULL, action TEXT NOT NULL,"
    " before_state JSONB, after_state JSONB NOT NULL, actor_id UUID,"
    " FOREIGN KEY (tenant_id, account_id)"
    " REFERENCES audit.accounts (tenant_id, account_id),"
    " FOREIGN KEY (actor_id) REFERENCES auth.users (id))",
    # Deliberately no physical FK: this common audit shape relies on
    # fk_augmentation for ownership. Its stable unique key is selected as the
    # incremental identity because the table has no primary key.
    "CREATE TABLE audit.logical_events ("
    " event_seq BIGINT NOT NULL UNIQUE, tenant_id INT NOT NULL,"
    " account_id INT NOT NULL, payload JSONB NOT NULL)",
    "INSERT INTO auth.users VALUES"
    " ('00000000-0000-0000-0000-000000000001', 'Initial actor'),"
    " ('00000000-0000-0000-0000-000000000002', 'Later actor')",
    "INSERT INTO audit.accounts VALUES"
    " (1, 10, 'selected account'), (2, 20, 'outside tenant')",
    "INSERT INTO audit.account_versions VALUES"
    " (1, 10, 1, '2025-01-01', NULL, true, '{\"tier\": \"silver\"}',"
    "  '00000000-0000-0000-0000-000000000001'),"
    " (2, 20, 1, '2025-01-01', NULL, true, '{\"tier\": \"outside\"}',"
    "  '00000000-0000-0000-0000-000000000001')",
    "INSERT INTO audit.version_comments VALUES"
    " (1, 1, 10, 1, 'initial version comment'),"
    " (2, 2, 20, 1, 'outside version comment')",
    "INSERT INTO audit.account_events VALUES"
    " ('10000000-0000-0000-0000-000000000001', 1, 10, '2025-01-02',"
    "  'created', NULL, '{\"tier\": \"silver\"}',"
    "  '00000000-0000-0000-0000-000000000001'),"
    " ('20000000-0000-0000-0000-000000000001', 2, 20, '2025-01-02',"
    "  'created', NULL, '{\"tier\": \"outside\"}',"
    "  '00000000-0000-0000-0000-000000000001')",
    "INSERT INTO audit.logical_events VALUES"
    ' (1, 1, 10, \'{"source": "initial"}\'),'
    ' (2, 2, 20, \'{"source": "outside"}\')',
]


def _audit_history_config():
    return {
        "initial_targets": [{"table": "audit.accounts", "where": "tenant_id = 1"}],
        "passthrough_tables": [],
        "keep_disconnected_tables": False,
        "fk_augmentation": [
            {
                "fk_table": "audit.logical_events",
                "fk_columns": ["tenant_id", "account_id"],
                "target_table": "audit.accounts",
                "target_columns": ["tenant_id", "account_id"],
            }
        ],
        "incremental_keys": [
            {"table": "audit.logical_events", "columns": ["event_seq"]}
        ],
    }


def _apply_audit_history_mutations(source_db):
    source = psycopg.connect(
        dbname=source_db,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )
    with source.cursor() as cur:
        # Close the current SCD2 row, then add its replacement under a live
        # partial unique index (one active version per tenant/account).
        cur.execute(
            "UPDATE audit.account_versions"
            " SET valid_to = '2025-02-01', active = false"
            " WHERE tenant_id = 1 AND account_id = 10 AND version = 1"
        )
        cur.execute(
            "INSERT INTO audit.account_versions VALUES"
            " (1, 10, 2, '2025-02-01', NULL, true, %s, %s)",
            (
                Json({"tier": "gold", "limits": {"daily": 5000}}),
                "00000000-0000-0000-0000-000000000002",
            ),
        )
        # One new child points at the old historical version; another points
        # at the replacement. Grow must find both through exact composite FKs.
        cur.execute(
            "INSERT INTO audit.version_comments VALUES"
            " (3, 1, 10, 1, 'late comment on old version'),"
            " (4, 1, 10, 2, 'comment on replacement')"
        )
        # Refresh an existing wide event and append another UUID-keyed event.
        cur.execute(
            "UPDATE audit.account_events SET after_state = %s"
            " WHERE event_id = '10000000-0000-0000-0000-000000000001'",
            (Json({"tier": "silver", "reviewed": True}),),
        )
        cur.execute(
            "INSERT INTO audit.account_events VALUES"
            " ('10000000-0000-0000-0000-000000000002', 1, 10, '2025-02-01',"
            " 'tier_changed', %s, %s, %s)",
            (
                Json({"tier": "silver"}),
                Json({"tier": "gold"}),
                "00000000-0000-0000-0000-000000000002",
            ),
        )
        cur.execute(
            "INSERT INTO audit.logical_events VALUES (3, 1, 10, %s)",
            (Json({"source": "logical", "revision": 2}),),
        )

        # A newly matching direct target and all of its audit children should
        # arrive in both incremental modes.
        cur.execute("INSERT INTO audit.accounts VALUES (1, 11, 'new account')")
        cur.execute(
            "INSERT INTO audit.account_versions VALUES"
            " (1, 11, 1, '2025-03-01', NULL, true, %s, NULL)",
            (Json({"tier": "bronze"}),),
        )
        cur.execute(
            "INSERT INTO audit.account_events VALUES"
            " ('11000000-0000-0000-0000-000000000001', 1, 11, '2025-03-01',"
            " 'created', NULL, %s, NULL)",
            (Json({"tier": "bronze"}),),
        )
        cur.execute(
            "INSERT INTO audit.logical_events VALUES (4, 1, 11, %s)",
            (Json({"source": "new-account"}),),
        )
    source.commit()
    source.close()


@pytest.fixture(scope="module", params=["grow", "topup"])
def audit_history_dbs(request):
    mode = request.param
    suffix = "_audit_" + mode
    param = {
        "use_temp_tables": False,
        "use_copy_protocol": True,
        "suffix_override": suffix,
        "setup_sql": AUDIT_HISTORY_SETUP,
        "config_overrides": _audit_history_config(),
    }
    source_db, dest_db = _run_subsetter(**param)
    _apply_audit_history_mutations(source_db)
    _run_subsetter(**param, mode=mode)
    # A mutation-free repeat proves all selected identities are idempotent.
    _run_subsetter(**param, mode=mode)

    dest = psycopg.connect(
        dbname=dest_db,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )
    yield mode, dest
    dest.close()
    _drop_test_databases(source_db, dest_db)


def test_history_new_direct_target_arrives_in_both_incremental_modes(
    audit_history_dbs,
):
    _, dest = audit_history_dbs
    assert (
        _query_one(
            dest,
            "SELECT COUNT(*) FROM audit.accounts"
            " WHERE tenant_id = 1 AND account_id = 11",
        )
        == 1
    )
    assert (
        _query_one(
            dest,
            "SELECT COUNT(*) FROM audit.account_versions"
            " WHERE tenant_id = 1 AND account_id = 11",
        )
        == 1
    )
    assert (
        _query_one(
            dest,
            "SELECT COUNT(*) FROM audit.account_events"
            " WHERE tenant_id = 1 AND account_id = 11",
        )
        == 1
    )
    assert (
        _query_one(
            dest,
            "SELECT COUNT(*) FROM audit.logical_events"
            " WHERE tenant_id = 1 AND account_id = 11",
        )
        == 1
    )


def test_history_existing_parent_follows_mode_contract(audit_history_dbs):
    mode, dest = audit_history_dbs
    expected = 2 if mode == "grow" else 1
    assert (
        _query_one(
            dest,
            "SELECT COUNT(*) FROM audit.account_versions"
            " WHERE tenant_id = 1 AND account_id = 10",
        )
        == expected
    )
    assert (
        _query_one(
            dest,
            "SELECT COUNT(*) FROM audit.account_events"
            " WHERE tenant_id = 1 AND account_id = 10",
        )
        == expected
    )
    assert (
        _query_one(
            dest,
            "SELECT COUNT(*) FROM audit.logical_events"
            " WHERE tenant_id = 1 AND account_id = 10",
        )
        == expected
    )


def test_history_grow_refreshes_scd2_json_and_exact_version_children(
    audit_history_dbs,
):
    mode, dest = audit_history_dbs
    if mode == "grow":
        assert (
            _query_one(
                dest,
                "SELECT state ->> 'tier' FROM audit.account_versions"
                " WHERE tenant_id = 1 AND account_id = 10 AND active",
            )
            == "gold"
        )
        assert (
            _query_one(
                dest,
                "SELECT COUNT(*) FROM audit.account_versions"
                " WHERE tenant_id = 1 AND account_id = 10"
                " AND version = 1 AND NOT active AND valid_to IS NOT NULL",
            )
            == 1
        )
        assert (
            _query_one(
                dest,
                "SELECT (after_state ->> 'reviewed')::boolean"
                " FROM audit.account_events"
                " WHERE event_id = '10000000-0000-0000-0000-000000000001'",
            )
            is True
        )
        assert (
            _query_one(
                dest,
                "SELECT COUNT(*) FROM audit.version_comments"
                " WHERE tenant_id = 1 AND account_id = 10",
            )
            == 3
        )
        assert (
            _query_one(
                dest,
                "SELECT COUNT(*) FROM auth.users"
                " WHERE id = '00000000-0000-0000-0000-000000000002'",
            )
            == 1
        )
    else:
        # Topup intentionally freezes children of the already resident account.
        assert (
            _query_one(
                dest,
                "SELECT state ->> 'tier' FROM audit.account_versions"
                " WHERE tenant_id = 1 AND account_id = 10 AND active",
            )
            == "silver"
        )
        assert (
            _query_one(
                dest,
                "SELECT after_state ? 'reviewed' FROM audit.account_events"
                " WHERE event_id = '10000000-0000-0000-0000-000000000001'",
            )
            is False
        )
        assert (
            _query_one(
                dest,
                "SELECT COUNT(*) FROM audit.version_comments"
                " WHERE tenant_id = 1 AND account_id = 10",
            )
            == 1
        )


def test_history_audit_corpus_has_no_duplicates_or_cross_tenant_leaks(
    audit_history_dbs,
):
    _, dest = audit_history_dbs
    identities = {
        "audit.accounts": "tenant_id, account_id",
        "audit.account_versions": "tenant_id, account_id, version",
        "audit.version_comments": "id",
        "audit.account_events": "event_id",
        "audit.logical_events": "event_seq",
    }
    for table, columns in identities.items():
        total = _query_one(dest, f"SELECT COUNT(*) FROM {table}")
        distinct = _query_one(dest, f"SELECT COUNT(DISTINCT ({columns})) FROM {table}")
        assert total == distinct, f"{table}: duplicate history identities"
    assert (
        _query_one(dest, "SELECT COUNT(*) FROM audit.accounts WHERE tenant_id = 2") == 0
    )
    assert (
        _query_one(
            dest, "SELECT COUNT(*) FROM audit.account_events WHERE tenant_id = 2"
        )
        == 0
    )
    assert _query_one(dest, "SELECT to_regnamespace('_condenser') IS NULL")


def test_failed_history_grow_rejects_partial_config_edits(monkeypatch):
    suffix = "_audit_resume"
    config_overrides = _audit_history_config()
    param = {
        "use_temp_tables": False,
        "use_copy_protocol": True,
        "suffix_override": suffix,
        "setup_sql": AUDIT_HISTORY_SETUP,
        "config_overrides": config_overrides,
    }
    source_db = SOURCE_DB + suffix
    dest_db = DEST_DB + suffix
    try:
        source_db, dest_db = _run_subsetter(**param)
        _apply_audit_history_mutations(source_db)

        helper = database_helper.get_specific_helper()
        real_update_sequences = helper.update_sequence_numbering

        def fail_after_transfer(*args, **kwargs):
            raise RuntimeError("late history test failure")

        monkeypatch.setattr(helper, "update_sequence_numbering", fail_after_transfer)
        with pytest.raises(RuntimeError, match="late history test failure"):
            _run_subsetter(**param, mode="grow")
        monkeypatch.setattr(helper, "update_sequence_numbering", real_update_sequences)

        incomplete_configs = []

        missing_augmentation = _audit_history_config()
        missing_augmentation["fk_augmentation"] = []
        incomplete_configs.append(missing_augmentation)

        missing_identity = _audit_history_config()
        missing_identity["incremental_keys"] = []
        incomplete_configs.append(missing_identity)

        partially_changed_target = _audit_history_config()
        partially_changed_target["initial_targets"] = [
            {"table": "audit.accounts", "where": "tenant_id = 1 AND account_id = 11"}
        ]
        incomplete_configs.append(partially_changed_target)

        for incomplete in incomplete_configs:
            with pytest.raises(RuntimeError, match="different configuration"):
                _run_subsetter(
                    **{**param, "config_overrides": incomplete},
                    mode="grow",
                )

        # The exact original effective config can still resume and clean up.
        _run_subsetter(**param, mode="grow")
        dest = psycopg.connect(
            dbname=dest_db,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
        )
        assert (
            _query_one(
                dest,
                "SELECT COUNT(*) FROM audit.logical_events WHERE event_seq = 3",
            )
            == 1
        )
        assert (
            _query_one(
                dest,
                "SELECT state ->> 'tier' FROM audit.account_versions"
                " WHERE tenant_id = 1 AND account_id = 10 AND active",
            )
            == "gold"
        )
        assert _query_one(dest, "SELECT to_regnamespace('_condenser') IS NULL")
        dest.close()
    finally:
        _drop_test_databases(source_db, dest_db)


# ============================================================
# COMPOSITE-PK DELTA JOIN (downstream restriction)
# ============================================================


@pytest.fixture(scope="module")
def incremental_composite_dbs():
    """Exercise the downstream delta join on a composite-PK child.

    stock_levels (PK warehouse_id, product_id) is made a passthrough table so
    a topup re-run inserts a new stock row, and the downstream pass must pull
    its product via the composite delta join. Also catches unqualified column
    references in the delta join SQL (warehouse_id/product_id exist in both
    the child and its delta table)."""
    param = {
        "use_temp_tables": False,
        "use_copy_protocol": False,
        "suffix_override": "_incr_comp",
        "config_overrides": {
            "passthrough_tables": ["public.regions", "inventory.stock_levels"]
        },
    }
    source_db, dest_db = _run_subsetter(**param)

    src = psycopg.connect(
        dbname=source_db,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )
    with src.cursor() as cur:
        cur.execute(
            "INSERT INTO inventory.products (name, price, metadata)"
            " VALUES ('Comp Widget', 3.00, NULL)"
        )
        cur.execute(
            "INSERT INTO inventory.stock_levels (warehouse_id, product_id, quantity)"
            " VALUES (2, 21, 7)"
        )
    src.commit()
    src.close()

    _run_subsetter(**param, mode="topup")

    dest = psycopg.connect(
        dbname=dest_db,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )
    yield dest
    dest.close()

    admin = _admin_conn()
    with admin.cursor() as cur:
        for db in (source_db, dest_db):
            cur.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = %s AND pid <> pg_backend_pid()",
                (db,),
            )
            cur.execute(f"DROP DATABASE IF EXISTS {db}")
    admin.close()


def test_composite_pk_downstream_delta_join(incremental_composite_dbs):
    dest = incremental_composite_dbs
    assert (
        _query_one(
            dest,
            "SELECT COUNT(*) FROM inventory.stock_levels"
            " WHERE warehouse_id = 2 AND product_id = 21",
        )
        == 1
    )
    # product 21 arrives only via the composite delta join on stock_levels
    assert (
        _query_one(dest, "SELECT COUNT(*) FROM inventory.products WHERE id = 21") == 1
    )
    total = _query_one(dest, "SELECT COUNT(*) FROM inventory.stock_levels")
    distinct = _query_one(
        dest,
        "SELECT COUNT(DISTINCT (warehouse_id, product_id)) FROM inventory.stock_levels",
    )
    assert total == distinct


# ============================================================
# MULTI-FK PAIRS ACROSS ID-BATCH BOUNDARIES (regression)
# ============================================================


@pytest.fixture(scope="module")
def multi_fk_batch_dbs():
    """Subset a two-FKs-to-one-parent table with a tiny ID batch size.

    parent has 6 rows, link forms a ring (1-2, 2-3, ... 6-1). All parents are
    imported, so AND semantics require every link. With batches of 2 parent
    IDs, a streamed join that binds the same batch to both constraints drops
    every ring edge whose ends fall in different batches.
    """
    import db_condenser.psql_database_helper as helper_mod

    source_db = SOURCE_DB + "_mfk"
    dest_db = DEST_DB + "_mfk"
    admin = _admin_conn()
    with admin.cursor() as cur:
        for db in (source_db, dest_db):
            cur.execute(f"DROP DATABASE IF EXISTS {db}")
            cur.execute(f"CREATE DATABASE {db}")
    admin.close()

    src = _admin_conn(source_db)
    with src.cursor() as cur:
        cur.execute("""
            CREATE TABLE parent (id INT PRIMARY KEY);
            CREATE TABLE link (
                id INT PRIMARY KEY,
                from_id INT REFERENCES parent(id),
                to_id   INT REFERENCES parent(id),
                priority INT NOT NULL DEFAULT 0
            );
            INSERT INTO parent SELECT generate_series(1, 7);
            INSERT INTO link VALUES
                (1, 1, 2, 0), (2, 2, 3, 0), (3, 3, 4, 0),
                (4, 4, 5, 0), (5, 5, 6, 0), (6, 6, 1, 0),
                (7, NULL, 1, 0), (8, 2, NULL, 0),
                (100, 7, 7, 1);
        """)
    src.close()

    raw_config = {
        "db_type": "postgres",
        "initial_targets": [{"table": "public.parent", "where": "id <= 6"}],
        "upstream_filters": [
            {
                "table": "public.link",
                "condition": "link.id < 100 OR link.priority = 1",
            }
        ],
        "source_db_connection_info": {
            "user_name": DB_USER,
            "password": DB_PASSWORD,
            "host": DB_HOST,
            "db_name": source_db,
            "port": DB_PORT,
        },
        "destination_db_connection_info": {
            "user_name": DB_USER,
            "password": DB_PASSWORD,
            "host": DB_HOST,
            "db_name": dest_db,
            "port": DB_PORT,
        },
    }
    config_reader.reset_config()
    config_reader.config = config_reader._raw_dict_to_config(raw_config)
    config = config_reader.get_config()

    real_batch_size = helper_mod.compute_batch_size
    helper_mod.compute_batch_size = lambda column_count: 2
    try:
        source_dbc = DbConnect(config.db_type, config.source_db_connection_info)
        destination_dbc = DbConnect(
            config.db_type, config.destination_db_connection_info
        )
        database = db_creator(config.db_type, source_dbc, destination_dbc)
        database.teardown()
        database.create()
        db_helper = database_helper.get_specific_helper()
        all_tables = db_helper.list_all_tables(source_dbc)
        subsetter = Subset(source_dbc, destination_dbc, all_tables)
        try:
            subsetter.prep_temp_dbs()
            subsetter.run_middle_out()
        finally:
            subsetter.unprep_temp_dbs()
            subsetter.close_connections()
    finally:
        helper_mod.compute_batch_size = real_batch_size

    dest = psycopg.connect(
        dbname=dest_db,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )
    yield dest
    dest.close()

    admin = _admin_conn()
    with admin.cursor() as cur:
        for db in (source_db, dest_db):
            cur.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = %s AND pid <> pg_backend_pid()",
                (db,),
            )
            cur.execute(f"DROP DATABASE IF EXISTS {db}")
    admin.close()


def test_multi_fk_pairs_survive_batch_boundaries(multi_fk_batch_dbs):
    dest = multi_fk_batch_dbs
    assert _query_one(dest, "SELECT COUNT(*) FROM parent") == 6
    # every link's parents are imported, so every link must be included,
    # regardless of which ID batch each parent landed in
    assert _query_one(dest, "SELECT COUNT(*) FROM link") == 8
    assert _query_one(dest, "SELECT COUNT(*) FROM link WHERE id = 100") == 0
