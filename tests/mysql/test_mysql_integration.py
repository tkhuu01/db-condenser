import os
import uuid

import mysql.connector
import pytest

from db_condenser import config_reader, database_helper, mysql_database_helper
from db_condenser.config_reader import (
    Config,
    DbConnectInfo,
    DbType,
    InitialTarget,
    PreFilter,
)
from db_condenser.db_connect import DbConnect
from db_condenser.mysql_database_creator import MySqlDatabaseCreator
from db_condenser.subset import Subset

MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASSWORD = "test"


def _server_connection():
    try:
        return mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
        )
    except mysql.connector.Error as error:
        pytest.skip("MySQL 8.4 test service is unavailable: {}".format(error))


def _create_databases(source_db, destination_db):
    conn = _server_connection()
    with conn.cursor() as cur:
        cur.execute("CREATE DATABASE {}".format(source_db))
        cur.execute("CREATE DATABASE {}".format(destination_db))
    conn.commit()
    conn.close()


def _drop_databases(source_db, destination_db):
    for name, prefix in (
        (source_db, "condenser_mysql_source_"),
        (destination_db, "condenser_mysql_destination_"),
    ):
        suffix = name.removeprefix(prefix)
        if len(suffix) != 32 or any(c not in "0123456789abcdef" for c in suffix):
            raise ValueError("refusing to drop non-test database {}".format(name))
    conn = _server_connection()
    with conn.cursor() as cur:
        cur.execute("DROP DATABASE {}".format(source_db))
        cur.execute("DROP DATABASE {}".format(destination_db))
    conn.commit()
    conn.close()


@pytest.fixture
def mysql_databases():
    suffix = uuid.uuid4().hex
    source_db = "condenser_mysql_source_" + suffix
    destination_db = "condenser_mysql_destination_" + suffix
    _create_databases(source_db, destination_db)
    _seed_schema(source_db, destination_db)
    try:
        yield source_db, destination_db
    finally:
        config_reader.reset_config()
        _drop_databases(source_db, destination_db)


def _seed_schema(source_db, destination_db):
    conn = _server_connection()
    statements = [
        """
        CREATE TABLE {db}.customers (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            normalized_name VARCHAR(50)
                GENERATED ALWAYS AS (UPPER(name)) STORED
        )
        """,
        """
        CREATE TABLE {db}.products (
            code VARCHAR(20) PRIMARY KEY,
            name VARCHAR(50) NOT NULL
        )
        """,
        """
        CREATE TABLE {db}.orders (
            id BIGINT PRIMARY KEY,
            customer_id BIGINT NOT NULL,
            product_code VARCHAR(20) NOT NULL,
            CONSTRAINT orders_customer_fk FOREIGN KEY (customer_id)
                REFERENCES customers (id),
            CONSTRAINT orders_product_fk FOREIGN KEY (product_code)
                REFERENCES products (code)
        )
        """,
        """
        CREATE TABLE {db}.transfers (
            id BIGINT PRIMARY KEY,
            from_customer_id BIGINT NULL,
            to_customer_id BIGINT NULL,
            CONSTRAINT transfers_from_fk FOREIGN KEY (from_customer_id)
                REFERENCES customers (id),
            CONSTRAINT transfers_to_fk FOREIGN KEY (to_customer_id)
                REFERENCES customers (id)
        )
        """,
        """
        CREATE TABLE {db}.zero_ids (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            note VARCHAR(50) NOT NULL
        )
        """,
    ]
    with conn.cursor() as cur:
        for database in (source_db, destination_db):
            for statement in statements:
                cur.execute(statement.format(db=database))
        cur.execute(
            "INSERT INTO {}.customers (id, name)"
            " VALUES (1, 'selected'), (2, 'excluded')".format(source_db)
        )
        cur.execute(
            "INSERT INTO {}.products VALUES ('A', 'alpha'), ('B', 'beta')".format(
                source_db
            )
        )
        cur.execute(
            "INSERT INTO {}.orders VALUES"
            " (10, 1, 'A'), (11, 1, 'B'), (12, 2, 'B')".format(source_db)
        )
        cur.execute(
            "INSERT INTO {}.transfers VALUES"
            " (20, 1, 1), (21, 1, NULL), (22, 1, 2),"
            " (23, 2, 1), (24, 2, 2)".format(source_db)
        )
        cur.execute(
            "SET SESSION sql_mode = CONCAT(@@SESSION.sql_mode,"
            " ',NO_AUTO_VALUE_ON_ZERO')"
        )
        cur.execute(
            "INSERT INTO {}.zero_ids VALUES (0, 'zero'), (5, 'five')".format(source_db)
        )
        cur.execute(
            "CREATE VIEW {}.customer_names AS SELECT id, name FROM {}.customers".format(
                source_db, source_db
            )
        )
        cur.execute(
            "CREATE PROCEDURE {}.count_customers() "
            "SELECT COUNT(*) AS customer_count FROM {}.customers".format(
                source_db, source_db
            )
        )
        cur.execute(
            "CREATE TRIGGER {}.uppercase_zero_note "
            "BEFORE INSERT ON {}.zero_ids FOR EACH ROW "
            "SET NEW.note = UPPER(NEW.note)".format(source_db, source_db)
        )
        cur.execute(
            "CREATE EVENT {}.future_zero_note "
            "ON SCHEDULE AT CURRENT_TIMESTAMP + INTERVAL 1 DAY "
            "DO INSERT INTO {}.zero_ids (note) VALUES ('event')".format(
                source_db, source_db
            )
        )
    conn.commit()
    conn.close()


@pytest.mark.skipif(
    not os.environ.get("MYSQL_PATH"),
    reason="MYSQL_PATH must point to MySQL 8.4 client utilities",
)
def test_mysql_database_creator_copies_complete_schema(mysql_databases):
    source_db, destination_db = mysql_databases
    source_info = DbConnectInfo(
        user_name=MYSQL_USER,
        password=MYSQL_PASSWORD,
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        db_name=source_db,
    )
    destination_info = DbConnectInfo(
        user_name=MYSQL_USER,
        password=MYSQL_PASSWORD,
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        db_name=destination_db,
    )
    creator = MySqlDatabaseCreator(
        DbConnect(DbType.MYSQL, source_info),
        DbConnect(DbType.MYSQL, destination_info),
    )

    creator.teardown()
    creator.create()

    conn = _server_connection()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT table_type FROM information_schema.tables "
            "WHERE table_schema = %s AND table_name = 'customer_names'",
            (destination_db,),
        )
        assert cur.fetchone() == ("VIEW",)
        cur.execute(
            "SELECT generation_expression FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = 'customers' "
            "AND column_name = 'normalized_name'",
            (destination_db,),
        )
        assert cur.fetchone()[0]
        cur.execute(
            "SELECT routine_name FROM information_schema.routines "
            "WHERE routine_schema = %s AND routine_name = 'count_customers'",
            (destination_db,),
        )
        assert cur.fetchone() == ("count_customers",)
        cur.execute(
            "SELECT trigger_name FROM information_schema.triggers "
            "WHERE trigger_schema = %s AND trigger_name = 'uppercase_zero_note'",
            (destination_db,),
        )
        assert cur.fetchone() == ("uppercase_zero_note",)
        cur.execute(
            "SELECT event_name FROM information_schema.events "
            "WHERE event_schema = %s AND event_name = 'future_zero_note'",
            (destination_db,),
        )
        assert cur.fetchone() == ("future_zero_note",)
        cur.execute(
            "SELECT COUNT(*) FROM information_schema.statistics "
            "WHERE table_schema = %s AND table_name = 'customers' "
            "AND index_name = 'PRIMARY'",
            (destination_db,),
        )
        assert cur.fetchone() == (1,)
        cur.execute(
            "SELECT COUNT(*) FROM information_schema.referential_constraints "
            "WHERE constraint_schema = %s AND table_name = 'orders'",
            (destination_db,),
        )
        assert cur.fetchone() == (2,)
    conn.close()


def _ids(destination_db, table):
    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=destination_db,
    )
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM {} ORDER BY id".format(table))
        result = [row[0] for row in cur.fetchall()]
    conn.close()
    return result


def _product_codes(destination_db):
    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=destination_db,
    )
    with conn.cursor() as cur:
        cur.execute("SELECT code FROM products ORDER BY code")
        result = [row[0] for row in cur.fetchall()]
    conn.close()
    return result


def _run_mysql_subset(
    source_db,
    destination_db,
    initial_targets,
    *,
    passthrough_tables=None,
    keep_disconnected_tables=False,
    max_rows_per_table=None,
):
    source_info = DbConnectInfo(
        user_name=MYSQL_USER,
        password=MYSQL_PASSWORD,
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        db_name=source_db,
    )
    destination_info = DbConnectInfo(
        user_name=MYSQL_USER,
        password=MYSQL_PASSWORD,
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        db_name=destination_db,
    )
    config_reader.config = Config(
        db_type=DbType.MYSQL,
        initial_targets=initial_targets,
        source_db_connection_info=source_info,
        destination_db_connection_info=destination_info,
        passthrough_tables=passthrough_tables or [],
        keep_disconnected_tables=keep_disconnected_tables,
        max_rows_per_table=max_rows_per_table,
    )
    source_dbc = DbConnect(DbType.MYSQL, source_info)
    destination_dbc = DbConnect(DbType.MYSQL, destination_info)
    helper = mysql_database_helper
    subsetter = Subset(
        source_dbc,
        destination_dbc,
        helper.list_all_tables(source_dbc),
    )
    succeeded = False
    try:
        subsetter.prep_temp_dbs()
        subsetter.run_middle_out()
        succeeded = True
    finally:
        try:
            subsetter.unprep_temp_dbs(succeeded=succeeded)
        finally:
            subsetter.close_connections()


def _table_count(database, table):
    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=database,
    )
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM `{}`".format(table))
        count = cur.fetchone()[0]
    conn.close()
    return count


@pytest.mark.parametrize("use_temp_tables", [False, True])
def test_mysql_middle_out_recreate_closes_relationships(
    use_temp_tables, mysql_databases
):
    source_db, destination_db = mysql_databases
    source_info = DbConnectInfo(
        user_name=MYSQL_USER,
        password=MYSQL_PASSWORD,
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        db_name=source_db,
    )
    destination_info = DbConnectInfo(
        user_name=MYSQL_USER,
        password=MYSQL_PASSWORD,
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        db_name=destination_db,
    )
    config_reader.config = Config(
        db_type=DbType.MYSQL,
        initial_targets=[
            InitialTarget(
                table=source_db + ".customers",
                where="id IN (1, 2)",
                pre_filter="selected_customer",
            ),
            InitialTarget(
                table=source_db + ".customers",
                where="id IN (1, 2)",
                pre_filter="selected_customer",
            ),
        ],
        source_db_connection_info=source_info,
        destination_db_connection_info=destination_info,
        use_temp_tables=use_temp_tables,
        passthrough_tables=[source_db + ".zero_ids"],
        pre_filters=[
            PreFilter(name="selected_customer", query="SELECT 1", column="id")
        ],
    )
    source_dbc = DbConnect(DbType.MYSQL, source_info)
    destination_dbc = DbConnect(DbType.MYSQL, destination_info)
    helper = database_helper.get_specific_helper()
    all_tables = helper.list_all_tables(source_dbc)
    subsetter = Subset(source_dbc, destination_dbc, all_tables)
    try:
        subsetter.prep_temp_dbs()
        subsetter.run_middle_out()
    finally:
        subsetter.unprep_temp_dbs()
        subsetter.close_connections()

    assert _ids(destination_db, "customers") == [1]
    assert _ids(destination_db, "orders") == [10, 11]
    assert _ids(destination_db, "transfers") == [20, 21]
    assert _ids(destination_db, "zero_ids") == [0, 5]
    assert _product_codes(destination_db) == ["A", "B"]

    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=destination_db,
    )
    with conn.cursor() as cur:
        cur.execute("SELECT name, normalized_name FROM customers WHERE id = 1")
        assert cur.fetchone() == ("selected", "SELECTED")
        cur.execute("INSERT INTO customers (name) VALUES ('destination-local')")
        assert cur.lastrowid == 2
        cur.execute("INSERT INTO zero_ids (note) VALUES ('next')")
        assert cur.lastrowid == 6
    conn.commit()
    conn.close()


@pytest.mark.parametrize("use_temp_tables", [False, True])
def test_mysql_grow_adds_and_refreshes_primary_key_rows(
    use_temp_tables, mysql_databases
):
    source_db, destination_db = mysql_databases
    source_info = DbConnectInfo(
        user_name=MYSQL_USER,
        password=MYSQL_PASSWORD,
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        db_name=source_db,
    )
    destination_info = DbConnectInfo(
        user_name=MYSQL_USER,
        password=MYSQL_PASSWORD,
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        db_name=destination_db,
    )

    def run(mode):
        config_reader.config = Config(
            db_type=DbType.MYSQL,
            initial_targets=[
                InitialTarget(
                    table=source_db + ".customers",
                    where="id IN (1, 3)",
                )
            ],
            source_db_connection_info=source_info,
            destination_db_connection_info=destination_info,
            use_temp_tables=use_temp_tables,
            destination_mode=mode,
        )
        source_dbc = DbConnect(DbType.MYSQL, source_info)
        destination_dbc = DbConnect(DbType.MYSQL, destination_info)
        helper = database_helper.get_specific_helper()
        subsetter = Subset(
            source_dbc,
            destination_dbc,
            helper.list_all_tables(source_dbc),
        )
        succeeded = False
        try:
            subsetter.prep_temp_dbs()
            subsetter.run_middle_out()
            succeeded = True
        finally:
            try:
                subsetter.unprep_temp_dbs(succeeded=succeeded)
            finally:
                subsetter.close_connections()

    run(config_reader.DestinationMode.RECREATE)

    conn = _server_connection()
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE {}.customers SET name = 'selected-refreshed' WHERE id = 1".format(
                source_db
            )
        )
        cur.execute(
            "INSERT INTO {}.customers (id, name) VALUES (3, 'new-target')".format(
                source_db
            )
        )
        cur.execute("INSERT INTO {}.products VALUES ('C', 'gamma')".format(source_db))
        cur.execute(
            "UPDATE {}.orders SET product_code = 'C' WHERE id = 10".format(source_db)
        )
        cur.execute(
            "INSERT INTO {}.orders VALUES (13, 1, 'A'), (14, 3, 'B')".format(source_db)
        )
    conn.commit()
    conn.close()

    run(config_reader.DestinationMode.GROW)
    run(config_reader.DestinationMode.GROW)

    assert _ids(destination_db, "customers") == [1, 3]
    assert _ids(destination_db, "orders") == [10, 11, 13, 14]
    assert _product_codes(destination_db) == ["A", "B", "C"]

    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=destination_db,
    )
    with conn.cursor() as cur:
        cur.execute("SELECT name, normalized_name FROM customers WHERE id = 1")
        assert cur.fetchone() == ("selected-refreshed", "SELECTED-REFRESHED")
        cur.execute("SELECT product_code FROM orders WHERE id = 10")
        assert cur.fetchone() == ("C",)
        cur.execute(
            "SELECT COUNT(*) FROM orders o LEFT JOIN customers c "
            "ON c.id = o.customer_id WHERE c.id IS NULL"
        )
        assert cur.fetchone() == (0,)
        cur.execute(
            "SELECT COUNT(*) FROM orders o LEFT JOIN products p "
            "ON p.code = o.product_code WHERE p.code IS NULL"
        )
        assert cur.fetchone() == (0,)
    conn.close()


def _assert_mysql_grow_preflight_fails(source_db, destination_db, target_table, error):
    source_info = DbConnectInfo(
        user_name=MYSQL_USER,
        password=MYSQL_PASSWORD,
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        db_name=source_db,
    )
    destination_info = DbConnectInfo(
        user_name=MYSQL_USER,
        password=MYSQL_PASSWORD,
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        db_name=destination_db,
    )
    config_reader.config = Config(
        db_type=DbType.MYSQL,
        initial_targets=[InitialTarget(table=target_table, where="TRUE")],
        source_db_connection_info=source_info,
        destination_db_connection_info=destination_info,
        destination_mode=config_reader.DestinationMode.GROW,
    )
    source_dbc = DbConnect(DbType.MYSQL, source_info)
    destination_dbc = DbConnect(DbType.MYSQL, destination_info)
    helper = database_helper.get_specific_helper()
    subsetter = Subset(
        source_dbc,
        destination_dbc,
        helper.list_all_tables(source_dbc),
    )
    try:
        with pytest.raises(ValueError, match=error):
            subsetter.prep_temp_dbs()
    finally:
        subsetter.close_connections()


def test_mysql_grow_rejects_secondary_unique_indexes(mysql_databases):
    source_db, destination_db = mysql_databases
    conn = _server_connection()
    with conn.cursor() as cur:
        cur.execute(
            "ALTER TABLE {}.customers ADD UNIQUE KEY customers_name_unique (name)".format(
                destination_db
            )
        )
    conn.commit()
    conn.close()

    _assert_mysql_grow_preflight_fails(
        source_db,
        destination_db,
        source_db + ".customers",
        "secondary unique index customers_name_unique",
    )


def test_mysql_grow_rejects_tables_without_primary_keys(mysql_databases):
    source_db, destination_db = mysql_databases
    conn = _server_connection()
    with conn.cursor() as cur:
        for database in (source_db, destination_db):
            cur.execute(
                "CREATE TABLE {}.unkeyed (value VARCHAR(50) NOT NULL)".format(database)
            )
    conn.commit()
    conn.close()

    _assert_mysql_grow_preflight_fails(
        source_db,
        destination_db,
        source_db + ".unkeyed",
        "requires a primary key",
    )


@pytest.mark.parametrize(
    ("target_table", "max_rows_per_table"),
    [
        ("stream_parents", None),
        ("stream_children", None),
        ("stream_parents", 1001),
    ],
)
def test_mysql_streams_more_than_one_destination_id_batch(
    target_table, max_rows_per_table, mysql_databases
):
    source_db, destination_db = mysql_databases
    conn = _server_connection()
    with conn.cursor() as cur:
        for database in (source_db, destination_db):
            cur.execute(
                "CREATE TABLE `{}`.stream_parents (id INT PRIMARY KEY)".format(database)
            )
            cur.execute(
                "CREATE TABLE `{}`.stream_children ("
                "id INT PRIMARY KEY, parent_id INT NOT NULL, "
                "FOREIGN KEY (parent_id) REFERENCES stream_parents(id))".format(
                    database
                )
            )
        rows = [(value,) for value in range(1, 1002)]
        cur.executemany(
            "INSERT INTO `{}`.stream_parents VALUES (%s)".format(source_db), rows
        )
        cur.executemany(
            "INSERT INTO `{}`.stream_children VALUES (%s, %s)".format(source_db),
            [(value, value) for (value,) in rows],
        )
    conn.commit()
    conn.close()

    _run_mysql_subset(
        source_db,
        destination_db,
        [InitialTarget(table=source_db + "." + target_table, where="TRUE")],
        max_rows_per_table=max_rows_per_table,
    )

    assert _table_count(destination_db, "stream_parents") == 1001
    assert _table_count(destination_db, "stream_children") == 1001


def test_mysql_downstream_keyset_pages_composite_text_ids(mysql_databases, monkeypatch):
    source_db, destination_db = mysql_databases
    conn = _server_connection()
    with conn.cursor() as cur:
        for database in (source_db, destination_db):
            cur.execute(
                "CREATE TABLE `{}`.composite_parents ("
                "code VARCHAR(20) COLLATE utf8mb4_bin NOT NULL, "
                "version INT NOT NULL, "
                "PRIMARY KEY (code, version))".format(database)
            )
            cur.execute(
                "CREATE TABLE `{}`.composite_children ("
                "id INT PRIMARY KEY, "
                "parent_code VARCHAR(20) COLLATE utf8mb4_bin NOT NULL, "
                "parent_version INT NOT NULL, "
                "FOREIGN KEY (parent_code, parent_version) "
                "REFERENCES composite_parents(code, version))".format(database)
            )
        cur.execute(
            "INSERT INTO `{}`.composite_parents VALUES "
            "('zeta', 2), ('alpha', 10), ('alpha', 2), ('A', 1), ('a', 1)".format(
                source_db
            )
        )
        cur.execute(
            "INSERT INTO `{}`.composite_children VALUES "
            "(1, 'zeta', 2), (2, 'alpha', 10), (3, 'alpha', 2), "
            "(4, 'A', 1), (5, 'a', 1)".format(source_db)
        )
    conn.commit()
    conn.close()

    monkeypatch.setattr(
        mysql_database_helper, "get_batch_size", lambda _column_count: 1
    )
    _run_mysql_subset(
        source_db,
        destination_db,
        [InitialTarget(table=source_db + ".composite_children", where="TRUE")],
    )

    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=destination_db,
    )
    with conn.cursor() as cur:
        cur.execute(
            "SELECT code, version FROM composite_parents ORDER BY code, version"
        )
        assert cur.fetchall() == [
            ("A", 1),
            ("a", 1),
            ("alpha", 2),
            ("alpha", 10),
            ("zeta", 2),
        ]
    conn.close()


def test_mysql_multi_fk_queries_keep_every_id_set_bounded(mysql_databases, monkeypatch):
    source_db, destination_db = mysql_databases
    conn = _server_connection()
    with conn.cursor() as cur:
        for database in (source_db, destination_db):
            cur.execute(
                "CREATE TABLE `{}`.pair_parents (id INT PRIMARY KEY)".format(database)
            )
            cur.execute(
                "CREATE TABLE `{}`.pair_children ("
                "id INT PRIMARY KEY, left_id INT NULL, right_id INT NULL, "
                "FOREIGN KEY (left_id) REFERENCES pair_parents(id), "
                "FOREIGN KEY (right_id) REFERENCES pair_parents(id))".format(database)
            )
        cur.execute(
            "INSERT INTO `{}`.pair_parents VALUES (1), (2), (3)".format(source_db)
        )
        cur.execute(
            "INSERT INTO `{}`.pair_children VALUES "
            "(10, 1, 3), (11, 3, 1), (12, 1, 2), "
            "(13, 1, NULL), (14, 2, 1), (15, 2, 2)".format(source_db)
        )
    conn.commit()
    conn.close()

    helper = mysql_database_helper
    real_build_id_table = helper.build_id_table
    observed_sizes = []

    def bounded_build_id_table(rows, columns, datatypes, alias):
        observed_sizes.append(len(rows))
        assert len(rows) <= 1
        return real_build_id_table(rows, columns, datatypes, alias)

    monkeypatch.setattr(helper, "get_batch_size", lambda _column_count: 1)
    monkeypatch.setattr(helper, "build_id_table", bounded_build_id_table)

    _run_mysql_subset(
        source_db,
        destination_db,
        [
            InitialTarget(
                table=source_db + ".pair_parents",
                where="id IN (1, 3)",
            )
        ],
    )

    assert observed_sizes
    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=destination_db,
    )
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM pair_children ORDER BY id")
        assert [row[0] for row in cur.fetchall()] == [10, 11, 13]
    conn.close()


@pytest.mark.parametrize("copy_path", ["passthrough", "disconnected"])
def test_mysql_full_table_copy_includes_invisible_columns(copy_path, mysql_databases):
    source_db, destination_db = mysql_databases
    conn = _server_connection()
    with conn.cursor() as cur:
        for database in (source_db, destination_db):
            cur.execute(
                "CREATE TABLE `{}`.hidden_rows ("
                "id INT PRIMARY KEY, secret VARCHAR(50) INVISIBLE)".format(database)
            )
        cur.execute(
            "INSERT INTO `{}`.hidden_rows (id, secret) VALUES (1, 'included')".format(
                source_db
            )
        )
    conn.commit()
    conn.close()

    hidden_table = source_db + ".hidden_rows"
    _run_mysql_subset(
        source_db,
        destination_db,
        [InitialTarget(table=source_db + ".customers", where="id = 1")],
        passthrough_tables=[hidden_table] if copy_path == "passthrough" else [],
        keep_disconnected_tables=copy_path == "disconnected",
    )

    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=destination_db,
    )
    with conn.cursor() as cur:
        cur.execute("SELECT id, secret FROM hidden_rows")
        assert cur.fetchall() == [(1, "included")]
    conn.close()
