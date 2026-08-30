import pytest

from db_condenser import mysql_database_helper


class VersionConnection:
    def __init__(self, version):
        self.version = version

    def cursor(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def execute(self, query):
        assert query == "SELECT VERSION()"

    def fetchone(self):
        return (self.version,)


def test_mysql_id_table_uses_row_major_bounded_parameters():
    sql, params = mysql_database_helper.build_id_table(
        [(1, "retail"), (2, "wholesale")],
        ["customer_id", "kind"],
        {"customer_id": "int", "kind": "varchar"},
        "ids0",
    )

    assert sql == ("(SELECT %s AS col0, %s AS col1 UNION ALL SELECT %s, %s) AS ids0")
    assert params == [1, "retail", 2, "wholesale"]
    assert mysql_database_helper.get_batch_size(1) == 1000
    assert mysql_database_helper.get_batch_size(100) == 600


def test_mysql_id_table_can_represent_an_empty_constraint_set():
    sql, params = mysql_database_helper.build_id_table(
        [],
        ["customer_id", "kind"],
        {"customer_id": "int", "kind": "varchar"},
        "ids0",
    )

    assert sql == "(SELECT NULL AS col0, NULL AS col1 WHERE FALSE) AS ids0"
    assert params == []


def test_mysql_destination_id_temp_table_uses_real_columns_and_an_index(monkeypatch):
    queries = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            pass

        def execute(self, query):
            queries.append(query)

    class Connection:
        def cursor(self):
            return Cursor()

    monkeypatch.setattr(mysql_database_helper.uuid, "uuid4", lambda: "fixed")
    monkeypatch.setattr(
        mysql_database_helper,
        "fully_qualified_table",
        lambda table: "`{}`".format(table),
    )
    monkeypatch.setattr(
        mysql_database_helper, "quoter", lambda column: "`{}`".format(column)
    )

    table = mysql_database_helper.create_id_temp_table(
        Connection(), 2, "destination.parents", ["code", "version"]
    )

    assert table == "tonic_subset_fixed"
    assert queries == [
        "CREATE TEMPORARY TABLE `tonic_subset_fixed` AS SELECT "
        "IF(TRUE, `code`, NULL) AS `col0`,"
        "IF(TRUE, `version`, NULL) AS `col1` "
        "FROM `destination.parents` LIMIT 0",
        "CREATE INDEX `tonic_subset_ids` ON `tonic_subset_fixed` (`col0`,`col1`)",
    ]


def test_mysql_membership_values_are_split_into_safe_batches():
    values = list(range(2001))

    batches = list(mysql_database_helper.iter_membership_batches(values))
    sql, params = mysql_database_helper.build_membership_filter(
        "`customers`.`region_id`", batches[-1]
    )

    assert [len(batch) for batch in batches] == [1000, 1000, 1]
    assert sql == "`customers`.`region_id` IN (%s)"
    assert params == [2000]


@pytest.mark.parametrize("version", ["8.0.46", "10.11.13-MariaDB"])
def test_mysql_rejects_unsupported_server_lines(version):
    with pytest.raises(RuntimeError, match="MySQL 8.4|Unsupported MySQL"):
        mysql_database_helper.validate_supported_version(VersionConnection(version))


@pytest.mark.parametrize("version", ["8.4.7", "9.7.0", "26.7.0"])
def test_mysql_accepts_current_lts_and_newer_server_lines(version):
    mysql_database_helper.validate_supported_version(VersionConnection(version))
