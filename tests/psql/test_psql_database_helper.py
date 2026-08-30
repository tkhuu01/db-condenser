from db_condenser import psql_database_helper


def test_postgres_id_table_keeps_typed_array_parameters():
    sql, params = psql_database_helper.build_id_table(
        [(1, "retail"), (2, "wholesale")],
        ["customer_id", "kind"],
        {"customer_id": "int4", "kind": "text"},
        "ids0",
    )

    assert sql == "unnest(%s::int4[], %s::text[]) AS ids0(col0, col1)"
    assert params == [[1, 2], ["retail", "wholesale"]]
