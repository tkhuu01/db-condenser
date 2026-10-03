from contextlib import ExitStack, closing

from db_condenser import database_helper
from db_condenser.backends.contracts import Backend
from db_condenser.db_connect import MySqlConnection


def tabulate(
    source_dbc,
    destination_dbc,
    tables,
    total_elapsed=None,
    *,
    backend: Backend | None = None,
):
    row_counts = list()
    db_helper = (
        backend if backend is not None else database_helper.get_specific_helper()
    )
    with ExitStack() as connections:
        source_conn = connections.enter_context(closing(source_dbc.get_db_connection()))
        dest_conn = connections.enter_context(
            closing(destination_dbc.get_db_connection())
        )
        for table in tables:
            o = db_helper.get_table_count_estimate(
                table_name(table), schema_name(table), source_conn
            )
            dest_schema_name = (
                dest_conn.db_name
                if isinstance(dest_conn, MySqlConnection)
                else schema_name(table)
            )
            n = db_helper.get_table_count_estimate(
                table_name(table), dest_schema_name, dest_conn
            )
            row_counts.append((table, max(int(o), 0), max(int(n), 0)))

    name_w = max(len("Table") + 1, max((len(x[0]) for x in row_counts), default=0) + 1)
    src_w = max(
        len("Source"), max((len(_fmt_count(x[1])) for x in row_counts), default=0)
    )
    dst_w = max(
        len("Dest"), max((len(_fmt_count(x[2])) for x in row_counts), default=0)
    )

    header = "  {:<{}}  {:>{}}  {:>{}}  {:>7}".format(
        "Table", name_w, "Source", src_w, "Dest", dst_w, "Ratio"
    )
    sep = "  " + "-" * (name_w + src_w + dst_w + 13)

    print()
    print(sep)
    print(header)
    print(sep)
    for name, src, dst in row_counts:
        ratio = dst / src if src > 0 else 0
        print(
            "  {:<{}}  {:>{}}  {:>{}}  {:>6.1f}%".format(
                name,
                name_w,
                _fmt_count(src),
                src_w,
                _fmt_count(dst),
                dst_w,
                ratio * 100,
            )
        )
    print(sep)

    total_src = sum(x[1] for x in row_counts)
    total_dst = sum(x[2] for x in row_counts)
    total_ratio = total_dst / total_src if total_src > 0 else 0
    print(
        "  {:<{}}  {:>{}}  {:>{}}  {:>6.1f}%".format(
            "TOTAL",
            name_w,
            _fmt_count(total_src),
            src_w,
            _fmt_count(total_dst),
            dst_w,
            total_ratio * 100,
        )
    )
    if total_elapsed is not None:
        print("  Elapsed: {:.1f}s".format(total_elapsed))
    print(sep)
    print()


def _fmt_count(n):
    return "{:,}".format(n)


def schema_name(table):
    return table.split(".")[0]


def table_name(table):
    return table.split(".")[1]
