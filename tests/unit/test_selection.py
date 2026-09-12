"""Traversal/execution boundary and SQL invariants, without database sockets."""

import threading
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, create_autospec

import pytest

from db_condenser import config_reader
from db_condenser.backends import get_backend
from db_condenser.backends.contracts import Backend, RunSession, SelectionExecutor
from db_condenser.backends.execution import SqlSelectionExecutor
from db_condenser.backends.upstream import (
    _build_upstream_unnest_query,
    _upstream_ids_query,
)
from db_condenser.config_reader import DbType, InitialTarget
from db_condenser.subset import Subset


def configure(monkeypatch, db_type="postgres", **options):
    info = dict(
        host="unused", port=5432, db_name="app", user_name="unused", password="unused"
    )
    config = config_reader._raw_dict_to_config(
        {
            "db_type": db_type,
            "source_db_connection_info": info,
            "destination_db_connection_info": info,
            "initial_targets": [{"table": "app.root", "where": "id = 1"}],
            **options,
        }
    )
    monkeypatch.setattr(config_reader, "config", config)
    return config


def relationship(child, parent):
    return dict(
        fk_table=child,
        fk_columns=["parent_id"],
        target_table=parent,
        target_columns=["id"],
    )


@pytest.mark.parametrize("parallel", [False, True])
def test_traversal_orders_operations_without_sql(monkeypatch, parallel):
    # Deliberately use MySQL config even when the fake offers parallel reads:
    # core scheduling must follow the executor, not hard-code a database type.
    configure(
        monkeypatch,
        "mysql",
        passthrough_tables=["app.audit"],
        keep_disconnected_tables=True,
    )
    backend = create_autospec(Backend, instance=True, spec_set=True)
    session = create_autospec(RunSession, instance=True)
    session.source = Mock()
    session.destination = Mock()
    backend.open_run.return_value = session
    backend.uses_incremental.return_value = False
    executor = create_autospec(SelectionExecutor, instance=True, spec_set=True)
    executor.parallel_reads = parallel
    executor.copy_table_parallel.return_value = False
    backend.selection_executor.return_value = executor
    relationships = [
        relationship("app.root", "app.ancestor"),
        relationship("app.child", "app.root"),
        relationship("app.grandchild", "app.child"),
    ]
    backend.get_unredacted_fk_relationships.return_value = relationships
    tables = [
        "app.ancestor",
        "app.root",
        "app.child",
        "app.grandchild",
        "app.audit",
        "app.unrelated",
    ]
    source, destination = Mock(), Mock()
    events = []
    executor.load_pre_filters.side_effect = lambda: events.append(("pre_filters",))
    executor.select_direct.side_effect = lambda target, _: events.append(
        ("direct", target.table)
    )
    executor.select_direct_parallel.side_effect = lambda target, _: events.append(
        ("direct_parallel", target.table)
    )

    def upstream(table, processed, *_args, **_kwargs):
        events.append(("upstream", table, frozenset(processed)))
        return True

    executor.select_upstream.side_effect = upstream
    executor.copy_table.side_effect = lambda table, *_args, **_kwargs: events.append(
        ("copy", table)
    )
    executor.select_downstream.side_effect = lambda table, *_args, **_kwargs: (
        events.append(("downstream", table))
    )
    subset = Subset(source, destination, tables, backend=backend)
    subset.prep_temp_dbs()
    try:
        subset.run_middle_out()
    finally:
        subset.unprep_temp_dbs()
        subset.close_connections()
    assert events == [
        ("pre_filters",),
        ("direct_parallel" if parallel else "direct", "app.root"),
        ("upstream", "app.child", frozenset(["app.root"])),
        ("upstream", "app.grandchild", frozenset(["app.root", "app.child"])),
        ("copy", "app.audit"),
        ("downstream", "app.grandchild"),
        ("downstream", "app.child"),
        ("downstream", "app.root"),
        ("downstream", "app.ancestor"),
        ("copy", "app.unrelated"),
    ]
    if parallel:
        executor.copy_table_parallel.assert_called_once_with("app.audit")
    else:
        executor.copy_table_parallel.assert_not_called()
    for conn in [
        session.source,
        session.destination,
        session.open_source_connection.return_value,
        destination.get_db_connection.return_value,
    ]:
        conn.cursor.assert_not_called()
    session.prepare.assert_called_once()
    session.finish.assert_called_once_with(True)
    session.close.assert_called_once()
    session.open_source_connection.return_value.close.assert_called_once()
    destination.get_db_connection.return_value.close.assert_called_once()


def test_failed_executor_factory_closes_open_run(monkeypatch):
    configure(monkeypatch)
    backend = create_autospec(Backend, instance=True, spec_set=True)
    backend.uses_incremental.return_value = False
    failure = RuntimeError("executor unavailable")
    backend.selection_executor.side_effect = failure
    with pytest.raises(RuntimeError) as exc:
        Subset(Mock(), Mock(), ["app.root"], backend=backend)
    assert exc.value is failure
    backend.open_run.return_value.close.assert_called_once()


@pytest.fixture(params=list(DbType), ids=lambda value: value.value)
def sql_executor(request, monkeypatch):
    config = configure(monkeypatch, request.param.value)
    backend = get_backend(request.param)
    monkeypatch.setattr(
        backend, "get_table_columns", Mock(return_value=["id", "payload"])
    )
    monkeypatch.setattr(backend, "copy_rows", Mock())
    session = SimpleNamespace(
        source=MagicMock(), destination=MagicMock(), source_pool=[]
    )
    executor = backend.selection_executor(session, Mock(), config)
    executor.load_pre_filters()
    return executor


@pytest.mark.parametrize("percent", [False, True], ids=["where", "sampling"])
def test_direct_sql_and_sampling_remain_backend_specific(sql_executor, percent):
    executor = sql_executor
    postgres = executor.config.db_type == DbType.POSTGRES
    q = '"' if postgres else "`"
    target = (
        InitialTarget(table="app.root", percent=10)
        if percent
        else InitialTarget(table="app.root", where="id = 1")
    )
    executor.select_direct(target, [])
    predicate = (
        ("random() < 0.1" if postgres else "rand() < 0.1") if percent else "id = 1"
    )
    executor.copy_rows.assert_called_once_with(
        executor.source,
        executor.destination,
        f"SELECT {q}root{q}.{q}id{q},{q}root{q}.{q}payload{q} FROM {q}app{q}.{q}root{q} WHERE {predicate}",
        "app.root",
        None,
    )


def test_passthrough_limit_is_explicit_not_applied_to_disconnected_copy(sql_executor):
    executor = sql_executor
    executor.config.max_rows_per_table = 7
    executor.copy_table("app.root", limit=7)
    assert executor.copy_rows.call_args.args[2].endswith(" LIMIT 7")
    executor.copy_table("app.root")
    assert "LIMIT" not in executor.copy_rows.call_args.args[2]
    assert executor.copy_table_parallel("app.root") is False


def test_composite_array_query_preserves_key_pairing_and_nullable_rules(monkeypatch):
    configure(monkeypatch)
    executor = Mock()
    constraint = dict(fk_columns=["tenant", "code"])
    query, params = _build_upstream_unnest_query(
        executor,
        '"app"."child"',
        [(constraint, [(1, "alpha"), (2, "beta")])],
        {"tenant": "int4", "code": "text"},
        ["active"],
        required_join=0,
        columns_query='"child"."id"',
    )
    assert params == [[1, 2], ["alpha", "beta"]]
    assert query == (
        'SELECT "child"."id" FROM "app"."child"'
        " LEFT JOIN unnest(%s::int4[], %s::text[]) AS ids0(col0, col1)"
        ' ON "app"."child"."tenant" = ids0.col0 AND "app"."child"."code" = ids0.col1'
        ' WHERE ("app"."child"."tenant" IS NULL OR "app"."child"."code" IS NULL OR ids0.col0 IS NOT NULL)'
        " AND ids0.col0 IS NOT NULL AND (active)"
    )


def test_topup_query_joins_composite_identity_and_only_inserted_delta(monkeypatch):
    configure(monkeypatch)
    query = _upstream_ids_query(
        Mock(),
        "app.parent",
        ["reference"],
        Mock(),
        {"app.parent": ('"_condenser"."delta"', ["tenant", "code"])},
    )
    assert query == (
        'SELECT DISTINCT _t."reference" FROM "app"."parent" _t'
        ' JOIN "_condenser"."delta" _d ON _t."tenant" = _d."tenant"'
        ' AND _t."code" = _d."code" AND _d._inserted'
    )


def test_parallel_stage_failure_aborts_barrier_and_closes_worker_destinations(
    monkeypatch,
):
    config = configure(monkeypatch, destination_mode="grow")
    backend = get_backend(DbType.POSTGRES)
    helper = Mock()
    helper.has_secondary_unique.return_value = True
    helper.delta_for.return_value = ("delta", ["id"])
    helper.get_table_page_count.return_value = 100
    sources = [MagicMock(), MagicMock()]
    destinations = [MagicMock(), MagicMock()]
    factory = Mock()
    factory.get_db_connection.side_effect = destinations
    session = SimpleNamespace(
        source=MagicMock(), destination=MagicMock(), source_pool=sources
    )
    executor = SqlSelectionExecutor(backend, helper, session, factory, config)

    def stage(source, *_args):
        if source is sources[0]:
            raise RuntimeError("staging failed")

    helper.stage_rows.side_effect = stage
    with pytest.raises((RuntimeError, threading.BrokenBarrierError)):
        executor.copy_table_parallel("app.root")
    assert all(call.args[-1] != "insert" for call in helper.apply_staged.call_args_list)
    for conn in destinations:
        conn.close.assert_called_once()
    for conn in sources:
        conn.close.assert_not_called()
        conn.commit.assert_not_called()
