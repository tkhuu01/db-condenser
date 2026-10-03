"""One application workflow, with no real database or schema-tool calls."""

import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock, create_autospec

import pytest

from db_condenser import config_reader, direct_subset, run_subset, runner
from db_condenser.config_reader import DbType, DestinationMode
from db_condenser.subset import Subset


@pytest.fixture
def workflow(monkeypatch):
    info = dict(
        user_name="test", password="test", host="localhost", port=5432, db_name="app"
    )
    raw = dict(
        db_type="postgres",
        source_db_connection_info=info,
        destination_db_connection_info={**info, "db_name": "dest"},
        initial_targets=[dict(table="public.parent", where="selected")],
        excluded_tables=["public.excluded"],
        pre_constraint_sql=["pre"],
        post_subset_sql=["post"],
    )
    config = config_reader._raw_dict_to_config(raw)
    events, connections = [], []

    def record(name, result=None):
        def call(*args, **kwargs):
            assert config_reader.get_config() is state.active_config
            events.append(name)
            if name == state.fail_at:
                raise state.failure
            return result

        return call

    backend = Mock()
    schema = backend.schema_manager.return_value
    subset = create_autospec(Subset, instance=True, spec_set=True)
    source, destination = Mock(), Mock()
    state = SimpleNamespace(
        config=config,
        raw=raw,
        active_config=config,
        events=events,
        connections=connections,
        backend=backend,
        schema=schema,
        subset=subset,
        source=source,
        destination=destination,
        fail_at=None,
        failure=RuntimeError("injected failure"),
    )

    def connection():
        conn = Mock()
        connections.append(conn)
        return conn

    destination.get_db_connection.side_effect = connection
    monkeypatch.setattr(
        runner, "DbConnect", Mock(side_effect=[source, destination] * 3)
    )
    monkeypatch.setattr(runner, "get_backend", Mock(return_value=backend))
    backend.schema_manager.side_effect = record("schema", schema)
    schema.teardown.side_effect = record("teardown")
    schema.create.side_effect = record("create")
    schema.add_constraints.side_effect = record("constraints")
    backend.list_all_tables.side_effect = record(
        "tables", ["public.parent", "public.excluded", "pgbench.accounts"]
    )
    monkeypatch.setattr(runner, "Subset", Mock(side_effect=record("construct", subset)))
    subset._prepare.side_effect = record("prepare")
    subset._run_middle_out.side_effect = record("select")
    subset._finalize.side_effect = record("finalize")
    subset._close.side_effect = record("close")
    backend.run_query.side_effect = lambda statement, conn: record(statement)()
    backend.update_sequence_numbering.side_effect = record("sequences")
    monkeypatch.setattr(
        runner.result_tabulator, "tabulate", Mock(side_effect=record("report"))
    )
    return state


@pytest.mark.parametrize("mode", list(DestinationMode))
@pytest.mark.parametrize("no_constraints", [False, True])
def test_complete_workflow_order(workflow, mode, no_constraints):
    w = workflow
    w.active_config = replace(w.config, destination_mode=mode)
    assert (
        run_subset(w.active_config, no_constraints=no_constraints, verbose=True) is None
    )
    expected = ["schema"]
    if mode == DestinationMode.RECREATE:
        expected += ["teardown", "create"]
    expected += ["tables", "construct", "prepare", "select", "pre"]
    if mode == DestinationMode.RECREATE and not no_constraints:
        expected += ["constraints"]
    assert w.events == expected + ["post", "sequences", "report", "finalize", "close"]
    w.subset._finalize.assert_called_once_with(True)
    runner.Subset.assert_called_once_with(
        w.source,
        w.destination,
        ["public.parent", "pgbench.accounts"],
        backend=w.backend,
    )
    w.backend.update_sequence_numbering.assert_called_once_with(
        w.connections[-1], ["public.parent"]
    )
    for connection in w.connections:
        connection.close.assert_called_once_with()
    assert config_reader.config is None


@pytest.mark.parametrize(
    "phase",
    [
        "schema",
        "teardown",
        "create",
        "tables",
        "construct",
        "prepare",
        "select",
        "pre",
        "constraints",
        "post",
        "sequences",
        "report",
        "finalize",
        "close",
    ],
)
def test_failures_restore_configuration_and_close(workflow, monkeypatch, phase):
    w = workflow
    previous = replace(w.config)
    monkeypatch.setattr(config_reader, "config", previous)
    w.fail_at = phase
    with pytest.raises(RuntimeError) as exc:
        run_subset(w.config)
    assert exc.value is w.failure
    if "prepare" in w.events:
        w.subset._close.assert_called_once_with()
        w.subset._finalize.assert_called_once_with(phase in ("finalize", "close"))
    else:
        w.subset._finalize.assert_not_called()
        w.subset._close.assert_not_called()
    for connection in w.connections:
        connection.close.assert_called_once_with()
    assert config_reader.config is previous


def test_interruption_is_not_swallowed(workflow):
    workflow.fail_at = "select"
    workflow.failure = KeyboardInterrupt()
    with pytest.raises(KeyboardInterrupt):
        run_subset(workflow.config)
    workflow.subset._finalize.assert_called_once_with(False)
    workflow.subset._close.assert_called_once_with()
    assert config_reader.config is None


def test_cleanup_failure_keeps_original_exception_context(workflow):
    workflow.fail_at = "select"
    cleanup_error = RuntimeError("cleanup failure")
    workflow.subset._finalize.side_effect = cleanup_error
    with pytest.raises(RuntimeError) as exc:
        run_subset(workflow.config)
    assert exc.value is cleanup_error
    assert exc.value.__context__ is workflow.failure
    workflow.subset._close.assert_called_once_with()
    assert config_reader.config is None


def test_report_can_be_disabled_without_skipping_finishing_work(workflow):
    run_subset(workflow.config, report=False)
    assert workflow.events[-4:] == ["post", "sequences", "finalize", "close"]
    runner.result_tabulator.tabulate.assert_not_called()


def test_mysql_retains_no_sequence_reset(workflow):
    workflow.active_config = replace(workflow.config, db_type=DbType.MYSQL)
    run_subset(workflow.active_config)
    workflow.backend.update_sequence_numbering.assert_not_called()
    workflow.subset._finalize.assert_called_once_with(True)
    for connection in workflow.connections:
        connection.close.assert_called_once_with()


@pytest.mark.parametrize("as_string", [False, True])
def test_file_input_uses_shared_parser(workflow, tmp_path, monkeypatch, as_string):
    path = tmp_path / "config.json"
    path.write_text(json.dumps(workflow.raw))
    original = config_reader.load_config

    def load(path):
        workflow.active_config = original(path)
        return workflow.active_config

    monkeypatch.setattr(config_reader, "load_config", load)
    run_subset(str(path) if as_string else path)
    assert workflow.active_config == workflow.config
    assert config_reader.config is None


@pytest.mark.parametrize("contents", ["{", "{}", None])
def test_bad_file_does_not_start_workflow(workflow, tmp_path, contents):
    path = tmp_path / "config.json"
    if contents is not None:
        path.write_text(contents)
    with pytest.raises((ValueError, KeyError, FileNotFoundError)):
        run_subset(path)
    assert workflow.events == []
    assert config_reader.config is None


def test_sequential_calls_use_their_own_config(workflow):
    run_subset(workflow.config)
    workflow.active_config = replace(
        workflow.config, destination_mode=DestinationMode.GROW
    )
    run_subset(workflow.active_config)
    assert workflow.events.count("select") == 2
    assert workflow.events.count("teardown") == 1
    assert config_reader.config is None


@pytest.mark.parametrize("answer", ["y", "n"])
def test_cli_confirms_before_delegating(workflow, monkeypatch, answer):
    config = replace(
        workflow.config,
        destination_db_connection_info=replace(
            workflow.config.destination_db_connection_info, host="remote"
        ),
    )
    args = SimpleNamespace(
        help_config=False,
        example_config=False,
        config="chosen.json",
        yes=False,
        verbose=True,
        no_constraints=True,
    )
    monkeypatch.setattr(direct_subset, "_parse_args", lambda: args)
    monkeypatch.setattr(config_reader, "load_config", Mock(return_value=config))
    monkeypatch.setattr("builtins.input", lambda _: answer)
    delegated = Mock()
    monkeypatch.setattr(direct_subset, "run_subset", delegated)
    if answer == "n":
        with pytest.raises(SystemExit):
            direct_subset.main()
        delegated.assert_not_called()
    else:
        direct_subset.main()
        delegated.assert_called_once_with(config, verbose=True, no_constraints=True)
    assert workflow.events == []
    assert config_reader.config is None


@pytest.mark.parametrize(
    "host,yes", [("localhost", False), ("127.0.0.1", False), ("remote", True)]
)
def test_cli_skips_confirmation_when_requested(workflow, monkeypatch, host, yes):
    config = replace(
        workflow.config,
        destination_db_connection_info=replace(
            workflow.config.destination_db_connection_info, host=host
        ),
    )
    monkeypatch.setattr(
        direct_subset,
        "_parse_args",
        lambda: SimpleNamespace(
            help_config=False,
            example_config=False,
            config=None,
            yes=yes,
            verbose=False,
            no_constraints=False,
        ),
    )
    monkeypatch.setattr(config_reader, "load_config", Mock(return_value=config))
    confirm, delegated = Mock(), Mock()
    monkeypatch.setattr(direct_subset, "_confirm_destination", confirm)
    monkeypatch.setattr(direct_subset, "run_subset", delegated)
    direct_subset.main()
    confirm.assert_not_called()
    delegated.assert_called_once_with(config, verbose=False, no_constraints=False)


def test_cli_missing_file_does_not_start_run(monkeypatch, capsys):
    monkeypatch.setattr(
        direct_subset,
        "_parse_args",
        lambda: SimpleNamespace(
            help_config=False, example_config=False, config="missing.json"
        ),
    )
    monkeypatch.setattr(
        config_reader, "load_config", Mock(side_effect=FileNotFoundError)
    )
    delegated = Mock()
    monkeypatch.setattr(direct_subset, "run_subset", delegated)
    with pytest.raises(SystemExit) as exc:
        direct_subset.main()
    assert exc.value.code == 1
    assert "missing.json" in capsys.readouterr().err
    delegated.assert_not_called()
