"""Retry SSH connection failures before measurement, never benchmark failures."""

import errno
import time
from unittest.mock import Mock

import pytest
from paramiko.ssh_exception import AuthenticationException, NoValidConnectionsError

from redisbench_admin.run_remote import remote_client


def connection_error():
    return NoValidConnectionsError(
        {("127.0.0.1", 22): OSError(errno.ECONNREFUSED, "refused")}
    )


@pytest.fixture
def run_client(monkeypatch):
    monkeypatch.setattr(
        remote_client,
        "extract_benchmark_tool_settings",
        lambda *args: (None, 0, 0, 0, "redis-benchmark", None, None, None),
    )
    monkeypatch.setattr(
        remote_client, "benchmark_tools_sanity_check", lambda *args: None
    )
    monkeypatch.setattr(
        remote_client, "prepare_benchmark_parameters", lambda *args: (None, "benchmark")
    )
    setup = Mock()
    benchmark = Mock(side_effect=RuntimeError("measurement reached"))
    sleep = Mock()
    monkeypatch.setattr(remote_client, "remote_tool_pre_bench_step", setup)
    monkeypatch.setattr(remote_client, "run_remote_benchmark", benchmark)
    monkeypatch.setattr(time, "sleep", sleep)

    def run():
        remote_client.run_remote_client_tool(*([None] * 21))

    return run, setup, benchmark, sleep


def test_setup_connection_failure_then_success(run_client):
    run, setup, benchmark, sleep = run_client
    setup.side_effect = [connection_error(), None]
    with pytest.raises(RuntimeError, match="measurement reached"):
        run()
    assert setup.call_count == 2
    benchmark.assert_called_once()
    sleep.assert_called_once_with(1)


def test_setup_retry_budget_exhausted(run_client):
    run, setup, benchmark, sleep = run_client
    error = connection_error()
    setup.side_effect = error
    with pytest.raises(NoValidConnectionsError) as raised:
        run()
    assert raised.value is error
    assert setup.call_count == 4
    assert [c.args[0] for c in sleep.call_args_list] == [1, 2, 4]
    benchmark.assert_not_called()


@pytest.mark.parametrize(
    "error", [AuthenticationException("denied"), RuntimeError("remote command failed")]
)
def test_other_setup_errors_are_not_retried(run_client, error):
    run, setup, benchmark, sleep = run_client
    setup.side_effect = error
    with pytest.raises(type(error)) as raised:
        run()
    assert raised.value is error
    setup.assert_called_once()
    benchmark.assert_not_called()
    sleep.assert_not_called()


def test_connection_failure_during_measurement_is_not_retried(run_client):
    run, setup, benchmark, sleep = run_client
    error = connection_error()
    benchmark.side_effect = error
    with pytest.raises(NoValidConnectionsError) as raised:
        run()
    assert raised.value is error
    setup.assert_called_once()
    benchmark.assert_called_once()
    sleep.assert_not_called()
