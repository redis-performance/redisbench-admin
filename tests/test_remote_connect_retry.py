#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
"""Tests for the retry/backoff behavior of connect_remote_ssh (MOD-19933).

Only establishing the SSH connection is retried, so a retry can never replay
a remote setup step or a benchmark measurement.
"""

import errno
from unittest.mock import MagicMock

import paramiko
import pytest
from paramiko.ssh_exception import NoValidConnectionsError

from redisbench_admin.utils import remote as remote_module


def _connection_refused():
    return NoValidConnectionsError(
        {("127.0.0.1", 22): OSError(errno.ECONNREFUSED, "refused")}
    )


@pytest.fixture
def ssh_clients(monkeypatch):
    monkeypatch.setattr(
        remote_module.paramiko.RSAKey, "from_private_key_file", MagicMock()
    )
    sleep = MagicMock()
    monkeypatch.setattr(remote_module.time, "sleep", sleep)
    clients = []

    def configure(*connect_side_effects):
        for side_effect in connect_side_effects:
            client = MagicMock()
            client.connect.side_effect = side_effect
            clients.append(client)
        monkeypatch.setattr(
            remote_module.paramiko, "SSHClient", MagicMock(side_effect=clients)
        )
        return clients, sleep

    return configure


def _connect(max_retries=3):
    return remote_module.connect_remote_ssh(
        22, "key.pem", "client.example", "ubuntu", max_retries=max_retries
    )


def test_connect_succeeds_on_first_attempt_without_retry(ssh_clients):
    clients, sleep = ssh_clients(None)
    assert _connect() is clients[0]
    sleep.assert_not_called()


def test_connect_bounds_each_attempt_with_timeout(ssh_clients):
    clients, _ = ssh_clients(None)
    remote_module.connect_remote_ssh(
        22, "key.pem", "client.example", "ubuntu", timeout=7
    )
    kwargs = clients[0].connect.call_args.kwargs
    assert kwargs["timeout"] == 7
    assert kwargs["banner_timeout"] == 7


def test_connect_retries_timed_out_connection(ssh_clients):
    clients, sleep = ssh_clients(TimeoutError("timed out"), None)
    assert _connect() is clients[1]
    sleep.assert_called_once_with(1)


def test_connect_retries_refused_connection_then_succeeds(ssh_clients):
    clients, sleep = ssh_clients(_connection_refused(), None)
    assert _connect() is clients[1]
    clients[0].close.assert_called_once_with()
    sleep.assert_called_once_with(1)


def test_connect_retries_ssh_banner_error(ssh_clients):
    clients, sleep = ssh_clients(
        paramiko.SSHException("Error reading SSH protocol banner"), None
    )
    assert _connect() is clients[1]
    sleep.assert_called_once_with(1)


def test_connect_gives_up_after_max_retries_and_reraises(ssh_clients):
    error = _connection_refused()
    clients, sleep = ssh_clients(error, error, error, error)
    with pytest.raises(NoValidConnectionsError) as raised:
        _connect(max_retries=3)
    assert raised.value is error
    assert [c.args[0] for c in sleep.call_args_list] == [1, 2, 4]
    for client in clients:
        client.close.assert_called_once_with()


def test_connect_does_not_retry_authentication_failure(ssh_clients):
    error = paramiko.AuthenticationException("denied")
    clients, sleep = ssh_clients(error)
    with pytest.raises(paramiko.AuthenticationException) as raised:
        _connect()
    assert raised.value is error
    sleep.assert_not_called()


def test_remote_command_is_not_replayed_when_it_fails(ssh_clients, monkeypatch):
    # a failure after the connection is open (e.g. while the benchmark runs)
    # propagates as before: only connection establishment is retried
    clients, sleep = ssh_clients(None)
    clients[0].exec_command.side_effect = paramiko.SSHException("channel closed")
    with pytest.raises(paramiko.SSHException, match="channel closed"):
        remote_module.execute_remote_commands(
            "client.example", "ubuntu", "key.pem", ["memtier_benchmark"], 22
        )
    clients[0].exec_command.assert_called_once()
    sleep.assert_not_called()
