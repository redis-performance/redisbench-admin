#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
"""Tests for the SSH-tunnel reconnection helpers in run_remote.

Covers:
- _try_get_redis_pids: success / partial-fail / all-fail / empty
- _reconnect_ssh_tunnel_and_redis: missing-params guard, happy path with
  env mutation, and old-tunnel-stop failure being swallowed.
"""
from unittest.mock import MagicMock, patch

import pytest

from redisbench_admin.run_remote.run_remote import (
    _reconnect_ssh_tunnel_and_redis,
    _try_get_redis_pids,
)


def _conn_returning_pid(pid):
    conn = MagicMock()
    conn.info.return_value = {"process_id": pid}
    return conn


def _conn_raising(exc):
    conn = MagicMock()
    conn.info.side_effect = exc
    return conn


# ---------- _try_get_redis_pids ----------


def test_try_get_redis_pids_all_success():
    conns = [_conn_returning_pid(111), _conn_returning_pid(222)]
    pids, all_failed = _try_get_redis_pids(conns)
    assert pids == [111, 222]
    assert all_failed is False


def test_try_get_redis_pids_partial_failure_is_not_all_failed():
    conns = [_conn_returning_pid(111), _conn_raising(ConnectionError("boom"))]
    pids, all_failed = _try_get_redis_pids(conns)
    assert pids == [111, None]
    assert all_failed is False


def test_try_get_redis_pids_all_failure_flags_all_failed():
    conns = [
        _conn_raising(ConnectionError("boom-1")),
        _conn_raising(TimeoutError("boom-2")),
    ]
    pids, all_failed = _try_get_redis_pids(conns)
    assert pids == [None, None]
    assert all_failed is True


def test_try_get_redis_pids_empty_conns_is_not_all_failed():
    """Empty conn list shouldn't trigger reconnection logic."""
    pids, all_failed = _try_get_redis_pids([])
    assert pids == []
    assert all_failed is False


def test_try_get_redis_pids_missing_process_id_field_is_success():
    """A response without 'process_id' yields None but is NOT a failure."""
    conn = MagicMock()
    conn.info.return_value = {"redis_version": "7.4.0"}  # no process_id
    pids, all_failed = _try_get_redis_pids([conn])
    assert pids == [None]
    assert all_failed is False


# ---------- _reconnect_ssh_tunnel_and_redis ----------


def _full_env(**overrides):
    env = {
        "server_private_ip": "10.0.0.5",
        "server_public_ip": "1.2.3.4",
        "username": "ubuntu",
        "db_ssh_port": 22,
        "private_key": "/tmp/key.pem",
        "redis_password": "secret",
        "server_plaintext_port": 6379,
        "ssh_tunnel": None,
        "redis_conns": [],
    }
    env.update(overrides)
    return env


@pytest.mark.parametrize(
    "missing_field",
    ["server_private_ip", "server_public_ip", "username", "private_key"],
)
def test_reconnect_raises_when_required_param_missing(missing_field):
    setup_details = {"env": _full_env(**{missing_field: None})}
    with pytest.raises(ConnectionError, match="missing connection parameters"):
        _reconnect_ssh_tunnel_and_redis(setup_details)


def test_reconnect_happy_path_updates_env_and_returns_new_conn_and_tunnel():
    new_conn = MagicMock(name="new_redis_conn")
    new_tunnel = MagicMock(name="new_ssh_tunnel")

    old_tunnel = MagicMock(name="old_ssh_tunnel")
    setup_details = {"env": _full_env(ssh_tunnel=old_tunnel)}

    with patch(
        "redisbench_admin.run.ssh.ssh_tunnel_redisconn",
        return_value=(new_conn, new_tunnel),
    ) as mocked_setup:
        conns, tunnel = _reconnect_ssh_tunnel_and_redis(setup_details)

    # Old tunnel was stopped before opening the new one.
    old_tunnel.stop.assert_called_once()

    # ssh_tunnel_redisconn called with the params from env, in the expected order.
    mocked_setup.assert_called_once_with(
        6379,  # server_plaintext_port
        "10.0.0.5",  # server_private_ip
        "1.2.3.4",  # server_public_ip
        "ubuntu",  # username
        22,  # db_ssh_port
        "/tmp/key.pem",  # private_key
        "secret",  # redis_password
    )

    # Return values
    assert conns == [new_conn]
    assert tunnel is new_tunnel

    # env mutation so subsequent reuses see fresh state.
    assert setup_details["env"]["redis_conns"] == [new_conn]
    assert setup_details["env"]["ssh_tunnel"] is new_tunnel


def test_reconnect_swallows_old_tunnel_stop_failure():
    """If the dead tunnel raises on .stop(), reconnection should still proceed."""
    new_conn = MagicMock()
    new_tunnel = MagicMock()

    flaky_old_tunnel = MagicMock()
    flaky_old_tunnel.stop.side_effect = RuntimeError("tunnel already dead")
    setup_details = {"env": _full_env(ssh_tunnel=flaky_old_tunnel)}

    with patch(
        "redisbench_admin.run.ssh.ssh_tunnel_redisconn",
        return_value=(new_conn, new_tunnel),
    ):
        conns, tunnel = _reconnect_ssh_tunnel_and_redis(setup_details)

    flaky_old_tunnel.stop.assert_called_once()
    assert conns == [new_conn]
    assert tunnel is new_tunnel


def test_reconnect_handles_no_previous_tunnel():
    """If env has no prior ssh_tunnel (None), reconnection still succeeds."""
    new_conn = MagicMock()
    new_tunnel = MagicMock()
    setup_details = {"env": _full_env(ssh_tunnel=None)}

    with patch(
        "redisbench_admin.run.ssh.ssh_tunnel_redisconn",
        return_value=(new_conn, new_tunnel),
    ):
        conns, tunnel = _reconnect_ssh_tunnel_and_redis(setup_details)

    assert conns == [new_conn]
    assert tunnel is new_tunnel
    assert setup_details["env"]["ssh_tunnel"] is new_tunnel
