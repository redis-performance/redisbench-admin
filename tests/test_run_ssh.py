#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
"""Tests for redisbench_admin.run.ssh.ssh_tunnel_redisconn's ping retry.

redis-server is started remotely with `--daemonize yes`, which can return
before the server has finished loading modules and binding its listening
socket. These tests cover the retry loop that guards against that race.
"""

from unittest.mock import MagicMock, patch

import pytest
import redis

from redisbench_admin.run import ssh as ssh_module


def _patch_common(monkeypatch, ping_side_effect):
    monkeypatch.setattr(
        ssh_module.paramiko.RSAKey, "from_private_key_file", MagicMock()
    )
    monkeypatch.setattr(ssh_module, "connect_remote_ssh", MagicMock())
    monkeypatch.setattr(ssh_module, "check_connection", MagicMock(return_value=True))
    monkeypatch.setattr(ssh_module, "time", MagicMock(sleep=MagicMock()))

    tunnel = MagicMock()
    tunnel.local_bind_port = 12345
    monkeypatch.setattr(
        ssh_module, "SSHTunnelForwarder", MagicMock(return_value=tunnel)
    )

    redis_conn = MagicMock()
    redis_conn.ping.side_effect = ping_side_effect
    monkeypatch.setattr(ssh_module.redis, "Redis", MagicMock(return_value=redis_conn))
    return tunnel, redis_conn


def test_ssh_tunnel_redisconn_retries_until_ping_succeeds(monkeypatch):
    tunnel, redis_conn = _patch_common(
        monkeypatch,
        ping_side_effect=[
            redis.exceptions.ConnectionError("still starting"),
            redis.exceptions.ConnectionError("still starting"),
            True,
        ],
    )

    conn, returned_tunnel = ssh_module.ssh_tunnel_redisconn(
        6379, "10.0.0.5", "1.2.3.4", "ubuntu", 22, "/tmp/key.pem"
    )

    assert conn is redis_conn
    assert returned_tunnel is tunnel
    assert redis_conn.ping.call_count == 3
    assert ssh_module.time.sleep.call_count == 2


def test_ssh_tunnel_redisconn_raises_after_exhausting_retries(monkeypatch):
    _patch_common(
        monkeypatch,
        ping_side_effect=redis.exceptions.ConnectionError("never comes up"),
    )

    with pytest.raises(redis.exceptions.ConnectionError, match="never comes up"):
        ssh_module.ssh_tunnel_redisconn(
            6379, "10.0.0.5", "1.2.3.4", "ubuntu", 22, "/tmp/key.pem"
        )

    assert ssh_module.time.sleep.call_count == ssh_module.REDIS_PING_RETRIES


def test_ssh_tunnel_redisconn_succeeds_on_first_ping_without_sleeping(monkeypatch):
    tunnel, redis_conn = _patch_common(monkeypatch, ping_side_effect=[True])

    conn, returned_tunnel = ssh_module.ssh_tunnel_redisconn(
        6379, "10.0.0.5", "1.2.3.4", "ubuntu", 22, "/tmp/key.pem"
    )

    assert conn is redis_conn
    assert returned_tunnel is tunnel
    assert redis_conn.ping.call_count == 1
    ssh_module.time.sleep.assert_not_called()
