#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#

from unittest import mock

import pytest

from redisbench_admin.run.cluster import (
    remote_binary_supports_cluster_bus_port_protected_mode,
)
from redisbench_admin.environments.oss_cluster import (
    binary_supports_cluster_bus_port_protected_mode,
    generate_cluster_redis_server_args,
)


def test_generate_startup_nodes_array():
    assert True


def test_generate_cluster_redis_server_args_passes_redis_7_directive():
    """Test that enable_redis_7_config_directives parameter is passed through."""
    command, logfile = generate_cluster_redis_server_args(
        binary="redis-server",
        dbdir="/tmp",
        local_module_file=None,
        ip="127.0.0.1",
        port=6379,
        configuration_parameters=None,
        daemonize="no",
        modules_configuration_parameters_map={},
        logname_prefix=None,
        enable_debug_command="yes",
        enable_redis_7_config_directives=True,
    )

    # The command should be a list containing the redis-server args
    assert isinstance(command, list)
    assert "redis-server" in command
    # Verify cluster-specific args are present
    assert "--enable-debug-command" in command
    assert command[command.index("--enable-debug-command") + 1] == "yes"


def test_generate_cluster_redis_server_args_without_redis_7_directive():
    """Test that --enable-debug-command is not included when enable_redis_7_config_directives=False."""
    command, logfile = generate_cluster_redis_server_args(
        binary="redis-server",
        dbdir="/tmp",
        local_module_file=None,
        ip="127.0.0.1",
        port=6379,
        configuration_parameters=None,
        daemonize="no",
        modules_configuration_parameters_map={},
        logname_prefix=None,
        enable_debug_command="yes",
        enable_redis_7_config_directives=False,
    )

    # The command should be a list containing the redis-server args
    assert isinstance(command, list)
    assert "redis-server" in command
    # Verify that --enable-debug-command is NOT present when directive is disabled
    assert "--enable-debug-command" not in command


def _cluster_args(**kwargs):
    command, _ = generate_cluster_redis_server_args(
        binary="redis-server",
        dbdir="/tmp",
        local_module_file=None,
        ip="127.0.0.1",
        port=6379,
        daemonize="no",
        **kwargs,
    )
    return command


def test_cluster_bus_port_protected_mode_waived_only_when_requested():
    assert "--cluster-bus-port-protected-mode" not in _cluster_args()
    command = _cluster_args(waive_cluster_bus_port_protection=True)
    assert command[command.index("--cluster-bus-port-protected-mode") + 1] == "no"


def test_cluster_bus_port_protected_mode_explicit_value_comes_last():
    # Redis keeps the last occurrence, so an explicit spec value must follow the waiver.
    command = _cluster_args(
        configuration_parameters={"cluster-bus-port-protected-mode": "yes"},
        waive_cluster_bus_port_protection=True,
    )
    positions = [
        i for i, a in enumerate(command) if a == "--cluster-bus-port-protected-mode"
    ]
    assert len(positions) == 2
    assert command[positions[-1] + 1] == "yes"


def test_binary_supports_cluster_bus_port_protected_mode(tmp_path):
    new = tmp_path / "new-redis"
    new.write_bytes(b"\x00cluster-bus-port-protected-mode\x00")
    old = tmp_path / "old-redis"
    old.write_bytes(b"\x00cluster-enabled\x00")
    new.chmod(0o755)
    old.chmod(0o755)
    assert binary_supports_cluster_bus_port_protected_mode(str(new))
    assert not binary_supports_cluster_bus_port_protected_mode(str(old))
    assert not binary_supports_cluster_bus_port_protected_mode(str(tmp_path / "missing"))


@pytest.mark.parametrize(
    "exit_status,expected", [(0, True), (1, False), (2, False), (-1, False)]
)
def test_remote_binary_supports_cluster_bus_port_protected_mode(
    exit_status, expected
):
    with mock.patch(
        "redisbench_admin.run.cluster.execute_remote_commands",
        return_value=[[exit_status, [], []]],
    ) as execute:
        assert (
            remote_binary_supports_cluster_bus_port_protected_mode(
                "1.2.3.4", "ubuntu", "key", 22, "redis-server"
            )
            is expected
        )
    probe = execute.call_args[0][3][0]
    assert "cluster-bus-port-protected-mode" in probe and "redis-server" in probe


def test_binary_supports_cluster_bus_port_protected_mode_unreadable():
    with mock.patch(
        "redisbench_admin.environments.oss_cluster.shutil.which",
        return_value="/usr/bin/redis-server",
    ), mock.patch("builtins.open", side_effect=PermissionError("denied")):
        assert not binary_supports_cluster_bus_port_protected_mode("redis-server")


def test_remote_probe_quotes_binary_path():
    binary = "/opt/my redis/redis-server"
    with mock.patch(
        "redisbench_admin.run.cluster.execute_remote_commands",
        return_value=[[0, [], []]],
    ) as execute:
        remote_binary_supports_cluster_bus_port_protected_mode(
            "1.2.3.4", "ubuntu", "key", 22, binary
        )
    probe = execute.call_args[0][3][0]
    assert "\"$(command -v '/opt/my redis/redis-server')\"" in probe
