#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#

from redisbench_admin.environments.oss_cluster import (
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
