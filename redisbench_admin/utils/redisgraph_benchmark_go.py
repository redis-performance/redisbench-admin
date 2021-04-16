from redisbench_admin.utils.remote import (
    check_dataset_remote_requirements,
    copy_file_to_remote_setup,
    execute_remote_commands,
)


def spin_up_standalone_remote_redis(
    benchmark_config,
    server_public_ip,
    username,
    private_key,
    local_module_file,
    remote_module_file,
    remote_dataset_file,
    dirname=".",
):
    # copy the rdb to DB machine
    check_dataset_remote_requirements(
        benchmark_config,
        server_public_ip,
        username,
        private_key,
        remote_dataset_file,
        dirname,
    )

    # copy the module to the DB machine
    copy_file_to_remote_setup(
        server_public_ip, username, private_key, local_module_file, remote_module_file
    )
    execute_remote_commands(
        server_public_ip,
        username,
        private_key,
        ["chmod 755 {}".format(remote_module_file)],
    )
    # start redis-server
    commands = [
        "redis-server --dir /tmp/ --daemonize yes --protected-mode no --loadmodule {}".format(
            remote_module_file
        )
    ]
    execute_remote_commands(server_public_ip, username, private_key, commands)


def setup_remote_benchmark_tool_redisgraph_benchmark_go(
    client_public_ip, username, private_key, redisbenchmark_go_link
):
    commands = [
        "wget {} -q -O /tmp/redisgraph-benchmark-go".format(redisbenchmark_go_link),
        "chmod 755 /tmp/redisgraph-benchmark-go",
    ]
    execute_remote_commands(client_public_ip, username, private_key, commands)
