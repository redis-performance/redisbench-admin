from redisbench_admin.utils.remote import (
    checkDatasetRemoteRequirements,
    copyFileToRemoteSetup,
    executeRemoteCommands,
)


def spinUpRemoteRedis(
        benchmark_config,
        server_public_ip,
        username,
        private_key,
        local_module_file,
        remote_module_file,
        remote_dataset_file,
        dirname=".",
):
    res = False
    # copy the rdb to DB machine
    dataset = None
    checkDatasetRemoteRequirements(
        benchmark_config,
        server_public_ip,
        username,
        private_key,
        remote_dataset_file,
        dirname
    )

    # copy the module to the DB machine
    copyFileToRemoteSetup(
        server_public_ip,
        username,
        private_key,
        local_module_file,
        remote_module_file
    )
    executeRemoteCommands(
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
    executeRemoteCommands(server_public_ip, username, private_key, commands)


def setupRemoteBenchmarkTool_redisgraph_benchmark_go(
        client_public_ip, username, private_key, redisbenchmark_go_link
):
    commands = [
        "wget {} -q -O /tmp/redisgraph-benchmark-go".format(redisbenchmark_go_link),
        "chmod 755 /tmp/redisgraph-benchmark-go",
    ]
    executeRemoteCommands(client_public_ip, username, private_key, commands)
