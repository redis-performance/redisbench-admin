import logging

from redisbench_admin.utils.remote import (
    checkDatasetRemoteRequirements,
    copyFileToRemoteSetup,
    executeRemoteCommands,
    getFileFromRemoteSetup,
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


def setupRemoteBenchmark(
        client_public_ip, username, private_key, redisbenchmark_go_link
):
    commands = [
        "wget {} -q -O /tmp/redisgraph-benchmark-go".format(redisbenchmark_go_link),
        "chmod 755 /tmp/redisgraph-benchmark-go",
    ]
    executeRemoteCommands(client_public_ip, username, private_key, commands)


def runRemoteBenchmark(
        client_public_ip,
        username,
        private_key,
        remote_results_file,
        local_results_file,
        command
):
    remote_run_result = False
    res = executeRemoteCommands(client_public_ip, username, private_key, [" ".join(command)])
    recv_exit_status, stdout, stderr = res[0]

    if recv_exit_status != 0:
        logging.error("Exit status of remote command execution {}. Printing stdout and stderr".format(recv_exit_status))
        logging.error("remote process stdout: ".format(stdout))
        logging.error("remote process stderr: ".format(stderr))
    else:
        logging.info("Remote process exited normally. Exit code {}. Printing stdout.".format(recv_exit_status))
        logging.info("remote process stdout: ".format(stdout))
        logging.info("Extracting the benchmark results")
        remote_run_result = True
        getFileFromRemoteSetup(
            client_public_ip,
            username,
            private_key,
            local_results_file,
            remote_results_file,
        )
    return remote_run_result
