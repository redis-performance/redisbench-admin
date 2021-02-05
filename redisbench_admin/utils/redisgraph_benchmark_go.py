import logging

from redisbench_admin.utils.remote import checkDatasetRemoteRequirements, copyFileToRemoteSetup, executeRemoteCommands, \
    getFileFromRemoteSetup


def prepareBenchmarkCommand(
        server_private_ip: object, server_plaintext_port: object, benchmark_config: object, results_file: object
) -> str:
    """
    Prepares redisgraph-benchmark-go command parameters
    :param server_private_ip:
    :param server_plaintext_port:
    :param benchmark_config:
    :param results_file:
    :return: string containing the required command to run the benchmark given the configurations
    """
    queries_str = ""
    for k in benchmark_config["queries"]:
        query = k["q"]
        queries_str += ' -query "{}"'.format(query)
        if "ratio" in k:
            queries_str += " -query-ratio {}".format(k["ratio"])
    for k in benchmark_config["clientconfig"]:
        if "graph" in k:
            queries_str += " -graph-key {}".format(k["graph"])
        if "clients" in k:
            queries_str += " -c {}".format(k["clients"])
        if "requests" in k:
            queries_str += " -n {}".format(k["requests"])
        if "rps" in k:
            queries_str += " -rps {}".format(k["rps"])
    queries_str += " -h {}".format(server_private_ip)
    queries_str += " -p {}".format(server_plaintext_port)

    queries_str += " -json-out-file {}".format(results_file)
    logging.info(
        "Running the benchmark with the following parameters: {}".format(queries_str)
    )
    return queries_str


def spinUpRemoteRedis(
        benchmark_config,
        server_public_ip,
        username,
        private_key,
        local_module_file,
        remote_module_file,
        remote_dataset_file,
):
    # copy the rdb to DB machine
    dataset = None
    checkDatasetRemoteRequirements(
        benchmark_config,
        server_public_ip,
        username,
        private_key,
        remote_dataset_file,
    )

    # copy the module to the DB machine
    copyFileToRemoteSetup(
        server_public_ip,
        username,
        private_key,
        local_module_file,
        remote_module_file,
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
        server_private_ip,
        server_plaintext_port,
        benchmark_config,
        remote_results_file,
        local_results_file,
):
    queries_str = prepareBenchmarkCommand(
        server_private_ip, server_plaintext_port, benchmark_config, remote_results_file
    )
    commands = ["/tmp/redisgraph-benchmark-go {}".format(queries_str)]
    executeRemoteCommands(client_public_ip, username, private_key, commands)
    logging.info("Extracting the benchmark results")
    getFileFromRemoteSetup(
        client_public_ip,
        username,
        private_key,
        local_results_file,
        remote_results_file,
    )
