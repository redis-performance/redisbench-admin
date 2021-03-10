import logging


def prepareRedisGraphBenchmarkGoCommand(
        executable_path: str,
        server_private_ip: object,
        server_plaintext_port: object,
        benchmark_config: object,
        results_file: object,
):
    """
    Prepares redisgraph-benchmark-go command parameters
    :param server_private_ip:
    :param server_plaintext_port:
    :param benchmark_config:
    :param results_file:
    :return: string containing the required command to run the benchmark given the configurations
    """
    queries_str = [executable_path]
    for k in benchmark_config["parameters"]:
        if "graph" in k:
            queries_str.extend(["-graph-key", "'{}'".format(k["graph"])])
        if "clients" in k:
            queries_str.extend(["-c", "{}".format(k["clients"])])
        if "requests" in k:
            queries_str.extend(["-n", "{}".format(k["requests"])])
        if "rps" in k:
            queries_str.extend(["-rps", "{}".format(k["rps"])])
        if "queries" in k:
            for kk in k["queries"]:
                query = kk["q"]
                queries_str.extend(["-query", "'{}'".format(query)])
                if "ratio" in kk:
                    queries_str.extend(["-query-ratio", "{}".format(kk["ratio"])])
    queries_str.extend(["-h", "{}".format(server_private_ip)])
    queries_str.extend(["-p", "{}".format(server_plaintext_port)])
    queries_str.extend(["-json-out-file", "{}".format(results_file)])
    logging.info(
        "Running the benchmark with the following parameters: {}".format(
            " ".join(queries_str)
        )
    )
    return queries_str
