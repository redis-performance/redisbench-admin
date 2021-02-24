import csv
import logging
import shlex


def redis_benchmark_from_stdout_csv_to_json(stdout, start_time, start_time_str):
    results_dict = {"Tests": {}, "StartTime": int(start_time.strftime("%s")),
                    "StartTimeHuman": start_time_str}
    csv_data = list(csv.reader(stdout.decode('ascii').splitlines(), delimiter=","))
    header = csv_data[0]
    for row in csv_data[1:]:
        test_name = row[0]
        results_dict["Tests"][test_name] = {}
        for pos, value in enumerate(row[1:]):
            results_dict["Tests"][test_name][header[pos + 1]] = value
    return results_dict


def prepareRedisBenchmarkCommand(
        executable_path: str,
        server_private_ip: object,
        server_plaintext_port: object,
        benchmark_config: object,
) -> str:
    """
    Prepares redis-benchmark command parameters
    :param server_private_ip:
    :param server_plaintext_port:
    :param benchmark_config:
    :return: string containing the required command to run the benchmark given the configurations
    """
    queries_str = [executable_path]
    queries_str.extend(["-h", "{}".format(server_private_ip)])
    queries_str.extend(["-p", "{}".format(server_plaintext_port)])

    # we need the csv output
    queries_str.extend(["--csv", "-e"])
    last_append = None
    for k in benchmark_config["parameters"]:
        if "clients" in k:
            queries_str.extend(["-c", "{}".format(k["clients"])])
        if "requests" in k:
            queries_str.extend(["-n", "{}".format(k["requests"])])
        if "threads" in k:
            queries_str.extend(["--threads", "{}".format(k["threads"])])
        if "pipeline" in k:
            queries_str.extend(["-P", "{}".format(k["pipeline"])])
        # if we have the command keywork then it needs to be at the end of args
        if "command" in k:
            last_append = shlex.split(k["command"])
    if last_append is not None:
        queries_str.extend(last_append)
    logging.info(
        "Running the benchmark with the following parameters: {}".format(
            " ".join(queries_str)
        )
    )
    return queries_str
