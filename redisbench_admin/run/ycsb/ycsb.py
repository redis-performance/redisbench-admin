def prepare_ycsb_benchmark_command(
    executable_path: str,
    server_private_ip: object,
    server_plaintext_port: object,
    benchmark_config: object,
    current_workdir,
):
    """
    Prepares ycsb command parameters
    :param executable_path:
    :param server_private_ip:
    :param server_plaintext_port:
    :param benchmark_config:
    :param current_workdir:
    :return: [string] containing the required command to run the benchmark given the configurations
    """
    command_arr = [executable_path]

    # we need the csv output
    database = None
    step = None
    workload = None
    threads = None
    override_workload_properties = []
    for k in benchmark_config["parameters"]:
        if "database" in k:
            database = k["database"]
        if "step" in k:
            step = k["step"]
        if "workload" in k:
            workload = k["workload"]
            if workload.startswith("./"):
                workload = "{}{}".format(current_workdir, workload[1:])
        if "threads" in k:
            threads = k["threads"]
        if "override_workload_properties" in k:
            override_workload_properties = k["override_workload_properties"]

    command_arr.append(step)
    command_arr.append(database)

    command_arr.extend(["-P", "{}".format(workload)])
    if threads:
        command_arr.extend(["-p", '"threadcount={}"'.format(threads)])

    command_arr.extend(["-p", '"redis.host={}"'.format(server_private_ip)])

    command_arr.extend(["-p", '"redis.port={}"'.format(server_plaintext_port)])

    for prop in override_workload_properties:
        for k, v in prop.items():
            if type(v) == str and v.startswith("./"):
                v = "{}{}".format(current_workdir, v[1:])
            command_arr.extend(["-p", '"{}={}"'.format(k, v)])

    command_str = " ".join(command_arr)
    return command_arr, command_str
