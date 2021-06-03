import csv
import re


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
            if current_workdir is not None and workload.startswith("./"):
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
            if current_workdir is not None and type(v) == str and v.startswith("./"):
                v = "{}{}".format(current_workdir, v[1:])
            command_arr.extend(["-p", "{}={}".format(k, v)])

    command_str = " ".join(command_arr)
    return command_arr, command_str


def post_process_ycsb_results(stdout, start_time_ms, start_time_str):
    results_dict = {
        "Tests": {},
        "StartTime": start_time_ms,
        "StartTimeHuman": start_time_str,
    }
    if type(stdout) == bytes:
        stdout = stdout.decode("ascii")
    if type(stdout) is not list:
        stdout = stdout.splitlines()
    csv_data = list(csv.reader(stdout, delimiter=","))
    start_row = 0
    for row in csv_data:
        if len(row) >= 1:
            if "[OVERALL]" in row[0]:
                break
        start_row = start_row + 1
    for row in csv_data[start_row:]:
        if len(row) >= 3:
            op_group = row[0].strip()[1:-1]
            metric_name = row[1].strip()
            metric_name = re.sub("[^0-9a-zA-Z]+", "_", metric_name)
            value = row[2].strip()
            if op_group not in results_dict["Tests"]:
                results_dict["Tests"][op_group] = {}
            results_dict["Tests"][op_group][metric_name] = value
    return results_dict
