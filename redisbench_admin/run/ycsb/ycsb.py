import logging
import shutil

import wget

from redisbench_admin.utils.utils import decompress_file


def ensure_ycsb_on_path(toolname, toolremotepath, storepath):
    """
    Helper method to ensure ycsb is on path
    Args:
        toolname: name of the tool ( should be ycsb most of times )
        toolremotepath: remote path to retrieve tool from
        storepath: store path
    """
    which_benchmark_tool = shutil.which(toolname)
    if which_benchmark_tool is None:
        logging.warning(
            "Benchmark tool {} was not detected on path. Retrieving it from {} and storing it at {}".format(toolname,
                                                                                                            toolremotepath,
                                                                                                            storepath))
        wget.download(toolremotepath, storepath)
        decompress_file(storepath)


def prepareYCSBBenchmarkCommand(
        executable_path: str,
        server_private_ip: object,
        server_plaintext_port: object,
        benchmark_config: object,
):
    """
    Prepares ycsb command parameters
    :param server_private_ip:
    :param server_plaintext_port:
    :param benchmark_config:
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
        if "threads" in k:
            threads = k["threads"]
        if "override_workload_properties" in k:
            override_workload_properties = k["override_workload_properties"]

    command_arr.append(step)
    command_arr.append(database)

    command_arr.extend(["-P", "{}".format(workload)])
    if threads:
        command_arr.extend(["-p", "\"threadcount={}\"".format(threads)])

    command_arr.extend(["-p", "\"redis.host={}\"".format(server_private_ip)])

    command_arr.extend(["-p", "\"redis.port={}\"".format(server_plaintext_port)])

    for property in override_workload_properties:
        for k, v in property.items():
            command_arr.extend(["-p", "\"{}={}\"".format(k, v)])

    command_str = " ".join(command_arr)
    logging.info(
        "Running the benchmark with the following parameters:\n\tArgs array: {}\n\tArgs str: {}".format(
            command_arr, command_str
        )
    )
    return command_arr, command_str
