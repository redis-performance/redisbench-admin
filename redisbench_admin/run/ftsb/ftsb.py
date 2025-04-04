#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#

from redisbench_admin.utils.local import check_if_needs_remote_fetch


def prepare_ftsb_benchmark_command(
    executable_path: str,
    server_private_ip: object,
    server_plaintext_port: object,
    benchmark_config: object,
    current_workdir,
    result_file: str,
    remote_queries_file,
    is_remote: bool,
    cluster_api_enabled: bool = False,
    redis_password=None,
):
    """
    Prepares prepare_ftsb_benchmark_command command parameters
    :param executable_path:
    :param server_private_ip:
    :param server_plaintext_port:
    :param benchmark_config:
    :param current_workdir:
    :return: [string] containing the required command to run the benchmark given the configurations
    """
    command_arr = [executable_path]
    command_arr.extend(
        ["--host", "{}:{}".format(server_private_ip, server_plaintext_port)]
    )
    if redis_password is not None:
        command_arr.extend(["--a", redis_password])
    if cluster_api_enabled is True:
        command_arr.extend(["--cluster-mode"])
    if "parameters" in benchmark_config:
        for k in benchmark_config["parameters"]:
            if "input" in k:
                input_file = k["input"]
                input_file = check_if_needs_remote_fetch(
                    input_file, "/tmp", None, remote_queries_file, is_remote
                )
                command_arr.extend(["--input", input_file])
            else:
                for kk in k.keys():
                    command_arr.extend(["--{}".format(kk), str(k[kk])])

    command_arr.extend(["--json-out-file", result_file])

    command_str = " ".join(command_arr)
    return command_arr, command_str


def extract_ftsb_extra_links(
    benchmark_config, benchmark_tool, config_key="clientconfig", architecture="x86_64"
):
    remote_tool_link = "/tmp/{}".format(benchmark_tool)
    arch_txt = "amd64"
    if architecture == "aarch64":
        arch_txt = "arm64"

    tool_link = (
        "https://s3.amazonaws.com/benchmarks.redislabs/"
        + f"redisearch/tools/ftsb/{benchmark_tool}_linux_{arch_txt}"
    )
    queries_file_link = None
    for entry in benchmark_config[config_key]:
        if "parameters" in entry:
            for parameter in entry["parameters"]:
                if "input" in parameter:
                    queries_file_link = parameter["input"]
    return queries_file_link, remote_tool_link, tool_link
