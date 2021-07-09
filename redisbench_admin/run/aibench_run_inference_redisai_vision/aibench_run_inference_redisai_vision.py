#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#

from redisbench_admin.utils.local import check_if_needs_remote_fetch


def prepare_aibench_benchmark_command(
    executable_path: str,
    server_private_ip: object,
    server_plaintext_port: object,
    benchmark_config: object,
    current_workdir,
    result_file: str,
    remote_queries_file,
    is_remote: bool,
):
    command_arr = [executable_path]

    command_arr.extend(["--host", "{}".format(server_private_ip)])
    command_arr.extend(["--port", "{}".format(server_plaintext_port)])
    if "parameters" in benchmark_config:
        for k in benchmark_config["parameters"]:
            if "file" in k:
                input_file = k["file"]
                input_file = check_if_needs_remote_fetch(
                    input_file, "/tmp", None, remote_queries_file, is_remote
                )
                command_arr.extend(["--file", input_file])
            else:
                for kk in k.keys():
                    command_arr.extend(["--{}".format(kk), str(k[kk])])

    command_arr.extend(["--json-out-file", result_file])

    command_str = " ".join(command_arr)
    return command_arr, command_str


def extract_aibench_extra_links(benchmark_config, benchmark_tool):
    remote_tool_link = "/tmp/{}".format(benchmark_tool)
    tool_link = (
        "https://s3.amazonaws.com/benchmarks.redislabs/"
        + "tools/redisai/aibench/{}_linux_amd64".format(benchmark_tool)
    )
    queries_file_link = None
    for entry in benchmark_config["clientconfig"]:
        if "parameters" in entry:
            for parameter in entry["parameters"]:
                if "file" in parameter:
                    queries_file_link = parameter["file"]
    return queries_file_link, remote_tool_link, tool_link
