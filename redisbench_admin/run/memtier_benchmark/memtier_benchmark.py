#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#

import logging
import re
import subprocess

from redisbench_admin.utils.remote import execute_remote_commands


def prepare_memtier_benchmark_command(
    executable_path: str,
    server_private_ip: object,
    server_plaintext_port: object,
    benchmark_config: object,
    cluster_api_enabled: bool,
    result_file: str,
):
    command_arr = [executable_path]
    command_arr.extend(["-s", "{}".format(server_private_ip)])
    command_arr.extend(["-p", "{}".format(server_plaintext_port)])
    command_arr.extend(["--hide-histogram"])

    if cluster_api_enabled:
        command_arr.extend(["--cluster-mode"])

    for k in benchmark_config["parameters"]:
        for kk in k.keys():
            command_arr.extend(["--{}".format(kk), str(k[kk])])

    command_arr.extend(["--json-out-file", result_file])
    command_str = " ".join(command_arr)
    return command_arr, command_str


def ensure_memtier_benchmark_version_from_input(
    benchmark_min_tool_version,
    benchmark_min_tool_version_major,
    benchmark_min_tool_version_minor,
    benchmark_min_tool_version_patch,
    benchmark_tool,
    stdout,
):
    version_output = stdout.split("\n")[0]
    logging.info(
        "Detected benchmark config tool {} with version {}".format(
            benchmark_tool, version_output
        )
    )
    p = re.compile(r"memtier_benchmark (\d+)\.(\d+)\.(\d+)")
    m = p.match(version_output)
    if m is None:
        raise Exception(
            "Unable to detect benchmark tool version, and the benchmark requires a min version: {}".format(
                benchmark_min_tool_version
            )
        )
    major = m.group(1)
    minor = m.group(2)
    patch = m.group(3)
    if (
        major < benchmark_min_tool_version_major
        or (
            major == benchmark_min_tool_version_major
            and minor < benchmark_min_tool_version_minor
        )
        or (
            major == benchmark_min_tool_version_major
            and minor == benchmark_min_tool_version_minor
            and patch < benchmark_min_tool_version_patch
        )
    ):
        raise Exception(
            "Detected benchmark version that is inferior than the minimum required. {} < {}".format(
                version_output, benchmark_min_tool_version
            )
        )


def memtier_benchmark_ensure_min_version_local(
    benchmark_tool,
    benchmark_min_tool_version,
    benchmark_min_tool_version_major,
    benchmark_min_tool_version_minor,
    benchmark_min_tool_version_patch,
):
    benchmark_client_process = subprocess.Popen(
        args=[benchmark_tool, "--version"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    (stdout, sterr) = benchmark_client_process.communicate()
    ensure_memtier_benchmark_version_from_input(
        benchmark_min_tool_version,
        benchmark_min_tool_version_major,
        benchmark_min_tool_version_minor,
        benchmark_min_tool_version_patch,
        benchmark_tool,
        stdout.decode("ascii"),
    )


def memtier_benchmark_ensure_min_version_remote(
    benchmark_tool,
    benchmark_min_tool_version,
    benchmark_min_tool_version_major,
    benchmark_min_tool_version_minor,
    benchmark_min_tool_version_patch,
    client_public_ip,
    username,
    private_key,
    client_ssh_port,
):
    res = execute_remote_commands(
        client_public_ip,
        username,
        private_key,
        ["{} --version".format(benchmark_tool)],
        client_ssh_port,
    )
    recv_exit_status, stdout, stderr = res[0]
    ensure_memtier_benchmark_version_from_input(
        benchmark_min_tool_version,
        benchmark_min_tool_version_major,
        benchmark_min_tool_version_minor,
        benchmark_min_tool_version_patch,
        benchmark_tool,
        stdout[0],
    )
