import csv
import logging
import re
import shlex
import subprocess

from redisbench_admin.utils.remote import execute_remote_commands


def redis_benchmark_from_stdout_csv_to_json(
    csv_data, start_time_ms, start_time_str, overload_test_name=None
):
    results_dict = {
        "Tests": {},
        "StartTime": start_time_ms,
        "StartTimeHuman": start_time_str,
    }
    csv_data = list(csv.reader(csv_data.splitlines(), delimiter=","))
    header = csv_data[0]
    for row in csv_data[1:]:
        test_name = row[0]
        if overload_test_name is not None:
            test_name = overload_test_name
        results_dict["Tests"][test_name] = {}
        for pos, value in enumerate(row[1:]):
            results_dict["Tests"][test_name][header[pos + 1]] = value
    return results_dict


def prepare_redis_benchmark_command(
    executable_path: str,
    server_private_ip: object,
    server_plaintext_port: object,
    benchmark_config: object,
):
    """
    Prepares redis-benchmark command parameters
    :param executable_path:
    :param server_private_ip:
    :param server_plaintext_port:
    :param benchmark_config:
    :return: [string] containing the required command to run
        the benchmark given the configurations
    """
    command_arr = [executable_path]
    command_arr.extend(["-h", "{}".format(server_private_ip)])
    command_arr.extend(["-p", "{}".format(server_plaintext_port)])

    # we need the csv output
    command_arr.extend(["--csv", "-e"])
    last_append = None
    last_str = ""
    for k in benchmark_config["parameters"]:
        if "clients" in k:
            command_arr.extend(["-c", "{}".format(k["clients"])])
        if "requests" in k:
            command_arr.extend(["-n", "{}".format(k["requests"])])
        if "threads" in k:
            command_arr.extend(["--threads", "{}".format(k["threads"])])
        if "pipeline" in k:
            command_arr.extend(["-P", "{}".format(k["pipeline"])])
        # if we have the command keywork then it needs to be at the end of args
        if "command" in k:
            last_str = k["command"]
            last_append = shlex.split(k["command"])
    command_str = " ".join(command_arr)
    if last_append is not None:
        command_arr.extend(last_append)
        command_str = command_str + " " + last_str
    logging.info(
        "Running the benchmark with the following parameters:"
        "\n\tArgs array: {}\n\tArgs str: {}".format(command_arr, command_str)
    )
    return command_arr, command_str


def ensure_redis_benchmark_version_from_input(
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
    p = re.compile(r"redis-benchmark (\d+)\.(\d+)\.(\d+)")
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


def redis_benchmark_ensure_min_version_local(
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
    ensure_redis_benchmark_version_from_input(
        benchmark_min_tool_version,
        benchmark_min_tool_version_major,
        benchmark_min_tool_version_minor,
        benchmark_min_tool_version_patch,
        benchmark_tool,
        stdout.decode("ascii"),
    )


def redis_benchmark_ensure_min_version_remote(
    benchmark_tool,
    benchmark_min_tool_version,
    benchmark_min_tool_version_major,
    benchmark_min_tool_version_minor,
    benchmark_min_tool_version_patch,
    client_public_ip,
    username,
    private_key,
):
    res = execute_remote_commands(
        client_public_ip, username, private_key, ["{} --version".format(benchmark_tool)]
    )
    recv_exit_status, stdout, stderr = res[0]
    ensure_redis_benchmark_version_from_input(
        benchmark_min_tool_version,
        benchmark_min_tool_version_major,
        benchmark_min_tool_version_minor,
        benchmark_min_tool_version_patch,
        benchmark_tool,
        stdout[0],
    )
