#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#

import logging
import re
import subprocess
import shlex
from redisbench_admin.utils.remote import execute_remote_commands
from redisbench_admin.utils.local import check_if_needs_remote_fetch


def extract_monitor_input_from_arguments(arguments):
    """
    Extract --monitor-input value from arguments string.

    Args:
        arguments: The arguments string from benchmark config

    Returns:
        The monitor-input value if found, None otherwise
    """
    if not arguments:
        return None

    # Match --monitor-input followed by value (with = or space separator)
    # Handles: --monitor-input=value, --monitor-input value, quoted values
    pattern = r"--monitor-input[=\s]+(?:(['\"])(.+?)\1|(\S+))"
    match = re.search(pattern, arguments)
    if match:
        # Return the quoted value (group 2) or unquoted value (group 3)
        return match.group(2) if match.group(2) else match.group(3)
    return None


def replace_monitor_input_in_arguments(arguments, old_value, new_value):
    """
    Replace monitor-input value in arguments string.

    Args:
        arguments: The original arguments string
        old_value: The URL/path to replace
        new_value: The new local path

    Returns:
        Modified arguments string with replaced value
    """
    if not arguments or not old_value:
        return arguments

    # Replace the old value with new value, preserving quotes if present
    return arguments.replace(old_value, new_value)


def process_monitor_input_in_arguments(
    arguments, local_temp_dir="/tmp", is_remote=False, remote_monitor_file=None
):
    """
    Process arguments string to handle --monitor-input URLs.
    Downloads remote files (http/https/s3) and replaces URLs with local paths.

    Args:
        arguments: The arguments string from benchmark config
        local_temp_dir: Directory to store downloaded files
        is_remote: Whether this is for remote execution
        remote_monitor_file: For remote execution, the path where file will be on remote

    Returns:
        Tuple of (modified_arguments, monitor_input_url_or_none)
    """
    if not arguments:
        return arguments, None

    monitor_input = extract_monitor_input_from_arguments(arguments)
    if not monitor_input:
        return arguments, None

    # Check if it's a URL that needs to be fetched
    if monitor_input.startswith("http") or monitor_input.startswith("s3"):
        logging.info("Detected remote monitor-input URL: {}".format(monitor_input))

        if is_remote and remote_monitor_file:
            # For remote execution, use the remote file path
            # The actual download happens in setup_remote_benchmark_tool_requirements
            local_path = remote_monitor_file
            logging.info(
                "Using remote monitor file path: {}".format(remote_monitor_file)
            )
        else:
            # For local execution, download the file
            local_path = check_if_needs_remote_fetch(
                monitor_input, local_temp_dir, None, None, is_remote
            )
            logging.info(
                "Downloaded monitor-input to local path: {}".format(local_path)
            )

        # Replace URL with local/remote path in arguments
        modified_arguments = replace_monitor_input_in_arguments(
            arguments, monitor_input, local_path
        )
        return modified_arguments, monitor_input

    return arguments, None


def prepare_memtier_benchmark_command(
    executable_path: str,
    server_private_ip: object,
    server_plaintext_port: object,
    benchmark_config: object,
    cluster_api_enabled: bool,
    result_file: str,
    redis_pass=None,
    local_temp_dir="/tmp",
    is_remote=False,
    remote_monitor_file=None,
):
    command_arr = [executable_path]
    command_arr.extend(["-s", "{}".format(server_private_ip)])
    command_arr.extend(["-p", "{}".format(server_plaintext_port)])
    command_arr.extend(["--hide-histogram"])
    if redis_pass is not None:
        command_arr.extend(["-a", "{}".format(redis_pass)])
    if cluster_api_enabled:
        command_arr.extend(["--cluster-mode"])
    if "parameters" in benchmark_config:
        parameters = benchmark_config["parameters"]

        # Handle v0.4 spec where parameters is a dict
        if isinstance(parameters, dict):
            for key, value in parameters.items():
                command_arr.extend(["--{}".format(key), str(value)])

        # Handle v0.1-0.3 spec where parameters is a list of dicts
        elif isinstance(parameters, list):
            for k in parameters:
                if isinstance(k, dict):
                    for kk in k.keys():
                        command_arr.extend(["--{}".format(kk), str(k[kk])])

    command_arr.extend(["--json-out-file", result_file])
    command_str = " ".join(command_arr)
    if "arguments" in benchmark_config:
        # Process monitor-input URLs before adding arguments
        arguments = benchmark_config["arguments"]
        arguments, _ = process_monitor_input_in_arguments(
            arguments, local_temp_dir, is_remote, remote_monitor_file
        )
        command_str = command_str + " " + arguments
        command_arr.extend(shlex.split(arguments))
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
