#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import datetime
import logging

from redisbench_admin.run.common import (
    prepare_benchmark_parameters,
)
from redisbench_admin.run.run import calculate_client_tool_duration_and_check
from redisbench_admin.run_remote.remote_helpers import (
    benchmark_tools_sanity_check,
    remote_tool_pre_bench_step,
    post_process_remote_run,
)
from redisbench_admin.utils.benchmark_config import extract_benchmark_tool_settings
from redisbench_admin.utils.remote import (
    execute_remote_commands,
    fetch_file_from_remote_setup,
)


def run_remote_client_tool(
    allowed_tools,
    artifact_version,
    benchmark_config,
    client_public_ip,
    cluster_api_enabled,
    local_bench_fname,
    remote_results_file,
    return_code,
    server_plaintext_port,
    server_private_ip,
    start_time_ms,
    start_time_str,
    username,
    config_key,
    os_str,
    arch_str,
    step_name,
    warn_min_duration,
    client_ssh_port,
    private_key,
):
    (
        benchmark_min_tool_version,
        benchmark_min_tool_version_major,
        benchmark_min_tool_version_minor,
        benchmark_min_tool_version_patch,
        benchmark_tool,
        benchmark_tool_source,
        _,
        _,
    ) = extract_benchmark_tool_settings(benchmark_config, config_key)
    benchmark_tools_sanity_check(allowed_tools, benchmark_tool)
    # setup the benchmark tool
    remote_tool_pre_bench_step(
        benchmark_config,
        benchmark_min_tool_version,
        benchmark_min_tool_version_major,
        benchmark_min_tool_version_minor,
        benchmark_min_tool_version_patch,
        benchmark_tool,
        client_public_ip,
        username,
        benchmark_tool_source,
        config_key,
        os_str,
        arch_str,
        client_ssh_port,
        private_key,
    )
    command, command_str = prepare_benchmark_parameters(
        benchmark_config,
        benchmark_tool,
        server_plaintext_port,
        server_private_ip,
        remote_results_file,
        True,
        None,
        cluster_api_enabled,
        config_key,
    )
    tmp = None
    if benchmark_tool == "redis-benchmark":
        tmp = local_bench_fname
        local_bench_fname = "result.csv"
    benchmark_start_time = datetime.datetime.now()
    # run the benchmark
    remote_run_result, stdout, _ = run_remote_benchmark(
        client_public_ip,
        username,
        private_key,
        remote_results_file,
        local_bench_fname,
        command_str,
        client_ssh_port,
    )
    benchmark_end_time = datetime.datetime.now()
    benchmark_duration_seconds = calculate_client_tool_duration_and_check(
        benchmark_end_time, benchmark_start_time, step_name, warn_min_duration
    )
    (
        artifact_version,
        local_bench_fname,
        results_dict,
        return_code,
    ) = post_process_remote_run(
        artifact_version,
        benchmark_config,
        benchmark_tool,
        local_bench_fname,
        return_code,
        start_time_ms,
        start_time_str,
        stdout,
        tmp,
    )
    return (
        artifact_version,
        benchmark_duration_seconds,
        local_bench_fname,
        remote_run_result,
        results_dict,
        return_code,
    )


def run_remote_benchmark(
    client_public_ip,
    username,
    private_key,
    remote_results_file,
    local_results_file,
    command,
    ssh_port=22,
):
    remote_run_result = False
    res = execute_remote_commands(
        client_public_ip, username, private_key, [command], ssh_port
    )
    recv_exit_status, stdout, stderr = res[0]

    if recv_exit_status != 0:
        logging.error(
            "Exit status of remote command execution {}. Printing stdout and stderr".format(
                recv_exit_status
            )
        )
        logging.error("remote process stdout: {}".format(stdout))
        logging.error("remote process stderr: {}".format(stderr))
    else:
        logging.info(
            "Remote process exited normally. Exit code {}. Printing stdout.".format(
                recv_exit_status
            )
        )
        logging.info("remote process stdout: {}".format(stdout))
        logging.info("Extracting the benchmark results")
        remote_run_result = True
        if "ycsb" not in command:
            fetch_file_from_remote_setup(
                client_public_ip,
                username,
                private_key,
                local_results_file,
                remote_results_file,
            )
    return remote_run_result, stdout, stderr
