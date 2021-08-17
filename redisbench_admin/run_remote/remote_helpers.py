#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import json
import logging
import os

from redisbench_admin.run.aibench_run_inference_redisai_vision.aibench_run_inference_redisai_vision import (
    extract_aibench_extra_links,
)
from redisbench_admin.run.ftsb.ftsb import extract_ftsb_extra_links
from redisbench_admin.run.redis_benchmark.redis_benchmark import (
    redis_benchmark_ensure_min_version_remote,
    redisbenchmark_go_link,
)
from redisbench_admin.run.tsbs_run_queries_redistimeseries.tsbs_run_queries_redistimeseries import (
    extract_tsbs_extra_links,
)
from redisbench_admin.run_remote.consts import private_key
from redisbench_admin.utils.benchmark_config import results_dict_kpi_check
from redisbench_admin.utils.redisgraph_benchmark_go import (
    setup_remote_benchmark_tool_redisgraph_benchmark_go,
    setup_remote_benchmark_tool_ycsb_redisearch,
)
from redisbench_admin.utils.remote import (
    execute_remote_commands,
    extract_redisgraph_version_from_resultdict,
)
from redisbench_admin.utils.results import post_process_benchmark_results


def absoluteFilePaths(directory):
    for dirpath, _, filenames in os.walk(directory):
        for f in filenames:
            yield os.path.abspath(os.path.join(dirpath, f))


def benchmark_tools_sanity_check(args, benchmark_tool):
    if benchmark_tool is not None:
        logging.info("Detected benchmark config tool {}".format(benchmark_tool))
    else:
        raise Exception(
            "Unable to detect benchmark tool within 'clientconfig' section. Aborting..."
        )
    if benchmark_tool not in args.allowed_tools.split(","):
        raise Exception(
            "Benchmark tool {} not in the allowed tools list [{}]. Aborting...".format(
                benchmark_tool, args.allowed_tools
            )
        )


def remote_tool_pre_bench_step(
    benchmark_config,
    benchmark_min_tool_version,
    benchmark_min_tool_version_major,
    benchmark_min_tool_version_minor,
    benchmark_min_tool_version_patch,
    benchmark_tool,
    client_public_ip,
    username,
):
    logging.info("Settting up remote tool {} requirements".format(benchmark_tool))
    if benchmark_tool == "redisgraph-benchmark-go":
        setup_remote_benchmark_tool_redisgraph_benchmark_go(
            client_public_ip,
            username,
            private_key,
            redisbenchmark_go_link,
        )
    if "ycsb" in benchmark_tool:
        setup_remote_benchmark_tool_ycsb_redisearch(
            client_public_ip,
            username,
            private_key,
        )

    if "ftsb_" in benchmark_tool:
        (
            queries_file_link,
            remote_tool_link,
            tool_link,
        ) = extract_ftsb_extra_links(benchmark_config, benchmark_tool)
        logging.info(
            "FTSB Extracted:\nremote tool input: {}\nremote tool link: {}\ntool path: {}".format(
                queries_file_link, remote_tool_link, tool_link
            )
        )

        setup_remote_benchmark_tool_requirements_ftsb(
            client_public_ip,
            username,
            private_key,
            tool_link,
            queries_file_link,
            remote_tool_link,
        )

    if "tsbs_" in benchmark_tool:
        (
            queries_file_link,
            remote_tool_link,
            tool_link,
        ) = extract_tsbs_extra_links(benchmark_config, benchmark_tool)

        setup_remote_benchmark_tool_requirements_tsbs(
            client_public_ip,
            username,
            private_key,
            tool_link,
            queries_file_link,
            remote_tool_link,
        )
    if "aibench_" in benchmark_tool:
        (
            queries_file_link,
            remote_tool_link,
            tool_link,
        ) = extract_aibench_extra_links(benchmark_config, benchmark_tool)

        setup_remote_benchmark_tool_requirements_tsbs(
            client_public_ip,
            username,
            private_key,
            tool_link,
            queries_file_link,
            remote_tool_link,
        )
    if benchmark_min_tool_version is not None and benchmark_tool == "redis-benchmark":
        redis_benchmark_ensure_min_version_remote(
            benchmark_tool,
            benchmark_min_tool_version,
            benchmark_min_tool_version_major,
            benchmark_min_tool_version_minor,
            benchmark_min_tool_version_patch,
            client_public_ip,
            username,
            private_key,
        )
    logging.info("Finished up remote tool {} requirements".format(benchmark_tool))


def setup_remote_benchmark_tool_requirements_ftsb(
    client_public_ip,
    username,
    private_key,
    tool_link,
    queries_file_link,
    remote_tool_link,
    remote_input_file="/tmp/input.data",
):
    commands = [
        "wget {} -q -O {}".format(tool_link, remote_tool_link),
        "wget {} -q -O {}".format(queries_file_link, remote_input_file),
        "chmod 755 {}".format(remote_tool_link),
    ]
    execute_remote_commands(client_public_ip, username, private_key, commands)


def setup_remote_benchmark_tool_requirements_tsbs(
    client_public_ip,
    username,
    private_key,
    tool_link,
    queries_file_link,
    remote_tool_link,
    remote_input_file="/tmp/input.data",
):
    commands = [
        "wget {} -q -O {}".format(tool_link, remote_tool_link),
        "wget {} -q -O {}".format(queries_file_link, remote_input_file),
        "chmod 755 {}".format(remote_tool_link),
    ]
    execute_remote_commands(client_public_ip, username, private_key, commands)


def extract_artifact_version_remote(
    server_public_ip, server_public_port, username, private_key
):
    commands = [
        "redis-cli -h {} -p {} info modules".format("localhost", server_public_port),
    ]
    res = execute_remote_commands(server_public_ip, username, private_key, commands)
    recv_exit_status, stdout, stderr = res[0]
    print(stdout)
    module_name, version = extract_module_semver_from_info_modules_cmd(stdout)
    return module_name, version


def extract_module_semver_from_info_modules_cmd(stdout):
    versions = []
    module_names = []
    if type(stdout) == bytes:
        stdout = stdout.decode()
    if type(stdout) == str:
        info_modules_output = stdout.split("\n")[1:]
    else:
        info_modules_output = stdout[1:]
    for module_detail_line in info_modules_output:
        detail_arr = module_detail_line.split(",")
        if len(detail_arr) > 1:
            module_name = detail_arr[0].split("=")
            module_name = module_name[1]
            version = detail_arr[1].split("=")[1]
            logging.info(
                "Detected artifact={}, semver={}.".format(module_name, version)
            )
            module_names.append(module_name)
            versions.append(version)
    return module_names, versions


def post_process_remote_run(
    artifact_version,
    benchmark_config,
    benchmark_tool,
    local_benchmark_output_filename,
    return_code,
    start_time_ms,
    start_time_str,
    stdout,
    tmp,
    result_csv_filename="result.csv",
):
    if benchmark_tool == "redis-benchmark":
        local_benchmark_output_filename = tmp
        with open(result_csv_filename, "r") as txt_file:
            stdout = txt_file.read()
    if benchmark_tool == "redis-benchmark" or benchmark_tool == "ycsb":
        post_process_benchmark_results(
            benchmark_tool,
            local_benchmark_output_filename,
            start_time_ms,
            start_time_str,
            stdout,
        )
    with open(local_benchmark_output_filename, "r") as json_file:
        results_dict = json.load(json_file)
    # check KPIs
    return_code = results_dict_kpi_check(benchmark_config, results_dict, return_code)
    # if the benchmark tool is redisgraph-benchmark-go and
    # we still dont have the artifact semver we can extract it from the results dict
    if benchmark_tool == "redisgraph-benchmark-go" and artifact_version is None:
        artifact_version = extract_redisgraph_version_from_resultdict(results_dict)
    if artifact_version is None:
        artifact_version = "N/A"
    return artifact_version, local_benchmark_output_filename, results_dict, return_code
