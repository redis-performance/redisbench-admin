#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import csv
import datetime
import datetime as dt
import logging
import os
import time
import redis

from redisbench_admin.run.aibench_run_inference_redisai_vision.aibench_run_inference_redisai_vision import (
    prepare_aibench_benchmark_command,
)
from redisbench_admin.run.ftsb.ftsb import prepare_ftsb_benchmark_command
from redisbench_admin.run.redis_benchmark.redis_benchmark import (
    prepare_redis_benchmark_command,
)
from redisbench_admin.run.redisgraph_benchmark_go.redisgraph_benchmark_go import (
    prepare_redisgraph_benchmark_go_command,
)
from redisbench_admin.run.tsbs_run_queries_redistimeseries.tsbs_run_queries_redistimeseries import (
    prepare_tsbs_benchmark_command,
)
from redisbench_admin.run.ycsb.ycsb import prepare_ycsb_benchmark_command
from redisbench_admin.run_remote.remote_helpers import (
    extract_module_semver_from_info_modules_cmd,
)
from redisbench_admin.utils.benchmark_config import (
    parse_exporter_timemetric,
    parse_exporter_metrics_definition,
    parse_exporter_timemetric_definition,
    check_required_modules,
)
from redisbench_admin.utils.remote import (
    execute_remote_commands,
    fetch_file_from_remote_setup,
    extract_perversion_timeseries_from_results,
    push_data_to_redistimeseries,
    extract_perbranch_timeseries_from_results,
)

BENCHMARK_REPETITIONS = int(os.getenv("BENCHMARK_REPETITIONS", 1))


def prepare_benchmark_parameters(
    benchmark_config,
    benchmark_tool,
    server_plaintext_port,
    server_private_ip,
    remote_results_file,
    isremote=False,
    current_workdir=None,
    cluster_api_enabled=False,
):
    command_arr = None
    command_str = None
    for entry in benchmark_config["clientconfig"]:
        if "parameters" in entry:
            if "redis-benchmark" in benchmark_tool:
                command_arr, command_str = prepare_redis_benchmark_command(
                    benchmark_tool, server_private_ip, server_plaintext_port, entry
                )
                if isremote is True:
                    redirect_file = "> {}".format(remote_results_file)
                    command_arr.append(redirect_file)
                    command_str = command_str + " " + redirect_file

            if "redisgraph-benchmark-go" in benchmark_tool:
                if isremote is True:
                    benchmark_tool = "/tmp/redisgraph-benchmark-go"
                command_arr, command_str = prepare_redisgraph_benchmark_go_command(
                    benchmark_tool,
                    server_private_ip,
                    server_plaintext_port,
                    entry,
                    remote_results_file,
                    isremote,
                )

            if "ycsb" in benchmark_tool:
                if isremote is True:
                    benchmark_tool = (
                        "/tmp/ycsb-redisearch-binding-0.18.0-SNAPSHOT/bin/ycsb"
                    )
                    current_workdir = "/tmp/ycsb-redisearch-binding-0.18.0-SNAPSHOT"
                command_arr, command_str = prepare_ycsb_benchmark_command(
                    benchmark_tool,
                    server_private_ip,
                    server_plaintext_port,
                    entry,
                    current_workdir,
                )
            if "tsbs_" in benchmark_tool:
                input_data_file = None
                if isremote is True:
                    benchmark_tool = "/tmp/{}".format(benchmark_tool)
                    input_data_file = "/tmp/input.data"
                (command_arr, command_str,) = prepare_tsbs_benchmark_command(
                    benchmark_tool,
                    server_private_ip,
                    server_plaintext_port,
                    entry,
                    current_workdir,
                    remote_results_file,
                    input_data_file,
                    isremote,
                    cluster_api_enabled,
                )
            if "ftsb_" in benchmark_tool:
                input_data_file = None
                if isremote is True:
                    benchmark_tool = "/tmp/{}".format(benchmark_tool)
                    input_data_file = "/tmp/input.data"
                (command_arr, command_str,) = prepare_ftsb_benchmark_command(
                    benchmark_tool,
                    server_private_ip,
                    server_plaintext_port,
                    entry,
                    current_workdir,
                    remote_results_file,
                    input_data_file,
                    isremote,
                    cluster_api_enabled,
                )
            if "aibench_" in benchmark_tool:
                input_data_file = None
                if isremote is True:
                    benchmark_tool = "/tmp/{}".format(benchmark_tool)
                    input_data_file = "/tmp/input.data"
                (command_arr, command_str,) = prepare_aibench_benchmark_command(
                    benchmark_tool,
                    server_private_ip,
                    server_plaintext_port,
                    entry,
                    current_workdir,
                    remote_results_file,
                    input_data_file,
                    isremote,
                )
    printed_command_str = command_str
    printed_command_arr = command_arr
    if len(command_str) > 200:
        printed_command_str = command_str[:200] + "... (trimmed output) ..."
        printed_command_arr = printed_command_arr[:1] + ["(...) trimmed output...."]
    logging.info(
        "Running the benchmark with the following parameters:\n\tArgs array: {}\n\tArgs str: {}".format(
            printed_command_arr, printed_command_str
        )
    )
    return command_arr, command_str


def run_remote_benchmark(
    client_public_ip,
    username,
    private_key,
    remote_results_file,
    local_results_file,
    command,
):
    remote_run_result = False
    res = execute_remote_commands(client_public_ip, username, private_key, [command])
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


def common_exporter_logic(
    deployment_type,
    exporter_timemetric_path,
    metrics,
    results_dict,
    rts,
    test_name,
    tf_github_branch,
    tf_github_org,
    tf_github_repo,
    tf_triggering_env,
    artifact_version="N/A",
):
    per_version_time_series_dict = None
    per_branch_time_series_dict = None

    if exporter_timemetric_path is not None and len(metrics) > 0:
        # extract timestamp
        datapoints_timestamp = parse_exporter_timemetric(
            exporter_timemetric_path, results_dict
        )

        # extract per branch datapoints
        (
            ok,
            per_version_time_series_dict,
        ) = extract_perversion_timeseries_from_results(
            datapoints_timestamp,
            metrics,
            results_dict,
            artifact_version,
            tf_github_org,
            tf_github_repo,
            deployment_type,
            test_name,
            tf_triggering_env,
        )
        if ok:
            # push per-version data
            push_data_to_redistimeseries(rts, per_version_time_series_dict)
        if tf_github_branch is not None and tf_github_branch != "":
            # extract per branch datapoints
            ok, per_branch_time_series_dict = extract_perbranch_timeseries_from_results(
                datapoints_timestamp,
                metrics,
                results_dict,
                str(tf_github_branch),
                tf_github_org,
                tf_github_repo,
                deployment_type,
                test_name,
                tf_triggering_env,
            )
            if ok:
                # push per-branch data
                push_data_to_redistimeseries(rts, per_branch_time_series_dict)
        else:
            logging.warning(
                "Requested to push data to RedisTimeSeries but no git"
                " branch definition was found. git branch value {}".format(
                    tf_github_branch
                )
            )
    else:
        logging.error(
            "Requested to push data to RedisTimeSeries but "
            'no exporter definition was found. Missing "exporter" config.'
        )
    return per_version_time_series_dict, per_branch_time_series_dict


def get_start_time_vars(start_time=None):
    if start_time is None:
        start_time = dt.datetime.utcnow()
    start_time_ms = int((start_time - dt.datetime(1970, 1, 1)).total_seconds() * 1000)
    start_time_str = start_time.strftime("%Y-%m-%d-%H-%M-%S")
    return start_time, start_time_ms, start_time_str


def execute_init_commands(benchmark_config, r, dbconfig_keyname="dbconfig"):
    cmds = None
    if dbconfig_keyname in benchmark_config:
        for k in benchmark_config[dbconfig_keyname]:
            if "init_commands" in k:
                cmds = k["init_commands"]
    if cmds is not None:
        for cmd in cmds:
            is_array = False
            if '"' in cmd:
                cols = []
                for lines in csv.reader(
                    cmd,
                    quotechar='"',
                    delimiter=" ",
                    quoting=csv.QUOTE_ALL,
                    skipinitialspace=True,
                ):
                    if lines[0] != " " and len(lines[0]) > 0:
                        cols.append(lines[0])
                cmd = cols
                is_array = True
            try:
                logging.info("Sending init command: {}".format(cmd))
                if is_array:
                    stdout = r.execute_command(*cmd)
                else:
                    stdout = r.execute_command(cmd)
                logging.info("Command reply: {}".format(stdout))
            except redis.connection.ConnectionError as e:
                logging.error(
                    "Error establishing connection to Redis. Message: {}".format(
                        e.__str__()
                    )
                )


def extract_test_feasible_setups(
    benchmark_config, param, default_specs, backwards_compatible=True
):
    feasible_setups_map = {}
    if param in benchmark_config:
        feasible_setups_list = benchmark_config[param]
        for setup_name in feasible_setups_list:
            feasible_setups_map[setup_name] = {}
            if "setups" in default_specs:
                for setup in default_specs["setups"]:
                    if setup_name == setup["name"]:
                        feasible_setups_map[setup_name] = setup
    if len(feasible_setups_map.keys()) == 0 and backwards_compatible:
        feasible_setups_map["oss-standalone"] = {
            "name": "oss-standalone",
            "type": "oss-standalone",
            "redis_topology": {"primaries": 1, "replicas": 0},
            "resources": {"requests": {"cpu": "1000m"}, "limits": {"cpu": "2000m"}},
        }
        logging.info(
            "Using a backwards compatible 'oss-standalone' setup, with settings: {}".format(
                feasible_setups_map["oss-standalone"]
            )
        )

    return feasible_setups_map


def get_setup_type_and_primaries_count(setup_settings):
    setup_type = setup_settings["type"]
    shard_count = setup_settings["redis_topology"]["primaries"]
    return setup_type, shard_count


def merge_default_and_config_metrics(
    benchmark_config, default_metrics, exporter_timemetric_path
):
    metrics = default_metrics
    if "exporter" in benchmark_config:
        extra_metrics = parse_exporter_metrics_definition(benchmark_config["exporter"])
        metrics.extend(extra_metrics)
        extra_timemetric_path = parse_exporter_timemetric_definition(
            benchmark_config["exporter"]
        )
        if extra_timemetric_path is not None:
            exporter_timemetric_path = extra_timemetric_path
    return exporter_timemetric_path, metrics


def run_redis_pre_steps(benchmark_config, r, required_modules):
    stdout = r.execute_command("info modules")
    (
        module_names,
        artifact_versions,
    ) = extract_module_semver_from_info_modules_cmd(stdout)
    check_required_modules(module_names, required_modules)
    # run initialization commands before benchmark starts
    logging.info("Running initialization commands before benchmark starts.")
    execute_init_commands_start_time = datetime.datetime.now()
    execute_init_commands(benchmark_config, r)
    execute_init_commands_duration_seconds = (
        datetime.datetime.now() - execute_init_commands_start_time
    ).seconds
    logging.info(
        "Running initialization commands took {} secs.".format(
            execute_init_commands_duration_seconds
        )
    )
    if "search" in module_names:
        logging.info(
            "Given redisearch was detected, checking for any index that is still indexing."
        )
        loading_indices = r.execute_command("ft._list")
        logging.info("Detected {} indices.".format(len(loading_indices)))
        while len(loading_indices) > 0:
            logging.info(
                "There are still {} indices loading. {}".format(
                    len(loading_indices), loading_indices
                )
            )
            for index_pos, fts_indexname in enumerate(loading_indices, start=0):
                if type(fts_indexname) == bytes:
                    fts_indexname = fts_indexname.decode()
                ft_info = r.execute_command("ft.info {}".format(fts_indexname))
                is_indexing = None
                percent_indexed = "0.0"
                for arraypos, arrayval in enumerate(ft_info, start=0):
                    if arrayval == b"percent_indexed" or arrayval == "percent_indexed":
                        percent_indexed = ft_info[arraypos + 1]
                    if arrayval == b"indexing" or arrayval == "indexing":
                        is_indexing = ft_info[arraypos + 1]

                logging.info(
                    "indexing={} ; percent_indexed={}.".format(
                        is_indexing, percent_indexed
                    )
                )
                if is_indexing == "0" or is_indexing == b"0" or is_indexing == 0:
                    loading_indices.pop(index_pos)

            time.sleep(5)
        logging.info("Loaded all secondary indices.")

    return artifact_versions[0]


def dso_check(dso, local_module_file):
    if dso is None:
        logging.warning("No dso specified for perf analysis {}".format(dso))
        if local_module_file is not None:

            if type(local_module_file) == str:
                dso = local_module_file
                logging.warning(
                    "Using provided module = {} to specify dso".format(
                        local_module_file
                    )
                )
            if type(local_module_file) == list:
                dso = local_module_file[0]
                logging.warning(
                    "Using first module = {} to specify dso".format(
                        local_module_file[0]
                    )
                )
    return dso
