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
from pytablewriter import MarkdownTableWriter

from redisbench_admin.run.aibench_run_inference_redisai_vision.aibench_run_inference_redisai_vision import (
    prepare_aibench_benchmark_command,
)
from redisbench_admin.run.ftsb.ftsb import prepare_ftsb_benchmark_command
from redisbench_admin.run.memtier_benchmark.memtier_benchmark import (
    prepare_memtier_benchmark_command,
)
from redisbench_admin.run.metrics import extract_results_table
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
    extract_perversion_timeseries_from_results,
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
    config_key="clientconfig",
):
    command_arr = None
    command_str = None
    # v0.1 to 0.3 spec
    if type(benchmark_config[config_key]) == list:
        for entry in benchmark_config[config_key]:
            if "parameters" in entry:
                command_arr, command_str = prepare_benchmark_parameters_specif_tooling(
                    benchmark_tool,
                    cluster_api_enabled,
                    command_arr,
                    command_str,
                    current_workdir,
                    entry,
                    isremote,
                    remote_results_file,
                    server_plaintext_port,
                    server_private_ip,
                )
    # v0.4 spec
    elif type(benchmark_config[config_key]) == dict:
        entry = benchmark_config[config_key]
        command_arr, command_str = prepare_benchmark_parameters_specif_tooling(
            benchmark_tool,
            cluster_api_enabled,
            command_arr,
            command_str,
            current_workdir,
            entry,
            isremote,
            remote_results_file,
            server_plaintext_port,
            server_private_ip,
        )
    printed_command_str = command_str
    printed_command_arr = command_arr
    if len(command_str) > 500:
        printed_command_str = command_str[:500] + "... (trimmed output) ..."
        printed_command_arr = printed_command_arr[:1] + ["(...) trimmed output...."]
    logging.info(
        "Running the benchmark with the following parameters:\n\tArgs array: {}\n\tArgs str: {}".format(
            printed_command_arr, printed_command_str
        )
    )
    return command_arr, command_str


def prepare_benchmark_parameters_specif_tooling(
    benchmark_tool,
    cluster_api_enabled,
    command_arr,
    command_str,
    current_workdir,
    entry,
    isremote,
    remote_results_file,
    server_plaintext_port,
    server_private_ip,
):
    if "redis-benchmark" in benchmark_tool:
        command_arr, command_str = prepare_redis_benchmark_command(
            benchmark_tool,
            server_private_ip,
            server_plaintext_port,
            entry,
            cluster_api_enabled,
            current_workdir,
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
            benchmark_tool = "/tmp/ycsb/bin/ycsb"
            current_workdir = "/tmp/ycsb"
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
    if "memtier_benchmark" in benchmark_tool:
        (command_arr, command_str,) = prepare_memtier_benchmark_command(
            benchmark_tool,
            server_private_ip,
            server_plaintext_port,
            entry,
            cluster_api_enabled,
            remote_results_file,
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
    return command_arr, command_str


def common_exporter_logic(
    deployment_name,
    deployment_type,
    exporter_timemetric_path,
    metrics,
    results_dict,
    test_name,
    tf_github_branch,
    tf_github_org,
    tf_github_repo,
    tf_triggering_env,
    artifact_version="N/A",
    metadata_tags={},
    build_variant_name=None,
    running_platform=None,
    datapoints_timestamp=None,
):
    per_version_time_series_dict = {}
    per_branch_time_series_dict = {}
    testcase_metric_context_paths = []
    version_target_tables = None
    branch_target_tables = None
    used_ts = datapoints_timestamp

    if exporter_timemetric_path is not None and used_ts is None:
        # extract timestamp
        used_ts = parse_exporter_timemetric(exporter_timemetric_path, results_dict)

    if used_ts is None:
        used_ts = int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000.0)
        logging.warning(
            "Error while trying to parse datapoints timestamp. Using current system timestamp Error: {}".format(
                used_ts
            )
        )
    assert used_ts is not None
    if (
        artifact_version is not None
        and artifact_version != ""
        and artifact_version != "N/A"
    ):
        # extract per-version datapoints
        (
            _,
            per_version_time_series_dict,
            version_target_tables,
        ) = extract_perversion_timeseries_from_results(
            used_ts,
            metrics,
            results_dict,
            artifact_version,
            tf_github_org,
            tf_github_repo,
            deployment_name,
            deployment_type,
            test_name,
            tf_triggering_env,
            metadata_tags,
            build_variant_name,
            running_platform,
            testcase_metric_context_paths,
        )
    if tf_github_branch is not None and tf_github_branch != "":
        # extract per branch datapoints
        (
            _,
            per_branch_time_series_dict,
            branch_target_tables,
        ) = extract_perbranch_timeseries_from_results(
            used_ts,
            metrics,
            results_dict,
            str(tf_github_branch),
            tf_github_org,
            tf_github_repo,
            deployment_name,
            deployment_type,
            test_name,
            tf_triggering_env,
            metadata_tags,
            build_variant_name,
            running_platform,
            testcase_metric_context_paths,
        )
    else:
        logging.error(
            "Requested to push data to RedisTimeSeries but "
            'no exporter definition was found. Missing "exporter" config.'
        )
    return (
        per_version_time_series_dict,
        per_branch_time_series_dict,
        testcase_metric_context_paths,
        version_target_tables,
        branch_target_tables,
    )


def get_start_time_vars(start_time=None):
    if start_time is None:
        start_time = dt.datetime.utcnow()
    start_time_ms = int((start_time - dt.datetime(1970, 1, 1)).total_seconds() * 1000)
    start_time_str = start_time.strftime("%Y-%m-%d-%H-%M-%S")
    return start_time, start_time_ms, start_time_str


def check_dbconfig_tool_requirement(benchmark_config, dbconfig_keyname="dbconfig"):
    required = False
    if dbconfig_keyname in benchmark_config:
        for k in benchmark_config[dbconfig_keyname]:
            if "tool" in k:
                required = True
    return required


def check_dbconfig_keyspacelen_requirement(
    benchmark_config, dbconfig_keyname="dbconfig"
):
    required = False
    keyspacelen = None
    if dbconfig_keyname in benchmark_config:
        for k in benchmark_config[dbconfig_keyname]:
            if "check" in k:
                if "keyspacelen" in k["check"]:
                    required = True
                    keyspacelen = int(k["check"]["keyspacelen"])
    return required, keyspacelen


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
            if default_specs is not None:
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
    setup_name = setup_settings["name"]
    shard_count = setup_settings["redis_topology"]["primaries"]
    return setup_name, setup_type, shard_count


def merge_default_and_config_metrics(
    benchmark_config, default_metrics, exporter_timemetric_path
):
    if default_metrics is None:
        default_metrics = []
    metrics = default_metrics
    if benchmark_config is not None:
        if "exporter" in benchmark_config:
            extra_metrics = parse_exporter_metrics_definition(
                benchmark_config["exporter"]
            )
            metrics.extend(extra_metrics)
            extra_timemetric_path = parse_exporter_timemetric_definition(
                benchmark_config["exporter"]
            )
            if extra_timemetric_path is not None:
                exporter_timemetric_path = extra_timemetric_path
    return exporter_timemetric_path, metrics


def run_redis_pre_steps(benchmark_config, r, required_modules):
    # In case we have modules we use it's artifact version
    # otherwise we use redis version as artifact version
    version = "N/A"
    if required_modules is not None and len(required_modules) > 0:
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
                        if (
                            arrayval == b"percent_indexed"
                            or arrayval == "percent_indexed"
                        ):
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

        version = artifact_versions[0]
    else:
        version = r.info("server")["redis_version"]

    return version


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


def dbconfig_keyspacelen_check(benchmark_config, redis_conns):
    result = True
    (
        requires_keyspacelen_check,
        keyspacelen,
    ) = check_dbconfig_keyspacelen_requirement(benchmark_config)
    if requires_keyspacelen_check:
        result = False
        logging.info(
            "Ensuring keyspace length requirement = {} is met.".format(keyspacelen)
        )
        total_keys = 0
        for shard_conn in redis_conns:
            keyspace_dict = shard_conn.info("keyspace")
            for _, dbdict in keyspace_dict.items():
                shard_keys = dbdict["keys"]
                total_keys += shard_keys

        if total_keys == keyspacelen:
            logging.info(
                "The total numbers of keys in setup matches the expected spec: {}=={}".format(
                    keyspacelen, total_keys
                )
            )
            result = True
        else:
            logging.error(
                "The total numbers of keys in setup does not match the expected spec: {}!={}. Aborting...".format(
                    keyspacelen, total_keys
                )
            )
            raise Exception(
                "The total numbers of keys in setup does not match the expected spec: {}!={}. Aborting...".format(
                    keyspacelen, total_keys
                )
            )
    return result


def common_properties_log(
    tf_bin_path,
    tf_github_actor,
    tf_github_branch,
    tf_github_org,
    tf_github_repo,
    tf_github_sha,
    tf_setup_name_sufix,
    tf_triggering_env,
    private_key,
):
    logging.info("Using the following vars on deployment:")
    logging.info("\tPrivate key path: {}".format(private_key))
    logging.info("\tterraform bin path: {}".format(tf_bin_path))
    logging.info("\tgithub_actor: {}".format(tf_github_actor))
    logging.info("\tgithub_org: {}".format(tf_github_org))
    logging.info("\tgithub_repo: {}".format(tf_github_repo))
    logging.info("\tgithub_branch: {}".format(tf_github_branch))
    logging.info("\tgithub_sha: {}".format(tf_github_sha))
    logging.info("\ttriggering env: {}".format(tf_triggering_env))
    logging.info("\tsetup_name sufix: {}".format(tf_setup_name_sufix))


def print_results_table_stdout(
    benchmark_config, default_metrics, results_dict, setup_name, test_name
):
    # check which metrics to extract
    (_, metrics,) = merge_default_and_config_metrics(
        benchmark_config,
        default_metrics,
        None,
    )
    table_name = "Results for {} test-case on {} topology".format(test_name, setup_name)
    results_matrix_headers = [
        "Metric JSON Path",
        "Metric Value",
    ]
    results_matrix = extract_results_table(metrics, results_dict)
    results_matrix = [[x[0], "{:.3f}".format(x[3])] for x in results_matrix]
    writer = MarkdownTableWriter(
        table_name=table_name,
        headers=results_matrix_headers,
        value_matrix=results_matrix,
    )
    writer.write_table()
