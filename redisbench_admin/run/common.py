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
from redisbench_admin.run.ann.ann import (
    prepare_ann_benchmark_command,
    ANN_MULTIRUN_PATH,
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
from redisbench_admin.run.ycsb.ycsb import (
    prepare_ycsb_benchmark_command,
    prepare_go_ycsb_benchmark_command,
)
from redisbench_admin.run_remote.args import OVERRIDE_MODULES
from redisbench_admin.run_remote.remote_helpers import (
    extract_module_semver_from_info_modules_cmd,
)
from redisbench_admin.utils.benchmark_config import (
    parse_exporter_timemetric,
    parse_exporter_metrics_definition,
    parse_exporter_timemetric_definition,
    check_required_modules,
)
from redisbench_admin.utils.redisgraph_benchmark_go import (
    get_redisbench_admin_remote_path,
)
from redisbench_admin.utils.remote import (
    extract_perversion_timeseries_from_results,
    extract_perbranch_timeseries_from_results,
    extract_perhash_timeseries_from_results,
    ARCH_X86,
)
from redisbench_admin.run.asm import execute_asm_commands

BENCHMARK_REPETITIONS = int(os.getenv("BENCHMARK_REPETITIONS", 1))
# circleci related info
CIRCLE_BUILD_URL = os.getenv("CIRCLE_BUILD_URL", None)
CIRCLE_JOB = os.getenv("CIRCLE_JOB", None)
WH_TOKEN = os.getenv("PERFORMANCE_WH_TOKEN", None)
PERFORMANCE_GH_TOKEN = os.getenv("PERFORMANCE_GH_TOKEN", None)
REDIS_BINARY = os.getenv("REDIS_BINARY", "redis-server")


def extract_input_file_url_from_parameters(entry, benchmark_tool):
    """
    Extract the input file URL from the parameters entry.
    Different tools use different parameter names:
    - ftsb_*: uses "input" parameter
    - tsbs_*, aibench_*: uses "file" parameter
    - memtier_benchmark: uses "monitor-input" in arguments string

    Args:
        entry: The benchmark config entry containing "parameters" or "arguments"
        benchmark_tool: The benchmark tool name

    Returns:
        The file URL if found, None otherwise
    """
    # Handle memtier_benchmark with monitor-input in arguments
    if "memtier_benchmark" in benchmark_tool:
        from redisbench_admin.run.memtier_benchmark.memtier_benchmark import (
            extract_monitor_input_from_arguments,
        )

        if "arguments" in entry:
            monitor_input = extract_monitor_input_from_arguments(entry["arguments"])
            if monitor_input and (
                monitor_input.startswith("http") or monitor_input.startswith("s3")
            ):
                return monitor_input
        return None

    if "parameters" not in entry:
        return None

    # Determine which parameter name to look for based on the tool
    if "ftsb_" in benchmark_tool:
        param_name = "input"
    elif "tsbs_" in benchmark_tool or "aibench_" in benchmark_tool:
        param_name = "file"
    else:
        return None

    parameters = entry["parameters"]

    # Handle v0.4 spec where parameters is a dict
    if isinstance(parameters, dict):
        return parameters.get(param_name)

    # Handle v0.1-0.3 spec where parameters is a list of dicts
    if isinstance(parameters, list):
        for param in parameters:
            if isinstance(param, dict) and param_name in param:
                return param[param_name]

    return None


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
    client_public_ip=None,
    username=None,
    private_key=None,
    client_ssh_port=None,
    redis_password=None,
    log_out_file=None,
):
    command_arr = None
    command_str = None
    # v0.1 to 0.3 spec
    if type(benchmark_config[config_key]) == list:
        for entry in benchmark_config[config_key]:
            if "parameters" in entry:
                # Extract input file URL from parameters
                input_file_url = extract_input_file_url_from_parameters(
                    entry, benchmark_tool
                )
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
                    client_public_ip,
                    username,
                    private_key,
                    client_ssh_port,
                    redis_password,
                    input_file_url,
                    log_out_file,
                )
    # v0.4 spec
    elif type(benchmark_config[config_key]) == dict:
        entry = benchmark_config[config_key]
        # Extract input file URL from parameters
        input_file_url = extract_input_file_url_from_parameters(entry, benchmark_tool)
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
            client_public_ip,
            username,
            private_key,
            client_ssh_port,
            redis_password,
            input_file_url,
            log_out_file,
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
    client_public_ip,
    username,
    private_key,
    client_ssh_port,
    redis_password=None,
    input_file_url=None,
    log_out_file=None,
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
    if "ycsb" in benchmark_tool and "go-ycsb" not in benchmark_tool:
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
    if "go-ycsb" in benchmark_tool:
        if isremote is True:
            benchmark_tool = "/tmp/{}".format(benchmark_tool)
        command_arr, command_str = prepare_go_ycsb_benchmark_command(
            benchmark_tool,
            server_private_ip,
            server_plaintext_port,
            entry,
            current_workdir,
            cluster_api_enabled,
            redis_password,
        )

    if "tsbs_" in benchmark_tool:
        input_data_file = None
        if isremote is True:
            from redisbench_admin.utils.utils import get_remote_input_file_from_url

            benchmark_tool = "/tmp/{}".format(benchmark_tool)
            input_data_file = get_remote_input_file_from_url(input_file_url)
        (
            command_arr,
            command_str,
        ) = prepare_tsbs_benchmark_command(
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
        # For remote execution, get the remote path for monitor-input file
        remote_monitor_file = None
        if isremote and input_file_url:
            from redisbench_admin.utils.utils import get_remote_input_file_from_url

            remote_monitor_file = get_remote_input_file_from_url(input_file_url)
        (
            command_arr,
            command_str,
        ) = prepare_memtier_benchmark_command(
            benchmark_tool,
            server_private_ip,
            server_plaintext_port,
            entry,
            cluster_api_enabled,
            remote_results_file,
            redis_password,
            local_temp_dir=current_workdir if current_workdir else "/tmp",
            is_remote=isremote,
            remote_monitor_file=remote_monitor_file,
        )
    if "ann" in benchmark_tool:
        ann_path = ANN_MULTIRUN_PATH
        if isremote is True:
            [recv_exit_status, stdout, stderr] = get_redisbench_admin_remote_path(
                client_public_ip, username, private_key, client_ssh_port
            )[0]
            ann_path = stdout[0].strip() + "/run/ann/pkg/multirun.py"
            logging.info("Remote ann-benchmark path: {}".format(ann_path))

        (
            command_arr,
            command_str,
        ) = prepare_ann_benchmark_command(
            server_private_ip,
            server_plaintext_port,
            cluster_api_enabled,
            entry,
            remote_results_file,
            current_workdir,
            ann_path,
        )
    if "ftsb_" in benchmark_tool:
        input_data_file = None
        if isremote is True:
            from redisbench_admin.utils.utils import get_remote_input_file_from_url

            benchmark_tool = "/tmp/{}".format(benchmark_tool)
            input_data_file = get_remote_input_file_from_url(input_file_url)
        (
            command_arr,
            command_str,
        ) = prepare_ftsb_benchmark_command(
            benchmark_tool,
            server_private_ip,
            server_plaintext_port,
            entry,
            current_workdir,
            remote_results_file,
            input_data_file,
            isremote,
            cluster_api_enabled,
            redis_password,
            log_out_file,
        )
    if "aibench_" in benchmark_tool:
        input_data_file = None
        if isremote is True:
            from redisbench_admin.utils.utils import get_remote_input_file_from_url

            benchmark_tool = "/tmp/{}".format(benchmark_tool)
            input_data_file = get_remote_input_file_from_url(input_file_url)
        (
            command_arr,
            command_str,
        ) = prepare_aibench_benchmark_command(
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
    tf_github_sha=None,
    arch=ARCH_X86,
):
    per_version_time_series_dict = {}
    per_branch_time_series_dict = {}
    per_hash_time_series_dict = {}
    testcase_metric_context_paths = []
    version_target_tables = None
    branch_target_tables = None
    hash_target_tables = None
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
            tf_github_sha=tf_github_sha,
            arch=arch,
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
            tf_github_sha=tf_github_sha,
            arch=arch,
        )
    else:
        logging.error(
            "Requested to push data to RedisTimeSeries but "
            'no exporter definition was found. Missing "exporter" config.'
        )
    if tf_github_sha is not None and tf_github_sha != "":
        (
            _,
            per_hash_time_series_dict,
            hash_target_tables,
        ) = extract_perhash_timeseries_from_results(
            used_ts,
            metrics,
            results_dict,
            str(tf_github_sha),
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
            arch=arch,
        )
    return (
        per_version_time_series_dict,
        per_branch_time_series_dict,
        testcase_metric_context_paths,
        version_target_tables,
        branch_target_tables,
        per_hash_time_series_dict,
        hash_target_tables,
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
        dbconfig = benchmark_config[dbconfig_keyname]
        # Handle both dict and list formats
        if isinstance(dbconfig, dict):
            # New format: dbconfig is a dict
            if "tool" in dbconfig:
                required = True
        elif isinstance(dbconfig, list):
            # Old format: dbconfig is a list of dicts
            for k in dbconfig:
                if isinstance(k, dict) and "tool" in k:
                    required = True
    return required


def check_dbconfig_keyspacelen_requirement(
    benchmark_config, dbconfig_keyname="dbconfig"
):
    required = False
    keyspacelen = None
    keyspacelen_min = None
    if dbconfig_keyname in benchmark_config:
        if type(benchmark_config[dbconfig_keyname]) == list:
            for k in benchmark_config[dbconfig_keyname]:
                # Handle both dict and non-dict entries in the list
                if isinstance(k, dict) and "check" in k:
                    if "keyspacelen" in k["check"]:
                        required = True
                        keyspacelen = int(k["check"]["keyspacelen"])
                    if "keyspacelen_min" in k["check"]:
                        required = True
                        keyspacelen_min = int(k["check"]["keyspacelen_min"])
        if type(benchmark_config[dbconfig_keyname]) == dict:
            if "check" in benchmark_config[dbconfig_keyname]:
                if "keyspacelen" in benchmark_config[dbconfig_keyname]["check"]:
                    required = True
                    keyspacelen = int(
                        benchmark_config[dbconfig_keyname]["check"]["keyspacelen"]
                    )
                if "keyspacelen_min" in benchmark_config[dbconfig_keyname]["check"]:
                    required = True
                    keyspacelen_min = int(
                        benchmark_config[dbconfig_keyname]["check"]["keyspacelen_min"]
                    )
    return required, keyspacelen, keyspacelen_min


def _execute_dbconfig_commands(
    benchmark_config,
    r,
    command_key,
    log_label,
    broadcast_ft_create,
    dbconfig_keyname="dbconfig",
):
    """Execute a list of Redis commands declared under dbconfig.<command_key>.

    Supports both dict-format (`dbconfig: {<command_key>: [...]}`) and the
    legacy list-format (`dbconfig: [- <command_key>: [...]]`). Each command
    may be a list (`["SET", "k", "v"]`) or a quoted-string form
    (`'"SET" "k" "v"'`).

    Returns the count of commands successfully sent. ConnectionError on a
    given command is logged and the loop continues.

    Parameters
    ----------
    command_key : str
        The dbconfig sub-key to read commands from ("init_commands" or
        "post_commands").
    log_label : str
        Human-readable label used in INFO log lines (e.g. "init command").
    broadcast_ft_create : bool
        If True, FT.CREATE is sent to all nodes via target_nodes="all" so it
        works on OSS Cluster. Init-commands set this; post-commands do not
        (preserved for backward compatibility).
    """
    cmds = None
    res = 0
    if dbconfig_keyname in benchmark_config:
        dbconfig = benchmark_config[dbconfig_keyname]
        # Handle both dict and list formats
        if isinstance(dbconfig, dict):
            if command_key in dbconfig:
                cmds = dbconfig[command_key]
        elif isinstance(dbconfig, list):
            for k in dbconfig:
                if isinstance(k, dict) and command_key in k:
                    cmds = k[command_key]
    if cmds is None:
        return res

    for cmd in cmds:
        is_array = False
        if type(cmd) == list:
            is_array = True
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
            logging.info("Sending {}: {}".format(log_label, cmd))
            stdout = ""
            verb = cmd[0] if is_array else cmd
            if broadcast_ft_create and "FT.CREATE" in verb:
                logging.info("Detected FT.CREATE to all nodes on OSS Cluster")
                try:
                    if is_array:
                        stdout = r.execute_command(*cmd, target_nodes="all")
                    else:
                        stdout = r.execute_command(cmd, target_nodes="all")
                except redis.exceptions.ResponseError:
                    pass
            else:
                if is_array:
                    stdout = r.execute_command(*cmd)
                else:
                    stdout = r.execute_command(cmd)
            res = res + 1
            logging.info("Command reply: {}".format(stdout))
        except redis.connection.ConnectionError as e:
            logging.error(
                "Error establishing connection to Redis. Message: {}".format(
                    e.__str__()
                )
            )

    return res


def execute_init_commands(benchmark_config, r, dbconfig_keyname="dbconfig"):
    return _execute_dbconfig_commands(
        benchmark_config,
        r,
        command_key="init_commands",
        log_label="init command",
        broadcast_ft_create=True,
        dbconfig_keyname=dbconfig_keyname,
    )


def execute_post_commands(benchmark_config, r, dbconfig_keyname="dbconfig"):
    return _execute_dbconfig_commands(
        benchmark_config,
        r,
        command_key="post_commands",
        log_label="post command",
        broadcast_ft_create=False,
        dbconfig_keyname=dbconfig_keyname,
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
                #  spec:
                #   setups:
                #   - name: oss-standalone
                #     type: oss-standalone
                #     redis_topology:
                #       primaries: 1
                #       replicas: 1
                #       placement: "sparse"
                #     resources:
                #       requests:
                #         cpus: "2"
                #         memory: "10g"
                #   - name: oss-standalone-threads-6
                #     type: oss-standalone
                #     redis_topology:
                #       primaries: 1
                #       replicas: 1
                #       placement: "sparse"
                #     resources:
                #       requests:
                #         cpus: "2"
                #         memory: "10g"
                #     dbconfig:
                #       module-configuration-parameters:
                #         redisearch:
                #           WORKERS: 6
                #           MIN_OPERATION_WORKERS: 6
                #         module-oss:
                #           WORKERS: 6
                #           MIN_OPERATION_WORKERS: 6
                if "setups" in default_specs:
                    for setup in default_specs["setups"]:
                        if setup_name == setup["name"]:
                            feasible_setups_map[setup_name] = setup
    if len(feasible_setups_map.keys()) == 0 and backwards_compatible:
        setup_name = "oss-standalone"
        setup_type = "oss-standalone"
        OVERRIDE_SETUP_TYPE = os.getenv("OVERRIDE_SETUP_TYPE", None)
        if OVERRIDE_SETUP_TYPE is not None:
            logging.info(
                f"Overriding SETUP_TYPE with {OVERRIDE_SETUP_TYPE} (original value was {setup_type})"
            )
            setup_type = OVERRIDE_SETUP_TYPE
        OVERRIDE_SETUP_NAME = os.getenv("OVERRIDE_SETUP_NAME", None)
        if OVERRIDE_SETUP_NAME is not None:
            logging.info(
                f"Overriding SETUP_NAME with {OVERRIDE_SETUP_NAME} (original value was {setup_name})"
            )
            setup_name = OVERRIDE_SETUP_NAME

        feasible_setups_map[setup_name] = {
            "name": setup_name,
            "type": setup_type,
            "redis_topology": {"primaries": 1, "replicas": 0},
            "resources": {"requests": {"cpu": "1000m"}, "limits": {"cpu": "2000m"}},
        }
        logging.info(
            "Using a backwards compatible 'oss-standalone' setup, with settings: {}".format(
                feasible_setups_map[setup_name]
            )
        )
    logging.info(
        f"There a total of {len(feasible_setups_map.keys())} setups. Setups: {feasible_setups_map}"
    )
    return feasible_setups_map


def get_setup_type_and_primaries_count(setup_settings):
    setup_type = setup_settings["type"]
    setup_name = setup_settings["name"]
    shard_count = setup_settings["redis_topology"]["primaries"]
    OVERRIDE_SETUP_TYPE = os.getenv("OVERRIDE_SETUP_TYPE", None)
    if OVERRIDE_SETUP_TYPE is not None:
        logging.info(
            f"Overriding SETUP_TYPE with {OVERRIDE_SETUP_TYPE} (original value was {setup_type})"
        )
        setup_type = OVERRIDE_SETUP_TYPE
    OVERRIDE_SETUP_NAME = os.getenv("OVERRIDE_SETUP_NAME", None)
    if OVERRIDE_SETUP_NAME is not None:
        logging.info(
            f"Overriding SETUP_NAME with {OVERRIDE_SETUP_NAME} (original value was {setup_name})"
        )
        setup_name = OVERRIDE_SETUP_NAME
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


def run_redis_pre_steps(benchmark_config, r, required_modules, run_wait_for=True):
    """Run the pre-benchmark db steps and return (artifact_version, measurements).

    `measurements` holds the timings of the dbconfig `wait_for` conditions, which
    the callers merge into the benchmark results dict so that the `exporter` yaml
    section can reference them.
    """
    # In case we have modules we use it's artifact version
    # otherwise we use redis version as artifact version
    version = "N/A"
    measurements = {}
    # run initialization commands before benchmark starts
    logging.info("Running initialization commands before benchmark starts.")
    execute_init_commands_start_time = datetime.datetime.now()
    execute_init_commands(benchmark_config, r)
    execute_asm_commands(benchmark_config, r)
    execute_init_commands_duration_seconds = (
        datetime.datetime.now() - execute_init_commands_start_time
    ).seconds
    logging.info(
        "Running initialization commands took {} secs.".format(
            execute_init_commands_duration_seconds
        )
    )
    # the wait_for conditions are evaluated right after the init commands and
    # before search_specific_init, otherwise the unconditional indexing barrier
    # below would drain the background indexing and every measurement would be 0
    wait_for_specs = extract_dbconfig_wait_for(benchmark_config)
    if run_wait_for:
        measurements = dbconfig_wait_for_conditions(wait_for_specs, r)
    stdout = r.execute_command("info modules")
    (
        module_names,
        artifact_versions,
    ) = extract_module_semver_from_info_modules_cmd(stdout)
    if OVERRIDE_MODULES is not None:
        module_names = OVERRIDE_MODULES.split(",")
    if "search" in module_names:
        if run_wait_for and wait_for_covers_field(wait_for_specs, "indexing"):
            logging.info(
                "Detected redisearch module. Skipping the indexing barrier given a"
                " dbconfig wait_for entry already waited on the indexing field."
            )
        else:
            logging.info(
                "Detected redisearch module. Ensuring all indices are indexed prior benchmark"
            )
            search_specific_init(r, module_names)
    if required_modules is not None and len(required_modules) > 0:
        check_required_modules(module_names, required_modules)

        version = artifact_versions[0]
    else:
        version = r.info("server")["redis_version"]

    return version, measurements


def run_redis_post_steps(benchmark_config, r):
    logging.info("Running post commands after benchmark completes.")
    execute_post_commands_start_time = datetime.datetime.now()
    execute_post_commands(benchmark_config, r)
    execute_post_commands_duration_seconds = (
        datetime.datetime.now() - execute_post_commands_start_time
    ).seconds
    logging.info(
        "Running post commands took {} secs.".format(
            execute_post_commands_duration_seconds
        )
    )


def search_specific_init(r, module_names):
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


WAIT_FOR_DEFAULT_POLL_INTERVAL_MS = int(
    os.getenv("WAIT_FOR_DEFAULT_POLL_INTERVAL_MS", 100)
)
WAIT_FOR_DEFAULT_TIMEOUT_SECS = int(os.getenv("WAIT_FOR_DEFAULT_TIMEOUT_SECS", 900))
WAIT_FOR_PROGRESS_LOG_INTERVAL_SECS = 5.0
WAIT_FOR_COMPARISONS = ["eq", "ne", "lt", "le", "gt", "ge"]


def extract_dbconfig_wait_for(benchmark_config, dbconfig_keyname="dbconfig"):
    """Extract the `wait_for` specs declared on the dbconfig section.

    Supports both the v0.4+ dict form and the legacy list-of-dicts form, the same
    way `check_dbconfig_keyspacelen_requirement` does.
    """
    specs = []
    if dbconfig_keyname not in benchmark_config:
        return specs
    dbconfig = benchmark_config[dbconfig_keyname]
    entries = dbconfig if type(dbconfig) == list else [dbconfig]
    for entry in entries:
        if type(entry) != dict or "wait_for" not in entry:
            continue
        wait_for = entry["wait_for"]
        if type(wait_for) == dict:
            wait_for = [wait_for]
        specs.extend(wait_for)
    return specs


def wait_for_covers_field(wait_for_specs, field):
    for spec in wait_for_specs:
        if spec.get("field", None) == field:
            return True
    return False


def decode_if_bytes(value):
    return value.decode() if type(value) == bytes else value


def flat_reply_to_dict(reply):
    """Convert a flat key/value array reply (FT.INFO, ...) into a dict.

    Keys are decoded so that the field names declared on the yaml match
    independently of the client's decode_responses setting. RESP3 map replies are
    already dicts and only need their keys decoded.
    """
    if type(reply) == dict:
        return {decode_if_bytes(k): v for k, v in reply.items()}
    reply_dict = {}
    for pos in range(0, len(reply) - 1, 2):
        reply_dict[decode_if_bytes(reply[pos])] = reply[pos + 1]
    return reply_dict


def wait_for_compare(actual, comparison, expected):
    """Compare a server-side reply field against the yaml declared expectation.

    Comparisons are numeric whenever both sides parse as a number -- FT.INFO
    replies `indexing` as an integer but a RESP2 client may hand it over as a
    byte string -- and fall back to a string comparison otherwise.
    """
    actual = decode_if_bytes(actual)
    expected = decode_if_bytes(expected)
    try:
        actual = float(actual)
        expected = float(expected)
    except (TypeError, ValueError):
        actual = str(actual)
        expected = str(expected)
        if comparison not in ["eq", "ne"]:
            raise Exception(
                "Comparison '{}' requires numeric values. Got actual={} expected={}".format(
                    comparison, actual, expected
                )
            )
    if comparison == "eq":
        return actual == expected
    if comparison == "ne":
        return actual != expected
    if comparison == "lt":
        return actual < expected
    if comparison == "le":
        return actual <= expected
    if comparison == "gt":
        return actual > expected
    if comparison == "ge":
        return actual >= expected
    raise Exception(
        "Unsupported wait_for comparison '{}'. Supported ones: {}".format(
            comparison, WAIT_FOR_COMPARISONS
        )
    )


def extract_wait_for_comparison(spec):
    comparisons = [c for c in WAIT_FOR_COMPARISONS if c in spec]
    if len(comparisons) != 1:
        raise Exception(
            "Each wait_for entry requires exactly one of {}. Got {} on spec {}".format(
                WAIT_FOR_COMPARISONS, comparisons, spec
            )
        )
    comparison = comparisons[0]
    return comparison, spec[comparison]


def wait_for_condition(spec, redis_conn):
    """Poll a server-side condition and return how long it took to be met.

    The clock starts on the first poll -- i.e. right after the dbconfig
    `init_commands` returned -- so for a `FT.CREATE` on an already loaded
    keyspace the measured duration is the background indexing wall clock time.
    Raises on timeout instead of recording a bogus duration.
    """
    name = spec.get("name", None)
    command = spec.get("command", None)
    field = spec.get("field", None)
    if name is None or command is None or field is None:
        raise Exception(
            "A wait_for entry requires the 'name', 'command' and 'field' properties. Got {}".format(
                spec
            )
        )
    comparison, expected = extract_wait_for_comparison(spec)
    poll_interval_secs = (
        float(spec.get("poll_interval_ms", WAIT_FOR_DEFAULT_POLL_INTERVAL_MS)) / 1000.0
    )
    timeout_secs = float(spec.get("timeout_secs", WAIT_FOR_DEFAULT_TIMEOUT_SECS))
    required_fields = spec.get("require", {})
    recorded_fields = spec.get("record_fields", [])

    logging.info(
        "Waiting for '{}' to be met: {} field '{}' {} {} (poll every {} ms, timeout {} secs).".format(
            name,
            command,
            field,
            comparison,
            expected,
            spec.get("poll_interval_ms", WAIT_FOR_DEFAULT_POLL_INTERVAL_MS),
            timeout_secs,
        )
    )
    start_time = time.time()
    last_progress_log = start_time
    reply_dict = {}
    polls = 0
    while True:
        reply_dict = flat_reply_to_dict(redis_conn.execute_command(command))
        polls += 1
        if field not in reply_dict:
            raise Exception(
                "wait_for '{}': field '{}' is not present on the reply of '{}'. Available fields: {}".format(
                    name, field, command, list(reply_dict.keys())
                )
            )
        elapsed = time.time() - start_time
        if wait_for_compare(reply_dict[field], comparison, expected):
            break
        if elapsed > timeout_secs:
            raise Exception(
                "wait_for '{}' timed out after {:.3f} secs ({} polls). Last observed {}={}, expected {} {}.".format(
                    name,
                    elapsed,
                    polls,
                    field,
                    decode_if_bytes(reply_dict[field]),
                    comparison,
                    expected,
                )
            )
        if time.time() - last_progress_log >= WAIT_FOR_PROGRESS_LOG_INTERVAL_SECS:
            last_progress_log = time.time()
            logging.info(
                "wait_for '{}' still pending after {:.1f} secs. {}={}. {}".format(
                    name,
                    elapsed,
                    field,
                    decode_if_bytes(reply_dict[field]),
                    {
                        k: decode_if_bytes(reply_dict.get(k, "n/a"))
                        for k in recorded_fields
                    },
                )
            )
        time.sleep(poll_interval_secs)

    duration_secs = time.time() - start_time
    logging.info(
        "🟢 wait_for '{}' met after {:.3f} secs ({} polls).".format(
            name, duration_secs, polls
        )
    )

    # A condition can also be met for the wrong reason -- a RediSearch background
    # scan aborted by OOM flips `indexing` back to 0 without having indexed
    # everything -- so validate the caller declared invariants before recording.
    for required_field, required_value in required_fields.items():
        if required_field not in reply_dict:
            raise Exception(
                "wait_for '{}': required field '{}' is not present on the reply of '{}'.".format(
                    name, required_field, command
                )
            )
        if not wait_for_compare(reply_dict[required_field], "eq", required_value):
            raise Exception(
                "wait_for '{}': condition was met but the required field '{}' is {} instead of {}. "
                "Refusing to record a partial result.".format(
                    name,
                    required_field,
                    decode_if_bytes(reply_dict[required_field]),
                    required_value,
                )
            )

    measurements = {
        "{}_secs".format(name): duration_secs,
        "{}_ms".format(name): duration_secs * 1000.0,
    }
    for recorded_field in recorded_fields:
        if recorded_field not in reply_dict:
            logging.warning(
                "wait_for '{}': record_fields entry '{}' is not present on the reply of '{}'. Skipping it.".format(
                    name, recorded_field, command
                )
            )
            continue
        try:
            measurements["{}_{}".format(name, recorded_field)] = float(
                decode_if_bytes(reply_dict[recorded_field])
            )
        except (TypeError, ValueError):
            logging.warning(
                "wait_for '{}': record_fields entry '{}' is not numeric. Skipping it.".format(
                    name, recorded_field
                )
            )
    return measurements


def dbconfig_wait_for_conditions(wait_for_specs, redis_conn):
    """Evaluate every dbconfig `wait_for` entry and return the measurements dict.

    On oss-cluster a single connection is enough: the coordinator aggregates
    FT.INFO's `indexing` as a sum across shards, so `indexing == 0` means every
    shard finished.
    """
    measurements = {}
    for spec in wait_for_specs:
        measurements.update(wait_for_condition(spec, redis_conn))
    if len(measurements) > 0:
        logging.info("wait_for measurements: {}".format(measurements))
    return measurements


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


def dbconfig_keyspacelen_check(
    benchmark_config, redis_conns, ignore_keyspace_errors=False, timeout=60
):
    start_time = time.time()
    (
        requires_keyspacelen_check,
        keyspacelen,
        keyspacelen_min,
    ) = check_dbconfig_keyspacelen_requirement(benchmark_config)

    if not requires_keyspacelen_check:
        return True

    # Build requirement description for logging
    if keyspacelen is not None:
        requirement_desc = f"exactly {keyspacelen}"
    else:
        requirement_desc = f"at least {keyspacelen_min}"

    attempt = 0
    while time.time() - start_time < timeout:
        logging.info(
            f"Ensuring keyspace length requirement ({requirement_desc}) is met. attempt #{attempt + 1}"
        )
        total_keys = 0
        for shard_conn in redis_conns:
            keyspace_dict = shard_conn.info("keyspace")
            for _, dbdict in keyspace_dict.items():
                total_keys += dbdict.get("keys", 0)

        # Check exact match if keyspacelen is specified
        if keyspacelen is not None and total_keys == keyspacelen:
            logging.info(
                f"🟢 Keyspace check PASSED: expected={keyspacelen}, got={total_keys}"
            )
            return True

        # Check minimum if keyspacelen_min is specified
        if keyspacelen_min is not None and total_keys >= keyspacelen_min:
            logging.info(
                f"🟢 Keyspace check PASSED: expected>={keyspacelen_min}, got={total_keys}"
            )
            return True

        # Build mismatch message
        if keyspacelen is not None:
            mismatch_msg = f"Keyspace length mismatch ({total_keys} != {keyspacelen})"
        else:
            mismatch_msg = (
                f"Keyspace length below minimum ({total_keys} < {keyspacelen_min})"
            )

        logging.warning(
            "{}. Retrying in {} seconds...".format(mismatch_msg, 2**attempt)
        )
        time.sleep(2**attempt)  # Exponential backoff
        attempt += 1

    # Build error message
    if keyspacelen is not None:
        error_msg = f"The total number of keys in setup does not match the expected spec: {keyspacelen} != {total_keys}. Aborting after {attempt + 1} tries..."
    else:
        error_msg = f"The total number of keys in setup is below the minimum: {total_keys} < {keyspacelen_min}. Aborting after {attempt + 1} tries..."

    logging.error(
        f"🔴 Keyspace check FAILED: expected {requirement_desc}, got={total_keys} (after {attempt + 1} tries)"
    )

    if not ignore_keyspace_errors:
        raise Exception(error_msg)

    return False


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
    benchmark_config,
    default_metrics,
    results_dict,
    setup_name,
    setup_type,
    test_name,
    cpu_usage=None,
    kv_overall={},
    metric_names=[],
):
    # check which metrics to extract
    (
        _,
        metrics,
    ) = merge_default_and_config_metrics(
        benchmark_config,
        default_metrics,
        None,
    )
    table_name = "Results for {} test-case on {} topology (type={})".format(
        test_name, setup_name, setup_type
    )
    results_matrix_headers = [
        "Metric JSON Path",
        "Metric Value",
    ]
    results_matrix = extract_results_table(metrics, results_dict)
    if cpu_usage is not None:
        results_matrix.append(["Total shards CPU usage %", "", "", cpu_usage])
    for metric_name in metric_names:
        if metric_name in kv_overall:
            metric_value = kv_overall[metric_name]
            results_matrix.append([f"Total shards {metric_name}", "", "", metric_value])

    results_matrix = [[x[0], "{:.3f}".format(x[3])] for x in results_matrix]
    writer = MarkdownTableWriter(
        table_name=table_name,
        headers=results_matrix_headers,
        value_matrix=results_matrix,
    )
    writer.write_table()
