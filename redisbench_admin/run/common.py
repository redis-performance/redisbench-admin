import datetime as dt
import logging
import re

import yaml

from redisbench_admin.run.redis_benchmark.redis_benchmark import prepareRedisBenchmarkCommand
from redisbench_admin.run.redisgraph_benchmark_go.redisgraph_benchmark_go import prepareRedisGraphBenchmarkGoCommand
from redisbench_admin.run.ycsb.ycsb import prepareYCSBBenchmarkCommand
from redisbench_admin.utils.benchmark_config import parseExporterMetricsDefinition, parseExporterTimeMetricDefinition, \
    parseExporterTimeMetric
from redisbench_admin.utils.remote import executeRemoteCommands, getFileFromRemoteSetup, extractRedisGraphVersion, \
    extractPerVersionTimeSeriesFromResults, pushDataToRedisTimeSeries, extractPerBranchTimeSeriesFromResults


def extract_benchmark_tool_settings(benchmark_config):
    benchmark_tool = None
    benchmark_tool_source = None
    benchmark_min_tool_version = None
    benchmark_min_tool_version_major = None
    benchmark_min_tool_version_minor = None
    benchmark_min_tool_version_patch = None

    for entry in benchmark_config["clientconfig"]:
        if 'tool' in entry:
            benchmark_tool = entry['tool']
        if 'tool_source' in entry:
            benchmark_tool_source = entry['tool_source']
        if 'min-tool-version' in entry:
            benchmark_min_tool_version = entry['min-tool-version']
            p = re.compile("(\d+)\.(\d+)\.(\d+)")
            m = p.match(benchmark_min_tool_version)
            if m is None:
                logging.error(
                    "Unable to extract semversion from 'min-tool-version'. Will not enforce version")
                benchmark_min_tool_version = None
            else:
                benchmark_min_tool_version_major = m.group(1)
                benchmark_min_tool_version_minor = m.group(2)
                benchmark_min_tool_version_patch = m.group(3)
    return benchmark_min_tool_version, benchmark_min_tool_version_major, benchmark_min_tool_version_minor, benchmark_min_tool_version_patch, benchmark_tool, benchmark_tool_source


def prepare_benchmark_parameters(benchmark_config, benchmark_tool, server_plaintext_port, server_private_ip,
                                 remote_results_file, isremote=False, current_workdir=None):
    for entry in benchmark_config["clientconfig"]:
        if 'parameters' in entry:
            if 'redis-benchmark' in benchmark_tool:
                command_arr, command_str = prepareRedisBenchmarkCommand(
                    benchmark_tool,
                    server_private_ip,
                    server_plaintext_port,
                    entry
                )
                redirect_file = ">{}".format(remote_results_file)
                command_arr.append(redirect_file)
                command_str = command_str + " " + redirect_file

            if 'redisgraph-benchmark-go' in benchmark_tool:
                if isremote is True:
                    benchmark_tool = "/tmp/redisgraph-benchmark-go"
                command_arr = prepareRedisGraphBenchmarkGoCommand(
                    benchmark_tool,
                    server_private_ip,
                    server_plaintext_port,
                    entry,
                    remote_results_file,
                    isremote
                )
                command_str = " ".join(command_arr)

            if 'ycsb' in benchmark_tool:
                command_arr, command_str = prepareYCSBBenchmarkCommand(
                    benchmark_tool,
                    server_private_ip,
                    server_plaintext_port,
                    entry,
                    current_workdir
                )
    logging.info(
        "Running the benchmark with the following parameters:\n\tArgs array: {}\n\tArgs str: {}".format(
            command_arr, command_str
        )
    )
    return command_arr, command_str


def runRemoteBenchmark(
        client_public_ip,
        username,
        private_key,
        remote_results_file,
        local_results_file,
        command
):
    remote_run_result = False
    res = executeRemoteCommands(client_public_ip, username, private_key, [command])
    recv_exit_status, stdout, stderr = res[0]

    if recv_exit_status != 0:
        logging.error("Exit status of remote command execution {}. Printing stdout and stderr".format(recv_exit_status))
        logging.error("remote process stdout: ".format(stdout))
        logging.error("remote process stderr: ".format(stderr))
    else:
        logging.info("Remote process exited normally. Exit code {}. Printing stdout.".format(recv_exit_status))
        logging.info("remote process stdout: ".format(stdout))
        logging.info("Extracting the benchmark results")
        remote_run_result = True
        getFileFromRemoteSetup(
            client_public_ip,
            username,
            private_key,
            local_results_file,
            remote_results_file,
        )
    return remote_run_result


def merge_default_and_specific_properties_dictType(benchmark_config, default_properties, propertygroup_keyname,
                                                   usecase_filename):
    if propertygroup_keyname not in benchmark_config:
        benchmark_config[propertygroup_keyname] = default_properties
        logging.info(
            "Using exclusively default '{}' properties (total={}) given the file {} had no '{}' property group".format(
                propertygroup_keyname, len(benchmark_config[propertygroup_keyname].keys()), usecase_filename,
                propertygroup_keyname)
        )
    else:
        use_case_specific_properties = benchmark_config[propertygroup_keyname]
        for default_property in default_properties:
            default_rule, default_details = list(default_property.items())[0]
            default_condition = list(default_details.values())[0]
            comparison_key = "{}{}".format(default_rule, default_condition)
            found = False
            found_details = None
            for usecase_kpi in use_case_specific_properties:
                usecase_rule, usecase_details = list(usecase_kpi.items())[0]
                usecase_condition = list(usecase_details.values())[0]
                usecase_comparison_key = "{}{}".format(usecase_rule, usecase_condition)
                if comparison_key == usecase_comparison_key:
                    found = True
                    found_details = usecase_details
            if found:
                logging.info(
                    "Skipping to add default '{}' property ({}) given the file {} had the same specific property ({})".format(
                        propertygroup_keyname,
                        default_property, usecase_filename, usecase_kpi)
                )
            else:
                use_case_specific_properties.append(default_property)
                logging.info(
                    "Adding a default '{}' property ({}) given the file {} did not had the specific property".format(
                        propertygroup_keyname, default_property, usecase_filename)
                )


def process_default_yaml_properties_file(default_kpis, default_metrics, defaults_filename, exporter_timemetric_path,
                                         stream):
    default_config = yaml.safe_load(stream)
    if "exporter" in default_config:
        default_metrics = parseExporterMetricsDefinition(default_config["exporter"])
        if len(default_metrics) > 0:
            logging.info(
                "Found RedisTimeSeries default metrics specification. Will include the following metrics on all benchmarks {}".format(
                    " ".join(default_metrics)
                )
            )
        exporter_timemetric_path = parseExporterTimeMetricDefinition(
            default_config["exporter"]
        )
        if exporter_timemetric_path is not None:
            logging.info(
                "Found RedisTimeSeries default time metric specification. Will use the following JSON path to retrieve the test time {}".format(
                    exporter_timemetric_path
                )
            )
    if "kpis" in default_config:
        logging.info(
            "Loading default KPIs specifications from file: {}".format(defaults_filename)
        )
        default_kpis = default_config["kpis"]
    return default_kpis, default_metrics, exporter_timemetric_path


def common_exporter_logic(deployment_type, exporter_timemetric_path, metrics, results_dict, rts, test_name,
                          tf_github_branch, tf_github_org, tf_github_repo, tf_triggering_env):
    if exporter_timemetric_path is not None and len(metrics) > 0:
        # extract timestamp
        datapoints_timestamp = parseExporterTimeMetric(
            exporter_timemetric_path, results_dict
        )

        rg_version = extractRedisGraphVersion(results_dict)
        if rg_version is None:
            rg_version = "N/A"

        # extract per branch datapoints
        (
            ok,
            per_version_time_series_dict,
        ) = extractPerVersionTimeSeriesFromResults(
            datapoints_timestamp,
            metrics,
            results_dict,
            rg_version,
            tf_github_org,
            tf_github_repo,
            deployment_type,
            test_name,
            tf_triggering_env,
        )

        # push per-branch data
        pushDataToRedisTimeSeries(rts, per_version_time_series_dict)
        if tf_github_branch != None and tf_github_branch != '':
            # extract per branch datapoints
            ok, branch_time_series_dict = extractPerBranchTimeSeriesFromResults(
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
            # push per-branch data
            pushDataToRedisTimeSeries(rts, branch_time_series_dict)
        else:
            logging.warning(
                "Requested to push data to RedisTimeSeries but no git branch definition was found. git branch value {}".format(
                    tf_github_branch)
            )
    else:
        logging.error(
            "Requested to push data to RedisTimeSeries but no exporter definition was found. Missing \"exporter\" config."
        )


def get_start_time_vars(start_time=dt.datetime.utcnow()):
    start_time_ms = int((start_time - dt.datetime(1970, 1, 1)).total_seconds() * 1000)
    start_time_str = start_time.strftime("%Y-%m-%d-%H-%M-%S")
    return start_time, start_time_ms, start_time_str
