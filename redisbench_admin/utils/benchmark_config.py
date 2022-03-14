#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#

import logging
import os
import pathlib
import re

import yaml
from jsonpath_ng import parse

from redisbench_admin.utils.remote import (
    validate_result_expectations,
    fetch_remote_id_from_config,
)


def parse_exporter_metrics_definition(
    benchmark_config: dict, configkey: str = "redistimeseries"
):
    metrics = []
    if configkey in benchmark_config:
        if "metrics" in benchmark_config[configkey]:
            for metric_name in benchmark_config[configkey]["metrics"]:
                metrics.append(metric_name)
    return metrics


def parse_exporter_timemetric_definition(
    benchmark_config: dict, configkey: str = "redistimeseries"
):
    metric_path = None
    if "timemetric" in benchmark_config[configkey]:
        metric_path = benchmark_config[configkey]["timemetric"]
    return metric_path


def parse_exporter_timemetric(metric_path: str, results_dict: dict):
    datapoints_timestamp = None
    try:
        jsonpath_expr = parse(metric_path)
        find_res = jsonpath_expr.find(results_dict)
        if len(find_res) > 0:
            datapoints_timestamp = int(find_res[0].value)
    except Exception as e:
        logging.error(
            "Unable to parse time-metric {}. Error: {}".format(metric_path, e.__str__())
        )
    return datapoints_timestamp


def prepare_benchmark_definitions(args):
    benchmark_definitions = {}
    result = True
    defaults_filename, files = get_testfiles_to_process(args)

    (
        default_kpis,
        default_metrics,
        exporter_timemetric_path,
        default_specs,
        clusterconfig,
    ) = get_defaults(defaults_filename)
    for usecase_filename in files:
        with open(usecase_filename, "r", encoding="utf8") as stream:
            test_result, benchmark_config, test_name = get_final_benchmark_config(
                default_kpis, stream, usecase_filename
            )
            result &= test_result
            if test_result:
                benchmark_definitions[test_name] = benchmark_config
    return (
        result,
        benchmark_definitions,
        default_metrics,
        exporter_timemetric_path,
        default_specs,
        clusterconfig,
    )


def process_benchmark_definitions_remote_timeouts(benchmark_definitions):
    remote_envs_timeout = {}
    # prepare the timeout for each different remote type
    for test_name, benchmark_config in benchmark_definitions.items():
        if "remote" in benchmark_config:
            remote_id = fetch_remote_id_from_config(benchmark_config["remote"])
            termination_timeout_secs = get_termination_timeout_secs(benchmark_config)
            if remote_id not in remote_envs_timeout:
                remote_envs_timeout[remote_id] = 0
            remote_envs_timeout[remote_id] = (
                remote_envs_timeout[remote_id] + termination_timeout_secs
            )
    return remote_envs_timeout


def get_defaults(defaults_filename):
    default_metrics = []
    exporter_timemetric_path = None
    default_kpis = None
    default_specs = None
    cluster_config = None
    if os.path.exists(defaults_filename):
        with open(defaults_filename, "r") as stream:
            logging.info(
                "Loading default specifications from file: {}".format(defaults_filename)
            )
            (
                default_kpis,
                default_metrics,
                exporter_timemetric_path,
                default_specs,
                cluster_config,
            ) = process_default_yaml_properties_file(
                default_kpis,
                default_metrics,
                defaults_filename,
                exporter_timemetric_path,
                stream,
            )
    return (
        default_kpis,
        default_metrics,
        exporter_timemetric_path,
        default_specs,
        cluster_config,
    )


def get_final_benchmark_config(default_kpis, stream, usecase_filename):
    result = False
    benchmark_config = None
    test_name = None
    try:
        os.path.dirname(os.path.abspath(usecase_filename))
        benchmark_config = yaml.safe_load(stream)
        kpis_keyname = "kpis"
        if default_kpis is not None:
            merge_default_and_specific_properties_dict_type(
                benchmark_config, default_kpis, kpis_keyname, usecase_filename
            )
        test_name = benchmark_config["name"]
        result = True
    except Exception as e:
        logging.error(
            "while loading file {} and error was returned: {}".format(
                usecase_filename, e.__str__()
            )
        )
        pass

    return result, benchmark_config, test_name


def merge_default_and_specific_properties_dict_type(
    benchmark_config, default_properties, propertygroup_keyname, usecase_filename
):
    if propertygroup_keyname not in benchmark_config:
        benchmark_config[propertygroup_keyname] = default_properties
        pg_len = 0
        if type(benchmark_config[propertygroup_keyname]) == list:
            pg_len = len(benchmark_config[propertygroup_keyname])
        elif type(benchmark_config[propertygroup_keyname]) == dict:
            pg_len = len(benchmark_config[propertygroup_keyname].keys())
        logging.info(
            "Using exclusively default '{}' properties (total={}) given the file {} had no '{}' property group".format(
                propertygroup_keyname,
                pg_len,
                usecase_filename,
                propertygroup_keyname,
            )
        )
    else:
        usecase_kpi = None
        use_case_specific_properties = benchmark_config[propertygroup_keyname]
        for default_property in default_properties:
            default_rule, default_details = list(default_property.items())[0]
            default_condition = list(default_details.values())[0]
            comparison_key = "{}{}".format(default_rule, default_condition)
            found = False
            for usecase_kpi in use_case_specific_properties:
                usecase_rule, usecase_details = list(usecase_kpi.items())[0]
                usecase_condition = list(usecase_details.values())[0]
                usecase_comparison_key = "{}{}".format(usecase_rule, usecase_condition)
                if comparison_key == usecase_comparison_key:
                    found = True
            if found:
                logging.info(
                    "Skipping to add default '{}' property ({}) given the file {}"
                    " had the same specific property ({})".format(
                        propertygroup_keyname,
                        default_property,
                        usecase_filename,
                        usecase_kpi,
                    )
                )
            else:
                use_case_specific_properties.append(default_property)
                logging.info(
                    "Adding a default '{}' property ({}) given the file {} did not had the specific property".format(
                        propertygroup_keyname, default_property, usecase_filename
                    )
                )


def extract_redis_dbconfig_parameters(benchmark_config, dbconfig_keyname):
    redis_configuration_parameters = {}
    modules_configuration_parameters_map = {}
    dataset_load_timeout_secs = 120
    dataset_name = None
    dbconfig_present = False
    if dbconfig_keyname in benchmark_config:
        dbconfig_present = True
        if type(benchmark_config[dbconfig_keyname]) == list:
            for k in benchmark_config[dbconfig_keyname]:
                if "module-configuration-parameters" in k:
                    modules_configuration_parameters_map = k[
                        "module-configuration-parameters"
                    ]
                if "configuration-parameters" in k:
                    cp = k["configuration-parameters"]
                    for item in cp:
                        for k, v in item.items():
                            redis_configuration_parameters[k] = v
                if "dataset_load_timeout_secs" in k:
                    dataset_load_timeout_secs = k["dataset_load_timeout_secs"]
                if "dataset_name" in k:
                    dataset_name = k["dataset_name"]
        if type(benchmark_config[dbconfig_keyname]) == dict:
            if "module-configuration-parameters" in benchmark_config[dbconfig_keyname]:
                modules_configuration_parameters_map = benchmark_config[
                    dbconfig_keyname
                ]["module-configuration-parameters"]
            if "configuration-parameters" in benchmark_config[dbconfig_keyname]:
                cp = benchmark_config[dbconfig_keyname]["configuration-parameters"]
                for k, v in cp.items():
                    redis_configuration_parameters[k] = v
                if "dataset_load_timeout_secs" in cp:
                    dataset_load_timeout_secs = cp["dataset_load_timeout_secs"]
                if "dataset_name" in cp:
                    dataset_name = cp["dataset_name"]

    return (
        dbconfig_present,
        dataset_name,
        redis_configuration_parameters,
        dataset_load_timeout_secs,
        modules_configuration_parameters_map,
    )


def process_default_yaml_properties_file(
    default_kpis, default_metrics, defaults_filename, exporter_timemetric_path, stream
):
    default_config = yaml.safe_load(stream)
    default_specs = None
    cluster_config = None
    default_metrics, exporter_timemetric_path = extract_exporter_metrics(default_config)
    if "kpis" in default_config:
        logging.info(
            "Loading default KPIs specifications from file: {}".format(
                defaults_filename
            )
        )
        default_kpis = default_config["kpis"]
    if "spec" in default_config:
        logging.info(
            "Loading default setup SPECs from file: {}".format(defaults_filename)
        )
        default_specs = default_config["spec"]
    if "clusterconfig" in default_config:
        logging.info(
            "Loading cluster-config default steps from file: {}".format(
                defaults_filename
            )
        )
        cluster_config = default_config["clusterconfig"]
    return (
        default_kpis,
        default_metrics,
        exporter_timemetric_path,
        default_specs,
        cluster_config,
    )


def extract_exporter_metrics(default_config):
    if "exporter" in default_config:
        default_metrics = parse_exporter_metrics_definition(default_config["exporter"])
        if len(default_metrics) > 0:
            logging.info(
                "Found RedisTimeSeries default metrics specification."
                " Will include the following metrics on all benchmarks {}".format(
                    " ".join(default_metrics)
                )
            )
        exporter_timemetric_path = parse_exporter_timemetric_definition(
            default_config["exporter"]
        )
        if exporter_timemetric_path is not None:
            logging.info(
                "Found RedisTimeSeries default time metric specification."
                " Will use the following JSON path to retrieve the test time {}".format(
                    exporter_timemetric_path
                )
            )
    return default_metrics, exporter_timemetric_path


def get_metadata_tags(benchmark_config):
    metadata_tags = {}
    if "metadata" in benchmark_config:
        metadata_tags = benchmark_config["metadata"]
    if "labels" in metadata_tags:
        if type(metadata_tags["labels"]) == dict:
            metadata_tags = metadata_tags["labels"]
    return metadata_tags


def get_termination_timeout_secs(benchmark_config):
    timeout_seconds = 600
    if "timeout_seconds" in benchmark_config:
        timeout_seconds = int(benchmark_config["timeout_seconds"])
    return timeout_seconds


def extract_benchmark_type_from_config(
    benchmark_config,
    config_key="clientconfig",
    benchmark_type_key="benchmark_type",
    default_benchmark_type="mixed",
):
    benchmark_config_present = False
    benchmark_type = None
    if config_key in benchmark_config:

        if type(benchmark_config[config_key]) == list:
            for entry in benchmark_config[config_key]:
                if benchmark_type_key in entry:
                    benchmark_type = entry[benchmark_type_key]
                    benchmark_config_present = True
        elif type(benchmark_config[config_key]) == dict:
            if benchmark_type_key in benchmark_config[config_key]:
                benchmark_type = benchmark_config[config_key][benchmark_type_key]
                benchmark_config_present = True
    if benchmark_type is None:
        logging.info(
            "Given the '{}' info was not present on {} we will assume the most inclusive default: '{}'".format(
                benchmark_type_key, config_key, default_benchmark_type
            )
        )
        benchmark_type = default_benchmark_type
    return benchmark_config_present, benchmark_type


def extract_benchmark_tool_settings(benchmark_config, config_key="clientconfig"):
    benchmark_tool = None
    benchmark_tool_source = None
    benchmark_tool_source_inner_path = None
    benchmark_min_tool_version = None
    benchmark_min_tool_version_major = None
    benchmark_min_tool_version_minor = None
    benchmark_min_tool_version_patch = None
    benchmark_tool_property_map = benchmark_config[config_key]
    if type(benchmark_tool_property_map) == dict:
        (
            benchmark_min_tool_version,
            benchmark_min_tool_version_major,
            benchmark_min_tool_version_minor,
            benchmark_min_tool_version_patch,
            benchmark_tool,
            benchmark_tool_source,
            benchmark_tool_source_inner_path,
        ) = tool_entry_check(
            benchmark_min_tool_version_major,
            benchmark_min_tool_version_minor,
            benchmark_min_tool_version_patch,
            benchmark_tool,
            benchmark_tool_source,
            benchmark_tool_source_inner_path,
            benchmark_tool_property_map,
        )
    elif type(benchmark_tool_property_map) == list:
        for entry in benchmark_config[config_key]:
            (
                benchmark_min_tool_version,
                benchmark_min_tool_version_major,
                benchmark_min_tool_version_minor,
                benchmark_min_tool_version_patch,
                benchmark_tool,
                benchmark_tool_source,
                benchmark_tool_source_inner_path,
            ) = tool_entry_check(
                benchmark_min_tool_version_major,
                benchmark_min_tool_version_minor,
                benchmark_min_tool_version_patch,
                benchmark_tool,
                benchmark_tool_source,
                benchmark_tool_source_inner_path,
                entry,
            )
    return (
        benchmark_min_tool_version,
        benchmark_min_tool_version_major,
        benchmark_min_tool_version_minor,
        benchmark_min_tool_version_patch,
        benchmark_tool,
        benchmark_tool_source,
        benchmark_tool_source_inner_path,
        benchmark_tool_property_map,
    )


def tool_entry_check(
    benchmark_min_tool_version_major,
    benchmark_min_tool_version_minor,
    benchmark_min_tool_version_patch,
    benchmark_tool,
    benchmark_tool_source,
    benchmark_tool_source_inner_path,
    entry,
):
    benchmark_min_tool_version = None
    if "tool" in entry:
        benchmark_tool = entry["tool"]
    if "tool_source" in entry:
        for inner_entry in entry["tool_source"]:
            if "remote" in inner_entry:
                benchmark_tool_source = inner_entry["remote"]
            if "bin_path" in inner_entry:
                benchmark_tool_source_inner_path = inner_entry["bin_path"]
    if "min-tool-version" in entry:
        benchmark_min_tool_version = entry["min-tool-version"]
        (
            benchmark_min_tool_version,
            benchmark_min_tool_version_major,
            benchmark_min_tool_version_minor,
            benchmark_min_tool_version_patch,
        ) = min_ver_check(
            benchmark_min_tool_version,
            benchmark_min_tool_version_major,
            benchmark_min_tool_version_minor,
            benchmark_min_tool_version_patch,
        )
    return (
        benchmark_min_tool_version,
        benchmark_min_tool_version_major,
        benchmark_min_tool_version_minor,
        benchmark_min_tool_version_patch,
        benchmark_tool,
        benchmark_tool_source,
        benchmark_tool_source_inner_path,
    )


def min_ver_check(
    benchmark_min_tool_version,
    benchmark_min_tool_version_major,
    benchmark_min_tool_version_minor,
    benchmark_min_tool_version_patch,
):
    p = re.compile(r"(\d+)\.(\d+)\.(\d+)")
    m = p.match(benchmark_min_tool_version)
    if m is None:
        logging.error(
            "Unable to extract semversion from 'min-tool-version'."
            " Will not enforce version"
        )
        benchmark_min_tool_version = None
    else:
        benchmark_min_tool_version_major = m.group(1)
        benchmark_min_tool_version_minor = m.group(2)
        benchmark_min_tool_version_patch = m.group(3)
    return (
        benchmark_min_tool_version,
        benchmark_min_tool_version_major,
        benchmark_min_tool_version_minor,
        benchmark_min_tool_version_patch,
    )


def get_testfiles_to_process(args):
    defaults_filename = args.defaults_filename
    if args.test == "":
        files = pathlib.Path().glob(args.test_glob)
        files = [str(x) for x in files]
        if defaults_filename in files:
            files.remove(defaults_filename)

        logging.info(
            "Running all specified benchmarks: {}".format(
                " ".join([str(x) for x in files])
            )
        )
    else:
        files = args.test.split(",")
        logging.info("Running specific benchmark in file: {}".format(files))
    return defaults_filename, files


def check_required_modules(module_names, required_modules):
    if required_modules is not None:
        if len(required_modules) > 0:
            logging.info(
                "Checking if the following required modules {} are present".format(
                    required_modules
                )
            )
            for required_module in required_modules:
                if required_module not in module_names:
                    raise Exception(
                        "Unable to detect required module {} in {}. Aborting...".format(
                            required_module,
                            module_names,
                        )
                    )


def results_dict_kpi_check(benchmark_config, results_dict, return_code):
    result = True
    if "kpis" in benchmark_config:
        result = validate_result_expectations(
            benchmark_config,
            results_dict,
            result,
            expectations_key="kpis",
        )
        if result is not True:
            return_code |= 1
    return return_code
