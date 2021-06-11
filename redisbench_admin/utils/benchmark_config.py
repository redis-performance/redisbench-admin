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

from redisbench_admin.utils.remote import validate_result_expectations


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
    jsonpath_expr = parse(metric_path)
    datapoints_timestamp = int(jsonpath_expr.find(results_dict)[0].value)
    return datapoints_timestamp


def prepare_benchmark_definitions(args):
    benchmark_definitions = {}
    defaults_filename, files = get_testfiles_to_process(args)
    default_metrics = []
    exporter_timemetric_path = None
    default_kpis = None
    if os.path.exists(defaults_filename):
        with open(defaults_filename, "r") as stream:
            logging.info(
                "Loading default specifications from file: {}".format(defaults_filename)
            )
            (
                default_kpis,
                default_metrics,
                exporter_timemetric_path,
            ) = process_default_yaml_properties_file(
                default_kpis,
                default_metrics,
                defaults_filename,
                exporter_timemetric_path,
                stream,
            )
    for usecase_filename in files:
        with open(usecase_filename, "r") as stream:
            os.path.dirname(os.path.abspath(usecase_filename))
            benchmark_config = yaml.safe_load(stream)
            kpis_keyname = "kpis"
            if default_kpis is not None:
                merge_default_and_specific_properties_dict_type(
                    benchmark_config, default_kpis, kpis_keyname, usecase_filename
                )
            test_name = benchmark_config["name"]
            benchmark_definitions[test_name] = benchmark_config
    return benchmark_definitions, default_metrics, exporter_timemetric_path


def merge_default_and_specific_properties_dict_type(
    benchmark_config, default_properties, propertygroup_keyname, usecase_filename
):
    if propertygroup_keyname not in benchmark_config:
        benchmark_config[propertygroup_keyname] = default_properties
        logging.info(
            "Using exclusively default '{}' properties (total={}) given the file {} had no '{}' property group".format(
                propertygroup_keyname,
                len(benchmark_config[propertygroup_keyname].keys()),
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
    dataset_load_timeout_secs = 120
    if dbconfig_keyname in benchmark_config:
        for k in benchmark_config[dbconfig_keyname]:
            if "configuration-parameters" in k:
                cp = k["configuration-parameters"]
                for item in cp:
                    for k, v in item.items():
                        redis_configuration_parameters[k] = v
            if "dataset_load_timeout_secs" in k:
                dataset_load_timeout_secs = k["dataset_load_timeout_secs"]

    return redis_configuration_parameters, dataset_load_timeout_secs


def process_default_yaml_properties_file(
    default_kpis, default_metrics, defaults_filename, exporter_timemetric_path, stream
):
    default_config = yaml.safe_load(stream)
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
    if "kpis" in default_config:
        logging.info(
            "Loading default KPIs specifications from file: {}".format(
                defaults_filename
            )
        )
        default_kpis = default_config["kpis"]
    return default_kpis, default_metrics, exporter_timemetric_path


def extract_benchmark_tool_settings(benchmark_config):
    benchmark_tool = None
    benchmark_tool_source = None
    benchmark_tool_source_inner_path = None
    benchmark_min_tool_version = None
    benchmark_min_tool_version_major = None
    benchmark_min_tool_version_minor = None
    benchmark_min_tool_version_patch = None

    for entry in benchmark_config["clientconfig"]:
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
        benchmark_tool,
        benchmark_tool_source,
        benchmark_tool_source_inner_path,
    )


def get_testfiles_to_process(args):
    defaults_filename = "defaults.yml"
    if args.test == "":
        files = pathlib.Path().glob("*.yml")
        files = [str(x) for x in files]
        if defaults_filename in files:
            files.remove(defaults_filename)

        logging.info(
            "Running all specified benchmarks: {}".format(
                " ".join([str(x) for x in files])
            )
        )
    else:
        logging.info("Running specific benchmark in file: {}".format(args.test))
        files = [args.test]
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
