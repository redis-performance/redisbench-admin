#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import logging

from redisbench_admin.run.common import extract_test_feasible_setups
from redisbench_admin.run_remote.consts import min_recommended_benchmark_duration
from redisbench_admin.utils.benchmark_config import (
    extract_benchmark_type_from_config,
    extract_redis_dbconfig_parameters,
)


def calculate_client_tool_duration_and_check(
    benchmark_end_time, benchmark_start_time, step_name="Benchmark", warn_min=True
):
    benchmark_duration_seconds = (benchmark_end_time - benchmark_start_time).seconds
    logging.info("{} duration {} secs.".format(step_name, benchmark_duration_seconds))
    if benchmark_duration_seconds < min_recommended_benchmark_duration and warn_min:
        logging.warning(
            "{} duration of {} secs is bellow the considered"
            " minimum duration for a stable run ({} secs).".format(
                step_name,
                benchmark_duration_seconds,
                min_recommended_benchmark_duration,
            )
        )
    return benchmark_duration_seconds


def define_benchmark_plan(benchmark_definitions, default_specs):
    benchmark_runs_plan = {}
    for test_name, benchmark_config in benchmark_definitions.items():
        # extract benchmark-type
        _, benchmark_type = extract_benchmark_type_from_config(benchmark_config)
        logging.info(
            "Using benchmark type: {} for test {}".format(benchmark_type, test_name)
        )
        if benchmark_type not in benchmark_runs_plan:
            benchmark_runs_plan[benchmark_type] = {}

        # extract dataset-name
        dbconfig_present, dataset_name, _, _, _ = extract_redis_dbconfig_parameters(
            benchmark_config, "dbconfig"
        )
        if dataset_name is None:
            dataset_name = test_name
            logging.info(
                "Given no dataset name was found on the db config, using test name as key for unique dataset reference: {}".format(
                    test_name
                )
            )
        if dataset_name not in benchmark_runs_plan[benchmark_type]:
            benchmark_runs_plan[benchmark_type][dataset_name] = {}

        test_setups = extract_test_feasible_setups(
            benchmark_config, "setups", default_specs
        )

        for setup_name, setup_settings in test_setups.items():
            if setup_name not in benchmark_runs_plan[benchmark_type][dataset_name]:
                benchmark_runs_plan[benchmark_type][dataset_name][setup_name] = {}
                benchmark_runs_plan[benchmark_type][dataset_name][setup_name][
                    "setup_settings"
                ] = setup_settings
                benchmark_runs_plan[benchmark_type][dataset_name][setup_name][
                    "benchmarks"
                ] = {}

            benchmark_runs_plan[benchmark_type][dataset_name][setup_name]["benchmarks"][
                test_name
            ] = benchmark_config

    return benchmark_runs_plan
