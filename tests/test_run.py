#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import datetime
import time
import yaml

from redisbench_admin.run.run import calculate_client_tool_duration_and_check
from redisbench_admin.run.run import define_benchmark_plan
from redisbench_admin.utils.benchmark_config import process_default_yaml_properties_file


def test_calculate_client_tool_duration_and_check():
    sleep_time = 1
    benchmark_start_time = datetime.datetime.now()
    time.sleep(sleep_time)
    benchmark_end_time = datetime.datetime.now()
    benchmark_duration_seconds = calculate_client_tool_duration_and_check(
        benchmark_end_time, benchmark_start_time, "benchmark", True
    )
    assert benchmark_duration_seconds >= sleep_time


def test_define_benchmark_plan():
    benchmark_definitions = {
        "test1": {
            "dbconfig": {"dataset_name": "dataset1"},
            "setups": ["oss-standalone", "oss-standalone-threads-6"],
        },
        "test2": {
            "setups": ["oss-standalone", "oss-standalone-threads-6"],
        },
    }
    default_specs = {}
    with open("./tests/test_data/defaults-with-dbconfig.yml", "r") as yml_file:
        (
            _,
            _,
            _,
            _,
            default_specs,
            _,
        ) = process_default_yaml_properties_file(
            None, None, None, "1.yml", None, yml_file
        )

    expected_output = {
        # "performance": {
        #     "dataset1": {
        #         "setup1": {
        #             "setup_settings": {
        #                 "config1": "value1",
        #                 "dbconfig": {"host": "localhost"},
        #             },
        #             "benchmarks": {
        #                 "test1": {
        #                     "type": "performance",
        #                     "dbconfig": {
        #                         "dataset_name": "dataset1",
        #                         "host": "localhost",
        #                     },
        #                     "setups": {
        #                         "setup1": {
        #                             "config1": "value1",
        #                             "dbconfig": {"host": "localhost"},
        #                         }
        #                     },
        #                 }
        #             },
        #         }
        #     }
        # }
    }

    benchmark_plan = define_benchmark_plan(benchmark_definitions, default_specs)

    # ensure we merge properly the 2 configs
    assert (
        benchmark_plan["mixed"]["dataset1"]["oss-standalone"]["benchmarks"]["test1"][
            "dbconfig"
        ]["dataset_name"]
        == "dataset1"
    )

    assert (
        benchmark_plan["mixed"]["dataset1"]["oss-standalone"]["benchmarks"]["test1"][
            "dbconfig"
        ]["dataset_name"]
        == "dataset1"
    )
    assert (
        benchmark_plan["mixed"]["dataset1"]["oss-standalone-threads-6"]["benchmarks"][
            "test1"
        ]["dbconfig"]["dataset_name"]
        == "dataset1"
    )
    assert (
        benchmark_plan["mixed"]["dataset1"]["oss-standalone-threads-6"]["benchmarks"][
            "test1"
        ]["dbconfig"]["module-configuration-parameters"]["redisearch"]["WORKERS"]
        == 6
    )
    assert "module-configuration-parameters" not in (
        benchmark_plan["mixed"]["dataset1"]["oss-standalone"]["benchmarks"]["test1"][
            "dbconfig"
        ]
    )
    # 'module-configuration-parameters'

    # assert True
    # TODO: add this check when the feature is ready
    # assert (
    #     benchmark_plan == expected_output
    # ), f"Expected {expected_output}, but got {benchmark_plan}"
