import datetime as dt
from unittest import TestCase

import yaml

from redisbench_admin.export.common.common import (
    add_datapoint,
    split_tags_string,
    get_or_none,
)
from redisbench_admin.run.common import (
    get_start_time_vars,
    prepare_benchmark_parameters,
    extract_benchmark_tool_settings,
    common_exporter_logic,
    process_default_yaml_properties_file,
)


class Test(TestCase):
    def test_add_datapoint(self):
        time_series_dict = {}
        broader_ts_name = "ts"
        tags_array = []
        add_datapoint(time_series_dict, broader_ts_name, 1, 5.0, tags_array)
        add_datapoint(time_series_dict, broader_ts_name, 4, 10.0, tags_array)
        add_datapoint(time_series_dict, broader_ts_name, 60, 10.0, tags_array)
        assert time_series_dict == {
            "ts": {
                "data": [5.0, 10.0, 10.0],
                "index": [1, 4, 60],
                "tags": {},
                "tags-array": [],
            }
        }

    def test_split_tags_string(self):
        result = split_tags_string("k1=v1,k2=v2")
        assert result == [{"k1": "v1"}, {"k2": "v2"}]

    def test_get_or_none(self):
        res = get_or_none({}, "k")
        assert res == None
        res = get_or_none({"k": "v"}, "k")
        assert res == "v"


def test_get_start_time_vars():
    start_time, start_time_ms, start_time_str = get_start_time_vars()
    assert type(start_time_ms) == int
    assert start_time_ms > 0
    assert type(start_time_str) == str
    start_time = dt.datetime.utcnow()
    start_time, start_time_ms, start_time_str = get_start_time_vars(start_time)
    assert type(start_time_ms) == int
    assert (
        int((start_time - dt.datetime(1970, 1, 1)).total_seconds() * 1000)
        == start_time_ms
    )
    assert type(start_time_str) == str


def test_prepare_benchmark_parameters():
    with open("./tests/test_data/ycsb-config.yml", "r") as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        command_arr, command_str = prepare_benchmark_parameters(
            benchmark_config, "ycsb", "6380", "localhost", "out.txt", False
        )
        assert (
            command_str
            == 'ycsb load redisearch -P workloads/workload-ecommerce -p "threadcount=64"'
            ' -p "redis.host=localhost" -p "redis.port=6380"'
            " -p recordcount=100000 -p operationcount=100000"
        )


def test_extract_benchmark_tool_settings():
    config_files = [
        "./tests/test_data/ycsb-config.yml",
        "./tests/test_data/redis-benchmark.yml",
        "./tests/test_data/redisgraph-benchmark-go.yml",
    ]
    for file in config_files:
        with open(file, "r") as yml_file:
            benchmark_config = yaml.safe_load(yml_file)
            (
                benchmark_min_tool_version,
                benchmark_min_tool_version_major,
                benchmark_min_tool_version_minor,
                benchmark_min_tool_version_patch,
                benchmark_tool,
                benchmark_tool_source,
            ) = extract_benchmark_tool_settings(benchmark_config)
            assert benchmark_tool is not None
            prepare_benchmark_parameters(
                benchmark_config, benchmark_tool, "9999", "localhost", "out.txt", False
            )


def test_common_exporter_logic():
    # negative test
    common_exporter_logic(None, None, None, None, None, None, None, None, None, None)


def test_process_default_yaml_properties_file():
    with open("./tests/test_data/common-properties-v0.1.yml", "r") as yml_file:
        (
            default_kpis,
            default_metrics,
            exporter_timemetric_path,
        ) = process_default_yaml_properties_file(None, None, "1.yml", None, yml_file)
        assert exporter_timemetric_path == "$.StartTime"
        assert default_kpis is None
