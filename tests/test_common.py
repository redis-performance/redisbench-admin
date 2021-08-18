import datetime as dt
import json
from unittest import TestCase

import redis
import yaml
from redistimeseries.client import Client

from redisbench_admin.export.common.common import (
    add_datapoint,
    split_tags_string,
    get_or_none,
)
from redisbench_admin.run.common import (
    prepare_benchmark_parameters,
    get_start_time_vars,
    common_exporter_logic,
    extract_test_feasible_setups,
    get_setup_type_and_primaries_count,
    merge_default_and_config_metrics,
)

from redisbench_admin.utils.benchmark_config import (
    process_default_yaml_properties_file,
    extract_benchmark_tool_settings,
    get_defaults,
    get_final_benchmark_config,
)
from redisbench_admin.utils.utils import get_ts_metric_name


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
            == 'ycsb load redisearch -P ./workloads/workload-ecommerce -p "threadcount=64"'
            ' -p "redis.host=localhost" -p "redis.port=6380"'
            " -p dictfile=./bin/uci_online_retail.csv"
            " -p recordcount=100000 -p operationcount=100000"
        )
    with open(
        "./tests/test_data/redis-benchmark-full-suite-1Mkeys-100B.yml", "r"
    ) as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        command_arr, command_str = prepare_benchmark_parameters(
            benchmark_config, "redis-benchmark", "6380", "localhost", "out.txt", False
        )
        assert (
            command_str
            == "redis-benchmark -h localhost -p 6380 --csv -e -c 50 -n 100000 --threads 2 -P 1"
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
                benchmark_tool_source_bin_path,
                _,
            ) = extract_benchmark_tool_settings(benchmark_config)
            assert benchmark_tool is not None
            prepare_benchmark_parameters(
                benchmark_config, benchmark_tool, "9999", "localhost", "out.txt", False
            )


def test_extract_benchmark_tool_settings_with_remote():
    with open("./tests/test_data/ycsb-config.yml", "r") as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        (
            benchmark_min_tool_version,
            benchmark_min_tool_version_major,
            benchmark_min_tool_version_minor,
            benchmark_min_tool_version_patch,
            benchmark_tool,
            benchmark_tool_source,
            benchmark_tool_source_bin_path,
            _,
        ) = extract_benchmark_tool_settings(benchmark_config)
        assert benchmark_tool is not None
        assert benchmark_tool_source is not None
        assert benchmark_tool_source_bin_path is not None


def test_extract_benchmark_tool_settings_with_resource():
    with open("./tests/test_data/ycsb-config.yml", "r") as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        (
            benchmark_min_tool_version,
            benchmark_min_tool_version_major,
            benchmark_min_tool_version_minor,
            benchmark_min_tool_version_patch,
            benchmark_tool,
            benchmark_tool_source,
            benchmark_tool_source_bin_path,
            benchmark_tool_property_map,
        ) = extract_benchmark_tool_settings(benchmark_config)
        assert benchmark_tool is not None
        assert benchmark_tool_source is not None
        assert benchmark_tool_source_bin_path is not None


def test_common_exporter_logic():
    # negative test
    common_exporter_logic(None, None, None, None, None, None, None, None, None, None)
    try:
        rts = Client()
        rts.redis.ping()
        with open(
            "./tests/test_data/redis-benchmark-full-suite-1Mkeys-100B.yml", "r"
        ) as yml_file:
            benchmark_config = yaml.safe_load(yml_file)
            merged_exporter_timemetric_path, metrics = merge_default_and_config_metrics(
                benchmark_config, None, None
            )
            with open(
                "./tests/test_data/results/oss-standalone-2021-07-23-16-15-12-71d4528-redis-benchmark-full-suite-1Mkeys-100B.json",
                "r",
            ) as json_file:
                results_dict = json.load(json_file)
                tf_github_org = "redis"
                tf_github_repo = "redis"
                tf_github_branch = "unstable"
                project_version = "6.2.4"
                tf_triggering_env = "gh"
                test_name = "redis-benchmark-full-suite-1Mkeys-100B"
                deployment_type = "oss-standalone"
                common_exporter_logic(
                    deployment_type,
                    merged_exporter_timemetric_path,
                    metrics,
                    results_dict,
                    rts,
                    test_name,
                    tf_github_branch,
                    tf_github_org,
                    tf_github_repo,
                    tf_triggering_env,
                    project_version,
                )
                metric_name = "rps"
                use_metric_context_path = True
                metric_context_path = "MSET"

                ts_key_name = get_ts_metric_name(
                    "by.branch",
                    "unstable",
                    tf_github_org,
                    tf_github_repo,
                    deployment_type,
                    test_name,
                    tf_triggering_env,
                    metric_name,
                    metric_context_path,
                    use_metric_context_path,
                )

                assert ts_key_name.encode() in rts.redis.keys()
                rts.redis.flushall()

                # test for build variant
                build_variant_name = "variant-1"

                common_exporter_logic(
                    deployment_type,
                    merged_exporter_timemetric_path,
                    metrics,
                    results_dict,
                    rts,
                    test_name,
                    tf_github_branch,
                    tf_github_org,
                    tf_github_repo,
                    tf_triggering_env,
                    project_version,
                    {},
                    build_variant_name,
                )
                metric_name = "rps"
                use_metric_context_path = True
                metric_context_path = "MSET"

                ts_key_name = get_ts_metric_name(
                    "by.branch",
                    "unstable",
                    tf_github_org,
                    tf_github_repo,
                    deployment_type,
                    test_name,
                    tf_triggering_env,
                    metric_name,
                    metric_context_path,
                    use_metric_context_path,
                    build_variant_name,
                )
                assert ts_key_name.encode() in rts.redis.keys()
                rts.redis.flushall()

                # test for build variant and extra metadata flags
                build_variant_name = "variant-1"

                common_exporter_logic(
                    deployment_type,
                    merged_exporter_timemetric_path,
                    metrics,
                    results_dict,
                    rts,
                    test_name,
                    tf_github_branch,
                    tf_github_org,
                    tf_github_repo,
                    tf_triggering_env,
                    project_version,
                    {"arch": "amd64", "compiler": "icc", "compiler_version": "10.3"},
                    build_variant_name,
                )
                metric_name = "rps"
                use_metric_context_path = True
                metric_context_path = "MSET"

                ts_key_name = get_ts_metric_name(
                    "by.branch",
                    "unstable",
                    tf_github_org,
                    tf_github_repo,
                    deployment_type,
                    test_name,
                    tf_triggering_env,
                    metric_name,
                    metric_context_path,
                    use_metric_context_path,
                    build_variant_name,
                )
                assert ts_key_name.encode() in rts.redis.keys()
                assert "arch" in rts.info(ts_key_name).labels
                assert "compiler" in rts.info(ts_key_name).labels
                assert "compiler_version" in rts.info(ts_key_name).labels

    except redis.exceptions.ConnectionError:
        pass


def test_process_default_yaml_properties_file():
    with open("./tests/test_data/common-properties-v0.1.yml", "r") as yml_file:
        (
            default_kpis,
            default_metrics,
            exporter_timemetric_path,
            default_specs,
            cluster_config,
        ) = process_default_yaml_properties_file(None, None, "1.yml", None, yml_file)
        assert exporter_timemetric_path == "$.StartTime"
        assert default_kpis is None
        assert default_specs is None
        assert cluster_config is None


def test_extract_test_feasible_setups():
    defaults_filename = "./tests/test_data/common-properties-v0.3.yml"
    (
        default_kpis,
        default_metrics,
        exporter_timemetric_path,
        default_specs,
        cluster_config,
    ) = get_defaults(defaults_filename)
    usecase_filename = "./tests/test_data/tsbs-devops-ingestion-scale100-4days-v2.yml"
    with open(usecase_filename, "r") as stream:
        benchmark_config, test_name = get_final_benchmark_config(
            default_kpis, stream, usecase_filename
        )
    assert cluster_config == {
        "init_commands": [
            {
                "commands": ["RG.REFRESHCLUSTER"],
                "when_modules_present": ["redisgears.so"],
            }
        ]
    }

    assert default_specs is not None
    assert len(default_specs["setups"]) == 6
    test_setups = extract_test_feasible_setups(
        benchmark_config, "setups", default_specs
    )
    assert len(test_setups.keys()) == 2
    assert "oss-standalone" in test_setups
    assert test_setups["oss-standalone"] != {}
    standalone_setup_type = test_setups["oss-standalone"]["type"]
    standalone_shard_count = test_setups["oss-standalone"]["redis_topology"][
        "primaries"
    ]
    assert standalone_setup_type == "oss-standalone"
    assert standalone_shard_count == 1
    t, c = get_setup_type_and_primaries_count(test_setups["oss-standalone"])
    assert standalone_setup_type == t
    assert standalone_shard_count == c

    assert "oss-cluster-3-primaries" in test_setups
    assert test_setups["oss-cluster-3-primaries"] != {}
    osscluster_setup_type = test_setups["oss-cluster-3-primaries"]["type"]
    osscluster_shard_count = test_setups["oss-cluster-3-primaries"]["redis_topology"][
        "primaries"
    ]
    t, c = get_setup_type_and_primaries_count(test_setups["oss-cluster-3-primaries"])
    assert osscluster_setup_type == t
    assert osscluster_shard_count == c
