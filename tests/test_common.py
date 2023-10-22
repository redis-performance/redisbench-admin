import datetime as dt
import json
from unittest import TestCase

import argparse
import redis
import yaml
from redisbench_admin.utils.remote import push_data_to_redistimeseries

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
    check_dbconfig_tool_requirement,
    check_dbconfig_keyspacelen_requirement,
    dso_check,
    dbconfig_keyspacelen_check,
    common_properties_log,
    execute_init_commands,
)
from redisbench_admin.run_remote.args import create_run_remote_arguments

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
        assert result == {"k1": "v1", "k2": "v2"}

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
            == "redis-benchmark -h localhost -p 6380 --csv -e -c 50 -n 100000 --threads 2 -P 1 -r 1000000"
        )


def test_extract_benchmark_tool_settings():
    config_files = [
        "./tests/test_data/ycsb-config.yml",
        "./tests/test_data/redis-benchmark.yml",
        "./tests/test_data/redisgraph-benchmark-go.yml",
        "./tests/test_data/ann-config.yml",
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
        rts = redis.Redis(port=16379)
        rts.ping()
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
                deployment_name = "oss-standalone"
                deployment_type = "oss-standalone"
                (
                    per_version_time_series_dict,
                    per_branch_time_series_dict,
                    _,
                    _,
                    _,
                ) = common_exporter_logic(
                    deployment_name,
                    deployment_type,
                    merged_exporter_timemetric_path,
                    metrics,
                    results_dict,
                    test_name,
                    tf_github_branch,
                    tf_github_org,
                    tf_github_repo,
                    tf_triggering_env,
                    project_version,
                )
                push_data_to_redistimeseries(rts, per_version_time_series_dict)
                push_data_to_redistimeseries(rts, per_branch_time_series_dict)
                metric_name = "rps"
                use_metric_context_path = True
                metric_context_path = "MSET"

                ts_key_name = get_ts_metric_name(
                    "by.branch",
                    "unstable",
                    tf_github_org,
                    tf_github_repo,
                    deployment_name,
                    deployment_type,
                    test_name,
                    tf_triggering_env,
                    metric_name,
                    metric_context_path,
                    use_metric_context_path,
                )

                assert ts_key_name.encode() in rts.keys()
                rts.flushall()

                # test for build variant
                build_variant_name = "variant-1"

                (
                    per_version_time_series_dict,
                    per_branch_time_series_dict,
                    _,
                    _,
                    _,
                ) = common_exporter_logic(
                    deployment_name,
                    deployment_type,
                    merged_exporter_timemetric_path,
                    metrics,
                    results_dict,
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
                    deployment_name,
                    deployment_type,
                    test_name,
                    tf_triggering_env,
                    metric_name,
                    metric_context_path,
                    use_metric_context_path,
                    build_variant_name,
                )
                datapoint_errors, datapoint_inserts = push_data_to_redistimeseries(
                    rts, per_version_time_series_dict
                )
                datapoint_errors, datapoint_inserts = push_data_to_redistimeseries(
                    rts, per_branch_time_series_dict
                )
                assert ts_key_name.encode() in rts.keys()
                rts.flushall()

                # test for build variant and extra metadata flags
                build_variant_name = "variant-1"

                (
                    per_version_time_series_dict,
                    per_branch_time_series_dict,
                    _,
                    _,
                    _,
                ) = common_exporter_logic(
                    deployment_name,
                    deployment_type,
                    merged_exporter_timemetric_path,
                    metrics,
                    results_dict,
                    test_name,
                    tf_github_branch,
                    tf_github_org,
                    tf_github_repo,
                    tf_triggering_env,
                    project_version,
                    {"arch": "amd64", "compiler": "icc", "compiler_version": "10.3"},
                    build_variant_name,
                )
                datapoint_errors, datapoint_inserts = push_data_to_redistimeseries(
                    rts, per_version_time_series_dict
                )
                datapoint_errors, datapoint_inserts = push_data_to_redistimeseries(
                    rts, per_branch_time_series_dict
                )
                metric_name = "rps"
                use_metric_context_path = True
                metric_context_path = "MSET"

                ts_key_name = get_ts_metric_name(
                    "by.branch",
                    "unstable",
                    tf_github_org,
                    tf_github_repo,
                    deployment_name,
                    deployment_type,
                    test_name,
                    tf_triggering_env,
                    metric_name,
                    metric_context_path,
                    use_metric_context_path,
                    build_variant_name,
                )
                assert ts_key_name.encode() in rts.keys()
                assert "arch" in rts.ts().info(ts_key_name).labels
                assert "compiler" in rts.ts().info(ts_key_name).labels
                assert "compiler_version" in rts.ts().info(ts_key_name).labels

    except redis.exceptions.ConnectionError:
        pass


def test_process_default_yaml_properties_file():
    with open("./tests/test_data/common-properties-v0.1.yml", "r") as yml_file:
        (
            default_kpis,
            _,
            default_metrics,
            exporter_timemetric_path,
            default_specs,
            cluster_config,
        ) = process_default_yaml_properties_file(
            None, None, None, "1.yml", None, yml_file
        )
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
        _, benchmark_config, test_name = get_final_benchmark_config(
            default_kpis, None, stream, usecase_filename
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
    n, t, c = get_setup_type_and_primaries_count(test_setups["oss-standalone"])
    assert standalone_setup_type == t
    assert standalone_shard_count == c

    assert "oss-cluster-3-primaries" in test_setups
    assert test_setups["oss-cluster-3-primaries"] != {}
    osscluster_setup_type = test_setups["oss-cluster-3-primaries"]["type"]
    osscluster_shard_count = test_setups["oss-cluster-3-primaries"]["redis_topology"][
        "primaries"
    ]
    n, t, c = get_setup_type_and_primaries_count(test_setups["oss-cluster-3-primaries"])
    assert osscluster_setup_type == t
    assert osscluster_shard_count == c

    # wrong read
    res, benchmark_config, test_name = get_final_benchmark_config(
        default_kpis, None, stream, "dont exist"
    )
    assert res == False
    assert benchmark_config == None
    assert benchmark_config == None


def test_check_dbconfig_tool_requirement():
    with open(
        "./tests/test_data/redis-benchmark-full-suite-1Mkeys-100B.yml", "r"
    ) as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        requires_tool_dbconfig = check_dbconfig_tool_requirement(benchmark_config)
        assert requires_tool_dbconfig == False

    with open("./tests/test_data/tsbs-targets.yml", "r") as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        requires_tool_dbconfig = check_dbconfig_tool_requirement(benchmark_config)
        assert requires_tool_dbconfig == True


def test_check_dbconfig_keyspacelen_requirement():
    with open(
        "./tests/test_data/redis-benchmark-full-suite-1Mkeys-100B.yml", "r"
    ) as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        (
            requires_keyspacelen_check,
            keyspacelen,
        ) = check_dbconfig_keyspacelen_requirement(benchmark_config)
        assert requires_keyspacelen_check == False
        assert keyspacelen == None

    with open("./tests/test_data/tsbs-targets.yml", "r") as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        (
            requires_keyspacelen_check,
            keyspacelen,
        ) = check_dbconfig_keyspacelen_requirement(benchmark_config)
        assert requires_keyspacelen_check == True
        assert keyspacelen == 1000


def test_prepare_benchmark_parameters_specif_tooling():
    config_files = [
        "./tests/test_data/tsbs-devops-ingestion-scale100-4days.yml",
        "./tests/test_data/tsbs-scale100-cpu-max-all-1.yml",
        "./tests/test_data/redis-benchmark.yml",
        "./tests/test_data/redisgraph-benchmark-go.yml",
        "./tests/test_data/ftsb-1M-enwiki_abstract-hashes-fulltext-simple-1word-query.yml",
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
            remote_results_file = "out.remote.txt"
            command_arr, command_str = prepare_benchmark_parameters(
                benchmark_config,
                benchmark_tool,
                "9999",
                "localhost",
                remote_results_file,
                True,
            )
            assert command_str.endswith(remote_results_file)


def test_dso_check():
    dso = dso_check(None, "redistimeseries.so")
    assert dso == "redistimeseries.so"

    dso = dso_check(None, ["redistimeseries.so"])
    assert dso == "redistimeseries.so"

    dso = dso_check(None, ["redisgears.so", "redistimeseries.so"])
    assert dso == "redisgears.so"


def test_dbconfig_keyspacelen_check():
    from redis import StrictRedis
    from redis.exceptions import ConnectionError

    redis_port = 16379
    try:
        redis = StrictRedis(port=redis_port)
        redis.ping()
        redis.flushall()
        redis_conns = [redis]

        with open(
            "./tests/test_data/redis-benchmark-full-suite-1Mkeys-100B.yml", "r"
        ) as yml_file:
            benchmark_config = yaml.safe_load(yml_file)
            # no keyspace len check
            result = dbconfig_keyspacelen_check(benchmark_config, redis_conns)
            assert result == True

        with open("./tests/test_data/tsbs-targets.yml", "r") as yml_file:
            benchmark_config = yaml.safe_load(yml_file)
            # check and fail
            try:
                result = dbconfig_keyspacelen_check(benchmark_config, redis_conns)
            except Exception as e:
                assert (
                    e.__str__()
                    == "The total numbers of keys in setup does not match the expected spec: 1000!=0. Aborting..."
                )

            # check and pass
            for x in range(0, 1000):
                redis.set(x, "A")
            result = dbconfig_keyspacelen_check(benchmark_config, redis_conns)
            assert result == True

    except ConnectionError:
        pass


def test_common_properties_log():
    parser = argparse.ArgumentParser(
        description="test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = create_run_remote_arguments(parser)
    args = parser.parse_args(
        args=[
            "--inventory",
            "server_private_ip=10.0.0.1,server_public_ip=1.1.1.1,client_public_ip=2.2.2.2",
        ]
    )
    tf_bin_path = args.terraform_bin_path
    tf_github_org = args.github_org
    tf_github_actor = args.github_actor
    tf_github_repo = args.github_repo
    tf_github_sha = args.github_sha
    tf_github_branch = args.github_branch
    tf_triggering_env = "ci"
    tf_setup_name_sufix = "suffix"
    private_key = "key1"
    common_properties_log(
        tf_bin_path,
        tf_github_actor,
        tf_github_branch,
        tf_github_org,
        tf_github_repo,
        tf_github_sha,
        tf_setup_name_sufix,
        tf_triggering_env,
        private_key,
    )


def test_execute_init_commands():
    from redis import StrictRedis
    from redis.exceptions import ConnectionError

    redis_port = 16379
    try:
        redis = StrictRedis(port=redis_port)
        redis.ping()
        redis.flushall()
        redis.config_resetstat()

        with open("./tests/test_data/init-commands-array.yml", "r") as yml_file:
            benchmark_config = yaml.safe_load(yml_file)
            # no keyspace len check
            total_cmds = execute_init_commands(benchmark_config, redis)
            assert total_cmds == 3

        assert b"key" in redis.keys()
        assert b"key2" in redis.keys()
    except ConnectionError:
        pass
