import argparse
import json

import yaml

from redisbench_admin.run_remote.args import create_run_remote_arguments
from redisbench_admin.utils.benchmark_config import (
    results_dict_kpi_check,
    check_required_modules,
    extract_redis_dbconfig_parameters,
    extract_benchmark_type_from_config,
    extract_client_dataset_name,
    get_metadata_tags,
    get_termination_timeout_secs,
    prepare_benchmark_definitions,
    process_benchmark_definitions_remote_timeouts,
    get_testfiles_to_process,
)
from redisbench_admin.run.run import define_benchmark_plan


def test_results_dict_kpi_check():
    return_code = results_dict_kpi_check({}, {}, 0)
    assert return_code == 0
    return_code = results_dict_kpi_check({}, {}, 1)
    assert return_code == 1
    with open(
        "./tests/test_data/redisgraph-benchmark-go-result.json", "r"
    ) as result_fd:
        results_dict = json.load(result_fd)
        with open(
            "./tests/test_data/redisgraph-benchmark-go-defaults.yml", "r"
        ) as config_fd:
            benchmark_config = yaml.safe_load(config_fd)
            return_code = results_dict_kpi_check(benchmark_config, results_dict, 0)
            assert return_code == 0
        with open(
            "./tests/test_data/redisgraph-benchmark-go-bad-kpis.yml", "r"
        ) as config_fd:
            benchmark_config = yaml.safe_load(config_fd)
            return_code = results_dict_kpi_check(benchmark_config, results_dict, 0)
            assert return_code == 1


def test_check_required_modules():
    check_required_modules([], [])
    try:
        check_required_modules(["s"], ["search"])
    except Exception as e:
        assert "Unable to detect required module" in e.__str__()
    try:
        check_required_modules([], ["search"])
    except Exception as e:
        assert "Unable to detect required module" in e.__str__()
    check_required_modules(["search", "ReJSON", "TimeSeries"], ["search"])
    check_required_modules(["search", "ReJSON", "TimeSeries"], ["search", "TimeSeries"])


def test_extract_redis_configuration_parameters():
    with open(
        "./tests/test_data/redisgraph-benchmark-go-defaults.yml", "r"
    ) as config_fd:
        benchmark_config = yaml.safe_load(config_fd)
        (
            dbconfig_present,
            _,
            redis_configuration_parameters,
            dataset_load_timeout_secs,
            modules_configuration_parameters_map,
        ) = extract_redis_dbconfig_parameters(benchmark_config, "dbconfig")
        assert redis_configuration_parameters == {}
        assert modules_configuration_parameters_map == {}
        assert dataset_load_timeout_secs == 120
        assert dbconfig_present == False

    with open(
        "./tests/test_data/tsbs-devops-ingestion-scale100-4days-keyspace.yml", "r"
    ) as config_fd:
        benchmark_config = yaml.safe_load(config_fd)
        (
            dbconfig_present,
            _,
            redis_configuration_parameters,
            dataset_load_timeout_secs,
            modules_configuration_parameters_map,
        ) = extract_redis_dbconfig_parameters(benchmark_config, "dbconfig")
        assert dataset_load_timeout_secs == 120
        assert modules_configuration_parameters_map == {}
        assert dbconfig_present == True
        assert redis_configuration_parameters == {
            "notify-keyspace-events": "KEA",
            "timeout": 0,
        }

    with open(
        "./tests/test_data/redisgraph-benchmark-go-dataset-timeout.yml", "r"
    ) as config_fd:
        benchmark_config = yaml.safe_load(config_fd)
        (
            dbconfig_present,
            _,
            redis_configuration_parameters,
            dataset_load_timeout_secs,
            modules_configuration_parameters_map,
        ) = extract_redis_dbconfig_parameters(benchmark_config, "dbconfig")
        assert dataset_load_timeout_secs == 1200
        assert modules_configuration_parameters_map == {}
        assert dbconfig_present == True

    with open(
        "./tests/test_data/tsbs-scale100-cpu-max-all-1@4139rps.yml", "r"
    ) as config_fd:
        benchmark_config = yaml.safe_load(config_fd)
        (
            dbconfig_present,
            _,
            redis_configuration_parameters,
            dataset_load_timeout_secs,
            modules_configuration_parameters_map,
        ) = extract_redis_dbconfig_parameters(benchmark_config, "dbconfig")
        assert dataset_load_timeout_secs == 120
        assert modules_configuration_parameters_map == {
            "redistimeseries": {"CHUNK_SIZE_BYTES": 128}
        }
        assert dbconfig_present == True


def test_extract_benchmark_type_from_config():
    with open("./tests/test_data/vecsim-memtier.yml", "r") as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        benchmark_config_present, benchmark_type = extract_benchmark_type_from_config(
            benchmark_config
        )
        assert benchmark_type == "read-only"
        assert benchmark_config_present == True

    with open("./tests/test_data/redis-benchmark.yml", "r") as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        benchmark_config_present, benchmark_type = extract_benchmark_type_from_config(
            benchmark_config
        )
        assert benchmark_type == "mixed"
        assert benchmark_config_present == False


def test_get_metadata_tags():
    with open("./tests/test_data/vecsim-memtier.yml", "r") as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        metadata_tags = get_metadata_tags(benchmark_config)
        assert metadata_tags == {"component": "vecsim"}

    with open("./tests/test_data/redis-benchmark.yml", "r") as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        metadata_tags = get_metadata_tags(benchmark_config)
        assert metadata_tags == {}

    with open(
        "./tests/test_data/tsbs-scale100-cpu-max-all-1@4139rps.yml", "r"
    ) as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        metadata_tags = get_metadata_tags(benchmark_config)
        assert metadata_tags == {"includes_targets": "true", "test_type": "query"}


def test_get_termination_timeout_secs():
    with open("./tests/test_data/vecsim-memtier.yml", "r") as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        timeout_seconds = get_termination_timeout_secs(benchmark_config)
        assert timeout_seconds == 600

    with open("./tests/test_data/vecsim-memtier-timeout.yml", "r") as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        timeout_seconds = get_termination_timeout_secs(benchmark_config)
        assert timeout_seconds == 1200


def test_prepare_benchmark_definitions():
    parser = argparse.ArgumentParser(
        description="test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = create_run_remote_arguments(parser)
    args = parser.parse_args(
        args=[
            "--github_actor",
            "gh.user",
            "--module_path",
            "mymodule.so",
            "--test-glob",
            "./tests/test_data/benchmark_definitions/*.yml",
        ]
    )
    (
        result,
        benchmark_definitions,
        default_metrics,
        exporter_timemetric_path,
        default_specs,
        clusterconfig,
    ) = prepare_benchmark_definitions(args)
    assert result == True
    assert len(benchmark_definitions.keys()) == 6


def test_process_benchmark_definitions_remote_timeouts():
    parser = argparse.ArgumentParser(
        description="test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = create_run_remote_arguments(parser)
    args = parser.parse_args(
        args=[
            "--github_actor",
            "gh.user",
            "--module_path",
            "mymodule.so",
            "--test-glob",
            "./tests/test_data/benchmark_definitions/*.yml",
        ]
    )
    (
        result,
        benchmark_definitions,
        default_metrics,
        exporter_timemetric_path,
        default_specs,
        clusterconfig,
    ) = prepare_benchmark_definitions(args)
    assert result == True
    assert len(benchmark_definitions.keys()) == 6
    remote_envs_timeout = process_benchmark_definitions_remote_timeouts(
        benchmark_definitions
    )
    assert len(remote_envs_timeout.keys()) == 2
    # we have the default timeout + the one specified
    timeouts = list(remote_envs_timeout.values())
    timeouts.sort()
    assert (600 + 1200) in timeouts
    assert (3600 + 600 + 1200) in timeouts


def test_get_testfiles_to_process():
    test_glob_pattern_all = "./tests/test_data/benchmark_definitions/*.yml"
    test_glob_pattern_graph500 = "./tests/test_data/benchmark_definitions/graph500*.yml"
    test_files_to_process_all = get_testfiles_to_process(
        test_glob_pattern_all, "", "defaults.yml"
    )
    assert 6 == len(test_files_to_process_all)
    test_files_to_process_graph500_glob = get_testfiles_to_process(
        test_glob_pattern_graph500, "", "defaults.yml"
    )
    assert 3 == len(test_files_to_process_graph500_glob)
    test_files_to_process = get_testfiles_to_process(
        "./tests/test_data/benchmark_definitions/*.yml",
        "",
        "defaults.yml",
        "graph500.+.yml",
    )
    assert 3 == len(test_files_to_process)
    assert test_files_to_process_graph500_glob == test_files_to_process

    test_files_to_process = get_testfiles_to_process(
        "./tests/test_data/benchmark_definitions/*.yml",
        "",
        "defaults.yml",
        "^(?!.*graph500).*.yml",
    )
    assert 3 == len(test_files_to_process)
    for test_graph in test_files_to_process_graph500_glob:
        assert test_graph not in test_files_to_process

    test_files_to_process_group_member_1 = get_testfiles_to_process(
        test_glob_pattern_all, "", "defaults.yml", ".*", 1, 2
    )
    test_files_to_process_group_member_2 = get_testfiles_to_process(
        test_glob_pattern_all, "", "defaults.yml", ".*", 2, 2
    )
    assert 3 == len(test_files_to_process_group_member_1)
    assert 3 == len(test_files_to_process_group_member_2)

    test_files_to_process_graph500_glob_group_member_1 = get_testfiles_to_process(
        test_glob_pattern_graph500, "", "defaults.yml", ".*", 1, 2
    )
    assert 2 == len(test_files_to_process_graph500_glob_group_member_1)
    test_files_to_process_graph500_glob_group_member_2 = get_testfiles_to_process(
        test_glob_pattern_graph500, "", "defaults.yml", ".*", 2, 2
    )
    assert 1 == len(test_files_to_process_graph500_glob_group_member_2)

    test_files_to_process_graph500_glob_group_member_1 = get_testfiles_to_process(
        test_glob_pattern_graph500, "", "defaults.yml", ".*", 1, 3
    )
    test_files_to_process_graph500_glob_group_member_2 = get_testfiles_to_process(
        test_glob_pattern_graph500, "", "defaults.yml", ".*", 2, 3
    )
    test_files_to_process_graph500_glob_group_member_3 = get_testfiles_to_process(
        test_glob_pattern_graph500, "", "defaults.yml", ".*", 3, 3
    )
    assert 1 == len(test_files_to_process_graph500_glob_group_member_1)
    assert 1 == len(test_files_to_process_graph500_glob_group_member_2)
    assert 1 == len(test_files_to_process_graph500_glob_group_member_3)

    test_files_to_process_all_glob_group_member_1 = get_testfiles_to_process(
        test_glob_pattern_all, "", "defaults.yml", ".*", 1, 5
    )
    assert 1 == len(test_files_to_process_all_glob_group_member_1)
    assert (
        test_files_to_process_all[0] == test_files_to_process_all_glob_group_member_1[0]
    )
    test_files_to_process_all_glob_group_member_4 = get_testfiles_to_process(
        test_glob_pattern_all, "", "defaults.yml", ".*", 4, 5
    )
    assert 1 == len(test_files_to_process_all_glob_group_member_4)
    assert (
        test_files_to_process_all[3] == test_files_to_process_all_glob_group_member_4[0]
    )
    test_files_to_process_all_glob_group_member_5 = get_testfiles_to_process(
        test_glob_pattern_all, "", "defaults.yml", ".*", 5, 5
    )
    assert 2 == len(test_files_to_process_all_glob_group_member_5)
    assert (
        test_files_to_process_all[4:6] == test_files_to_process_all_glob_group_member_5
    )


def test_extract_client_dataset_name():
    """Test extracting dataset_name from clientconfig.

    dataset_name in clientconfig indicates that the benchmark PRODUCES a dataset.
    This is used by load benchmarks that create data for other benchmarks to use.
    """
    # Test with dict format clientconfig containing dataset_name (load benchmark produces dataset)
    with open("./tests/test_data/vanilla-memtier-load.yml", "r") as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        dataset_name = extract_client_dataset_name(benchmark_config)
        assert dataset_name == "vanilla-memtier"

    # Test with clientconfig that doesn't have dataset_name (query benchmark uses dataset via dbconfig)
    with open("./tests/test_data/vanilla-memtier-query.yml", "r") as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        # Query benchmark has dataset_name in dbconfig, not clientconfig
        dataset_name = extract_client_dataset_name(benchmark_config)
        assert dataset_name is None

    # Test with clientconfig that doesn't have dataset_name
    with open("./tests/test_data/redis-benchmark.yml", "r") as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        dataset_name = extract_client_dataset_name(benchmark_config)
        assert dataset_name is None

    # Test with list format clientconfig (no dataset_name)
    with open("./tests/test_data/ycsb-config.yml", "r") as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        dataset_name = extract_client_dataset_name(benchmark_config)
        assert dataset_name is None


def test_define_benchmark_plan_with_clientconfig_dataset_name():
    """Test that define_benchmark_plan correctly groups benchmarks by dataset_name.

    - Load benchmark: has dataset_name in clientconfig (produces the dataset)
    - Query benchmark: has dataset_name in dbconfig (uses the dataset)

    Both should be grouped under the same dataset_name "vanilla-memtier".
    """
    benchmark_definitions = {}

    # Load benchmark with dataset_name in clientconfig (produces dataset)
    with open("./tests/test_data/vanilla-memtier-load.yml", "r") as yml_file:
        load_config = yaml.safe_load(yml_file)
        benchmark_definitions[load_config["name"]] = load_config

    # Query benchmark with dataset_name in dbconfig (uses dataset)
    with open("./tests/test_data/vanilla-memtier-query.yml", "r") as yml_file:
        query_config = yaml.safe_load(yml_file)
        benchmark_definitions[query_config["name"]] = query_config

    # Define the benchmark plan
    benchmark_runs_plan = define_benchmark_plan(benchmark_definitions, None)

    # Both benchmarks should be grouped under the same dataset_name "vanilla-memtier"
    # The load benchmark is "mixed" type and query is "read-only" type
    assert "mixed" in benchmark_runs_plan
    assert "read-only" in benchmark_runs_plan

    # Both should have the same dataset_name
    assert "vanilla-memtier" in benchmark_runs_plan["mixed"]
    assert "vanilla-memtier" in benchmark_runs_plan["read-only"]

    # Verify the benchmarks are correctly placed
    assert (
        "vanilla-memtier-load"
        in benchmark_runs_plan["mixed"]["vanilla-memtier"]["oss-standalone"][
            "benchmarks"
        ]
    )
    assert (
        "vanilla-memtier-query"
        in benchmark_runs_plan["read-only"]["vanilla-memtier"]["oss-standalone"][
            "benchmarks"
        ]
    )
