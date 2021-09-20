import json

import yaml

from redisbench_admin.utils.benchmark_config import (
    results_dict_kpi_check,
    check_required_modules,
    extract_redis_dbconfig_parameters,
)


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
        ) = extract_redis_dbconfig_parameters(benchmark_config, "dbconfig")
        assert redis_configuration_parameters == {}
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
        ) = extract_redis_dbconfig_parameters(benchmark_config, "dbconfig")
        assert dataset_load_timeout_secs == 120
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
        ) = extract_redis_dbconfig_parameters(benchmark_config, "dbconfig")
        assert dataset_load_timeout_secs == 1200
        assert dbconfig_present == True
