import json

import yaml

from redisbench_admin.utils.benchmark_config import (
    results_dict_kpi_check,
    check_required_modules,
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
