import json

import yaml

from redisbench_admin.utils.benchmark_config import results_dict_kpi_check


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
