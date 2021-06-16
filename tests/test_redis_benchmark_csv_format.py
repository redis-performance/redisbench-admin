from unittest import TestCase

from redisbench_admin.export.redis_benchmark.redis_benchmark_csv_format import (
    fill_tags_from_passed_array,
    redis_benchmark_export_logic,
)
from redisbench_admin.run.redis_benchmark.redis_benchmark import (
    redis_benchmark_from_stdout_csv_to_json,
)


class Test(TestCase):
    def test_fill_tags_from_passed_array(self):
        kv_array = [
            {"git_sha": "a331"},
        ]
        (
            deployment_type,
            git_sha,
            project,
            project_version,
            run_stage,
        ) = fill_tags_from_passed_array(kv_array)
        assert git_sha == "a331"
        assert deployment_type == None


def test_redis_benchmark_export_logic():
    with open("./tests/test_data/redis-benchmark-6.2.0-csv.out", "r") as csv_file:
        csv_data = csv_file.read()
        results_dict = redis_benchmark_from_stdout_csv_to_json(csv_data, 1, "1")
        redis_benchmark_export_logic(results_dict, [], None, {})
        assert "SET" in results_dict["Tests"]
        assert "139082.06" == results_dict["Tests"]["SET"]["rps"]
        assert "GET" in results_dict["Tests"]
        assert "136986.30" == results_dict["Tests"]["GET"]["rps"]
        assert len("136986.30") == len(results_dict["Tests"]["GET"]["rps"])

    with open("./tests/test_data/redis-benchmark-6.2.0-csv.out.2", "r") as csv_file:
        csv_data = csv_file.read()
        results_dict = redis_benchmark_from_stdout_csv_to_json(csv_data, 1, "1")
        redis_benchmark_export_logic(results_dict, [], None, {})
        assert "JSON.SET" in results_dict["Tests"]
        assert "73391.80" == results_dict["Tests"]["JSON.SET"]["rps"]

    with open("./tests/test_data/redis-benchmark-6.2.0-csv.out.2", "r") as csv_file:
        csv_data = csv_file.read()
        results_dict = redis_benchmark_from_stdout_csv_to_json(
            csv_data, 1, "1", "Overall"
        )
        redis_benchmark_export_logic(results_dict, [], None, {})
        assert "Overall" in results_dict["Tests"]

    with open("./tests/test_data/redis-benchmark-6.2.0-csv.out.3", "r") as csv_file:
        csv_data = csv_file.read()
        results_dict = redis_benchmark_from_stdout_csv_to_json(csv_data, 1, "1")
        redis_benchmark_export_logic(results_dict, [], None, {})
        assert len(results_dict["Tests"].keys()) == 0

    with open("./tests/test_data/redis-benchmark-6.2.4-csv.out", "r") as csv_file:
        csv_data = csv_file.read()
        results_dict = redis_benchmark_from_stdout_csv_to_json(csv_data, 1, "1")
        redis_benchmark_export_logic(results_dict, [], None, {})
        # "JSON.GET jsonsl-1 .", "19920.32", "0.183", "0.104", "0.183", "0.239", "0.287", "0.351"
        assert "JSON.GET" in results_dict["Tests"]
        assert "19920.32" == results_dict["Tests"]["JSON.GET"]["rps"]
