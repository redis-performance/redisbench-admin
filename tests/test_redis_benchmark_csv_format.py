from unittest import TestCase

from redisbench_admin.export.redis_benchmark.redis_benchmark_csv_format import (
    fill_tags_from_passed_array, redis_benchmark_export_logic,
)
from redisbench_admin.run.redis_benchmark.redis_benchmark import redis_benchmark_from_stdout_csv_to_json


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
        redis_benchmark_export_logic(results_dict,[],None,{},False)
