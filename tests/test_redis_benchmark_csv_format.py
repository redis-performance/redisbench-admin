from unittest import TestCase

from redisbench_admin.export.redis_benchmark.redis_benchmark_csv_format import fill_tags_from_passed_array


class Test(TestCase):
    def test_fill_tags_from_passed_array(self):
        kv_array = [{"git_sha": "a331"},
                    ]
        deployment_type, git_sha, project, project_version, run_stage = fill_tags_from_passed_array(kv_array)
        assert git_sha == "a331"
        assert deployment_type == None
