from unittest import TestCase

from redisbench_admin.utils.utils import retrieve_local_or_remote_input_json


class Test(TestCase):
    def test_retrieve_local_or_remote_input_json(self):
        benchmark_config = retrieve_local_or_remote_input_json("./tests/test_data/redis-benchmark.6.2.results.csv", ".",
                                                               "opt", "csv", csv_header=False)
        assert benchmark_config["./tests/test_data/redis-benchmark.6.2.results.csv"]['col_0'][0] == "test"
        benchmark_config = retrieve_local_or_remote_input_json("./tests/test_data/redis-benchmark.6.0.results.csv", ".",
                                                               "opt", "csv", csv_header=False)
        assert benchmark_config["./tests/test_data/redis-benchmark.6.0.results.csv"]['col_0'][0] == "PING_INLINE"
