import os
import yaml

from redisbench_admin.run_local.local_helpers import (
    check_benchmark_binaries_local_requirements,
)


def test_check_benchmark_binaries_local_requirements():
    filename = "ycsb-redisearch-binding-0.18.0-SNAPSHOT.tar.gz"
    inner_foldername = "ycsb-redisearch-binding-0.18.0-SNAPSHOT"
    binaries_localtemp_dir = "./binaries"
    with open("./tests/test_data/ycsb-config.yml", "r") as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        (
            benchmark_tool,
            which_benchmark_tool,
            benchmark_tool_workdir,
        ) = check_benchmark_binaries_local_requirements(
            benchmark_config, "ycsb", binaries_localtemp_dir
        )
        assert which_benchmark_tool == os.path.abspath(
            "./binaries/ycsb-redisearch-binding-0.18.0-SNAPSHOT/bin/ycsb"
        )
        assert benchmark_tool_workdir == os.path.abspath(
            "./binaries/ycsb-redisearch-binding-0.18.0-SNAPSHOT"
        )
        assert benchmark_tool == "ycsb"
