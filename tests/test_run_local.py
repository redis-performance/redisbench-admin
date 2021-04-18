import os
import tarfile
from io import BytesIO
from time import sleep

import yaml

from redisbench_admin.run_local.run_local import (
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

#
# def test_run_local_command_logic():
#     LD_LIBRARY_PATH = "/usr/lib/redis/modules"
#     import docker
#
#     client = docker.from_env()
#     container = client.containers.run("redislabs/redistimeseries:edge", detach=True)
#     sleep(5)
#     module_file_stream, file_stats = container.get_archive(
#         "{}/{}".format(LD_LIBRARY_PATH, "redistimeseries.so")
#     )
#     file_obj = BytesIO()
#     for i in module_file_stream:
#         file_obj.write(i)
#     file_obj.seek(0)
#     with open("test.so", "wb") as fo:
#         fo.write(file_obj.read())
#     container.stop()
