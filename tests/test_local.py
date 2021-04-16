import os
import shutil
import tempfile

import redis

from redisbench_admin.utils.local import (
    check_dataset_local_requirements,
    generate_standalone_redis_server_args,
    spin_up_local_redis,
)

#
# def test_check_dataset_local_requirements():
#     url = "https://s3.amazonaws.com/benchmarks.redislabs/redistimeseries/tsbs/datasets/devops/functional/scale-100-redistimeseries_data.rdb"
#     # no db config
#     check_dataset_local_requirements({}, ".", ".")
#     # dbconfig with no dataset key
#     check_dataset_local_requirements({"dbconfig": {}}, ".", ".")
#
#     # dbconfig with local filename
#     directory_from = tempfile.mkdtemp()
#     directory_to = tempfile.mkdtemp()
#     temp_file1 = tempfile.NamedTemporaryFile(dir=directory_from)
#     check_dataset_local_requirements({"dbconfig": [{"dataset": temp_file1.name}]}, directory_to.__str__(), None)
#     assert os.path.exists("{}/{}".format(directory_from.__str__(), temp_file1.name.split("/")[-1]))
#     assert os.path.exists("{}/{}".format(directory_to.__str__(), "dump.rdb"))
#     shutil.rmtree(directory_from)
#     shutil.rmtree(directory_to)
#
#     # dbconfig with remote filename
#     tests_remote_tmp_datasets = "./tests/temp-datasets"
#     if os.path.isdir(tests_remote_tmp_datasets):
#         shutil.rmtree(tests_remote_tmp_datasets)
#
#     directory_to = tempfile.mkdtemp()
#     check_dataset_local_requirements({"dbconfig": [{"dataset": url}]}, directory_to.__str__(), None,
#                                   tests_remote_tmp_datasets)
#     assert os.path.exists("{}/{}".format(directory_to.__str__(), "dump.rdb"))
#     assert os.path.exists("{}/{}".format(tests_remote_tmp_datasets, "scale-100-redistimeseries_data.rdb"))
#     check_dataset_local_requirements({"dbconfig": [{"dataset": url}]}, directory_to.__str__(), None,
#                                   tests_remote_tmp_datasets)
#     assert os.path.exists("{}/{}".format(directory_to.__str__(), "dump.rdb"))
#     shutil.rmtree(directory_to)
#     if os.path.isdir(tests_remote_tmp_datasets):
#         shutil.rmtree(tests_remote_tmp_datasets)


def test_generate_standalone_redis_server_args():
    cmd = generate_standalone_redis_server_args(".", None, "9999")
    assert cmd == ["redis-server", "--save", '""', "--port", "9999", "--dir", "."]
    local_module_file = "m1.so"
    cmd = generate_standalone_redis_server_args(".", local_module_file, "1010")
    assert cmd == [
        "redis-server",
        "--save",
        '""',
        "--port",
        "1010",
        "--dir",
        ".",
        "--loadmodule",
        os.path.abspath(local_module_file),
    ]


def test_spin_up_local_redis():
    if shutil.which("redis-server"):
        port = 9999
        spin_up_local_redis(".", port, None)
        r = redis.Redis(host="localhost", port=port)
        assert r.ping() == True
