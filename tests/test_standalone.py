#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import logging
import os
from io import BytesIO
from time import sleep

import yaml

from redisbench_admin.run_remote.standalone import (
    spin_up_standalone_remote_redis,
)


def get_test_data_module():
    LD_LIBRARY_PATH = "/usr/lib/redis/modules"
    module_file = "./tests/test_data/redistimeseries.so"
    container_name = "redislabs/redistimeseries:edge"
    shared_lib = "redistimeseries.so"
    if not os.path.exists(module_file):
        logging.info(
            "Using docker image {} t retrieve module file {} to {}.".format(
                container_name, shared_lib, module_file
            )
        )
        import docker

        client = docker.from_env()
        container = client.containers.run(container_name, detach=True)
        sleep(5)
        module_file_stream, file_stats = container.get_archive(
            "{}/{}".format(LD_LIBRARY_PATH, shared_lib)
        )
        file_obj = BytesIO()
        for i in module_file_stream:
            file_obj.write(i)
        file_obj.seek(0)
        with open(module_file, "wb") as fo:
            fo.write(file_obj.read())
        container.stop()
    return module_file


def test_spin_up_standalone_remote_redis():
    port = 2222
    username = "ubuntu"
    private_key = "./tests/test_data/test-ssh/tox_rsa"
    server_public_ip = "localhost"
    module_file = get_test_data_module()
    from shutil import copyfile

    module2 = "{}.1".format(module_file)
    copyfile(module_file, module2)
    local_module_files = [module_file, module2]
    logname = "test_spin_up_standalone_remote_redis.log"
    with open("./tests/test_data/redis-benchmark-vanilla.yml", "r") as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
    temporary_dir = "/tmp"
    full_logfile = spin_up_standalone_remote_redis(
        temporary_dir,
        server_public_ip,
        username,
        private_key,
        None,
        logname,
        None,
        port,
    )
