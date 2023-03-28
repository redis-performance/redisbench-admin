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
    generate_remote_standalone_redis_cmd,
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
    db_server_ip = os.getenv("DB_SERVER_HOST", None)
    if db_server_ip is None:
        return

    logname = "test_spin_up_standalone_remote_redis.log"
    temporary_dir = "/tmp"
    spin_up_standalone_remote_redis(
        temporary_dir,
        db_server_ip,
        username,
        private_key,
        None,
        logname,
        None,
        port,
    )


def test_generate_remote_standalone_redis_cmd():
    modules_configuration_parameters_map = {"m1": {"CHUNK_SIZE_BYTES": 128}}
    full_logfile, initial_redis_cmd = generate_remote_standalone_redis_cmd(
        "log1", None, ["m1.so"], ".", modules_configuration_parameters_map, False
    )
    assert initial_redis_cmd.endswith("m1.so CHUNK_SIZE_BYTES 128")

    # 2 modules
    modules_configuration_parameters_map = {"m1": {"CHUNK_SIZE_BYTES": 128}}
    full_logfile, initial_redis_cmd = generate_remote_standalone_redis_cmd(
        "log1",
        None,
        ["m1.so", "m2.so"],
        ".",
        modules_configuration_parameters_map,
        False,
    )
    assert initial_redis_cmd.endswith("m2.so")
