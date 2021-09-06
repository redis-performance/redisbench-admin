#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import logging
import os
import tarfile
from io import BytesIO
from time import sleep

import yaml

from redisbench_admin.run_remote.consts import remote_module_file_dir
from redisbench_admin.run_remote.standalone import spin_up_standalone_remote_redis


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
    port = 22
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

    full_logfile, dataset = spin_up_standalone_remote_redis(
        benchmark_config,
        server_public_ip,
        username,
        private_key,
        local_module_files,
        remote_module_file_dir,
        None,
        logname,
        ".",
        None,
        None,
        port,
    )
    import paramiko

    k = paramiko.RSAKey.from_private_key_file(private_key)
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    logging.info("Connecting to remote server {}".format(server_public_ip))
    client.connect(hostname=server_public_ip, port=port, username=username, pkey=k)
    _, stdout, _ = client.exec_command("ls /tmp")
    stdout = [x.strip() for x in stdout.readlines()]

    # ensure we have 2 modules
    assert len(stdout) == 2
    assert "redistimeseries.so" in stdout
    assert "redistimeseries.so.1" in stdout
    client.close()
