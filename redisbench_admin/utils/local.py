#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#

import logging
import os
import subprocess
from shutil import copyfile

import redis
import wget

from redisbench_admin.utils.utils import wait_for_conn


def check_dataset_local_requirements(
    benchmark_config,
    redis_dbdir,
    dirname=None,
    datasets_localtemp_dir="./datasets",
    dbconfig_keyname="dbconfig",
):
    dataset = None
    full_path = None
    tmp_path = None
    if dbconfig_keyname in benchmark_config:
        for k in benchmark_config[dbconfig_keyname]:
            if "dataset" in k:
                dataset = k["dataset"]
        if dataset is not None:
            full_path = check_if_needs_remote_fetch(
                dataset, datasets_localtemp_dir, dirname
            )
            tmp_path = "{}/dump.rdb".format(redis_dbdir)
            logging.info(
                "Copying rdb from {} to {}/dump.rdb".format(full_path, redis_dbdir)
            )
            copyfile(full_path, tmp_path)
    return dataset, full_path, tmp_path


def check_if_needs_remote_fetch(
    property, localtemp_dir, dirname, full_path=None, is_remote=False
):
    if property.startswith("http"):
        if not os.path.isdir(localtemp_dir):
            os.mkdir(localtemp_dir)
        if full_path is None:
            filename = property.split("/")[-1]
            full_path = "{}/{}".format(localtemp_dir, filename)
        if not os.path.exists(full_path) and is_remote is False:
            logging.info(
                "Retrieving remote file from {} to {}. Using the dir {} as a cache for next time.".format(
                    property, full_path, localtemp_dir
                )
            )
            wget.download(property, full_path)
        else:
            logging.info(
                "Reusing cached remote file (located at {} ).".format(full_path)
            )
    else:
        full_path = property
        if dirname is not None:
            full_path = "{}/{}".format(dirname, full_path)

    return full_path


def spin_up_local_redis(
    dbdir,
    port,
    local_module_file,
    configuration_parameters=None,
):
    command = generate_standalone_redis_server_args(
        dbdir, local_module_file, port, configuration_parameters
    )

    logging.info(
        "Running local redis-server with the following args: {}".format(
            " ".join(command)
        )
    )
    redis_process = subprocess.Popen(command)
    result = wait_for_conn(redis.StrictRedis(port=port))
    if result is True:
        logging.info("Redis available")
    return redis_process


def generate_standalone_redis_server_args(
    dbdir, local_module_file, port, configuration_parameters=None
):
    # start redis-server
    command = [
        "redis-server",
        "--save",
        '""',
        "--port",
        "{}".format(port),
        "--dir",
        dbdir,
    ]
    if configuration_parameters is not None:
        for parameter, parameter_value in configuration_parameters.items():
            command.extend(
                [
                    "--{}".format(parameter),
                    parameter_value,
                ]
            )
    if local_module_file is not None:
        command.extend(
            [
                "--loadmodule",
                os.path.abspath(local_module_file),
            ]
        )
    return command


def is_process_alive(process):
    if not process:
        return False
    # Check if child process has terminated. Set and return returncode
    # attribute
    if process.poll() is None:
        return True
    return False


def get_local_run_full_filename(
    start_time_str,
    github_branch,
    test_name,
):
    benchmark_output_filename = (
        "{start_time_str}-{github_branch}-{test_name}.json".format(
            start_time_str=start_time_str,
            github_branch=github_branch,
            test_name=test_name,
        )
    )
    return benchmark_output_filename
