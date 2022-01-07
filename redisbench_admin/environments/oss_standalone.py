#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import logging
import subprocess

import redis

from redisbench_admin.utils.utils import wait_for_conn, redis_server_config_module_part


def spin_up_local_redis(
    binary,
    port,
    dbdir,
    local_module_files,
    configuration_parameters=None,
    dbdir_folder=None,
    dataset_load_timeout_secs=120,
    modules_configuration_parameters_map={},
):
    command = generate_standalone_redis_server_args(
        binary,
        dbdir,
        local_module_files,
        port,
        configuration_parameters,
        modules_configuration_parameters_map,
    )

    logging.info(
        "Running local redis-server with the following args: {}".format(
            " ".join(command)
        )
    )
    redis_process = subprocess.Popen(command)
    result = wait_for_conn(redis.StrictRedis(port=port), dataset_load_timeout_secs)
    if result is True:
        logging.info("Redis available")
    return [redis_process]


def generate_standalone_redis_server_args(
    binary,
    dbdir,
    local_module_files,
    port,
    configuration_parameters=None,
    modules_configuration_parameters_map={},
):
    # start redis-server
    command = [
        binary,
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
    if local_module_files is not None:
        if type(local_module_files) == str:
            redis_server_config_module_part(
                command, local_module_files, modules_configuration_parameters_map
            )
        if type(local_module_files) == list:
            for mod in local_module_files:
                redis_server_config_module_part(
                    command, mod, modules_configuration_parameters_map
                )
    return command
