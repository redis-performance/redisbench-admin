#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
#
import logging
import os

from redisbench_admin.utils.remote import (
    check_dataset_remote_requirements,
    copy_file_to_remote_setup,
    execute_remote_commands,
)
from redisbench_admin.utils.ssh import SSHSession


def spin_up_standalone_remote_redis(
    benchmark_config,
    server_public_ip,
    username,
    private_key,
    local_module_files,
    remote_module_file_dir,
    remote_dataset_file,
    logfile,
    dirname=".",
    redis_configuration_parameters=None,
    dbdir_folder=None,
    port=22,
):
    # copy the rdb to DB machine
    _, dataset, _, _ = check_dataset_remote_requirements(
        benchmark_config,
        server_public_ip,
        username,
        private_key,
        remote_dataset_file,
        dirname,
        1,
        False,
    )
    temporary_dir = "/tmp"
    initial_redis_cmd = 'redis-server --save "" --logfile {} --dir {} --daemonize yes --protected-mode no'.format(
        logfile, temporary_dir
    )
    full_logfile = "{}/{}".format(temporary_dir, logfile)
    if dbdir_folder is not None:
        logging.info(
            "Copying entire content of {} into temporary path: {}".format(
                dbdir_folder, temporary_dir
            )
        )
        ssh = SSHSession(server_public_ip, username, key_file=open(private_key, "r"))
        ssh.put_all(dbdir_folder, temporary_dir)

    if redis_configuration_parameters is not None:
        for (
            configuration_parameter,
            configuration_value,
        ) in redis_configuration_parameters.items():
            initial_redis_cmd += " --{} {}".format(
                configuration_parameter, configuration_value
            )
    if local_module_files is not None:
        for local_module_file in local_module_files:
            remote_module_file = "{}/{}".format(
                remote_module_file_dir, os.path.basename(local_module_file)
            )
            # copy the module to the DB machine
            copy_file_to_remote_setup(
                server_public_ip,
                username,
                private_key,
                local_module_file,
                remote_module_file,
                None,
                port,
            )
            execute_remote_commands(
                server_public_ip,
                username,
                private_key,
                ["chmod 755 {}".format(remote_module_file)],
                port,
            )
            initial_redis_cmd += " --loadmodule {}".format(remote_module_file)
    # start redis-server
    commands = [initial_redis_cmd]
    execute_remote_commands(server_public_ip, username, private_key, commands, port)
    return full_logfile, dataset
