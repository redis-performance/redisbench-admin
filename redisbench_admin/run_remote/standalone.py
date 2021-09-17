#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
#
import logging
import os

from redisbench_admin.utils.remote import (
    copy_file_to_remote_setup,
    execute_remote_commands,
)
from redisbench_admin.utils.ssh import SSHSession


def spin_up_standalone_remote_redis(
    temporary_dir,
    server_public_ip,
    username,
    private_key,
    remote_module_files,
    logfile,
    redis_configuration_parameters=None,
    port=22,
):

    full_logfile, initial_redis_cmd = generate_remote_standalone_redis_cmd(
        logfile, redis_configuration_parameters, remote_module_files, temporary_dir
    )

    # start redis-server
    commands = [initial_redis_cmd]
    execute_remote_commands(server_public_ip, username, private_key, commands, port)
    return full_logfile


def cp_local_dbdir_to_remote(
    dbdir_folder, private_key, server_public_ip, temporary_dir, username
):
    if dbdir_folder is not None:
        logging.info(
            "Copying entire content of {} into temporary path: {}".format(
                dbdir_folder, temporary_dir
            )
        )
        ssh = SSHSession(server_public_ip, username, key_file=open(private_key, "r"))
        ssh.put_all(dbdir_folder, temporary_dir)


def remote_module_files_cp(
    local_module_files,
    port,
    private_key,
    remote_module_file_dir,
    server_public_ip,
    username,
):
    remote_module_files = []
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
            remote_module_files.append(remote_module_file)
    return remote_module_files


def generate_remote_standalone_redis_cmd(
    logfile, redis_configuration_parameters, remote_module_files, temporary_dir
):
    initial_redis_cmd = 'redis-server --save "" --logfile {} --dir {} --daemonize yes --protected-mode no'.format(
        logfile, temporary_dir
    )
    full_logfile = "{}/{}".format(temporary_dir, logfile)
    if redis_configuration_parameters is not None:
        for (
            configuration_parameter,
            configuration_value,
        ) in redis_configuration_parameters.items():
            initial_redis_cmd += " --{} {}".format(
                configuration_parameter, configuration_value
            )
    if remote_module_files is not None:
        for remote_module_file in remote_module_files:
            initial_redis_cmd += " --loadmodule {}".format(remote_module_file)
    return full_logfile, initial_redis_cmd
