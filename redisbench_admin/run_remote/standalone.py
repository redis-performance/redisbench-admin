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
from redisbench_admin.utils.utils import redis_server_config_module_part


def spin_up_standalone_remote_redis(
    temporary_dir,
    server_public_ip,
    username,
    private_key,
    remote_module_files,
    logfile,
    redis_configuration_parameters=None,
    port=22,
    modules_configuration_parameters_map={},
):

    full_logfile, initial_redis_cmd = generate_remote_standalone_redis_cmd(
        logfile,
        redis_configuration_parameters,
        remote_module_files,
        temporary_dir,
        modules_configuration_parameters_map,
    )

    # start redis-server
    commands = [initial_redis_cmd]
    res = execute_remote_commands(
        server_public_ip, username, private_key, commands, port
    )
    for pos, res_pos in enumerate(res):
        [recv_exit_status, stdout, stderr] = res_pos
        if recv_exit_status != 0:
            logging.error(
                "Remote primary shard {} command returned exit code {}. stdout {}. stderr {}".format(
                    pos, recv_exit_status, stdout, stderr
                )
            )
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
    logfile,
    redis_configuration_parameters,
    remote_module_files,
    temporary_dir,
    modules_configuration_parameters_map,
):
    initial_redis_cmd = "redis-server --save '' --logfile {} --dir {} --daemonize yes --protected-mode no".format(
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
    command = []
    if remote_module_files is not None:
        if type(remote_module_files) == str:
            redis_server_config_module_part(
                command, remote_module_files, modules_configuration_parameters_map
            )
        if type(remote_module_files) == list:
            for mod in remote_module_files:
                redis_server_config_module_part(
                    command, mod, modules_configuration_parameters_map
                )
    if remote_module_files is not None:
        initial_redis_cmd += " " + " ".join(command)
    return full_logfile, initial_redis_cmd
