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
    redis_7=True,
):
    full_logfile, initial_redis_cmd = generate_remote_standalone_redis_cmd(
        logfile,
        redis_configuration_parameters,
        remote_module_files,
        temporary_dir,
        modules_configuration_parameters_map,
        redis_7,
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
    continue_on_module_check_error=False,
):
    remote_module_files = []
    if local_module_files is not None:
        for local_module_file in local_module_files:
            splitted_module_and_plugins = []
            if type(local_module_file) is str:
                splitted_module_and_plugins = local_module_file.split(" ")
            if type(local_module_file) is list:
                splitted_module_and_plugins = local_module_file
            if len(splitted_module_and_plugins) > 1:
                logging.info(
                    "Detected a module and plugin(s) pairs {}".format(
                        splitted_module_and_plugins
                    )
                )
            abs_splitted_module_and_plugins = [
                os.path.abspath(x) for x in splitted_module_and_plugins
            ]
            remote_module_files_in = ""
            for pos, local_module_file_and_plugin in enumerate(
                abs_splitted_module_and_plugins, start=1
            ):
                file_basename = os.path.basename(local_module_file_and_plugin)
                remote_module_file = "{}/{}".format(
                    remote_module_file_dir,
                    file_basename,
                )
                logging.info(
                    "remote_module_file: {}. basename: {}".format(
                        remote_module_file, file_basename
                    )
                )
                # copy the module to the DB machine
                cp_res = copy_file_to_remote_setup(
                    server_public_ip,
                    username,
                    private_key,
                    local_module_file_and_plugin,
                    remote_module_file,
                    None,
                    port,
                    continue_on_module_check_error,
                )
                if cp_res:
                    execute_remote_commands(
                        server_public_ip,
                        username,
                        private_key,
                        ["chmod 755 {}".format(remote_module_file)],
                        port,
                    )
                else:
                    # If the copy was unsuccessful restore path to original basename
                    remote_module_file = file_basename
                    logging.info(
                        "Given the copy was unsuccessful restore path to original basename: {}.".format(
                            remote_module_file
                        )
                    )
                if pos > 1:
                    remote_module_files_in = remote_module_files_in + " "
                remote_module_files_in = remote_module_files_in + remote_module_file
        remote_module_files.append(remote_module_files_in)
    logging.info(
        "There are a total of {} remote files {}".format(
            len(remote_module_files), remote_module_files
        )
    )
    return remote_module_files


def generate_remote_standalone_redis_cmd(
    logfile,
    redis_configuration_parameters,
    remote_module_files,
    temporary_dir,
    modules_configuration_parameters_map,
    enable_redis_7_config_directives=True,
    enable_debug_command="yes",
):
    initial_redis_cmd = "redis-server --save '' --logfile {} --dir {} --daemonize yes --protected-mode no ".format(
        logfile, temporary_dir
    )
    if enable_redis_7_config_directives:
        extra_str = " --enable-debug-command {} ".format(enable_debug_command)
        initial_redis_cmd = initial_redis_cmd + extra_str
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
            logging.info(
                "There are a total of {} modules".format(len(remote_module_files))
            )
            for mod in remote_module_files:
                redis_server_config_module_part(
                    command, mod, modules_configuration_parameters_map
                )
    if remote_module_files is not None:
        initial_redis_cmd += " " + " ".join(command)
    return full_logfile, initial_redis_cmd
