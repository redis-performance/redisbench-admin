#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import logging

from redisbench_admin.utils.remote import execute_remote_commands

from redisbench_admin.environments.oss_cluster import generate_cluster_redis_server_args
from redisbench_admin.utils.utils import wait_for_conn


def cluster_init_steps(clusterconfig, redis_conns, local_module_file):
    startup_nodes = generate_startup_nodes_array(redis_conns)

    if clusterconfig is not None:
        if "init_commands" in clusterconfig:
            for command_group in clusterconfig["init_commands"]:
                skip = False
                if "when_modules_present" in command_group:
                    m_found = False
                    for module_required in command_group["when_modules_present"]:
                        if local_module_file is not None:
                            if type(local_module_file) == list:
                                for local_m in local_module_file:
                                    if module_required in local_m:
                                        m_found = True
                                        logging.info(
                                            "Required module {}  found in {}".format(
                                                module_required,
                                                local_m,
                                            )
                                        )
                            else:
                                if module_required in local_module_file:
                                    m_found = True
                                    logging.info(
                                        "Required module {}  found in {}".format(
                                            module_required,
                                            local_module_file,
                                        )
                                    )
                    skip = not (m_found)
                if skip is False:
                    for command in command_group["commands"]:
                        for conn_n, rc in enumerate(redis_conns):
                            rc.execute_command(command)
                            logging.info(
                                "Cluster node {}: sent command {}".format(
                                    conn_n + 1, command
                                )
                            )
                else:
                    logging.info(
                        "Skipping to send the command group: {}.".format(
                            command_group["commands"],
                        )
                        + "Given the when_modules_present condition ({}) is not true.".format(
                            command_group["when_modules_present"],
                        )
                    )
    return startup_nodes


def debug_reload_rdb(dataset_load_timeout_secs, redis_conns):
    for primary_n, conn in enumerate(redis_conns):
        host = conn.connection_pool.connection_kwargs["host"]
        port = conn.connection_pool.connection_kwargs["port"]
        logging.info(
            "force debug reload nosave to replace the current database with the contents of an existing RDB file. Doing it for host:{} port:{}".format(
                host,
                port,
            )
        )
        # force debug reload nosave to replace the current database with the contents of an existing RDB file
        conn.execute_command("DEBUG RELOAD NOSAVE")
    for primary_n, conn in enumerate(redis_conns):
        logging.info(
            "Waiting for connection for primary #{}".format(
                primary_n,
            )
        )
        wait_for_conn(conn, dataset_load_timeout_secs, "PING", True, initial_sleep=0)


def generate_startup_nodes_array(redis_conns):
    startup_nodes = []
    for conn in redis_conns:
        logging.info(conn)
        logging.info(conn.connection_pool)
        logging.info(conn.connection_pool.connection_kwargs)
        startup_nodes.append(
            {
                "host": conn.connection_pool.connection_kwargs["host"],
                "port": "{}".format(conn.connection_pool.connection_kwargs["port"]),
            }
        )
    return startup_nodes


# noinspection PyBroadException
def spin_up_redis_cluster_remote_redis(
    server_public_ip,
    server_private_ip,
    username,
    private_key,
    remote_module_files,
    redis_configuration_parameters,
    dbdir_folder,
    shard_count,
    start_port,
    ssh_port,
    modules_configuration_parameters_map,
    logname,
    redis_7=True,
):
    logging.info("Generating the remote redis-server command arguments")
    redis_process_commands = []
    logfiles = []
    logname_prefix = logname[: len(logname) - 4] + "-"
    for master_shard_id in range(1, shard_count + 1):
        shard_port = master_shard_id + start_port - 1

        command, logfile = generate_cluster_redis_server_args(
            "redis-server",
            dbdir_folder,
            remote_module_files,
            server_private_ip,
            shard_port,
            redis_configuration_parameters,
            "yes",
            modules_configuration_parameters_map,
            logname_prefix,
            "yes",
            redis_7,
        )
        logging.error(
            "Remote primary shard {} command: {}".format(
                master_shard_id, " ".join(command)
            )
        )
        logfiles.append(logfile)
        redis_process_commands.append(" ".join(command))
    res = execute_remote_commands(
        server_public_ip, username, private_key, redis_process_commands, ssh_port
    )
    for pos, res_pos in enumerate(res):
        [recv_exit_status, stdout, stderr] = res_pos
        if recv_exit_status != 0:
            logging.error(
                "Remote primary shard {} command returned exit code {}. stdout {}. stderr {}".format(
                    pos, recv_exit_status, stdout, stderr
                )
            )

    return logfiles
