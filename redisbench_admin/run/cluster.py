#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import logging

import redis


def cluster_init_steps(
    args, clusterconfig, local_module_file, r_conns, shard_count, contains_dataset=True
):
    startup_nodes = []
    for p in range(args.port, args.port + shard_count):
        primary_conn = redis.StrictRedis(port=p)
        if contains_dataset:
            # force debug reload nosave to replace the current database with the contents of an existing RDB file
            primary_conn.execute_command("DEBUG RELOAD NOSAVE")
        r_conns.append(primary_conn)
        startup_nodes.append({"host": "127.0.0.1", "port": "{}".format(p)})
    if clusterconfig is not None:
        if "init_commands" in clusterconfig:
            for command_group in clusterconfig["init_commands"]:
                skip = False
                if "when_modules_present" in command_group:
                    m_found = False
                    for module_required in command_group["when_modules_present"]:
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
                        for conn_n, rc in enumerate(r_conns):
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


# noinspection PyBroadException
def spin_up_redis_cluster_remote_redis(
    benchmark_config,
    server_public_ip,
    username,
    private_key,
    local_module_file,
    remote_module_file,
    remote_dataset_file,
    logname,
    dirname,
    redis_configuration_parameters,
    dbdir_folder,
    shard_count,
    port,
):
    logging.error(
        "Remote cluster is still not implemented =(. We're working hard to get it ASAP =)!!"
    )
