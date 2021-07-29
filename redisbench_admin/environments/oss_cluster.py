#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import logging
import os
import subprocess
from time import sleep

import redis

from redisbench_admin.utils.utils import wait_for_conn


def spin_up_local_redis_cluster(
    dbdir,
    shard_count,
    start_port,
    local_module_file,
    configuration_parameters=None,
    dbdir_folder=None,
    dataset_load_timeout_secs=60,
):
    redis_processes = []
    meet_cmds = []
    redis_conns = []
    for master_shard_id in range(1, shard_count + 1):
        shard_port = master_shard_id + start_port - 1

        command = generate_cluster_redis_server_args(
            dbdir, local_module_file, shard_port, configuration_parameters
        )

        logging.info(
            "Running local redis-server cluster with the following args: {}".format(
                " ".join(command)
            )
        )
        redis_process = subprocess.Popen(command)
        r = redis.StrictRedis(port=shard_port)
        result = wait_for_conn(r, dataset_load_timeout_secs)
        if result is True:
            logging.info("Redis available")
        meet_cmds.append("CLUSTER MEET {} {}".format("127.0.0.1", shard_port))
        redis_conns.append(r)
        redis_processes.append(redis_process)

    try:

        # redis_conns[0].execute_command("CONFIG SET cluster-enabled yes")
        for redis_conn in redis_conns:
            for cmd in meet_cmds:
                redis_conn.execute_command(cmd)

        end_exclusive_slot = 16384
        slots_per_node = int(end_exclusive_slot / shard_count)
        logging.info("Slots per node {}".format(slots_per_node))

        for n, redis_conn in enumerate(redis_conns):
            node_slots_start = int((n) * slots_per_node)
            node_slots_end_exclusive_slot = int((n + 1) * slots_per_node)
            if n == (shard_count - 1):
                node_slots_end_exclusive_slot = end_exclusive_slot
            logging.info(
                "Node {}. slots {}-{}".format(
                    n, node_slots_start, node_slots_end_exclusive_slot - 1
                )
            )
            for x in range(node_slots_start, node_slots_end_exclusive_slot):
                redis_conn.execute_command("CLUSTER ADDSLOTS {}".format(x))

        for n, redis_conn in enumerate(redis_conns):
            cluster_slots_ok = 0
            while cluster_slots_ok < end_exclusive_slot:
                cluster_slots_ok = int(
                    redis_conn.execute_command("cluster info")["cluster_slots_ok"]
                )
                logging.info(
                    "Node {}: Total cluster_slots_ok {}".format(n, cluster_slots_ok)
                )
                sleep(1)
    except redis.exceptions.RedisError as e:
        logging.warning("Received an error {}".format(e.__str__()))

    return redis_processes


def generate_cluster_redis_server_args(
    dbdir,
    local_module_file,
    port,
    configuration_parameters=None,
):
    # start redis-server
    command = [
        "redis-server",
        "--appendonly",
        "no",
        "--cluster-enabled",
        "yes",
        "--dbfilename",
        get_cluster_dbfilename(port),
        "--cluster-config-file",
        "cluster-node-port-{}.config".format(port),
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
        if type(local_module_file) == str:
            command.extend(
                [
                    "--loadmodule",
                    os.path.abspath(local_module_file),
                ]
            )
        if type(local_module_file) == list:
            for mod in local_module_file:
                command.extend(
                    [
                        "--loadmodule",
                        os.path.abspath(mod),
                    ]
                )
    return command


def get_cluster_dbfilename(port):
    return "cluster-node-port-{}.rdb".format(port)
