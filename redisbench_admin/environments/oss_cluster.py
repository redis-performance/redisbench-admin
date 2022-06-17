#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import logging
import subprocess
from time import sleep

import redis

from redisbench_admin.utils.utils import (
    wait_for_conn,
    redis_server_config_module_part,
    generate_common_server_args,
)


def spin_up_local_redis_cluster(
    binary,
    dbdir,
    shard_count,
    ip,
    start_port,
    local_module_file,
    configuration_parameters=None,
    dataset_load_timeout_secs=60,
    modules_configuration_parameters_map={},
    redis_7=True,
):
    redis_processes = []
    redis_conns = []

    for master_shard_id in range(1, shard_count + 1):
        shard_port = master_shard_id + start_port - 1
        binary = binary
        if master_shard_id > 1:
            binary = "redis-server"
            logging.info("Ignoring redis binary definition for primary shard > 1")
        command, _ = generate_cluster_redis_server_args(
            binary,
            dbdir,
            local_module_file,
            ip,
            shard_port,
            configuration_parameters,
            "no",
            modules_configuration_parameters_map,
            None,
            "yes",
            redis_7,
        )

        logging.info(
            "Running local redis-server cluster with the following args: {}".format(
                " ".join(command)
            )
        )
        redis_process = subprocess.Popen(command)
        r = redis.Redis(port=shard_port)
        result = wait_for_conn(r, dataset_load_timeout_secs)
        if result is True:
            logging.info("Redis available. pid={}".format(redis_process.pid))
            r.client_setname("redisbench-admin-cluster-#{}".format(master_shard_id))
        redis_conns.append(r)
        redis_processes.append(redis_process)
    return redis_processes, redis_conns


def setup_redis_cluster_from_conns(redis_conns, shard_count, shard_host, start_port):
    logging.info("Setting up cluster. Total {} primaries.".format(len(redis_conns)))
    meet_cmds = generate_meet_cmds(shard_count, shard_host, start_port)
    status = setup_oss_cluster_from_conns(meet_cmds, redis_conns, shard_count)
    if status is True:
        for conn in redis_conns:
            conn.execute_command("CLUSTER SAVECONFIG")
    return status


def generate_meet_cmds(shard_count, shard_host, start_port):
    meet_cmds = []

    for master_shard_id in range(1, shard_count + 1):
        shard_port = master_shard_id + start_port - 1
        meet_cmds.append("CLUSTER MEET {} {}".format(shard_host, shard_port))
    return meet_cmds


def setup_oss_cluster_from_conns(meet_cmds, redis_conns, shard_count):
    status = False
    try:
        for primary_pos, redis_conn in enumerate(redis_conns):
            logging.info(
                "Sending to primary #{} a total of {} MEET commands".format(
                    primary_pos, len(meet_cmds)
                )
            )
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
            shard_slots_str = " ".join(
                [
                    "{}".format(x)
                    for x in range(node_slots_start, node_slots_end_exclusive_slot)
                ]
            )
            redis_conn.execute_command("CLUSTER ADDSLOTS {}".format(shard_slots_str))

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
        for n, redis_conn in enumerate(redis_conns):
            cluster_state_ok = False
            while cluster_state_ok is False:
                cluster_state_ok = bool(
                    redis_conn.execute_command("cluster info")["cluster_state"]
                )
                logging.info("Node {}: cluster_state {}".format(n, cluster_state_ok))
                sleep(1)
        status = True
    except redis.exceptions.RedisError as e:
        logging.warning("Received an error {}".format(e.__str__()))
        status = False
    return status


def generate_cluster_redis_server_args(
    binary,
    dbdir,
    local_module_file,
    ip,
    port,
    configuration_parameters=None,
    daemonize="yes",
    modules_configuration_parameters_map={},
    logname_prefix=None,
    enable_debug_command="yes",
    enable_redis_7_config_directives=False,
):
    if logname_prefix is None:
        logname_prefix = ""
    logfile = "{}cluster-node-port-{}.log".format(logname_prefix, port)
    dbfilename = get_cluster_dbfilename(port)

    command = generate_common_server_args(
        binary, daemonize, dbdir, dbfilename, enable_debug_command, ip, logfile, port
    )
    command.extend(
        [
            "--cluster-enabled",
            "yes",
            "--cluster-config-file",
            "cluster-node-port-{}.config".format(port),
            "--cluster-announce-ip",
            "{}".format(ip),
        ]
    )
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
            redis_server_config_module_part(
                command, local_module_file, modules_configuration_parameters_map
            )
        if type(local_module_file) == list:
            for mod in local_module_file:
                redis_server_config_module_part(
                    command, mod, modules_configuration_parameters_map
                )
    return command, logfile


def get_cluster_dbfilename(port):
    return "cluster-node-port-{}.rdb".format(port)
