#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import logging
import tempfile

import redis

from redisbench_admin.environments.oss_cluster import (
    spin_up_local_redis_cluster,
    setup_redis_cluster_from_conns,
)
from redisbench_admin.environments.oss_standalone import spin_up_local_redis
from redisbench_admin.run.cluster import cluster_init_steps
from redisbench_admin.run.common import run_redis_pre_steps
from redisbench_admin.utils.benchmark_config import extract_redis_dbconfig_parameters
from redisbench_admin.utils.local import (
    check_dataset_local_requirements,
    is_process_alive,
)


def local_db_spin(
    args,
    benchmark_config,
    clusterconfig,
    dbdir_folder,
    dirname,
    local_module_file,
    redis_processes,
    required_modules,
    setup_type,
    shard_count,
):
    # setup Redis
    # copy the rdb to DB machine
    temporary_dir = tempfile.mkdtemp()
    logging.info(
        "Using local temporary dir to spin up Redis Instance. Path: {}".format(
            temporary_dir
        )
    )
    if dbdir_folder is not None:
        from distutils.dir_util import copy_tree

        copy_tree(dbdir_folder, temporary_dir)
        logging.info(
            "Copied entire content of {} into temporary path: {}".format(
                dbdir_folder, temporary_dir
            )
        )
    (
        _,
        _,
        redis_configuration_parameters,
        dataset_load_timeout_secs,
    ) = extract_redis_dbconfig_parameters(benchmark_config, "dbconfig")
    cluster_api_enabled = False
    logging.info(
        "Using a dataset load timeout of {} seconds.".format(dataset_load_timeout_secs)
    )
    redis_conns = []
    if setup_type == "oss-cluster":
        cluster_api_enabled = True
        shard_host = "127.0.0.1"
        redis_processes, redis_conns = spin_up_local_redis_cluster(
            temporary_dir,
            shard_count,
            shard_host,
            args.port,
            local_module_file,
            redis_configuration_parameters,
            dataset_load_timeout_secs,
        )

        status = setup_redis_cluster_from_conns(
            redis_conns, shard_count, shard_host, args.port
        )
        if status is False:
            raise Exception("Redis cluster setup failed. Failing test.")

    dataset, _, _ = check_dataset_local_requirements(
        benchmark_config,
        temporary_dir,
        dirname,
        "./datasets",
        "dbconfig",
        shard_count,
        cluster_api_enabled,
    )
    if setup_type == "oss-standalone":
        redis_processes = spin_up_local_redis(
            "redis-server",
            args.port,
            temporary_dir,
            local_module_file,
            redis_configuration_parameters,
            dbdir_folder,
            dataset_load_timeout_secs,
        )

        for redis_process in redis_processes:
            if is_process_alive(redis_process) is False:
                raise Exception("Redis process is not alive. Failing test.")

        r = redis.StrictRedis(port=args.port)
        redis_conns.append(r)
    if setup_type == "oss-cluster":

        cluster_init_steps(clusterconfig, redis_conns, local_module_file)

    run_redis_pre_steps(benchmark_config, redis_conns[0], required_modules)
    return cluster_api_enabled, redis_conns, redis_processes
