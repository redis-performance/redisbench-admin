#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import logging
import tempfile
import datetime

import redis

from redisbench_admin.run.run import calculate_client_tool_duration_and_check
from redisbench_admin.run_local.local_helpers import (
    check_benchmark_binaries_local_requirements,
    run_local_benchmark,
)

from redisbench_admin.environments.oss_cluster import (
    spin_up_local_redis_cluster,
    setup_redis_cluster_from_conns,
)
from redisbench_admin.environments.oss_standalone import spin_up_local_redis
from redisbench_admin.run.cluster import cluster_init_steps
from redisbench_admin.run.common import (
    run_redis_pre_steps,
    check_dbconfig_tool_requirement,
    prepare_benchmark_parameters,
    dbconfig_keyspacelen_check,
)
from redisbench_admin.utils.benchmark_config import extract_redis_dbconfig_parameters
from redisbench_admin.utils.local import (
    check_dataset_local_requirements,
    is_process_alive,
)


def local_db_spin(
    binary,
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
    flushall_on_every_test_start=False,
    ignore_keyspace_errors=False,
):
    redis_conns = []
    artifact_version = "n/a"
    result = True
    temporary_dir = tempfile.mkdtemp()
    cluster_api_enabled = False
    if setup_type == "oss-cluster":
        cluster_api_enabled = True
    dataset, dataset_name, _, _ = check_dataset_local_requirements(
        benchmark_config,
        temporary_dir,
        dirname,
        "./datasets",
        "dbconfig",
        shard_count,
        cluster_api_enabled,
    )

    if args.skip_db_setup:
        logging.info("Skipping DB Setup...")
        if dataset is not None:
            logging.info("Given this benchmark requires an RDB load will skip it...")
            result = False
            return (
                result,
                artifact_version,
                cluster_api_enabled,
                redis_conns,
                redis_processes,
            )
    else:
        if args.skip_redis_spin is False:
            # setup Redis
            # copy the rdb to DB machine
            redis_7 = args.redis_7
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
                modules_configuration_parameters_map,
            ) = extract_redis_dbconfig_parameters(benchmark_config, "dbconfig")

            logging.info(
                "Using a dataset load timeout of {} seconds.".format(
                    dataset_load_timeout_secs
                )
            )

            if setup_type == "oss-cluster":
                cluster_api_enabled = True
                redis_processes, redis_conns = spin_up_local_redis_cluster(
                    binary,
                    temporary_dir,
                    shard_count,
                    args.host,
                    args.port,
                    local_module_file,
                    redis_configuration_parameters,
                    dataset_load_timeout_secs,
                    modules_configuration_parameters_map,
                    redis_7,
                )

                status = setup_redis_cluster_from_conns(
                    redis_conns, shard_count, args.host, args.port
                )
                if status is False:
                    raise Exception("Redis cluster setup failed. Failing test.")

            if setup_type == "oss-standalone":
                redis_processes = spin_up_local_redis(
                    binary,
                    args.port,
                    temporary_dir,
                    local_module_file,
                    redis_configuration_parameters,
                    dbdir_folder,
                    dataset_load_timeout_secs,
                    modules_configuration_parameters_map,
                    redis_7,
                )
            if setup_type == "oss-cluster":
                for shardn, redis_process in enumerate(redis_processes):
                    logging.info(
                        "Checking if shard #{} process with pid={} is alive".format(
                            shardn + 1, redis_process.pid
                        )
                    )
                    if is_process_alive(redis_process) is False:
                        raise Exception("Redis process is not alive. Failing test.")
                cluster_init_steps(clusterconfig, redis_conns, local_module_file)
        else:
            logging.info("Skipping DB spin step...")

    if setup_type == "oss-standalone":
        r = redis.Redis(port=args.port, host=args.host)
        r.ping()
        r.client_setname("redisbench-admin-standalone")
        redis_conns.append(r)

    if dataset is None:
        if flushall_on_every_test_start:
            logging.info("Will flush all data at test start...")
            for shard_n, shard_conn in enumerate(redis_conns):
                logging.info(f"Flushing all in shard {shard_n}...")
                shard_conn.flushall()

    if check_dbconfig_tool_requirement(benchmark_config):
        logging.info("Detected the requirements to load data via client tool")
        local_benchmark_output_filename = "{}/load-data.txt".format(temporary_dir)
        (
            benchmark_tool,
            full_benchmark_path,
            benchmark_tool_workdir,
        ) = check_benchmark_binaries_local_requirements(
            benchmark_config, args.allowed_tools, "./binaries", "dbconfig"
        )

        # prepare the benchmark command
        command, command_str = prepare_benchmark_parameters(
            benchmark_config,
            full_benchmark_path,
            args.port,
            args.host,
            local_benchmark_output_filename,
            False,
            benchmark_tool_workdir,
            cluster_api_enabled,
            "dbconfig",
        )

        # run the benchmark
        load_via_benchmark_start_time = datetime.datetime.now()
        run_local_benchmark(benchmark_tool, command)
        load_via_benchmark_end_time = datetime.datetime.now()
        load_via_benchmark_duration_seconds = calculate_client_tool_duration_and_check(
            load_via_benchmark_end_time, load_via_benchmark_start_time
        )
        logging.info(
            "Loading data via benchmark tool took {} secs.".format(
                load_via_benchmark_duration_seconds
            )
        )

    dbconfig_keyspacelen_check(benchmark_config, redis_conns, ignore_keyspace_errors)

    artifact_version = run_redis_pre_steps(
        benchmark_config, redis_conns[0], required_modules
    )

    return result, artifact_version, cluster_api_enabled, redis_conns, redis_processes
