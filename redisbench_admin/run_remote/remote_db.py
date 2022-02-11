#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import datetime
import logging

import redis

from redisbench_admin.environments.oss_cluster import setup_redis_cluster_from_conns
from redisbench_admin.run.cluster import (
    spin_up_redis_cluster_remote_redis,
    debug_reload_rdb,
    cluster_init_steps,
)
from redisbench_admin.run.common import (
    check_dbconfig_tool_requirement,
    get_start_time_vars,
    dbconfig_keyspacelen_check,
    run_redis_pre_steps,
)
from redisbench_admin.run.ssh import ssh_tunnel_redisconn
from redisbench_admin.run_remote.consts import (
    remote_module_file_dir,
    remote_dataset_folder,
)
from redisbench_admin.run_remote.remote_client import run_remote_client_tool
from redisbench_admin.run_remote.remote_failures import failed_remote_run_artifact_store
from redisbench_admin.run_remote.standalone import (
    cp_local_dbdir_to_remote,
    remote_module_files_cp,
    spin_up_standalone_remote_redis,
)
from redisbench_admin.utils.benchmark_config import extract_redis_dbconfig_parameters
from redisbench_admin.utils.remote import (
    execute_remote_commands,
    check_dataset_remote_requirements,
    get_run_full_filename,
)


def remote_tmpdir_prune(
    server_public_ip, ssh_port, temporary_dir, username, private_key
):
    execute_remote_commands(
        server_public_ip,
        username,
        private_key,
        [
            "rm -rf {}/*.log".format(temporary_dir),
            "rm -rf {}/*.config".format(temporary_dir),
            "rm -rf {}/*.rdb".format(temporary_dir),
            "rm -rf {}/*.out".format(temporary_dir),
            "rm -rf {}/*.data".format(temporary_dir),
            "pkill -9 redis-server",
        ],
        ssh_port,
    )


def remote_db_spin(
    allowed_tools,
    benchmark_config,
    client_public_ip,
    clusterconfig,
    dbdir_folder,
    dirname,
    local_module_files,
    logname,
    required_modules,
    return_code,
    server_plaintext_port,
    server_private_ip,
    server_public_ip,
    setup_name,
    setup_type,
    shard_count,
    db_ssh_port,
    client_ssh_port,
    temporary_dir,
    test_name,
    testcase_start_time_str,
    tf_github_branch,
    tf_github_org,
    tf_github_repo,
    tf_github_sha,
    username,
    private_key,
    s3_bucket_name,
    s3_bucket_path,
):
    (
        _,
        _,
        redis_configuration_parameters,
        dataset_load_timeout_secs,
        modules_configuration_parameters_map,
    ) = extract_redis_dbconfig_parameters(benchmark_config, "dbconfig")

    cluster_start_port = 20000
    full_logfiles = []
    cluster_enabled = False
    if setup_type == "oss-cluster":
        cluster_enabled = True
    cp_local_dbdir_to_remote(
        dbdir_folder,
        private_key,
        server_public_ip,
        temporary_dir,
        username,
    )
    logging.info("Checking if there are modules we need to cp to remote host...")
    remote_module_files = remote_module_files_cp(
        local_module_files,
        db_ssh_port,
        private_key,
        remote_module_file_dir,
        server_public_ip,
        username,
    )
    # setup Redis
    redis_setup_result = True
    redis_conns = []
    topology_setup_start_time = datetime.datetime.now()
    if setup_type == "oss-cluster":
        logfiles = spin_up_redis_cluster_remote_redis(
            server_public_ip,
            server_private_ip,
            username,
            private_key,
            remote_module_files,
            redis_configuration_parameters,
            temporary_dir,
            shard_count,
            cluster_start_port,
            db_ssh_port,
            modules_configuration_parameters_map,
            logname,
        )
        try:
            for p in range(cluster_start_port, cluster_start_port + shard_count):
                local_redis_conn, ssh_tunnel = ssh_tunnel_redisconn(
                    p,
                    server_private_ip,
                    server_public_ip,
                    username,
                    db_ssh_port,
                    private_key,
                )
                local_redis_conn.ping()
                redis_conns.append(local_redis_conn)
        except redis.exceptions.ConnectionError as e:
            logging.error("A error occurred while spinning DB: {}".format(e.__str__()))
            remote_file = "{}/{}".format(temporary_dir, logfiles[0])
            logging.error(
                "Trying to fetch DB remote log {} into {}".format(
                    remote_file, logfiles[0]
                )
            )
            failed_remote_run_artifact_store(
                True,
                client_public_ip,
                dirname,
                remote_file,
                logfiles[0],
                s3_bucket_name,
                s3_bucket_path,
                username,
                private_key,
            )

    if setup_type == "oss-standalone":

        try:
            full_logfile = spin_up_standalone_remote_redis(
                temporary_dir,
                server_public_ip,
                username,
                private_key,
                remote_module_files,
                logname,
                redis_configuration_parameters,
                db_ssh_port,
                modules_configuration_parameters_map,
            )
            full_logfiles.append(full_logfile)
            local_redis_conn, ssh_tunnel = ssh_tunnel_redisconn(
                server_plaintext_port,
                server_private_ip,
                server_public_ip,
                username,
                db_ssh_port,
                private_key,
            )
            redis_conns.append(local_redis_conn)
        except redis.exceptions.ConnectionError as e:
            logging.error("A error occurred while spinning DB: {}".format(e.__str__()))
            remote_file = full_logfile
            logging.error(
                "Trying to fetch DB remote log {} into {}".format(
                    remote_file, full_logfile
                )
            )
            failed_remote_run_artifact_store(
                True,
                client_public_ip,
                "",
                remote_file,
                full_logfile[1:],
                s3_bucket_name,
                s3_bucket_path,
                username,
                private_key,
            )

    if cluster_enabled:
        setup_redis_cluster_from_conns(
            redis_conns,
            shard_count,
            server_private_ip,
            cluster_start_port,
        )
        server_plaintext_port = cluster_start_port

    topology_setup_end_time = datetime.datetime.now()
    topology_setup_duration_seconds = (
        topology_setup_end_time - topology_setup_start_time
    ).seconds
    logging.info(
        "Topology setup duration {} secs.".format(topology_setup_duration_seconds)
    )
    logging.info("Starting dataset loading...")
    dataset_load_start_time = datetime.datetime.now()
    # common steps to cluster and standalone
    # copy the rdb to DB machine
    _, dataset, _, _ = check_dataset_remote_requirements(
        benchmark_config,
        server_public_ip,
        username,
        private_key,
        remote_dataset_folder,
        dirname,
        shard_count,
        cluster_enabled,
        cluster_start_port,
        db_ssh_port,
    )
    if dataset is not None:
        # force debug reload nosave to replace the current database with the
        # contents of an existing RDB file
        debug_reload_rdb(dataset_load_timeout_secs, redis_conns)

    if setup_type == "oss-cluster":
        cluster_init_steps(clusterconfig, redis_conns, local_module_files)
        redis_setup_result = True

    if check_dbconfig_tool_requirement(benchmark_config):
        logging.info("Detected the requirements to load data via client tool")
        (
            start_time,
            start_time_ms,
            start_time_str,
        ) = get_start_time_vars()
        remote_load_out_file = "/tmp/benchmark-result-{}_{}.out".format(
            test_name, testcase_start_time_str
        )
        local_bench_fname = get_run_full_filename(
            start_time_str,
            setup_name,
            tf_github_org,
            tf_github_repo,
            tf_github_branch,
            test_name,
            tf_github_sha,
        )
        (
            _,
            loading_duration_seconds,
            local_bench_fname,
            remote_run_result,
            results_dict,
            return_code,
        ) = run_remote_client_tool(
            allowed_tools,
            None,
            benchmark_config,
            client_public_ip,
            cluster_enabled,
            local_bench_fname,
            remote_load_out_file,
            return_code,
            server_plaintext_port,
            server_private_ip,
            start_time_ms,
            start_time_str,
            username,
            "dbconfig",
            "linux",
            "amd64",
            "Loading data via client tool",
            False,
            client_ssh_port,
            private_key,
        )
        logging.info(
            "Finished loading the data via client tool. Took {} seconds. Result={}".format(
                loading_duration_seconds, remote_run_result
            )
        )
        redis_setup_result &= remote_run_result
    dataset_load_end_time = datetime.datetime.now()
    if redis_setup_result is True:
        logging.info("Redis available")
    else:
        logging.error("Remote redis is not available")
        raise Exception("Remote redis is not available. Aborting...")
    dataset_load_duration_seconds = (
        dataset_load_end_time - dataset_load_start_time
    ).seconds
    logging.info(
        "Dataset loading duration {} secs.".format(dataset_load_duration_seconds)
    )
    dbconfig_keyspacelen_check(
        benchmark_config,
        redis_conns,
    )
    artifact_version = run_redis_pre_steps(
        benchmark_config, redis_conns[0], required_modules
    )
    return (
        artifact_version,
        cluster_enabled,
        dataset_load_duration_seconds,
        full_logfiles,
        redis_conns,
        return_code,
        server_plaintext_port,
        ssh_tunnel,
    )
