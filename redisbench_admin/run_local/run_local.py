#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#

import json
import logging
import os
import sys
import tempfile
import datetime
import traceback

import redis
from rediscluster import RedisCluster

from redisbench_admin.environments.oss_cluster import spin_up_local_redis_cluster

from redisbench_admin.run.cluster import cluster_init_steps
from redisbench_admin.run.common import (
    prepare_benchmark_parameters,
    get_start_time_vars,
    BENCHMARK_REPETITIONS,
    extract_test_feasible_setups,
    get_setup_type_and_primaries_count,
    run_redis_pre_steps,
    dso_check,
)
from redisbench_admin.run.run import calculate_benchmark_duration_and_check
from redisbench_admin.run_local.local_helpers import (
    run_local_benchmark,
    check_benchmark_binaries_local_requirements,
)
from redisbench_admin.run_local.profile_local import (
    local_profilers_print_artifacts_table,
    local_profilers_stop_if_required,
    local_profilers_start_if_required,
    check_compatible_system_and_kernel_and_prepare_profile,
    local_profilers_platform_checks,
)
from redisbench_admin.utils.benchmark_config import (
    prepare_benchmark_definitions,
    results_dict_kpi_check,
    extract_redis_dbconfig_parameters,
)
from redisbench_admin.utils.local import (
    get_local_run_full_filename,
    is_process_alive,
    check_dataset_local_requirements,
)
from redisbench_admin.environments.oss_standalone import spin_up_local_redis
from redisbench_admin.utils.remote import (
    extract_git_vars,
)
from redisbench_admin.utils.results import post_process_benchmark_results


def run_local_command_logic(args, project_name, project_version):
    logging.info(
        "Using: {project_name} {project_version}".format(
            project_name=project_name, project_version=project_version
        )
    )
    (
        github_org_name,
        github_repo_name,
        github_sha,
        github_actor,
        github_branch,
        github_branch_detached,
    ) = extract_git_vars()

    dbdir_folder = args.dbdir_folder
    os.path.abspath(".")
    required_modules = args.required_module
    profilers_enabled = args.enable_profilers
    s3_bucket_name = args.s3_bucket_name
    profilers_list = []
    if profilers_enabled:
        profilers_list = args.profilers.split(",")
        res = check_compatible_system_and_kernel_and_prepare_profile(args)
        if res is False:
            exit(1)

    logging.info("Retrieved the following local info:")
    logging.info("\tgithub_actor: {}".format(github_actor))
    logging.info("\tgithub_org: {}".format(github_org_name))
    logging.info("\tgithub_repo: {}".format(github_repo_name))
    logging.info("\tgithub_branch: {}".format(github_branch))
    logging.info("\tgithub_sha: {}".format(github_sha))

    local_module_file = args.module_path
    logging.info("Using the following modules {}".format(local_module_file))

    dso = dso_check(args.dso, local_module_file)
    # start the profile
    collection_summary_str = ""
    if profilers_enabled:
        collection_summary_str = local_profilers_platform_checks(
            dso, github_actor, github_branch, github_repo_name, github_sha
        )

    (
        benchmark_definitions,
        _,
        _,
        default_specs,
        clusterconfig,
    ) = prepare_benchmark_definitions(args)

    return_code = 0
    profilers_artifacts_matrix = []
    for repetition in range(1, BENCHMARK_REPETITIONS + 1):
        for test_name, benchmark_config in benchmark_definitions.items():
            logging.info(
                "Repetition {} of {}. Running test {}".format(
                    repetition, BENCHMARK_REPETITIONS, test_name
                )
            )
            test_setups = extract_test_feasible_setups(
                benchmark_config, "setups", default_specs
            )
            for setup_name, setup_settings in test_setups.items():
                setup_type, shard_count = get_setup_type_and_primaries_count(
                    setup_settings
                )
                if setup_type in args.allowed_envs:
                    redis_processes = []
                    logging.info(
                        "Starting setup named {} of topology type {}. Total primaries: {}".format(
                            setup_name, setup_type, shard_count
                        )
                    )
                    # after we've spinned Redis, even on error we should always teardown
                    # in case of some unexpected error we fail the test
                    # noinspection PyBroadException
                    try:
                        dirname = "."
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
                            redis_configuration_parameters,
                            dataset_load_timeout_secs,
                        ) = extract_redis_dbconfig_parameters(
                            benchmark_config, "dbconfig"
                        )
                        cluster_api_enabled = False

                        logging.info(
                            "Using a dataset load timeout of {} seconds.".format(
                                dataset_load_timeout_secs
                            )
                        )

                        if setup_type == "oss-cluster":
                            cluster_api_enabled = True
                            # pass
                            redis_processes = spin_up_local_redis_cluster(
                                temporary_dir,
                                shard_count,
                                args.port,
                                local_module_file,
                                redis_configuration_parameters,
                                dbdir_folder,
                                dataset_load_timeout_secs,
                            )
                            for redis_process in redis_processes:
                                if is_process_alive(redis_process) is False:
                                    raise Exception(
                                        "Redis process is not alive. Failing test."
                                    )
                            # we use node 0 for the checks
                            r = redis.StrictRedis(port=args.port)
                            r_conns = []
                            for p in range(args.port, args.port + shard_count):
                                redis.StrictRedis(port=p).execute_command(
                                    "CLUSTER SAVECONFIG"
                                )

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
                                    raise Exception(
                                        "Redis process is not alive. Failing test."
                                    )

                            r = redis.StrictRedis(port=args.port)

                        if setup_type == "oss-cluster":
                            contains_rdb = False
                            if dataset is not None:
                                contains_rdb = True
                            startup_nodes = cluster_init_steps(
                                args,
                                clusterconfig,
                                local_module_file,
                                r_conns,
                                shard_count,
                                contains_rdb,
                            )

                            rc = RedisCluster(
                                startup_nodes=startup_nodes, decode_responses=True
                            )
                            cluster_info = rc.cluster_info()
                            logging.info(
                                "Cluster info after initialization: {}.".format(
                                    cluster_info
                                )
                            )
                        run_redis_pre_steps(benchmark_config, r, required_modules)

                        # setup the benchmark
                        (
                            start_time,
                            start_time_ms,
                            start_time_str,
                        ) = get_start_time_vars()
                        local_benchmark_output_filename = get_local_run_full_filename(
                            start_time_str, github_branch, test_name, setup_name
                        )
                        logging.info(
                            "Will store benchmark json output to local file {}".format(
                                local_benchmark_output_filename
                            )
                        )

                        (
                            benchmark_tool,
                            full_benchmark_path,
                            benchmark_tool_workdir,
                        ) = check_benchmark_binaries_local_requirements(
                            benchmark_config, args.allowed_tools
                        )

                        # prepare the benchmark command
                        command, command_str = prepare_benchmark_parameters(
                            benchmark_config,
                            full_benchmark_path,
                            args.port,
                            "localhost",
                            local_benchmark_output_filename,
                            False,
                            benchmark_tool_workdir,
                            cluster_api_enabled,
                        )

                        # start the profile
                        (
                            profiler_name,
                            profilers_map,
                        ) = local_profilers_start_if_required(
                            profilers_enabled,
                            profilers_list,
                            redis_processes,
                            setup_name,
                            start_time_str,
                            test_name,
                        )

                        # run the benchmark
                        benchmark_start_time = datetime.datetime.now()
                        stdout, stderr = run_local_benchmark(benchmark_tool, command)
                        benchmark_end_time = datetime.datetime.now()
                        benchmark_duration_seconds = (
                            calculate_benchmark_duration_and_check(
                                benchmark_end_time, benchmark_start_time
                            )
                        )

                        logging.info("Extracting the benchmark results")
                        logging.info("stdout: {}".format(stdout))
                        logging.info("stderr: {}".format(stderr))

                        local_profilers_stop_if_required(
                            args,
                            benchmark_duration_seconds,
                            collection_summary_str,
                            dso,
                            github_org_name,
                            github_repo_name,
                            profiler_name,
                            profilers_artifacts_matrix,
                            profilers_enabled,
                            profilers_map,
                            redis_processes,
                            s3_bucket_name,
                            test_name,
                        )

                        post_process_benchmark_results(
                            benchmark_tool,
                            local_benchmark_output_filename,
                            start_time_ms,
                            start_time_str,
                            stdout,
                        )

                        with open(local_benchmark_output_filename, "r") as json_file:
                            results_dict = json.load(json_file)

                            # check KPIs
                            return_code = results_dict_kpi_check(
                                benchmark_config, results_dict, return_code
                            )
                        stdout = r.shutdown(save=False)
                    except:
                        return_code |= 1
                        logging.critical(
                            "Some unexpected exception was caught "
                            "during local work. Failing test...."
                        )
                        logging.critical(sys.exc_info()[0])
                        print("-" * 60)
                        traceback.print_exc(file=sys.stdout)
                        print("-" * 60)
                    # tear-down
                    logging.info("Tearing down setup")
                    for redis_process in redis_processes:
                        if redis_process is not None:
                            redis_process.kill()
                    logging.info("Tear-down completed")

    if profilers_enabled:
        local_profilers_print_artifacts_table(profilers_artifacts_matrix)
    exit(return_code)
