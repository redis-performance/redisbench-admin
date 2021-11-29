#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#

import json
import logging
import os
import sys
import datetime
import traceback

from redistimeseries.client import Client

from redisbench_admin.run.common import (
    prepare_benchmark_parameters,
    get_start_time_vars,
    BENCHMARK_REPETITIONS,
    get_setup_type_and_primaries_count,
    dso_check,
    print_results_table_stdout,
)
from redisbench_admin.run.redistimeseries import datasink_profile_tabular_data
from redisbench_admin.run.run import (
    calculate_client_tool_duration_and_check,
    define_benchmark_plan,
)
from redisbench_admin.run_local.local_db import local_db_spin
from redisbench_admin.run_local.local_helpers import (
    run_local_benchmark,
    check_benchmark_binaries_local_requirements,
)
from redisbench_admin.run_local.profile_local import (
    local_profilers_print_artifacts_table,
    profilers_stop_if_required,
    profilers_start_if_required,
    check_compatible_system_and_kernel_and_prepare_profile,
    local_profilers_platform_checks,
)
from redisbench_admin.utils.benchmark_config import (
    prepare_benchmark_definitions,
    results_dict_kpi_check,
)
from redisbench_admin.utils.local import (
    get_local_run_full_filename,
)
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
    tf_triggering_env = args.triggering_env

    logging.info("Retrieved the following local info:")
    logging.info("\tgithub_actor: {}".format(github_actor))
    logging.info("\tgithub_org: {}".format(github_org_name))
    logging.info("\tgithub_repo: {}".format(github_repo_name))
    logging.info("\tgithub_branch: {}".format(github_branch))
    logging.info("\tgithub_sha: {}".format(github_sha))

    local_module_file = args.module_path
    logging.info("Using the following modules {}".format(local_module_file))

    rts = None
    if args.push_results_redistimeseries:
        logging.info("Checking connection to RedisTimeSeries.")
        rts = Client(
            host=args.redistimeseries_host,
            port=args.redistimeseries_port,
            password=args.redistimeseries_pass,
        )
        rts.redis.ping()

    dso = dso_check(args.dso, local_module_file)
    # start the profile
    collection_summary_str = ""
    if profilers_enabled:
        collection_summary_str = local_profilers_platform_checks(
            dso, github_actor, github_branch, github_repo_name, github_sha
        )

    (
        benchmark_definitions,
        default_metrics,
        _,
        default_specs,
        clusterconfig,
    ) = prepare_benchmark_definitions(args)

    return_code = 0
    profilers_artifacts_matrix = []
    # we have a map of test-type, dataset-name, topology, test-name
    benchmark_runs_plan = define_benchmark_plan(benchmark_definitions, default_specs)
    for benchmark_type, bench_by_dataset_map in benchmark_runs_plan.items():
        for (
            dataset_name,
            bench_by_dataset_and_setup_map,
        ) in bench_by_dataset_map.items():
            for setup_name, setup_details in bench_by_dataset_and_setup_map.items():
                setup_settings = setup_details["setup_settings"]
                benchmarks_map = setup_details["benchmarks"]
                # we start with an empty per bench-type/setup-name
                setup_details["env"] = None
                for test_name, benchmark_config in benchmarks_map.items():
                    for repetition in range(1, BENCHMARK_REPETITIONS + 1):
                        logging.info(
                            "Repetition {} of {}. Running test {}".format(
                                repetition, BENCHMARK_REPETITIONS, test_name
                            )
                        )

                        (
                            setup_name,
                            setup_type,
                            shard_count,
                        ) = get_setup_type_and_primaries_count(setup_settings)
                        if args.allowed_setups != "":
                            allowed_setups = args.allowed_setups.split()
                            if setup_name not in allowed_setups:
                                logging.warning(
                                    "SKIPPING setup named {} of topology type {}.".format(
                                        setup_name, setup_type
                                    )
                                )
                                continue
                        if setup_type in args.allowed_envs:
                            redis_processes = []
                            # after we've spinned Redis, even on error we should always teardown
                            # in case of some unexpected error we fail the test
                            # noinspection PyBroadException
                            try:
                                dirname = "."
                                if setup_details["env"] is None:
                                    logging.info(
                                        "Starting setup named {} of topology type {}. Total primaries: {}".format(
                                            setup_name, setup_type, shard_count
                                        )
                                    )
                                    (
                                        cluster_api_enabled,
                                        redis_conns,
                                        redis_processes,
                                    ) = local_db_spin(
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
                                    )
                                    if benchmark_type == "read-only":
                                        logging.info(
                                            "Given the benchmark for this setup is ready-only we will prepare to reuse it on the next read-only benchmarks (if any )."
                                        )
                                        setup_details["env"] = {}
                                        setup_details["env"][
                                            "cluster_api_enabled"
                                        ] = cluster_api_enabled
                                        setup_details["env"][
                                            "redis_conns"
                                        ] = redis_conns
                                        setup_details["env"][
                                            "redis_processes"
                                        ] = redis_processes
                                else:
                                    assert benchmark_type == "read-only"
                                    logging.info(
                                        "Given the benchmark for this setup is ready-only, and this setup was already spinned we will reuse the previous, conns and process info."
                                    )
                                    cluster_api_enabled = setup_details["env"][
                                        "cluster_api_enabled"
                                    ]
                                    redis_conns = setup_details["env"]["redis_conns"]
                                    redis_processes = setup_details["env"][
                                        "redis_processes"
                                    ]

                                # setup the benchmark
                                (
                                    start_time,
                                    start_time_ms,
                                    start_time_str,
                                ) = get_start_time_vars()
                                local_benchmark_output_filename = (
                                    get_local_run_full_filename(
                                        start_time_str,
                                        github_branch,
                                        test_name,
                                        setup_name,
                                    )
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
                                redis_pids = [
                                    redis_process.pid
                                    for redis_process in redis_processes
                                ]
                                # start the profile
                                (
                                    profiler_name,
                                    profilers_map,
                                ) = profilers_start_if_required(
                                    profilers_enabled,
                                    profilers_list,
                                    redis_pids,
                                    setup_name,
                                    start_time_str,
                                    test_name,
                                )

                                # run the benchmark
                                benchmark_start_time = datetime.datetime.now()
                                stdout, stderr = run_local_benchmark(
                                    benchmark_tool, command
                                )
                                benchmark_end_time = datetime.datetime.now()
                                benchmark_duration_seconds = (
                                    calculate_client_tool_duration_and_check(
                                        benchmark_end_time, benchmark_start_time
                                    )
                                )

                                logging.info("Extracting the benchmark results")
                                logging.info("stdout: {}".format(stdout))
                                logging.info("stderr: {}".format(stderr))

                                (
                                    _,
                                    overall_tabular_data_map,
                                ) = profilers_stop_if_required(
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
                                    redis_pids,
                                    s3_bucket_name,
                                    test_name,
                                )
                                if (
                                    profilers_enabled
                                    and args.push_results_redistimeseries
                                ):
                                    datasink_profile_tabular_data(
                                        github_branch,
                                        github_org_name,
                                        github_repo_name,
                                        github_sha,
                                        overall_tabular_data_map,
                                        rts,
                                        setup_type,
                                        start_time_ms,
                                        start_time_str,
                                        test_name,
                                        tf_triggering_env,
                                    )

                                post_process_benchmark_results(
                                    benchmark_tool,
                                    local_benchmark_output_filename,
                                    start_time_ms,
                                    start_time_str,
                                    stdout,
                                )

                                with open(
                                    local_benchmark_output_filename, "r"
                                ) as json_file:
                                    results_dict = json.load(json_file)
                                    print_results_table_stdout(
                                        benchmark_config,
                                        default_metrics,
                                        results_dict,
                                        setup_name,
                                        test_name,
                                    )

                                    # check KPIs
                                    return_code = results_dict_kpi_check(
                                        benchmark_config, results_dict, return_code
                                    )
                                if setup_details["env"] is None:
                                    if args.keep_env_and_topo is False:
                                        for conn in redis_conns:
                                            conn.shutdown(save=False)
                                    else:
                                        logging.info(
                                            "Keeping environment and topology active upon request."
                                        )

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
                                setup_details["env"] = None

                            # tear-down
                            if setup_details["env"] is None:
                                if args.keep_env_and_topo is False:
                                    teardown_local_setup(
                                        redis_conns, redis_processes, setup_name
                                    )
                                else:
                                    logging.info(
                                        "Keeping environment and topology active upon request."
                                    )

                        else:
                            logging.info(
                                "Setup type {} not in allowed envs: {}".format(
                                    setup_type, args.allowed_envs
                                )
                            )
                if setup_details["env"] is not None:
                    if args.keep_env_and_topo is False:
                        teardown_local_setup(redis_conns, redis_processes, setup_name)
                        setup_details["env"] = None
                    else:
                        logging.info(
                            "Keeping environment and topology active upon request."
                        )

    if profilers_enabled:
        local_profilers_print_artifacts_table(profilers_artifacts_matrix)
    exit(return_code)


def teardown_local_setup(redis_conns, redis_processes, setup_name):
    logging.info("Tearing down setup {}".format(setup_name))
    for redis_process in redis_processes:
        if redis_process is not None:
            redis_process.kill()
    for conn in redis_conns:
        conn.shutdown(nosave=True)
    logging.info("Tear-down completed")
