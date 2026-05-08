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
import redis
from redisbench_admin.run.git import git_vars_crosscheck

from redisbench_admin.utils.remote import (
    get_project_ts_tags,
    push_data_to_redistimeseries,
    perform_connectivity_test,
    detect_target_arch,
    ARCH_X86,
)
from redisbench_admin.utils.redisearch import extract_module_git_sha

import redisbench_admin.run.metrics
from redisbench_admin.profilers.perf import PERF_CALLGRAPH_MODE
from redisbench_admin.profilers.profilers_schema import (
    local_profilers_print_artifacts_table,
)
from redisbench_admin.run.args import PROFILE_FREQ
from redisbench_admin.run.common import (
    prepare_benchmark_parameters,
    get_start_time_vars,
    BENCHMARK_REPETITIONS,
    get_setup_type_and_primaries_count,
    dso_check,
    print_results_table_stdout,
    run_redis_post_steps,
)
from redisbench_admin.run.metrics import (
    from_info_to_overall_shard_cpu,
    collect_redis_metrics,
    collect_cpu_data,
)

from redisbench_admin.run.redistimeseries import (
    datasink_profile_tabular_data,
    timeseries_test_sucess_flow,
)
from redisbench_admin.run.run import (
    calculate_client_tool_duration_and_check,
    define_benchmark_plan,
    ensure_mixed_types_first,
    reorganize_benchmark_plan,
    log_benchmark_plan_table,
    EnvironmentTracker,
)
from redisbench_admin.run_local.local_db import local_db_spin
from redisbench_admin.run_local.local_helpers import (
    run_local_benchmark,
    check_benchmark_binaries_local_requirements,
)
from redisbench_admin.profilers.profilers_local import (
    profilers_stop_if_required,
    profilers_start_if_required,
    check_compatible_system_and_kernel_and_prepare_profile,
    local_profilers_platform_checks,
)
from redisbench_admin.utils.benchmark_config import (
    prepare_benchmark_definitions,
    results_dict_kpi_check,
    get_metadata_tags,
)
from redisbench_admin.utils.local import (
    get_local_run_full_filename,
)

from redisbench_admin.utils.results import post_process_benchmark_results

import threading


def run_local_command_logic(args, project_name, project_version):
    logging.info(
        "Using: {project_name} {project_version}".format(
            project_name=project_name, project_version=project_version
        )
    )
    tf_github_org = args.github_org
    tf_github_actor = args.github_actor
    tf_github_repo = args.github_repo
    tf_github_sha = args.github_sha
    args_github_sha_explicit = args.github_sha not in (None, "")
    tf_github_branch = args.github_branch

    (
        github_actor,
        github_branch,
        github_org_name,
        github_repo_name,
        github_sha,
    ) = git_vars_crosscheck(
        tf_github_actor, tf_github_branch, tf_github_org, tf_github_repo, tf_github_sha
    )

    dbdir_folder = args.dbdir_folder
    os.path.abspath(".")
    required_modules = args.required_module
    profilers_enabled = args.enable_profilers
    s3_bucket_name = args.s3_bucket_name
    flushall_on_every_test_start = args.flushall_on_every_test_start
    ignore_keyspace_errors = args.ignore_keyspace_errors
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
        logging.info(
            "Checking connection to RedisTimeSeries to host: {}:{}".format(
                args.redistimeseries_host, args.redistimeseries_port
            )
        )
        rts = redis.Redis(
            host=args.redistimeseries_host,
            port=args.redistimeseries_port,
            password=args.redistimeseries_pass,
            retry_on_timeout=True,
        )
        rts.ping()

    dso = dso_check(args.dso, local_module_file)
    # start the profile
    collection_summary_str = ""
    if profilers_enabled:
        collection_summary_str = local_profilers_platform_checks(
            dso, github_actor, github_branch, github_repo_name, github_sha
        )

    (
        _,
        benchmark_definitions,
        default_metrics,
        exporter_timemetric_path,
        default_specs,
        clusterconfig,
    ) = prepare_benchmark_definitions(args)

    return_code = 0
    profilers_artifacts_matrix = []
    # we have a map of test-type, dataset-name, topology, test-name
    benchmark_runs_plan = define_benchmark_plan(benchmark_definitions, default_specs)

    # Parse allowed_setups for filtering the plan display
    allowed_setups_list = None
    if args.allowed_setups != "":
        allowed_setups_list = args.allowed_setups.split(",")

    # Log the benchmark execution plan as a table (filtered by allowed_setups if specified)
    log_benchmark_plan_table(benchmark_runs_plan, allowed_setups_list)

    # Track if we should reuse the environment from mixed benchmarks for read-only ones
    reuse_mixed = "mixed" in benchmark_runs_plan and "read-only" in benchmark_runs_plan
    if reuse_mixed:
        logging.info(
            "Detected mixed and read-only benchmarks. Will reuse environment for dataset optimization."
        )

    # Shared environment storage across benchmark types, keyed by (dataset_name, setup_name)
    shared_env = {}
    # Track environment creation and reuse for summary
    env_tracker = EnvironmentTracker()

    # Reorganize plan: setup -> dataset -> benchmark_type (with mixed first)
    reorganized_plan = reorganize_benchmark_plan(benchmark_runs_plan)

    for setup_name in sorted(reorganized_plan.keys()):
        logging.info("Running benchmarks for setup {}.".format(setup_name))

        bench_by_dataset_map = reorganized_plan[setup_name]
        for dataset_name in sorted(bench_by_dataset_map.keys()):
            logging.info("Running benchmarks for dataset {}.".format(dataset_name))

            bench_by_type_map = bench_by_dataset_map[dataset_name]
            for benchmark_type in ensure_mixed_types_first(bench_by_type_map.keys()):
                logging.info("Running benchmarks of type {}.".format(benchmark_type))

                setup_details = bench_by_type_map[benchmark_type]
                setup_settings = setup_details["setup_settings"]
                benchmarks_map = setup_details["benchmarks"]

                # Check if we have a shared environment from a previous benchmark type
                env_key = (dataset_name, setup_name)
                if reuse_mixed and env_key in shared_env:
                    setup_details["env"] = shared_env[env_key]
                    # Remove from shared_env so it gets torn down after we're done
                    del shared_env[env_key]
                    logging.info(
                        f"Reusing shared environment for dataset '{dataset_name}' and setup '{setup_name}'"
                    )
                else:
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
                            allowed_setups = args.allowed_setups.split(",")
                            logging.info(
                                "Checking if setup named {} of topology type {}. Total primaries: {} is in the allowed list of setups {}".format(
                                    setup_name, setup_type, shard_count, allowed_setups
                                )
                            )
                            if setup_name not in allowed_setups:
                                logging.warning(
                                    "SKIPPING setup named {} of topology type {}.".format(
                                        setup_name, setup_type
                                    )
                                )
                                continue
                        if setup_type in args.allowed_envs:
                            redis_processes = []
                            redis_conns = []
                            # after we've spinned Redis, even on error we should always teardown
                            # in case of some unexpected error we fail the test
                            # noinspection PyBroadException
                            try:
                                dirname = args.db_dirname
                                if setup_details["env"] is None:
                                    logging.info(
                                        "Starting setup named {} of topology type {}. Total primaries: {}".format(
                                            setup_name, setup_type, shard_count
                                        )
                                    )
                                    binary = args.redis_binary
                                    if " " in binary:
                                        binary = binary.split(" ")
                                    (
                                        result_db_spin,
                                        artifact_version,
                                        cluster_api_enabled,
                                        redis_conns,
                                        redis_processes,
                                    ) = local_db_spin(
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
                                        flushall_on_every_test_start,
                                        ignore_keyspace_errors,
                                    )
                                    if result_db_spin is False:
                                        logging.warning(
                                            "Skipping this test given DB spin stage failed..."
                                        )
                                        continue
                                    # Save environment for reuse if:
                                    # - benchmark is read-only (for next read-only benchmarks)
                                    # - benchmark is mixed AND we have read-only benchmarks to reuse the dataset
                                    if benchmark_type == "read-only" or reuse_mixed:
                                        if benchmark_type == "mixed":
                                            logging.info(
                                                f"Saving environment from mixed benchmark for dataset reuse (dataset: {dataset_name}, setup: {setup_name})."
                                            )
                                        else:
                                            logging.info(
                                                "Given the benchmark for this setup is read-only we will prepare to reuse it on the next read-only benchmarks (if any)."
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
                                        # Store Redis PIDs for verification when reusing
                                        setup_details["env"]["redis_pids"] = [
                                            p.pid
                                            for p in redis_processes
                                            if p is not None
                                        ]
                                        logging.info(
                                            f"Stored Redis PIDs for reuse verification: {setup_details['env']['redis_pids']}"
                                        )
                                        # Track environment creation
                                        env_tracker.record_env_created(
                                            dataset_name,
                                            setup_name,
                                            test_name,
                                            redis_pids=setup_details["env"][
                                                "redis_pids"
                                            ],
                                        )
                                        # Save to shared storage for cross-benchmark-type reuse
                                        if reuse_mixed:
                                            shared_env[env_key] = setup_details["env"]
                                else:
                                    logging.info(
                                        f"Reusing environment from previous benchmark for dataset reuse (dataset: {dataset_name}, setup: {setup_name})."
                                    )
                                    cluster_api_enabled = setup_details["env"][
                                        "cluster_api_enabled"
                                    ]
                                    redis_conns = setup_details["env"]["redis_conns"]
                                    redis_processes = setup_details["env"][
                                        "redis_processes"
                                    ]
                                    # Verify Redis PIDs are the same (same Redis instance is being reused)
                                    expected_pids = setup_details["env"]["redis_pids"]
                                    current_pids = [
                                        p.pid for p in redis_processes if p is not None
                                    ]
                                    pids_match = current_pids == expected_pids
                                    if pids_match:
                                        logging.info(
                                            f"🟢 Redis PID check PASSED: expected={expected_pids}, got={current_pids} (same Redis instance reused)"
                                        )
                                    else:
                                        logging.error(
                                            f"🔴 Redis PID check FAILED: expected={expected_pids}, got={current_pids}"
                                        )
                                        assert (
                                            False
                                        ), f"Redis PIDs mismatch! Expected {expected_pids}, got {current_pids}. Environment was not properly reused."
                                    # Track environment reuse
                                    env_tracker.record_env_reused(
                                        dataset_name,
                                        setup_name,
                                        test_name,
                                        pids_match=pids_match,
                                    )

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

                                # For ftsb tools, set up log file path
                                local_log_out_file = None
                                if "ftsb_" in benchmark_tool:
                                    local_log_out_file = (
                                        local_benchmark_output_filename.replace(
                                            ".json", ".log"
                                        )
                                    )
                                    logging.info(
                                        "Will store benchmark log output to local file {}".format(
                                            local_log_out_file
                                        )
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
                                    "clientconfig",
                                    None,
                                    None,
                                    None,
                                    None,
                                    args.password,
                                    local_log_out_file,
                                )
                                # get the pids
                                redis_pids = [
                                    redis_conn.info("server")["process_id"]
                                    for redis_conn in redis_conns
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
                                    PROFILE_FREQ,
                                    PERF_CALLGRAPH_MODE,
                                )

                                # run the benchmark

                                cpu_stats_thread = threading.Thread(
                                    target=collect_cpu_data,
                                    args=(redis_conns, 5.0, 1.0),
                                )
                                redisbench_admin.run.metrics.BENCHMARK_RUNNING_GLOBAL = (
                                    True
                                )
                                cpu_stats_thread.start()
                                benchmark_start_time = datetime.datetime.now()
                                logging.info(
                                    "Running benchmark command: {}".format(command)
                                )
                                # Handle dry-run modes
                                if args.dry_run or args.dry_run_with_preload:
                                    logging.info(
                                        "🏃 Dry-run mode detected - performing connectivity tests"
                                    )

                                    # Test basic connectivity after setup
                                    connectivity_success = perform_connectivity_test(
                                        redis_conns, "after local environment setup"
                                    )

                                    if args.dry_run_with_preload:
                                        logging.info(
                                            "📦 Dry-run with preload - data loading already completed during setup"
                                        )
                                        # Test connectivity after preload (data was loaded during local_db_spin)
                                        connectivity_success = (
                                            perform_connectivity_test(
                                                redis_conns, "after data preloading"
                                            )
                                            and connectivity_success
                                        )

                                    # Print dry-run summary
                                    logging.info("=" * 50)
                                    logging.info("🎯 DRY-RUN SUMMARY")
                                    logging.info("=" * 50)
                                    logging.info(
                                        f"✅ Database: {setup_type} ({'cluster' if cluster_api_enabled else 'standalone'}) started locally"
                                    )
                                    logging.info(
                                        f"✅ Client tools: {benchmark_tool} available"
                                    )
                                    logging.info(
                                        f"{'✅' if connectivity_success else '❌'} Connectivity: {len(redis_conns)} connection(s) tested"
                                    )
                                    if args.dry_run_with_preload:
                                        logging.info(
                                            "✅ Data preload: Completed during setup"
                                        )
                                    logging.info("🏁 Dry-run completed successfully")
                                    logging.info(
                                        "⏭️  Benchmark execution skipped (dry-run mode)"
                                    )
                                    logging.info("=" * 50)

                                    # Skip benchmark execution and continue to next test
                                else:
                                    stdout, stderr = run_local_benchmark(
                                        benchmark_tool, command
                                    )
                                benchmark_end_time = datetime.datetime.now()
                                redisbench_admin.run.metrics.BENCHMARK_RUNNING_GLOBAL = (
                                    False
                                )
                                cpu_stats_thread.join()
                                (
                                    total_shards_cpu_usage,
                                    cpu_usage_map,
                                ) = from_info_to_overall_shard_cpu(
                                    redisbench_admin.run.metrics.BENCHMARK_CPU_STATS_GLOBAL
                                )
                                logging.info(
                                    "Total CPU usage ({:.3f} %)".format(
                                        total_shards_cpu_usage
                                    )
                                )
                                logging.info(
                                    "CPU MAP: {}".format(
                                        json.dumps(cpu_usage_map, indent=2)
                                    )
                                )
                                benchmark_duration_seconds = (
                                    calculate_client_tool_duration_and_check(
                                        benchmark_end_time, benchmark_start_time
                                    )
                                )
                                # run post commands after benchmark completes
                                if args.dry_run is False:
                                    for redis_conn in redis_conns:
                                        run_redis_post_steps(
                                            benchmark_config, redis_conn
                                        )
                                if args.dry_run is False:
                                    logging.info("Extracting the benchmark results")
                                    logging.info("stdout: {}".format(stdout))
                                    logging.info("stderr: {}".format(stderr))

                                (
                                    _,
                                    overall_tabular_data_map,
                                ) = profilers_stop_if_required(
                                    args.upload_results_s3,
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

                                (
                                    end_time_ms,
                                    _,
                                    overall_end_time_metrics,
                                ) = collect_redis_metrics(
                                    redis_conns,
                                    ["memory"],
                                    {
                                        "memory": [
                                            "used_memory",
                                            "used_memory_dataset",
                                        ]
                                    },
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
                                results_dict = {}
                                if args.dry_run is False:
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
                                            setup_type,
                                            test_name,
                                            total_shards_cpu_usage,
                                            overall_end_time_metrics,
                                            [
                                                "memory_used_memory",
                                                "memory_used_memory_dataset",
                                            ],
                                        )
                                        export_redis_metrics(
                                            artifact_version,
                                            end_time_ms,
                                            overall_end_time_metrics,
                                            rts,
                                            setup_name,
                                            setup_type,
                                            test_name,
                                            tf_github_branch,
                                            tf_github_org,
                                            tf_github_repo,
                                            tf_triggering_env,
                                            {"metric-type": "redis-metrics"},
                                            0,
                                        )

                                        # check KPIs
                                        return_code = results_dict_kpi_check(
                                            benchmark_config, results_dict, return_code
                                        )

                                metadata_tags = get_metadata_tags(benchmark_config)
                                if args_github_sha_explicit:
                                    push_github_sha = tf_github_sha
                                else:
                                    push_github_sha = None
                                    if redis_conns:
                                        push_github_sha = extract_module_git_sha(
                                            redis_conns[0]
                                        )
                                    if not push_github_sha:
                                        push_github_sha = github_sha
                                push_arch = None
                                if redis_conns:
                                    push_arch = detect_target_arch(redis_conns[0])
                                if not push_arch:
                                    push_arch = args.architecture or ARCH_X86
                                (
                                    _,
                                    branch_target_tables,
                                ) = timeseries_test_sucess_flow(
                                    args.push_results_redistimeseries,
                                    artifact_version,
                                    benchmark_config,
                                    benchmark_duration_seconds,
                                    0,
                                    default_metrics,
                                    setup_name,
                                    setup_type,
                                    exporter_timemetric_path,
                                    results_dict,
                                    rts,
                                    start_time_ms,
                                    test_name,
                                    github_branch,
                                    github_org_name,
                                    github_repo_name,
                                    tf_triggering_env,
                                    metadata_tags,
                                    tf_github_sha=push_github_sha,
                                    arch=push_arch,
                                )

                                if setup_details["env"] is None:
                                    if args.keep_env_and_topo is False:
                                        for conn in redis_conns:
                                            conn.shutdown(save=False)
                                    else:
                                        logging.info(
                                            "Keeping environment and topology active upon user request (--keep_env_and_topo)."
                                        )
                                else:
                                    logging.info(
                                        "Keeping environment and topology active for dataset reuse on next benchmark."
                                    )

                            except KeyboardInterrupt:
                                logging.critical(
                                    "Detected Keyboard interrupt...Tearing down and exiting!"
                                )
                                teardown_local_setup(
                                    redis_conns, redis_processes, setup_name
                                )
                                exit(1)
                            except:
                                return_code |= 1
                                logging.critical(
                                    "Some unexpected exception was caught "
                                    "during local work. Failing test...."
                                )
                                if len(sys.exc_info()) > 0:
                                    logging.critical(sys.exc_info()[0])
                                else:
                                    logging.critical(sys.exc_info())
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
                                        "Keeping environment and topology active upon user request (--keep_env_and_topo)."
                                    )

                        else:
                            logging.info(
                                "Setup type {} not in allowed envs: {}".format(
                                    setup_type, args.allowed_envs
                                )
                            )
                if setup_details["env"] is not None:
                    # Check if this environment is shared for dataset reuse across benchmark types
                    env_key = (dataset_name, setup_name)
                    is_shared = reuse_mixed and env_key in shared_env
                    if args.keep_env_and_topo is False and not is_shared:
                        teardown_local_setup(redis_conns, redis_processes, setup_name)
                        setup_details["env"] = None
                    elif is_shared:
                        logging.info(
                            f"Keeping environment active for dataset reuse across benchmark types (dataset: {dataset_name}, setup: {setup_name})."
                        )
                    else:
                        logging.info(
                            "Keeping environment and topology active upon user request (--keep_env_and_topo)."
                        )

    if profilers_enabled:
        local_profilers_print_artifacts_table(profilers_artifacts_matrix)
    # Log environment reuse summary
    env_tracker.log_summary_table()
    exit(return_code)


def teardown_local_setup(redis_conns, redis_processes, setup_name):
    logging.info("Tearing down setup {}".format(setup_name))
    for redis_process in redis_processes:
        if redis_process is not None:
            redis_process.kill()
    for conn in redis_conns:
        conn.shutdown(nosave=True)
    logging.info("Tear-down completed")


def export_redis_metrics(
    artifact_version,
    end_time_ms,
    overall_end_time_metrics,
    rts,
    setup_name,
    setup_type,
    test_name,
    tf_github_branch,
    tf_github_org,
    tf_github_repo,
    tf_triggering_env,
    metadata_dict=None,
    expire_ms=0,
):
    datapoint_errors = 0
    datapoint_inserts = 0
    sprefix = (
        "ci.benchmarks.redislabs/"
        + "{triggering_env}/{github_org}/{github_repo}".format(
            triggering_env=tf_triggering_env,
            github_org=tf_github_org,
            github_repo=tf_github_repo,
        )
    )

    logging.info(
        "Adding a total of {} server side metrics collected at the end of benchmark (deployment_name={}, deployment_type={})".format(
            len(list(overall_end_time_metrics.items())), setup_name, setup_type
        )
    )
    timeseries_dict = {}
    by_variants = {}
    if tf_github_branch is not None and tf_github_branch != "":
        by_variants["by.branch/{}".format(tf_github_branch)] = {
            "branch": tf_github_branch
        }
    if artifact_version is not None and artifact_version != "":
        by_variants["by.version/{}".format(artifact_version)] = {
            "version": artifact_version
        }
    for (
        by_variant,
        variant_labels_dict,
    ) in by_variants.items():
        for (
            metric_name,
            metric_value,
        ) in overall_end_time_metrics.items():
            tsname_metric = "{}/{}/{}/benchmark_end/{}/{}".format(
                sprefix,
                test_name,
                by_variant,
                setup_name,
                metric_name,
            )

            logging.debug(
                "Adding a redis server side metric collected at the end of benchmark."
                + " metric_name={} metric_value={} time-series name: {}".format(
                    metric_name,
                    metric_value,
                    tsname_metric,
                )
            )
            variant_labels_dict["metric"] = metric_name
            commandstats_latencystats_process_name(
                metric_name, "commandstats_cmdstat_", setup_name, variant_labels_dict
            )
            commandstats_latencystats_process_name(
                metric_name,
                "latencystats_latency_percentiles_usec_",
                setup_name,
                variant_labels_dict,
            )

            variant_labels_dict["test_name"] = test_name
            if metadata_dict is not None:
                variant_labels_dict.update(metadata_dict)

            timeseries_dict[tsname_metric] = {
                "labels": get_project_ts_tags(
                    tf_github_org,
                    tf_github_repo,
                    setup_name,
                    setup_type,
                    tf_triggering_env,
                    variant_labels_dict,
                    None,
                    None,
                ),
                "data": {end_time_ms: metric_value},
            }
    i_errors, i_inserts = push_data_to_redistimeseries(rts, timeseries_dict, expire_ms)
    datapoint_errors = datapoint_errors + i_errors
    datapoint_inserts = datapoint_inserts + i_inserts
    return datapoint_errors, datapoint_inserts


def commandstats_latencystats_process_name(
    metric_name, prefix, setup_name, variant_labels_dict
):
    if prefix in metric_name:
        command_and_metric_and_shard = metric_name[len(prefix) :]
        command = (
            command_and_metric_and_shard[0]
            + command_and_metric_and_shard[1:].split("_", 1)[0]
        )
        metric_and_shard = command_and_metric_and_shard[1:].split("_", 1)[1]
        metric = metric_and_shard
        shard = "1"
        if "_shard_" in metric_and_shard:
            metric = metric_and_shard.split("_shard_")[0]
            shard = metric_and_shard.split("_shard_")[1]
        variant_labels_dict["metric"] = metric
        variant_labels_dict["command"] = command
        variant_labels_dict["command_and_metric"] = "{} - {}".format(command, metric)
        variant_labels_dict["command_and_metric_and_setup"] = "{} - {} - {}".format(
            command, metric, setup_name
        )
        variant_labels_dict["command_and_setup"] = "{} - {}".format(command, setup_name)
        variant_labels_dict["shard"] = shard
        variant_labels_dict["metric_and_shard"] = metric_and_shard

        version = None
        branch = None
        if "version" in variant_labels_dict:
            version = variant_labels_dict["version"]
        if "branch" in variant_labels_dict:
            branch = variant_labels_dict["branch"]

        if version is not None:
            variant_labels_dict["command_and_metric_and_version"] = (
                "{} - {} - {}".format(command, metric, version)
            )
            variant_labels_dict["command_and_metric_and_setup_and_version"] = (
                "{} - {} - {} - {}".format(command, metric, setup_name, version)
            )

        if branch is not None:
            variant_labels_dict["command_and_metric_and_branch"] = (
                "{} - {} - {}".format(command, metric, branch)
            )
            variant_labels_dict["command_and_metric_and_setup_and_branch"] = (
                "{} - {} - {} - {}".format(command, metric, setup_name, branch)
            )
