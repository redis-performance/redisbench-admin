#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import logging
import random
import string
import sys
import traceback
import redis
import pytablewriter
from pytablewriter import MarkdownTableWriter
import redisbench_admin.run.metrics
from redisbench_admin.profilers.perf import PERF_CALLGRAPH_MODE
from redisbench_admin.run.metrics import (
    from_info_to_overall_shard_cpu,
    collect_redis_metrics,
)
from redisbench_admin.profilers.perf_daemon_caller import (
    PerfDaemonRemoteCaller,
    PERF_DAEMON_LOGNAME,
)
from redisbench_admin.run.args import PROFILE_FREQ
from redisbench_admin.run.common import (
    get_start_time_vars,
    BENCHMARK_REPETITIONS,
    get_setup_type_and_primaries_count,
    common_properties_log,
    print_results_table_stdout,
)
from redisbench_admin.run.git import git_vars_crosscheck
from redisbench_admin.run.grafana import generate_artifacts_table_grafana_redis
from redisbench_admin.run.modules import redis_modules_check
from redisbench_admin.run.redistimeseries import (
    timeseries_test_sucess_flow,
    timeseries_test_failure_flow,
)
from redisbench_admin.run.run import define_benchmark_plan
from redisbench_admin.run.s3 import get_test_s3_bucket_path
from redisbench_admin.run.ssh import ssh_pem_check
from redisbench_admin.run_remote.args import TF_OVERRIDE_NAME
from redisbench_admin.run_remote.consts import min_recommended_benchmark_duration
from redisbench_admin.run_remote.remote_client import run_remote_client_tool
from redisbench_admin.run_remote.remote_db import (
    remote_tmpdir_prune,
    remote_db_spin,
    db_error_artifacts,
)
from redisbench_admin.run_remote.remote_env import remote_env_setup
from redisbench_admin.run_remote.remote_failures import failed_remote_run_artifact_store
from redisbench_admin.run_remote.terraform import (
    terraform_destroy,
)
from redisbench_admin.utils.benchmark_config import (
    prepare_benchmark_definitions,
    get_metadata_tags,
    process_benchmark_definitions_remote_timeouts,
)
from redisbench_admin.utils.redisgraph_benchmark_go import setup_remote_benchmark_agent
from redisbench_admin.utils.remote import (
    get_run_full_filename,
    get_overall_dashboard_keynames,
    check_ec2_env,
    get_project_ts_tags,
    push_data_to_redistimeseries,
    fetch_remote_id_from_config,
)

from redisbench_admin.utils.utils import (
    EC2_PRIVATE_PEM,
    upload_artifacts_to_s3,
    EC2_ACCESS_KEY,
    EC2_SECRET_KEY,
    EC2_REGION,
)


# 7 days expire
STALL_INFO_DAYS = 7
EXPIRE_TIME_SECS_PROFILE_KEYS = 60 * 60 * 24 * STALL_INFO_DAYS
EXPIRE_TIME_MSECS_PROFILE_KEYS = EXPIRE_TIME_SECS_PROFILE_KEYS * 1000


def run_remote_command_logic(args, project_name, project_version):
    logging.info(
        "Using: {project_name} {project_version}".format(
            project_name=project_name, project_version=project_version
        )
    )
    tf_bin_path = args.terraform_bin_path
    tf_github_org = args.github_org
    tf_github_actor = args.github_actor
    tf_github_repo = args.github_repo
    tf_github_sha = args.github_sha
    tf_github_branch = args.github_branch
    required_modules = args.required_module

    (
        tf_github_actor,
        tf_github_branch,
        tf_github_org,
        tf_github_repo,
        tf_github_sha,
    ) = git_vars_crosscheck(
        tf_github_actor, tf_github_branch, tf_github_org, tf_github_repo, tf_github_sha
    )

    tf_triggering_env = args.triggering_env
    tf_setup_name_sufix = "{}-{}".format(args.setup_name_sufix, tf_github_sha)
    s3_bucket_name = args.s3_bucket_name
    local_module_files = args.module_path
    dbdir_folder = args.dbdir_folder
    private_key = args.private_key
    grafana_profile_dashboard = args.grafana_profile_dashboard
    profilers_enabled = args.enable_profilers
    keep_env_and_topo = args.keep_env_and_topo

    if args.skip_env_vars_verify is False:
        check_ec2_env()

    redis_modules_check(local_module_files)

    common_properties_log(
        tf_bin_path,
        tf_github_actor,
        tf_github_branch,
        tf_github_org,
        tf_github_repo,
        tf_github_sha,
        tf_setup_name_sufix,
        tf_triggering_env,
        private_key,
    )

    ssh_pem_check(EC2_PRIVATE_PEM, private_key)

    (
        benchmark_defs_result,
        benchmark_definitions,
        default_metrics,
        exporter_timemetric_path,
        default_specs,
        clusterconfig,
    ) = prepare_benchmark_definitions(args)

    return_code = 0
    if benchmark_defs_result is False:
        return_code = 1
        if args.fail_fast:
            logging.critical(
                "Detected errors while preparing benchmark definitions. Exiting right away!"
            )
            exit(1)

    remote_envs = {}
    dirname = "."
    (
        _,
        _,
        _,
        tsname_project_total_failures,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
    ) = get_overall_dashboard_keynames(tf_github_org, tf_github_repo, tf_triggering_env)
    rts = None
    allowed_tools = args.allowed_tools

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
        )
        rts.ping()

    remote_envs_timeout = process_benchmark_definitions_remote_timeouts(
        benchmark_definitions
    )

    for remote_id, termination_timeout_secs in remote_envs_timeout.items():
        logging.info(
            "Using a timeout of {} seconds for remote setup: {}".format(
                termination_timeout_secs, remote_id
            )
        )

    # we have a map of test-type, dataset-name, topology, test-name
    benchmark_runs_plan = define_benchmark_plan(benchmark_definitions, default_specs)

    profiler_dashboard_table_name = "Profiler dashboard links"
    profiler_dashboard_table_headers = ["Setup", "Test-case", "Grafana Dashboard"]
    profiler_dashboard_links = []

    benchmark_artifacts_table_name = "Benchmark client artifacts"
    benchmark_artifacts_table_headers = ["Setup", "Test-case", "Artifact", "link"]
    benchmark_artifacts_links = []

    # contains the overall target-tables ( if any target is defined )
    overall_tables = {}

    for benchmark_type, bench_by_dataset_map in benchmark_runs_plan.items():
        if return_code != 0 and args.fail_fast:
            logging.warning(
                "Given you've selected fail fast skipping benchmark_type {}".format(
                    benchmark_type
                )
            )
            continue
        logging.info("Running benchmarks of type {}.".format(benchmark_type))
        for (
            dataset_name,
            bench_by_dataset_and_setup_map,
        ) in bench_by_dataset_map.items():
            if return_code != 0 and args.fail_fast:
                logging.warning(
                    "Given you've selected fail fast skipping dataset {}".format(
                        dataset_name
                    )
                )
                continue
            logging.info("Running benchmarks for dataset {}.".format(dataset_name))
            for setup_name, setup_details in bench_by_dataset_and_setup_map.items():
                if return_code != 0 and args.fail_fast:
                    logging.warning(
                        "Given you've selected fail fast skipping setup {}".format(
                            setup_name
                        )
                    )
                    continue

                setup_settings = setup_details["setup_settings"]
                benchmarks_map = setup_details["benchmarks"]
                # we start with an empty per bench-type/setup-name
                setup_details["env"] = None

                # map from setup name to overall target-tables ( if any target is defined )
                overall_tables[setup_name] = {}

                for test_name, benchmark_config in benchmarks_map.items():
                    if return_code != 0 and args.fail_fast:
                        logging.warning(
                            "Given you've selected fail fast skipping test {}".format(
                                test_name
                            )
                        )
                        continue
                    metadata_tags = get_metadata_tags(benchmark_config)
                    logging.info(
                        "Including the extra metadata tags into this test generated time-series: {}".format(
                            metadata_tags
                        )
                    )
                    for repetition in range(1, BENCHMARK_REPETITIONS + 1):
                        if return_code != 0 and args.fail_fast:
                            logging.warning(
                                "Given you've selected fail fast skipping repetition {}".format(
                                    repetition
                                )
                            )
                            continue
                        remote_perf = None
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
                        s3_bucket_path = get_test_s3_bucket_path(
                            s3_bucket_name, test_name, tf_github_org, tf_github_repo
                        )
                        if setup_type in args.allowed_envs:

                            logging.info(
                                "Starting setup named {} of topology type {}. Total primaries: {}".format(
                                    setup_name, setup_type, shard_count
                                )
                            )
                            if "remote" in benchmark_config:
                                remote_id = fetch_remote_id_from_config(
                                    benchmark_config["remote"]
                                )
                                tf_timeout_secs = remote_envs_timeout[remote_id]
                                client_artifacts = []
                                client_artifacts_map = {}
                                temporary_dir = get_tmp_folder_rnd()
                                (
                                    client_public_ip,
                                    server_plaintext_port,
                                    server_private_ip,
                                    server_public_ip,
                                    db_ssh_port,
                                    client_ssh_port,
                                    username,
                                ) = remote_env_setup(
                                    args,
                                    benchmark_config,
                                    remote_envs,
                                    repetition,
                                    test_name,
                                    tf_bin_path,
                                    tf_github_actor,
                                    tf_github_org,
                                    tf_github_repo,
                                    tf_github_sha,
                                    tf_setup_name_sufix,
                                    tf_triggering_env,
                                    tf_timeout_secs,
                                    TF_OVERRIDE_NAME,
                                )

                                # after we've created the env, even on error we should always teardown
                                # in case of some unexpected error we fail the test
                                try:

                                    (
                                        _,
                                        _,
                                        testcase_start_time_str,
                                    ) = get_start_time_vars()
                                    logname = "{}_{}.log".format(
                                        test_name, testcase_start_time_str
                                    )
                                    remote_results_file = (
                                        "/tmp/benchmark-result-{}_{}.out".format(
                                            test_name, testcase_start_time_str
                                        )
                                    )

                                    logging.info(
                                        "Starting common steps to cluster and standalone..."
                                    )
                                    full_logfiles = []
                                    if setup_details["env"] is None:
                                        # ensure /tmp folder is free of benchmark data from previous runs
                                        remote_tmpdir_prune(
                                            server_public_ip,
                                            db_ssh_port,
                                            temporary_dir,
                                            username,
                                            private_key,
                                        )
                                        logging.info(
                                            "Starting setup named {} of topology type {}. Total primaries: {}".format(
                                                setup_name, setup_type, shard_count
                                            )
                                        )
                                        (
                                            artifact_version,
                                            cluster_enabled,
                                            dataset_load_duration_seconds,
                                            full_logfiles,
                                            redis_conns,
                                            return_code,
                                            server_plaintext_port,
                                            ssh_tunnel,
                                        ) = remote_db_spin(
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
                                        )

                                        if benchmark_type == "read-only":
                                            ro_benchmark_set(
                                                artifact_version,
                                                cluster_enabled,
                                                dataset_load_duration_seconds,
                                                redis_conns,
                                                return_code,
                                                server_plaintext_port,
                                                setup_details,
                                                ssh_tunnel,
                                                full_logfiles,
                                            )
                                    else:
                                        (
                                            artifact_version,
                                            cluster_enabled,
                                            dataset_load_duration_seconds,
                                            full_logfiles,
                                            redis_conns,
                                            return_code,
                                            server_plaintext_port,
                                            ssh_tunnel,
                                        ) = ro_benchmark_reuse(
                                            artifact_version,
                                            benchmark_type,
                                            cluster_enabled,
                                            dataset_load_duration_seconds,
                                            full_logfiles,
                                            redis_conns,
                                            return_code,
                                            server_plaintext_port,
                                            setup_details,
                                            ssh_tunnel,
                                        )

                                    if profilers_enabled:
                                        setup_remote_benchmark_agent(
                                            server_public_ip,
                                            username,
                                            private_key,
                                            db_ssh_port,
                                        )

                                    (
                                        start_time,
                                        start_time_ms,
                                        start_time_str,
                                    ) = get_start_time_vars()

                                    local_bench_fname = get_run_full_filename(
                                        start_time_str,
                                        setup_name,
                                        tf_github_org,
                                        tf_github_repo,
                                        tf_github_branch,
                                        test_name,
                                        tf_github_sha,
                                    )
                                    if profilers_enabled:
                                        remote_perf = PerfDaemonRemoteCaller(
                                            "{}:5000".format(server_public_ip),
                                            test_name=test_name,
                                            setup_name=setup_name,
                                            github_actor=tf_github_actor,
                                            github_branch=tf_github_branch,
                                            github_repo_name=tf_github_repo,
                                            github_org_name=tf_github_org,
                                            github_sha=tf_github_sha,
                                            aws_access_key_id=EC2_ACCESS_KEY,
                                            aws_secret_access_key=EC2_SECRET_KEY,
                                            region_name=EC2_REGION,
                                        )
                                        primary_one_pid = redis_conns[0].info()[
                                            "process_id"
                                        ]
                                        start_profile_result = (
                                            remote_perf.start_profile(
                                                primary_one_pid,
                                                "",
                                                PROFILE_FREQ,
                                                PERF_CALLGRAPH_MODE,
                                            )
                                        )
                                        if start_profile_result is True:
                                            logging.info(
                                                "Successfully started remote profile for Redis with PID: {}. Used call-graph mode {}".format(
                                                    primary_one_pid, PERF_CALLGRAPH_MODE
                                                )
                                            )

                                    logging.info(
                                        "Will store benchmark json output to local file {}".format(
                                            local_bench_fname
                                        )
                                    )

                                    (
                                        artifact_version,
                                        benchmark_duration_seconds,
                                        local_bench_fname,
                                        remote_run_result,
                                        results_dict,
                                        return_code,
                                        client_output_artifacts,
                                    ) = run_remote_client_tool(
                                        allowed_tools,
                                        artifact_version,
                                        benchmark_config,
                                        client_public_ip,
                                        cluster_enabled,
                                        local_bench_fname,
                                        remote_results_file,
                                        return_code,
                                        server_plaintext_port,
                                        server_private_ip,
                                        start_time_ms,
                                        start_time_str,
                                        username,
                                        "clientconfig",
                                        "linux",
                                        "amd64",
                                        "Benchmark",
                                        min_recommended_benchmark_duration,
                                        client_ssh_port,
                                        private_key,
                                        True,
                                        redis_conns,
                                    )

                                    if profilers_enabled:
                                        logging.info("Stopping remote profiler")
                                        profiler_result = remote_perf.stop_profile()
                                        if profiler_result is False:
                                            logging.error(
                                                "Unsuccessful profiler stop."
                                                + " Fetching remote perf-daemon logfile {}".format(
                                                    PERF_DAEMON_LOGNAME
                                                )
                                            )
                                            failed_remote_run_artifact_store(
                                                args.upload_results_s3,
                                                server_public_ip,
                                                dirname,
                                                PERF_DAEMON_LOGNAME,
                                                logname,
                                                s3_bucket_name,
                                                s3_bucket_path,
                                                username,
                                                private_key,
                                            )
                                            return_code |= 1
                                        (
                                            perf_stop_status,
                                            profile_artifacts,
                                            _,
                                        ) = remote_perf.generate_outputs(test_name)
                                        if len(profile_artifacts) == 0:
                                            logging.error(
                                                "No profiler artifact was retrieved"
                                            )
                                        else:
                                            https_link = (
                                                generate_artifacts_table_grafana_redis(
                                                    args,
                                                    grafana_profile_dashboard,
                                                    profile_artifacts,
                                                    rts,
                                                    setup_name,
                                                    start_time_ms,
                                                    start_time_str,
                                                    test_name,
                                                    tf_github_org,
                                                    tf_github_repo,
                                                    tf_github_sha,
                                                    tf_github_branch,
                                                )
                                            )
                                            profiler_dashboard_links.append(
                                                [
                                                    setup_name,
                                                    test_name,
                                                    " {} ".format(https_link),
                                                ]
                                            )
                                            logging.info(
                                                "Published new profile info for this testcase. Access it via: {}".format(
                                                    https_link
                                                )
                                            )

                                    (
                                        total_shards_cpu_usage,
                                        cpu_usage_map,
                                    ) = from_info_to_overall_shard_cpu(
                                        redisbench_admin.run.metrics.BENCHMARK_CPU_STATS_GLOBAL
                                    )
                                    if total_shards_cpu_usage is None:
                                        total_shards_cpu_usage_str = "n/a"
                                    else:
                                        total_shards_cpu_usage_str = "{:.3f}".format(
                                            total_shards_cpu_usage
                                        )
                                    logging.info(
                                        "Total CPU usage ({} %)".format(
                                            total_shards_cpu_usage_str
                                        )
                                    )

                                    if remote_run_result is False:
                                        db_error_artifacts(
                                            db_ssh_port,
                                            dirname,
                                            full_logfiles,
                                            logname,
                                            private_key,
                                            s3_bucket_name,
                                            s3_bucket_path,
                                            server_public_ip,
                                            temporary_dir,
                                            args.upload_results_s3,
                                            username,
                                        )
                                        return_code |= 1
                                        raise Exception(
                                            "Failed to run remote benchmark."
                                        )

                                    else:
                                        if args.push_results_redistimeseries and (
                                            artifact_version is not None
                                            or tf_github_branch == "master"
                                        ):
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
                                            if total_shards_cpu_usage is not None:
                                                overall_end_time_metrics[
                                                    "total_shards_used_cpu_pct"
                                                ] = total_shards_cpu_usage
                                            expire_ms = 7 * 24 * 60 * 60 * 1000
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
                                                expire_ms,
                                            )
                                            if args.collect_commandstats:
                                                (
                                                    end_time_ms,
                                                    _,
                                                    overall_commandstats_metrics,
                                                ) = collect_redis_metrics(
                                                    redis_conns, ["commandstats"]
                                                )
                                                export_redis_metrics(
                                                    artifact_version,
                                                    end_time_ms,
                                                    overall_commandstats_metrics,
                                                    rts,
                                                    setup_name,
                                                    setup_type,
                                                    test_name,
                                                    tf_github_branch,
                                                    tf_github_org,
                                                    tf_github_repo,
                                                    tf_triggering_env,
                                                    {"metric-type": "commandstats"},
                                                    expire_ms,
                                                )

                                        if setup_details["env"] is None:
                                            if keep_env_and_topo is False:
                                                shutdown_remote_redis(
                                                    redis_conns, ssh_tunnel
                                                )
                                            else:
                                                logging.info(
                                                    "Keeping environment and topology active upon request."
                                                )
                                                logging.info(
                                                    "client_public_ip = {}".format(
                                                        client_public_ip
                                                    )
                                                )
                                                logging.info(
                                                    "server_public_ip = {}".format(
                                                        server_private_ip
                                                    )
                                                )
                                                logging.info(
                                                    "server_private_ip = {}".format(
                                                        server_public_ip
                                                    )
                                                )

                                        (
                                            _,
                                            branch_target_tables,
                                        ) = timeseries_test_sucess_flow(
                                            args.push_results_redistimeseries,
                                            artifact_version,
                                            benchmark_config,
                                            benchmark_duration_seconds,
                                            dataset_load_duration_seconds,
                                            default_metrics,
                                            setup_name,
                                            setup_type,
                                            exporter_timemetric_path,
                                            results_dict,
                                            rts,
                                            start_time_ms,
                                            test_name,
                                            tf_github_branch,
                                            tf_github_org,
                                            tf_github_repo,
                                            tf_triggering_env,
                                            metadata_tags,
                                        )
                                        if branch_target_tables is not None:
                                            for (
                                                branch_tt_keyname,
                                                branch_target_table,
                                            ) in branch_target_tables.items():
                                                if (
                                                    "contains-target"
                                                    not in branch_target_table
                                                ):
                                                    continue
                                                if (
                                                    branch_target_table[
                                                        "contains-target"
                                                    ]
                                                    is True
                                                ):
                                                    row = []
                                                    metric_name = branch_target_table[
                                                        "metric-name"
                                                    ]
                                                    header = []
                                                    for (
                                                        k,
                                                        v,
                                                    ) in branch_target_table.items():
                                                        if k != "contains-target":
                                                            header.append(k)
                                                            row.append(v)
                                                    if (
                                                        metric_name
                                                        not in overall_tables[
                                                            setup_name
                                                        ]
                                                    ):
                                                        overall_tables[setup_name][
                                                            metric_name
                                                        ] = {
                                                            "header": header,
                                                            "rows": [row],
                                                        }
                                                    else:
                                                        assert (
                                                            header
                                                            == overall_tables[
                                                                setup_name
                                                            ][metric_name]["header"]
                                                        )
                                                        overall_tables[setup_name][
                                                            metric_name
                                                        ]["rows"].append(row)

                                        print_results_table_stdout(
                                            benchmark_config,
                                            default_metrics,
                                            results_dict,
                                            setup_name,
                                            test_name,
                                            total_shards_cpu_usage,
                                        )
                                    client_artifacts.append(local_bench_fname)
                                    client_artifacts.extend(client_output_artifacts)

                                    if args.upload_results_s3:
                                        logging.info(
                                            "Uploading CLIENT results to s3. s3 bucket name: {}. s3 bucket path: {}".format(
                                                s3_bucket_name, s3_bucket_path
                                            )
                                        )
                                        client_artifacts_map = upload_artifacts_to_s3(
                                            client_artifacts,
                                            s3_bucket_name,
                                            s3_bucket_path,
                                        )

                                    benchmark_artifacts_table_headers = [
                                        "Setup",
                                        "Test-case",
                                        "Artifact",
                                        "link",
                                    ]
                                    for client_artifact in client_artifacts:
                                        client_artifact_link = "- n/a -"
                                        if client_artifact in client_artifacts_map:
                                            client_artifact_link = client_artifacts_map[
                                                client_artifact
                                            ]
                                        benchmark_artifacts_links.append(
                                            [
                                                setup_name,
                                                test_name,
                                                client_artifact,
                                                " {} ".format(client_artifact_link),
                                            ]
                                        )

                                except KeyboardInterrupt:
                                    logging.critical(
                                        "Detected Keyboard interruput...Destroy all remote envs and exiting right away!"
                                    )
                                    if args.inventory is None:
                                        terraform_destroy(
                                            remote_envs, keep_env_and_topo
                                        )
                                    exit(1)
                                except:
                                    (
                                        start_time,
                                        start_time_ms,
                                        start_time_str,
                                    ) = get_start_time_vars()
                                    timeseries_test_failure_flow(
                                        args,
                                        setup_name,
                                        setup_type,
                                        rts,
                                        start_time_ms,
                                        tf_github_org,
                                        tf_github_repo,
                                        tf_triggering_env,
                                        tsname_project_total_failures,
                                    )
                                    return_code |= 1
                                    logging.critical(
                                        "Some unexpected exception was caught "
                                        "during remote work. Failing test...."
                                    )
                                    logging.critical(sys.exc_info()[0])
                                    print("-" * 60)
                                    traceback.print_exc(file=sys.stdout)
                                    print("-" * 60)

                            else:
                                logging.info(
                                    "Test {} does not have remote config. Skipping test.".format(
                                        test_name
                                    )
                                )

    if len(benchmark_artifacts_links) > 0:
        writer = MarkdownTableWriter(
            table_name=benchmark_artifacts_table_name,
            headers=benchmark_artifacts_table_headers,
            value_matrix=benchmark_artifacts_links,
        )
        writer.write_table()

    if args.enable_profilers:
        writer = MarkdownTableWriter(
            table_name=profiler_dashboard_table_name,
            headers=profiler_dashboard_table_headers,
            value_matrix=profiler_dashboard_links,
        )
        writer.write_table()
    if args.inventory is None:
        terraform_destroy(remote_envs, keep_env_and_topo)

    if args.push_results_redistimeseries:
        for setup_name, setup_target_table in overall_tables.items():
            for metric_name, metric_target_dict in setup_target_table.items():
                target_tables_latest_key = "target_tables:by.branch/{branch}/{org}/{repo}/{setup}/{metric}:latest".format(
                    branch=tf_github_branch,
                    org=tf_github_org,
                    repo=tf_github_repo,
                    setup=setup_name,
                    metric=metric_name,
                )
                table_name = (
                    "Target table for setup {} and metric {}. branch={}".format(
                        setup_name, metric_name, tf_github_branch
                    )
                )
                logging.info(
                    "Populating overall {}. Used key={}".format(
                        table_name, target_tables_latest_key
                    )
                )
                headers = metric_target_dict["header"]
                matrix_html = metric_target_dict["rows"]
                htmlwriter = pytablewriter.HtmlTableWriter(
                    table_name=table_name,
                    headers=headers,
                    value_matrix=matrix_html,
                )
                profile_markdown_str = htmlwriter.dumps()
                profile_markdown_str = profile_markdown_str.replace("\n", "")
                rts.setex(
                    target_tables_latest_key,
                    EXPIRE_TIME_SECS_PROFILE_KEYS,
                    profile_markdown_str,
                )
    exit(return_code)


def get_tmp_folder_rnd():
    temporary_dir = "/tmp/{}".format(
        "".join(random.choice(string.ascii_lowercase) for i in range(20))
    )
    return temporary_dir


def ro_benchmark_reuse(
    artifact_version,
    benchmark_type,
    cluster_enabled,
    dataset_load_duration_seconds,
    full_logfiles,
    redis_conns,
    return_code,
    server_plaintext_port,
    setup_details,
    ssh_tunnel,
):
    assert benchmark_type == "read-only"
    logging.info(
        "Given the benchmark for this setup is ready-only, and this setup was already spinned we will reuse the previous, conns and process info."
    )
    artifact_version = setup_details["env"]["artifact_version"]
    cluster_enabled = setup_details["env"]["cluster_enabled"]
    dataset_load_duration_seconds = setup_details["env"][
        "dataset_load_duration_seconds"
    ]
    full_logfiles = setup_details["env"]["full_logfiles"]
    redis_conns = setup_details["env"]["redis_conns"]
    return_code = setup_details["env"]["return_code"]
    server_plaintext_port = setup_details["env"]["server_plaintext_port"]
    ssh_tunnel = setup_details["env"]["ssh_tunnel"]
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


def ro_benchmark_set(
    artifact_version,
    cluster_enabled,
    dataset_load_duration_seconds,
    redis_conns,
    return_code,
    server_plaintext_port,
    setup_details,
    ssh_tunnel,
    full_logfiles,
):
    logging.info(
        "Given the benchmark for this setup is ready-only we will prepare to reuse it on the next read-only benchmarks (if any )."
    )
    setup_details["env"] = {}
    setup_details["env"]["full_logfiles"] = full_logfiles

    setup_details["env"]["artifact_version"] = artifact_version
    setup_details["env"]["cluster_enabled"] = cluster_enabled
    setup_details["env"][
        "dataset_load_duration_seconds"
    ] = dataset_load_duration_seconds
    setup_details["env"]["redis_conns"] = redis_conns
    setup_details["env"]["return_code"] = return_code
    setup_details["env"]["server_plaintext_port"] = server_plaintext_port
    setup_details["env"]["ssh_tunnel"] = ssh_tunnel


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
        "Adding a total of {} server side metrics collected at the end of benchmark".format(
            len(list(overall_end_time_metrics.items()))
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
            tsname_metric = "{}/{}/{}/benchmark_end/{}".format(
                sprefix,
                test_name,
                by_variant,
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
            variant_labels_dict["test_name"] = test_name
            variant_labels_dict["metric"] = metric_name
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


def shutdown_remote_redis(redis_conns, ssh_tunnel):
    logging.info("Shutting down remote redis.")
    for conn in redis_conns:
        conn.shutdown(save=False)
    ssh_tunnel.close()  # Close the tunnel
