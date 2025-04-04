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
from redisbench_admin.run.args import PROFILE_FREQ, VALID_ARCHS
from redisbench_admin.run.common import (
    get_start_time_vars,
    BENCHMARK_REPETITIONS,
    CIRCLE_BUILD_URL,
    CIRCLE_JOB,
    WH_TOKEN,
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
from redisbench_admin.run_remote.args import TF_OVERRIDE_NAME, TF_OVERRIDE_REMOTE
from redisbench_admin.run_remote.consts import min_recommended_benchmark_duration
from redisbench_admin.run_remote.notifications import generate_failure_notification
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
    make_dashboard_callback,
)

from slack_sdk.webhook import WebhookClient

# 7 days expire
STALL_INFO_DAYS = 7
EXPIRE_TIME_SECS_PROFILE_KEYS = 60 * 60 * 24 * STALL_INFO_DAYS
EXPIRE_TIME_MSECS_PROFILE_KEYS = EXPIRE_TIME_SECS_PROFILE_KEYS * 1000


def is_important_data(tf_github_branch, artifact_version):
    return True


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
    collect_commandstats = args.collect_commandstats

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
    for pos, module_file in enumerate(local_module_files):
        if " " in module_file:
            logging.info(
                "Detected multiple files in single module path {}".format(module_file)
            )
            local_module_files[pos] = module_file.split(" ")
    dbdir_folder = args.dbdir_folder
    private_key = args.private_key
    grafana_profile_dashboard = args.grafana_profile_dashboard
    profilers_enabled = args.enable_profilers
    keep_env_and_topo = args.keep_env_and_topo
    skip_remote_db_setup = args.skip_db_setup
    flushall_on_every_test_start = args.flushall_on_every_test_start
    redis_7 = True
    cluster_start_port = 20000
    redis_password = args.db_pass
    ignore_keyspace_errors = args.ignore_keyspace_errors
    if WH_TOKEN is not None:
        webhook_notifications_active = True

    webhook_url = "https://hooks.slack.com/services/{}".format(WH_TOKEN)

    ci_job_link = CIRCLE_BUILD_URL
    ci_job_name = CIRCLE_JOB
    failure_reason = ""
    webhook_client_slack = None
    if ci_job_link is None:
        webhook_notifications_active = False
        logging.warning(
            "Disabling webhook notificaitons given CIRCLE_BUILD_URL is None"
        )

    if webhook_notifications_active is True:
        logging.info(
            "Detected where in a CI flow named {}. Here's the reference link: {}".format(
                ci_job_name, ci_job_link
            )
        )
        webhook_client_slack = WebhookClient(webhook_url)

    if args.skip_env_vars_verify is False:
        env_check_status, failure_reason = check_ec2_env()
        if env_check_status is False:
            if webhook_notifications_active:
                generate_failure_notification(
                    webhook_client_slack,
                    ci_job_name,
                    ci_job_link,
                    failure_reason,
                    tf_github_org,
                    tf_github_repo,
                    tf_github_branch,
                    None,
                )
            logging.critical("{}. Exiting right away!".format(failure_reason))
            exit(1)

    continue_on_module_check_error = args.continue_on_module_check_error
    module_check_status, error_message = redis_modules_check(local_module_files)
    if module_check_status is False:
        if continue_on_module_check_error is False:
            if webhook_notifications_active:
                failure_reason = error_message
                generate_failure_notification(
                    webhook_client_slack,
                    ci_job_name,
                    ci_job_link,
                    failure_reason,
                    tf_github_org,
                    tf_github_repo,
                    tf_github_branch,
                    None,
                )
            exit(1)
        else:
            logging.error(
                "the module check failed with the following message {} but you've decided to continue anyway.".format(
                    error_message
                )
            )

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
            failure_reason = "Detected errors while preparing benchmark definitions"
            logging.critical("{}. Exiting right away!".format(failure_reason))
            if webhook_notifications_active:
                generate_failure_notification(
                    webhook_client_slack,
                    ci_job_name,
                    ci_job_link,
                    failure_reason,
                    tf_github_org,
                    tf_github_repo,
                    tf_github_branch,
                    None,
                )

            exit(return_code)

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
            retry_on_timeout=True,
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
    architecture = args.architecture
    if architecture not in VALID_ARCHS:
        logging.critical(
            f"The specified architecture {architecture} is not valid. Specify one of {VALID_ARCHS}"
        )
        exit(1)
    else:
        logging.info(f"Running benchmark for architecture {architecture}")

    # contains the overall target-tables ( if any target is defined )
    overall_tables = {}

    # Used to only deploy spot once per run
    spot_instance_error = False
    ts_key_spot_price = f"ts:{tf_triggering_env}:tests:spot_price"
    ts_key_full_price = f"ts:{tf_triggering_env}:tests:full_price"
    ts_key_architecture = f"ts:{tf_triggering_env}:tests:arch:{architecture}"

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
                    if "arch" not in metadata_tags:
                        metadata_tags["arch"] = architecture
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
                                    spot_instance_error,
                                    spot_price_counter,
                                    full_price_counter,
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
                                    TF_OVERRIDE_REMOTE,
                                    spot_instance_error,
                                    0,
                                    0,
                                    architecture,
                                )

                                # after we've created the env, even on error we should always teardown
                                # in case of some unexpected error we fail the test
                                try:
                                    (
                                        _,
                                        start_time_setup_ms,
                                        testcase_start_time_str,
                                    ) = get_start_time_vars()
                                    if args.push_results_redistimeseries:
                                        logging.info(
                                            f"Updating overall arch tests counter {ts_key_architecture}"
                                        )
                                        rts.ts().add(
                                            ts_key_architecture,
                                            start_time_setup_ms,
                                            1,
                                            duplicate_policy="sum",
                                        )
                                        logging.info(
                                            f"Updating overall spot price tests counter {ts_key_spot_price}"
                                        )
                                        rts.ts().add(
                                            ts_key_spot_price,
                                            start_time_setup_ms,
                                            spot_price_counter,
                                            duplicate_policy="sum",
                                        )
                                        logging.info(
                                            f"Updating overall spot price full counter {ts_key_spot_price}"
                                        )
                                        rts.ts().add(
                                            ts_key_full_price,
                                            start_time_setup_ms,
                                            full_price_counter,
                                            duplicate_policy="sum",
                                        )
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
                                        if skip_remote_db_setup is False:
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
                                            redis_7,
                                            skip_remote_db_setup,
                                            cluster_start_port,
                                            redis_password,
                                            flushall_on_every_test_start,
                                            ignore_keyspace_errors,
                                            continue_on_module_check_error,
                                            60,
                                            architecture,
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

                                    for pos, redis_conn in enumerate(redis_conns):
                                        logging.info(
                                            "Resetting commmandstats for shard {}".format(
                                                pos
                                            )
                                        )
                                        try:
                                            redis_conn.config_resetstat()
                                        except redis.exceptions.ResponseError as e:
                                            logging.warning(
                                                "Catched an error while resetting status: {}".format(
                                                    e.__str__()
                                                )
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
                                        True,
                                        redis_password,
                                        architecture,
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
                                                    args.push_results_redistimeseries,
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

                                    total_shards_cpu_usage = None
                                    if skip_remote_db_setup is False:
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
                                        if (
                                            args.push_results_redistimeseries
                                            and is_important_data(
                                                tf_github_branch, artifact_version
                                            )
                                        ):
                                            try:
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
                                                    {
                                                        "metric-type": "redis-metrics",
                                                        "arch": architecture,
                                                    },
                                                    expire_ms,
                                                )
                                                if collect_commandstats:
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
                                                        {
                                                            "metric-type": "commandstats",
                                                            "arch": architecture,
                                                        },
                                                        expire_ms,
                                                    )
                                                    (
                                                        end_time_ms,
                                                        _,
                                                        overall_commandstats_metrics,
                                                    ) = collect_redis_metrics(
                                                        redis_conns, ["latencystats"]
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
                                                        {
                                                            "metric-type": "latencystats",
                                                            "arch": architecture,
                                                        },
                                                        expire_ms,
                                                    )
                                            except (
                                                redis.exceptions.ConnectionError
                                            ) as e:
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
                                                    "Failed to run remote benchmark. {}".format(
                                                        e.__str__()
                                                    )
                                                )

                                        if setup_details["env"] is None:
                                            if (
                                                keep_env_and_topo is False
                                                and skip_remote_db_setup is False
                                            ):
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
                                            setup_type,
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
                                    failure_reason = "Some unexpected exception was caught during remote work on test named {}".format(
                                        test_name
                                    )
                                    logging.critical(
                                        "{}. Failing test....".format(failure_reason)
                                    )

                                    logging.critical(sys.exc_info()[0])
                                    print("-" * 60)
                                    traceback.print_exc(file=sys.stdout)
                                    print("-" * 60)

                            else:
                                logging.info(
                                    f"Test {test_name} does not have remote config. Skipping test."
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

    if return_code != 0 and webhook_notifications_active:
        if failure_reason == "":
            failure_reason = "Some unexpected exception was caught during remote work"
        generate_failure_notification(
            webhook_client_slack,
            ci_job_name,
            ci_job_link,
            failure_reason,
            tf_github_org,
            tf_github_repo,
            tf_github_branch,
            None,
        )
    if args.callback:
        make_dashboard_callback(
            args.callback_url,
            return_code,
            ci_job_name,
            tf_github_repo,
            tf_github_branch,
            tf_github_sha,
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


def shutdown_remote_redis(redis_conns, ssh_tunnel):
    logging.info("Shutting down remote redis.")
    for conn in redis_conns:
        conn.shutdown(save=False)
    ssh_tunnel.close()  # Close the tunnel
