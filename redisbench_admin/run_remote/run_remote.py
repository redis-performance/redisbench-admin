#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import logging
import sys
import traceback

from redistimeseries.client import Client

from redisbench_admin.run.common import (
    get_start_time_vars,
    BENCHMARK_REPETITIONS,
    get_setup_type_and_primaries_count,
    common_properties_log,
)
from redisbench_admin.run.git import git_vars_crosscheck
from redisbench_admin.run.modules import redis_modules_check
from redisbench_admin.run.redistimeseries import (
    timeseries_test_sucess_flow,
    timeseries_test_failure_flow,
)
from redisbench_admin.run.run import define_benchmark_plan
from redisbench_admin.run.s3 import get_test_s3_bucket_path
from redisbench_admin.run.ssh import ssh_pem_check
from redisbench_admin.run_remote.consts import min_recommended_benchmark_duration
from redisbench_admin.run_remote.remote_client import run_remote_client_tool
from redisbench_admin.run_remote.remote_db import remote_tmpdir_prune, remote_db_spin
from redisbench_admin.run_remote.remote_env import remote_env_setup
from redisbench_admin.run_remote.remote_failures import failed_remote_run_artifact_store
from redisbench_admin.run_remote.terraform import (
    terraform_destroy,
)
from redisbench_admin.utils.benchmark_config import (
    prepare_benchmark_definitions,
)
from redisbench_admin.utils.remote import (
    get_run_full_filename,
    get_overall_dashboard_keynames,
    check_ec2_env,
)

from redisbench_admin.utils.utils import (
    EC2_PRIVATE_PEM,
    upload_artifacts_to_s3,
)


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
        benchmark_definitions,
        default_metrics,
        exporter_timemetric_path,
        default_specs,
        clusterconfig,
    ) = prepare_benchmark_definitions(args)

    return_code = 0
    remote_envs = {}
    dirname = "."
    (
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
        logging.info("Checking connection to RedisTimeSeries.")
        rts = Client(
            host=args.redistimeseries_host,
            port=args.redistimeseries_port,
            password=args.redistimeseries_pass,
        )
        rts.redis.ping()

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
                                temporary_dir = "/tmp"
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
                                        )
                                        if benchmark_type == "read-only":
                                            logging.info(
                                                "Given the benchmark for this setup is ready-only we will prepare to reuse it on the next read-only benchmarks (if any )."
                                            )
                                            setup_details["env"] = {}
                                            setup_details["env"][
                                                "artifact_version"
                                            ] = artifact_version
                                            setup_details["env"][
                                                "cluster_enabled"
                                            ] = cluster_enabled
                                            setup_details["env"][
                                                "dataset_load_duration_seconds"
                                            ] = dataset_load_duration_seconds

                                            setup_details["env"][
                                                "full_logfiles"
                                            ] = full_logfiles

                                            setup_details["env"][
                                                "redis_conns"
                                            ] = redis_conns

                                            setup_details["env"][
                                                "return_code"
                                            ] = return_code
                                            setup_details["env"][
                                                "server_plaintext_port"
                                            ] = server_plaintext_port

                                            setup_details["env"][
                                                "ssh_tunnel"
                                            ] = ssh_tunnel

                                    else:
                                        assert benchmark_type == "read-only"
                                        logging.info(
                                            "Given the benchmark for this setup is ready-only, and this setup was already spinned we will reuse the previous, conns and process info."
                                        )

                                        artifact_version = setup_details["env"][
                                            "artifact_version"
                                        ]
                                        cluster_enabled = setup_details["env"][
                                            "cluster_enabled"
                                        ]
                                        dataset_load_duration_seconds = setup_details[
                                            "env"
                                        ]["dataset_load_duration_seconds"]
                                        full_logfiles = setup_details["env"][
                                            "full_logfiles"
                                        ]
                                        redis_conns = setup_details["env"][
                                            "redis_conns"
                                        ]
                                        return_code = setup_details["env"][
                                            "return_code"
                                        ]
                                        server_plaintext_port = setup_details["env"][
                                            "server_plaintext_port"
                                        ]
                                        ssh_tunnel = setup_details["env"]["ssh_tunnel"]

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
                                    )

                                    if setup_details["env"] is None:
                                        if args.keep_env_and_topo is False:
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

                                    if remote_run_result is False:
                                        failed_remote_run_artifact_store(
                                            args,
                                            client_public_ip,
                                            dirname,
                                            full_logfiles[0],
                                            logname,
                                            s3_bucket_name,
                                            s3_bucket_path,
                                            username,
                                            private_key,
                                        )

                                    if args.upload_results_s3:
                                        logging.info(
                                            "Uploading results to s3. s3 bucket name: {}. s3 bucket path: {}".format(
                                                s3_bucket_name, s3_bucket_path
                                            )
                                        )
                                        artifacts = [local_bench_fname]
                                        upload_artifacts_to_s3(
                                            artifacts, s3_bucket_name, s3_bucket_path
                                        )

                                    timeseries_test_sucess_flow(
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
                                    )

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
    if args.inventory is None:
        terraform_destroy(remote_envs)
    exit(return_code)


def shutdown_remote_redis(redis_conns, ssh_tunnel):
    logging.info("Shutting down remote redis.")
    for conn in redis_conns:
        conn.shutdown(save=False)
    ssh_tunnel.close()  # Close the tunnel
