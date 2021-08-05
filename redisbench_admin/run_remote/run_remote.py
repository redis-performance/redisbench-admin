#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import datetime
import logging
import os
import sys
import traceback

from rediscluster.client import RedisCluster
from redistimeseries.client import Client

# from redisbench_admin.cli import populate_with_poetry_data
from redisbench_admin.run.cluster import (
    spin_up_redis_cluster_remote_redis,
    cluster_init_steps,
)
from redisbench_admin.run.common import (
    prepare_benchmark_parameters,
    run_remote_benchmark,
    get_start_time_vars,
    BENCHMARK_REPETITIONS,
    extract_test_feasible_setups,
    get_setup_type_and_primaries_count,
    run_redis_pre_steps,
)
from redisbench_admin.run.git import git_vars_crosscheck
from redisbench_admin.run.redistimeseries import (
    timeseries_test_sucess_flow,
    timeseries_test_failure_flow,
)
from redisbench_admin.run.run import calculate_benchmark_duration_and_check
from redisbench_admin.run.s3 import get_test_s3_bucket_path
from redisbench_admin.run.ssh import ssh_tunnel_redisconn, ssh_pem_check
from redisbench_admin.run_remote.consts import (
    remote_dataset_file,
    remote_module_file,
    private_key,
)
from redisbench_admin.run_remote.remote_failures import failed_remote_run_artifact_store
from redisbench_admin.run_remote.remote_helpers import (
    benchmark_tools_sanity_check,
    remote_tool_pre_bench_step,
    post_process_remote_run,
)
from redisbench_admin.run_remote.terraform import (
    terraform_spin_or_reuse_env,
    terraform_destroy,
)
from redisbench_admin.utils.benchmark_config import (
    extract_benchmark_tool_settings,
    prepare_benchmark_definitions,
    extract_redis_dbconfig_parameters,
)
from redisbench_admin.run_remote.standalone import spin_up_standalone_remote_redis
from redisbench_admin.utils.remote import (
    get_run_full_filename,
    get_overall_dashboard_keynames,
    check_ec2_env,
    execute_remote_commands,
)

from redisbench_admin.utils.utils import (
    wait_for_conn,
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
    local_module_file = args.module_path
    dbdir_folder = args.dbdir_folder

    if args.skip_env_vars_verify is False:
        check_ec2_env()

    logging.info("Using the following module artifact: {}".format(local_module_file))
    logging.info("Checking if module artifact exists...")
    if os.path.exists(local_module_file) is False:
        logging.error(
            "Specified module artifact does not exist: {}".format(local_module_file)
        )
        exit(1)
    else:
        logging.info(
            "Confirmed that module artifact: '{}' exists!".format(local_module_file)
        )

    logging.info("Using the following vars on terraform deployment:")
    logging.info("\tterraform bin path: {}".format(tf_bin_path))
    logging.info("\tgithub_actor: {}".format(tf_github_actor))
    logging.info("\tgithub_org: {}".format(tf_github_org))
    logging.info("\tgithub_repo: {}".format(tf_github_repo))
    logging.info("\tgithub_branch: {}".format(tf_github_branch))
    if tf_github_branch is None or tf_github_branch == "":
        logging.error(
            "The github branch information is not present!"
            " This implies that per-branch data is not pushed to the exporters!"
        )
    logging.info("\tgithub_sha: {}".format(tf_github_sha))
    logging.info("\ttriggering env: {}".format(tf_triggering_env))
    logging.info("\tprivate_key path: {}".format(private_key))
    logging.info("\tsetup_name sufix: {}".format(tf_setup_name_sufix))

    ssh_pem_check(EC2_PRIVATE_PEM)

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
        prefix,
        testcases_setname,
        tsname_project_total_failures,
        tsname_project_total_success,
    ) = get_overall_dashboard_keynames(tf_github_org, tf_github_repo, tf_triggering_env)
    rts = None
    if args.push_results_redistimeseries:
        logging.info("Checking connection to RedisTimeSeries.")
        rts = Client(
            host=args.redistimeseries_host,
            port=args.redistimeseries_port,
            password=args.redistimeseries_pass,
        )
        rts.redis.ping()
    for repetition in range(1, BENCHMARK_REPETITIONS + 1):
        for test_name, benchmark_config in benchmark_definitions.items():
            s3_bucket_path = get_test_s3_bucket_path(
                s3_bucket_name, test_name, tf_github_org, tf_github_repo
            )
            test_setups = extract_test_feasible_setups(
                benchmark_config, "setups", default_specs
            )
            for setup_name, setup_settings in test_setups.items():
                setup_type, shard_count = get_setup_type_and_primaries_count(
                    setup_settings
                )
                if setup_type in args.allowed_envs:
                    logging.info(
                        "Starting setup named {} of topology type {}. Total primaries: {}".format(
                            setup_name, setup_type, shard_count
                        )
                    )
                    if "remote" in benchmark_config:
                        (
                            client_public_ip,
                            deployment_type,
                            server_plaintext_port,
                            server_private_ip,
                            server_public_ip,
                            username,
                        ) = terraform_spin_or_reuse_env(
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
                            # ensure /tmp folder is free of benchmark data from previous runs
                            remote_working_folder = "/tmp"
                            execute_remote_commands(
                                server_public_ip,
                                username,
                                private_key,
                                [
                                    "rm -rf {}/*.log".format(remote_working_folder),
                                    "rm -rf {}/*.rdb".format(remote_working_folder),
                                    "rm -rf {}/*.out".format(remote_working_folder),
                                    "rm -rf {}/*.data".format(remote_working_folder),
                                ],
                            )
                            _, _, testcase_start_time_str = get_start_time_vars()
                            logname = "{}_{}.log".format(
                                test_name, testcase_start_time_str
                            )
                            remote_results_file = (
                                "/tmp/benchmark-result-{}_{}.out".format(
                                    test_name, testcase_start_time_str
                                )
                            )

                            (
                                redis_configuration_parameters,
                                dataset_load_timeout_secs,
                            ) = extract_redis_dbconfig_parameters(
                                benchmark_config, "dbconfig"
                            )

                            cluster_api_enabled = False
                            cluster_start_port = 20000
                            # setup Redis
                            if setup_type == "oss-cluster":
                                logging.error(
                                    "Remote cluster is still not implemented =(. We're working hard to get it ASAP =)!!"
                                )
                                continue
                                cluster_api_enabled = True
                                spin_up_redis_cluster_remote_redis(
                                    benchmark_config,
                                    server_public_ip,
                                    username,
                                    private_key,
                                    local_module_file,
                                    remote_module_file,
                                    remote_dataset_file,
                                    logname,
                                    dirname,
                                    redis_configuration_parameters,
                                    dbdir_folder,
                                    shard_count,
                                    cluster_start_port,
                                )
                                dataset_load_start_time = datetime.datetime.now()

                                # we use node 0 for the checks
                                local_redis_conn = ssh_tunnel_redisconn(
                                    cluster_start_port,
                                    server_private_ip,
                                    server_public_ip,
                                    username,
                                )
                                r_conns = []
                                for p in range(
                                    cluster_start_port, cluster_start_port + shard_count
                                ):
                                    local_redis_conn, ssh_tunnel = ssh_tunnel_redisconn(
                                        p,
                                        server_private_ip,
                                        server_public_ip,
                                        username,
                                    )
                                    local_redis_conn.execute_command(
                                        "CLUSTER SAVECONFIG"
                                    )

                            if setup_type == "oss-standalone":
                                full_logfile, dataset = spin_up_standalone_remote_redis(
                                    benchmark_config,
                                    server_public_ip,
                                    username,
                                    private_key,
                                    local_module_file,
                                    remote_module_file,
                                    remote_dataset_file,
                                    logname,
                                    dirname,
                                    redis_configuration_parameters,
                                    dbdir_folder,
                                )
                                dataset_load_start_time = datetime.datetime.now()
                                local_redis_conn, ssh_tunnel = ssh_tunnel_redisconn(
                                    server_plaintext_port,
                                    server_private_ip,
                                    server_public_ip,
                                    username,
                                )
                            result = wait_for_conn(
                                local_redis_conn, dataset_load_timeout_secs
                            )
                            logging.info("Starting dataset loading...")
                            dataset_load_end_time = datetime.datetime.now()
                            if result is True:
                                logging.info("Redis available")
                            else:
                                logging.error("Remote redis is not available")
                                raise Exception(
                                    "Remote redis is not available. Aborting..."
                                )

                            dataset_load_duration_seconds = (
                                dataset_load_end_time - dataset_load_start_time
                            ).seconds
                            logging.info(
                                "Dataset loading duration {} secs.".format(
                                    dataset_load_duration_seconds
                                )
                            )

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

                            artifact_version = run_redis_pre_steps(
                                benchmark_config, local_redis_conn, required_modules
                            )

                            (
                                benchmark_min_tool_version,
                                benchmark_min_tool_version_major,
                                benchmark_min_tool_version_minor,
                                benchmark_min_tool_version_patch,
                                benchmark_tool,
                                benchmark_tool_source,
                                _,
                                _,
                            ) = extract_benchmark_tool_settings(benchmark_config)
                            benchmark_tools_sanity_check(args, benchmark_tool)
                            # setup the benchmark tool
                            remote_tool_pre_bench_step(
                                benchmark_config,
                                benchmark_min_tool_version,
                                benchmark_min_tool_version_major,
                                benchmark_min_tool_version_minor,
                                benchmark_min_tool_version_patch,
                                benchmark_tool,
                                client_public_ip,
                                username,
                            )

                            command, command_str = prepare_benchmark_parameters(
                                benchmark_config,
                                benchmark_tool,
                                server_plaintext_port,
                                server_private_ip,
                                remote_results_file,
                                True,
                                None,
                                cluster_api_enabled,
                            )

                            (
                                start_time,
                                start_time_ms,
                                start_time_str,
                            ) = get_start_time_vars()
                            local_bench_fname = get_run_full_filename(
                                start_time_str,
                                deployment_type,
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
                            tmp = None
                            if benchmark_tool == "redis-benchmark":
                                tmp = local_bench_fname
                                local_bench_fname = "result.csv"

                            benchmark_start_time = datetime.datetime.now()
                            # run the benchmark
                            remote_run_result, stdout, _ = run_remote_benchmark(
                                client_public_ip,
                                username,
                                private_key,
                                remote_results_file,
                                local_bench_fname,
                                command_str,
                            )
                            benchmark_end_time = datetime.datetime.now()

                            logging.info("Shutting down remote redis.")
                            local_redis_conn.shutdown(save=False)
                            ssh_tunnel.close()  # Close the tunnel
                            if remote_run_result is False:
                                failed_remote_run_artifact_store(
                                    args,
                                    client_public_ip,
                                    dirname,
                                    full_logfile,
                                    logname,
                                    s3_bucket_name,
                                    s3_bucket_path,
                                    username,
                                )
                            benchmark_duration_seconds = (
                                calculate_benchmark_duration_and_check(
                                    benchmark_end_time, benchmark_start_time
                                )
                            )

                            (
                                artifact_version,
                                local_bench_fname,
                                results_dict,
                                return_code,
                            ) = post_process_remote_run(
                                artifact_version,
                                benchmark_config,
                                benchmark_tool,
                                local_bench_fname,
                                return_code,
                                start_time_ms,
                                start_time_str,
                                stdout,
                                tmp,
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
                                args,
                                artifact_version,
                                benchmark_config,
                                benchmark_duration_seconds,
                                dataset_load_duration_seconds,
                                default_metrics,
                                deployment_type,
                                exporter_timemetric_path,
                                results_dict,
                                rts,
                                start_time_ms,
                                test_name,
                                testcases_setname,
                                tf_github_branch,
                                tf_github_org,
                                tf_github_repo,
                                tf_triggering_env,
                                tsname_project_total_success,
                            )

                        except:
                            timeseries_test_failure_flow(
                                args,
                                deployment_type,
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
    terraform_destroy(remote_envs)
    exit(return_code)
