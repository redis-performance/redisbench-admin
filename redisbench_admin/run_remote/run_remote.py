#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import datetime
import json
import logging
import os
import sys
import traceback

import paramiko
import redis
from python_terraform import Terraform
from redistimeseries.client import Client

from redisbench_admin.run.common import (
    prepare_benchmark_parameters,
    run_remote_benchmark,
    common_exporter_logic,
    get_start_time_vars,
    execute_init_commands,
    BENCHMARK_REPETITIONS,
)
from redisbench_admin.run.redis_benchmark.redis_benchmark import (
    redis_benchmark_ensure_min_version_remote,
)
from redisbench_admin.utils.results import post_process_benchmark_results
from redisbench_admin.utils.benchmark_config import (
    parse_exporter_metrics_definition,
    parse_exporter_timemetric_definition,
    extract_benchmark_tool_settings,
    prepare_benchmark_definitions,
    check_required_modules,
    results_dict_kpi_check,
    extract_redis_dbconfig_parameters,
)
from redisbench_admin.utils.redisgraph_benchmark_go import (
    spin_up_standalone_remote_redis,
    setup_remote_benchmark_tool_redisgraph_benchmark_go,
    setup_remote_benchmark_tool_ycsb_redisearch,
)
from redisbench_admin.utils.remote import (
    extract_git_vars,
    upload_artifacts_to_s3,
    setup_remote_environment,
    check_and_fix_pem_str,
    get_run_full_filename,
    fetch_remote_setup_from_config,
    execute_remote_commands,
    extract_redisgraph_version_from_resultdict,
    retrieve_tf_connection_vars,
    get_project_ts_tags,
    get_overall_dashboard_keynames,
    EC2_PRIVATE_PEM,
    check_ec2_env,
)
from sshtunnel import SSHTunnelForwarder

# internal aux vars
from redisbench_admin.utils.utils import get_ts_metric_name, wait_for_conn

redisbenchmark_go_link = (
    "https://s3.amazonaws.com/benchmarks.redislabs/"
    "tools/redisgraph-benchmark-go/unstable/"
    "redisgraph-benchmark-go_linux_amd64"
)
remote_dataset_file = "/tmp/dump.rdb"
remote_module_file = "/tmp/module.so"
local_results_file = "./benchmark-result.out"
remote_results_file = "/tmp/benchmark-result.out"
private_key = "/tmp/benchmarks.redislabs.pem"
min_recommended_benchmark_duration = 60


# noinspection PyBroadException
def setup_remote_benchmark_tool_requirements_tsbs(
    client_public_ip,
    username,
    private_key,
    tool_link,
    queries_file_link,
    remote_tool_link,
    remote_input_file="/tmp/input.data",
):
    commands = [
        "wget {} -q -O {}".format(tool_link, remote_tool_link),
        "wget {} -q -O {}".format(queries_file_link, remote_input_file),
        "chmod 755 {}".format(remote_tool_link),
    ]
    execute_remote_commands(client_public_ip, username, private_key, commands)


def extract_artifact_version_remote(
    server_public_ip, server_public_port, username, private_key
):
    commands = [
        "redis-cli -h {} -p {} info modules".format("localhost", server_public_port),
    ]
    res = execute_remote_commands(server_public_ip, username, private_key, commands)
    recv_exit_status, stdout, stderr = res[0]
    print(stdout)
    module_name, version = extract_module_semver_from_info_modules_cmd(stdout)
    return module_name, version


def extract_module_semver_from_info_modules_cmd(stdout):
    versions = []
    module_names = []
    if type(stdout) == bytes:
        stdout = stdout.decode()
    if type(stdout) == str:
        info_modules_output = stdout.split("\n")[1:]
    else:
        info_modules_output = stdout[1:]
    for module_detail_line in info_modules_output:
        detail_arr = module_detail_line.split(",")
        if len(detail_arr) > 1:
            module_name = detail_arr[0].split("=")
            module_name = module_name[1]
            version = detail_arr[1].split("=")[1]
            logging.info(
                "Detected artifact={}, semver={}.".format(module_name, version)
            )
            module_names.append(module_name)
            versions.append(version)
    return module_names, versions


def run_remote_command_logic(args):
    tf_bin_path = args.terraform_bin_path
    tf_github_org = args.github_org
    tf_github_actor = args.github_actor
    tf_github_repo = args.github_repo
    tf_github_sha = args.github_sha
    tf_github_branch = args.github_branch
    required_modules = args.required_module

    if tf_github_org is None:
        (
            github_org_name,
            github_repo_name,
            github_sha,
            github_actor,
            github_branch,
            github_branch_detached,
        ) = extract_git_vars()
        logging.info(
            "Extracting tf_github_org given args.github_org was none. Extracte value {}".format(
                github_org_name
            )
        )
        tf_github_org = github_org_name
    if tf_github_actor is None:
        (
            github_org_name,
            github_repo_name,
            github_sha,
            github_actor,
            github_branch,
            github_branch_detached,
        ) = extract_git_vars()
        logging.info(
            "Extracting tf_github_actor given args.github_actor was none. Extracte value {}".format(
                github_actor
            )
        )
        tf_github_actor = github_actor
    if tf_github_repo is None:
        (
            github_org_name,
            github_repo_name,
            github_sha,
            github_actor,
            github_branch,
            github_branch_detached,
        ) = extract_git_vars()
        logging.info(
            "Extracting tf_github_repo given args.github_repo was none. Extracte value {}".format(
                github_repo_name
            )
        )
        tf_github_repo = github_repo_name
    if tf_github_sha is None:
        (
            github_org_name,
            github_repo_name,
            github_sha,
            github_actor,
            github_branch,
            github_branch_detached,
        ) = extract_git_vars()
        logging.info(
            "Extracting tf_github_sha given args.github_sha was none. Extracte value {}".format(
                github_sha
            )
        )
        tf_github_sha = github_sha
    if tf_github_branch is None:
        (
            github_org_name,
            github_repo_name,
            github_sha,
            github_actor,
            github_branch,
            github_branch_detached,
        ) = extract_git_vars()
        logging.info(
            "Extracting tf_github_branch given args.github_branch was none. Extracte value {}".format(
                github_branch
            )
        )
        tf_github_branch = github_branch

    tf_triggering_env = args.triggering_env
    tf_setup_name_sufix = "{}-{}".format(args.setup_name_sufix, tf_github_sha)
    s3_bucket_name = args.s3_bucket_name
    local_module_file = args.module_path

    if args.skip_env_vars_verify is False:
        check_ec2_env()

    if EC2_PRIVATE_PEM is None or EC2_PRIVATE_PEM == "":
        logging.error("missing required EC2_PRIVATE_PEM env variable")
        exit(1)

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

    with open(private_key, "w") as tmp_private_key_file:
        pem_str = check_and_fix_pem_str(EC2_PRIVATE_PEM)
        tmp_private_key_file.write(pem_str)

    if os.path.exists(private_key) is False:
        logging.error(
            "Specified private key path does not exist: {}".format(private_key)
        )
        exit(1)
    else:
        logging.info(
            "Confirmed that private key path artifact: '{}' exists!".format(private_key)
        )

    (
        benchmark_definitions,
        default_metrics,
        exporter_timemetric_path,
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

            if "remote" in benchmark_config:
                (
                    remote_setup,
                    deployment_type,
                    remote_id,
                ) = fetch_remote_setup_from_config(benchmark_config["remote"])
                logging.info(
                    "Repetition {} of {}. Deploying test {} on AWS using {}".format(
                        repetition, BENCHMARK_REPETITIONS, test_name, remote_setup
                    )
                )
                tf_setup_name = "{}{}".format(remote_setup, tf_setup_name_sufix)
                logging.info("Using full setup name: {}".format(tf_setup_name))
                if remote_id not in remote_envs:
                    # check if terraform is present
                    tf = Terraform(
                        working_dir=remote_setup,
                        terraform_bin_path=tf_bin_path,
                    )
                    (
                        tf_return_code,
                        username,
                        server_private_ip,
                        server_public_ip,
                        server_plaintext_port,
                        client_private_ip,
                        client_public_ip,
                    ) = setup_remote_environment(
                        tf,
                        tf_github_sha,
                        tf_github_actor,
                        tf_setup_name,
                        tf_github_org,
                        tf_github_repo,
                        tf_triggering_env,
                    )
                    remote_envs[remote_id] = tf
                else:
                    logging.info("Reusing remote setup {}".format(remote_id))
                    tf = remote_envs[remote_id]
                    (
                        tf_return_code,
                        username,
                        server_private_ip,
                        server_public_ip,
                        server_plaintext_port,
                        client_private_ip,
                        client_public_ip,
                    ) = retrieve_tf_connection_vars(None, tf)

                    local_redis_conn, ssh_tunnel = ssh_tunnel_redisconn(
                        server_plaintext_port,
                        server_private_ip,
                        server_public_ip,
                        username,
                    )
                    local_redis_conn.shutdown()
                    ssh_tunnel.close()  # Close the tunnel

                # after we've created the env, even on error we should always teardown
                # in case of some unexpected error we fail the test
                try:

                    (
                        redis_configuration_parameters,
                        dataset_load_timeout_secs,
                    ) = extract_redis_dbconfig_parameters(benchmark_config, "dbconfig")
                    # setup Redis
                    spin_up_standalone_remote_redis(
                        benchmark_config,
                        server_public_ip,
                        username,
                        private_key,
                        local_module_file,
                        remote_module_file,
                        remote_dataset_file,
                        dirname,
                        redis_configuration_parameters,
                    )
                    dataset_load_start_time = datetime.datetime.now()
                    local_redis_conn, ssh_tunnel = ssh_tunnel_redisconn(
                        server_plaintext_port,
                        server_private_ip,
                        server_public_ip,
                        username,
                    )
                    result = wait_for_conn(local_redis_conn, dataset_load_timeout_secs)
                    dataset_load_end_time = datetime.datetime.now()
                    if result is True:
                        logging.info("Redis available")
                    else:
                        logging.error("Remote redis is not available")
                        raise Exception("Remote redis is not available. Aborting...")

                    dataset_load_duration_seconds = (
                        dataset_load_end_time - dataset_load_start_time
                    ).seconds
                    logging.info(
                        "Dataset loading duration {} secs.".format(
                            dataset_load_duration_seconds
                        )
                    )

                    stdout = local_redis_conn.execute_command("info modules")
                    (
                        module_names,
                        artifact_versions,
                    ) = extract_module_semver_from_info_modules_cmd(stdout)

                    check_required_modules(module_names, required_modules)

                    # run initialization commands before benchmark starts
                    logging.info(
                        "Running initialization commands before benchmark starts."
                    )
                    execute_init_commands_start_time = datetime.datetime.now()
                    execute_init_commands(benchmark_config, local_redis_conn)
                    execute_init_commands_duration_seconds = (
                        datetime.datetime.now() - execute_init_commands_start_time
                    ).seconds
                    logging.info(
                        "Running initialization commands took {} secs.".format(
                            execute_init_commands_duration_seconds
                        )
                    )

                    ssh_tunnel.close()  # Close the tunnel

                    artifact_version = artifact_versions[0]
                    (
                        benchmark_min_tool_version,
                        benchmark_min_tool_version_major,
                        benchmark_min_tool_version_minor,
                        benchmark_min_tool_version_patch,
                        benchmark_tool,
                        benchmark_tool_source,
                        _,
                    ) = extract_benchmark_tool_settings(benchmark_config)
                    benchmark_tools_sanity_check(args, benchmark_tool)
                    # setup the benchmark tool
                    if benchmark_tool == "redisgraph-benchmark-go":
                        setup_remote_benchmark_tool_redisgraph_benchmark_go(
                            client_public_ip,
                            username,
                            private_key,
                            redisbenchmark_go_link,
                        )
                    if "ycsb" in benchmark_tool:
                        setup_remote_benchmark_tool_ycsb_redisearch(
                            client_public_ip,
                            username,
                            private_key,
                        )
                    if "tsbs_" in benchmark_tool:
                        (
                            queries_file_link,
                            remote_tool_link,
                            tool_link,
                        ) = extract_tsbs_extra_links(benchmark_config, benchmark_tool)

                        setup_remote_benchmark_tool_requirements_tsbs(
                            client_public_ip,
                            username,
                            private_key,
                            tool_link,
                            queries_file_link,
                            remote_tool_link,
                        )

                    if (
                        benchmark_min_tool_version is not None
                        and benchmark_tool == "redis-benchmark"
                    ):
                        redis_benchmark_ensure_min_version_remote(
                            benchmark_tool,
                            benchmark_min_tool_version,
                            benchmark_min_tool_version_major,
                            benchmark_min_tool_version_minor,
                            benchmark_min_tool_version_patch,
                            client_public_ip,
                            username,
                            private_key,
                        )

                    command, command_str = prepare_benchmark_parameters(
                        benchmark_config,
                        benchmark_tool,
                        server_plaintext_port,
                        server_private_ip,
                        remote_results_file,
                        True,
                    )

                    start_time, start_time_ms, start_time_str = get_start_time_vars()
                    local_benchmark_output_filename = get_run_full_filename(
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
                            local_benchmark_output_filename
                        )
                    )
                    tmp = None
                    if benchmark_tool == "redis-benchmark":
                        tmp = local_benchmark_output_filename
                        local_benchmark_output_filename = "result.csv"

                    benchmark_start_time = datetime.datetime.now()
                    # run the benchmark
                    _, stdout, _ = run_remote_benchmark(
                        client_public_ip,
                        username,
                        private_key,
                        remote_results_file,
                        local_benchmark_output_filename,
                        command_str,
                    )
                    benchmark_end_time = datetime.datetime.now()
                    benchmark_duration_seconds = (
                        benchmark_end_time - benchmark_start_time
                    ).seconds
                    logging.info(
                        "Benchmark duration {} secs.".format(benchmark_duration_seconds)
                    )
                    if benchmark_duration_seconds < min_recommended_benchmark_duration:
                        logging.warning(
                            "Benchmark duration of {} secs is bellow the considered"
                            " minimum duration for a stable run ({} secs).".format(
                                benchmark_duration_seconds,
                                min_recommended_benchmark_duration,
                            )
                        )

                    if benchmark_tool == "redis-benchmark":
                        local_benchmark_output_filename = tmp
                        with open("result.csv", "r") as txt_file:
                            stdout = txt_file.read()

                    if benchmark_tool == "redis-benchmark" or benchmark_tool == "ycsb":
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

                    # if the benchmark tool is redisgraph-benchmark-go and
                    # we still dont have the artifact semver we can extract it from the results dict
                    if (
                        benchmark_tool == "redisgraph-benchmark-go"
                        and artifact_version is None
                    ):
                        artifact_version = extract_redisgraph_version_from_resultdict(
                            results_dict
                        )

                    if artifact_version is None:
                        artifact_version = "N/A"

                    if args.upload_results_s3:
                        logging.info(
                            "Uploading results to s3. s3 bucket name: {}. s3 bucket path: {}".format(
                                s3_bucket_name, s3_bucket_path
                            )
                        )
                        artifacts = [local_benchmark_output_filename]
                        upload_artifacts_to_s3(
                            artifacts, s3_bucket_name, s3_bucket_path
                        )

                    if args.push_results_redistimeseries:
                        logging.info("Pushing results to RedisTimeSeries.")
                        redistimeseries_results_logic(
                            artifact_version,
                            benchmark_config,
                            default_metrics,
                            deployment_type,
                            exporter_timemetric_path,
                            results_dict,
                            rts,
                            test_name,
                            tf_github_branch,
                            tf_github_org,
                            tf_github_repo,
                            tf_triggering_env,
                        )
                        try:
                            rts.redis.sadd(testcases_setname, test_name)
                            rts.incrby(
                                tsname_project_total_success,
                                1,
                                timestamp=start_time_ms,
                                labels=get_project_ts_tags(
                                    tf_github_org,
                                    tf_github_repo,
                                    deployment_type,
                                    tf_triggering_env,
                                ),
                            )
                            if tf_github_branch is not None and tf_github_branch != "":
                                add_standardized_metric_bybranch(
                                    "benchmark_duration",
                                    benchmark_duration_seconds,
                                    str(tf_github_branch),
                                    deployment_type,
                                    rts,
                                    start_time_ms,
                                    test_name,
                                    tf_github_org,
                                    tf_github_repo,
                                    tf_triggering_env,
                                )
                                add_standardized_metric_bybranch(
                                    "dataset_load_duration",
                                    dataset_load_duration_seconds,
                                    str(tf_github_branch),
                                    deployment_type,
                                    rts,
                                    start_time_ms,
                                    test_name,
                                    tf_github_org,
                                    tf_github_repo,
                                    tf_triggering_env,
                                )
                            add_standardized_metric_byversion(
                                "benchmark_duration",
                                benchmark_duration_seconds,
                                artifact_version,
                                deployment_type,
                                rts,
                                start_time_ms,
                                test_name,
                                tf_github_org,
                                tf_github_repo,
                                tf_triggering_env,
                            )
                            add_standardized_metric_byversion(
                                "dataset_load_duration",
                                dataset_load_duration_seconds,
                                artifact_version,
                                deployment_type,
                                rts,
                                start_time_ms,
                                test_name,
                                tf_github_org,
                                tf_github_repo,
                                tf_triggering_env,
                            )
                        except redis.exceptions.ResponseError as e:
                            logging.warning(
                                "Error while updating secondary data structures {}. ".format(
                                    e.__str__()
                                )
                            )
                            pass

                except:
                    if args.push_results_redistimeseries:
                        if start_time_ms is None:
                            _, start_time_ms, _ = get_start_time_vars()
                        try:
                            rts.incrby(
                                tsname_project_total_failures,
                                1,
                                timestamp=start_time_ms,
                                labels=get_project_ts_tags(
                                    tf_github_org,
                                    tf_github_repo,
                                    deployment_type,
                                    tf_triggering_env,
                                ),
                            )
                        except redis.exceptions.ResponseError as e:
                            logging.warning(
                                "Error while updating secondary data structures {}. ".format(
                                    e.__str__()
                                )
                            )
                            pass
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
    for remote_setup_name, tf in remote_envs.items():
        # tear-down
        logging.info("Tearing down setup {}".format(remote_setup_name))
        tf.destroy()
        logging.info("Tear-down completed")

    exit(return_code)


def add_standardized_metric_bybranch(
    metric_name,
    metric_value,
    tf_github_branch,
    deployment_type,
    rts,
    start_time_ms,
    test_name,
    tf_github_org,
    tf_github_repo,
    tf_triggering_env,
):
    tsname_use_case_duration = get_ts_metric_name(
        "by.branch",
        tf_github_branch,
        tf_github_org,
        tf_github_repo,
        deployment_type,
        test_name,
        tf_triggering_env,
        metric_name,
    )
    labels = get_project_ts_tags(
        tf_github_org,
        tf_github_repo,
        deployment_type,
        tf_triggering_env,
    )
    labels["branch"] = tf_github_branch
    labels["test_name"] = str(test_name)
    labels["metric"] = str(metric_name)
    logging.info(
        "Adding metric {}={} to time-serie named {}".format(
            metric_name, metric_value, tsname_use_case_duration
        )
    )
    try:
        logging.info(
            "Creating timeseries named {} with labels {}".format(
                tsname_use_case_duration, labels
            )
        )
        rts.create(tsname_use_case_duration, labels=labels)
    except redis.exceptions.ResponseError:
        logging.warning(
            "Timeseries named {} already exists".format(tsname_use_case_duration)
        )
        pass
    rts.add(
        tsname_use_case_duration,
        start_time_ms,
        metric_value,
        labels=labels,
    )


def add_standardized_metric_byversion(
    metric_name,
    metric_value,
    artifact_version,
    deployment_type,
    rts,
    start_time_ms,
    test_name,
    tf_github_org,
    tf_github_repo,
    tf_triggering_env,
):
    tsname_use_case_duration = get_ts_metric_name(
        "by.version",
        artifact_version,
        tf_github_org,
        tf_github_repo,
        deployment_type,
        test_name,
        tf_triggering_env,
        metric_name,
    )
    labels = get_project_ts_tags(
        tf_github_org,
        tf_github_repo,
        deployment_type,
        tf_triggering_env,
    )
    labels["version"] = artifact_version
    labels["test_name"] = str(test_name)
    labels["metric"] = str(metric_name)
    logging.info(
        "Adding metric {}={} to time-serie named {}".format(
            metric_name, metric_value, tsname_use_case_duration
        )
    )
    try:
        logging.info(
            "Creating timeseries named {} with labels {}".format(
                tsname_use_case_duration, labels
            )
        )
        rts.create(tsname_use_case_duration, labels=labels)
    except redis.exceptions.ResponseError:
        logging.warning(
            "Timeseries named {} already exists".format(tsname_use_case_duration)
        )
        pass
    rts.add(
        tsname_use_case_duration,
        start_time_ms,
        metric_value,
        labels=labels,
    )


def ssh_tunnel_redisconn(
    server_plaintext_port, server_private_ip, server_public_ip, username
):
    ssh_pkey = paramiko.RSAKey.from_private_key_file(private_key)

    # Use sshtunnelforwarder tunnel to connect redis via springboard
    server = SSHTunnelForwarder(
        ssh_address_or_host=(server_public_ip, 22),
        ssh_username=username,
        ssh_pkey=ssh_pkey,
        logger=logging.getLogger(),
        remote_bind_address=(
            server_private_ip,
            server_plaintext_port,
        ),  # remote redis server
        local_bind_address=("0.0.0.0", 10022),  # enable local forwarding port
    )
    server.start()  # start tunnel
    r = redis.StrictRedis(host="localhost", port=server.local_bind_port)
    return r, server


def extract_tsbs_extra_links(benchmark_config, benchmark_tool):
    remote_tool_link = "/tmp/{}".format(benchmark_tool)
    tool_link = (
        "https://s3.amazonaws.com/benchmarks.redislabs/"
        + "redistimeseries/tools/tsbs/{}_linux_amd64".format(benchmark_tool)
    )
    queries_file_link = None
    for entry in benchmark_config["clientconfig"]:
        if "parameters" in entry:
            for parameter in entry["parameters"]:
                if "file" in parameter:
                    queries_file_link = parameter["file"]
    return queries_file_link, remote_tool_link, tool_link


def get_test_s3_bucket_path(
    s3_bucket_name, test_name, tf_github_org, tf_github_repo, folder="results"
):
    s3_bucket_path = "{github_org}/{github_repo}/{folder}/{test_name}/".format(
        github_org=tf_github_org,
        github_repo=tf_github_repo,
        test_name=test_name,
        folder=folder,
    )
    return s3_bucket_path


def redistimeseries_results_logic(
    artifact_version,
    benchmark_config,
    default_metrics,
    deployment_type,
    exporter_timemetric_path,
    results_dict,
    rts,
    test_name,
    tf_github_branch,
    tf_github_org,
    tf_github_repo,
    tf_triggering_env,
):
    # check which metrics to extract
    exporter_timemetric_path, metrics = merge_default_and_config_metrics(
        benchmark_config, default_metrics, exporter_timemetric_path
    )
    per_version_time_series_dict, per_branch_time_series_dict = common_exporter_logic(
        deployment_type,
        exporter_timemetric_path,
        metrics,
        results_dict,
        rts,
        test_name,
        tf_github_branch,
        tf_github_org,
        tf_github_repo,
        tf_triggering_env,
        artifact_version,
    )
    return per_version_time_series_dict, per_branch_time_series_dict


def merge_default_and_config_metrics(
    benchmark_config, default_metrics, exporter_timemetric_path
):
    metrics = default_metrics
    if "exporter" in benchmark_config:
        extra_metrics = parse_exporter_metrics_definition(benchmark_config["exporter"])
        metrics.extend(extra_metrics)
        extra_timemetric_path = parse_exporter_timemetric_definition(
            benchmark_config["exporter"]
        )
        if extra_timemetric_path is not None:
            exporter_timemetric_path = extra_timemetric_path
    return exporter_timemetric_path, metrics


def benchmark_tools_sanity_check(args, benchmark_tool):
    if benchmark_tool is not None:
        logging.info("Detected benchmark config tool {}".format(benchmark_tool))
    else:
        raise Exception(
            "Unable to detect benchmark tool within 'clientconfig' section. Aborting..."
        )
    if benchmark_tool not in args.allowed_tools.split(","):
        raise Exception(
            "Benchmark tool {} not in the allowed tools list [{}]. Aborting...".format(
                benchmark_tool, args.allowed_tools
            )
        )


def absoluteFilePaths(directory):
    for dirpath, _, filenames in os.walk(directory):
        for f in filenames:
            yield os.path.abspath(os.path.join(dirpath, f))
