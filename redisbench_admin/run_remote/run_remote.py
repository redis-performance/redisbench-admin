import json
import logging
import os
import pathlib
import sys
import traceback

import yaml
from python_terraform import Terraform
from redistimeseries.client import Client

from redisbench_admin.run.common import (
    extract_benchmark_tool_settings,
    prepare_benchmark_parameters,
    run_remote_benchmark,
    merge_default_and_specific_properties_dict_type,
    process_default_yaml_properties_file,
    common_exporter_logic,
    get_start_time_vars,
)
from redisbench_admin.run.redis_benchmark.redis_benchmark import (
    redis_benchmark_from_stdout_csv_to_json,
    redis_benchmark_ensure_min_version_remote,
)
from redisbench_admin.utils.benchmark_config import (
    parse_exporter_metrics_definition,
    parse_exporter_timemetric_definition,
)
from redisbench_admin.utils.redisgraph_benchmark_go import (
    spin_up_standalone_remote_redis,
    setup_remote_benchmark_tool_redisgraph_benchmark_go,
)
from redisbench_admin.utils.remote import (
    extract_git_vars,
    validate_result_expectations,
    upload_artifacts_to_s3,
    setup_remote_environment,
    check_and_fix_pem_str,
    get_run_full_filename,
    fetch_remote_setup_from_config,
)

# internal aux vars
redisbenchmark_go_link = (
    "https://s3.amazonaws.com/benchmarks.redislabs/"
    "redisgraph/redisgraph-benchmark-go/unstable/"
    "redisgraph-benchmark-go_linux_amd64"
)
remote_dataset_file = "/tmp/dump.rdb"
remote_module_file = "/tmp/module.so"
local_results_file = "./benchmark-result.out"
remote_results_file = "/tmp/benchmark-result.out"
private_key = "/tmp/benchmarks.redislabs.pem"

# environment variables
PERFORMANCE_RTS_AUTH = os.getenv("PERFORMANCE_RTS_AUTH", None)
PERFORMANCE_RTS_HOST = os.getenv("PERFORMANCE_RTS_HOST", 6379)
PERFORMANCE_RTS_PORT = os.getenv("PERFORMANCE_RTS_PORT", None)
TERRAFORM_BIN_PATH = os.getenv("TERRAFORM_BIN_PATH", "terraform")
EC2_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", None)
EC2_REGION = os.getenv("AWS_DEFAULT_REGION", None)
EC2_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", None)
EC2_PRIVATE_PEM = os.getenv("EC2_PRIVATE_PEM", None)


# noinspection PyBroadException
def run_remote_command_logic(args):
    tf_bin_path = args.terraform_bin_path
    tf_github_org = args.github_org
    tf_github_actor = args.github_actor
    tf_github_repo = args.github_repo
    tf_github_sha = args.github_sha
    tf_github_branch = args.github_branch

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
        if EC2_ACCESS_KEY is None or EC2_ACCESS_KEY == "":
            logging.error("missing required AWS_ACCESS_KEY_ID env variable")
            exit(1)
        if EC2_REGION is None or EC2_REGION == "":
            logging.error("missing required AWS_DEFAULT_REGION env variable")
            exit(1)
        if EC2_SECRET_KEY is None or EC2_SECRET_KEY == "":
            logging.error("missing required AWS_SECRET_ACCESS_KEY env variable")
            exit(1)

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

    return_code = 0
    if args.test == "":
        files = pathlib.Path().glob("*.yml")
        files = [str(x) for x in files]
        if "defaults.yml" in files:
            files.remove("defaults.yml")
        logging.info(
            "Running all specified benchmarks: {}".format(
                " ".join([str(x) for x in files])
            )
        )
    else:
        logging.info("Running specific benchmark in file: {}".format(args.test))
        files = [args.test]

    default_metrics = []
    exporter_timemetric_path = None
    defaults_filename = "defaults.yml"
    default_kpis = None
    if os.path.exists(defaults_filename):
        with open(defaults_filename, "r") as stream:
            logging.info(
                "Loading default specifications from file: {}".format(defaults_filename)
            )
            (
                default_kpis,
                default_metrics,
                exporter_timemetric_path,
            ) = process_default_yaml_properties_file(
                default_kpis,
                default_metrics,
                defaults_filename,
                exporter_timemetric_path,
                stream,
            )

    for usecase_filename in files:
        with open(usecase_filename, "r") as stream:
            dirname = os.path.dirname(os.path.abspath(usecase_filename))
            benchmark_config = yaml.safe_load(stream)
            test_name = benchmark_config["name"]

            if default_kpis is not None:
                merge_default_and_specific_properties_dict_type(
                    benchmark_config, default_kpis, "kpis", usecase_filename
                )
            s3_bucket_path = "{github_org}/{github_repo}/results/{test_name}/".format(
                github_org=tf_github_org,
                github_repo=tf_github_repo,
                test_name=test_name,
            )
            "https://s3.amazonaws.com/{bucket_name}/{bucket_path}".format(
                bucket_name=s3_bucket_name, bucket_path=s3_bucket_path
            )

            if "remote" in benchmark_config:
                remote_setup, deployment_type = fetch_remote_setup_from_config(
                    benchmark_config["remote"]
                )
                logging.info(
                    "Deploying test defined in {} on AWS using {}".format(
                        usecase_filename, remote_setup
                    )
                )
                tf_setup_name = "{}{}".format(remote_setup, tf_setup_name_sufix)
                logging.info("Using full setup name: {}".format(tf_setup_name))
                # check if terraform is present
                tf = Terraform(
                    working_dir=remote_setup,
                    terraform_bin_path=tf_bin_path,
                )
                (
                    return_code,
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
                # after we've created the env, even on error we should always teardown
                # in case of some unexpected error we fail the test
                try:
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
                    )

                    (
                        benchmark_min_tool_version,
                        benchmark_min_tool_version_major,
                        benchmark_min_tool_version_minor,
                        benchmark_min_tool_version_patch,
                        benchmark_tool,
                        benchmark_tool_source,
                        _,
                    ) = extract_benchmark_tool_settings(benchmark_config)
                    if benchmark_tool is not None:
                        logging.info(
                            "Detected benchmark config tool {}".format(benchmark_tool)
                        )
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
                    # setup the benchmark tool
                    if benchmark_tool == "redisgraph-benchmark-go":
                        setup_remote_benchmark_tool_redisgraph_benchmark_go(
                            client_public_ip,
                            username,
                            private_key,
                            redisbenchmark_go_link,
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
                    # run the benchmark
                    run_remote_benchmark(
                        client_public_ip,
                        username,
                        private_key,
                        remote_results_file,
                        local_benchmark_output_filename,
                        command_str,
                    )
                    if benchmark_tool == "redis-benchmark":
                        local_benchmark_output_filename = tmp
                        logging.info(
                            "Converting redis-benchmark output to json. Storing it in: {}".format(
                                local_benchmark_output_filename
                            )
                        )
                        with open("result.csv", "r") as txt_file:
                            csv_data = txt_file.read()
                            logging.info(csv_data)
                            results_dict = redis_benchmark_from_stdout_csv_to_json(
                                csv_data,
                                start_time_ms,
                                start_time_str,
                                overload_test_name="Overall",
                            )
                        with open(local_benchmark_output_filename, "w") as json_file:
                            json.dump(results_dict, json_file, indent=True)

                    # check KPIs
                    result = True
                    with open(local_benchmark_output_filename, "r") as json_file:
                        results_dict = json.load(json_file)

                    if "kpis" in benchmark_config:
                        result = validate_result_expectations(
                            benchmark_config,
                            results_dict,
                            result,
                            expectations_key="kpis",
                        )
                        if result is not True:
                            return_code |= 1

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
                        rts = Client(
                            host=args.redistimesies_host,
                            port=args.redistimesies_port,
                            password=args.redistimesies_pass,
                        )
                        # check which metrics to extract
                        metrics = default_metrics
                        if "exporter" in benchmark_config:
                            extra_metrics = parse_exporter_metrics_definition(
                                benchmark_config["exporter"]
                            )
                            metrics.extend(extra_metrics)
                            extra_timemetric_path = (
                                parse_exporter_timemetric_definition(
                                    benchmark_config["exporter"]
                                )
                            )
                            if extra_timemetric_path is not None:
                                exporter_timemetric_path = extra_timemetric_path

                        common_exporter_logic(
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
                        )
                except:
                    return_code |= 1
                    logging.critical(
                        "Some unexpected exception was caught "
                        "during remote work. Failing test...."
                    )
                    logging.critical(sys.exc_info()[0])
                    print("-" * 60)
                    traceback.print_exc(file=sys.stdout)
                    print("-" * 60)
                finally:
                    # tear-down
                    logging.info("Tearing down setup")
                    tf.destroy()
                    logging.info("Tear-down completed")

    exit(return_code)


def absoluteFilePaths(directory):
    for dirpath, _, filenames in os.walk(directory):
        for f in filenames:
            yield os.path.abspath(os.path.join(dirpath, f))
