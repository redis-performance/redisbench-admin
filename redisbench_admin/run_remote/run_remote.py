import datetime as dt
import json
import logging
import os
import pathlib
import re
import sys
import traceback

import yaml
from python_terraform import Terraform
from redistimeseries.client import Client

from redisbench_admin.run.redis_benchmark.redis_benchmark import prepareRedisBenchmarkCommand
from redisbench_admin.run.run import redis_benchmark_ensure_min_version
from redisbench_admin.utils.benchmark_config import (
    parseExporterMetricsDefinition,
    parseExporterTimeMetricDefinition,
    parseExporterTimeMetric,
)
from redisbench_admin.utils.local import prepareRedisGraphBenchmarkGoCommand
from redisbench_admin.utils.redisgraph_benchmark_go import (
    spinUpRemoteRedis,
    setupRemoteBenchmark,
    runRemoteBenchmark,
)
from redisbench_admin.utils.remote import (
    extract_git_vars,
    validateResultExpectations,
    upload_artifacts_to_s3,
    setupRemoteEnviroment,
    checkAndFixPemStr,
    get_run_full_filename,
    fetchRemoteSetupFromConfig,
    pushDataToRedisTimeSeries,
    extractPerBranchTimeSeriesFromResults,
    extractRedisGraphVersion,
    extractPerVersionTimeSeriesFromResults,
)

# internal aux vars
redisbenchmark_go_link = "https://s3.amazonaws.com/benchmarks.redislabs/redisgraph/redisgraph-benchmark-go/unstable/redisgraph-benchmark-go_linux_amd64"
remote_dataset_file = "/tmp/dump.rdb"
remote_module_file = "/tmp/redisgraph.so"
local_results_file = "./benchmark-result.json"
remote_results_file = "/tmp/benchmark-result.json"
private_key = "/tmp/benchmarks.redislabs.redisgraph.pem"

# environment variables
PERFORMANCE_RTS_AUTH = os.getenv("PERFORMANCE_RTS_AUTH", None)
PERFORMANCE_RTS_HOST = os.getenv("PERFORMANCE_RTS_HOST", 6379)
PERFORMANCE_RTS_PORT = os.getenv("PERFORMANCE_RTS_PORT", None)
TERRAFORM_BIN_PATH = os.getenv("TERRAFORM_BIN_PATH", "terraform")
EC2_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", None)
EC2_REGION = os.getenv("AWS_DEFAULT_REGION", None)
EC2_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", None)
EC2_PRIVATE_PEM = os.getenv("EC2_PRIVATE_PEM", None)


def run_remote_command_logic(args):
    tf_bin_path = args.terraform_bin_path
    tf_github_org = args.github_org
    tf_github_actor = args.github_actor
    tf_github_repo = args.github_repo
    tf_github_sha = args.github_sha
    tf_github_branch = args.github_branch

    if tf_github_actor is None:
        (
            github_org_name,
            github_repo_name,
            github_sha,
            github_actor,
            github_branch,
        ) = extract_git_vars()
        tf_github_org = github_org_name
    if tf_github_actor is None:
        (
            github_org_name,
            github_repo_name,
            github_sha,
            github_actor,
            github_branch,
        ) = extract_git_vars()
        tf_github_actor = github_actor
    if tf_github_repo is None:
        (
            github_org_name,
            github_repo_name,
            github_sha,
            github_actor,
            github_branch,
        ) = extract_git_vars()
        tf_github_repo = github_repo_name
    if tf_github_sha is None:
        (
            github_org_name,
            github_repo_name,
            github_sha,
            github_actor,
            github_branch,
        ) = extract_git_vars()
        tf_github_sha = github_sha
    if tf_github_branch is None:
        (
            github_org_name,
            github_repo_name,
            github_sha,
            github_actor,
            github_branch,
        ) = extract_git_vars()
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
        logging.error("Specified module artifact does not exist: {}".format(local_module_file))
        exit(1)
    else:
        logging.info("Confirmed that module artifact: '{}' exists!".format(local_module_file))

    logging.info("Using the following vars on terraform deployment:")
    logging.info("\tterraform bin path: {}".format(tf_bin_path))
    logging.info("\tgithub_actor: {}".format(tf_github_actor))
    logging.info("\tgithub_org: {}".format(tf_github_org))
    logging.info("\tgithub_repo: {}".format(tf_github_repo))
    logging.info("\tgithub_branch: {}".format(tf_github_branch))
    logging.info("\tgithub_sha: {}".format(tf_github_sha))
    logging.info("\ttriggering env: {}".format(tf_triggering_env))
    logging.info("\tprivate_key path: {}".format(private_key))
    logging.info("\tsetup_name sufix: {}".format(tf_setup_name_sufix))

    with open(private_key, "w") as tmp_private_key_file:
        pem_str = checkAndFixPemStr(EC2_PRIVATE_PEM)
        tmp_private_key_file.write(pem_str)

    return_code = 0

    files = []
    if args.test == "":
        files = pathlib.Path().glob("*.yml")
        files = [str(x) for x in files]
        if "defaults.yml" in files:
            files.remove("defaults.yml")
        logging.info(
            "Running all specified benchmarks: {}".format(" ".join([str(x) for x in files]))
        )
    else:
        logging.info("Running specific benchmark in file: {}".format(args.test))
        files = [args.test]

    default_metrics = []
    exporter_timemetric_path = None
    defaults_filename = "defaults.yml"
    if os.path.exists(defaults_filename):
        with open(defaults_filename, "r") as stream:
            logging.info(
                "Loading default specifications from file: {}".format(defaults_filename)
            )
            default_config = yaml.safe_load(stream)
            if "exporter" in default_config:
                default_metrics = parseExporterMetricsDefinition(default_config["exporter"])
                if len(default_metrics) > 0:
                    logging.info(
                        "Found RedisTimeSeries default metrics specification. Will include the following metrics on all benchmarks {}".format(
                            " ".join(default_metrics)
                        )
                    )
                exporter_timemetric_path = parseExporterTimeMetricDefinition(
                    default_config["exporter"]
                )
                if exporter_timemetric_path is not None:
                    logging.info(
                        "Found RedisTimeSeries default time metric specification. Will use the following JSON path to retrieve the test time {}".format(
                            exporter_timemetric_path
                        )
                    )

    for f in files:
        with open(f, "r") as stream:
            dirname = os.path.dirname(os.path.abspath(f))
            benchmark_config = yaml.safe_load(stream)
            test_name = benchmark_config["name"]
            s3_bucket_path = "{github_org}/{github_repo}/results/{test_name}/".format(
                github_org=tf_github_org, github_repo=tf_github_repo, test_name=test_name
            )
            s3_uri = "https://s3.amazonaws.com/{bucket_name}/{bucket_path}".format(
                bucket_name=s3_bucket_name, bucket_path=s3_bucket_path
            )

            if "remote" in benchmark_config:
                remote_setup, deployment_type = fetchRemoteSetupFromConfig(
                    benchmark_config["remote"]
                )
                logging.info(
                    "Deploying test defined in {} on AWS using {}".format(f, remote_setup)
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
                ) = setupRemoteEnviroment(
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
                    # setup RedisGraph
                    spinUpRemoteRedis(
                        benchmark_config,
                        server_public_ip,
                        username,
                        private_key,
                        local_module_file,
                        remote_module_file,
                        remote_dataset_file,
                        dirname,
                    )

                    # setup the benchmark
                    setupRemoteBenchmark(
                        client_public_ip, username, private_key, redisbenchmark_go_link
                    )
                    start_time = dt.datetime.now()
                    start_time_str = start_time.strftime("%Y-%m-%d-%H-%M-%S")
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

                    benchmark_tool = None
                    benchmark_min_tool_version = None
                    benchmark_min_tool_version_major = None
                    benchmark_min_tool_version_minor = None
                    benchmark_min_tool_version_patch = None
                    for entry in benchmark_config["clientconfig"]:
                        benchmark_min_tool_version, benchmark_min_tool_version_major, benchmark_min_tool_version_minor, benchmark_min_tool_version_patch, benchmark_tool = extract_tool_info(
                            benchmark_min_tool_version, benchmark_min_tool_version_major,
                            benchmark_min_tool_version_minor, benchmark_min_tool_version_patch, benchmark_tool, entry)
                    if benchmark_tool is not None:
                        logging.info("Detected benchmark config tool {}".format(benchmark_tool))
                    else:
                        raise Exception("Unable to detect benchmark tool within 'clientconfig' section. Aborting...")

                    if benchmark_tool not in args.allowed_tools.split(","):
                        raise Exception(
                            "Benchmark tool {} not in the allowed tools list [{}]. Aborting...".format(benchmark_tool,
                                                                                                       args.allowed_tools))

                    if benchmark_min_tool_version is not None and benchmark_tool == "redis-benchmark":
                        redis_benchmark_ensure_min_version(benchmark_tool, benchmark_min_tool_version,
                                                           benchmark_min_tool_version_major,
                                                           benchmark_min_tool_version_minor,
                                                           benchmark_min_tool_version_patch)

                    for entry in benchmark_config["clientconfig"]:
                        if 'parameters' in entry:
                            if benchmark_tool == 'redis-benchmark':
                                command = prepareRedisBenchmarkCommand(
                                    "redis-benchmark",
                                    server_private_ip,
                                    server_plaintext_port,
                                    entry
                                )
                            if benchmark_tool == 'redisgraph-benchmark-go':
                                command = prepareRedisGraphBenchmarkGoCommand(
                                    "/tmp/redisgraph-benchmark-go",
                                    server_private_ip,
                                    server_plaintext_port,
                                    entry,
                                    remote_results_file,
                                )

                    # run the benchmark
                    runRemoteBenchmark(
                        client_public_ip,
                        username,
                        private_key,
                        remote_results_file,
                        local_benchmark_output_filename,
                        command
                    )

                    # check KPIs
                    result = True
                    results_dict = None
                    with open(local_benchmark_output_filename, "r") as json_file:
                        results_dict = json.load(json_file)

                    if "kpis" in benchmark_config:
                        result = validateResultExpectations(
                            benchmark_config, results_dict, result, expectations_key="kpis"
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
                        upload_artifacts_to_s3(artifacts, s3_bucket_name, s3_bucket_path)

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
                            extra_metrics = parseExporterMetricsDefinition(
                                benchmark_config["exporter"]
                            )
                            metrics.extend(extra_metrics)
                            extra_timemetric_path = parseExporterTimeMetricDefinition(
                                benchmark_config["exporter"]
                            )
                            if extra_timemetric_path is not None:
                                exporter_timemetric_path = extra_timemetric_path

                        # extract timestamp
                        datapoints_timestamp = parseExporterTimeMetric(
                            exporter_timemetric_path, results_dict
                        )

                        rg_version = extractRedisGraphVersion(results_dict)

                        # extract per branch datapoints
                        (
                            ok,
                            per_version_time_series_dict,
                        ) = extractPerVersionTimeSeriesFromResults(
                            datapoints_timestamp,
                            metrics,
                            results_dict,
                            rg_version,
                            tf_github_org,
                            tf_github_repo,
                            deployment_type,
                            test_name,
                            tf_triggering_env,
                        )

                        # push per-branch data
                        pushDataToRedisTimeSeries(rts, per_version_time_series_dict)

                        # extract per branch datapoints
                        ok, branch_time_series_dict = extractPerBranchTimeSeriesFromResults(
                            datapoints_timestamp,
                            metrics,
                            results_dict,
                            str(tf_github_branch),
                            tf_github_org,
                            tf_github_repo,
                            deployment_type,
                            test_name,
                            tf_triggering_env,
                        )

                        # push per-branch data
                        pushDataToRedisTimeSeries(rts, branch_time_series_dict)
                except:
                    return_code |= 1
                    logging.critical(
                        "Some unexpected exception was caught during remote work. Failing test...."
                    )
                    logging.critical(sys.exc_info()[0])
                    print("-" * 60)
                    traceback.print_exc(file=sys.stdout)
                    print("-" * 60)
                finally:
                    # tear-down
                    logging.info("Tearing down setup")
                    tf_output = tf.destroy()
                    logging.info("Tear-down completed")

    exit(return_code)


def extract_tool_info(benchmark_min_tool_version, benchmark_min_tool_version_major, benchmark_min_tool_version_minor,
                      benchmark_min_tool_version_patch, benchmark_tool, entry):
    if 'tool' in entry:
        benchmark_tool = entry['tool']
    if 'min-tool-version' in entry:
        benchmark_min_tool_version = entry['min-tool-version']
        p = re.compile("(\d+)\.(\d+)\.(\d+)")
        m = p.match(benchmark_min_tool_version)
        if m is None:
            logging.error(
                "Unable to extract semversion from 'min-tool-version'. Will not enforce version")
            benchmark_min_tool_version = None
        else:
            benchmark_min_tool_version_major = m.group(1)
            benchmark_min_tool_version_minor = m.group(2)
            benchmark_min_tool_version_patch = m.group(3)
    return benchmark_min_tool_version, benchmark_min_tool_version_major, benchmark_min_tool_version_minor, benchmark_min_tool_version_patch, benchmark_tool
