import datetime as dt
import json
import logging
import os
import pathlib
import re
import subprocess
import sys

import yaml

from redisbench_admin.run.redis_benchmark.redis_benchmark import prepareRedisBenchmarkCommand, \
    redis_benchmark_from_stdout_csv_to_json
from redisbench_admin.run.run import redis_benchmark_ensure_min_version
from redisbench_admin.utils.local import (
    spinUpLocalRedis,
    getLocalRunFullFilename,
    isProcessAlive, prepareRedisGraphBenchmarkGoCommand,
)
from redisbench_admin.utils.remote import (
    extract_git_vars,
    validateResultExpectations,
)


def run_local_command_logic(args):
    (
        github_org_name,
        github_repo_name,
        github_sha,
        github_actor,
        github_branch,
    ) = extract_git_vars()

    local_module_file = args.module_path

    logging.info("Retrieved the following local info:")
    logging.info("\tgithub_actor: {}".format(github_actor))
    logging.info("\tgithub_org: {}".format(github_org_name))
    logging.info("\tgithub_repo: {}".format(github_repo_name))
    logging.info("\tgithub_branch: {}".format(github_branch))
    logging.info("\tgithub_sha: {}".format(github_sha))

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

    for f in files:
        with open(f, "r") as stream:
            dirname = os.path.dirname(os.path.abspath(f))
            redis_process = None
            benchmark_config = yaml.safe_load(stream)
            test_name = benchmark_config["name"]
            # after we've spinned Redis, even on error we should always teardown
            # in case of some unexpected error we fail the test
            try:
                # setup Redis
                redis_process = spinUpLocalRedis(
                    benchmark_config,
                    args.port,
                    local_module_file, dirname,
                )
                if isProcessAlive(redis_process) is False:
                    raise Exception("Redis process is not alive. Failing test.")
                # setup the benchmark
                start_time = dt.datetime.now()
                start_time_str = start_time.strftime("%Y-%m-%d-%H-%M-%S")
                local_benchmark_output_filename = getLocalRunFullFilename(
                    start_time_str,
                    github_branch,
                    test_name,
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
                                "localhost",
                                args.port,
                                entry
                            )
                        if benchmark_tool == 'redisgraph-benchmark-go':
                            print(entry)
                            command = prepareRedisGraphBenchmarkGoCommand(
                                "redisgraph-benchmark-go",
                                "localhost",
                                args.port,
                                entry,
                                local_benchmark_output_filename,
                            )

                # run the benchmark
                if benchmark_tool == 'redis-benchmark':
                    benchmark_client_process = subprocess.Popen(args=command, stdout=subprocess.PIPE,
                                                                stderr=subprocess.STDOUT)
                else:
                    benchmark_client_process = subprocess.Popen(args=command)
                (stdout, sterr) = benchmark_client_process.communicate()
                logging.info("Extracting the benchmark results")

                if benchmark_tool == 'redis-benchmark':
                    logging.info("Converting redis-benchmark output to json. Storing it in: {}".format(
                        local_benchmark_output_filename))
                    results_dict = redis_benchmark_from_stdout_csv_to_json(stdout, start_time, start_time_str)
                    with open(local_benchmark_output_filename, "w") as json_file:
                        json.dump(results_dict, json_file, indent=True)

                # check KPIs
                result = True
                with open(local_benchmark_output_filename, "r") as json_file:
                    results_dict = json.load(json_file)

                if "kpis" in benchmark_config:
                    result = validateResultExpectations(
                        benchmark_config, results_dict, result, expectations_key="kpis"
                    )
                    if result is not True:
                        return_code |= 1
            except:
                return_code |= 1
                logging.critical(
                    "Some unexpected exception was caught during remote work. Failing test...."
                )
                logging.critical(sys.exc_info())
        # tear-down
        logging.info("Tearing down setup")
        if redis_process is not None:
            redis_process.kill()
        logging.info("Tear-down completed")

    exit(return_code)
