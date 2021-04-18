import json
import logging
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile

import wget
import yaml

from redisbench_admin.run.common import (
    extract_benchmark_tool_settings,
    prepare_benchmark_parameters,
    merge_default_and_specific_properties_dict_type,
    process_default_yaml_properties_file,
    get_start_time_vars,
)
from redisbench_admin.run.redis_benchmark.redis_benchmark import (
    redis_benchmark_from_stdout_csv_to_json,
    redis_benchmark_ensure_min_version_local,
)
from redisbench_admin.run.ycsb.ycsb import post_process_ycsb_results
from redisbench_admin.utils.local import (
    spin_up_local_redis,
    get_local_run_full_filename,
    is_process_alive,
    check_dataset_local_requirements,
)
from redisbench_admin.utils.remote import (
    extract_git_vars,
    validate_result_expectations,
)
from redisbench_admin.utils.utils import decompress_file, get_decompressed_filename


def run_local_command_logic(args):
    (
        github_org_name,
        github_repo_name,
        github_sha,
        github_actor,
        github_branch,
        github_branch_detached,
    ) = extract_git_vars()

    local_module_file = args.module_path
    os.path.abspath(".")

    logging.info("Retrieved the following local info:")
    logging.info("\tgithub_actor: {}".format(github_actor))
    logging.info("\tgithub_org: {}".format(github_org_name))
    logging.info("\tgithub_repo: {}".format(github_repo_name))
    logging.info("\tgithub_branch: {}".format(github_branch))
    logging.info("\tgithub_sha: {}".format(github_sha))

    return_code = 0
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

    if args.test == "":
        files = pathlib.Path().glob("*.yml")
        files = [str(x) for x in files]
        if defaults_filename in files:
            files.remove(defaults_filename)

        logging.info(
            "Running all specified benchmarks: {}".format(
                " ".join([str(x) for x in files])
            )
        )
    else:
        logging.info("Running specific benchmark in file: {}".format(args.test))
        files = [args.test]

    for usecase_filename in files:
        with open(usecase_filename, "r") as stream:
            os.path.dirname(os.path.abspath(usecase_filename))
            redis_process = None
            benchmark_config = yaml.safe_load(stream)
            kpis_keyname = "kpis"
            if default_kpis is not None:
                merge_default_and_specific_properties_dict_type(
                    benchmark_config, default_kpis, kpis_keyname, usecase_filename
                )

            test_name = benchmark_config["name"]
            # after we've spinned Redis, even on error we should always teardown
            # in case of some unexpected error we fail the test
            # noinspection PyBroadException
            try:
                dirname = (".",)
                # setup Redis
                # copy the rdb to DB machine
                temporary_dir = tempfile.mkdtemp()
                logging.info(
                    "Using local temporary dir to spin up Redis Instance. Path: {}".format(
                        temporary_dir
                    )
                )
                check_dataset_local_requirements(
                    benchmark_config, temporary_dir, dirname
                )

                redis_process = spin_up_local_redis(
                    temporary_dir, args.port, local_module_file
                )
                if is_process_alive(redis_process) is False:
                    raise Exception("Redis process is not alive. Failing test.")
                # setup the benchmark
                start_time, start_time_ms, start_time_str = get_start_time_vars()
                local_benchmark_output_filename = get_local_run_full_filename(
                    start_time_str,
                    github_branch,
                    test_name,
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
                )

                # run the benchmark
                stdout, stderr = run_local_benchmark(benchmark_tool, command)
                logging.info("Extracting the benchmark results")
                logging.info("stdout: {}".format(stdout))
                logging.info("stderr: {}".format(stderr))

                post_process_benchmark_results(
                    benchmark_tool,
                    local_benchmark_output_filename,
                    start_time_ms,
                    start_time_str,
                    stdout,
                )

                # check KPIs
                result = True
                with open(local_benchmark_output_filename, "r") as json_file:
                    results_dict = json.load(json_file)

                if "kpis" in benchmark_config:
                    result = validate_result_expectations(
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


def run_local_benchmark(benchmark_tool, command):
    if benchmark_tool == "redis-benchmark" or benchmark_tool == "ycsb":
        benchmark_client_process = subprocess.Popen(
            args=command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )
    else:
        benchmark_client_process = subprocess.Popen(args=command)
    (stdout, sterr) = benchmark_client_process.communicate()
    return stdout, sterr


def post_process_benchmark_results(
    benchmark_tool,
    local_benchmark_output_filename,
    start_time_ms,
    start_time_str,
    stdout,
):
    if benchmark_tool == "redis-benchmark":
        logging.info(
            "Converting redis-benchmark output to json. Storing it in: {}".format(
                local_benchmark_output_filename
            )
        )
        results_dict = redis_benchmark_from_stdout_csv_to_json(
            stdout.decode("ascii"),
            start_time_ms,
            start_time_str,
            overload_test_name="Overall",
        )
        with open(local_benchmark_output_filename, "w") as json_file:
            json.dump(results_dict, json_file, indent=True)
    if benchmark_tool == "ycsb":
        logging.info(
            "Converting ycsb output to json. Storing it in: {}".format(
                local_benchmark_output_filename
            )
        )
        results_dict = post_process_ycsb_results(
            stdout.decode("ascii"),
            start_time_ms,
            start_time_str,
        )
        with open(local_benchmark_output_filename, "w") as json_file:
            json.dump(results_dict, json_file, indent=True)


def check_benchmark_binaries_local_requirements(
    benchmark_config, allowed_tools, binaries_localtemp_dir="./binaries"
):
    (
        benchmark_min_tool_version,
        benchmark_min_tool_version_major,
        benchmark_min_tool_version_minor,
        benchmark_min_tool_version_patch,
        benchmark_tool,
        tool_source,
        tool_source_bin_path,
    ) = extract_benchmark_tool_settings(benchmark_config)
    which_benchmark_tool = None
    if benchmark_tool is not None:
        logging.info("Detected benchmark config tool {}".format(benchmark_tool))
    else:
        raise Exception(
            "Unable to detect benchmark tool within 'clientconfig' section. Aborting..."
        )
    benchmark_tool_workdir = os.path.abspath(".")
    if benchmark_tool is not None:
        logging.info("Checking benchmark tool {} is accessible".format(benchmark_tool))
        which_benchmark_tool = shutil.which(benchmark_tool)
        if which_benchmark_tool is None:
            (
                benchmark_tool_workdir,
                which_benchmark_tool,
            ) = fetch_benchmark_tool_from_source_to_local(
                benchmark_tool,
                benchmark_tool_workdir,
                binaries_localtemp_dir,
                tool_source,
                tool_source_bin_path,
                which_benchmark_tool,
            )
        else:
            logging.info(
                "Tool {} was detected at {}".format(
                    benchmark_tool, which_benchmark_tool
                )
            )

    if benchmark_tool not in allowed_tools.split(","):
        raise Exception(
            "Benchmark tool {} not in the allowed tools list [{}]. Aborting...".format(
                benchmark_tool, allowed_tools
            )
        )

    if benchmark_min_tool_version is not None and benchmark_tool == "redis-benchmark":
        redis_benchmark_ensure_min_version_local(
            benchmark_tool,
            benchmark_min_tool_version,
            benchmark_min_tool_version_major,
            benchmark_min_tool_version_minor,
            benchmark_min_tool_version_patch,
        )
    which_benchmark_tool = os.path.abspath(which_benchmark_tool)
    return benchmark_tool, which_benchmark_tool, benchmark_tool_workdir


def fetch_benchmark_tool_from_source_to_local(
    benchmark_tool,
    benchmark_tool_workdir,
    binaries_localtemp_dir,
    tool_source,
    bin_path,
    which_benchmark_tool,
):
    if tool_source is not None and bin_path is not None:
        logging.info(
            "Tool {} was not detected on path. Using remote source to retrieve it: {}".format(
                benchmark_tool, tool_source
            )
        )
        if tool_source.startswith("http"):
            if not os.path.isdir(binaries_localtemp_dir):
                os.mkdir(binaries_localtemp_dir)
            filename = tool_source.split("/")[-1]
            full_path = "{}/{}".format(binaries_localtemp_dir, filename)
            if not os.path.exists(full_path):
                logging.info(
                    "Retrieving remote file from {} to {}. Using the dir {} as a cache for next time.".format(
                        tool_source, full_path, binaries_localtemp_dir
                    )
                )
                wget.download(tool_source, full_path)
            logging.info(
                "Decompressing {} into {}.".format(full_path, binaries_localtemp_dir)
            )
            if not os.path.exists(get_decompressed_filename(full_path)):
                full_path = decompress_file(full_path, binaries_localtemp_dir)
            else:
                full_path = get_decompressed_filename(full_path)
            benchmark_tool_workdir = os.path.abspath(full_path)
            which_benchmark_tool = os.path.abspath(
                "{}/{}".format(benchmark_tool_workdir, bin_path)
            )
            if which_benchmark_tool is None:
                raise Exception(
                    "Benchmark tool {} was not acessible at {}. Aborting...".format(
                        benchmark_tool, full_path
                    )
                )
            else:
                logging.info(
                    "Reusing cached remote file (located at {} ).".format(full_path)
                )

    else:
        raise Exception(
            "Benchmark tool {} was not acessible. Aborting...".format(benchmark_tool)
        )
    return benchmark_tool_workdir, which_benchmark_tool


def which_local(benchmark_tool, executable, full_path, which_benchmark_tool):
    if which_benchmark_tool:
        return which_benchmark_tool
    for dir_file_triple in os.walk(full_path):
        current_dir = dir_file_triple[0]
        inner_files = dir_file_triple[2]
        for filename in inner_files:
            full_path_filename = "{}/{}".format(current_dir, filename)
            st = os.stat(full_path_filename)
            mode = st.st_mode
            if mode & executable and benchmark_tool == filename:
                which_benchmark_tool = full_path_filename
                break
    return which_benchmark_tool
