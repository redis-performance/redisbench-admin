#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#

import json
import logging
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import datetime

import redis
import wget
from redisbench_admin.profilers.perf import Perf
from cpuinfo import get_cpu_info
from pytablewriter import MarkdownTableWriter

from redisbench_admin.run.common import (
    prepare_benchmark_parameters,
    get_start_time_vars,
    execute_init_commands,
)
from redisbench_admin.run_local.args import PROFILE_FREQ
from redisbench_admin.utils.benchmark_config import (
    prepare_benchmark_definitions,
    extract_benchmark_tool_settings,
    check_required_modules,
    results_dict_kpi_check,
    extract_redis_dbconfig_parameters,
)
from redisbench_admin.run.redis_benchmark.redis_benchmark import (
    redis_benchmark_ensure_min_version_local,
)
from redisbench_admin.run_remote.run_remote import (
    extract_module_semver_from_info_modules_cmd,
    get_test_s3_bucket_path,
)
from redisbench_admin.utils.local import (
    spin_up_local_redis,
    get_local_run_full_filename,
    is_process_alive,
    check_dataset_local_requirements,
)
from redisbench_admin.utils.remote import (
    extract_git_vars,
)
from redisbench_admin.utils.results import post_process_benchmark_results
from redisbench_admin.utils.utils import (
    decompress_file,
    get_decompressed_filename,
    upload_artifacts_to_s3,
)


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
    required_modules = args.required_module
    profilers_enabled = args.enable_profilers
    s3_bucket_name = args.s3_bucket_name
    profilers_map = {}
    profilers_list = []
    if profilers_enabled:
        profilers_list = args.profilers.split(",")
        res, profilers_map = check_compatible_system_and_kernel_and_prepare_profile(
            args
        )
        if res is False:
            exit(1)

    logging.info("Retrieved the following local info:")
    logging.info("\tgithub_actor: {}".format(github_actor))
    logging.info("\tgithub_org: {}".format(github_org_name))
    logging.info("\tgithub_repo: {}".format(github_repo_name))
    logging.info("\tgithub_branch: {}".format(github_branch))
    logging.info("\tgithub_sha: {}".format(github_sha))

    local_platform_info = get_cpu_info()
    cpu_brand = local_platform_info["brand"]
    cpu_core_count = local_platform_info["count"]
    platform_uname_release = platform.uname().release
    platform_uname_system = platform.uname().system
    platform_uname_node = platform.uname().node
    span_x = 800
    collection_summary_str = (
        '<tspan x="{}" dy="1.2em">Collection platform: system=\'{}\''.format(
            span_x, platform_uname_system
        )
        + " release='{}', node='{}', cpu='{}', core-count={}</tspan>".format(
            platform_uname_release,
            platform_uname_node,
            cpu_brand,
            cpu_core_count,
        )
    )
    collection_summary_str += (
        '<tspan x="{}" dy="1.2em">Collection trigger: github_actor=\'{}\' '.format(
            span_x, github_actor
        )
    )
    collection_summary_str += (
        " github_repo='{}', github_branch='{}', github_sha='{}'</tspan>".format(
            github_repo_name, github_branch, github_sha
        )
    )

    benchmark_definitions, _, _ = prepare_benchmark_definitions(args)

    return_code = 0
    profilers_artifacts_matrix = []

    for test_name, benchmark_config in benchmark_definitions.items():
        redis_process = None

        # after we've spinned Redis, even on error we should always teardown
        # in case of some unexpected error we fail the test
        # noinspection PyBroadException
        try:
            dirname = "."
            # setup Redis
            # copy the rdb to DB machine
            temporary_dir = tempfile.mkdtemp()
            logging.info(
                "Using local temporary dir to spin up Redis Instance. Path: {}".format(
                    temporary_dir
                )
            )
            check_dataset_local_requirements(benchmark_config, temporary_dir, dirname)

            redis_configuration_parameters, _ = extract_redis_dbconfig_parameters(
                benchmark_config, "dbconfig"
            )

            redis_process = spin_up_local_redis(
                temporary_dir,
                args.port,
                local_module_file,
                redis_configuration_parameters,
            )

            if is_process_alive(redis_process) is False:
                raise Exception("Redis process is not alive. Failing test.")

            r = redis.StrictRedis(port=args.port)
            stdout = r.execute_command("info modules")
            (
                module_names,
                _,
            ) = extract_module_semver_from_info_modules_cmd(stdout)

            check_required_modules(module_names, required_modules)

            # run initialization commands before benchmark starts
            logging.info("Running initialization commands before benchmark starts.")
            execute_init_commands_start_time = datetime.datetime.now()
            execute_init_commands(benchmark_config, r)
            execute_init_commands_duration_seconds = (
                datetime.datetime.now() - execute_init_commands_start_time
            ).seconds
            logging.info(
                "Running initialization commands took {} secs.".format(
                    execute_init_commands_duration_seconds
                )
            )

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

            # start the profile
            if profilers_enabled:
                logging.info("Profilers are enabled")
                _, profilers_map = get_profilers_map(profilers_list)
                for profiler_name, profiler_obj in profilers_map.items():
                    logging.info(
                        "Starting profiler {} for pid {}".format(
                            profiler_name, redis_process.pid
                        )
                    )
                    profile_filename = (
                        "profile_{test_name}_{profile}_{start_time_str}.out".format(
                            profile=profiler_name,
                            test_name=test_name,
                            start_time_str=start_time_str,
                        )
                    )
                    profiler_obj.start_profile(
                        redis_process.pid, profile_filename, PROFILE_FREQ
                    )

            # run the benchmark
            benchmark_start_time = datetime.datetime.now()
            stdout, stderr = run_local_benchmark(benchmark_tool, command)
            benchmark_end_time = datetime.datetime.now()
            benchmark_duration_seconds = (
                benchmark_end_time - benchmark_start_time
            ).seconds

            logging.info("Extracting the benchmark results")
            logging.info("stdout: {}".format(stdout))
            logging.info("stderr: {}".format(stderr))

            if profilers_enabled:
                expected_min_duration = 60
                if benchmark_duration_seconds < expected_min_duration:
                    logging.warning(
                        "Total benchmark duration ({} secs) was bellow {} seconds. ".format(
                            benchmark_duration_seconds, expected_min_duration
                        )
                        + "Given the profile frequency {} it means that at max we mad {} profiles.".format(
                            PROFILE_FREQ, int(PROFILE_FREQ) * benchmark_duration_seconds
                        )
                        + "Please increase benchmark time for more accurate profiles."
                        + "If that is not possible please change the profile frequency to an higher value."
                        + "via the env variable PROFILE_FREQ. NOTICE THAT THIS INCREASES OVERHEAD!!!"
                    )
                s3_bucket_path = get_test_s3_bucket_path(
                    s3_bucket_name,
                    test_name,
                    github_org_name,
                    github_repo_name,
                    "profiles",
                )
                for profiler_name, profiler_obj in profilers_map.items():
                    # Collect and fold stacks
                    logging.info(
                        "Stopping profiler {} for pid {}".format(
                            profiler_name, redis_process.pid
                        )
                    )
                    profiler_obj.stop_profile()

                    # Generate:
                    #  - artifact with Flame Graph SVG
                    #  - artifact with output graph image in PNG format
                    #  - artifact with top entries in text form
                    (
                        profile_res,
                        profile_res_artifacts_map,
                    ) = profiler_obj.generate_outputs(
                        test_name,
                        details=collection_summary_str,
                        binary=local_module_file,
                    )
                    if profile_res is True:
                        logging.info(
                            "Profiler {} for pid {} ran successfully and generated {} artifacts.".format(
                                profiler_name,
                                redis_process.pid,
                                len(profile_res_artifacts_map.values()),
                            )
                        )
                        artifact_paths = []
                        for (
                            artifact_name,
                            profile_artifact,
                        ) in profile_res_artifacts_map.items():
                            profilers_artifacts_matrix.append(
                                [
                                    test_name,
                                    profiler_name,
                                    artifact_name,
                                    profile_artifact,
                                ]
                            )
                            artifact_paths.append(profile_artifact)
                            logging.info(
                                "artifact {}: {}.".format(
                                    artifact_name, profile_artifact
                                )
                            )
                        if args.upload_results_s3:
                            logging.info(
                                "Uploading results to s3. s3 bucket name: {}. s3 bucket path: {}".format(
                                    s3_bucket_name, s3_bucket_path
                                )
                            )
                            upload_artifacts_to_s3(
                                artifact_paths, s3_bucket_name, s3_bucket_path
                            )

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

    if profilers_enabled:
        logging.info("Printing profiler generated artifacts")

        writer = MarkdownTableWriter(
            table_name="Profiler artifacts",
            headers=["Test Case", "Profiler", "Artifact", "Local file"],
            value_matrix=profilers_artifacts_matrix,
        )
        writer.write_table()

    exit(return_code)


def check_compatible_system_and_kernel_and_prepare_profile(args):
    """
    Checks if we can do local profiling, that for now is only available
    via Linux based platforms and kernel versions >=4.9
    Args:
        args:
    """
    res = True
    logging.info("Enabled profilers: {}".format(args.profilers))
    logging.info("Checking if system is capable of running those profilers")
    if "Linux" not in platform.system():
        logging.error(
            "Platform needs to be Linux based. Current platform: {}".format(
                platform.system()
            )
        )
        res = False
    # check for release >= 4.9
    release = platform.release()
    logging.info("Detected platform release: {}".format(release))
    major_minor = release.split(".")[:2]
    system_kernel_major_v = major_minor[0]
    system_kernel_minor_v = major_minor[1]
    if float(system_kernel_major_v) < 4:
        logging.error(
            "kernel version needs to be >= 4.9. Detected version: {}".format(release)
        )
        res = False
    if float(system_kernel_major_v) == 4 and float(system_kernel_minor_v) < 9:
        logging.error(
            "kernel version needs to be >= 4.9. Detected version: {}".format(release)
        )
        res = False
    # a map between profiler name and profiler object wrapper
    res, profilers_map = get_profilers_map(args.profilers.split(","))
    return res, profilers_map


def get_profilers_map(profilers_list):
    profilers_map = {}
    res = True
    for profiler_name in profilers_list:
        try:
            if "perf:" in profiler_name:
                logging.info("Preparing perf (a.k.a. perf_events ) profiler")
                perf = Perf()
                profilers_map[profiler_name] = perf
        except Exception as e:
            logging.error(
                "Detected error while trying to prepare profiler named {}. Error: {}".format(
                    profiler_name, e.__str__()
                )
            )
            res = False
    return res, profilers_map


def run_local_benchmark(benchmark_tool, command):
    if benchmark_tool == "redis-benchmark" or benchmark_tool == "ycsb":
        benchmark_client_process = subprocess.Popen(
            args=command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )
    else:
        benchmark_client_process = subprocess.Popen(args=command)
    (stdout, sterr) = benchmark_client_process.communicate()
    return stdout, sterr


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
