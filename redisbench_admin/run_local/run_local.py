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
import traceback

import redis
import wget
from rediscluster import RedisCluster

from redisbench_admin.environments.oss_cluster import spin_up_local_redis_cluster
from redisbench_admin.profilers.perf import Perf
from cpuinfo import get_cpu_info
from pytablewriter import MarkdownTableWriter

from redisbench_admin.profilers.vtune import Vtune
from redisbench_admin.run.common import (
    prepare_benchmark_parameters,
    get_start_time_vars,
    execute_init_commands,
    BENCHMARK_REPETITIONS,
    extract_test_feasible_setups,
    get_setup_type_and_primaries_count,
)
from redisbench_admin.run_local.args import PROFILE_FREQ, MAX_PROFILERS_PER_TYPE
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
    get_local_run_full_filename,
    is_process_alive,
    check_dataset_local_requirements,
)
from redisbench_admin.environments.oss_standalone import spin_up_local_redis
from redisbench_admin.utils.remote import (
    extract_git_vars,
)
from redisbench_admin.utils.results import post_process_benchmark_results
from redisbench_admin.utils.utils import (
    decompress_file,
    get_decompressed_filename,
    upload_artifacts_to_s3,
)

# from rediscluster import RedisCluster


def run_local_command_logic(args):
    (
        github_org_name,
        github_repo_name,
        github_sha,
        github_actor,
        github_branch,
        github_branch_detached,
    ) = extract_git_vars()

    dbdir_folder = args.dbdir_folder
    os.path.abspath(".")
    required_modules = args.required_module
    profilers_enabled = args.enable_profilers
    s3_bucket_name = args.s3_bucket_name
    profilers_list = []
    if profilers_enabled:
        profilers_list = args.profilers.split(",")
        res = check_compatible_system_and_kernel_and_prepare_profile(args)
        if res is False:
            exit(1)

    logging.info("Retrieved the following local info:")
    logging.info("\tgithub_actor: {}".format(github_actor))
    logging.info("\tgithub_org: {}".format(github_org_name))
    logging.info("\tgithub_repo: {}".format(github_repo_name))
    logging.info("\tgithub_branch: {}".format(github_branch))
    logging.info("\tgithub_sha: {}".format(github_sha))

    local_module_file = args.module_path
    logging.info("Using the following modules {}".format(local_module_file))
    dso = args.dso
    if dso is None:
        logging.warning("No dso specified for perf analysis {}".format(dso))
        if local_module_file is not None:

            if type(local_module_file) == str:
                dso = local_module_file
                logging.warning(
                    "Using provided module = {} to specify dso".format(
                        local_module_file
                    )
                )
            if type(local_module_file) == list:
                dso = local_module_file[0]
                logging.warning(
                    "Using first module = {} to specify dso".format(
                        local_module_file[0]
                    )
                )

    logging.info("Using dso for perf analysis {}".format(dso))

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

    (
        benchmark_definitions,
        _,
        _,
        default_specs,
        clusterconfig,
    ) = prepare_benchmark_definitions(args)

    return_code = 0
    profilers_artifacts_matrix = []
    for repetition in range(1, BENCHMARK_REPETITIONS + 1):
        for test_name, benchmark_config in benchmark_definitions.items():
            logging.info(
                "Repetition {} of {}. Running test {}".format(
                    repetition, BENCHMARK_REPETITIONS, test_name
                )
            )
            test_setups = extract_test_feasible_setups(
                benchmark_config, "setups", default_specs
            )
            for setup_name, setup_settings in test_setups.items():
                setup_type, shard_count = get_setup_type_and_primaries_count(
                    setup_settings
                )
                if setup_type in args.allowed_envs:
                    redis_processes = []
                    logging.info(
                        "Starting setup named {} of topology type {}. Total primaries: {}".format(
                            setup_name, setup_type, shard_count
                        )
                    )
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
                        if dbdir_folder is not None:
                            from distutils.dir_util import copy_tree

                            copy_tree(dbdir_folder, temporary_dir)
                            logging.info(
                                "Copied entire content of {} into temporary path: {}".format(
                                    dbdir_folder, temporary_dir
                                )
                            )
                        (
                            redis_configuration_parameters,
                            _,
                        ) = extract_redis_dbconfig_parameters(
                            benchmark_config, "dbconfig"
                        )
                        cluster_api_enabled = False

                        if setup_type == "oss-cluster":
                            cluster_api_enabled = True
                            # pass
                            redis_processes = spin_up_local_redis_cluster(
                                temporary_dir,
                                shard_count,
                                args.port,
                                local_module_file,
                                redis_configuration_parameters,
                                dbdir_folder,
                            )
                            for redis_process in redis_processes:
                                if is_process_alive(redis_process) is False:
                                    raise Exception(
                                        "Redis process is not alive. Failing test."
                                    )
                            # we use node 0 for the checks
                            r = redis.StrictRedis(port=args.port)
                            r_conns = []
                            for p in range(args.port, args.port + shard_count):
                                redis.StrictRedis(port=p).execute_command(
                                    "CLUSTER SAVECONFIG"
                                )

                        check_dataset_local_requirements(
                            benchmark_config,
                            temporary_dir,
                            dirname,
                            "./datasets",
                            "dbconfig",
                            shard_count,
                            cluster_api_enabled,
                        )

                        if setup_type == "oss-standalone":
                            redis_processes = spin_up_local_redis(
                                temporary_dir,
                                args.port,
                                local_module_file,
                                redis_configuration_parameters,
                                dbdir_folder,
                            )

                            for redis_process in redis_processes:
                                if is_process_alive(redis_process) is False:
                                    raise Exception(
                                        "Redis process is not alive. Failing test."
                                    )

                            r = redis.StrictRedis(port=args.port)

                        if setup_type == "oss-cluster":
                            startup_nodes = []
                            for p in range(args.port, args.port + shard_count):
                                primary_conn = redis.StrictRedis(port=p)
                                primary_conn.execute_command("DEBUG RELOAD NOSAVE")
                                r_conns.append(primary_conn)
                                startup_nodes.append(
                                    {"host": "127.0.0.1", "port": "{}".format(p)}
                                )
                            if clusterconfig is not None:
                                if "init_commands" in clusterconfig:
                                    for command_group in clusterconfig["init_commands"]:
                                        skip = False
                                        if "when_modules_present" in command_group:
                                            m_found = False
                                            for module_required in command_group[
                                                "when_modules_present"
                                            ]:
                                                if type(local_module_file) == list:
                                                    for local_m in local_module_file:
                                                        if module_required in local_m:
                                                            m_found = True
                                                            logging.info(
                                                                "Required module {}  found in {}".format(
                                                                    module_required,
                                                                    local_m,
                                                                )
                                                            )
                                                else:
                                                    if (
                                                        module_required
                                                        in local_module_file
                                                    ):
                                                        m_found = True
                                                        logging.info(
                                                            "Required module {}  found in {}".format(
                                                                module_required,
                                                                local_module_file,
                                                            )
                                                        )
                                            skip = not (m_found)
                                        if skip is False:
                                            for command in command_group["commands"]:
                                                for conn_n, rc in enumerate(r_conns):
                                                    rc.execute_command(command)
                                                    logging.info(
                                                        "Cluster node {}: sent command {}".format(
                                                            conn_n + 1, command
                                                        )
                                                    )
                                        else:
                                            logging.info(
                                                "Skipping to send the command group: {}.".format(
                                                    command_group["commands"],
                                                )
                                                + "Given the when_modules_present condition ({}) is not true.".format(
                                                    command_group[
                                                        "when_modules_present"
                                                    ],
                                                )
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
                        stdout = r.execute_command("info modules")
                        (
                            module_names,
                            _,
                        ) = extract_module_semver_from_info_modules_cmd(stdout)

                        check_required_modules(module_names, required_modules)

                        # run initialization commands before benchmark starts
                        logging.info(
                            "Running initialization commands before benchmark starts."
                        )
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
                        (
                            start_time,
                            start_time_ms,
                            start_time_str,
                        ) = get_start_time_vars()
                        local_benchmark_output_filename = get_local_run_full_filename(
                            start_time_str, github_branch, test_name, setup_name
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
                            cluster_api_enabled,
                        )

                        # start the profile
                        if profilers_enabled:
                            logging.info("Profilers are enabled")
                            total_involved_processes = len(redis_processes)
                            _, profilers_map = get_profilers_map(
                                profilers_list,
                                total_involved_processes,
                                MAX_PROFILERS_PER_TYPE,
                            )
                            for (
                                profiler_name,
                                profiler_obj_arr,
                            ) in profilers_map.items():
                                for setup_process_number, redis_process in enumerate(
                                    redis_processes
                                ):
                                    if (setup_process_number + 1) > len(
                                        profiler_obj_arr
                                    ):
                                        continue
                                    profiler_obj = profiler_obj_arr[
                                        setup_process_number
                                    ]
                                    setup_process_number = setup_process_number + 1
                                    logging.info(
                                        "Starting profiler {} for Process {} of {}: pid {}".format(
                                            profiler_name,
                                            setup_process_number,
                                            total_involved_processes,
                                            redis_process.pid,
                                        )
                                    )
                                    profile_filename = (
                                        "profile_{setup_name}".format(
                                            setup_name=setup_name,
                                        )
                                        + "__primary-{primary_n}-of-{total_primaries}".format(
                                            primary_n=setup_process_number,
                                            total_primaries=total_involved_processes,
                                        )
                                        + "__{test_name}_{profile}_{start_time_str}.out".format(
                                            profile=profiler_name,
                                            test_name=test_name,
                                            start_time_str=start_time_str,
                                        )
                                    )
                                    profiler_obj.start_profile(
                                        redis_process.pid,
                                        profile_filename,
                                        PROFILE_FREQ,
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
                            overall_artifacts_map = {}
                            expected_min_duration = 60
                            if benchmark_duration_seconds < expected_min_duration:
                                logging.warning(
                                    "Total benchmark duration ({} secs) was bellow {} seconds. ".format(
                                        benchmark_duration_seconds,
                                        expected_min_duration,
                                    )
                                    + "Given the profile frequency {} it means that at max we mad {} profiles.".format(
                                        PROFILE_FREQ,
                                        int(PROFILE_FREQ) * benchmark_duration_seconds,
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
                            total_involved_processes = len(redis_processes)
                            for (
                                profiler_name,
                                profiler_obj_arr,
                            ) in profilers_map.items():
                                for setup_process_number, profiler_obj in enumerate(
                                    profiler_obj_arr
                                ):
                                    logging.info(
                                        "Stopping profiler {} for Process {} of {}: pid {}".format(
                                            profiler_name,
                                            setup_process_number,
                                            total_involved_processes,
                                            profiler_obj.pid,
                                        )
                                    )

                                    profile_res = profiler_obj.stop_profile()
                                    if profile_res is True:
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
                                            binary=dso,
                                            primary_id=(setup_process_number + 1),
                                            total_primaries=total_involved_processes,
                                        )
                                        if profile_res is True:
                                            logging.info(
                                                "Profiler {} for pid {} ran successfully and generated {} artifacts.".format(
                                                    profiler_name,
                                                    profiler_obj.pid,
                                                    len(
                                                        profile_res_artifacts_map.values()
                                                    ),
                                                )
                                            )
                                            overall_artifacts_map.update(
                                                profile_res_artifacts_map
                                            )

                            for (
                                artifact_name,
                                profile_artifact,
                            ) in overall_artifacts_map.items():
                                s3_link = None
                                if args.upload_results_s3:
                                    logging.info(
                                        "Uploading results to s3. s3 bucket name: {}. s3 bucket path: {}".format(
                                            s3_bucket_name, s3_bucket_path
                                        )
                                    )
                                    url_map = upload_artifacts_to_s3(
                                        [profile_artifact],
                                        s3_bucket_name,
                                        s3_bucket_path,
                                    )
                                    s3_link = list(url_map.values())[0]
                                profilers_artifacts_matrix.append(
                                    [
                                        test_name,
                                        profiler_name,
                                        artifact_name,
                                        profile_artifact,
                                        s3_link,
                                    ]
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
                        stdout = r.shutdown(save=False)
                    except:
                        return_code |= 1
                        logging.critical(
                            "Some unexpected exception was caught during local work. Failing test...."
                        )
                        logging.critical(sys.exc_info())
                        logging.critical("Traceback: {}".format(traceback.format_exc()))
                    # tear-down
                    logging.info("Tearing down setup")
                    for redis_process in redis_processes:
                        if redis_process is not None:
                            redis_process.kill()
                    logging.info("Tear-down completed")

    if profilers_enabled:
        logging.info("Printing profiler generated artifacts")

        writer = MarkdownTableWriter(
            table_name="Profiler artifacts",
            headers=["Test Case", "Profiler", "Artifact", "Local file", "s3 link"],
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
    res, _ = get_profilers_map(args.profilers.split(","), 1)
    return res


def get_profilers_map(profilers_list, total_involved_processes, max_profilers=1):
    profilers_map = {}
    res = True
    for profiler_name in profilers_list:
        try:
            profilers_map[profiler_name] = []
            if "perf:" in profiler_name:
                for profilers_per_type in range(1, total_involved_processes + 1):
                    if profilers_per_type <= max_profilers:
                        logging.info(
                            "Preparing perf (a.k.a. perf_events ) profiler for proc {} of {}".format(
                                profilers_per_type, total_involved_processes
                            )
                        )
                        perf = Perf()
                        profilers_map[profiler_name].append(perf)
            if "vtune" in profiler_name:

                for profilers_per_type in range(total_involved_processes):
                    logging.info(
                        "Preparing Intel(R) VTune(TM) profiler for proc {} of {}".format(
                            profilers_per_type, total_involved_processes
                        )
                    )
                    if profilers_per_type <= max_profilers:
                        vtune = Vtune()
                        profilers_map[profiler_name].append(vtune)
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
