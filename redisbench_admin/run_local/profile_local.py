#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import logging
import platform

from cpuinfo import get_cpu_info
from pytablewriter import MarkdownTableWriter

from redisbench_admin.profilers.perf import Perf
from redisbench_admin.profilers.vtune import Vtune

from redisbench_admin.run.s3 import get_test_s3_bucket_path
from redisbench_admin.run_local.args import PROFILE_FREQ, MAX_PROFILERS_PER_TYPE
from redisbench_admin.utils.utils import upload_artifacts_to_s3


def local_profilers_print_artifacts_table(profilers_artifacts_matrix):
    logging.info("Printing profiler generated artifacts")
    writer = MarkdownTableWriter(
        table_name="Profiler artifacts",
        headers=["Test Case", "Profiler", "Artifact", "Local file", "s3 link"],
        value_matrix=profilers_artifacts_matrix,
    )
    writer.write_table()


def local_profilers_stop_if_required(
    args,
    benchmark_duration_seconds,
    collection_summary_str,
    dso,
    github_org_name,
    github_repo_name,
    profiler_name,
    profilers_artifacts_matrix,
    profilers_enabled,
    profilers_map,
    redis_processes,
    s3_bucket_name,
    test_name,
):
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
            for setup_process_number, profiler_obj in enumerate(profiler_obj_arr):
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
                                len(profile_res_artifacts_map.values()),
                            )
                        )
                        overall_artifacts_map.update(profile_res_artifacts_map)

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


def local_profilers_start_if_required(
    profilers_enabled,
    profilers_list,
    redis_processes,
    setup_name,
    start_time_str,
    test_name,
):
    profilers_map = {}
    profiler_name = None
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
            for setup_process_number, redis_process in enumerate(redis_processes):
                if (setup_process_number + 1) > len(profiler_obj_arr):
                    continue
                profiler_obj = profiler_obj_arr[setup_process_number]
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
    return profiler_name, profilers_map


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


def local_profilers_platform_checks(
    dso, github_actor, github_branch, github_repo_name, github_sha
):
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
    return collection_summary_str
