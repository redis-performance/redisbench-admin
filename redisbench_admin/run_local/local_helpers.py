#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import logging
import os
import shutil
import subprocess
import sys

import wget

from redisbench_admin.run.redis_benchmark.redis_benchmark import (
    redis_benchmark_ensure_min_version_local,
)
from redisbench_admin.utils.benchmark_config import extract_benchmark_tool_settings
from redisbench_admin.utils.utils import get_decompressed_filename, decompress_file


BENCHMARK_TOOLS_WITH_STDOUT_RESULTS = {"redis-benchmark", "ycsb"}


def _start_local_benchmark_client(benchmark_tool, command, suppress_output=False):
    if suppress_output:
        return subprocess.Popen(
            args=command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
    if benchmark_tool in BENCHMARK_TOOLS_WITH_STDOUT_RESULTS:
        return subprocess.Popen(
            args=command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )
    return subprocess.Popen(args=command)


def run_local_benchmark(benchmark_tool, command):
    try:
        benchmark_client_process = _start_local_benchmark_client(
            benchmark_tool, command
        )
        (stdout, sterr) = benchmark_client_process.communicate()
        if sterr:
            logging.critical(
                "Error while running {}. Error: {}".format(command, sterr.strip())
            )
    except OSError as error:
        errorMessage = (
            ">>> Error while executing:\n"
            + str(command)
            + "\n>>> Returned with error:\n"
            + str(error)
        )
        logging.error(errorMessage)
    except:
        logging.error("Error > ", sys.exc_info()[0])

    return stdout, sterr


def run_local_benchmark_with_background(
    benchmark_tool, command, background_benchmark_tool, background_command
):
    background_process = _start_local_benchmark_client(
        background_benchmark_tool, background_command, suppress_output=True
    )
    try:
        benchmark_client_process = _start_local_benchmark_client(
            benchmark_tool, command
        )
        stdout, sterr = benchmark_client_process.communicate()
    except (Exception, KeyboardInterrupt):
        if background_process is not None and background_process.poll() is None:
            background_process.terminate()
            background_process.communicate()
        raise

    if benchmark_client_process.returncode != 0:
        if background_process is not None and background_process.poll() is None:
            background_process.terminate()
            background_process.communicate()
        raise RuntimeError(
            "{} exited with status {}".format(
                benchmark_tool, benchmark_client_process.returncode
            )
        )

    if background_process.poll() is None:
        logging.warning(
            "Primary client finished while the background client is still running; "
            "waiting for the background workload to finish"
        )
    background_process.communicate()
    if background_process.returncode != 0:
        raise RuntimeError(
            "Background {} exited with status {}".format(
                background_benchmark_tool, background_process.returncode
            )
        )
    return stdout, sterr


def run_local_full_benchmark(
    benchmark_tool,
    command,
    background_benchmark_tool=None,
    background_command=None,
):
    """Run a primary benchmark with an optional concurrent background client.

    The primary client's output remains the benchmark output. Both processes
    must exit successfully; otherwise the complete benchmark run fails.
    """
    if background_command is None:
        return run_local_benchmark(benchmark_tool, command)
    return run_local_benchmark_with_background(
        benchmark_tool, command, background_benchmark_tool, background_command
    )


def check_benchmark_binaries_local_requirements(
    benchmark_config,
    allowed_tools,
    binaries_localtemp_dir="./binaries",
    config_key="clientconfig",
):
    (
        benchmark_min_tool_version,
        benchmark_min_tool_version_major,
        benchmark_min_tool_version_minor,
        benchmark_min_tool_version_patch,
        benchmark_tool,
        tool_source,
        tool_source_bin_path,
        _,
    ) = extract_benchmark_tool_settings(benchmark_config, config_key)
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
    if which_benchmark_tool is not None:
        which_benchmark_tool = os.path.abspath(which_benchmark_tool)
    else:
        which_benchmark_tool = benchmark_tool
    return benchmark_tool, which_benchmark_tool, benchmark_tool_workdir


def fetch_benchmark_tool_from_source_to_local(
    benchmark_tool,
    benchmark_tool_workdir,
    binaries_localtemp_dir,
    tool_source,
    bin_path,
    which_benchmark_tool,
):
    benchmark_tool_workdir = benchmark_tool_workdir
    which_benchmark_tool = None
    if "ann" not in benchmark_tool:
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
                        "Retrieving tool remote file from {} to {}. Using the dir {} as a cache for next time.".format(
                            tool_source, full_path, binaries_localtemp_dir
                        )
                    )
                    wget.download(tool_source, full_path)
                logging.info(
                    "Decompressing {} into {}.".format(
                        full_path, binaries_localtemp_dir
                    )
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
                        "Reusing tool cached remote file (located at {} ).".format(
                            full_path
                        )
                    )

        else:
            raise Exception(
                "Benchmark tool {} was not acessible. Aborting...".format(
                    benchmark_tool
                )
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
