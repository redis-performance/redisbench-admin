#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import logging
import os
import shutil
import subprocess

import wget

from redisbench_admin.run.redis_benchmark.redis_benchmark import (
    redis_benchmark_ensure_min_version_local,
)
from redisbench_admin.utils.benchmark_config import extract_benchmark_tool_settings
from redisbench_admin.utils.utils import get_decompressed_filename, decompress_file


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
        _,
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
