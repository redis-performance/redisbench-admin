#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#

import logging
import os
from shutil import copyfile

import wget

from redisbench_admin.environments.oss_cluster import get_cluster_dbfilename


def check_dataset_local_requirements(
    benchmark_config,
    redis_dbdir,
    dirname=None,
    datasets_localtemp_dir="./datasets",
    dbconfig_keyname="dbconfig",
    number_primaries=1,
    is_cluster=False,
    is_remote=False,
):
    dataset = None
    dataset_name = None
    full_path = None
    tmp_path = None
    if dbconfig_keyname in benchmark_config:
        entry_type = type(benchmark_config[dbconfig_keyname])
        if entry_type == list:
            for k in benchmark_config[dbconfig_keyname]:
                if "dataset" in k:
                    dataset = k["dataset"]
                    full_path = k["dataset"]
                if "dataset_name" in k:
                    dataset_name = k["dataset_name"]
        if entry_type == dict:
            k = benchmark_config[dbconfig_keyname]
            if "dataset" in k:
                dataset = k["dataset"]
                full_path = k["dataset"]
            if "dataset_name" in k:
                dataset_name = k["dataset_name"]
        if dataset is not None and is_remote is False:
            full_path = check_if_needs_remote_fetch(
                dataset, datasets_localtemp_dir, dirname, None, is_remote
            )

            if is_cluster is False:
                tmp_path = "{}/dump.rdb".format(redis_dbdir)
                logging.info("Copying rdb from {} to {}".format(full_path, tmp_path))
                copyfile(full_path, tmp_path)
            else:
                start_port = 6379
                for primary_number in range(number_primaries):
                    primary_port = start_port + primary_number
                    tmp_path = "{}/{}".format(
                        redis_dbdir, get_cluster_dbfilename(primary_port)
                    )
                    logging.info(
                        "Copying rdb from {} to {}".format(full_path, tmp_path)
                    )
                    copyfile(full_path, tmp_path)

    return dataset, dataset_name, full_path, tmp_path


def check_if_needs_remote_fetch(
    property, localtemp_dir, dirname, full_path=None, is_remote=False
):
    if property.startswith("http"):
        if not os.path.isdir(localtemp_dir):
            os.mkdir(localtemp_dir)
        if full_path is None:
            filename = property.split("/")[-1]
            full_path = "{}/{}".format(localtemp_dir, filename)
        if not os.path.exists(full_path) and is_remote is False:
            logging.info(
                "Retrieving remote file from {} to {}. Using the dir {} as a cache for next time.".format(
                    property, full_path, localtemp_dir
                )
            )
            wget.download(property, full_path)
        else:
            logging.info(
                "Reusing cached remote file (located at {} ).".format(full_path)
            )
    else:
        full_path = property
        if dirname is not None:
            full_path = "{}/{}".format(dirname, full_path)

    return full_path


def is_process_alive(process):
    if not process:
        return False
    # Check if child process has terminated. Set and return returncode
    # attribute
    if process.poll() is None:
        return True
    return False


def get_local_run_full_filename(
    start_time_str,
    github_branch,
    test_name,
    setup_name,
):
    benchmark_output_filename = (
        "{setup_name}-{start_time_str}-{github_branch}-{test_name}.json".format(
            setup_name=setup_name,
            start_time_str=start_time_str,
            github_branch=github_branch,
            test_name=test_name,
        )
    )
    return benchmark_output_filename
