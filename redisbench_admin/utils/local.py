import logging
import os
import subprocess
import time
from shutil import copyfile

import redis
import wget


def check_dataset_local_requirements(
    benchmark_config,
    redis_dbdir,
    dirname=None,
    datasets_localtemp_dir="./datasets",
    dbconfig_keyname="dbconfig",
):
    dataset = None
    full_path = None
    tmp_path = None
    if dbconfig_keyname in benchmark_config:
        for k in benchmark_config[dbconfig_keyname]:
            if "dataset" in k:
                dataset = k["dataset"]
        if dataset is not None:
            if dataset.startswith("http"):
                if not os.path.isdir(datasets_localtemp_dir):
                    os.mkdir(datasets_localtemp_dir)
                filename = dataset.split("/")[-1]
                full_path = "{}/{}".format(datasets_localtemp_dir, filename)
                if not os.path.exists(full_path):
                    logging.info(
                        "Retrieving remote file from {} to {}. Using the dir {} as a cache for next time.".format(
                            dataset, full_path, datasets_localtemp_dir
                        )
                    )
                    wget.download(dataset, full_path)
                else:
                    logging.info(
                        "Reusing cached remote file (located at {} ).".format(full_path)
                    )
            else:
                full_path = dataset
                if dirname is not None:
                    full_path = "{}/{}".format(dirname, full_path)
                logging.info(
                    "Copying rdb from {} to {}/dump.rdb".format(full_path, redis_dbdir)
                )
            tmp_path = "{}/dump.rdb".format(redis_dbdir)
            copyfile(full_path, tmp_path)
    return dataset, full_path, tmp_path


def wait_for_conn(conn, retries=20, command="PING", should_be=True):
    """Wait until a given Redis connection is ready"""
    result = False
    while retries > 0 and result is False:
        try:
            if conn.execute_command(command) == should_be:
                result = True
        except redis.exceptions.BusyLoadingError:
            time.sleep(0.1)  # give extra 100msec in case of RDB loading
        except redis.ConnectionError as err:
            str(err)
        except redis.ResponseError as err:
            err1 = str(err)
            if not err1.startswith("DENIED"):
                raise
        time.sleep(0.1)
        retries -= 1
        logging.debug("Waiting for Redis")
    return result


def spin_up_local_redis(
    dbdir,
    port,
    local_module_file,
):
    command = generate_standalone_redis_server_args(dbdir, local_module_file, port)

    logging.info(
        "Running local redis-server with the following args: {}".format(
            " ".join(command)
        )
    )
    redis_process = subprocess.Popen(command)
    result = wait_for_conn(redis.StrictRedis())
    if result is True:
        logging.info("Redis available")
    return redis_process


def generate_standalone_redis_server_args(dbdir, local_module_file, port):
    # start redis-server
    command = [
        "redis-server",
        "--save",
        '""',
        "--port",
        "{}".format(port),
        "--dir",
        dbdir,
    ]
    if local_module_file is not None:
        command.extend(
            [
                "--loadmodule",
                os.path.abspath(local_module_file),
            ]
        )
    return command


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
):
    benchmark_output_filename = (
        "{start_time_str}-{github_branch}-{test_name}.json".format(
            start_time_str=start_time_str,
            github_branch=github_branch,
            test_name=test_name,
        )
    )
    return benchmark_output_filename
