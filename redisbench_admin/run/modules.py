#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import logging
import os


def redis_modules_check(local_module_files):
    status = True
    error_message = ""
    if local_module_files is not None:
        logging.info(
            "Using the following module artifacts: {}".format(local_module_files)
        )
        for local_module_file in local_module_files:
            if type(local_module_file) is str:
                logging.info(
                    "Checking if module artifact {} exists...".format(local_module_file)
                )
                error_message, status = exists_check(
                    error_message, local_module_file, status
                )
            if type(local_module_file) is list:
                for inner_local_module_file in local_module_file:
                    logging.info(
                        "Checking if module artifact {} exists...".format(
                            inner_local_module_file
                        )
                    )
                    error_message, status = exists_check(
                        error_message, inner_local_module_file, status
                    )

    return status, error_message


def exists_check(error_message, local_module_file, status):
    if os.path.exists(local_module_file) is False:
        error_message = "Specified module artifact does not exist: {}".format(
            local_module_file
        )
        logging.error(error_message)
        status = False
    else:
        logging.info(
            "Confirmed that module artifact: '{}' exists!".format(local_module_file)
        )
    return error_message, status


def redis_files_check(redis_server_binary_path, redis_conf_path):
    """
    Check if custom Redis server binary and config file paths exist.

    Args:
        redis_server_binary_path: Path to custom redis-server binary (can be None)
        redis_conf_path: Path to custom redis.conf file (can be None)

    Returns:
        tuple: (status, error_message) where status is True if all files exist
    """
    status = True
    error_message = ""

    if redis_server_binary_path is not None:
        # Convert relative paths to absolute paths
        redis_server_binary_path = os.path.abspath(
            os.path.expanduser(redis_server_binary_path)
        )
        logging.info(
            "Checking if custom Redis server binary {} exists...".format(
                redis_server_binary_path
            )
        )
        if not os.path.exists(redis_server_binary_path):
            error_message = "Specified Redis server binary does not exist: {}".format(
                redis_server_binary_path
            )
            logging.error(error_message)
            status = False
        elif not os.path.isfile(redis_server_binary_path):
            error_message = (
                "Specified Redis server binary path is not a file: {}".format(
                    redis_server_binary_path
                )
            )
            logging.error(error_message)
            status = False
        elif not os.access(redis_server_binary_path, os.X_OK):
            error_message = (
                "Specified Redis server binary is not executable: {}".format(
                    redis_server_binary_path
                )
            )
            logging.error(error_message)
            status = False
        else:
            logging.info(
                "✅ Confirmed that Redis server binary: '{}' exists and is executable!".format(
                    redis_server_binary_path
                )
            )

    if redis_conf_path is not None:
        # Convert relative paths to absolute paths
        redis_conf_path = os.path.abspath(os.path.expanduser(redis_conf_path))
        logging.info(
            "Checking if custom Redis config file {} exists...".format(redis_conf_path)
        )
        if not os.path.exists(redis_conf_path):
            error_message = "Specified Redis config file does not exist: {}".format(
                redis_conf_path
            )
            logging.error(error_message)
            status = False
        elif not os.path.isfile(redis_conf_path):
            error_message = "Specified Redis config file path is not a file: {}".format(
                redis_conf_path
            )
            logging.error(error_message)
            status = False
        elif not os.access(redis_conf_path, os.R_OK):
            error_message = "Specified Redis config file is not readable: {}".format(
                redis_conf_path
            )
            logging.error(error_message)
            status = False
        else:
            logging.info(
                "✅ Confirmed that Redis config file: '{}' exists and is readable!".format(
                    redis_conf_path
                )
            )

    return status, error_message
