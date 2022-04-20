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
            logging.info(
                "Checking if module artifact {} exists...".format(local_module_file)
            )
            if os.path.exists(local_module_file) is False:
                error_message = "Specified module artifact does not exist: {}".format(
                    local_module_file
                )
                logging.error(error_message)
            else:
                logging.info(
                    "Confirmed that module artifact: '{}' exists!".format(
                        local_module_file
                    )
                )
    return status, error_message
