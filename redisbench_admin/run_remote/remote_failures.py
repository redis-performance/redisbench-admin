#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import logging

from redisbench_admin.utils.remote import fetch_file_from_remote_setup
from redisbench_admin.utils.utils import upload_artifacts_to_s3


def failed_remote_run_artifact_store(
    args,
    client_public_ip,
    dirname,
    full_logfile,
    logname,
    s3_bucket_name,
    s3_bucket_path,
    username,
    private_key,
):
    local_logfile = "{}/{}".format(dirname, logname)
    logging.error(
        "The benchmark returned an error exit status. Fetching remote logfile {} into {}".format(
            full_logfile, local_logfile
        )
    )
    fetch_file_from_remote_setup(
        client_public_ip,
        username,
        private_key,
        local_logfile,
        full_logfile,
    )
    if args.upload_results_s3:
        logging.info(
            "Uploading logfile {} to s3. s3 bucket name: {}. s3 bucket path: {}".format(
                local_logfile, s3_bucket_name, s3_bucket_path
            )
        )
        artifacts = [local_logfile]
        upload_artifacts_to_s3(artifacts, s3_bucket_name, s3_bucket_path)
