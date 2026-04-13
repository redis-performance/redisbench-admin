#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import logging

from redisbench_admin.utils.remote import fetch_file_from_remote_setup
from redisbench_admin.utils.utils import upload_artifacts_to_s3


def failed_remote_run_artifact_store(
    upload_results_s3,
    client_public_ip,
    dirname,
    remote_file,
    local_file,
    s3_bucket_name,
    s3_bucket_path,
    username,
    private_key,
):
    local_file_fullpath = "{}/{}".format(dirname, local_file)
    logging.error(
        "The benchmark returned an error exit status. Fetching remote file {} into {}".format(
            remote_file, local_file_fullpath
        )
    )
    fetch_success = False
    try:
        fetch_file_from_remote_setup(
            client_public_ip,
            username,
            private_key,
            local_file_fullpath,
            remote_file,
        )
        fetch_success = True
    except FileNotFoundError as f:
        logging.error("Unable to fetch remote file: {}".format(f.__str__()))
    except Exception as e:
        logging.error("Error fetching remote file: {}".format(e.__str__()))

    # Only upload to S3 if the file was successfully fetched
    if upload_results_s3 and fetch_success and os.path.exists(local_file_fullpath):
        logging.info(
            "Uploading file {} to s3. s3 bucket name: {}. s3 bucket path: {}".format(
                local_file_fullpath, s3_bucket_name, s3_bucket_path
            )
        )
        artifacts = [local_file_fullpath]
        artifacts_map = upload_artifacts_to_s3(
            artifacts, s3_bucket_name, s3_bucket_path
        )
        for artifact_name, url in artifacts_map.items():
            logging.info("Artifact: {}. URL: {}".format(artifact_name, url))
    elif upload_results_s3 and not fetch_success:
        logging.warning(
            "Skipping S3 upload for {} because the file could not be fetched from remote".format(
                local_file
            )
        )
