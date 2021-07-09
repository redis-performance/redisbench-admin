#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#


def get_test_s3_bucket_path(
    s3_bucket_name, test_name, tf_github_org, tf_github_repo, folder="results"
):
    s3_bucket_path = "{github_org}/{github_repo}/{folder}/{test_name}/".format(
        github_org=tf_github_org,
        github_repo=tf_github_repo,
        test_name=test_name,
        folder=folder,
    )
    return s3_bucket_path
