#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import os
import socket

# environment variables
from redisbench_admin.utils.remote import (
    TERRAFORM_BIN_PATH,
    PERFORMANCE_RTS_HOST,
    PERFORMANCE_RTS_PORT,
    PERFORMANCE_RTS_AUTH,
    PERFORMANCE_RTS_PUSH,
)


DEFAULT_TRIGGERING_ENV = socket.gethostname()
TRIGGERING_ENV = os.getenv("TRIGGERING_ENV", DEFAULT_TRIGGERING_ENV)


def create_run_remote_arguments(parser):
    parser.add_argument("--module_path", type=str, required=True)
    parser.add_argument(
        "--allowed-tools",
        type=str,
        default="redis-benchmark,redisgraph-benchmark-go,ycsb,tsbs_run_queries_redistimeseries,tsbs_load_redistimeseries",
        help="comma separated list of allowed tools for this module. By default all the supported are allowed.",
    )
    parser.add_argument(
        "--test",
        type=str,
        default="",
        help="specify a test to run. By default will run all of them.",
    )
    parser.add_argument("--github_actor", type=str, default=None, nargs="?", const="")
    parser.add_argument("--github_repo", type=str, default=None)
    parser.add_argument("--github_org", type=str, default=None)
    parser.add_argument("--github_sha", type=str, default=None, nargs="?", const="")
    parser.add_argument(
        "--required-module",
        default=None,
        action="append",
        help="path to the module file. "
        "You can use `--required-module` more than once",
    )
    parser.add_argument("--github_branch", type=str, default=None, nargs="?", const="")
    parser.add_argument("--triggering_env", type=str, default=TRIGGERING_ENV)
    parser.add_argument("--terraform_bin_path", type=str, default=TERRAFORM_BIN_PATH)
    parser.add_argument("--setup_name_sufix", type=str, default="")
    parser.add_argument(
        "--s3_bucket_name",
        type=str,
        default="ci.benchmarks.redislabs",
        help="S3 bucket name.",
    )
    parser.add_argument(
        "--upload_results_s3",
        default=False,
        action="store_true",
        help="uploads the result files and configuration file to public "
        "'ci.benchmarks.redislabs' bucket. Proper credentials are required",
    )
    parser.add_argument(
        "--redistimeseries_host", type=str, default=PERFORMANCE_RTS_HOST
    )
    parser.add_argument(
        "--redistimeseries_port", type=int, default=PERFORMANCE_RTS_PORT
    )
    parser.add_argument(
        "--redistimeseries_pass", type=str, default=PERFORMANCE_RTS_AUTH
    )
    parser.add_argument(
        "--push_results_redistimeseries",
        default=PERFORMANCE_RTS_PUSH,
        action="store_true",
        help="uploads the results to RedisTimeSeries. Proper credentials are required",
    )
    parser.add_argument(
        "--skip-env-vars-verify",
        default=False,
        action="store_true",
        help="skip environment variables check",
    )
    return parser
