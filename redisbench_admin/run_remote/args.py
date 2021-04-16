import os
import socket

# environment variables
PERFORMANCE_RTS_AUTH = os.getenv("PERFORMANCE_RTS_AUTH", None)
PERFORMANCE_RTS_HOST = os.getenv("PERFORMANCE_RTS_HOST", 6379)
PERFORMANCE_RTS_PORT = os.getenv("PERFORMANCE_RTS_PORT", None)
TERRAFORM_BIN_PATH = os.getenv("TERRAFORM_BIN_PATH", "terraform")


def create_run_remote_arguments(parser):
    parser.add_argument("--module_path", type=str, required=True)
    parser.add_argument(
        "--allowed-tools",
        type=str,
        default="redis-benchmark,redisgraph-benchmark-go",
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
    parser.add_argument("--github_branch", type=str, default=None, nargs="?", const="")
    parser.add_argument("--triggering_env", type=str, default=socket.gethostname())
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
    parser.add_argument("--redistimesies_host", type=str, default=PERFORMANCE_RTS_HOST)
    parser.add_argument("--redistimesies_port", type=int, default=PERFORMANCE_RTS_PORT)
    parser.add_argument("--redistimesies_pass", type=str, default=PERFORMANCE_RTS_AUTH)
    parser.add_argument(
        "--push_results_redistimeseries",
        default=False,
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
