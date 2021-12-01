#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import os
import socket

from redisbench_admin.profilers.profilers import (
    PROFILERS_DEFAULT,
    PROFILE_FREQ_DEFAULT,
    ALLOWED_PROFILERS,
)

from redisbench_admin.utils.remote import (
    PERFORMANCE_RTS_HOST,
    PERFORMANCE_RTS_PORT,
    PERFORMANCE_RTS_AUTH,
    PERFORMANCE_RTS_PUSH,
)

DEFAULT_TRIGGERING_ENV = socket.gethostname()
TRIGGERING_ENV = os.getenv("TRIGGERING_ENV", DEFAULT_TRIGGERING_ENV)
ENV = os.getenv("ENV", "oss-standalone,oss-cluster")
SETUP = os.getenv("SETUP", "")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "ci.benchmarks.redislabs")
PUSH_S3 = bool(os.getenv("PUSH_S3", False))
PROFILERS_DSO = os.getenv("PROFILERS_DSO", None)
PROFILERS_ENABLED = bool(int(os.getenv("PROFILE", 0)))
PROFILERS = os.getenv("PROFILERS", PROFILERS_DEFAULT)
MAX_PROFILERS_PER_TYPE = int(os.getenv("MAX_PROFILERS", 1))
PROFILE_FREQ = os.getenv("PROFILE_FREQ", PROFILE_FREQ_DEFAULT)
KEEP_ENV = bool(os.getenv("KEEP_ENV", False))


def common_run_args(parser):
    parser.add_argument(
        "--keep_env_and_topo",
        required=False,
        default=KEEP_ENV,
        action="store_true",
        help="Keep environment and topology up after benchmark.",
    )
    parser.add_argument(
        "--dbdir_folder",
        type=str,
        required=False,
        help="If specified the entire contents of the folder are copied to the redis dir.",
    )
    parser.add_argument(
        "--allowed-tools",
        type=str,
        default="redis-benchmark,redisgraph-benchmark-go,ycsb,"
        + "tsbs_run_queries_redistimeseries,tsbs_load_redistimeseries,"
        + "ftsb_redisearch,"
        + "aibench_run_inference_redisai_vision",
        help="comma separated list of allowed tools for this module. By default all the supported are allowed.",
    )
    parser.add_argument(
        "--test",
        type=str,
        default="",
        help="specify a test to run. By default will run all of them.",
    )
    parser.add_argument(
        "--defaults_filename",
        type=str,
        default="defaults.yml",
        help="specify the defaults file containing spec topologies, common metric extractions,etc...",
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
    parser.add_argument(
        "--module_path",
        required=False,
        default=None,
        action="append",
        help="path to the module file. " "You can use `--module_path` more than once. ",
    )
    parser.add_argument("--profilers", type=str, default=PROFILERS)
    parser.add_argument(
        "--enable-profilers",
        default=PROFILERS_ENABLED,
        action="store_true",
        help="Enable Identifying On-CPU and Off-CPU Time using perf/ebpf/vtune tooling. "
        + "By default the chosen profilers are {}".format(PROFILERS_DEFAULT)
        + "Full list of profilers: {}".format(ALLOWED_PROFILERS)
        + "Only available on x86 Linux platform and kernel version >= 4.9",
    )
    parser.add_argument("--dso", type=str, required=False, default=PROFILERS_DSO)
    parser.add_argument(
        "--s3_bucket_name",
        type=str,
        default=S3_BUCKET_NAME,
        help="S3 bucket name.",
    )
    parser.add_argument(
        "--upload_results_s3",
        default=PUSH_S3,
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
        "--allowed-envs",
        type=str,
        default=ENV,
        help="Comma delimited allowed topologies: 'oss-standalone','oss-cluster'",
    )
    parser.add_argument(
        "--allowed-setups",
        type=str,
        default=SETUP,
        help="Comma delimited allowed setups. By default all setups are allowed.",
    )
    parser.add_argument(
        "--grafana-profile-dashboard",
        type=str,
        default="https://benchmarksrediscom.grafana.net/d/uRPZar57k/ci-profiler-viewer",
    )
    return parser
