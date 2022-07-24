#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import datetime

from redisbench_admin.utils.remote import (
    PERFORMANCE_RTS_HOST,
    PERFORMANCE_RTS_PORT,
    PERFORMANCE_RTS_AUTH,
)


def create_export_arguments(parser):
    parser.add_argument(
        "--benchmark-result-file",
        type=str,
        required=True,
        help="benchmark results file to read results from.",
    )
    parser.add_argument(
        "--exporter-spec-file",
        type=str,
        help="Exporter definition file, containing info of the metrics to extract",
    )
    parser.add_argument(
        "--deployment-name",
        type=str,
        default="oss-standalone",
        help="Deployment name",
    )
    parser.add_argument(
        "--deployment-type",
        type=str,
        default="oss-standalone",
        help="Deployment Type",
    )
    parser.add_argument(
        "--deployment-version",
        type=str,
        default=None,
        help="semver of the deployed setup. If None then only per branch/ref time-series are created",
    )
    parser.add_argument(
        "--test-name",
        type=str,
        default=None,
        help="Test name",
    )
    parser.add_argument("--github_actor", type=str, default=None, nargs="?", const="")
    parser.add_argument("--github_repo", type=str, default=None)
    parser.add_argument("--github_org", type=str, default=None)
    parser.add_argument("--github_branch", type=str, default=None, nargs="?", const="")
    parser.add_argument("--triggering_env", type=str, default="ci")
    parser.add_argument(
        "--exporter",
        type=str,
        default="redistimeseries",
        help="exporter to be used ( either csv or redistimeseries )",
    )
    parser.add_argument(
        "--results-format",
        type=str,
        default="json",
        help="results format of the the benchmark results files to read "
        "results from ( either google.benchmark, pyperf-json, csv, json, redis-benchmark-txt )",
    )
    parser.add_argument(
        "--use-result",
        type=str,
        default="median-result",
        help="for each key-metric, use either worst-result, best-result, or median-result",
    )
    parser.add_argument(
        "--extra-tags",
        type=str,
        default="",
        help="comma separated extra tags in the format of key1=value,key2=value,...",
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
    parser.add_argument("--redistimeseries_user", type=str, default=None)
    parser.add_argument(
        "--override-test-time",
        type=lambda s: datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S"),
        help='Override the test time passing datetime in format "%%Y-%%m-%%d %%H:%%M:%%S". Example valid datetime: "2021-01-01 10:00:00". Times are in UTC TZ. If this argument is set, the parsed test time is overridden.',
    )
    return parser
