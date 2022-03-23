#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#

# environment variables
import datetime

from redisbench_admin.run.args import TRIGGERING_ENV
from redisbench_admin.run.common import get_start_time_vars
from redisbench_admin.utils.remote import (
    PERFORMANCE_RTS_HOST,
    PERFORMANCE_RTS_PORT,
    PERFORMANCE_RTS_AUTH,
    extract_git_vars,
    PERFORMANCE_RTS_USER,
)

(
    GITHUB_ORG,
    GITHUB_REPO,
    _,
    _,
    _,
    _,
) = extract_git_vars()

_, NOW_UTC, _ = get_start_time_vars()
LAST_WEEK_UTC = NOW_UTC - (7 * 24 * 60 * 60 * 1000)
START_TIME_NOW_UTC, _, _ = get_start_time_vars()
START_TIME_LAST_WEEK_UTC = START_TIME_NOW_UTC - datetime.timedelta(days=7)


def create_compare_arguments(parser):
    parser.add_argument(
        "--test",
        type=str,
        default="",
        help="specify a test (or a comma separated list of tests) to use for comparison. If none is specified by default will use all of them.",
    )
    parser.add_argument("--github_repo", type=str, default=GITHUB_REPO)
    parser.add_argument("--github_org", type=str, default=GITHUB_ORG)
    parser.add_argument("--triggering_env", type=str, default=TRIGGERING_ENV)
    parser.add_argument("--deployment_name", type=str, default="oss-standalone")
    parser.add_argument("--deployment_type", type=str, default="oss-standalone")
    parser.add_argument("--baseline_deployment_name", type=str, default="")
    parser.add_argument("--comparison_deployment_name", type=str, default="")
    parser.add_argument("--metric_name", type=str, default="Tests.Overall.rps")
    parser.add_argument(
        "--from-date",
        type=lambda s: datetime.datetime.strptime(s, "%Y-%m-%d"),
        default=START_TIME_LAST_WEEK_UTC,
        help="Only consider regressions with a percentage over the defined limit. (0-100)",
    )
    parser.add_argument(
        "--to-date",
        type=lambda s: datetime.datetime.strptime(s, "%Y-%m-%d"),
        default=START_TIME_NOW_UTC,
    )
    parser.add_argument(
        "--metric_mode",
        type=str,
        default="higher-better",
        help="either 'lower-better' or 'higher-better'",
    )
    parser.add_argument("--baseline-branch", type=str, default=None, required=False)
    parser.add_argument("--baseline-tag", type=str, default=None, required=False)
    parser.add_argument("--comparison-branch", type=str, default=None, required=False)
    parser.add_argument("--comparison-tag", type=str, default=None, required=False)
    parser.add_argument("--print-regressions-only", type=bool, default=False)
    parser.add_argument("--verbose", type=bool, default=False)
    parser.add_argument("--use_metric_context_path", type=bool, default=False)
    parser.add_argument("--testname_regex", type=str, default=".*", required=False)
    parser.add_argument(
        "--regressions-percent-lower-limit",
        type=float,
        default=5.0,
        help="Only consider regressions with a percentage over the defined limit. (0-100)",
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
        "--redistimeseries_user", type=str, default=PERFORMANCE_RTS_USER
    )
    parser.add_argument(
        "--from_timestamp",
        default=None,
        help="The minimum period to use for the the value fetching",
    )
    parser.add_argument("--to_timestamp", default=None)
    return parser
