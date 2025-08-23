#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#

# environment variables
import datetime
import os
from redisbench_admin.run.common import get_start_time_vars, PERFORMANCE_GH_TOKEN
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
LAST_MONTH_UTC = NOW_UTC - (31 * 24 * 60 * 60 * 1000)
START_TIME_NOW_UTC, _, _ = get_start_time_vars()
START_TIME_LAST_MONTH_UTC = START_TIME_NOW_UTC - datetime.timedelta(days=30)
ARCH_X86 = "x86_64"
ARCH_ARM = "aarch64"
VALID_ARCHS = [ARCH_X86, ARCH_ARM]
ARCH = os.getenv("ARCH", ARCH_X86)


def create_compare_arguments(parser):
    parser.add_argument(
        "--test",
        type=str,
        default="",
        help="specify a test (or a comma separated list of tests) to use for comparison. If none is specified by default will use all of them.",
    )
    parser.add_argument(
        "--defaults_filename",
        type=str,
        default="defaults.yml",
        help="specify the defaults file containing spec topologies, common metric extractions,etc...",
    )
    parser.add_argument("--github_repo", type=str, default=GITHUB_REPO)
    parser.add_argument("--github_org", type=str, default=GITHUB_ORG)
    parser.add_argument("--triggering_env", type=str, default="circleci")
    parser.add_argument("--github_token", type=str, default=PERFORMANCE_GH_TOKEN)
    parser.add_argument("--pull-request", type=str, default=None, nargs="?", const="")
    parser.add_argument("--deployment_name", type=str, default="oss-standalone")
    parser.add_argument("--deployment_type", type=str, default="oss-standalone")
    parser.add_argument("--baseline_deployment_name", type=str, default="")
    parser.add_argument("--comparison_deployment_name", type=str, default="")
    parser.add_argument("--metric_name", type=str, default=None)
    parser.add_argument("--running_platform", type=str, default=None)
    parser.add_argument("--extra-filter", type=str, default=None)
    parser.add_argument(
        "--baseline_architecture",
        type=str,
        required=False,
        default=ARCH,
        help=f"Architecture to filter baseline time-series. One of {VALID_ARCHS}.",
    )
    parser.add_argument(
        "--comparison_architecture",
        type=str,
        required=False,
        default=ARCH,
        help=f"Architecture to filter comparison time-series. One of {VALID_ARCHS}.",
    )
    parser.add_argument(
        "--last_n",
        type=int,
        default=-1,
        help="Use the last N samples for each time-serie. by default will use all available values",
    )
    parser.add_argument(
        "--last_n_baseline",
        type=int,
        default=7,
        help="Use the last N samples for each time-serie. by default will use last 7 available values",
    )
    parser.add_argument(
        "--last_n_comparison",
        type=int,
        default=1,
        help="Use the last N samples for each time-serie. by default will use last value only",
    )
    parser.add_argument(
        "--first_n_baseline",
        type=int,
        default=-1,
        help="Use the last N samples for each time-serie. by default will use last 7 available values",
    )
    parser.add_argument(
        "--first_n_comparison",
        type=int,
        default=-1,
        help="Use the last N samples for each time-serie. by default will use last value only",
    )
    parser.add_argument(
        "--from-date",
        type=lambda s: datetime.datetime.strptime(s, "%Y-%m-%d"),
        default=START_TIME_LAST_MONTH_UTC,
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
    parser.add_argument("--print-improvements-only", type=bool, default=False)
    parser.add_argument("--skip-unstable", type=bool, default=False)
    parser.add_argument("--verbose", type=bool, default=False)
    parser.add_argument("--simple-table", type=bool, default=False)
    parser.add_argument("--use_metric_context_path", type=bool, default=False)
    parser.add_argument("--testname_regex", type=str, default=".*", required=False)
    parser.add_argument("--test-regex", type=str, default=".*", required=False)
    parser.add_argument(
        "--regressions-percent-lower-limit",
        type=float,
        default=8.0,
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

    parser.add_argument(
        "--grafana_base_dashboard",
        type=str,
        default="https://benchmarksrediscom.grafana.net/d/",
    )
    parser.add_argument(
        "--grafana_uid",
        default=None,
    )
    parser.add_argument(
        "--auto-approve",
        required=False,
        default=False,
        action="store_true",
        help="Skip interactive approval of changes to github before applying.",
    )
    return parser
