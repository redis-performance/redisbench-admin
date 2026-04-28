#  BSD 3-Clause License
#
#  Copyright (c) 2026., Redis Performance Group
#  All rights reserved.
#
"""argparse wiring for the ``compare-perrun`` CLI."""
import os


def create_compare_perrun_arguments(parser):
    parser.add_argument(
        "--baseline-branch",
        type=str,
        default=None,
        help="Baseline git branch (mutually exclusive with --baseline-tag).",
    )
    parser.add_argument(
        "--baseline-tag",
        type=str,
        default=None,
        help="Baseline release tag (mutually exclusive with --baseline-branch).",
    )
    parser.add_argument(
        "--comparison-branch",
        type=str,
        default=None,
        help="Candidate git branch (mutually exclusive with --comparison-tag).",
    )
    parser.add_argument(
        "--comparison-tag",
        type=str,
        default=None,
        help="Candidate release tag (mutually exclusive with --comparison-branch).",
    )
    parser.add_argument(
        "--test",
        type=str,
        default=None,
        help="Restrict the comparison to a single test (default: all matching tests).",
    )
    parser.add_argument(
        "--metric",
        type=str,
        default="rps,lat_p99_us",
        help="Comma-separated metrics to evaluate (default: rps,lat_p99_us).",
    )
    parser.add_argument(
        "--alpha",
        type=float,
        default=0.05,
        help="Significance level for the paired t-test (default: 0.05).",
    )
    parser.add_argument(
        "--regression-threshold-pct",
        type=float,
        default=3.0,
        help="Median %% change beyond which a (test, metric) is flagged as regressed.",
    )
    parser.add_argument(
        "--pg-dsn",
        type=str,
        default=os.environ.get("PERFORMANCE_PG_DSN", ""),
        help="Postgres DSN (defaults to PERFORMANCE_PG_DSN).",
    )
    parser.add_argument(
        "--output-format",
        type=str,
        choices=["text", "markdown", "json"],
        default="text",
        help="Report output format (default: text).",
    )
    parser.add_argument(
        "--output-file",
        type=str,
        default=None,
        help="Write the report to this path instead of stdout.",
    )
    return parser
