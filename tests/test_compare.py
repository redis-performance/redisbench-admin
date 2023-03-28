import argparse
import os

import redis

from redisbench_admin.compare.args import create_compare_arguments
from redisbench_admin.compare.compare import compare_command_logic
from redisbench_admin.export.args import create_export_arguments
from redisbench_admin.export.export import export_command_logic


def test_compare_command_logic():
    rts_host = os.getenv("RTS_DATASINK_HOST", None)
    rts_port = 16379
    rts_pass = ""
    if rts_host is None:
        return
    rts = redis.Redis(port=16379, host=rts_host)
    rts.ping()
    rts.flushall()
    parser = argparse.ArgumentParser(
        description="test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = create_export_arguments(parser)

    # mimic the baseline data
    args = parser.parse_args(
        args=[
            "--results-format",
            "google.benchmark",
            "--benchmark-result-file",
            "./tests/test_data/results/google.benchmark.json",
            "--redistimeseries_host",
            rts_host,
            "--redistimeseries_port",
            "{}".format(rts_port),
            "--redistimeseries_pass",
            "{}".format(rts_pass),
            "--github_branch",
            "master",
            "--github_org",
            "redis-org",
            "--github_repo",
            "redis-repo",
            "--triggering_env",
            "circleci",
        ]
    )
    try:
        export_command_logic(args, "tool", "v0")
    except SystemExit as e:
        assert e.code == 0

    # mimic the comparison data
    parser = argparse.ArgumentParser(
        description="test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = create_export_arguments(parser)
    args = parser.parse_args(
        args=[
            "--results-format",
            "google.benchmark",
            "--benchmark-result-file",
            "./tests/test_data/results/google.benchmark.json",
            "--redistimeseries_host",
            rts_host,
            "--redistimeseries_port",
            "{}".format(rts_port),
            "--redistimeseries_pass",
            "{}".format(rts_pass),
            "--github_branch",
            "comparison",
            "--github_org",
            "redis-org",
            "--github_repo",
            "redis-repo",
            "--triggering_env",
            "circleci",
        ]
    )
    try:
        export_command_logic(args, "tool", "v0")
    except SystemExit as e:
        assert e.code == 0

    # mimic the comparison data
    parser = argparse.ArgumentParser(
        description="test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = create_compare_arguments(parser)
    args = parser.parse_args(
        args=[
            "--redistimeseries_host",
            rts_host,
            "--redistimeseries_port",
            "{}".format(rts_port),
            "--redistimeseries_pass",
            "{}".format(rts_pass),
            "--baseline-branch",
            "master",
            "--comparison-branch",
            "comparison",
            "--github_org",
            "redis-org",
            "--github_repo",
            "redis-repo",
            "--metric_name",
            "cpu_time",
            "--to-date",
            "2100-01-01",
        ]
    )
    try:
        (
            detected_regressions,
            comment_body,
            total_improvements,
            total_regressions,
            total_stable,
            total_unstable,
            total_comparison_points,
        ) = compare_command_logic(args, "tool", "v0")
        total_tests = rts.scard(
            "ci.benchmarks.redislabs/circleci/redis-org/redis-repo:testcases"
        )
        assert total_tests > 0
        assert total_comparison_points == total_tests
        assert total_regressions == 0
        assert total_unstable == 0
        assert total_stable == total_tests
        assert total_improvements == 0
        assert detected_regressions == []
        # ensure that we have testcases date
        assert "(1 datapoints)" in comment_body
        assert (
            "Detected a total of {} stable tests between versions".format(total_tests)
            in comment_body
        )
        assert "Automated performance analysis summary" in comment_body
    except SystemExit as e:
        assert e.code == 0
