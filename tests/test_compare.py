import argparse
import os

import redis

from redisbench_admin.compare.args import create_compare_arguments
from redisbench_admin.compare.compare import compare_command_logic
from redisbench_admin.export.args import create_export_arguments
from redisbench_admin.export.export import export_command_logic


def _get_redis_connection():
    """Get Redis connection for testing."""
    assert "RTS_PORT" in os.environ
    rts_port = os.environ.get("RTS_PORT", None)
    rts_host = os.getenv("RTS_DATASINK_HOST", None)
    rts_pass = ""
    if rts_host is None:
        return None, None, None, None
    rts = redis.Redis(port=rts_port, host=rts_host)
    rts.ping()
    return rts, rts_host, rts_port, rts_pass


def _export_benchmark_data(
    rts_host,
    rts_port,
    rts_pass,
    github_branch,
    github_org,
    github_repo,
    triggering_env="circleci",
):
    """Helper to export benchmark data."""
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
            github_branch,
            "--github_org",
            github_org,
            "--github_repo",
            github_repo,
            "--triggering_env",
            triggering_env,
        ]
    )
    try:
        export_command_logic(args, "tool", "v0")
    except SystemExit as e:
        assert e.code == 0


def _run_comparison(
    rts_host,
    rts_port,
    rts_pass,
    baseline_branch,
    comparison_branch,
    github_org,
    github_repo,
    metric_name="cpu_time",
    triggering_env="circleci",
):
    """Helper to run comparison logic."""
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
            baseline_branch,
            "--comparison-branch",
            comparison_branch,
            "--github_org",
            github_org,
            "--github_repo",
            github_repo,
            "--metric_name",
            metric_name,
            "--to-date",
            "2100-01-01",
            "--triggering_env",
            triggering_env,
        ]
    )
    return compare_command_logic(args, "tool", "v0")


def test_compare_command_logic():
    """Test basic compare command logic."""
    rts, rts_host, rts_port, rts_pass = _get_redis_connection()
    if rts is None:
        return
    rts.flushall()

    # Export baseline and comparison data
    _export_benchmark_data(
        rts_host, rts_port, rts_pass, "master", "redis-org", "redis-repo"
    )
    _export_benchmark_data(
        rts_host, rts_port, rts_pass, "comparison", "redis-org", "redis-repo"
    )

    # Run comparison
    (
        detected_regressions,
        comment_body,
        total_improvements,
        total_regressions,
        total_stable,
        total_unstable,
        total_comparison_points,
        _,
        _,
        _,
    ) = _run_comparison(
        rts_host, rts_port, rts_pass, "master", "comparison", "redis-org", "redis-repo"
    )

    # Verify results
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
    assert "0.0%" in comment_body
    assert (
        "Detected a total of {} stable tests between versions".format(total_tests)
        in comment_body
    )
    assert "Automated performance analysis summary" in comment_body


def test_compare_with_org_repo_filtering():
    """Test that org and repo filtering correctly isolates time series."""
    rts, rts_host, rts_port, rts_pass = _get_redis_connection()
    if rts is None:
        return
    rts.flushall()

    # Export data for two org/repo combinations (baseline only for org2)
    _export_benchmark_data(rts_host, rts_port, rts_pass, "master", "org1", "repo1")
    _export_benchmark_data(rts_host, rts_port, rts_pass, "master", "org2", "repo2")
    _export_benchmark_data(rts_host, rts_port, rts_pass, "comparison", "org1", "repo1")

    # Comparison with org1/repo1 should find results
    (
        _,
        _,
        _,
        _,
        total_stable,
        _,
        total_comparison_points,
        _,
        _,
        _,
    ) = _run_comparison(
        rts_host, rts_port, rts_pass, "master", "comparison", "org1", "repo1"
    )
    total_tests_org1 = rts.scard(
        "ci.benchmarks.redislabs/circleci/org1/repo1:testcases"
    )
    assert total_tests_org1 > 0
    assert total_comparison_points == total_tests_org1
    assert total_stable == total_tests_org1

    # Comparison with org2/repo2 should have no comparison data
    (
        _,
        _,
        _,
        _,
        _,
        _,
        total_comparison_points,
        _,
        _,
        _,
    ) = _run_comparison(
        rts_host, rts_port, rts_pass, "master", "comparison", "org2", "repo2"
    )
    assert total_comparison_points == 0


def test_compare_filters_applied_to_timeseries_queries():
    """Test that github_org and github_repo filters are correctly applied to time series queries."""
    rts, rts_host, rts_port, rts_pass = _get_redis_connection()
    if rts is None:
        return
    rts.flushall()

    # Export baseline and comparison data
    _export_benchmark_data(
        rts_host, rts_port, rts_pass, "baseline", "test-org", "test-repo"
    )
    _export_benchmark_data(
        rts_host, rts_port, rts_pass, "feature", "test-org", "test-repo"
    )

    # Correct org/repo should find results
    (
        _,
        _,
        _,
        _,
        total_stable,
        _,
        total_comparison_points,
        _,
        _,
        _,
    ) = _run_comparison(
        rts_host, rts_port, rts_pass, "baseline", "feature", "test-org", "test-repo"
    )
    assert total_comparison_points > 0
    assert total_stable > 0

    # Wrong org should find no results
    (
        _,
        _,
        _,
        _,
        _,
        _,
        total_comparison_points,
        _,
        _,
        _,
    ) = _run_comparison(
        rts_host, rts_port, rts_pass, "baseline", "feature", "wrong-org", "test-repo"
    )
    assert total_comparison_points == 0
