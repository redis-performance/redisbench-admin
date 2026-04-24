import argparse
import os
from typing import NamedTuple, Optional

import pytest
import redis

from redisbench_admin.compare.args import create_compare_arguments
from redisbench_admin.compare.compare import compare_command_logic
from redisbench_admin.export.args import create_export_arguments
from redisbench_admin.export.export import export_command_logic


class CompareResult(NamedTuple):
    detected_regressions: list[str]
    comment_body: str
    total_improvements: int
    total_regressions: int
    total_stable: int
    total_unstable: int
    total_comparison_points: int
    total_unstable_baseline: int
    total_unstable_comparison: int
    total_latency_confirmed_regressions: int


def _get_redis_connection() -> Optional[tuple[redis.Redis, str, str, str]]:
    """Get Redis connection for testing."""
    rts_port = os.environ.get("RTS_PORT", None)
    rts_host = os.getenv("RTS_DATASINK_HOST", None)
    rts_pass = ""
    if rts_host is None or rts_port is None:
        return None
    rts = redis.Redis(port=int(rts_port), host=rts_host)
    rts.ping()
    return rts, rts_host, rts_port, rts_pass


def _export_benchmark_data(
    rts_host: str,
    rts_port: str,
    rts_pass: str,
    github_branch: str,
    github_org: str,
    github_repo: str,
    triggering_env: str = "circleci",
    architecture: Optional[str] = None,
) -> None:
    """Helper to export benchmark data.

    When `architecture` is provided, passes `--architecture <value>` to the
    exporter so the pushed time-series carry that arch tag (needed by
    multi-arch compare tests that filter on `arch=aarch64` etc.).
    """
    parser = argparse.ArgumentParser(
        description="test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = create_export_arguments(parser)
    cli_args = [
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
    if architecture is not None:
        cli_args += ["--architecture", architecture]
    args = parser.parse_args(args=cli_args)
    try:
        export_command_logic(args, "tool", "v0")
    except SystemExit as e:
        assert e.code == 0


def _run_comparison(
    rts_host: str,
    rts_port: str,
    rts_pass: str,
    baseline_branch: str,
    comparison_branch: str,
    github_org: str,
    github_repo: str,
    metric_name: str = "cpu_time",
    triggering_env: str = "circleci",
    architectures: Optional[str] = None,
    no_cross_arch: bool = False,
) -> CompareResult:
    """Helper to run comparison logic.

    When `architectures` is provided (e.g. "x86_64,aarch64"), invokes the
    multi-arch rendering path; the returned comment_body will contain one
    H2 section per arch plus an optional cross-arch section.
    """
    parser = argparse.ArgumentParser(
        description="test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = create_compare_arguments(parser)
    cli_args = [
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
    if architectures is not None:
        cli_args += ["--architectures", architectures]
    if no_cross_arch:
        cli_args += ["--no-cross-arch"]
    args = parser.parse_args(args=cli_args)
    return CompareResult(*compare_command_logic(args, "tool", "v0"))


def test_compare_command_logic() -> None:
    """Test basic compare command logic."""
    conn = _get_redis_connection()
    if conn is None:
        pytest.skip("Redis not available (RTS_DATASINK_HOST or RTS_PORT not set)")
    rts, rts_host, rts_port, rts_pass = conn
    rts.flushall()

    # Export baseline and comparison data
    _export_benchmark_data(
        rts_host, rts_port, rts_pass, "master", "redis-org", "redis-repo"
    )
    _export_benchmark_data(
        rts_host, rts_port, rts_pass, "comparison", "redis-org", "redis-repo"
    )

    # Run comparison
    result = _run_comparison(
        rts_host, rts_port, rts_pass, "master", "comparison", "redis-org", "redis-repo"
    )

    # Verify results
    total_tests: int = rts.scard(  # type: ignore[assignment]
        "ci.benchmarks.redislabs/circleci/redis-org/redis-repo:testcases"
    )
    assert total_tests > 0
    assert result.total_comparison_points == total_tests
    assert result.total_regressions == 0
    assert result.total_unstable == 0
    assert result.total_stable == total_tests
    assert result.total_improvements == 0
    assert result.detected_regressions == []
    assert "0.0%" in result.comment_body
    assert (
        "Detected a total of {} stable tests between versions".format(total_tests)
        in result.comment_body
    )
    assert "Automated performance analysis summary" in result.comment_body


def test_compare_with_org_repo_filtering() -> None:
    """Test that org and repo filtering correctly isolates time series."""
    conn = _get_redis_connection()
    if conn is None:
        pytest.skip("Redis not available (RTS_DATASINK_HOST or RTS_PORT not set)")
    rts, rts_host, rts_port, rts_pass = conn
    rts.flushall()

    # Export data for two org/repo combinations (baseline only for org2)
    _export_benchmark_data(rts_host, rts_port, rts_pass, "master", "org1", "repo1")
    _export_benchmark_data(rts_host, rts_port, rts_pass, "master", "org2", "repo2")
    _export_benchmark_data(rts_host, rts_port, rts_pass, "comparison", "org1", "repo1")

    # Comparison with org1/repo1 should find results
    result = _run_comparison(
        rts_host, rts_port, rts_pass, "master", "comparison", "org1", "repo1"
    )
    total_tests_org1: int = rts.scard(  # type: ignore[assignment]
        "ci.benchmarks.redislabs/circleci/org1/repo1:testcases"
    )
    assert total_tests_org1 > 0
    assert result.total_comparison_points == total_tests_org1
    assert result.total_stable == total_tests_org1

    # Comparison with org2/repo2 should have no comparison data
    result = _run_comparison(
        rts_host, rts_port, rts_pass, "master", "comparison", "org2", "repo2"
    )
    assert result.total_comparison_points == 0


def test_compare_filters_applied_to_timeseries_queries() -> None:
    """Test that github_org and github_repo filters are correctly applied to time series queries."""
    conn = _get_redis_connection()
    if conn is None:
        pytest.skip("Redis not available (RTS_DATASINK_HOST or RTS_PORT not set)")
    rts, rts_host, rts_port, rts_pass = conn
    rts.flushall()

    # Export baseline and comparison data
    _export_benchmark_data(
        rts_host, rts_port, rts_pass, "baseline", "test-org", "test-repo"
    )
    _export_benchmark_data(
        rts_host, rts_port, rts_pass, "feature", "test-org", "test-repo"
    )

    # Correct org/repo should find results
    result = _run_comparison(
        rts_host, rts_port, rts_pass, "baseline", "feature", "test-org", "test-repo"
    )
    assert result.total_comparison_points > 0
    assert result.total_stable > 0

    # Wrong org should find no results
    result = _run_comparison(
        rts_host, rts_port, rts_pass, "baseline", "feature", "wrong-org", "test-repo"
    )
    assert result.total_comparison_points == 0


# ----------------------------------------------------------------------
# Multi-arch comment tests (--architectures / --no-cross-arch)
# ----------------------------------------------------------------------


def test_compare_single_arch_backwards_compat() -> None:
    """Default invocation (no --architectures) must keep the legacy body
    shape: no per-arch H2 sections, no cross-arch section."""
    conn = _get_redis_connection()
    if conn is None:
        pytest.skip("Redis not available (RTS_DATASINK_HOST or RTS_PORT not set)")
    rts, rts_host, rts_port, rts_pass = conn
    rts.flushall()

    _export_benchmark_data(rts_host, rts_port, rts_pass, "master", "org", "repo")
    _export_benchmark_data(rts_host, rts_port, rts_pass, "feature", "org", "repo")
    result = _run_comparison(
        rts_host, rts_port, rts_pass, "master", "feature", "org", "repo"
    )
    assert "Automated performance analysis summary" in result.comment_body
    assert "## Architecture: `x86_64` — branch-over-branch" not in result.comment_body
    assert "## Cross-arch delta" not in result.comment_body


def test_compare_architectures_two_archs_produces_three_sections() -> None:
    """--architectures x86_64,aarch64 with data on both archs emits one
    branch-over-branch section per arch plus a cross-arch section."""
    conn = _get_redis_connection()
    if conn is None:
        pytest.skip("Redis not available (RTS_DATASINK_HOST or RTS_PORT not set)")
    rts, rts_host, rts_port, rts_pass = conn
    rts.flushall()

    # Push both baseline and comparison data for both archs so each arch's
    # branch-over-branch section has data AND the cross-arch section has
    # data on the comparison branch.
    for branch in ("master", "feature"):
        for arch in ("x86_64", "aarch64"):
            _export_benchmark_data(
                rts_host,
                rts_port,
                rts_pass,
                branch,
                "org",
                "repo",
                architecture=arch,
            )
    result = _run_comparison(
        rts_host,
        rts_port,
        rts_pass,
        "master",
        "feature",
        "org",
        "repo",
        architectures="x86_64,aarch64",
    )
    assert "## Architecture: `x86_64` — branch-over-branch" in result.comment_body
    assert "## Architecture: `aarch64` — branch-over-branch" in result.comment_body
    assert "## Cross-arch delta on `feature`" in result.comment_body
    assert "(`x86_64` → `aarch64`)" in result.comment_body
    # aggregated totals cover both per-arch sections (cross-arch is display-only)
    assert result.total_comparison_points > 0


def test_compare_architectures_missing_arch_shows_warning() -> None:
    """When --architectures lists an arch that has no data, that section
    renders a warning block and the cross-arch section is suppressed."""
    conn = _get_redis_connection()
    if conn is None:
        pytest.skip("Redis not available (RTS_DATASINK_HOST or RTS_PORT not set)")
    rts, rts_host, rts_port, rts_pass = conn
    rts.flushall()

    # Only x86_64 gets data; aarch64 intentionally missing.
    _export_benchmark_data(
        rts_host,
        rts_port,
        rts_pass,
        "master",
        "org",
        "repo",
        architecture="x86_64",
    )
    _export_benchmark_data(
        rts_host,
        rts_port,
        rts_pass,
        "feature",
        "org",
        "repo",
        architecture="x86_64",
    )
    result = _run_comparison(
        rts_host,
        rts_port,
        rts_pass,
        "master",
        "feature",
        "org",
        "repo",
        architectures="x86_64,aarch64",
    )
    assert "## Architecture: `x86_64` — branch-over-branch" in result.comment_body
    assert "## Architecture: `aarch64` — branch-over-branch" in result.comment_body
    assert "⚠️ No `aarch64` benchmark data found" in result.comment_body
    # Without data on both archs the cross-arch delta is not emitted.
    assert "## Cross-arch delta" not in result.comment_body


def test_compare_architectures_no_cross_arch_flag_skips_cross_section() -> None:
    """--no-cross-arch must suppress the cross-arch section even when both
    archs have data."""
    conn = _get_redis_connection()
    if conn is None:
        pytest.skip("Redis not available (RTS_DATASINK_HOST or RTS_PORT not set)")
    rts, rts_host, rts_port, rts_pass = conn
    rts.flushall()

    for branch in ("master", "feature"):
        for arch in ("x86_64", "aarch64"):
            _export_benchmark_data(
                rts_host,
                rts_port,
                rts_pass,
                branch,
                "org",
                "repo",
                architecture=arch,
            )
    result = _run_comparison(
        rts_host,
        rts_port,
        rts_pass,
        "master",
        "feature",
        "org",
        "repo",
        architectures="x86_64,aarch64",
        no_cross_arch=True,
    )
    assert "## Architecture: `x86_64` — branch-over-branch" in result.comment_body
    assert "## Architecture: `aarch64` — branch-over-branch" in result.comment_body
    assert "## Cross-arch delta" not in result.comment_body


def test_compare_architectures_single_entry_no_cross_arch() -> None:
    """--architectures with a single entry (e.g. 'x86_64') produces one
    section and no cross-arch delta -- there's nothing to cross-compare."""
    conn = _get_redis_connection()
    if conn is None:
        pytest.skip("Redis not available (RTS_DATASINK_HOST or RTS_PORT not set)")
    rts, rts_host, rts_port, rts_pass = conn
    rts.flushall()

    _export_benchmark_data(
        rts_host,
        rts_port,
        rts_pass,
        "master",
        "org",
        "repo",
        architecture="x86_64",
    )
    _export_benchmark_data(
        rts_host,
        rts_port,
        rts_pass,
        "feature",
        "org",
        "repo",
        architecture="x86_64",
    )
    result = _run_comparison(
        rts_host,
        rts_port,
        rts_pass,
        "master",
        "feature",
        "org",
        "repo",
        architectures="x86_64",
    )
    assert "## Architecture: `x86_64` — branch-over-branch" in result.comment_body
    assert "## Architecture: `aarch64`" not in result.comment_body
    assert "## Cross-arch delta" not in result.comment_body
