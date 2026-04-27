import logging

import pytest

from redisbench_admin.compare.compare import (
    get_by_strings,
    resolve_baseline_branch,
)


# ---------------------------------------------------------------------------
# get_by_strings — valid combinations
# ---------------------------------------------------------------------------


def test_get_by_strings_branch_vs_branch():
    baseline_str, by_baseline, comparison_str, by_comparison = get_by_strings(
        baseline_branch="master",
        comparison_branch="feature",
        baseline_tag=None,
        comparison_tag=None,
    )
    assert (baseline_str, by_baseline) == ("master", "branch")
    assert (comparison_str, by_comparison) == ("feature", "branch")


def test_get_by_strings_tag_baseline_branch_comparison():
    """Regression: this combo previously errored as mutually exclusive
    because the baseline-tag check was reading comparison_covered."""
    baseline_str, by_baseline, comparison_str, by_comparison = get_by_strings(
        baseline_branch=None,
        comparison_branch="feature",
        baseline_tag="v1.0",
        comparison_tag=None,
    )
    assert (baseline_str, by_baseline) == ("v1.0", "version")
    assert (comparison_str, by_comparison) == ("feature", "branch")


def test_get_by_strings_branch_baseline_tag_comparison():
    baseline_str, by_baseline, comparison_str, by_comparison = get_by_strings(
        baseline_branch="master",
        comparison_branch=None,
        baseline_tag=None,
        comparison_tag="v2.0",
    )
    assert (baseline_str, by_baseline) == ("master", "branch")
    assert (comparison_str, by_comparison) == ("v2.0", "version")


def test_get_by_strings_tag_vs_tag():
    baseline_str, by_baseline, comparison_str, by_comparison = get_by_strings(
        baseline_branch=None,
        comparison_branch=None,
        baseline_tag="v1.0",
        comparison_tag="v2.0",
    )
    assert (baseline_str, by_baseline) == ("v1.0", "version")
    assert (comparison_str, by_comparison) == ("v2.0", "version")


# ---------------------------------------------------------------------------
# get_by_strings — invalid combinations should still raise SystemExit
# ---------------------------------------------------------------------------


def test_get_by_strings_baseline_branch_and_tag_rejected(caplog):
    with caplog.at_level(logging.ERROR), pytest.raises(SystemExit) as excinfo:
        get_by_strings(
            baseline_branch="master",
            comparison_branch="feature",
            baseline_tag="v1.0",
            comparison_tag=None,
        )
    assert excinfo.value.code == 1
    assert any(
        "baseline-branch and --baseline-tag are mutually exclusive" in r.message
        for r in caplog.records
    )


def test_get_by_strings_comparison_branch_and_tag_rejected(caplog):
    with caplog.at_level(logging.ERROR), pytest.raises(SystemExit) as excinfo:
        get_by_strings(
            baseline_branch="master",
            comparison_branch="feature",
            baseline_tag=None,
            comparison_tag="v2.0",
        )
    assert excinfo.value.code == 1
    assert any(
        "comparison-branch and --comparison-tag are mutually exclusive" in r.message
        for r in caplog.records
    )


def test_get_by_strings_no_baseline_rejected(caplog):
    with caplog.at_level(logging.ERROR), pytest.raises(SystemExit) as excinfo:
        get_by_strings(
            baseline_branch=None,
            comparison_branch="feature",
            baseline_tag=None,
            comparison_tag=None,
        )
    assert excinfo.value.code == 1
    assert any(
        "--baseline-branch or --baseline-tag" in r.message for r in caplog.records
    )


def test_get_by_strings_no_comparison_rejected(caplog):
    with caplog.at_level(logging.ERROR), pytest.raises(SystemExit) as excinfo:
        get_by_strings(
            baseline_branch="master",
            comparison_branch=None,
            baseline_tag=None,
            comparison_tag=None,
        )
    assert excinfo.value.code == 1
    assert any(
        "--comparison-branch or --comparison-tag" in r.message for r in caplog.records
    )


# ---------------------------------------------------------------------------
# resolve_baseline_branch — defaults-file fallback
# ---------------------------------------------------------------------------


def test_resolve_baseline_branch_explicit_branch_wins():
    assert (
        resolve_baseline_branch(
            arg_baseline_branch="explicit",
            arg_baseline_tag=None,
            default_baseline_branch="master",
        )
        == "explicit"
    )


def test_resolve_baseline_branch_falls_back_to_default():
    assert (
        resolve_baseline_branch(
            arg_baseline_branch=None,
            arg_baseline_tag=None,
            default_baseline_branch="master",
        )
        == "master"
    )


def test_resolve_baseline_branch_skips_fallback_when_tag_set():
    """Regression: if --baseline-tag is given, the defaults-file branch must
    not be auto-applied — otherwise get_by_strings raises mutually exclusive."""
    assert (
        resolve_baseline_branch(
            arg_baseline_branch=None,
            arg_baseline_tag="v1.0",
            default_baseline_branch="master",
        )
        is None
    )


def test_resolve_baseline_branch_no_default_no_fallback():
    assert (
        resolve_baseline_branch(
            arg_baseline_branch=None,
            arg_baseline_tag=None,
            default_baseline_branch=None,
        )
        is None
    )
