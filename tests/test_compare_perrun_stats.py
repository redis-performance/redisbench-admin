#  BSD 3-Clause License
#
#  Copyright (c) 2026., Redis Performance Group
#  All rights reserved.
#
"""Tests for the per-run statistical helpers.

We compare against scipy when available (an extra confidence test that the
hand-rolled t-test survival function matches the gold standard); when scipy
isn't installed the hand-rolled values are still asserted against absolute
references so the test always exercises the maths."""
import math

import pytest

from redisbench_admin.compare_perrun.stats import (
    PairedTResult,
    paired_t_test,
    stability,
    _t_sf,
)


def test_t_sf_known_values():
    # Classic textbook reference: P(|T_10| > 2.228) ≈ 0.05.
    p = _t_sf(2.228, df=10)
    assert p == pytest.approx(0.05, abs=2e-3)
    # P(|T_30| > 2.042) ≈ 0.05
    p2 = _t_sf(2.042, df=30)
    assert p2 == pytest.approx(0.05, abs=2e-3)
    # P(|T_5| > 2.571) ≈ 0.05 (df=5)
    p3 = _t_sf(2.571, df=5)
    assert p3 == pytest.approx(0.05, abs=2e-3)


def test_t_sf_matches_scipy():
    sp_stats = pytest.importorskip("scipy.stats")

    for t in (0.5, 1.0, 2.0, 3.5):
        for df in (3, 5, 10, 30, 100):
            ours = _t_sf(t, df)
            theirs = 2.0 * (1.0 - sp_stats.t.cdf(t, df))
            # Hill 1970 is an asymptotic approximation; tolerance ~1e-3 holds
            # comfortably in our regime (df ≥ 3, |t| ≤ 5).
            assert ours == pytest.approx(theirs, abs=2e-3, rel=2e-2)


def test_paired_t_test_rejects_with_clear_difference():
    baseline = [100.0, 102.0, 99.0, 101.0, 98.0]
    candidate = [108.0, 109.0, 107.0, 110.0, 106.0]
    res = paired_t_test(baseline, candidate, alpha=0.05)
    assert isinstance(res, PairedTResult)
    assert res.n == 5
    assert res.p_value < 0.05
    assert res.significant is True
    assert res.median_pct_change > 5.0


def test_paired_t_test_does_not_reject_noisy_no_change():
    baseline = [100.0, 110.0, 90.0, 95.0, 105.0]
    candidate = [98.0, 112.0, 88.0, 96.0, 103.0]
    res = paired_t_test(baseline, candidate, alpha=0.05)
    assert res.significant is False


def test_paired_t_test_returns_none_with_one_pair():
    assert paired_t_test([1.0], [2.0]) is None


def test_paired_t_test_zero_variance_handled():
    """All diffs identical and non-zero → t = inf, p = 0."""
    base = [10.0, 10.0, 10.0]
    cand = [11.0, 11.0, 11.0]
    res = paired_t_test(base, cand)
    assert res.significant is True
    assert math.isinf(res.t_stat)
    assert res.p_value == 0.0


def test_paired_t_test_zero_variance_no_change():
    base = [10.0, 10.0]
    cand = [10.0, 10.0]
    res = paired_t_test(base, cand)
    assert res.significant is False
    assert res.t_stat == 0.0


def test_paired_t_test_length_mismatch_raises():
    with pytest.raises(ValueError):
        paired_t_test([1.0, 2.0], [3.0])


def test_stability_basic():
    s = stability([100.0, 101.0, 99.0, 100.5, 99.5])
    assert s.n == 5
    assert s.mean == pytest.approx(100.0, abs=1e-9)
    assert s.cv_pct < 1.0


def test_stability_returns_none_for_singleton():
    assert stability([42.0]) is None


def test_stability_zero_mean_yields_inf_cv():
    s = stability([0.0, 0.0, 0.001, -0.001])
    assert math.isinf(s.cv_pct) or s.cv_pct > 1e3
