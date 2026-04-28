#  BSD 3-Clause License
#
#  Copyright (c) 2026., Redis Performance Group
#  All rights reserved.
#
"""Lightweight statistical helpers for paired multi-run comparison.

We deliberately stay out of scipy: the comparison runs in CI hooks and we
want the dependency surface minimal. Welch's / paired t-tests come from the
``statistics`` module plus a pure-Python regularized incomplete beta (the
same continued-fraction recurrence Numerical Recipes uses) for the Student's-t
survival function.
"""
import math
import statistics
from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence


# ---------------------------------------------------------------------------
# Student's-t survival function (two-sided p-value)
# ---------------------------------------------------------------------------


def _t_sf(t: float, df: float) -> float:
    """Two-sided p-value for a t-statistic with ``df`` degrees of freedom.

    Uses the identity ``P(|T| > |t|) = I_x(df/2, 1/2)`` where
    ``x = df / (df + t^2)`` and ``I_x`` is the regularized incomplete beta
    function. This is the textbook relationship between Student's-t and the
    beta distribution and gives near-double-precision accuracy across the
    regime we care about (df ≥ 2, |t| ≤ ~50).
    """
    if df <= 0:
        return float("nan")
    if math.isinf(t):
        return 0.0
    x = df / (df + t * t)
    return _betai(df / 2.0, 0.5, x)


def _betai(a: float, b: float, x: float) -> float:
    """Regularized incomplete beta function ``I_x(a, b)``."""
    if x < 0.0 or x > 1.0 or a <= 0.0 or b <= 0.0:
        return float("nan")
    if x == 0.0:
        return 0.0
    if x == 1.0:
        return 1.0
    log_pre = (
        math.lgamma(a + b)
        - math.lgamma(a)
        - math.lgamma(b)
        + a * math.log(x)
        + b * math.log1p(-x)
    )
    pre = math.exp(log_pre)
    if x < (a + 1.0) / (a + b + 2.0):
        return pre * _beta_cf(a, b, x) / a
    return 1.0 - pre * _beta_cf(b, a, 1.0 - x) / b


def _beta_cf(
    a: float, b: float, x: float, maxit: int = 200, eps: float = 1e-12
) -> float:
    """Lentz-style continued fraction for the incomplete beta function.

    Direct port of Numerical Recipes §6.4 ``betacf``.
    """
    fpmin = 1e-300
    qab = a + b
    qap = a + 1.0
    qam = a - 1.0
    c = 1.0
    d = 1.0 - qab * x / qap
    if abs(d) < fpmin:
        d = fpmin
    d = 1.0 / d
    h = d
    for m in range(1, maxit + 1):
        m2 = 2 * m
        aa = m * (b - m) * x / ((qam + m2) * (a + m2))
        d = 1.0 + aa * d
        if abs(d) < fpmin:
            d = fpmin
        c = 1.0 + aa / c
        if abs(c) < fpmin:
            c = fpmin
        d = 1.0 / d
        h *= d * c
        aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
        d = 1.0 + aa * d
        if abs(d) < fpmin:
            d = fpmin
        c = 1.0 + aa / c
        if abs(c) < fpmin:
            c = fpmin
        d = 1.0 / d
        delta = d * c
        h *= delta
        if abs(delta - 1.0) < eps:
            return h
    return h


# ---------------------------------------------------------------------------
# Paired t-test (used when baseline / candidate share an iteration count)
# ---------------------------------------------------------------------------


@dataclass
class PairedTResult:
    n: int
    mean_baseline: float
    mean_candidate: float
    mean_delta: float
    median_pct_change: float
    t_stat: float
    p_value: float
    significant: bool


def paired_t_test(
    baseline: Sequence[float],
    candidate: Sequence[float],
    alpha: float = 0.05,
) -> Optional[PairedTResult]:
    """Two-sided paired t-test on per-iteration scalars.

    Pairs are positional — element ``i`` of each sequence is the same
    iteration. Returns ``None`` if there are fewer than two paired
    observations or the differences are degenerate (zero variance).
    """
    if len(baseline) != len(candidate):
        raise ValueError("baseline and candidate must be the same length")
    n = len(baseline)
    if n < 2:
        return None
    diffs = [c - b for b, c in zip(baseline, candidate)]
    mean_diff = statistics.fmean(diffs)
    var_diff = statistics.variance(diffs)
    if var_diff == 0:
        return PairedTResult(
            n=n,
            mean_baseline=statistics.fmean(baseline),
            mean_candidate=statistics.fmean(candidate),
            mean_delta=mean_diff,
            median_pct_change=_median_pct_change(baseline, candidate),
            t_stat=float("inf") if mean_diff != 0 else 0.0,
            p_value=0.0 if mean_diff != 0 else 1.0,
            significant=mean_diff != 0,
        )
    se = math.sqrt(var_diff / n)
    t = mean_diff / se
    p = _t_sf(t, df=n - 1)
    return PairedTResult(
        n=n,
        mean_baseline=statistics.fmean(baseline),
        mean_candidate=statistics.fmean(candidate),
        mean_delta=mean_diff,
        median_pct_change=_median_pct_change(baseline, candidate),
        t_stat=t,
        p_value=p,
        significant=p < alpha,
    )


def _median_pct_change(baseline: Iterable[float], candidate: Iterable[float]) -> float:
    """Median per-iteration percent change, robust to outliers.

    Returns ``+10.0`` if candidate is on average 10% higher. ``inf`` if any
    paired baseline is zero (sentinel — caller should handle).
    """
    pcts: List[float] = []
    for b, c in zip(baseline, candidate):
        if b == 0:
            return float("inf")
        pcts.append((c - b) / b * 100.0)
    if not pcts:
        return float("nan")
    return statistics.median(pcts)


# ---------------------------------------------------------------------------
# Stability / coefficient-of-variation summary
# ---------------------------------------------------------------------------


@dataclass
class StabilitySummary:
    n: int
    mean: float
    stdev: float
    cv_pct: float  # coefficient of variation, %


def stability(values: Sequence[float]) -> Optional[StabilitySummary]:
    """Stability of a single sequence — mean, stdev, CV%.

    Used to score how noisy a baseline (or candidate) is on its own,
    independent of any comparison.
    """
    if len(values) < 2:
        return None
    mean = statistics.fmean(values)
    stdev = statistics.stdev(values)
    cv = (stdev / mean * 100.0) if mean != 0 else float("inf")
    return StabilitySummary(n=len(values), mean=mean, stdev=stdev, cv_pct=cv)
