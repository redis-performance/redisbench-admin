#  BSD 3-Clause License
#
#  Copyright (c) 2026., Redis Performance Group
#  All rights reserved.
#
"""``compare-perrun`` — paired statistical comparison driven by PG samples.

For each (test, metric) the tool:

    1. Pulls per-iteration time series from ``bench_run_sample`` for
       baseline and candidate.
    2. Reduces each iteration to a scalar (median of the per-second values
       across the run) — robust against startup transients without throwing
       away the per-second granularity that's still in PG for ad-hoc drill-
       downs.
    3. Pairs iterations positionally and runs Welch's paired t-test.
    4. Classifies each metric as GREEN / WARN / REGRESSION / INCONCLUSIVE.

A "regression" is flagged when *both* the median percent change is worse than
``--regression-threshold-pct`` and the paired test is significant at
``--alpha``. That gates against significant-but-trivial drift and against
huge-but-noisy single iterations.
"""
import json
import logging
import statistics
import sys
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Tuple

from redisbench_admin.compare_perrun.stats import (
    PairedTResult,
    StabilitySummary,
    paired_t_test,
    stability,
)


# ---------------------------------------------------------------------------
# Verdict classification
# ---------------------------------------------------------------------------

GREEN = "GREEN"
WARN = "WARN"
REGRESSION = "REGRESSION"
IMPROVEMENT = "IMPROVEMENT"
INCONCLUSIVE = "INCONCLUSIVE"

# Metrics where "higher is better" — for these, a negative pct_change is bad.
# Default assumption: anything matching ``rps`` / ``ops`` / ``throughput``.
HIGHER_IS_BETTER_HINTS = ("rps", "ops", "throughput", "qps")


def metric_higher_is_better(metric: str) -> bool:
    return any(hint in metric.lower() for hint in HIGHER_IS_BETTER_HINTS)


def classify(
    metric: str,
    pct_change: float,
    p_value: Optional[float],
    threshold_pct: float,
    alpha: float,
) -> str:
    """Bucket a (metric, pct_change, p) into a verdict label."""
    if p_value is None or pct_change != pct_change:  # NaN check
        return INCONCLUSIVE
    higher_better = metric_higher_is_better(metric)
    is_regression = (
        (pct_change < -threshold_pct) if higher_better else (pct_change > threshold_pct)
    )
    is_improvement = (
        (pct_change > threshold_pct) if higher_better else (pct_change < -threshold_pct)
    )
    significant = p_value < alpha
    if is_regression and significant:
        return REGRESSION
    if is_improvement and significant:
        return IMPROVEMENT
    if is_regression or is_improvement:
        # Big change but noise might be drowning it — flag as WARN.
        return WARN
    return GREEN


# ---------------------------------------------------------------------------
# Data shape
# ---------------------------------------------------------------------------


@dataclass
class IterationSeries:
    """All per-second samples for one (run_id, metric) pair."""

    iteration: int
    values: List[float] = field(default_factory=list)


@dataclass
class MetricComparison:
    test: str
    metric: str
    baseline_iterations: List[IterationSeries]
    candidate_iterations: List[IterationSeries]
    paired_test: Optional[PairedTResult]
    baseline_stability: Optional[StabilitySummary]
    candidate_stability: Optional[StabilitySummary]
    verdict: str
    # Indexes (into the per-second arrays) where the candidate diverges hardest
    # — surfaced in the report so an engineer can drill down by ts_offset_s.
    worst_seconds: List[Tuple[int, float, float]] = field(default_factory=list)


@dataclass
class CompareReport:
    baseline_label: str
    candidate_label: str
    metrics: List[MetricComparison]


# ---------------------------------------------------------------------------
# Postgres queries
# ---------------------------------------------------------------------------


_RUNS_QUERY = """
SELECT r.run_id, r.test, r.iteration
FROM bench_run r
WHERE r.{ref_col} = %s
  AND ({test_filter} OR %s = '')
ORDER BY r.test, r.iteration
"""


def _ref_col(branch: Optional[str], tag: Optional[str]) -> Tuple[str, str]:
    if branch is not None and tag is not None:
        raise ValueError("branch and tag are mutually exclusive")
    if branch is not None:
        return "branch", branch
    if tag is not None:
        return "tag", tag
    raise ValueError("must specify branch or tag")


def fetch_runs(conn, branch=None, tag=None, test=None) -> List[Tuple[str, str, int]]:
    col, value = _ref_col(branch, tag)
    test_filter = "r.test = %s" if test else "TRUE"
    query = _RUNS_QUERY.format(ref_col=col, test_filter=test_filter)
    params: List = [value]
    if test:
        params.append(test)
    else:
        params.append("")  # unused placeholder for the OR %s = '' guard
    with conn.cursor() as cur:
        cur.execute(query, params)
        return [(str(rid), t, it) for rid, t, it in cur.fetchall()]


def fetch_samples(
    conn, run_ids: Sequence[str], metrics: Sequence[str]
) -> Dict[Tuple[str, str], List[float]]:
    """Bulk-load samples grouped by ``(run_id, metric)`` → ordered values."""
    if not run_ids or not metrics:
        return {}
    out: Dict[Tuple[str, str], List[float]] = {}
    with conn.cursor() as cur:
        cur.execute(
            "SELECT run_id, metric, ts_offset_s, value "
            "FROM bench_run_sample "
            "WHERE run_id = ANY(%s::uuid[]) AND metric = ANY(%s) "
            "ORDER BY run_id, metric, ts_offset_s",
            (list(run_ids), list(metrics)),
        )
        for run_id, metric, _ts, value in cur.fetchall():
            out.setdefault((str(run_id), metric), []).append(float(value))
    return out


# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------


def _iteration_scalar(values: Sequence[float]) -> Optional[float]:
    """Reduce a per-second series to one number — median of the run."""
    if not values:
        return None
    return statistics.median(values)


def _build_iteration_series(
    runs: Sequence[Tuple[str, str, int]],
    samples: Dict[Tuple[str, str], List[float]],
    metric: str,
    test: str,
) -> List[IterationSeries]:
    out: List[IterationSeries] = []
    for run_id, t, iteration in runs:
        if t != test:
            continue
        values = samples.get((run_id, metric), [])
        out.append(IterationSeries(iteration=iteration, values=list(values)))
    out.sort(key=lambda s: s.iteration)
    return out


def _worst_seconds(
    base_series: Sequence[IterationSeries],
    cand_series: Sequence[IterationSeries],
    top: int = 5,
) -> List[Tuple[int, float, float]]:
    """Find ts_offsets where the per-second median diverges hardest.

    Aligns by ts_offset_s (per-second index), aggregates across iterations
    by taking the median per side, then sorts by abs(delta).
    """
    base_per_sec: Dict[int, List[float]] = {}
    for series in base_series:
        for ts, v in enumerate(series.values):
            base_per_sec.setdefault(ts, []).append(v)
    cand_per_sec: Dict[int, List[float]] = {}
    for series in cand_series:
        for ts, v in enumerate(series.values):
            cand_per_sec.setdefault(ts, []).append(v)
    common = sorted(set(base_per_sec) & set(cand_per_sec))
    rows: List[Tuple[int, float, float]] = []
    for ts in common:
        base_med = statistics.median(base_per_sec[ts])
        cand_med = statistics.median(cand_per_sec[ts])
        rows.append((ts, base_med, cand_med))
    rows.sort(key=lambda r: abs(r[2] - r[1]), reverse=True)
    return rows[:top]


def compare_metric(
    test: str,
    metric: str,
    base_runs: Sequence[Tuple[str, str, int]],
    cand_runs: Sequence[Tuple[str, str, int]],
    samples: Dict[Tuple[str, str], List[float]],
    threshold_pct: float,
    alpha: float,
) -> MetricComparison:
    base_series = _build_iteration_series(base_runs, samples, metric, test)
    cand_series = _build_iteration_series(cand_runs, samples, metric, test)

    base_scalars = [
        s for s in (_iteration_scalar(it.values) for it in base_series) if s is not None
    ]
    cand_scalars = [
        s for s in (_iteration_scalar(it.values) for it in cand_series) if s is not None
    ]

    # Pair on the shorter sequence — if iteration counts differ we still want
    # *some* signal rather than skipping.
    paired_n = min(len(base_scalars), len(cand_scalars))
    paired_test = None
    pct_change = float("nan")
    p_value: Optional[float] = None
    if paired_n >= 2:
        paired_test = paired_t_test(
            base_scalars[:paired_n], cand_scalars[:paired_n], alpha=alpha
        )
        if paired_test is not None:
            pct_change = paired_test.median_pct_change
            p_value = paired_test.p_value

    verdict = classify(metric, pct_change, p_value, threshold_pct, alpha)

    return MetricComparison(
        test=test,
        metric=metric,
        baseline_iterations=base_series,
        candidate_iterations=cand_series,
        paired_test=paired_test,
        baseline_stability=stability(base_scalars),
        candidate_stability=stability(cand_scalars),
        verdict=verdict,
        worst_seconds=_worst_seconds(base_series, cand_series),
    )


def run_comparison(
    conn,
    *,
    baseline_branch: Optional[str],
    baseline_tag: Optional[str],
    comparison_branch: Optional[str],
    comparison_tag: Optional[str],
    test: Optional[str],
    metrics: Sequence[str],
    threshold_pct: float,
    alpha: float,
) -> CompareReport:
    base_runs = fetch_runs(conn, branch=baseline_branch, tag=baseline_tag, test=test)
    cand_runs = fetch_runs(
        conn, branch=comparison_branch, tag=comparison_tag, test=test
    )
    if not base_runs:
        raise RuntimeError(
            f"No baseline runs found in PG for branch={baseline_branch} tag={baseline_tag}"
        )
    if not cand_runs:
        raise RuntimeError(
            f"No candidate runs found in PG for branch={comparison_branch} "
            f"tag={comparison_tag}"
        )
    all_run_ids = [rid for rid, _t, _it in (*base_runs, *cand_runs)]
    samples = fetch_samples(conn, all_run_ids, metrics)

    tests = sorted(
        {t for _rid, t, _it in base_runs} & {t for _rid, t, _it in cand_runs}
    )
    if test:
        tests = [t for t in tests if t == test]

    metric_rows: List[MetricComparison] = []
    for t in tests:
        for metric in metrics:
            metric_rows.append(
                compare_metric(
                    t,
                    metric,
                    base_runs,
                    cand_runs,
                    samples,
                    threshold_pct,
                    alpha,
                )
            )

    return CompareReport(
        baseline_label=baseline_tag or baseline_branch or "?",
        candidate_label=comparison_tag or comparison_branch or "?",
        metrics=metric_rows,
    )


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def _verdict_glyph(verdict: str) -> str:
    return {
        GREEN: "[OK]",
        WARN: "[WARN]",
        REGRESSION: "[REGRESSION]",
        IMPROVEMENT: "[IMPROVE]",
        INCONCLUSIVE: "[INCONCLUSIVE]",
    }.get(verdict, "[?]")


def render_text(report: CompareReport) -> str:
    lines: List[str] = []
    lines.append(
        f"Per-run comparison: baseline={report.baseline_label}  "
        f"candidate={report.candidate_label}"
    )
    lines.append("=" * 78)
    for row in report.metrics:
        lines.append(f"{_verdict_glyph(row.verdict):16s} {row.test} :: {row.metric}")
        if row.paired_test:
            t = row.paired_test
            lines.append(
                f"    n={t.n}  median_pct_change={t.median_pct_change:+.2f}%  "
                f"p={t.p_value:.4f}  baseline_mean={t.mean_baseline:.2f}  "
                f"candidate_mean={t.mean_candidate:.2f}"
            )
        else:
            lines.append("    (insufficient data for paired test)")
        if row.baseline_stability and row.candidate_stability:
            lines.append(
                f"    cv_baseline={row.baseline_stability.cv_pct:.2f}%  "
                f"cv_candidate={row.candidate_stability.cv_pct:.2f}%"
            )
        if row.worst_seconds and row.verdict in (REGRESSION, WARN):
            lines.append("    worst per-second offsets:")
            for ts, b, c in row.worst_seconds:
                lines.append(
                    f"      ts={ts:4d}s  baseline={b:.2f}  candidate={c:.2f}  "
                    f"delta={(c - b):+.2f}"
                )
    return "\n".join(lines) + "\n"


def render_markdown(report: CompareReport) -> str:
    lines: List[str] = []
    lines.append(
        f"# Per-run comparison — `{report.baseline_label}` vs `{report.candidate_label}`"
    )
    lines.append("")
    lines.append(
        "| verdict | test | metric | n | median Δ% | p | baseline mean | candidate mean |"
    )
    lines.append("|---|---|---|---|---|---|---|---|")
    for row in report.metrics:
        if row.paired_test:
            t = row.paired_test
            lines.append(
                f"| {row.verdict} | `{row.test}` | `{row.metric}` | {t.n} | "
                f"{t.median_pct_change:+.2f} | {t.p_value:.4f} | "
                f"{t.mean_baseline:.2f} | {t.mean_candidate:.2f} |"
            )
        else:
            lines.append(
                f"| {row.verdict} | `{row.test}` | `{row.metric}` | - | - | - | - | - |"
            )
    return "\n".join(lines) + "\n"


def render_json(report: CompareReport) -> str:
    def _series(ser):
        return [{"iteration": s.iteration, "values": s.values} for s in ser]

    return json.dumps(
        {
            "baseline": report.baseline_label,
            "candidate": report.candidate_label,
            "metrics": [
                {
                    "test": m.test,
                    "metric": m.metric,
                    "verdict": m.verdict,
                    "paired_test": (
                        {
                            "n": m.paired_test.n,
                            "mean_baseline": m.paired_test.mean_baseline,
                            "mean_candidate": m.paired_test.mean_candidate,
                            "mean_delta": m.paired_test.mean_delta,
                            "median_pct_change": m.paired_test.median_pct_change,
                            "t_stat": m.paired_test.t_stat,
                            "p_value": m.paired_test.p_value,
                            "significant": m.paired_test.significant,
                        }
                        if m.paired_test
                        else None
                    ),
                    "baseline_stability": (
                        m.baseline_stability.__dict__ if m.baseline_stability else None
                    ),
                    "candidate_stability": (
                        m.candidate_stability.__dict__
                        if m.candidate_stability
                        else None
                    ),
                    "worst_seconds": [
                        {"ts": ts, "baseline": b, "candidate": c}
                        for ts, b, c in m.worst_seconds
                    ],
                    "baseline_iterations": _series(m.baseline_iterations),
                    "candidate_iterations": _series(m.candidate_iterations),
                }
                for m in report.metrics
            ],
        },
        indent=2,
    )


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def compare_perrun_command_logic(args, _project_name=None, _project_version=None):
    if (args.baseline_branch is None) == (args.baseline_tag is None):
        logging.error("Specify exactly one of --baseline-branch or --baseline-tag.")
        sys.exit(1)
    if (args.comparison_branch is None) == (args.comparison_tag is None):
        logging.error("Specify exactly one of --comparison-branch or --comparison-tag.")
        sys.exit(1)
    if not args.pg_dsn:
        logging.error(
            "PERFORMANCE_PG_DSN is empty and --pg-dsn was not given; "
            "compare-perrun requires a Postgres source."
        )
        sys.exit(1)

    metrics = [m.strip() for m in args.metric.split(",") if m.strip()]
    if not metrics:
        logging.error("--metric must list at least one metric")
        sys.exit(1)

    import psycopg  # noqa: WPS433  -- lazy import for parity with run/postgres.py

    with psycopg.connect(args.pg_dsn, autocommit=False) as conn:
        report = run_comparison(
            conn,
            baseline_branch=args.baseline_branch,
            baseline_tag=args.baseline_tag,
            comparison_branch=args.comparison_branch,
            comparison_tag=args.comparison_tag,
            test=args.test,
            metrics=metrics,
            threshold_pct=args.regression_threshold_pct,
            alpha=args.alpha,
        )

    if args.output_format == "markdown":
        rendered = render_markdown(report)
    elif args.output_format == "json":
        rendered = render_json(report)
    else:
        rendered = render_text(report)

    if args.output_file:
        with open(args.output_file, "w") as fh:
            fh.write(rendered)
        logging.info("Wrote report to %s", args.output_file)
    else:
        sys.stdout.write(rendered)

    # Exit non-zero if any metric regressed — useful for CI gating.
    has_regression = any(m.verdict == REGRESSION for m in report.metrics)
    if has_regression:
        sys.exit(2)
