#  BSD 3-Clause License
#
#  Copyright (c) 2026., Redis Performance Group
#  All rights reserved.
#
"""End-to-end tests for ``compare-perrun``.

Loads synthetic baseline + candidate per-second samples into a real
Postgres, runs the comparison, and asserts the verdict + drill-down. Skips
if no Postgres is available (set ``PG_TEST_DSN`` or run under tox with
``pg_datasink``)."""
import argparse
import json
import os
import random

import pytest

from redisbench_admin.compare_perrun.args import create_compare_perrun_arguments
from redisbench_admin.compare_perrun.compare_perrun import (
    GREEN,
    IMPROVEMENT,
    REGRESSION,
    classify,
    compare_perrun_command_logic,
    metric_higher_is_better,
    render_json,
    render_markdown,
    render_text,
    run_comparison,
)
from redisbench_admin.run.postgres import (
    derive_batch_id,
    derive_run_id,
    ensure_schema,
    postgres_test_success_flow,
)


# ---------------------------------------------------------------------------
# Pure-Python helpers — no DB
# ---------------------------------------------------------------------------


def test_metric_higher_is_better_classification():
    assert metric_higher_is_better("rps") is True
    assert metric_higher_is_better("ops_per_sec") is True
    assert metric_higher_is_better("throughput") is True
    assert metric_higher_is_better("qps") is True
    assert metric_higher_is_better("lat_p99_us") is False
    assert metric_higher_is_better("lat_avg_us") is False


@pytest.mark.parametrize(
    "metric,pct,p,expected",
    [
        # higher-is-better: -10% drop with significance → REGRESSION
        ("rps", -10.0, 0.001, REGRESSION),
        # higher-is-better: +10% gain with significance → IMPROVEMENT
        ("rps", +10.0, 0.001, IMPROVEMENT),
        # latency: +10% increase = regression for higher-is-worse
        ("lat_p99_us", +10.0, 0.001, REGRESSION),
        # latency: -10% decrease = improvement
        ("lat_p99_us", -10.0, 0.001, IMPROVEMENT),
        # within threshold + non-significant → GREEN
        ("rps", 0.5, 0.4, GREEN),
        # large change but not significant → WARN
        ("rps", -8.0, 0.5, "WARN"),
    ],
)
def test_classify_buckets(metric, pct, p, expected):
    assert classify(metric, pct, p, threshold_pct=3.0, alpha=0.05) == expected


def test_classify_inconclusive_when_no_p():
    assert classify("rps", 0.0, None, 3.0, 0.05) == "INCONCLUSIVE"


# ---------------------------------------------------------------------------
# Real DB — pulls a fixture into PG and runs the comparison
# ---------------------------------------------------------------------------


def _resolve_dsn():
    if os.environ.get("PG_TEST_DSN"):
        return os.environ["PG_TEST_DSN"]
    if os.environ.get("PG_PORT"):
        return f"postgresql://postgres:redisbench@localhost:{os.environ['PG_PORT']}/search"
    return None


@pytest.fixture
def pg_dsn():
    dsn = _resolve_dsn()
    if not dsn:
        pytest.skip("No Postgres available")
    return dsn


@pytest.fixture
def loaded_pg(pg_dsn, monkeypatch):
    """Push 5 baseline + 5 candidate iterations of synthetic data into PG.

    Baseline rps ≈ 100k ± 1%, candidate rps ≈ 92k ± 1% — that's an 8% drop,
    well past the 3% threshold and clearly significant.
    """
    monkeypatch.setenv("PERFORMANCE_PG_DSN", pg_dsn)
    psycopg = pytest.importorskip("psycopg")
    rng = random.Random(0)

    with psycopg.connect(pg_dsn) as bootstrap:
        ensure_schema(bootstrap)
        with bootstrap.cursor() as cur:
            cur.execute("TRUNCATE bench_run_sample, bench_run_metric, bench_run")
        bootstrap.commit()

    test_name = "regression-fixture"

    def push(iter_count, branch, tag, rps_base, lat_base):
        for it in range(1, iter_count + 1):
            batch = derive_batch_id(seed=f"{branch or tag}-batch")
            rid = derive_run_id(batch, test_name, it)
            samples = []
            for sec in range(60):
                samples.append((sec, "rps", rps_base * (1.0 + rng.uniform(-0.01, 0.01))))
                samples.append(
                    (sec, "lat_p99_us", lat_base * (1.0 + rng.uniform(-0.02, 0.02)))
                )
            postgres_test_success_flow(
                True,
                run_id=rid,
                batch_id=batch,
                iteration=it,
                test_name=test_name,
                metrics=[],
                results_dict={},
                samples=samples,
                branch=branch,
                tag=tag,
                duration_s=60,
            )

    push(5, branch=None, tag="8.6.0", rps_base=100_000.0, lat_base=1500.0)
    push(5, branch="master", tag=None, rps_base=92_000.0, lat_base=1700.0)
    return pg_dsn, test_name


def test_run_comparison_flags_regression(loaded_pg):
    dsn, _ = loaded_pg
    psycopg = pytest.importorskip("psycopg")
    with psycopg.connect(dsn) as conn:
        report = run_comparison(
            conn,
            baseline_branch=None,
            baseline_tag="8.6.0",
            comparison_branch="master",
            comparison_tag=None,
            test=None,
            metrics=["rps", "lat_p99_us"],
            threshold_pct=3.0,
            alpha=0.05,
        )

    assert report.baseline_label == "8.6.0"
    assert report.candidate_label == "master"
    by_metric = {m.metric: m for m in report.metrics}

    rps = by_metric["rps"]
    assert rps.verdict == REGRESSION
    assert rps.paired_test is not None
    assert rps.paired_test.n == 5
    assert rps.paired_test.median_pct_change < -3.0

    lat = by_metric["lat_p99_us"]
    assert lat.verdict == REGRESSION
    assert lat.paired_test.median_pct_change > 3.0


def test_run_comparison_no_regression_when_runs_match(pg_dsn, monkeypatch):
    """Same baseline twice → GREEN verdict."""
    monkeypatch.setenv("PERFORMANCE_PG_DSN", pg_dsn)
    psycopg = pytest.importorskip("psycopg")
    rng = random.Random(7)

    with psycopg.connect(pg_dsn) as bootstrap:
        ensure_schema(bootstrap)
        with bootstrap.cursor() as cur:
            cur.execute("TRUNCATE bench_run_sample, bench_run_metric, bench_run")
        bootstrap.commit()

    test_name = "stable-fixture"
    for tag, branch in (("v1", None), (None, "main")):
        for it in range(1, 5):
            batch = derive_batch_id(seed=f"{tag or branch}-stable")
            rid = derive_run_id(batch, test_name, it)
            samples = [
                (sec, "rps", 50_000.0 * (1.0 + rng.uniform(-0.005, 0.005)))
                for sec in range(60)
            ]
            postgres_test_success_flow(
                True,
                run_id=rid,
                batch_id=batch,
                iteration=it,
                test_name=test_name,
                metrics=[],
                results_dict={},
                samples=samples,
                branch=branch,
                tag=tag,
                duration_s=60,
            )

    with psycopg.connect(pg_dsn) as conn:
        report = run_comparison(
            conn,
            baseline_branch=None,
            baseline_tag="v1",
            comparison_branch="main",
            comparison_tag=None,
            test=None,
            metrics=["rps"],
            threshold_pct=3.0,
            alpha=0.05,
        )
    assert all(m.verdict == GREEN for m in report.metrics), [
        (m.metric, m.verdict) for m in report.metrics
    ]


def test_render_text_includes_verdict_and_metric(loaded_pg):
    dsn, _ = loaded_pg
    psycopg = pytest.importorskip("psycopg")
    with psycopg.connect(dsn) as conn:
        report = run_comparison(
            conn,
            baseline_branch=None,
            baseline_tag="8.6.0",
            comparison_branch="master",
            comparison_tag=None,
            test=None,
            metrics=["rps"],
            threshold_pct=3.0,
            alpha=0.05,
        )
    text = render_text(report)
    assert "REGRESSION" in text
    assert "rps" in text
    assert "regression-fixture" in text
    assert "worst per-second offsets" in text


def test_render_markdown_table_shape(loaded_pg):
    dsn, _ = loaded_pg
    psycopg = pytest.importorskip("psycopg")
    with psycopg.connect(dsn) as conn:
        report = run_comparison(
            conn,
            baseline_branch=None,
            baseline_tag="8.6.0",
            comparison_branch="master",
            comparison_tag=None,
            test=None,
            metrics=["rps"],
            threshold_pct=3.0,
            alpha=0.05,
        )
    md = render_markdown(report)
    assert md.startswith("# Per-run comparison")
    assert "| verdict |" in md
    assert "REGRESSION" in md


def test_render_json_round_trip(loaded_pg):
    dsn, _ = loaded_pg
    psycopg = pytest.importorskip("psycopg")
    with psycopg.connect(dsn) as conn:
        report = run_comparison(
            conn,
            baseline_branch=None,
            baseline_tag="8.6.0",
            comparison_branch="master",
            comparison_tag=None,
            test=None,
            metrics=["rps"],
            threshold_pct=3.0,
            alpha=0.05,
        )
    payload = json.loads(render_json(report))
    assert payload["baseline"] == "8.6.0"
    assert payload["candidate"] == "master"
    assert payload["metrics"][0]["metric"] == "rps"
    assert payload["metrics"][0]["verdict"] == REGRESSION
    assert payload["metrics"][0]["paired_test"]["n"] == 5


# ---------------------------------------------------------------------------
# CLI integration
# ---------------------------------------------------------------------------


def _make_args(**overrides):
    parser = argparse.ArgumentParser()
    create_compare_perrun_arguments(parser)
    args = parser.parse_args([])
    for k, v in overrides.items():
        setattr(args, k, v)
    return args


def test_cli_regression_exits_with_code_2(loaded_pg, tmp_path, capsys):
    dsn, _ = loaded_pg
    out = tmp_path / "report.json"
    args = _make_args(
        baseline_tag="8.6.0",
        comparison_branch="master",
        metric="rps",
        pg_dsn=dsn,
        output_format="json",
        output_file=str(out),
    )
    with pytest.raises(SystemExit) as excinfo:
        compare_perrun_command_logic(args)
    assert excinfo.value.code == 2  # regression detected
    payload = json.loads(out.read_text())
    assert payload["metrics"][0]["verdict"] == REGRESSION


def test_cli_rejects_baseline_branch_and_tag_simultaneously(loaded_pg):
    dsn, _ = loaded_pg
    args = _make_args(
        baseline_branch="master",
        baseline_tag="8.6.0",
        comparison_branch="master",
        pg_dsn=dsn,
    )
    with pytest.raises(SystemExit) as excinfo:
        compare_perrun_command_logic(args)
    assert excinfo.value.code == 1


def test_cli_rejects_missing_pg_dsn(loaded_pg, monkeypatch):
    monkeypatch.delenv("PERFORMANCE_PG_DSN", raising=False)
    args = _make_args(
        baseline_tag="8.6.0",
        comparison_branch="master",
        pg_dsn="",
    )
    with pytest.raises(SystemExit) as excinfo:
        compare_perrun_command_logic(args)
    assert excinfo.value.code == 1
