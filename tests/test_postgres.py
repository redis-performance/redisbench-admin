#  BSD 3-Clause License
#
#  Copyright (c) 2026., Redis Performance Group
#  All rights reserved.
#
"""Postgres exporter tests.

These tests exercise the real psycopg3 driver against a real Postgres
instance — no mocks of the connection. Bring the DB up locally with:

    docker run --rm -d --name pg-redisbench -p 5432:5432 \\
      -e POSTGRES_PASSWORD=redisbench -e POSTGRES_DB=search \\
      postgres:16-alpine

Then ``export PG_TEST_DSN=postgresql://postgres:redisbench@localhost:5432/search``
and run ``pytest tests/test_postgres.py``. Under tox, the ``pg_datasink``
container is started by ``tox-docker`` and its host-port is exposed via the
``PG_PORT`` env var (see tox.ini).

If neither ``PG_TEST_DSN`` nor ``PG_PORT`` is present, the integration tests
skip — but the pure-parse unit tests still run.
"""
import json
import os
import uuid

import pytest

from redisbench_admin.run.postgres import (
    SCHEMA_SQL,
    _coerce_test_name,
    _ms_to_iso,
    derive_batch_id,
    derive_run_id,
    ensure_schema,
    insert_metrics,
    insert_run,
    insert_samples,
    is_enabled,
    postgres_test_success_flow,
    summary_rows_from_results,
)


# ---------------------------------------------------------------------------
# Pure-Python helpers — no DB needed
# ---------------------------------------------------------------------------


def test_is_enabled_reads_env(monkeypatch):
    monkeypatch.delenv("PERFORMANCE_PG_DSN", raising=False)
    assert is_enabled() is False
    monkeypatch.setenv("PERFORMANCE_PG_DSN", "postgresql://u@h/db")
    assert is_enabled() is True
    monkeypatch.setenv("PERFORMANCE_PG_DSN", "")
    assert is_enabled() is False


def test_coerce_test_name_variants():
    assert _coerce_test_name(None) is None
    assert _coerce_test_name("foo") == "foo"
    assert _coerce_test_name(["a", "b"]) == "a,b"
    assert _coerce_test_name([]) is None


def test_ms_to_iso_handles_none_and_value():
    assert _ms_to_iso(None) is None
    iso = _ms_to_iso(0)
    assert iso.startswith("1970-01-01T00:00:00")


def test_summary_rows_from_results_uses_extract_results_table():
    metrics = ["$.Tests.PING_INLINE.rps", "$.Tests.PING_INLINE.p99_latency_ms"]
    results_dict = {
        "Tests": {
            "PING_INLINE": {"rps": "133155.80", "p99_latency_ms": "0.503"},
        }
    }
    rows = summary_rows_from_results(metrics, results_dict)
    by_metric = {name: value for name, _ctx, value in rows}
    assert by_metric["Tests.PING_INLINE.rps"] == pytest.approx(133155.80)
    assert by_metric["Tests.PING_INLINE.p99_latency_ms"] == pytest.approx(0.503)
    # context paths should be populated
    assert all(isinstance(ctx, str) for _name, ctx, _v in rows)


def test_summary_rows_from_results_skips_non_numeric():
    metrics = ["$.Tests.PING_INLINE.rps", "$.Tests.PING_INLINE.note"]
    results_dict = {"Tests": {"PING_INLINE": {"rps": "1.0", "note": "n/a"}}}
    rows = summary_rows_from_results(metrics, results_dict)
    assert len(rows) == 1
    assert rows[0][0] == "Tests.PING_INLINE.rps"


def test_postgres_test_success_flow_disabled_when_flag_false(monkeypatch):
    """If the push flag is False, never touch the DB even with DSN set."""
    monkeypatch.setenv("PERFORMANCE_PG_DSN", "postgresql://nope:nope@nope/nope")
    result = postgres_test_success_flow(False, test_name="t")
    assert result is None


def test_postgres_test_success_flow_disabled_when_no_dsn(monkeypatch):
    monkeypatch.delenv("PERFORMANCE_PG_DSN", raising=False)
    result = postgres_test_success_flow(True, test_name="t")
    assert result is None


def test_derive_batch_id_uses_explicit_seed(monkeypatch):
    monkeypatch.delenv("PERFORMANCE_BATCH_ID", raising=False)
    monkeypatch.delenv("GITHUB_RUN_ID", raising=False)
    a = derive_batch_id("explicit-seed")
    b = derive_batch_id("explicit-seed")
    assert a == b
    assert a != derive_batch_id("other-seed")


def test_derive_batch_id_prefers_env_var_over_github(monkeypatch):
    monkeypatch.setenv("PERFORMANCE_BATCH_ID", "manual-batch")
    monkeypatch.setenv("GITHUB_RUN_ID", "999999")
    direct = derive_batch_id("manual-batch")
    via_env = derive_batch_id()
    assert direct == via_env


def test_derive_batch_id_uses_github_run_id_and_attempt(monkeypatch):
    monkeypatch.delenv("PERFORMANCE_BATCH_ID", raising=False)
    monkeypatch.setenv("GITHUB_RUN_ID", "12345")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")
    attempt1 = derive_batch_id()
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "2")
    attempt2 = derive_batch_id()
    # Different attempts → different batch_ids; same input → reproducible.
    assert attempt1 != attempt2
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")
    assert derive_batch_id() == attempt1


def test_derive_batch_id_falls_back_to_uuid4_unseeded(monkeypatch):
    monkeypatch.delenv("PERFORMANCE_BATCH_ID", raising=False)
    monkeypatch.delenv("GITHUB_RUN_ID", raising=False)
    a = derive_batch_id()
    b = derive_batch_id()
    # No seed → fresh uuid4 each call.
    assert a != b


def test_derive_run_id_is_deterministic_within_batch():
    import uuid as _uuid

    batch = _uuid.UUID("11111111-1111-1111-1111-111111111111")
    a = derive_run_id(batch, "memtier-set", 1)
    b = derive_run_id(batch, "memtier-set", 1)
    assert a == b
    # Different iteration → different id
    assert a != derive_run_id(batch, "memtier-set", 2)
    # Different test → different id
    assert a != derive_run_id(batch, "memtier-get", 1)
    # Different batch → different id
    other = _uuid.UUID("22222222-2222-2222-2222-222222222222")
    assert a != derive_run_id(other, "memtier-set", 1)


def test_schema_sql_is_idempotent_string():
    """SCHEMA_SQL is exposed as a plain string so callers can preview."""
    assert "CREATE TABLE IF NOT EXISTS bench_run" in SCHEMA_SQL
    assert "CREATE TABLE IF NOT EXISTS bench_run_sample" in SCHEMA_SQL
    assert "CREATE TABLE IF NOT EXISTS bench_run_metric" in SCHEMA_SQL


# ---------------------------------------------------------------------------
# Real-DB integration — needs docker
# ---------------------------------------------------------------------------


def _resolve_dsn():
    """Resolve a DSN from either an explicit env var or the tox-docker port."""
    if os.environ.get("PG_TEST_DSN"):
        return os.environ["PG_TEST_DSN"]
    pg_port = os.environ.get("PG_PORT")
    if pg_port:
        # tox-docker exposes the mapped host port via PG_PORT
        return (
            f"postgresql://postgres:redisbench@localhost:{pg_port}/search"
        )
    return None


@pytest.fixture
def pg_dsn():
    dsn = _resolve_dsn()
    if not dsn:
        pytest.skip("No Postgres available (set PG_TEST_DSN or run under tox)")
    return dsn


@pytest.fixture
def pg_conn(pg_dsn):
    """A clean connection with all bench_* tables wiped before each test."""
    psycopg = pytest.importorskip("psycopg")
    conn = psycopg.connect(pg_dsn, autocommit=False)
    try:
        ensure_schema(conn)
        with conn.cursor() as cur:
            cur.execute("TRUNCATE bench_run_sample, bench_run_metric, bench_run")
        conn.commit()
        yield conn
    finally:
        conn.close()


def test_ensure_schema_is_idempotent(pg_dsn):
    psycopg = pytest.importorskip("psycopg")
    with psycopg.connect(pg_dsn) as conn:
        ensure_schema(conn)
        ensure_schema(conn)  # second call should not raise
        with conn.cursor() as cur:
            cur.execute(
                "SELECT to_regclass('bench_run'), "
                "to_regclass('bench_run_metric'), "
                "to_regclass('bench_run_sample')"
            )
            row = cur.fetchone()
            assert row == ("bench_run", "bench_run_metric", "bench_run_sample")


def test_insert_run_writes_summary_row(pg_conn):
    run_id = uuid.uuid4()
    insert_run(
        pg_conn,
        run_id=run_id,
        batch_id=run_id,
        iteration=1,
        test_name="vanilla-memtier-query",
        branch="master",
        tag=None,
        commit_sha="abc123",
        arch="x86_64",
        setup="oss-standalone",
        deployment_type="oss-standalone",
        triggering_env="ci",
        start_time_ms=1_700_000_000_000,
        duration_s=60,
    )
    pg_conn.commit()
    with pg_conn.cursor() as cur:
        cur.execute("SELECT test, branch, iteration, duration_s FROM bench_run")
        rows = cur.fetchall()
    assert rows == [("vanilla-memtier-query", "master", 1, 60)]


def test_insert_run_upserts_on_conflict(pg_conn):
    run_id = uuid.uuid4()
    insert_run(
        pg_conn,
        run_id=run_id,
        batch_id=None,
        iteration=1,
        test_name="t",
        branch="master",
        tag=None,
        commit_sha=None,
        arch=None,
        setup=None,
        deployment_type=None,
        triggering_env=None,
        start_time_ms=None,
        duration_s=10,
    )
    insert_run(
        pg_conn,
        run_id=run_id,
        batch_id=None,
        iteration=2,
        test_name="t",
        branch="master",
        tag=None,
        commit_sha=None,
        arch=None,
        setup=None,
        deployment_type=None,
        triggering_env=None,
        start_time_ms=None,
        duration_s=20,
    )
    pg_conn.commit()
    with pg_conn.cursor() as cur:
        cur.execute("SELECT iteration, duration_s FROM bench_run")
        assert cur.fetchall() == [(2, 20)]


def test_insert_metrics_via_copy(pg_conn):
    run_id = uuid.uuid4()
    insert_run(
        pg_conn,
        run_id=run_id,
        batch_id=run_id,
        iteration=1,
        test_name="t",
        branch="master",
        tag=None,
        commit_sha=None,
        arch=None,
        setup=None,
        deployment_type=None,
        triggering_env=None,
        start_time_ms=None,
        duration_s=None,
    )
    rows = [
        ("rps", "Tests.GET", 100000.5),
        ("p99_latency_ms", "Tests.GET", 1.2),
        ("rps", "Tests.SET", 90000.0),
    ]
    n = insert_metrics(pg_conn, run_id, rows)
    assert n == 3
    pg_conn.commit()
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT metric, context, value FROM bench_run_metric ORDER BY metric, context"
        )
        out = cur.fetchall()
    assert out == [
        ("p99_latency_ms", "Tests.GET", 1.2),
        ("rps", "Tests.GET", 100000.5),
        ("rps", "Tests.SET", 90000.0),
    ]


def test_insert_metrics_upserts_duplicates(pg_conn):
    run_id = uuid.uuid4()
    insert_run(
        pg_conn,
        run_id=run_id,
        batch_id=run_id,
        iteration=1,
        test_name="t",
        branch="master",
        tag=None,
        commit_sha=None,
        arch=None,
        setup=None,
        deployment_type=None,
        triggering_env=None,
        start_time_ms=None,
        duration_s=None,
    )
    insert_metrics(pg_conn, run_id, [("rps", "Tests.GET", 100.0)])
    insert_metrics(pg_conn, run_id, [("rps", "Tests.GET", 200.0)])
    pg_conn.commit()
    with pg_conn.cursor() as cur:
        cur.execute("SELECT value FROM bench_run_metric")
        assert cur.fetchall() == [(200.0,)]


def test_insert_samples_bulk_via_copy(pg_conn):
    run_id = uuid.uuid4()
    insert_run(
        pg_conn,
        run_id=run_id,
        batch_id=run_id,
        iteration=1,
        test_name="t",
        branch="master",
        tag=None,
        commit_sha=None,
        arch=None,
        setup=None,
        deployment_type=None,
        triggering_env=None,
        start_time_ms=None,
        duration_s=None,
    )
    samples = [(s, "rps", 100.0 + s) for s in range(60)]
    samples += [(s, "lat_p99_us", 200.0 + s) for s in range(60)]
    n = insert_samples(pg_conn, run_id, samples)
    assert n == 120
    pg_conn.commit()
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT count(*), min(ts_offset_s), max(ts_offset_s) "
            "FROM bench_run_sample WHERE metric = 'rps'"
        )
        assert cur.fetchone() == (60, 0, 59)


def test_insert_samples_handles_empty(pg_conn):
    run_id = uuid.uuid4()
    insert_run(
        pg_conn,
        run_id=run_id,
        batch_id=run_id,
        iteration=1,
        test_name="t",
        branch="master",
        tag=None,
        commit_sha=None,
        arch=None,
        setup=None,
        deployment_type=None,
        triggering_env=None,
        start_time_ms=None,
        duration_s=None,
    )
    assert insert_samples(pg_conn, run_id, []) == 0
    assert insert_metrics(pg_conn, run_id, []) == 0


def test_full_postgres_test_success_flow_end_to_end(pg_dsn, monkeypatch, tmp_path):
    """End-to-end: redis-benchmark JSON → schema bootstrap → run + metric rows."""
    monkeypatch.setenv("PERFORMANCE_PG_DSN", pg_dsn)
    psycopg = pytest.importorskip("psycopg")

    # Wipe so the assertion at the end is deterministic.
    with psycopg.connect(pg_dsn) as bootstrap:
        ensure_schema(bootstrap)
        with bootstrap.cursor() as cur:
            cur.execute("TRUNCATE bench_run_sample, bench_run_metric, bench_run")
        bootstrap.commit()

    fixture_path = (
        "tests/test_data/results/"
        "oss-standalone-2021-07-23-16-15-12-71d4528-redis-benchmark-full-suite-1Mkeys-100B.json"
    )
    with open(fixture_path) as f:
        results = json.load(f)
    metrics = [
        "$.Tests.*.rps",
        "$.Tests.*.p99_latency_ms",
    ]

    run_id = postgres_test_success_flow(
        True,
        test_name="redis-benchmark-full-suite-1Mkeys-100B",
        metrics=metrics,
        results_dict=results,
        branch="master",
        tag="6.2.4",
        commit_sha="71d4528",
        arch="x86_64",
        setup="oss-standalone",
        deployment_type="oss-standalone",
        triggering_env="ci",
        start_time_ms=1_700_000_000_000,
        duration_s=60,
        iteration=1,
    )
    assert run_id is not None

    with psycopg.connect(pg_dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM bench_run WHERE run_id = %s", (str(run_id),))
        (n_run,) = cur.fetchone()
        cur.execute("SELECT count(*) FROM bench_run_metric WHERE run_id = %s", (str(run_id),))
        (n_metric,) = cur.fetchone()

    assert n_run == 1
    # 17 tests * 2 metrics each in the fixture
    assert n_metric == len(results["Tests"]) * 2


def test_derive_run_id_makes_re_push_idempotent(pg_dsn, monkeypatch):
    """Two postgres_test_success_flow calls with the same derived run_id
    must collapse into a single bench_run row (the upsert path)."""
    monkeypatch.setenv("PERFORMANCE_PG_DSN", pg_dsn)
    monkeypatch.delenv("PERFORMANCE_BATCH_ID", raising=False)
    monkeypatch.setenv("GITHUB_RUN_ID", "987654")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")
    psycopg = pytest.importorskip("psycopg")

    with psycopg.connect(pg_dsn) as bootstrap:
        ensure_schema(bootstrap)
        with bootstrap.cursor() as cur:
            cur.execute("TRUNCATE bench_run_sample, bench_run_metric, bench_run")
        bootstrap.commit()

    batch = derive_batch_id()
    rid = derive_run_id(batch, "t", 1)

    first = postgres_test_success_flow(
        True,
        run_id=rid,
        batch_id=batch,
        iteration=1,
        test_name="t",
        metrics=[],
        results_dict={},
        branch="master",
        duration_s=10,
    )
    second = postgres_test_success_flow(
        True,
        run_id=rid,
        batch_id=batch,
        iteration=1,
        test_name="t",
        metrics=[],
        results_dict={},
        branch="master",
        duration_s=20,
    )
    assert first == second == rid

    with psycopg.connect(pg_dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*), max(duration_s) FROM bench_run")
        n, dur = cur.fetchone()
    assert n == 1
    assert dur == 20  # second call won the upsert


def test_full_flow_with_memtier_per_second_samples(pg_dsn, monkeypatch):
    """End-to-end: real memtier v1.3.1 JSON → schema bootstrap → samples persisted."""
    monkeypatch.setenv("PERFORMANCE_PG_DSN", pg_dsn)
    psycopg = pytest.importorskip("psycopg")
    from redisbench_admin.run.postgres_parsers import detect_and_parse

    with psycopg.connect(pg_dsn) as bootstrap:
        ensure_schema(bootstrap)
        with bootstrap.cursor() as cur:
            cur.execute("TRUNCATE bench_run_sample, bench_run_metric, bench_run")
        bootstrap.commit()

    samples = detect_and_parse(
        "tests/test_data/memtier_benchmark_v1.3.1_result.json"
    )
    assert samples, "memtier fixture must yield non-empty samples"

    run_id = postgres_test_success_flow(
        True,
        test_name="memtier-per-second-smoke",
        metrics=[],
        results_dict={},
        samples=samples,
        branch="master",
        iteration=1,
        start_time_ms=1_700_000_000_000,
        duration_s=11,
    )
    assert run_id is not None

    with psycopg.connect(pg_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM bench_run_sample WHERE run_id = %s",
            (str(run_id),),
        )
        (n_samples,) = cur.fetchone()
        cur.execute(
            "SELECT DISTINCT metric FROM bench_run_sample WHERE run_id = %s "
            "ORDER BY metric",
            (str(run_id),),
        )
        metrics = [row[0] for row in cur.fetchall()]
    assert n_samples == len(samples)
    assert metrics == sorted(
        ["rps", "lat_avg_us", "lat_p50_us", "lat_p99_us", "lat_p999_us"]
    )


def test_full_flow_paired_query_returns_baseline_vs_candidate(pg_dsn, monkeypatch):
    """Smoke-test the head-to-head SQL pattern from the handoff doc."""
    monkeypatch.setenv("PERFORMANCE_PG_DSN", pg_dsn)
    psycopg = pytest.importorskip("psycopg")

    with psycopg.connect(pg_dsn) as bootstrap:
        ensure_schema(bootstrap)
        with bootstrap.cursor() as cur:
            cur.execute("TRUNCATE bench_run_sample, bench_run_metric, bench_run")
        bootstrap.commit()

    # baseline (tag-tagged) and candidate (branch-tagged) iteration 1
    base = postgres_test_success_flow(
        True,
        test_name="t",
        metrics=[],
        results_dict={},
        branch=None,
        tag="8.6.0",
        iteration=1,
        samples=[(s, "rps", 100.0) for s in range(5)],
    )
    cand = postgres_test_success_flow(
        True,
        test_name="t",
        metrics=[],
        results_dict={},
        branch="master",
        tag=None,
        iteration=1,
        samples=[(s, "rps", 110.0) for s in range(5)],
    )
    assert base and cand

    paired_sql = """
    SELECT b.ts_offset_s, b.metric, b.value AS base, c.value AS cand,
           c.value - b.value AS delta
    FROM   bench_run rb JOIN bench_run rc
           ON rb.test = rc.test AND rb.iteration = rc.iteration
    JOIN   bench_run_sample b ON b.run_id = rb.run_id
    JOIN   bench_run_sample c ON c.run_id = rc.run_id
                              AND c.ts_offset_s = b.ts_offset_s
                              AND c.metric      = b.metric
    WHERE  rb.tag = '8.6.0' AND rc.branch = 'master'
    ORDER  BY rb.iteration, b.metric, b.ts_offset_s
    """
    with psycopg.connect(pg_dsn) as conn, conn.cursor() as cur:
        cur.execute(paired_sql)
        out = cur.fetchall()
    assert len(out) == 5
    assert all(row[4] == pytest.approx(10.0) for row in out)
