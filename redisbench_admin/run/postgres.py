#  BSD 3-Clause License
#
#  Copyright (c) 2026., Redis Performance Group
#  All rights reserved.
#
"""Postgres exporter for benchmark results.

Sibling to ``redisbench_admin.run.redistimeseries``. Writes per-run summary
rows (one row per ``test_name`` invocation) plus per-second samples to
Postgres so we can do paired multi-run stability analysis with classic stats
(t-test, KS) instead of the single-datapoint % delta the RTS path reports.

Disabled when the ``PERFORMANCE_PG_DSN`` env var is empty / unset, mirroring
how an empty ``PERFORMANCE_RTS_HOST`` disables the RTS push.
"""
import logging
import os
import uuid
from contextlib import contextmanager
from typing import Iterable, Optional, Tuple

from redisbench_admin.run.metrics import extract_results_table


PERFORMANCE_PG_DSN = os.getenv("PERFORMANCE_PG_DSN", "")


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS bench_run (
  run_id          uuid PRIMARY KEY,
  batch_id        uuid,
  iteration       int,
  test            text,
  branch          text,
  tag             text,
  commit_sha      text,
  arch            text,
  setup           text,
  deployment_type text,
  triggering_env  text,
  started_at      timestamptz,
  duration_s      int
);

CREATE TABLE IF NOT EXISTS bench_run_metric (
  run_id      uuid REFERENCES bench_run(run_id) ON DELETE CASCADE,
  metric      text NOT NULL,
  context     text,
  value       double precision NOT NULL,
  PRIMARY KEY (run_id, metric, context)
);

CREATE TABLE IF NOT EXISTS bench_run_sample (
  run_id      uuid REFERENCES bench_run(run_id) ON DELETE CASCADE,
  ts_offset_s int NOT NULL,
  metric      text NOT NULL,
  value       double precision NOT NULL,
  PRIMARY KEY (run_id, ts_offset_s, metric)
);

CREATE INDEX IF NOT EXISTS bench_run_lookup
  ON bench_run (test, branch, tag, iteration);
CREATE INDEX IF NOT EXISTS bench_run_sample_metric
  ON bench_run_sample (metric, ts_offset_s);
"""


def _dsn() -> str:
    """Read the DSN from the environment at call time.

    The lookup is deferred (instead of caching the module-level constant) so
    that tests and callers can flip ``PERFORMANCE_PG_DSN`` after import.
    """
    return os.environ.get("PERFORMANCE_PG_DSN", "") or ""


# UUIDv5 namespaces for deterministic IDs. These are hard-coded so the same
# (gh_run_id, test, iteration) tuple always produces the same UUID — re-pushing
# data is idempotent, and a row in PG can be cross-joined to its GH Actions run.
# Generated once via ``uuid.uuid5(uuid.NAMESPACE_URL, "redisbench-admin/<scope>")``.
_BATCH_NS = uuid.UUID("4d2bb8a1-9e42-5b0c-9a45-7e8b0e1f4d8a")
_RUN_NS = uuid.UUID("1f06a3e5-1e4e-5a6d-9c0a-3a8d6f2b9e1c")


def derive_batch_id(seed: Optional[str] = None) -> uuid.UUID:
    """Pick a batch_id, preferring deterministic CI-correlated seeds.

    Resolution order:
      1. ``seed`` argument (e.g. user-supplied ``--batch-id``).
      2. ``PERFORMANCE_BATCH_ID`` env var (explicit override for ad-hoc runs).
      3. ``GITHUB_RUN_ID`` (+ ``GITHUB_RUN_ATTEMPT`` if a workflow has been
         re-run, since attempt 2 is a different physical run).
      4. Fresh ``uuid.uuid4()`` for unseeded local runs.

    The CI-derived form is ``uuid5(_BATCH_NS, "<run_id>:<attempt>")`` so the
    PG row always joins back to the same GH Actions run, and re-pushing the
    same data on a hook retry doesn't duplicate.
    """
    if seed is None:
        seed = os.environ.get("PERFORMANCE_BATCH_ID")
    if seed is None:
        gh_run = os.environ.get("GITHUB_RUN_ID")
        if gh_run:
            attempt = os.environ.get("GITHUB_RUN_ATTEMPT", "1")
            seed = f"{gh_run}:{attempt}"
    if seed:
        return uuid.uuid5(_BATCH_NS, seed)
    return uuid.uuid4()


def derive_run_id(batch_id: uuid.UUID, test_name, iteration: int) -> uuid.UUID:
    """Stable ``run_id`` derived from ``(batch_id, test_name, iteration)``.

    Determinism makes re-pushing idempotent (the upsert in ``insert_run``
    targets the same primary key) and means that a CI hook retry won't write
    a duplicate row — we just overwrite ourselves.
    """
    seed = f"{batch_id}:{_coerce_test_name(test_name)}:{iteration}"
    return uuid.uuid5(_RUN_NS, seed)


def is_enabled() -> bool:
    return bool(_dsn())


@contextmanager
def connect(dsn: Optional[str] = None):
    """Context-managed psycopg3 connection.

    Imports ``psycopg`` lazily so that environments without psycopg installed
    can still ``import redisbench_admin.run.postgres`` (the module is referenced
    unconditionally from the run flows).
    """
    import psycopg  # noqa: WPS433  -- lazy import on purpose

    effective_dsn = dsn if dsn is not None else _dsn()
    if not effective_dsn:
        raise RuntimeError(
            "PERFORMANCE_PG_DSN is not set; refusing to open a Postgres connection."
        )
    conn = psycopg.connect(effective_dsn, autocommit=False)
    try:
        yield conn
    finally:
        conn.close()


def ensure_schema(conn) -> None:
    """Create the schema if it doesn't exist (idempotent)."""
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    conn.commit()


def _ms_to_iso(start_time_ms: Optional[int]) -> Optional[str]:
    if start_time_ms is None:
        return None
    import datetime as dt

    return dt.datetime.fromtimestamp(
        start_time_ms / 1000.0, tz=dt.timezone.utc
    ).isoformat()


def _coerce_test_name(test_name) -> Optional[str]:
    """``test_name`` arrives as either a str, a list of strs, or None."""
    if test_name is None:
        return None
    if isinstance(test_name, list):
        return ",".join(str(t) for t in test_name) if test_name else None
    return str(test_name)


def insert_run(
    conn,
    *,
    run_id: uuid.UUID,
    batch_id: Optional[uuid.UUID],
    iteration: Optional[int],
    test_name,
    branch: Optional[str],
    tag: Optional[str],
    commit_sha: Optional[str],
    arch: Optional[str],
    setup: Optional[str],
    deployment_type: Optional[str],
    triggering_env: Optional[str],
    start_time_ms: Optional[int],
    duration_s: Optional[int],
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO bench_run (
              run_id, batch_id, iteration, test, branch, tag, commit_sha,
              arch, setup, deployment_type, triggering_env, started_at,
              duration_s
            ) VALUES (
              %s, %s, %s, %s, %s, %s, %s,
              %s, %s, %s, %s, %s,
              %s
            )
            ON CONFLICT (run_id) DO UPDATE SET
              batch_id        = EXCLUDED.batch_id,
              iteration       = EXCLUDED.iteration,
              test            = EXCLUDED.test,
              branch          = EXCLUDED.branch,
              tag             = EXCLUDED.tag,
              commit_sha      = EXCLUDED.commit_sha,
              arch            = EXCLUDED.arch,
              setup           = EXCLUDED.setup,
              deployment_type = EXCLUDED.deployment_type,
              triggering_env  = EXCLUDED.triggering_env,
              started_at      = EXCLUDED.started_at,
              duration_s      = EXCLUDED.duration_s
            """,
            (
                str(run_id),
                str(batch_id) if batch_id is not None else None,
                iteration,
                _coerce_test_name(test_name),
                branch,
                tag,
                commit_sha,
                arch,
                setup,
                deployment_type,
                triggering_env,
                _ms_to_iso(start_time_ms),
                duration_s,
            ),
        )


def insert_metrics(
    conn, run_id: uuid.UUID, rows: Iterable[Tuple[str, str, float]]
) -> int:
    """Bulk-insert ``bench_run_metric`` rows via ``COPY``.

    ``rows`` yields ``(metric, context, value)`` tuples. Returns the count
    written. Tolerates duplicates by routing through a temp table + ``INSERT
    ... ON CONFLICT DO NOTHING`` (COPY itself doesn't support ON CONFLICT).
    """
    rows = list(rows)
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.execute(
            "CREATE TEMP TABLE IF NOT EXISTS _bench_run_metric_stage "
            "(metric text, context text, value double precision)"
        )
        cur.execute("TRUNCATE _bench_run_metric_stage")
        with cur.copy(
            "COPY _bench_run_metric_stage (metric, context, value) FROM STDIN"
        ) as copy:
            for metric, context, value in rows:
                copy.write_row((metric, context, value))
        cur.execute(
            """
            INSERT INTO bench_run_metric (run_id, metric, context, value)
            SELECT %s, metric, context, value FROM _bench_run_metric_stage
            ON CONFLICT (run_id, metric, context) DO UPDATE SET
              value = EXCLUDED.value
            """,
            (str(run_id),),
        )
    return len(rows)


def insert_samples(
    conn,
    run_id: uuid.UUID,
    rows: Iterable[Tuple[int, str, float]],
) -> int:
    """Bulk-insert ``bench_run_sample`` rows via ``COPY``.

    ``rows`` yields ``(ts_offset_s, metric, value)`` tuples.
    """
    rows = list(rows)
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.execute(
            "CREATE TEMP TABLE IF NOT EXISTS _bench_run_sample_stage "
            "(ts_offset_s int, metric text, value double precision)"
        )
        cur.execute("TRUNCATE _bench_run_sample_stage")
        with cur.copy(
            "COPY _bench_run_sample_stage (ts_offset_s, metric, value) FROM STDIN"
        ) as copy:
            for ts_offset_s, metric, value in rows:
                copy.write_row((ts_offset_s, metric, value))
        cur.execute(
            """
            INSERT INTO bench_run_sample (run_id, ts_offset_s, metric, value)
            SELECT %s, ts_offset_s, metric, value FROM _bench_run_sample_stage
            ON CONFLICT (run_id, ts_offset_s, metric) DO UPDATE SET
              value = EXCLUDED.value
            """,
            (str(run_id),),
        )
    return len(rows)


def summary_rows_from_results(metrics, results_dict):
    """Flatten ``results_dict`` into ``(metric, context, value)`` summary rows.

    Reuses ``extract_results_table`` so the metric set written to PG matches
    exactly what the RTS exporter writes — same jsonpaths, same numeric
    coercion, same skip-on-non-numeric logic.
    """
    cleaned = extract_results_table(metrics, results_dict)
    out = []
    for row in cleaned:
        # row layout: (metric_jsonpath, metric_context_path, metric_name,
        #              metric_value, test_case_targets_dict, use_metric_context_path)
        _, metric_context_path, metric_name, metric_value, _, _ = row
        out.append((metric_name, metric_context_path or "", float(metric_value)))
    return out


def postgres_test_success_flow(
    push_results_postgres: bool,
    *,
    run_id: Optional[uuid.UUID] = None,
    batch_id: Optional[uuid.UUID] = None,
    iteration: int = 1,
    test_name=None,
    benchmark_config=None,  # noqa: ARG001 -- accepted for API parity / future use
    metrics=None,
    results_dict=None,
    samples=None,
    branch: Optional[str] = None,
    tag: Optional[str] = None,
    commit_sha: Optional[str] = None,
    arch: Optional[str] = None,
    setup: Optional[str] = None,
    deployment_type: Optional[str] = None,
    triggering_env: Optional[str] = None,
    start_time_ms: Optional[int] = None,
    duration_s: Optional[int] = None,
    dsn: Optional[str] = None,
) -> Optional[uuid.UUID]:
    """Top-level entry point — mirrors ``timeseries_test_sucess_flow``.

    Returns the ``run_id`` written, or ``None`` when the exporter was disabled
    (no DSN, or push flag false). All connection/transaction management lives
    here so call sites stay one-liners.
    """
    if not push_results_postgres:
        return None
    effective_dsn = dsn if dsn is not None else _dsn()
    if not effective_dsn:
        logging.info(
            "PERFORMANCE_PG_DSN is empty — skipping Postgres export " "(test=%s).",
            _coerce_test_name(test_name),
        )
        return None

    if run_id is None:
        run_id = uuid.uuid4()
    if batch_id is None:
        batch_id = run_id

    summary_rows = []
    if metrics is not None and results_dict is not None:
        summary_rows = summary_rows_from_results(metrics, results_dict)

    sample_rows = list(samples) if samples is not None else []

    try:
        with connect(effective_dsn) as conn:
            ensure_schema(conn)
            insert_run(
                conn,
                run_id=run_id,
                batch_id=batch_id,
                iteration=iteration,
                test_name=test_name,
                branch=branch,
                tag=tag,
                commit_sha=commit_sha,
                arch=arch,
                setup=setup,
                deployment_type=deployment_type,
                triggering_env=triggering_env,
                start_time_ms=start_time_ms,
                duration_s=duration_s,
            )
            metric_count = insert_metrics(conn, run_id, summary_rows)
            sample_count = insert_samples(conn, run_id, sample_rows)
            conn.commit()
        logging.info(
            "Postgres export OK (run_id=%s, test=%s, summary_rows=%d, samples=%d).",
            run_id,
            _coerce_test_name(test_name),
            metric_count,
            sample_count,
        )
        return run_id
    except Exception as exc:  # pragma: no cover -- defensive logging
        logging.error(
            "Postgres export failed for run_id=%s (test=%s): %s",
            run_id,
            _coerce_test_name(test_name),
            exc,
        )
        # Don't crash the benchmark on a sink failure — RTS path may still succeed.
        return None
