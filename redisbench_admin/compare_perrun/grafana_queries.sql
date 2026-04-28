-- =====================================================================
-- Grafana panel queries for 1..N vs 1..N per-run comparison
-- Schema: bench_run + bench_run_sample (see redisbench_admin/run/postgres.py)
--
-- Variables expected on the dashboard (Grafana variable syntax):
--   $test       — single value (text), e.g. "ftsb-10K-enwiki_abstract-hashes-term-wildcard"
--   $metric     — single value (text), e.g. "rps", "lat_p99_us", "lat_p50_us"
--   $baseline   — branch or tag string (or use $baseline_branch / $baseline_tag pair)
--   $candidate  — branch or tag string
-- =====================================================================


-- ---------------------------------------------------------------------
-- Panel A — Per-iteration overlay (Time Series)
-- One line per (cohort, iteration). Use Grafana legend formatter
-- ${cohort}/it${iteration} so the 2N lines are easy to read.
-- ---------------------------------------------------------------------
SELECT
  s.ts_offset_s                                AS time,
  CASE WHEN r.branch = $baseline THEN 'baseline' ELSE 'candidate' END
    || '/it' || r.iteration                    AS metric,
  s.value
FROM bench_run r
JOIN bench_run_sample s ON s.run_id = r.run_id
WHERE r.test = $test
  AND s.metric = $metric
  AND r.branch IN ($baseline, $candidate)
ORDER BY r.iteration, s.ts_offset_s;


-- ---------------------------------------------------------------------
-- Panel B — Cohort median + p25/p75 band (Time Series, with Stat=median)
-- Two bands: baseline (median across all baseline iterations) and
-- candidate. Lets you see whether candidate's IQR overlaps baseline's.
-- ---------------------------------------------------------------------
SELECT
  s.ts_offset_s                                                       AS time,
  CASE WHEN r.branch = $baseline THEN 'baseline_p50' ELSE 'candidate_p50' END AS metric,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY s.value)                AS value
FROM bench_run r
JOIN bench_run_sample s ON s.run_id = r.run_id
WHERE r.test = $test
  AND s.metric = $metric
  AND r.branch IN ($baseline, $candidate)
GROUP BY 1, 2
ORDER BY 1;


-- ---------------------------------------------------------------------
-- Panel C — Per-iteration scalar summary (Bar Chart / Table)
-- One row per (cohort, iteration) with median + p99 of the iteration's
-- per-second series. Use this for the "stability matrix" panel.
-- ---------------------------------------------------------------------
SELECT
  CASE WHEN r.branch = $baseline THEN 'baseline' ELSE 'candidate' END AS cohort,
  r.iteration,
  r.commit_sha,
  r.arch,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY s.value) AS p50_of_run,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY s.value) AS p99_of_run,
  count(*) AS n_seconds
FROM bench_run r
JOIN bench_run_sample s ON s.run_id = r.run_id
WHERE r.test = $test
  AND s.metric = $metric
  AND r.branch IN ($baseline, $candidate)
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2;


-- ---------------------------------------------------------------------
-- Panel D — Pairwise % delta heatmap (Heatmap, single panel)
-- For each ts_offset_s, compute (cand_median - base_median) / base_median.
-- Drop the result on a horizontal time axis and you get a one-row strip
-- that turns red where candidate is worse, green where it's better.
-- ---------------------------------------------------------------------
WITH base AS (
  SELECT s.ts_offset_s,
         percentile_cont(0.5) WITHIN GROUP (ORDER BY s.value) AS v
  FROM bench_run r
  JOIN bench_run_sample s ON s.run_id = r.run_id
  WHERE r.test = $test AND s.metric = $metric AND r.branch = $baseline
  GROUP BY 1
),
cand AS (
  SELECT s.ts_offset_s,
         percentile_cont(0.5) WITHIN GROUP (ORDER BY s.value) AS v
  FROM bench_run r
  JOIN bench_run_sample s ON s.run_id = r.run_id
  WHERE r.test = $test AND s.metric = $metric AND r.branch = $candidate
  GROUP BY 1
)
SELECT base.ts_offset_s AS time,
       'pct_delta' AS metric,
       100.0 * (cand.v - base.v) / NULLIF(base.v, 0) AS value
FROM base JOIN cand USING (ts_offset_s)
ORDER BY 1;


-- ---------------------------------------------------------------------
-- Panel E — Test catalog (Variable query for $test)
-- Drives the dashboard's $test dropdown.
-- ---------------------------------------------------------------------
SELECT DISTINCT test
FROM bench_run
WHERE branch IN ($baseline, $candidate)
ORDER BY 1;


-- ---------------------------------------------------------------------
-- Panel F — Branch / tag list (Variable queries)
-- ---------------------------------------------------------------------
-- $baseline:
SELECT DISTINCT branch FROM bench_run WHERE branch IS NOT NULL ORDER BY 1;
-- (use the same query for $candidate, with its own variable.)


-- ---------------------------------------------------------------------
-- Panel G — Verdict header (Stat / Single Stat)
-- Re-compute the same verdict the CLI emits, in pure SQL, so the
-- dashboard shows GREEN/WARN/REGRESSION at a glance.
-- ---------------------------------------------------------------------
WITH per_iter AS (
  SELECT r.branch,
         r.iteration,
         percentile_cont(0.5) WITHIN GROUP (ORDER BY s.value) AS scalar
  FROM bench_run r
  JOIN bench_run_sample s ON s.run_id = r.run_id
  WHERE r.test = $test AND s.metric = $metric
    AND r.branch IN ($baseline, $candidate)
  GROUP BY 1, 2
),
agg AS (
  SELECT
    avg(scalar) FILTER (WHERE branch = $baseline)  AS base_mean,
    avg(scalar) FILTER (WHERE branch = $candidate) AS cand_mean,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY
      CASE WHEN branch = $candidate
           THEN scalar ELSE NULL END
    ) AS cand_med
  FROM per_iter
)
SELECT
  100.0 * (cand_mean - base_mean) / NULLIF(base_mean, 0) AS pct_change_mean,
  base_mean,
  cand_mean
FROM agg;
