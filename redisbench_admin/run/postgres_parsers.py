#  BSD 3-Clause License
#
#  Copyright (c) 2026., Redis Performance Group
#  All rights reserved.
#
"""Per-second sample extractors for the Postgres exporter.

Each parser converts an existing client-side artifact (memtier JSON,
ftsb JSON) into the ``(ts_offset_s, metric, value)`` tuples consumed by
``redisbench_admin.run.postgres.insert_samples``.

The metric vocabulary is intentionally narrow and aligned across tools so
``compare-perrun`` can do paired diffs:

    - ``rps``           — operations/second
    - ``lat_p50_us``    — p50 latency in microseconds
    - ``lat_p95_us``    — p95 latency in microseconds (ftsb only — memtier
                          doesn't emit p95 in its time-serie JSON)
    - ``lat_p99_us``    — p99 latency in microseconds
    - ``lat_p999_us``   — p99.9 latency in microseconds
    - ``lat_avg_us``    — average latency in microseconds
"""
import json
import logging
import os
from typing import Iterable, List, Tuple

Sample = Tuple[int, str, float]


# ---------------------------------------------------------------------------
# memtier_benchmark
# ---------------------------------------------------------------------------

# memtier reports latency in milliseconds; multiply by 1000 to get µs.
_MS_TO_US = 1000.0


def _memtier_latency_keys():
    """Map memtier per-second keys → our metric vocabulary (µs)."""
    return [
        ("Average Latency", "lat_avg_us"),
        ("p50.00", "lat_p50_us"),
        ("p99.00", "lat_p99_us"),
        ("p99.90", "lat_p999_us"),
    ]


def parse_memtier_json_samples(payload) -> List[Sample]:
    """Extract per-second samples from a memtier_benchmark JSON result.

    Reads ``ALL STATS.Totals.Time-Serie`` (memtier_benchmark v1.3.1+) — the
    aggregate across all SET/GET/etc. ops, since stability comparison should
    track total throughput, not a per-command split.

    Returns ``[]`` for older memtier versions that don't emit Time-Serie.
    """
    if isinstance(payload, (str, bytes, os.PathLike)):
        with open(payload) as fh:
            payload = json.load(fh)

    all_stats = payload.get("ALL STATS")
    if not isinstance(all_stats, dict):
        logging.debug(
            "memtier JSON missing 'ALL STATS' — older format (v1.3.0 and below) "
            "doesn't emit per-second samples; returning []"
        )
        return []
    totals = all_stats.get("Totals")
    if not isinstance(totals, dict):
        return []
    time_serie = totals.get("Time-Serie")
    if not isinstance(time_serie, dict):
        return []

    out: List[Sample] = []
    for sec_key, entry in time_serie.items():
        try:
            ts = int(sec_key)
        except (TypeError, ValueError):
            continue
        if not isinstance(entry, dict):
            continue
        count = entry.get("Count")
        if isinstance(count, (int, float)):
            # 1-second bucket → Count == ops/sec.
            out.append((ts, "rps", float(count)))
        for src, dest in _memtier_latency_keys():
            v = entry.get(src)
            if isinstance(v, (int, float)):
                out.append((ts, dest, float(v) * _MS_TO_US))
    return out


# ---------------------------------------------------------------------------
# ftsb_redisearch
# ---------------------------------------------------------------------------

# ftsb publishes latency as ``hist.ValueAtQuantile(p) / 10e2`` — i.e. raw
# microseconds divided by 1000 → milliseconds. We multiply by 1000 to recover µs.
_FTSB_QUANTILE_MAP = {
    "q50": "lat_p50_us",
    "q95": "lat_p95_us",
    "q99": "lat_p99_us",
    "q999": "lat_p999_us",
}

# The ftsb result file groups time series by op-type; we collapse to a single
# stream by preferring the most-relevant series for the run. ``readTs`` for
# query workloads, ``writeTs`` for ingest, and so on. If multiple are populated
# we union them — same ``Timestamp`` gets the last writer (rare in practice).
_FTSB_SERIES_PRIORITY = ("readTs", "writeTs", "updateTs", "deleteTs", "readCursorTs")


def parse_ftsb_json_samples(payload) -> List[Sample]:
    """Extract per-second samples from an ftsb_redisearch JSON result.

    Reads ``TimeSeries.<seriesName>`` arrays of
    ``{Timestamp, MultiValues: {rate, q50, q95, q99, q999}}``.

    The ``Timestamp`` is unix seconds; we normalise to ``ts_offset_s`` from
    the first non-empty point so the output is comparable to memtier's
    bucket-index format (``0..N-1``).
    """
    if isinstance(payload, (str, bytes, os.PathLike)):
        with open(payload) as fh:
            payload = json.load(fh)

    ts_block = payload.get("TimeSeries")
    if not isinstance(ts_block, dict):
        return []

    chosen_series = None
    for name in _FTSB_SERIES_PRIORITY:
        series = ts_block.get(name)
        if isinstance(series, list) and series:
            chosen_series = series
            break
    if chosen_series is None:
        return []

    timestamps = []
    for point in chosen_series:
        ts = point.get("Timestamp") if isinstance(point, dict) else None
        if isinstance(ts, (int, float)):
            timestamps.append(int(ts))
    if not timestamps:
        return []
    base_ts = min(timestamps)

    out: List[Sample] = []
    for point in chosen_series:
        if not isinstance(point, dict):
            continue
        ts = point.get("Timestamp")
        mv = point.get("MultiValues") or {}
        if not isinstance(ts, (int, float)) or not isinstance(mv, dict):
            continue
        offset = int(ts) - base_ts
        if "rate" in mv and isinstance(mv["rate"], (int, float)):
            out.append((offset, "rps", float(mv["rate"])))
        for src, dest in _FTSB_QUANTILE_MAP.items():
            v = mv.get(src)
            if isinstance(v, (int, float)):
                out.append((offset, dest, float(v) * _MS_TO_US))
    return out


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------


def detect_and_parse(path: str) -> List[Sample]:
    """Sniff a result file and return per-second samples.

    Falls through to ``[]`` if the file isn't JSON or doesn't match a known
    shape — a noisy log entry but no exception, since the PG sink is best-
    effort.
    """
    try:
        with open(path) as fh:
            payload = json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        logging.warning("Could not load %s for per-second extraction: %s", path, exc)
        return []

    if isinstance(payload, dict):
        if "ALL STATS" in payload:
            return parse_memtier_json_samples(payload)
        if "TimeSeries" in payload and isinstance(payload["TimeSeries"], dict):
            return parse_ftsb_json_samples(payload)
    logging.info(
        "Result file %s did not match memtier or ftsb per-second shape; "
        "no samples extracted",
        path,
    )
    return []


def merge_samples(*streams: Iterable[Sample]) -> List[Sample]:
    """Concatenate sample streams; later streams win on ``(ts, metric)`` collision."""
    seen = {}
    for stream in streams:
        for ts, metric, value in stream:
            seen[(ts, metric)] = value
    return [(ts, metric, value) for (ts, metric), value in seen.items()]
