#  BSD 3-Clause License
#
#  Copyright (c) 2026., Redis Performance Group
#  All rights reserved.
#
"""Golden-file tests for the per-second sample parsers.

These run with no external services — they read the existing memtier
fixture (``tests/test_data/memtier_benchmark_v1.3.1_result.json``) and a
synthetic ftsb fixture committed alongside this test, and assert the shape
+ values returned by ``postgres_parsers.parse_*_samples``.
"""
import json
import os

import pytest

from redisbench_admin.run.postgres_parsers import (
    detect_and_parse,
    merge_samples,
    parse_ftsb_json_samples,
    parse_memtier_json_samples,
)


HERE = os.path.dirname(__file__)
MEMTIER_V131 = os.path.join(HERE, "test_data", "memtier_benchmark_v1.3.1_result.json")
MEMTIER_V130 = os.path.join(HERE, "test_data", "memtier_benchmark_v1.3.0_result.json")
FTSB_SYNTHETIC = os.path.join(HERE, "test_data", "ftsb_synthetic_with_timeseries.json")


# ---------------------------------------------------------------------------
# memtier
# ---------------------------------------------------------------------------


def test_memtier_v131_parses_per_second_samples():
    samples = parse_memtier_json_samples(MEMTIER_V131)
    assert samples, "expected non-empty samples for v1.3.1 fixture"

    by_metric = {}
    for ts, metric, value in samples:
        by_metric.setdefault(metric, {})[ts] = value

    # v1.3.1 fixture has Totals.Time-Serie entries 0..10 → 11 buckets.
    assert sorted(by_metric.keys()) == sorted(
        ["rps", "lat_avg_us", "lat_p50_us", "lat_p99_us", "lat_p999_us"]
    )
    rps = by_metric["rps"]
    assert sorted(rps.keys()) == list(range(11))
    # First-second total Count == sum of Sets+Gets+Waits Count for sec 0.
    with open(MEMTIER_V131) as f:
        d = json.load(f)
    expected_count = d["ALL STATS"]["Totals"]["Time-Serie"]["0"]["Count"]
    assert rps[0] == pytest.approx(float(expected_count))

    # latency conversion ms→µs sanity check.
    p99_us_sec0 = by_metric["lat_p99_us"][0]
    p99_ms_sec0 = d["ALL STATS"]["Totals"]["Time-Serie"]["0"]["p99.00"]
    assert p99_us_sec0 == pytest.approx(p99_ms_sec0 * 1000.0)


def test_memtier_v130_returns_empty_no_per_second_data():
    """Older memtier didn't write Time-Serie — parser must not raise."""
    samples = parse_memtier_json_samples(MEMTIER_V130)
    assert samples == []


def test_memtier_parser_accepts_pre_loaded_dict():
    with open(MEMTIER_V131) as f:
        payload = json.load(f)
    samples = parse_memtier_json_samples(payload)
    assert samples


def test_memtier_parser_skips_non_dict_entries():
    """Defensive: malformed entry shouldn't blow up the whole parse."""
    payload = {
        "ALL STATS": {
            "Totals": {
                "Time-Serie": {
                    "0": {"Count": 100, "p99.00": 1.0},
                    "1": "garbage",
                    "bad-key": {"Count": 50},
                    "2": {"Count": 200, "p99.00": 1.5},
                }
            }
        }
    }
    samples = parse_memtier_json_samples(payload)
    seconds = sorted({ts for ts, _m, _v in samples if _m == "rps"})
    assert seconds == [0, 2]


# ---------------------------------------------------------------------------
# ftsb
# ---------------------------------------------------------------------------


def test_ftsb_parser_extracts_rate_and_quantiles():
    samples = parse_ftsb_json_samples(FTSB_SYNTHETIC)
    by_metric = {}
    for ts, metric, value in samples:
        by_metric.setdefault(metric, {})[ts] = value

    # The synthetic fixture has 5 seconds; offsets normalise to 0..4.
    assert sorted(by_metric["rps"].keys()) == [0, 1, 2, 3, 4]
    assert by_metric["rps"][0] == pytest.approx(9100.0)
    # ms → µs
    assert by_metric["lat_p99_us"][0] == pytest.approx(1.10 * 1000.0)
    assert by_metric["lat_p95_us"][0] == pytest.approx(0.62 * 1000.0)
    # All five quantile metrics should be present.
    assert {"rps", "lat_p50_us", "lat_p95_us", "lat_p99_us", "lat_p999_us"} <= set(
        by_metric.keys()
    )


def test_ftsb_parser_picks_first_populated_series():
    """When readTs is empty but writeTs has data, fall back to writeTs."""
    payload = {
        "TimeSeries": {
            "readTs": [],
            "writeTs": [
                {
                    "Timestamp": 1700000010,
                    "MultiValues": {"rate": 5000.0, "q99": 0.5},
                },
                {
                    "Timestamp": 1700000011,
                    "MultiValues": {"rate": 5100.0, "q99": 0.6},
                },
            ],
            "updateTs": [],
            "deleteTs": [],
            "readCursorTs": [],
        }
    }
    samples = parse_ftsb_json_samples(payload)
    rps = {ts: v for ts, m, v in samples if m == "rps"}
    assert rps == {0: 5000.0, 1: 5100.0}


def test_ftsb_parser_returns_empty_when_no_timeseries():
    assert parse_ftsb_json_samples({"TimeSeries": {}}) == []
    assert parse_ftsb_json_samples({}) == []


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------


def test_detect_and_parse_routes_to_memtier():
    out = detect_and_parse(MEMTIER_V131)
    assert any(metric == "rps" for _ts, metric, _v in out)


def test_detect_and_parse_routes_to_ftsb():
    out = detect_and_parse(FTSB_SYNTHETIC)
    assert any(metric == "rps" for _ts, metric, _v in out)


def test_detect_and_parse_swallows_bad_paths(tmp_path):
    # File doesn't exist → []
    assert detect_and_parse(str(tmp_path / "nope.json")) == []

    # File isn't JSON → []
    bad = tmp_path / "bad.json"
    bad.write_text("not valid json {")
    assert detect_and_parse(str(bad)) == []

    # Valid JSON but unknown shape → []
    unknown = tmp_path / "unknown.json"
    unknown.write_text(json.dumps({"hello": "world"}))
    assert detect_and_parse(str(unknown)) == []


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def test_merge_samples_resolves_collisions_last_wins():
    a = [(0, "rps", 1.0), (1, "rps", 2.0)]
    b = [(1, "rps", 99.0), (2, "rps", 3.0)]
    out = sorted(merge_samples(a, b))
    assert out == [(0, "rps", 1.0), (1, "rps", 99.0), (2, "rps", 3.0)]
