#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import json
import os
from unittest.mock import MagicMock

import redis
import yaml


from redisbench_admin.run.common import merge_default_and_config_metrics
from redisbench_admin.run.metrics import extract_results_table, collect_redis_metrics


def test_extract_results_table():
    with open(
        "./tests/test_data/redis-benchmark-full-suite-1Mkeys-100B.yml", "r"
    ) as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        merged_exporter_timemetric_path, metrics = merge_default_and_config_metrics(
            benchmark_config, None, None
        )
        with open(
            "./tests/test_data/results/oss-standalone-2021-07-23-16-15-12-71d4528-redis-benchmark-full-suite-1Mkeys-100B.json",
            "r",
        ) as json_file:
            results_dict = json.load(json_file)
            extract_results_table(
                metrics,
                results_dict,
            )


def test_collect_redis_metrics():
    import os
    import pytest

    rts_host = os.getenv("RTS_DATASINK_HOST", None)
    # Ensure we have the test DB to store results
    if "RTS_PORT" not in os.environ:
        pytest.skip("RTS_PORT environment variable not set")
    rts_port = os.environ.get("RTS_PORT", None)
    if rts_host is None:
        pytest.skip("RTS_DATASINK_HOST environment variable not set")
    rts = redis.Redis(port=rts_port, host=rts_host)
    rts.ping()
    time_ms, metrics_arr, overall_metrics = collect_redis_metrics([rts])
    assert len(metrics_arr) == 1
    assert len(metrics_arr[0].keys()) == 4
    assert "cpu" in metrics_arr[0].keys()
    assert "memory" in metrics_arr[0].keys()
    assert "commandstats" in metrics_arr[0].keys()
    assert "latencystats" in metrics_arr[0].keys()
    assert "allocator_active" in metrics_arr[0]["memory"]
    assert "cmdstat_ping" in metrics_arr[0]["commandstats"]
    allocator_active = metrics_arr[0]["memory"]["allocator_active"]
    allocator_active_kv = overall_metrics["memory_allocator_active"]
    assert allocator_active == allocator_active_kv

    _, metrics_arr, overall_metrics = collect_redis_metrics([rts, rts])
    assert "memory_allocator_active" in overall_metrics
    assert "cmdstat_ping" in metrics_arr[0]["commandstats"]
    assert "cmdstat_ping" in metrics_arr[1]["commandstats"]
    assert "latency_percentiles_usec_ping" in metrics_arr[0]["latencystats"]
    assert "latency_percentiles_usec_ping" in metrics_arr[1]["latencystats"]
    assert "commandstats_cmdstat_ping_calls_shard_1" in overall_metrics
    assert "commandstats_cmdstat_ping_calls_shard_2" in overall_metrics
    assert "latencystats_latency_percentiles_usec_ping_p50_shard_1" in overall_metrics
    assert "latencystats_latency_percentiles_usec_ping_p50_shard_2" in overall_metrics


def test_collect_redis_metrics_section_filter_drops_scalar_keys():
    """The section_filter is meant to restrict which INFO keys make it into
    the overall map. Prior to this test it was silently broken for scalar
    (int/float) values due to Python operator precedence on
    `if collect and type(v) is float or type(v) is int` -- the `or` binds
    last, so the `collect` flag was effectively ignored whenever v was an
    int, and filtered-out keys leaked through. Lock in the intended
    semantics: only keys listed in section_filter[section] end up in the
    flat overall dict; the rest are dropped."""
    conn = MagicMock()

    def _info(section):
        return {
            "section_a": {"keep_me": 10, "drop_me": 20, "also_drop": 30.5},
            "section_b": {"included_b": 100, "excluded_b": 200},
        }.get(section, {})

    conn.info.side_effect = _info
    _, _, overall = collect_redis_metrics(
        [conn],
        sections=["section_a", "section_b"],
        section_filter={
            "section_a": ["keep_me"],
            "section_b": ["included_b"],
        },
    )
    assert "section_a_keep_me" in overall
    assert overall["section_a_keep_me"] == 10
    assert "section_b_included_b" in overall
    assert overall["section_b_included_b"] == 100
    # The filtered-out keys must not appear in the overall dict.
    assert "section_a_drop_me" not in overall
    assert "section_a_also_drop" not in overall
    assert "section_b_excluded_b" not in overall


def test_collect_redis_metrics_no_filter_keeps_all_scalars():
    """When section_filter is None, every scalar INFO key is collected --
    this is the legacy default and the common code path. Regression check
    to ensure the operator-precedence fix didn't accidentally flip the
    default to 'filter everything out'."""
    conn = MagicMock()
    conn.info.side_effect = lambda section: (
        {"alpha": 1, "beta": 2, "gamma": 3} if section == "section_a" else {}
    )
    _, _, overall = collect_redis_metrics([conn], sections=["section_a"])
    assert overall["section_a_alpha"] == 1
    assert overall["section_a_beta"] == 2
    assert overall["section_a_gamma"] == 3
