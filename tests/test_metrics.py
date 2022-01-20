#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import json
import os

import yaml
from redistimeseries.client import Client

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
    rts_host = os.getenv("RTS_DATASINK_HOST", None)
    rts_port = 16379
    if rts_host is None:
        assert False
    rts = Client(port=rts_port, host=rts_host)
    rts.redis.ping()
    time_ms, metrics_arr, overall_metrics = collect_redis_metrics([rts.redis])
    assert len(metrics_arr) == 1
    assert len(metrics_arr[0].keys()) == 3
    assert "cpu" in metrics_arr[0].keys()
    assert "memory" in metrics_arr[0].keys()
    assert "commandstats" in metrics_arr[0].keys()
    assert "allocator_active" in metrics_arr[0]["memory"]
    assert "cmdstat_ping" in metrics_arr[0]["commandstats"]
    allocator_active = metrics_arr[0]["memory"]["allocator_active"]
    allocator_active_kv = overall_metrics["memory_allocator_active"]
    assert allocator_active == allocator_active_kv

    _, metrics_arr, overall_metrics = collect_redis_metrics([rts.redis, rts.redis])
    allocator_active_kv = overall_metrics["memory_allocator_active"]
    assert (2 * allocator_active) == allocator_active_kv
    assert "cmdstat_ping" in metrics_arr[0]["commandstats"]
    assert "cmdstat_ping" in metrics_arr[1]["commandstats"]
    assert "commandstats_cmdstat_ping_calls_shard_1" in overall_metrics
    assert "commandstats_cmdstat_ping_calls_shard_2" in overall_metrics
