#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#


redis_benchmark_metrics_definition = [
    {
        "step": "benchmark",
        "metric-family": "throughput",
        "metric-csv-col": "rps",
        "metric-name": "Overall commands per second",
        "unit": "commands/sec",
        "metric-type": "numeric",
        "comparison": "higher-better",
        "per-step-comparison-metric-priority": 1,
    },
    {
        "step": "benchmark",
        "metric-family": "latency",
        "metric-csv-col": "avg_latency_ms",
        "metric-name": "Overall average latency ms ( including RTT )",
        "unit": "ms",
        "metric-type": "numeric",
        "comparison": "lower-better",
        "per-step-comparison-metric-priority": 2,
    },
    {
        "step": "benchmark",
        "metric-family": "latency",
        "metric-csv-col": "min_latency_ms",
        "metric-name": "Overall minimum latency ms ( including RTT )",
        "unit": "ms",
        "metric-type": "numeric",
        "comparison": "lower-better",
        "per-step-comparison-metric-priority": None,
    },
    {
        "step": "benchmark",
        "metric-family": "latency",
        "metric-csv-col": "max_latency_ms",
        "metric-name": "Overall maximum latency ms ( including RTT )",
        "unit": "ms",
        "metric-type": "numeric",
        "comparison": "lower-better",
        "per-step-comparison-metric-priority": None,
    },
    {
        "step": "benchmark",
        "metric-family": "latency",
        "metric-csv-col": "p50_latency_ms",
        "metric-name": "Overall percentile 50 latency ms ( including RTT )",
        "unit": "ms",
        "metric-type": "numeric",
        "comparison": "lower-better",
        "per-step-comparison-metric-priority": None,
    },
    {
        "step": "benchmark",
        "metric-family": "latency",
        "metric-csv-col": "p95_latency_ms",
        "metric-name": "Overall percentile 95 latency ms ( including RTT )",
        "unit": "ms",
        "metric-type": "numeric",
        "comparison": "lower-better",
        "per-step-comparison-metric-priority": None,
    },
    {
        "step": "benchmark",
        "metric-family": "latency",
        "metric-csv-col": "p99_latency_ms",
        "metric-name": "Overall percentile 99 latency ms ( including RTT )",
        "unit": "ms",
        "metric-type": "numeric",
        "comparison": "lower-better",
        "per-step-comparison-metric-priority": None,
    },
]
