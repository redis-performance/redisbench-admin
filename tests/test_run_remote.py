#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import os

import redis
import yaml

from redisbench_admin.run.metrics import collect_redis_metrics

from redisbench_admin.run_remote.run_remote import (
    export_redis_metrics,
)


def test_export_redis_metrics():
    end_time_ms = 1
    overall_end_time_metrics = {}
    setup_name = "setup_name"
    test_name = "test1"
    tf_github_branch = None
    tf_github_org = "org"
    tf_github_repo = "repo"
    tf_triggering_env = "env"
    setup_type = "oss-standalone"
    artifact_version = None
    try:
        rts_host = os.getenv("RTS_DATASINK_HOST", None)
        rts_port = 16379
        if rts_host is None:
            return
        rts = redis.Redis(port=rts_port, host=rts_host)
        rts.ping()
        datapoint_errors, datapoint_inserts = export_redis_metrics(
            artifact_version,
            end_time_ms,
            overall_end_time_metrics,
            rts,
            setup_name,
            setup_type,
            test_name,
            tf_github_branch,
            tf_github_org,
            tf_github_repo,
            tf_triggering_env,
        )
        assert datapoint_errors == 0
        assert datapoint_inserts == 0
        tf_github_branch = ""
        artifact_version = ""
        datapoint_errors, datapoint_inserts = export_redis_metrics(
            artifact_version,
            end_time_ms,
            overall_end_time_metrics,
            rts,
            setup_name,
            setup_type,
            test_name,
            tf_github_branch,
            tf_github_org,
            tf_github_repo,
            tf_triggering_env,
        )
        assert datapoint_errors == 0
        assert datapoint_inserts == 0

        time_ms, _, overall_end_time_metrics = collect_redis_metrics([rts])
        artifact_version = "6.2.3"
        tf_github_branch = "master"
        datapoint_errors, datapoint_inserts = export_redis_metrics(
            artifact_version,
            time_ms,
            overall_end_time_metrics,
            rts,
            setup_name,
            setup_type,
            test_name,
            tf_github_branch,
            tf_github_org,
            tf_github_repo,
            tf_triggering_env,
            {"metric-type": "test-tag"},
        )
        labels_rts_cmdstats = (
            rts.ts()
            .info(
                "ci.benchmarks.redislabs/env/org/repo/test1/by.version/6.2.3/benchmark_end/setup_name/commandstats_cmdstat_ping_calls"
            )
            .labels
        )
        assert labels_rts_cmdstats["metric-type"] == "test-tag"
        assert labels_rts_cmdstats["command"] == "ping"
        assert labels_rts_cmdstats["command_and_setup"] == "ping - setup_name"
        assert (
            labels_rts_cmdstats["command_and_metric_and_setup"]
            == "ping - calls - setup_name"
        )
        assert (
            labels_rts_cmdstats["command_and_metric_and_setup_and_version"]
            == "ping - calls - setup_name - 6.2.3"
        )
        assert labels_rts_cmdstats["metric"] == "calls"
        assert labels_rts_cmdstats["shard"] == "1"
        assert labels_rts_cmdstats["metric_and_shard"] == "calls"

        labels_rts_cmdstats = (
            rts.ts()
            .info(
                "ci.benchmarks.redislabs/env/org/repo/test1/by.version/6.2.3/benchmark_end/setup_name/commandstats_cmdstat_ping_calls"
            )
            .labels
        )
        assert labels_rts_cmdstats["metric-type"] == "test-tag"
        assert labels_rts_cmdstats["command"] == "ping"
        assert labels_rts_cmdstats["command_and_setup"] == "ping - setup_name"
        assert (
            labels_rts_cmdstats["command_and_metric_and_setup"]
            == "ping - calls - setup_name"
        )
        assert (
            labels_rts_cmdstats["command_and_metric_and_setup_and_version"]
            == "ping - calls - setup_name - 6.2.3"
        )
        assert labels_rts_cmdstats["metric"] == "calls"
        assert labels_rts_cmdstats["shard"] == "1"
        assert labels_rts_cmdstats["metric_and_shard"] == "calls"

        # by branch
        labels_rts_cmdstats = (
            rts.ts()
            .info(
                "ci.benchmarks.redislabs/env/org/repo/test1/by.branch/master/benchmark_end/setup_name/commandstats_cmdstat_ping_calls"
            )
            .labels
        )
        assert labels_rts_cmdstats["metric-type"] == "test-tag"
        assert labels_rts_cmdstats["command"] == "ping"
        assert labels_rts_cmdstats["command_and_setup"] == "ping - setup_name"
        assert (
            labels_rts_cmdstats["command_and_metric_and_setup"]
            == "ping - calls - setup_name"
        )
        assert (
            labels_rts_cmdstats["command_and_metric_and_setup_and_branch"]
            == "ping - calls - setup_name - master"
        )
        assert labels_rts_cmdstats["metric"] == "calls"
        assert labels_rts_cmdstats["shard"] == "1"
        assert labels_rts_cmdstats["metric_and_shard"] == "calls"

        #
        assert "ci.benchmarks.redislabs/env/org/repo/test1/by.version/6.2.3/benchmark_end/setup_name/commandstats_cmdstat_ping_calls" in rts.ts().queryindex(
            ["metric-type=test-tag"]
        )
        assert "ci.benchmarks.redislabs/env/org/repo/test1/by.version/6.2.3/benchmark_end/setup_name/commandstats_cmdstat_ping_calls" in rts.ts().queryindex(
            ["command=ping"]
        )
        assert "ci.benchmarks.redislabs/env/org/repo/test1/by.version/6.2.3/benchmark_end/setup_name/latencystats_latency_percentiles_usec_ping_p50" in rts.ts().queryindex(
            ["command=ping"]
        )
        labels_rts_latencystats = (
            rts.ts()
            .info(
                "ci.benchmarks.redislabs/env/org/repo/test1/by.version/6.2.3/benchmark_end/setup_name/latencystats_latency_percentiles_usec_ping_p50"
            )
            .labels
        )
        assert labels_rts_latencystats["metric-type"] == "test-tag"
        assert labels_rts_latencystats["command"] == "ping"
        assert labels_rts_latencystats["command_and_setup"] == "ping - setup_name"
        assert labels_rts_latencystats["metric"] == "p50"
        assert labels_rts_latencystats["shard"] == "1"
        assert labels_rts_latencystats["metric_and_shard"] == "p50"
        assert datapoint_errors == 0
        assert datapoint_inserts == (2 * len(list(overall_end_time_metrics.keys())))
        tf_github_branch = "master"
        datapoint_errors, datapoint_inserts = export_redis_metrics(
            artifact_version,
            time_ms,
            overall_end_time_metrics,
            rts,
            setup_name,
            setup_type,
            test_name,
            tf_github_branch,
            tf_github_org,
            tf_github_repo,
            tf_triggering_env,
        )
        assert datapoint_errors == 0
        assert datapoint_inserts == (2 * len(list(overall_end_time_metrics.keys())))

    except redis.exceptions.ConnectionError:
        pass
