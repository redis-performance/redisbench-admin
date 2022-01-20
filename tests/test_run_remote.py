#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import os

import redis
from redisbench_admin.run.metrics import collect_redis_metrics
from redistimeseries.client import Client

from redisbench_admin.run_remote.run_remote import export_redis_metrics


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
            assert False
        rts = Client(port=rts_port, host=rts_host)
        rts.redis.ping()
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

        time_ms, _, overall_end_time_metrics = collect_redis_metrics([rts.redis])
        artifact_version = "6.2.3"
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
        assert (
            rts.info(
                "ci.benchmarks.redislabs/env/org/repo/test1/by.version/6.2.3/benchmark_end/commandstats_cmdstat_ping_calls"
            ).labels["metric-type"]
            == "test-tag"
        )
        assert (
            "ci.benchmarks.redislabs/env/org/repo/test1/by.version/6.2.3/benchmark_end/commandstats_cmdstat_ping_calls"
            in rts.queryindex(["metric-type=test-tag"])
        )
        assert datapoint_errors == 0
        assert datapoint_inserts == (1 * len(list(overall_end_time_metrics.keys())))
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
