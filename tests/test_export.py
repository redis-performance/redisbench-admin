import os
from unittest import TestCase
import redis
import argparse


from redisbench_admin.export.args import create_export_arguments
from redisbench_admin.export.common.common import get_timeserie_name
from redisbench_admin.export.export import (
    export_command_logic,
    export_opereto_csv_to_timeseries_dict,
)


class Test(TestCase):
    def test_get_timeserie_name(self):
        kv_array = [
            {"deployment-type": "docker-oss"},
            {"metric-name": "Overall Updates and Aggregates query q50 latency"},
        ]
        metric_name = get_timeserie_name(kv_array)
        expected_metric_name = "deployment-type=docker-oss:metric-name=overall_updates_and_aggregates_query_q50_latency"
        self.assertEqual(metric_name, expected_metric_name)


def test_export_command_logic():
    rts_host = os.getenv("RTS_DATASINK_HOST", None)
    rts_port = 16379
    rts_pass = ""
    if rts_host is None:
        return
    rts = redis.Redis(port=16379, host=rts_host)
    rts.ping()
    rts.flushall()
    parser = argparse.ArgumentParser(
        description="test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = create_export_arguments(parser)
    args = parser.parse_args(
        args=[
            "--benchmark-result-file",
            "./tests/test_data/memtier_benchmark_v1.3.0_result.json",
            "--exporter-spec-file",
            "./tests/test_data/common-properties-v0.5-memtier-previous-1.3.0.yml",
            "--redistimeseries_host",
            rts_host,
            "--redistimeseries_port",
            "{}".format(rts_port),
            "--redistimeseries_pass",
            "{}".format(rts_pass),
            "--deployment-type",
            "enterprise",
            "--deployment-name",
            "c5.4xlarge",
            "--test-name",
            "test-1",
            "--deployment-version",
            "6.2.0",
            "--github_repo",
            "redis",
            "--github_org",
            "redis",
            "--github_branch",
            "branch-feature-1",
            "--override-test-time",
            "2021-01-01 10:00:00",
        ]
    )
    try:
        export_command_logic(args, "tool", "v0")
    except SystemExit as e:
        assert e.code == 0


def test_export_command_logic():
    rts_host = os.getenv("RTS_DATASINK_HOST", None)
    rts_port = 16379
    rts_pass = ""
    if rts_host is None:
        return
    rts = redis.Redis(port=16379, host=rts_host)
    rts.ping()
    rts.flushall()
    parser = argparse.ArgumentParser(
        description="test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = create_export_arguments(parser)
    args = parser.parse_args(
        args=[
            "--benchmark-result-file",
            "./tests/test_data/result_report_1690282709.835491.json",
            "--exporter-spec-file",
            "./tests/test_data/common-properties-enterprise.yml",
            "--redistimeseries_host",
            rts_host,
            "--redistimeseries_port",
            "{}".format(rts_port),
            "--redistimeseries_pass",
            "{}".format(rts_pass),
            "--deployment-type",
            "enterprise",
            "--github_repo",
            "redis",
            "--github_org",
            "redis",
        ]
    )
    try:
        export_command_logic(args, "tool", "v0")
    except SystemExit as e:
        assert e.code == 0


def test_export_opereto_csv_to_timeseries_dict():
    break_by_dict = {"branch": "master", "version": "6.2.0"}
    benchmark_file = "./tests/test_data/2021-10-01.120753test_1sh_1wk_dual_ep_mixed_2thr_50conns_persistent_mtls_msetmget_kb_1p_csv_string.csv"
    results_dict = export_opereto_csv_to_timeseries_dict(
        benchmark_file,
        break_by_dict,
        1,
        "c5.re",
        "enterprise",
        {},
        "redis",
        "redis",
        "ci",
    )
    # break by = 2
    # 3 lines
    # 11 metrics
    assert len(results_dict.keys()) == 2 * 3 * 11


def test_export_command_logic_google_benchmark():
    rts_host = os.getenv("RTS_DATASINK_HOST", None)
    rts_port = 16379
    rts_pass = ""
    if rts_host is None:
        return
    rts = redis.Redis(port=16379, host=rts_host)
    rts.ping()
    rts.flushall()
    parser = argparse.ArgumentParser(
        description="test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = create_export_arguments(parser)
    args = parser.parse_args(
        args=[
            "--results-format",
            "google.benchmark",
            "--benchmark-result-file",
            "./tests/test_data/results/google.benchmark.json",
            "--redistimeseries_host",
            rts_host,
            "--redistimeseries_port",
            "{}".format(rts_port),
            "--redistimeseries_pass",
            "{}".format(rts_pass),
            "--github_branch",
            "branch-feature-1",
        ]
    )
    try:
        export_command_logic(args, "tool", "v0")
    except SystemExit as e:
        assert e.code == 0
