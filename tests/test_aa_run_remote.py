import argparse
import os

import yaml

from redisbench_admin.run_remote.args import create_run_remote_arguments
from redisbench_admin.run_remote.remote_helpers import (
    extract_module_semver_from_info_modules_cmd,
)
from redisbench_admin.run.s3 import get_test_s3_bucket_path
from redisbench_admin.run.tsbs_run_queries_redistimeseries.tsbs_run_queries_redistimeseries import (
    extract_tsbs_extra_links,
)
from redisbench_admin.run.common import merge_default_and_config_metrics
from redisbench_admin.run_remote.run_remote import run_remote_command_logic
from redisbench_admin.utils.benchmark_config import process_default_yaml_properties_file
from redisbench_admin.utils.remote import get_overall_dashboard_keynames


def test_extract_module_semver_from_info_modules_cmd():
    stdout = b"# Modules\r\nmodule:name=search,ver=999999,api=1,filters=0,usedby=[],using=[],options=[]\r\n".decode()
    module_name, semver = extract_module_semver_from_info_modules_cmd(stdout)
    assert semver[0] == "999999"
    assert module_name[0] == "search"
    module_name, semver = extract_module_semver_from_info_modules_cmd(b"")
    assert semver == []
    assert module_name == []


def test_redistimeseries_results_logic():
    # redistimeseries_results_logic(
    #     artifact_version,
    #     benchmark_config,
    #     default_metrics,
    #     deployment_type,
    #     exporter_timemetric_path,
    #     results_dict,
    #     rts,
    #     test_name,
    #     tf_github_branch,
    #     tf_github_org,
    #     tf_github_repo,
    #     tf_triggering_env,
    # )
    pass


def test_merge_default_and_config_metrics():
    # default metrics only test
    with open("./tests/test_data/common-properties-v0.1.yml", "r") as yml_file:
        (
            default_kpis,
            default_metrics,
            exporter_timemetric_path,
            default_specs,
            cluster_config,
        ) = process_default_yaml_properties_file(None, None, "1.yml", None, yml_file)
        assert exporter_timemetric_path == "$.StartTime"
        merged_exporter_timemetric_path, metrics = merge_default_and_config_metrics(
            {}, default_metrics, exporter_timemetric_path
        )
        assert merged_exporter_timemetric_path == exporter_timemetric_path
        assert default_metrics == metrics
        assert default_specs == None

    # default and specific metrics test
    with open("./tests/test_data/common-properties-v0.1.yml", "r") as yml_file:
        (
            default_kpis,
            default_metrics,
            exporter_timemetric_path,
            default_specs,
            cluster_config,
        ) = process_default_yaml_properties_file(None, None, "1.yml", None, yml_file)
        assert exporter_timemetric_path == "$.StartTime"
        assert default_specs == None
        with open(
            "./tests/test_data/redis-benchmark-with-exporter.yml", "r"
        ) as yml_file:
            benchmark_config = yaml.safe_load(yml_file)
            merged_exporter_timemetric_path, metrics = merge_default_and_config_metrics(
                benchmark_config, default_metrics, exporter_timemetric_path
            )
            assert merged_exporter_timemetric_path == "$.ST"
            assert "$.Tests.Overall.METRIC1" in metrics
            for m in default_metrics:
                assert m in metrics


def test_get_test_s3_bucket_path():
    bucket_path = get_test_s3_bucket_path("ci.bench", "test1", "org", "repo")
    assert "org/repo/results/test1/" == bucket_path


def test_get_overall_dashboard_keynames():
    (
        prefix,
        testcases_setname,
        deployment_name_setname,
        tsname_project_total_failures,
        tsname_project_total_success,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
    ) = get_overall_dashboard_keynames("org", "repo", "env")
    assert "ci.benchmarks.redislabs/env/org/repo:testcases" == testcases_setname
    assert "ci.benchmarks.redislabs/env/org/repo" == prefix
    assert (
        "ci.benchmarks.redislabs/env/org/repo:total_success"
        == tsname_project_total_success
    )
    assert (
        "ci.benchmarks.redislabs/env/org/repo:total_failures"
        == tsname_project_total_failures
    )
    (
        prefix,
        testcases_setname,
        deployment_name_setname,
        tsname_project_total_failures,
        tsname_project_total_success,
        running_platforms_setname,
        testcases_build_variant_setname,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
    ) = get_overall_dashboard_keynames(
        "org",
        "repo",
        "env",
        "build-1",
    )
    assert "ci.benchmarks.redislabs/env/org/repo:testcases" == testcases_setname
    assert "ci.benchmarks.redislabs/env/org/repo:platforms" == running_platforms_setname
    assert (
        "ci.benchmarks.redislabs/env/org/repo:build_variants"
        == testcases_build_variant_setname
    )
    assert "ci.benchmarks.redislabs/env/org/repo/build-1" == prefix
    assert (
        "ci.benchmarks.redislabs/env/org/repo/build-1:total_success"
        == tsname_project_total_success
    )
    assert (
        "ci.benchmarks.redislabs/env/org/repo/build-1:total_failures"
        == tsname_project_total_failures
    )


def test_extract_tsbs_extra_links():
    with open(
        "./tests/test_data/tsbs-devops-ingestion-scale100-4days-keyspace.yml", "r"
    ) as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        queries_file_link, remote_tool_link, tool_link = extract_tsbs_extra_links(
            benchmark_config, "tsbs_load_redistimeseries"
        )
        assert (
            queries_file_link
            == "https://s3.amazonaws.com/benchmarks.redislabs/redistimeseries/tsbs/datasets/devops/scale100/data_redistimeseries_cpu-only_100.dat"
        )
        assert remote_tool_link == "/tmp/tsbs_load_redistimeseries"
        assert tool_link == (
            "https://s3.amazonaws.com/benchmarks.redislabs/redistimeseries/tools/tsbs/tsbs_load_redistimeseries_linux_amd64"
        )


def test_run_remote_command_logic():
    private_key = "./tests/test_data/test-ssh/tox_rsa"
    parser = argparse.ArgumentParser(
        description="test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = create_run_remote_arguments(parser)
    db_server_ip = os.getenv("DB_SERVER_HOST", None)
    client_server_ip = os.getenv("CLIENT_SERVER_HOST", None)
    if db_server_ip is None or client_server_ip is None:
        assert False
    args = parser.parse_args(
        args=[
            "--inventory",
            "server_private_ip={},server_public_ip={},client_public_ip={}".format(
                db_server_ip, db_server_ip, client_server_ip
            ),
            "--db_ssh_port",
            "2222",
            "--client_ssh_port",
            "222",
            "--test",
            "./tests/test_data/redis-benchmark-vanilla.yml",
            "--skip-env-vars-verify",
            "--private_key",
            private_key,
        ]
    )
    # run_remote_command_logic(args, "tool", "v0")
