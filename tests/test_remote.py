import json

import redis
import yaml
from redistimeseries.client import Client

from redisbench_admin.run.redistimeseries import redistimeseries_results_logic
from redisbench_admin.run.common import merge_default_and_config_metrics
from redisbench_admin.utils.benchmark_config import process_default_yaml_properties_file
from redisbench_admin.utils.remote import (
    extract_git_vars,
    fetch_remote_setup_from_config,
    push_data_to_redistimeseries,
)


def test_extract_git_vars():
    (
        github_org_name,
        github_repo_name,
        github_sha,
        github_actor,
        github_branch,
        github_branch_detached,
    ) = extract_git_vars(".")
    assert github_org_name == "RedisLabsModules"
    assert github_repo_name == "redisbench-admin"
    assert github_sha != None and github_branch != ""
    if github_branch_detached is False:
        assert github_actor != None and github_branch != ""
        assert github_branch != None and github_branch != ""


def test_extract_git_vars_passing_repo():
    (
        github_org_name,
        github_repo_name,
        github_sha,
        github_actor,
        github_branch,
        github_branch_detached,
    ) = extract_git_vars(
        ".", github_url="https://github.com/RedisLabsModules/redisbench-admin"
    )
    assert github_org_name == "RedisLabsModules"
    assert github_repo_name == "redisbench-admin"
    assert github_sha != None and github_branch != ""
    if github_branch_detached is False:
        assert github_actor != None and github_branch != ""
        assert github_branch != None and github_branch != ""


def test_extract_git_vars_passing_repo2():
    (
        github_org_name,
        github_repo_name,
        github_sha,
        github_actor,
        github_branch,
        github_branch_detached,
    ) = extract_git_vars(
        ".", github_url="https://github.com/RedisLabsModules/redisbench-admin/"
    )
    assert github_org_name == "RedisLabsModules"
    assert github_repo_name == "redisbench-admin"
    assert github_sha != None and github_branch != ""
    if github_branch_detached is False:
        assert github_actor != None and github_branch != ""
        assert github_branch != None and github_branch != ""


def test_extract_git_vars_passing_repo3():
    (
        github_org_name,
        github_repo_name,
        github_sha,
        github_actor,
        github_branch,
        github_branch_detached,
    ) = extract_git_vars(
        ".", github_url="git@github.com:RedisLabsModules/redisbench-admin.git"
    )
    assert github_org_name == "RedisLabsModules"
    assert github_repo_name == "redisbench-admin"
    assert github_sha != None and github_branch != ""
    if github_branch_detached is False:
        assert github_actor != None and github_branch != ""
        assert github_branch != None and github_branch != ""


def test_fetch_remote_setup_from_config():
    terraform_working_dir, type, _ = fetch_remote_setup_from_config(
        [{"type": "oss-standalone"}, {"setup": "redistimeseries-m5d"}]
    )
    assert type == "oss-standalone"


def test_push_data_to_redistimeseries():
    time_series_dict = {}
    try:
        rts = Client()
        rts.redis.ping()
    except redis.exceptions.ConnectionError:
        pass
    finally:
        datapoint_errors, datapoint_inserts = push_data_to_redistimeseries(
            rts, time_series_dict
        )
        assert datapoint_errors == 0
        assert datapoint_inserts == 0


def test_extract_perversion_timeseries_from_results():
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
            "./tests/test_data/tsbs-devops-ingestion-scale100-4days.yml", "r"
        ) as yml_file:
            benchmark_config = yaml.safe_load(yml_file)
            merged_exporter_timemetric_path, metrics = merge_default_and_config_metrics(
                benchmark_config, default_metrics, exporter_timemetric_path
            )
            assert merged_exporter_timemetric_path == "$.StartTime"
            assert "$.Totals.metricRate" in metrics
            assert "$.Totals.rowRate" in metrics
            for m in default_metrics:
                assert m in metrics
        with open(
            "./tests/test_data/tsbs_load_redistimeseries_result.json", "r"
        ) as json_file:
            results_dict = json.load(json_file)

            (
                per_version_time_series_dict,
                per_branch_time_series_dict,
            ) = redistimeseries_results_logic(
                "N/A",
                benchmark_config,
                default_metrics,
                "oss",
                exporter_timemetric_path,
                results_dict,
                None,
                "test_name",
                "tf_github_branch",
                "tf_github_org",
                "tf_github_repo",
                "tf_triggering_env",
            )
            assert per_version_time_series_dict is not None
            assert len(per_version_time_series_dict.keys()) == 2
            for existing_metric in ["Totals.rowRate", "Totals.metricRate"]:
                assert (
                    "ci.benchmarks.redislabs/by.version/tf_triggering_env/tf_github_org/tf_github_repo/test_name/oss/N/A/{}".format(
                        existing_metric
                    )
                    in per_version_time_series_dict.keys()
                )
            assert per_branch_time_series_dict is not None
            assert len(per_branch_time_series_dict.keys()) == 2
            for existing_metric in ["Totals.rowRate", "Totals.metricRate"]:
                assert (
                    "ci.benchmarks.redislabs/by.branch/tf_triggering_env/tf_github_org/tf_github_repo/test_name/oss/tf_github_branch/{}".format(
                        existing_metric
                    )
                    in per_branch_time_series_dict.keys()
                )
