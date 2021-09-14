import json

import redis
import yaml
from redistimeseries.client import Client

from redisbench_admin.run.redistimeseries import (
    redistimeseries_results_logic,
    timeseries_test_sucess_flow,
)
from redisbench_admin.run.common import (
    merge_default_and_config_metrics,
    get_start_time_vars,
)
from redisbench_admin.utils.benchmark_config import process_default_yaml_properties_file
from redisbench_admin.utils.remote import (
    extract_git_vars,
    fetch_remote_setup_from_config,
    push_data_to_redistimeseries,
    extract_perversion_timeseries_from_results,
    extract_perbranch_timeseries_from_results,
    exporter_create_ts,
    get_overall_dashboard_keynames,
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
                _,
            ) = redistimeseries_results_logic(
                "1.0.0",
                benchmark_config,
                default_metrics,
                "oss-standalone",
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
                    "ci.benchmarks.redislabs/by.version/tf_triggering_env/tf_github_org/tf_github_repo/test_name/oss/oss-standalone/1.0.0/{}".format(
                        existing_metric
                    )
                    in per_version_time_series_dict.keys()
                )
            assert per_branch_time_series_dict is not None
            assert len(per_branch_time_series_dict.keys()) == 2
            for existing_metric in ["Totals.rowRate", "Totals.metricRate"]:
                assert (
                    "ci.benchmarks.redislabs/by.branch/tf_triggering_env/tf_github_org/tf_github_repo/test_name/oss/oss-standalone/tf_github_branch/{}".format(
                        existing_metric
                    )
                    in per_branch_time_series_dict.keys()
                )


def test_extract_timeseries_from_results():
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
            tf_github_org = "redis"
            tf_github_repo = "redis"
            tf_github_branch = "unstable"
            project_version = "6.2.4"
            tf_triggering_env = "gh"
            test_name = "redis-benchmark-full-suite-1Mkeys-100B"
            deployment_name = "oss-standalone"
            deployment_type = "oss-standalone"
            datapoints_timestamp = 1000
            # extract per branch datapoints
            (
                ok,
                per_version_time_series_dict,
            ) = extract_perversion_timeseries_from_results(
                datapoints_timestamp,
                metrics,
                results_dict,
                project_version,
                tf_github_org,
                tf_github_repo,
                deployment_name,
                deployment_type,
                test_name,
                tf_triggering_env,
            )
            assert ok == True
            assert (len(results_dict["Tests"].keys()) * len(metrics)) == len(
                per_version_time_series_dict.keys()
            )

            # extract per branch datapoints
            (
                ok,
                per_branch_time_series_dict,
            ) = extract_perbranch_timeseries_from_results(
                datapoints_timestamp,
                metrics,
                results_dict,
                tf_github_branch,
                tf_github_org,
                tf_github_repo,
                deployment_name,
                deployment_type,
                test_name,
                tf_triggering_env,
            )
            assert ok == True
            assert (len(results_dict["Tests"].keys()) * len(metrics)) == len(
                per_branch_time_series_dict.keys()
            )


def test_exporter_create_ts():
    try:
        rts = Client()
        rts.redis.ping()
        rts.redis.flushall()
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
                tf_github_org = "redis"
                tf_github_repo = "redis"
                tf_github_branch = "unstable"
                project_version = "6.2.4"
                tf_triggering_env = "gh"
                test_name = "redis-benchmark-full-suite-1Mkeys-100B"
                deployment_type = "oss-standalone"
                deployment_name = "oss-standalone"
                datapoints_timestamp = 1000
                (
                    prefix,
                    testcases_setname,
                    tsname_project_total_failures,
                    tsname_project_total_success,
                    running_platforms_setname,
                    build_variant_setname,
                    testcases_metric_context_path_setname,
                    testcases_and_metric_context_path_setname,
                    project_archs_setname,
                    project_oss_setname,
                    project_branches_setname,
                    project_versions_setname,
                    project_compilers_setname,
                ) = get_overall_dashboard_keynames(
                    tf_github_org,
                    tf_github_repo,
                    tf_triggering_env,
                    "build1",
                    "platform1",
                    test_name,
                )
                benchmark_duration_seconds = 60
                dataset_load_duration_seconds = 0
                _, start_time_ms, testcase_start_time_str = get_start_time_vars()

                timeseries_test_sucess_flow(
                    True,
                    project_version,
                    benchmark_config,
                    benchmark_duration_seconds,
                    dataset_load_duration_seconds,
                    metrics,
                    deployment_name,
                    deployment_type,
                    merged_exporter_timemetric_path,
                    results_dict,
                    rts,
                    start_time_ms,
                    test_name,
                    tf_github_branch,
                    tf_github_org,
                    tf_github_repo,
                    tf_triggering_env,
                    {},
                )
            ts_key = "ci.benchmarks.redislabs/by.branch/gh/redis/redis/redis-benchmark-full-suite-1Mkeys-100B/oss-standalone/unstable/max_latency_ms/RPOP"
            initial_labels = rts.info(ts_key).labels

            # test again and change some metadata
            timeseries_test_sucess_flow(
                True,
                project_version,
                benchmark_config,
                benchmark_duration_seconds,
                dataset_load_duration_seconds,
                metrics,
                deployment_name,
                deployment_type,
                merged_exporter_timemetric_path,
                results_dict,
                rts,
                start_time_ms,
                test_name,
                tf_github_branch,
                tf_github_org,
                tf_github_repo,
                tf_triggering_env,
                {"arch": "arm64", "os": "ubuntu:16.04", "compiler": "icc"},
            )
            initial_plus_update = {
                **initial_labels,
                "arch": "arm64",
                "os": "ubuntu:16.04",
                "compiler": "icc",
            }
            assert initial_plus_update == rts.info(ts_key).labels

    except redis.exceptions.ConnectionError:
        pass
