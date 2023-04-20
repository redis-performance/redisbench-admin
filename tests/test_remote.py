import json

import redis
import yaml

from redisbench_admin.run.redistimeseries import (
    prepare_timeseries_dict,
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
    common_timeseries_extraction,
    retrieve_tf_connection_vars,
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
    assert github_org_name == "redis-performance"
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
        ".", github_url="https://github.com/redis-performance/redisbench-admin"
    )
    assert github_org_name == "redis-performance"
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
        ".", github_url="https://github.com/redis-performance/redisbench-admin/"
    )
    assert github_org_name == "redis-performance"
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
        ".", github_url="git@github.com:redis-performance/redisbench-admin.git"
    )
    assert github_org_name == "redis-performance"
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
        rts = redis.Redis(port=16379)
        rts.ping()
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

            (timeseries_dict, _, _, _) = prepare_timeseries_dict(
                "1.0.0",
                benchmark_config,
                default_metrics,
                "oss-standalone",
                "oss",
                exporter_timemetric_path,
                results_dict,
                "test_name",
                "tf_github_branch",
                "tf_github_org",
                "tf_github_repo",
                "tf_triggering_env",
            )
            assert timeseries_dict is not None
            assert len(timeseries_dict.keys()) == 4
            for existing_metric in ["Totals.rowRate", "Totals.metricRate"]:
                assert (
                    "ci.benchmarks.redislabs/by.version/tf_triggering_env/tf_github_org/tf_github_repo/test_name/oss/oss-standalone/1.0.0/{}".format(
                        existing_metric
                    )
                    in timeseries_dict.keys()
                )
                assert (
                    "ci.benchmarks.redislabs/by.branch/tf_triggering_env/tf_github_org/tf_github_repo/test_name/oss/oss-standalone/tf_github_branch/{}".format(
                        existing_metric
                    )
                    in timeseries_dict.keys()
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
                _,
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
                _,
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
        rts = redis.Redis(port=16379)
        rts.ping()
        rts.flushall()
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
                    deployment_name_setname,
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
            initial_labels = rts.ts().info(ts_key).labels

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
            assert initial_plus_update == rts.ts().info(ts_key).labels

    except redis.exceptions.ConnectionError:
        pass


def test_common_timeseries_extraction():
    # v0.5 format
    # we're adding on purpose duplicate metrics to test for the de-duplication feature and the str vs dict feature
    metric_q50 = "Totals.overallQuantiles.all_queries.q50"
    t1_q50 = 7.18
    t2_q50 = 8.31
    self_q50 = 14.228
    metrics = [
        "$.{}".format(metric_q50),
        "$.{}".format(metric_q50),
        {
            "$.{}".format(metric_q50): {
                "target-1": t1_q50,
                "target-2": t2_q50,
            }
        },
        "$.Totals.overallQuantiles.all_queries.q100",
    ]
    results_dict = {
        "StartTime": 1631785523000,
        "EndTime": 1631785528000,
        "DurationMillis": 4933,
        "Totals": {
            "burnIn": 0,
            "limit": 0,
            "overallQuantiles": {
                "RedisTimeSeries_max_of_all_CPU_metrics_random_1_hosts_random_8h0m0s_by_1h": {
                    "q0": 0,
                    "q100": 39.285,
                    "q50": 14.228,
                    "q95": 28.045,
                    "q99": 33.075,
                    "q999": 36.537,
                },
                "all_queries": {
                    "q0": 0,
                    "q100": 39.285,
                    "q50": self_q50,
                    "q95": 28.045,
                    "q99": 33.075,
                    "q999": 36.537,
                },
            },
            "overallQueryRates": {
                "RedisTimeSeries_max_of_all_CPU_metrics_random_1_hosts_random_8h0m0s_by_1h": 2027.186427709223,
                "all_queries": 2027.186427709223,
            },
            "prewarmQueries": False,
        },
    }
    break_by_key = "branch"
    break_by_str = "by.{}".format(break_by_key)
    datapoints_timestamp = 1631785523000
    deployment_name = "oss-cluster-03-primaries"
    deployment_type = "oss-cluster"
    break_by_value = "master"
    test_name = "test1"
    tf_github_org = "redis"
    tf_github_repo = "redis"
    tf_triggering_env = "gh"

    timeseries_dict, _ = common_timeseries_extraction(
        break_by_key,
        break_by_str,
        datapoints_timestamp,
        deployment_name,
        deployment_type,
        metrics,
        break_by_value,
        results_dict,
        test_name,
        tf_github_org,
        tf_github_repo,
        tf_triggering_env,
    )
    # 3 series for q50, 1 serie for q100 (given there is no target there)
    assert len(timeseries_dict.keys()) == 4
    prefix = "ci.benchmarks.redislabs/by.branch/gh/redis/redis/test1/oss-cluster/oss-cluster-03-primaries/master/"
    key_self_q50 = "{}{}".format(prefix, metric_q50)
    key_self_t1 = "{}{}/target/target-1".format(prefix, metric_q50)
    key_self_t2 = "{}{}/target/target-2".format(prefix, metric_q50)
    assert timeseries_dict[key_self_q50]["data"] == {datapoints_timestamp: self_q50}
    assert timeseries_dict[key_self_q50]["labels"]["target+branch"] == "{} {}".format(
        break_by_value, tf_github_repo
    )
    assert timeseries_dict[key_self_t1]["data"] == {datapoints_timestamp: t1_q50}
    assert timeseries_dict[key_self_t1]["labels"]["target+branch"] == "{} {}".format(
        break_by_value, "target-1"
    )
    assert timeseries_dict[key_self_t2]["data"] == {datapoints_timestamp: t2_q50}
    assert timeseries_dict[key_self_t2]["labels"]["target+branch"] == "{} {}".format(
        break_by_value, "target-2"
    )


def test_exporter_create_ts():
    timeseries_name = "ts1"
    time_series = {"labels": {"metric-type": "commandstats"}}
    try:
        rts = redis.Redis(port=16379)
        rts.ping()
        rts.flushall()
        assert True == exporter_create_ts(rts, time_series, timeseries_name)
        assert rts.exists(timeseries_name)
        # no update
        assert False == exporter_create_ts(rts, time_series, timeseries_name)
        # change existing label
        time_series["labels"]["metric-type"] = "latencystats"
        assert True == exporter_create_ts(rts, time_series, timeseries_name)
        # add new label
        time_series["labels"]["metric-name"] = "latency_usecs"
        assert True == exporter_create_ts(rts, time_series, timeseries_name)
        # no update
        assert False == exporter_create_ts(rts, time_series, timeseries_name)

    except redis.exceptions.ConnectionError:
        pass


def test_retrieve_tf_connection_vars():
    tf_output = {
        "client_private_ip": {
            "sensitive": False,
            "type": ["tuple", ["string"]],
            "value": ["10.3.0.235"],
        },
        "client_public_ip": {
            "sensitive": False,
            "type": ["tuple", ["string"]],
            "value": ["3.135.206.198"],
        },
        "server_private_ip": {
            "sensitive": False,
            "type": ["tuple", ["string"]],
            "value": ["10.3.0.53"],
        },
        "server_public_ip": {
            "sensitive": False,
            "type": ["tuple", ["string"]],
            "value": ["18.219.10.142"],
        },
    }
    (
        tf_return_code,
        username,
        server_private_ip,
        server_public_ip,
        server_plaintext_port,
        client_private_ip,
        client_public_ip,
    ) = retrieve_tf_connection_vars(None, tf_output)
    assert server_private_ip == "10.3.0.53"
    assert server_public_ip == "18.219.10.142"
    assert username == "ubuntu"

    tf_output_new = {
        "client_private_ip": {
            "sensitive": False,
            "type": ["tuple", [["tuple", ["string"]]]],
            "value": [["10.3.0.175"]],
        },
        "client_public_ip": {
            "sensitive": False,
            "type": ["tuple", [["tuple", ["string"]]]],
            "value": [["3.136.234.93"]],
        },
        "server_private_ip": {
            "sensitive": False,
            "type": ["tuple", [["tuple", ["string", "string", "string"]]]],
            "value": [["10.3.0.236", "10.3.0.9", "10.3.0.211"]],
        },
        "server_public_ip": {
            "sensitive": False,
            "type": ["tuple", [["tuple", ["string", "string", "string"]]]],
            "value": [["3.143.24.7", "13.58.158.80", "3.139.82.224"]],
        },
        "ssh_user": {"sensitive": False, "type": "string", "value": "ec2"},
    }
    (
        tf_return_code,
        username,
        server_private_ip,
        server_public_ip,
        server_plaintext_port,
        client_private_ip,
        client_public_ip,
    ) = retrieve_tf_connection_vars(None, tf_output_new)
    assert server_private_ip == ["10.3.0.236", "10.3.0.9", "10.3.0.211"]
    assert server_public_ip == ["3.143.24.7", "13.58.158.80", "3.139.82.224"]
    assert username == "ec2"
