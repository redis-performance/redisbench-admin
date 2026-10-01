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
    get_run_full_filename,
    push_data_to_redistimeseries,
    extract_perversion_timeseries_from_results,
    extract_perbranch_timeseries_from_results,
    extract_perhash_timeseries_from_results,
    exporter_create_ts,
    get_overall_dashboard_keynames,
    common_timeseries_extraction,
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


def test_fetch_remote_setup_from_config_aarch64():
    architecture = "aarch64"
    path = None
    branch = "master"
    repo = "https://github.com/redis-performance/testing-infrastructure.git"
    terraform_working_dir, type, _ = fetch_remote_setup_from_config(
        [{"type": "oss-standalone"}, {"setup": "redisearch-m5"}],
        repo,
        branch,
        path,
        architecture,
    )
    assert type == "oss-standalone"
    assert terraform_working_dir.endswith("/oss-standalone-redisearch-m5-aarch64")


def test_push_data_to_redistimeseries():
    import os
    import pytest

    # Ensure we have the test DB to store results
    if "RTS_PORT" not in os.environ:
        pytest.skip("RTS_PORT environment variable not set")

    time_series_dict = {}
    rts_port = os.environ.get("RTS_PORT", None)
    try:
        rts = redis.Redis(port=rts_port)
        rts.ping()
        datapoint_errors, datapoint_inserts = push_data_to_redistimeseries(
            rts, time_series_dict
        )
        assert datapoint_errors == 0
        assert datapoint_inserts == 0
    except redis.exceptions.ConnectionError:
        pytest.skip("Could not connect to Redis")


def test_extract_perversion_timeseries_from_results():
    # default and specific metrics test
    with open("./tests/test_data/common-properties-v0.1.yml", "r") as yml_file:
        (
            default_kpis,
            default_remote,
            default_metrics,
            exporter_timemetric_path,
            default_specs,
            cluster_config,
        ) = process_default_yaml_properties_file(
            None, None, None, "1.yml", None, yml_file
        )
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

            timeseries_dict, _, _, _, _ = prepare_timeseries_dict(
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


def test_extract_perhash_timeseries_from_results():
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
            tf_github_sha = "deadbeefcafebabe0000000000000000deadbeef"
            tf_triggering_env = "gh"
            test_name = "redis-benchmark-full-suite-1Mkeys-100B"
            deployment_name = "oss-standalone"
            deployment_type = "oss-standalone"
            datapoints_timestamp = 1000
            (
                ok,
                per_hash_time_series_dict,
                target_tables,
            ) = extract_perhash_timeseries_from_results(
                datapoints_timestamp,
                metrics,
                results_dict,
                tf_github_sha,
                tf_github_org,
                tf_github_repo,
                deployment_name,
                deployment_type,
                test_name,
                tf_triggering_env,
            )
            assert ok is True
            assert (len(results_dict["Tests"].keys()) * len(metrics)) == len(
                per_hash_time_series_dict
            )
            # every series must be keyed under by.hash/<sha> and carry the sha label
            for ts_name, ts in per_hash_time_series_dict.items():
                assert "/by.hash/" in ts_name
                assert "/{}/".format(tf_github_sha) in ts_name
                assert ts["labels"]["github_sha"] == tf_github_sha
                assert ts["labels"]["hash"] == tf_github_sha


def test_prepare_timeseries_dict_with_github_sha():
    """End-to-end exercise of prepare_timeseries_dict with tf_github_sha
    -- confirms the per-hash branch of common_exporter_logic emits
    `by.hash/<sha>` keys on top of the branch/version ones."""
    with open("./tests/test_data/common-properties-v0.1.yml", "r") as yml_file:
        (
            _,
            _,
            default_metrics,
            exporter_timemetric_path,
            _,
            _,
        ) = process_default_yaml_properties_file(
            None, None, None, "1.yml", None, yml_file
        )
    with open(
        "./tests/test_data/tsbs-devops-ingestion-scale100-4days.yml", "r"
    ) as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
    with open(
        "./tests/test_data/tsbs_load_redistimeseries_result.json", "r"
    ) as json_file:
        results_dict = json.load(json_file)

    tf_github_sha = "abc1234deadbeef"
    timeseries_dict, _, _, _, hash_target_tables = prepare_timeseries_dict(
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
        tf_github_sha=tf_github_sha,
    )
    assert timeseries_dict is not None
    hash_keys = [k for k in timeseries_dict if "/by.hash/" in k]
    assert len(hash_keys) > 0
    for existing_metric in ["Totals.rowRate", "Totals.metricRate"]:
        assert (
            "ci.benchmarks.redislabs/by.hash/tf_triggering_env/tf_github_org/tf_github_repo/test_name/oss/oss-standalone/{}/{}".format(
                tf_github_sha, existing_metric
            )
            in timeseries_dict
        )
    for _, ts in timeseries_dict.items():
        assert ts["labels"].get("github_sha") == tf_github_sha
    assert hash_target_tables is not None
    assert len(hash_target_tables) > 0


def test_prepare_timeseries_dict_without_github_sha_skips_hash_keys():
    """Regression: omitting tf_github_sha must NOT emit by.hash keys."""
    with open("./tests/test_data/common-properties-v0.1.yml", "r") as yml_file:
        (
            _,
            _,
            default_metrics,
            exporter_timemetric_path,
            _,
            _,
        ) = process_default_yaml_properties_file(
            None, None, None, "1.yml", None, yml_file
        )
    with open(
        "./tests/test_data/tsbs-devops-ingestion-scale100-4days.yml", "r"
    ) as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
    with open(
        "./tests/test_data/tsbs_load_redistimeseries_result.json", "r"
    ) as json_file:
        results_dict = json.load(json_file)

    timeseries_dict, _, _, _, hash_target_tables = prepare_timeseries_dict(
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
    assert not any("/by.hash/" in k for k in timeseries_dict)
    for _, ts in timeseries_dict.items():
        assert "github_sha" not in ts["labels"]
    assert hash_target_tables == {} or hash_target_tables is None


def test_prepare_timeseries_dict_default_arch_x86_unchanged_keys():
    """Default arch is x86_64 → keys have no arch suffix, labels tag
    arch=x86_64. This is the pre-existing behavior and must be preserved."""
    with open("./tests/test_data/common-properties-v0.1.yml", "r") as yml_file:
        (
            _,
            _,
            default_metrics,
            exporter_timemetric_path,
            _,
            _,
        ) = process_default_yaml_properties_file(
            None, None, None, "1.yml", None, yml_file
        )
    with open(
        "./tests/test_data/tsbs-devops-ingestion-scale100-4days.yml", "r"
    ) as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
    with open(
        "./tests/test_data/tsbs_load_redistimeseries_result.json", "r"
    ) as json_file:
        results_dict = json.load(json_file)

    timeseries_dict, _, _, _, _ = prepare_timeseries_dict(
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
    assert not any("arch=" in k for k in timeseries_dict)
    for _, ts in timeseries_dict.items():
        assert ts["labels"]["arch"] == "x86_64"


def test_prepare_timeseries_dict_with_aarch64_arch_segregates_keys():
    """ARM benchmark pushed alongside the same benchmark name on x86 must
    NOT collide: keys carry an arch=aarch64 suffix and labels tag the
    running arch so Grafana can split them."""
    with open("./tests/test_data/common-properties-v0.1.yml", "r") as yml_file:
        (
            _,
            _,
            default_metrics,
            exporter_timemetric_path,
            _,
            _,
        ) = process_default_yaml_properties_file(
            None, None, None, "1.yml", None, yml_file
        )
    with open(
        "./tests/test_data/tsbs-devops-ingestion-scale100-4days.yml", "r"
    ) as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
    with open(
        "./tests/test_data/tsbs_load_redistimeseries_result.json", "r"
    ) as json_file:
        results_dict = json.load(json_file)

    timeseries_dict_x86, _, _, _, _ = prepare_timeseries_dict(
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
        arch="x86_64",
    )
    timeseries_dict_arm, _, _, _, _ = prepare_timeseries_dict(
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
        arch="aarch64",
    )

    # every ARM key must carry arch=aarch64 suffix; x86 keys must not
    for k in timeseries_dict_arm:
        assert k.endswith("arch=aarch64"), k
    for k in timeseries_dict_x86:
        assert "arch=" not in k, k

    # ARM ∩ x86 key sets must be disjoint — the whole point of this change
    assert set(timeseries_dict_x86).isdisjoint(set(timeseries_dict_arm))

    # labels carry the running arch on both sides
    for _, ts in timeseries_dict_arm.items():
        assert ts["labels"]["arch"] == "aarch64"
    for _, ts in timeseries_dict_x86.items():
        assert ts["labels"]["arch"] == "x86_64"


def test_exporter_create_ts():
    import os
    import pytest

    # Ensure we have the test DB to store results
    if "RTS_PORT" not in os.environ:
        pytest.skip("RTS_PORT environment variable not set")

    rts_port = os.environ.get("RTS_PORT", None)
    try:
        rts = redis.Redis(port=rts_port)
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
                {"arch": "x86_64", "os": "ubuntu:16.04", "compiler": "icc"},
            )
            initial_plus_update = {
                **initial_labels,
                "arch": "x86_64",
                "os": "ubuntu:16.04",
                "compiler": "icc",
            }
            assert initial_plus_update == rts.ts().info(ts_key).labels

    except redis.exceptions.ConnectionError:
        import pytest

        pytest.skip("Could not connect to Redis")


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


def test_exporter_create_ts_labels():
    import os
    import pytest

    # Ensure we have the test DB to store results
    if "RTS_PORT" not in os.environ:
        pytest.skip("RTS_PORT environment variable not set")

    timeseries_name = "ts1"
    time_series = {"labels": {"metric-type": "commandstats"}}
    rts_port = os.environ.get("RTS_PORT", None)
    try:
        rts = redis.Redis(port=rts_port)
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
        pytest.skip("Could not connect to Redis")


def test_get_run_full_filename_simple_branch():
    result = get_run_full_filename(
        start_time_str="2026-02-09-16-10-52",
        deployment_type="oss-standalone",
        github_org="RediSearch",
        github_repo="RediSearch",
        github_branch="master",
        test_name="bench1",
        github_sha="abc123",
    )
    assert result == (
        "2026-02-09-16-10-52-RediSearch-RediSearch-master"
        "-bench1-oss-standalone-abc123.json"
    )
    assert "/" not in result


def test_get_run_full_filename_branch_with_slash():
    """Regression test: branch names containing '/' must be sanitized so the
    resulting filename does not contain directory separators."""
    result = get_run_full_filename(
        start_time_str="2026-02-09-16-10-52",
        deployment_type="oss-standalone-threads-6",
        github_org="RediSearch",
        github_repo="RediSearch",
        github_branch="bd/hnsw-shared-lock",
        test_name="vecsim-arxiv-titles-384-angular-filters-m16-ef-128-tag-filter",
        github_sha="8727375b1c77b7f4bd509fb2105662b61fdc281d",
    )
    assert "/" not in result
    assert "bd-hnsw-shared-lock" in result
    assert result == (
        "2026-02-09-16-10-52-RediSearch-RediSearch-bd-hnsw-shared-lock"
        "-vecsim-arxiv-titles-384-angular-filters-m16-ef-128-tag-filter"
        "-oss-standalone-threads-6"
        "-8727375b1c77b7f4bd509fb2105662b61fdc281d.json"
    )


def test_get_run_full_filename_branch_with_multiple_slashes():
    """Branch names like 'feat/area/thing' should have all slashes replaced."""
    result = get_run_full_filename(
        start_time_str="2026-01-01-00-00-00",
        deployment_type="oss-standalone",
        github_org="org",
        github_repo="repo",
        github_branch="feat/area/thing",
        test_name="test",
        github_sha="deadbeef",
    )
    assert "/" not in result
    assert "feat-area-thing" in result


# --------------------------------------------------------------------------- #
# push_data_to_redistimeseries: two round trips, whatever the series count
# --------------------------------------------------------------------------- #
class _FakeInfo:
    def __init__(self, labels):
        self.labels = labels


class _FakePipeline:
    """Records what was queued and hands back programmed results."""

    def __init__(self, owner):
        self.owner = owner
        self.queued = []

    def info(self, key):
        self.queued.append(("info", key))
        return self

    def create(self, key, **kwargs):
        self.queued.append(("create", key, kwargs))
        return self

    def alter(self, key, **kwargs):
        self.queued.append(("alter", key, kwargs))
        return self

    def add(self, key, timestamp, value, **kwargs):
        self.queued.append(("add", key, timestamp, value))
        return self

    def pexpire(self, key, msecs):
        self.queued.append(("pexpire", key, msecs))
        return self

    def execute(self, raise_on_error=True):
        self.owner.executions.append(list(self.queued))
        results = []
        for cmd in self.queued:
            kind, key = cmd[0], cmd[1]
            if kind == "info":
                if key in self.owner.existing:
                    results.append(_FakeInfo(dict(self.owner.existing[key])))
                else:
                    results.append(
                        redis.exceptions.ResponseError("TSDB: the key does not exist")
                    )
            elif kind == "add" and key in self.owner.failing_adds:
                results.append(redis.exceptions.ResponseError("TSDB: rejected"))
            else:
                results.append(True)
        return results


class _FakeTSNamespace:
    def __init__(self, owner):
        self.owner = owner

    def pipeline(self, transaction=True):
        return _FakePipeline(self.owner)


class _FakeRTS:
    def __init__(self, existing=None, failing_adds=()):
        self.existing = existing or {}
        self.failing_adds = set(failing_adds)
        self.executions = []

    def ts(self):
        return _FakeTSNamespace(self)


def _series(labels, data):
    return {"labels": dict(labels), "data": dict(data)}


def test_push_uses_two_round_trips_regardless_of_series_count():
    for n_series in (1, 5, 40):
        tsd = {
            f"key/{i}": _series({"arch": "x86_64"}, {1000 + i: float(i)})
            for i in range(n_series)
        }
        rts = _FakeRTS()
        errors, inserts = push_data_to_redistimeseries(rts, tsd)
        assert errors == 0
        assert inserts == n_series
        # the whole point: cost does not scale with the number of series
        assert (
            len(rts.executions) == 2
        ), f"{n_series} series took {len(rts.executions)} round trips"


def test_push_creates_missing_and_skips_create_for_existing():
    labels = {"arch": "x86_64", "test_name": "t"}
    tsd = {
        "exists/1": _series(labels, {1000: 1.0}),
        "missing/1": _series(labels, {1000: 2.0}),
    }
    rts = _FakeRTS(existing={"exists/1": labels})
    errors, inserts = push_data_to_redistimeseries(rts, tsd)
    assert (errors, inserts) == (0, 2)
    writer = rts.executions[1]
    kinds = [(c[0], c[1]) for c in writer]
    assert ("create", "missing/1") in kinds
    assert ("create", "exists/1") not in kinds
    # matching labels must not trigger a needless TS.ALTER
    assert ("alter", "exists/1") not in kinds


def test_push_alters_when_labels_drifted():
    tsd = {"k": _series({"arch": "x86_64", "branch": "new"}, {1000: 1.0})}
    rts = _FakeRTS(existing={"k": {"arch": "x86_64", "branch": "old"}})
    push_data_to_redistimeseries(rts, tsd)
    kinds = [(c[0], c[1]) for c in rts.executions[1]]
    assert ("alter", "k") in kinds


def test_push_counts_failed_datapoints_rather_than_reporting_success():
    tsd = {
        "good": _series({}, {1000: 1.0}),
        "bad": _series({}, {1000: 2.0, 2000: 3.0}),
    }
    rts = _FakeRTS(failing_adds={"bad"})
    errors, inserts = push_data_to_redistimeseries(rts, tsd)
    assert errors == 2, "both rejected datapoints must be counted"
    assert inserts == 1


def test_push_drops_none_labels_and_qualifies_aarch64_keys():
    tsd = {
        "plain/key": _series({"arch": "aarch64", "empty": None}, {1000: 1.0}),
        "already/aarch64/key": _series({"arch": "aarch64"}, {1000: 1.0}),
    }
    rts = _FakeRTS()
    push_data_to_redistimeseries(rts, tsd)
    created = {c[1]: c[2]["labels"] for c in rts.executions[1] if c[0] == "create"}
    assert "plain/key/arch/aarch64" in created, created
    assert (
        "already/aarch64/key" in created
    ), "key already naming the arch must not be rewritten"
    assert "empty" not in created["plain/key/arch/aarch64"]


def test_push_auto_timestamp_sends_star_not_the_value():
    tsd = {"k": _series({}, {None: 7.5})}
    rts = _FakeRTS()
    errors, inserts = push_data_to_redistimeseries(rts, tsd)
    assert (errors, inserts) == (0, 1)
    adds = [c for c in rts.executions[1] if c[0] == "add"]
    assert adds == [("add", "k", "*", 7.5)], adds


def test_push_is_a_noop_without_client_or_data():
    assert push_data_to_redistimeseries(None, {"k": _series({}, {1: 1.0})}) == (0, 0)
    assert push_data_to_redistimeseries(_FakeRTS(), None) == (0, 0)
    rts = _FakeRTS()
    assert push_data_to_redistimeseries(rts, {}) == (0, 0)
    assert rts.executions == [], "an empty push must not talk to the database"
