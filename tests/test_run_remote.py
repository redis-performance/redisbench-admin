import yaml

from redisbench_admin.run_remote.run_remote import (
    extract_module_semver_from_info_modules_cmd,
    redistimeseries_results_logic,
    merge_default_and_config_metrics,
    get_test_s3_bucket_path,
)
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
        ) = process_default_yaml_properties_file(None, None, "1.yml", None, yml_file)
        assert exporter_timemetric_path == "$.StartTime"
        merged_exporter_timemetric_path, metrics = merge_default_and_config_metrics(
            {}, default_metrics, exporter_timemetric_path
        )
        assert merged_exporter_timemetric_path == exporter_timemetric_path
        assert default_metrics == metrics

    # default and specific metrics test
    with open("./tests/test_data/common-properties-v0.1.yml", "r") as yml_file:
        (
            default_kpis,
            default_metrics,
            exporter_timemetric_path,
        ) = process_default_yaml_properties_file(None, None, "1.yml", None, yml_file)
        assert exporter_timemetric_path == "$.StartTime"
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
        testcases_setname,
        tsname_project_total_failures,
        tsname_project_total_success,
    ) = get_overall_dashboard_keynames("org", "repo", "env")
    assert "ci.benchmarks.redislabs/env/org/repo:testcases" == testcases_setname
    assert (
        "ci.benchmarks.redislabs/env/org/repo:total_success"
        == tsname_project_total_success
    )
    assert (
        "ci.benchmarks.redislabs/env/org/repo:total_failures"
        == tsname_project_total_failures
    )
