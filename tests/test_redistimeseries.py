#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import json

import redis
import yaml
from redisbench_admin.utils.remote import get_overall_dashboard_keynames


from redisbench_admin.run.common import (
    merge_default_and_config_metrics,
    get_start_time_vars,
)
from redisbench_admin.run.redistimeseries import timeseries_test_sucess_flow


def test_timeseries_test_sucess_flow():
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
            len_metrics = len(metrics)
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
                    {
                        "arch": "amd64",
                        "os": "debian:8",
                        "compiler": "gcc",
                        "component": "search",
                    },
                    "build1",
                    "platform1",
                    None,
                    1,
                )

            assert rts.exists(testcases_and_metric_context_path_setname)
            assert rts.exists(testcases_metric_context_path_setname)
            assert rts.exists(deployment_name_setname)
            testcases_zsetname = testcases_setname + ":zset"
            testcases_zsetname_component_search = (
                testcases_setname + ":zset:component:search"
            )
            assert rts.exists(testcases_setname)
            assert rts.exists(testcases_zsetname)
            assert rts.zcard(testcases_zsetname)
            assert rts.zcard(testcases_zsetname_component_search)
            assert test_name.encode() in rts.zpopmin(testcases_zsetname)[0][0]
            assert (
                deployment_name.encode() in rts.zpopmin(deployment_name_setname)[0][0]
            )
            assert rts.exists(testcases_zsetname_component_search)
            assert (
                test_name.encode()
                in rts.zpopmin(testcases_zsetname_component_search)[0][0]
            )
            assert rts.exists(running_platforms_setname)
            assert rts.exists(build_variant_setname)

            assert "amd64".encode() in rts.smembers(project_archs_setname)
            assert "debian:8".encode() in rts.smembers(project_oss_setname)
            assert "gcc".encode() in rts.smembers(project_compilers_setname)
            assert project_version.encode() in rts.smembers(project_versions_setname)
            assert tf_github_branch.encode() in rts.smembers(project_branches_setname)
            assert "build1".encode() in rts.smembers(build_variant_setname)
            assert test_name.encode() in rts.smembers(testcases_setname)
            assert len(rts.smembers(testcases_setname)) == 1
            assert len(rts.smembers(project_branches_setname)) == 1
            assert len(rts.smembers(project_versions_setname)) == 1
            assert "platform1".encode() in rts.smembers(running_platforms_setname)
            assert len(rts.smembers(testcases_and_metric_context_path_setname)) == len(
                results_dict["Tests"].keys()
            )
            testcases_and_metric_context_path_members = [
                x.decode()
                for x in rts.smembers(testcases_and_metric_context_path_setname)
            ]
            metric_context_path_members = [
                x.decode() for x in rts.smembers(testcases_metric_context_path_setname)
            ]
            for metric_context_path in results_dict["Tests"].keys():
                assert (
                    "{}:{}".format(test_name, metric_context_path)
                    in testcases_and_metric_context_path_members
                )
            for metric_context_path in results_dict["Tests"].keys():
                assert metric_context_path in metric_context_path_members
            assert len(metric_context_path_members) == len(
                testcases_and_metric_context_path_members
            )
            assert [x.decode() for x in rts.smembers(testcases_setname)] == [test_name]
            # 2 (branch/version) x ( load time + test time  ) + project successes
            number_of_control_ts = 2 + 2 + 1
            # set with test names + per project tag sets ( os, branch, .... )
            number_of_control_redis = 10 + len_metrics

            keys = [x.decode() for x in rts.keys()]
            assert (
                len(results_dict["Tests"].keys()) * len(metrics)
                + number_of_control_redis
                + number_of_control_ts
            ) <= (len(keys))
            total_by_version = 0
            total_by_branch = 0
            total_metrics = 7
            for keyname in keys:
                if "by.branch" in keyname:
                    total_by_branch = total_by_branch + 1
                if "by.version" in keyname:
                    total_by_version = total_by_version + 1
            assert total_by_version > 0
            assert (
                total_by_version
                == len(results_dict["Tests"].keys()) * total_metrics + 2 + len_metrics
            )
            assert total_by_branch > 0
            assert (
                total_by_branch
                == len(results_dict["Tests"].keys()) * total_metrics + 2 + len_metrics
            )

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
                "build",
                "platform2",
            )
            assert "arm64".encode() in rts.smembers(project_archs_setname)
            assert "ubuntu:16.04".encode() in rts.smembers(project_oss_setname)
            assert "icc".encode() in rts.smembers(project_compilers_setname)
            assert "build".encode() in rts.smembers(build_variant_setname)
            assert "platform2".encode() in rts.smembers(running_platforms_setname)

            assert len(rts.smembers(project_archs_setname)) == 2
            assert len(rts.smembers(project_oss_setname)) == 2
            assert len(rts.smembers(project_compilers_setname)) == 2
            assert len(rts.smembers(build_variant_setname)) == 2
            assert len(rts.smembers(running_platforms_setname)) == 2
            assert len(rts.smembers(testcases_setname)) == 1
            assert len(rts.smembers(project_branches_setname)) == 1
            assert len(rts.smembers(project_versions_setname)) == 1

    except redis.exceptions.ConnectionError:
        pass
