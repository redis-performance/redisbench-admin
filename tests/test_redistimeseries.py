#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import json

import redis
import yaml
from redisbench_admin.utils.remote import get_overall_dashboard_keynames
from redistimeseries.client import Client

from redisbench_admin.run.common import (
    merge_default_and_config_metrics,
    get_start_time_vars,
)
from redisbench_admin.run.redistimeseries import timeseries_test_sucess_flow


def test_timeseries_test_sucess_flow():
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
                datapoints_timestamp = 1000
                (
                    prefix,
                    testcases_setname,
                    tsname_project_total_failures,
                    tsname_project_total_success,
                    _,
                    _,
                ) = get_overall_dashboard_keynames(
                    tf_github_org, tf_github_repo, tf_triggering_env
                )
                benchmark_duration_seconds = 60
                dataset_load_duration_seconds = 0
                tsname_project_total_success = 1
                _, start_time_ms, testcase_start_time_str = get_start_time_vars()

                timeseries_test_sucess_flow(
                    True,
                    project_version,
                    benchmark_config,
                    benchmark_duration_seconds,
                    dataset_load_duration_seconds,
                    metrics,
                    deployment_type,
                    merged_exporter_timemetric_path,
                    results_dict,
                    rts,
                    start_time_ms,
                    test_name,
                    testcases_setname,
                    tf_github_branch,
                    tf_github_org,
                    tf_github_repo,
                    tf_triggering_env,
                    tsname_project_total_success,
                )
            assert rts.redis.exists(testcases_setname)
            assert [x.decode() for x in rts.redis.smembers(testcases_setname)] == [
                test_name
            ]
            # 2 (branch/version) x ( load time + test time  ) + project successes
            number_of_control_ts = 2 + 2 + 1
            # set with test names
            number_of_control_redis = 1

            keys = [x.decode() for x in rts.redis.keys()]
            assert (
                len(results_dict["Tests"].keys()) * len(metrics)
                + number_of_control_redis
                + number_of_control_ts
            ) == (len(keys))
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
                == len(results_dict["Tests"].keys()) * total_metrics + 2
            )
            assert total_by_branch > 0
            assert (
                total_by_branch == len(results_dict["Tests"].keys()) * total_metrics + 2
            )
    except redis.exceptions.ConnectionError:
        pass
