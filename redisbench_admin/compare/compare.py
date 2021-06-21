#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import logging

import redis

from redisbench_admin.utils.remote import get_overall_dashboard_keynames
from redistimeseries.client import Client

from redisbench_admin.utils.utils import get_ts_metric_name


def compare_command_logic(args):
    logging.info("Checking connection to RedisTimeSeries.")
    rts = Client(
        host=args.redistimeseries_host,
        port=args.redistimeseries_port,
        password=args.redistimeseries_pass,
    )
    rts.redis.ping()

    tf_github_org = args.github_org
    tf_github_repo = args.github_repo
    tf_triggering_env = args.triggering_env
    deployment_type = args.deployment_type
    from_ts_ms = args.from_timestamp
    to_ts_ms = args.to_timestamp
    baseline_branch = args.baseline_branch
    comparison_branch = args.comparison_branch

    (
        prefix,
        testcases_setname,
        tsname_project_total_failures,
        tsname_project_total_success,
    ) = get_overall_dashboard_keynames(tf_github_org, tf_github_repo, tf_triggering_env)
    test_names = []
    try:
        test_names = rts.redis.smembers(testcases_setname)
    except redis.exceptions.ResponseError as e:
        logging.warning(
            "Error while updating secondary data structures {}. ".format(e.__str__())
        )
        pass

    logging.info(test_names)
    for test_name in test_names:
        metric_name = "Tests.Overall.rps"

        test_name = test_name.decode()
        ts_name_baseline = get_ts_metric_name(
            "by.branch",
            baseline_branch,
            tf_github_org,
            tf_github_repo,
            deployment_type,
            test_name,
            tf_triggering_env,
            metric_name,
        )
        ts_name_comparison = get_ts_metric_name(
            "by.branch",
            comparison_branch,
            tf_github_org,
            tf_github_repo,
            deployment_type,
            test_name,
            tf_triggering_env,
            metric_name,
        )
        baseline_v = "N/A"
        comparison_v = "N/A"
        try:

            baseline_datapoints = rts.revrange(
                ts_name_baseline, from_ts_ms, to_ts_ms, count=1
            )
            if len(baseline_datapoints) > 0:
                _, baseline_v = baseline_datapoints[0]
            comparison_datapoints = rts.revrange(
                ts_name_comparison, from_ts_ms, to_ts_ms, count=1
            )
            if len(comparison_datapoints) > 0:
                _, comparison_v = comparison_datapoints[0]
        except redis.exceptions.ResponseError:
            pass
        logging.info("{} | {} | {}".format(test_name, baseline_v, comparison_v))
