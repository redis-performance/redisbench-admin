#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import logging

import redis
from pytablewriter import MarkdownTableWriter

from redisbench_admin.utils.remote import get_overall_dashboard_keynames
from redistimeseries.client import Client

from redisbench_admin.utils.utils import get_ts_metric_name


def compare_command_logic(args, project_name, project_version):
    logging.info(
        "Using: {project_name} {project_version}".format(
            project_name=project_name, project_version=project_version
        )
    )
    logging.info(
        "Checking connection to RedisTimeSeries with user: {}, host: {}, port: {}".format(
            args.redistimeseries_user,
            args.redistimeseries_host,
            args.redistimeseries_port,
        )
    )
    rts = Client(
        host=args.redistimeseries_host,
        port=args.redistimeseries_port,
        password=args.redistimeseries_pass,
        username=args.redistimeseries_user,
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
    metric_name = args.metric_name
    metric_mode = args.metric_mode
    (
        prefix,
        testcases_setname,
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
    ) = get_overall_dashboard_keynames(tf_github_org, tf_github_repo, tf_triggering_env)
    test_names = []
    try:
        test_names = rts.redis.smembers(testcases_setname)
        test_names = list(test_names)
        test_names.sort()
    except redis.exceptions.ResponseError as e:
        logging.warning(
            "Error while trying to fetch test cases set (key={}) {}. ".format(
                testcases_setname, e.__str__()
            )
        )
        pass

    logging.warning(
        "Based on test-cases set (key={}) we have {} distinct benchmarks. ".format(
            testcases_setname, len(test_names)
        )
    )
    profilers_artifacts_matrix = []
    detected_regressions = []
    total_improvements = 0
    total_stable = 0
    total_regressions = 0
    for test_name in test_names:

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
        percentage_change = "N/A"
        percentage_change = 0.0
        if baseline_v != "N/A" and comparison_v != "N/A":
            if metric_mode == "higher-better":
                percentage_change = (
                    float(comparison_v) / float(baseline_v) - 1
                ) * 100.0
            else:
                # lower-better
                percentage_change = (
                    float(baseline_v) / float(comparison_v) - 1
                ) * 100.0
        if baseline_v != "N/A" or comparison_v != "N/A":
            detected_regression = False
            detected_improvement = False
            if (
                percentage_change < 0.0
                and percentage_change < -args.regressions_percent_lower_limit
            ):
                detected_regression = True
                total_regressions = total_regressions + 1
                detected_regressions.append(test_name)
            if (
                percentage_change > 0.0
                and percentage_change > args.regressions_percent_lower_limit
            ):
                detected_improvement = True
                total_improvements = total_improvements + 1

            if detected_improvement is False and detected_regression is False:
                total_stable = total_stable + 1

            if args.print_regressions_only is False or detected_regression:
                profilers_artifacts_matrix.append(
                    [
                        test_name,
                        baseline_v,
                        comparison_v,
                        percentage_change,
                    ]
                )

    logging.info("Printing differential analysis between branches")

    writer = MarkdownTableWriter(
        table_name="Comparison between {} and {} for metric: {}".format(
            baseline_branch, comparison_branch, metric_name
        ),
        headers=[
            "Test Case",
            "Baseline value",
            "Comparison Value",
            "% change ({})".format(metric_mode),
        ],
        value_matrix=profilers_artifacts_matrix,
    )
    writer.write_table()
    if total_stable > 0:
        logging.info(
            "Detected a total of {} stable tests between versions.".format(
                total_stable,
            )
        )
    if total_improvements > 0:
        logging.info(
            "Detected a total of {} improvements above the improvement water line (> {} %%)".format(
                total_improvements, args.regressions_percent_lower_limit
            )
        )
    if total_regressions > 0:
        logging.warning(
            "Detected a total of {} regressions bellow the regression water line (< -{} %%)".format(
                total_regressions, args.regressions_percent_lower_limit
            )
        )
        logging.warning("Printing BENCHMARK env var compatible list")
        logging.warning(
            "BENCHMARK={}".format(
                ",".join(["{}.yml".format(x) for x in detected_regressions])
            )
        )
