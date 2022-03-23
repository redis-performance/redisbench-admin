#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import logging
import re

import pandas as pd
import redis
from pytablewriter import MarkdownTableWriter
import humanize
import datetime as dt
from tqdm import tqdm
from redisbench_admin.utils.remote import get_overall_dashboard_keynames


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
    rts = redis.Redis(
        host=args.redistimeseries_host,
        port=args.redistimeseries_port,
        password=args.redistimeseries_pass,
        username=args.redistimeseries_user,
    )
    rts.ping()

    tf_github_org = args.github_org
    tf_github_repo = args.github_repo
    tf_triggering_env = args.triggering_env
    if args.baseline_deployment_name != "":
        baseline_deployment_name = args.baseline_deployment_name
    else:
        baseline_deployment_name = args.deployment_name
    if args.comparison_deployment_name != "":
        comparison_deployment_name = args.comparison_deployment_name
    else:
        comparison_deployment_name = args.deployment_name

    logging.info(
        "Using baseline deployment_name={} and comparison deployment_name={} for the analysis".format(
            baseline_deployment_name,
            comparison_deployment_name,
        )
    )
    from_ts_ms = args.from_timestamp
    to_ts_ms = args.to_timestamp
    if from_ts_ms is None:
        from_ts_ms = int(args.from_date.timestamp() * 1000)
    if to_ts_ms is None:
        to_ts_ms = int(args.to_date.timestamp() * 1000)
    from_human_str = humanize.naturaltime(
        dt.datetime.utcfromtimestamp(from_ts_ms / 1000)
    )
    to_human_str = humanize.naturaltime(dt.datetime.utcfromtimestamp(to_ts_ms / 1000))
    logging.info(
        "Using a time-delta from {} to {}".format(from_human_str, to_human_str)
    )
    use_tag = False
    use_branch = False
    baseline_branch = args.baseline_branch
    comparison_branch = args.comparison_branch
    by_str = ""
    baseline_str = ""
    comparison_str = ""
    if baseline_branch is not None and comparison_branch is not None:
        use_branch = True
        by_str = "branch"
        baseline_str = baseline_branch
        comparison_str = comparison_branch
    baseline_tag = args.baseline_tag
    comparison_tag = args.comparison_tag
    if baseline_tag is not None and comparison_tag is not None:
        use_tag = True
        by_str = "version"
        baseline_str = baseline_tag
        comparison_str = comparison_tag
    if use_branch is False and use_tag is False:
        logging.error(
            "You need to provider either "
            + "( --baseline-branch and --comparison-branch ) "
            + "or ( --baseline-tag and --comparison-tag ) args"
        )
        exit(1)
    if use_branch is True and use_tag is True:
        logging.error(
            +"( --baseline-branch and --comparison-branch ) "
            + "and ( --baseline-tag and --comparison-tag ) args are mutually exclusive"
        )
        exit(1)
    metric_name = args.metric_name
    metric_mode = args.metric_mode
    (
        prefix,
        testcases_setname,
        _,
        tsname_project_total_failures,
        tsname_project_total_success,
        _,
        _,
        _,
        testcases_metric_context_path_setname,
        _,
        _,
        _,
        _,
        _,
    ) = get_overall_dashboard_keynames(tf_github_org, tf_github_repo, tf_triggering_env)
    test_names = []
    used_key = testcases_setname
    test_filter = "test_name"

    if args.use_metric_context_path:
        test_filter = "test_name:metric_context_path"
        used_key = testcases_metric_context_path_setname

    tags_regex_string = re.compile(args.testname_regex)
    if args.test != "":
        test_names = args.test.split(",")
        logging.info("Using test name {}".format(test_names))
    else:
        try:
            test_names = rts.smembers(used_key)
            test_names = list(test_names)
            test_names.sort()
            final_test_names = []
            for test_name in test_names:
                test_name = test_name.decode()
                match_obj = re.search(tags_regex_string, test_name)
                if match_obj is not None:
                    final_test_names.append(test_name)
            test_names = final_test_names

        except redis.exceptions.ResponseError as e:
            logging.warning(
                "Error while trying to fetch test cases set (key={}) {}. ".format(
                    used_key, e.__str__()
                )
            )
            pass

        logging.warning(
            "Based on test-cases set (key={}) we have {} comparison points. ".format(
                used_key, len(test_names)
            )
        )
    profilers_artifacts_matrix = []
    detected_regressions = []
    total_improvements = 0
    total_stable = 0
    total_unstable = 0
    total_regressions = 0
    noise_waterline = 2.5
    progress = tqdm(unit="benchmark time-series", total=len(test_names))
    for test_name in test_names:
        filters_baseline = [
            "{}={}".format(by_str, baseline_str),
            "metric={}".format(metric_name),
            "{}={}".format(test_filter, test_name),
            "deployment_name={}".format(baseline_deployment_name),
            "triggering_env={}".format(tf_triggering_env),
        ]
        filters_comparison = [
            "{}={}".format(by_str, comparison_str),
            "metric={}".format(metric_name),
            "{}={}".format(test_filter, test_name),
            "deployment_name={}".format(comparison_deployment_name),
            "triggering_env={}".format(tf_triggering_env),
        ]
        baseline_timeseries = rts.ts().queryindex(filters_baseline)
        comparison_timeseries = rts.ts().queryindex(filters_comparison)

        # avoiding target time-series
        comparison_timeseries = [x for x in comparison_timeseries if "target" not in x]
        baseline_timeseries = [x for x in baseline_timeseries if "target" not in x]
        progress.update()
        if args.verbose:
            logging.info("Baseline timeseries {}".format(len(baseline_timeseries)))
            logging.info("Comparison timeseries {}".format(len(comparison_timeseries)))
        if len(baseline_timeseries) != 1:
            if args.verbose:
                logging.warning(
                    "Skipping this test given the value of timeseries !=1. Baseline timeseries {}".format(
                        len(baseline_timeseries)
                    )
                )
            continue
        else:
            ts_name_baseline = baseline_timeseries[0]
        if len(comparison_timeseries) != 1:
            if args.verbose:
                logging.warning(
                    "Comparison timeseries {}".format(len(comparison_timeseries))
                )
            continue
        else:
            ts_name_comparison = comparison_timeseries[0]

        baseline_v = "N/A"
        comparison_v = "N/A"
        baseline_nsamples = 0
        comparison_nsamples = 0
        baseline_values = []
        comparison_values = []
        percentage_change = "N/A"
        percentage_change = 0.0
        baseline_v_str = "N/A"
        baseline_median = "N/A"
        comparison_median = "N/A"
        baseline_std = "N/A"
        comparison_std = "N/A"
        comparison_v_str = "N/A"
        largest_variance = 0
        baseline_pct_change = "N/A"
        comparison_pct_change = "N/A"

        note = ""
        try:
            baseline_datapoints = rts.ts().revrange(
                ts_name_baseline, from_ts_ms, to_ts_ms
            )
            baseline_nsamples = len(baseline_datapoints)
            if baseline_nsamples > 0:
                _, baseline_v = baseline_datapoints[0]
                for tuple in baseline_datapoints:
                    baseline_values.append(tuple[1])
                baseline_df = pd.DataFrame(baseline_values)
                baseline_median = float(baseline_df.median())
                baseline_v = baseline_median
                baseline_std = float(baseline_df.std())
                baseline_pct_change = (baseline_std / baseline_median) * 100.0
                largest_variance = baseline_pct_change

            comparison_datapoints = rts.ts().revrange(
                ts_name_comparison, from_ts_ms, to_ts_ms
            )
            comparison_nsamples = len(comparison_datapoints)
            if comparison_nsamples > 0:
                _, comparison_v = comparison_datapoints[0]
                for tuple in comparison_datapoints:
                    comparison_values.append(tuple[1])
                comparison_df = pd.DataFrame(comparison_values)
                comparison_median = float(comparison_df.median())
                comparison_v = comparison_median
                comparison_std = float(comparison_df.std())
                comparison_pct_change = (comparison_std / comparison_median) * 100.0
                if comparison_pct_change > largest_variance:
                    largest_variance = comparison_pct_change

            waterline = args.regressions_percent_lower_limit
            if args.regressions_percent_lower_limit < largest_variance:
                note = "waterline={:.1f}%.".format(largest_variance)
                waterline = largest_variance

        except redis.exceptions.ResponseError:
            pass
        unstable = False
        if baseline_v != "N/A" and comparison_v != "N/A":
            stamp_b = ""
            unstable = False
            if comparison_pct_change > 10.0 or baseline_pct_change > 10.0:
                note = "UNSTABLE (very high variance)"
                unstable = True
            if baseline_pct_change > 10.0:
                stamp_b = "UNSTABLE"
            baseline_v_str = " {:.0f} +- {:.1f}% {} ({} datapoints)".format(
                baseline_v, baseline_pct_change, stamp_b, baseline_nsamples
            )
            stamp_c = ""
            if comparison_pct_change > 10.0:
                stamp_c = "UNSTABLE"
            comparison_v_str = " {:.0f} +- {:.1f}% {} ({} datapoints)".format(
                comparison_v, comparison_pct_change, stamp_c, comparison_nsamples
            )
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
            if percentage_change < 0.0 and not unstable:

                if percentage_change < -waterline:
                    detected_regression = True
                    total_regressions = total_regressions + 1
                    note = note + " REGRESSION"
                elif percentage_change < -noise_waterline:
                    note = note + " potential REGRESSION"
                else:
                    note = note + " -- no change --"
                detected_regressions.append(test_name)
            if percentage_change > 0.0 and not unstable:
                if percentage_change > waterline:
                    detected_improvement = True
                    total_improvements = total_improvements + 1
                    note = note + " IMPROVEMENT"
                elif percentage_change > noise_waterline:
                    note = note + " potential IMPROVEMENT"
                else:
                    note = note + " -- no change --"

            if (
                detected_improvement is False
                and detected_regression is False
                and not unstable
            ):
                total_stable = total_stable + 1

            if unstable:
                total_unstable += 1

            if args.print_regressions_only is False or detected_regression:
                percentage_change_str = "{:.1f}% ".format(percentage_change)
                profilers_artifacts_matrix.append(
                    [
                        test_name,
                        baseline_v_str,
                        comparison_v_str,
                        percentage_change_str,
                        note.strip(),
                    ]
                )

    logging.info("Printing differential analysis between branches")
    writer = MarkdownTableWriter(
        table_name="Comparison between {} and {} for metric: {}. Time Period from {} to {}. (environment used: {})".format(
            baseline_branch,
            comparison_branch,
            metric_name,
            from_human_str,
            to_human_str,
            baseline_deployment_name,
        ),
        headers=[
            "Test Case",
            "Baseline {} (median obs. +- std.dev)".format(baseline_branch),
            "Comparison {} (median obs. +- std.dev)".format(comparison_branch),
            "% change ({})".format(metric_mode),
            "Note",
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
    if total_unstable > 0:
        logging.warning(
            "Detected a total of {} highly unstable benchmarks.".format(total_unstable)
        )
    if total_improvements > 0:
        logging.info(
            "Detected a total of {} improvements above the improvement water line.".format(
                total_improvements
            )
        )
    if total_regressions > 0:
        logging.warning(
            "Detected a total of {} regressions bellow the regression water line {}.".format(
                total_regressions, args.regressions_percent_lower_limit
            )
        )
        logging.warning("Printing BENCHMARK env var compatible list")
        logging.warning(
            "BENCHMARK={}".format(
                ",".join(["{}.yml".format(x) for x in detected_regressions])
            )
        )
