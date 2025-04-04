#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import datetime
import logging
import re
import pandas as pd
import redis
import yaml
from pytablewriter import MarkdownTableWriter
import humanize
import datetime as dt
import os
from tqdm import tqdm
from github import Github
from slack_sdk.webhook import WebhookClient

from redisbench_admin.run.common import get_start_time_vars, WH_TOKEN
from redisbench_admin.run_remote.notifications import (
    generate_new_pr_comment_notification,
)
from redisbench_admin.utils.remote import get_overall_dashboard_keynames
from redisbench_admin.compare.args import ARCH_X86


def get_project_compare_zsets(triggering_env, org, repo):
    return "ci.benchmarks.redislabs/{}/{}/{}:compare:pull_requests:zset".format(
        triggering_env, org, repo
    )


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
        retry_on_timeout=True,
    )
    rts.ping()
    default_baseline_branch = None
    default_metrics_str = ""
    if args.defaults_filename != "" and os.path.exists(args.defaults_filename):
        logging.info(
            "Loading configuration from defaults file: {}".format(
                args.defaults_filename
            )
        )
        with open(args.defaults_filename) as yaml_fd:
            defaults_dict = yaml.safe_load(yaml_fd)
            if "exporter" in defaults_dict:
                exporter_dict = defaults_dict["exporter"]
                if "comparison" in exporter_dict:
                    comparison_dict = exporter_dict["comparison"]
                    if "metrics" in comparison_dict:
                        metrics = comparison_dict["metrics"]
                        logging.info("Detected defaults metrics info. reading metrics")
                        default_metrics = []

                        for metric in metrics:
                            if metric.startswith("$."):
                                metric = metric[2:]
                            logging.info("Will use metric: {}".format(metric))
                            default_metrics.append(metric)
                        if len(default_metrics) == 1:
                            default_metrics_str = default_metrics[0]
                        if len(default_metrics) > 1:
                            default_metrics_str = "({})".format(
                                ",".join(default_metrics)
                            )
                        logging.info("Default metrics: {}".format(default_metrics_str))

                    if "baseline-branch" in comparison_dict:
                        default_baseline_branch = comparison_dict["baseline-branch"]
                        logging.info(
                            "Detected baseline branch in defaults file. {}".format(
                                default_baseline_branch
                            )
                        )

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
    from_date = args.from_date
    to_date = args.to_date
    baseline_branch = args.baseline_branch
    if baseline_branch is None and default_baseline_branch is not None:
        logging.info(
            "Given --baseline-branch was null using the default baseline branch {}".format(
                default_baseline_branch
            )
        )
        baseline_branch = default_baseline_branch
    comparison_branch = args.comparison_branch
    simplify_table = args.simple_table
    print_regressions_only = args.print_regressions_only
    print_improvements_only = args.print_improvements_only
    skip_unstable = args.skip_unstable
    baseline_tag = args.baseline_tag
    comparison_tag = args.comparison_tag
    last_n_baseline = args.last_n
    last_n_comparison = args.last_n
    if last_n_baseline < 0:
        last_n_baseline = args.last_n_baseline
    if last_n_comparison < 0:
        last_n_comparison = args.last_n_comparison
    first_n_baseline = args.first_n_baseline
    first_n_comparison = args.first_n_comparison
    # Log the interval of values considered
    if first_n_baseline >= 0:
        logging.info(
            "Using samples in the range [{}:{}] for baseline analysis".format(
                first_n_baseline, last_n_baseline
            )
        )
    else:
        logging.info(
            "Using last {} samples for baseline analysis".format(last_n_baseline)
        )

    if first_n_comparison >= 0:
        logging.info(
            "Using samples in the range [{}:{}] for comparison analysis".format(
                first_n_comparison, last_n_comparison
            )
        )
    else:
        logging.info(
            "Using last {} samples for comparison analysis".format(last_n_comparison)
        )

    verbose = args.verbose
    regressions_percent_lower_limit = args.regressions_percent_lower_limit
    metric_name = args.metric_name
    if (metric_name is None or metric_name == "") and default_metrics_str != "":
        logging.info(
            "Given --metric_name was null using the default metric names {}".format(
                default_metrics_str
            )
        )
        metric_name = default_metrics_str

    if metric_name is None:
        logging.error(
            "You need to provider either "
            + " --metric_name or provide a defaults file via --defaults_filename that contains exporter.redistimeseries.comparison.metrics array. Exiting..."
        )
        exit(1)
    else:
        logging.info("Using metric {}".format(metric_name))

    metric_mode = args.metric_mode
    test = args.test
    use_metric_context_path = args.use_metric_context_path
    github_token = args.github_token
    pull_request = args.pull_request
    testname_regex = args.testname_regex
    auto_approve = args.auto_approve
    running_platform = args.running_platform
    grafana_base_dashboard = args.grafana_base_dashboard
    # using an access token
    is_actionable_pr = False
    contains_regression_comment = False
    regression_comment = None
    github_pr = None
    # slack related
    webhook_notifications_active = False
    webhook_client_slack = None
    if running_platform is not None:
        logging.info(
            "Using platform named: {} to do the comparison.\n\n".format(
                running_platform
            )
        )
    if WH_TOKEN is not None:
        webhook_notifications_active = True
        webhook_url = "https://hooks.slack.com/services/{}".format(WH_TOKEN)
        logging.info("Detected slack webhook token")
        webhook_client_slack = WebhookClient(webhook_url)

    old_regression_comment_body = ""
    if github_token is not None:
        logging.info("Detected github token")
        g = Github(github_token)
        if pull_request is not None and pull_request != "":
            pull_request_n = int(pull_request)
            github_pr = (
                g.get_user(tf_github_org)
                .get_repo(tf_github_repo)
                .get_issue(pull_request_n)
            )
            comments = github_pr.get_comments()
            pr_link = github_pr.html_url
            logging.info("Working on github PR already: {}".format(pr_link))
            is_actionable_pr = True
            contains_regression_comment, pos = check_regression_comment(comments)
            if contains_regression_comment:
                regression_comment = comments[pos]
                old_regression_comment_body = regression_comment.body
                logging.info(
                    "Already contains regression comment. Link: {}".format(
                        regression_comment.html_url
                    )
                )
                if verbose:
                    logging.info("Printing old regression comment:")
                    print("".join(["-" for x in range(1, 80)]))
                    print(regression_comment.body)
                    print("".join(["-" for x in range(1, 80)]))
            else:
                logging.info("Does not contain regression comment")

    grafana_dashboards_uids = {
        "redisgraph": "SH9_rQYGz",
        "redisbloom": "q4-5sRR7k",
        "redisearch": "3Ejv2wZnk",
        "redisjson": "UErSC0jGk",
        "redistimeseries": "2WMw61UGz",
    }
    baseline_architecture = args.baseline_architecture
    comparison_architecture = args.comparison_architecture
    uid = None
    if tf_github_repo.lower() in grafana_dashboards_uids:
        uid = grafana_dashboards_uids[tf_github_repo.lower()]
    grafana_link_base = None
    if uid is not None:
        grafana_link_base = "{}/{}".format(grafana_base_dashboard, uid)
        logging.info(
            "There is a grafana dashboard for this repo. Base link: {}".format(
                grafana_link_base
            )
        )

    (
        detected_regressions,
        table_output,
        total_improvements,
        total_regressions,
        total_stable,
        total_unstable,
        total_comparison_points,
    ) = compute_regression_table(
        rts,
        tf_github_org,
        tf_github_repo,
        tf_triggering_env,
        metric_name,
        comparison_branch,
        baseline_branch,
        baseline_tag,
        comparison_tag,
        baseline_deployment_name,
        comparison_deployment_name,
        print_improvements_only,
        print_regressions_only,
        skip_unstable,
        regressions_percent_lower_limit,
        simplify_table,
        test,
        testname_regex,
        verbose,
        last_n_baseline,
        last_n_comparison,
        metric_mode,
        from_date,
        from_ts_ms,
        to_date,
        to_ts_ms,
        use_metric_context_path,
        running_platform,
        baseline_architecture,
        comparison_architecture,
        first_n_baseline,
        first_n_comparison,
    )
    comment_body = ""
    if total_comparison_points > 0:
        comment_body = "### Automated performance analysis summary\n\n"
        comment_body += "This comment was automatically generated given there is performance data available.\n\n"
        if running_platform is not None:
            comment_body += "Using platform named: {} to do the comparison.\n\n".format(
                running_platform
            )
        comparison_summary = "In summary:\n"
        if total_stable > 0:
            comparison_summary += (
                "- Detected a total of {} stable tests between versions.\n".format(
                    total_stable,
                )
            )

        if total_unstable > 0:
            comparison_summary += (
                "- Detected a total of {} highly unstable benchmarks.\n".format(
                    total_unstable
                )
            )
        if total_improvements > 0:
            comparison_summary += "- Detected a total of {} improvements above the improvement water line.\n".format(
                total_improvements
            )
        if total_regressions > 0:
            comparison_summary += "- Detected a total of {} regressions bellow the regression water line {}.\n".format(
                total_regressions, args.regressions_percent_lower_limit
            )

        comment_body += comparison_summary
        comment_body += "\n"

        if grafana_link_base is not None:
            grafana_link = "{}/".format(grafana_link_base)
            if baseline_tag is not None and comparison_tag is not None:
                grafana_link += "?var-version={}&var-version={}".format(
                    baseline_tag, comparison_tag
                )
            if baseline_branch is not None and comparison_branch is not None:
                grafana_link += "?var-branch={}&var-branch={}".format(
                    baseline_branch, comparison_branch
                )
            comment_body += "You can check a comparison in detail via the [grafana link]({})".format(
                grafana_link
            )

        comment_body += "\n\n##" + table_output
        print(comment_body)

        if is_actionable_pr:
            zset_project_pull_request = get_project_compare_zsets(
                tf_triggering_env,
                tf_github_org,
                tf_github_repo,
            )
            logging.info(
                "Populating the pull request performance ZSETs: {} with branch {}".format(
                    zset_project_pull_request, comparison_branch
                )
            )
            _, start_time_ms, _ = get_start_time_vars()
            res = rts.zadd(
                zset_project_pull_request,
                {comparison_branch: start_time_ms},
            )
            logging.info(
                "Result of Populating the pull request performance ZSETs: {} with branch {}: {}".format(
                    zset_project_pull_request, comparison_branch, res
                )
            )
            user_input = "n"
            html_url = "n/a"
            regression_count = len(detected_regressions)
            (
                baseline_str,
                by_str_baseline,
                comparison_str,
                by_str_comparison,
            ) = get_by_strings(
                baseline_branch,
                comparison_branch,
                baseline_tag,
                comparison_tag,
            )

            if contains_regression_comment:
                same_comment = False
                if comment_body == old_regression_comment_body:
                    logging.info(
                        "The old regression comment is the same as the new comment. skipping..."
                    )
                    same_comment = True
                else:
                    logging.info(
                        "The old regression comment is different from the new comment. updating it..."
                    )
                    comment_body_arr = comment_body.split("\n")
                    old_regression_comment_body_arr = old_regression_comment_body.split(
                        "\n"
                    )
                    if verbose:
                        DF = [
                            x
                            for x in comment_body_arr
                            if x not in old_regression_comment_body_arr
                        ]
                        print("---------------------")
                        print(DF)
                        print("---------------------")
                if same_comment is False:
                    if auto_approve:
                        print("auto approving...")
                    else:
                        user_input = input(
                            "Do you wish to update the comment {} (y/n): ".format(
                                regression_comment.html_url
                            )
                        )
                    if user_input.lower() == "y" or auto_approve:
                        print("Updating comment {}".format(regression_comment.html_url))
                        regression_comment.edit(comment_body)
                        html_url = regression_comment.html_url
                        print(
                            "Updated comment. Access it via {}".format(
                                regression_comment.html_url
                            )
                        )
                        if webhook_notifications_active:
                            logging.info(
                                "Sending slack notification about updated comment..."
                            )
                            generate_new_pr_comment_notification(
                                webhook_client_slack,
                                comparison_summary,
                                html_url,
                                tf_github_org,
                                tf_github_repo,
                                baseline_str,
                                comparison_str,
                                regression_count,
                                "UPDATED",
                            )

            else:
                if auto_approve:
                    print("auto approving...")
                else:
                    user_input = input(
                        "Do you wish to add a comment in {} (y/n): ".format(pr_link)
                    )
                if user_input.lower() == "y" or auto_approve:
                    print("creating an comment in PR {}".format(pr_link))
                    regression_comment = github_pr.create_comment(comment_body)
                    html_url = regression_comment.html_url
                    print("created comment. Access it via {}".format(html_url))
                    if webhook_notifications_active:
                        logging.info("Sending slack notification about new comment...")
                        generate_new_pr_comment_notification(
                            webhook_client_slack,
                            comparison_summary,
                            html_url,
                            tf_github_org,
                            tf_github_repo,
                            baseline_str,
                            comparison_str,
                            regression_count,
                            "NEW",
                        )
    else:
        logging.error("There was no comparison points to produce a table...")
    return (
        detected_regressions,
        comment_body,
        total_improvements,
        total_regressions,
        total_stable,
        total_unstable,
        total_comparison_points,
    )


def check_regression_comment(comments):
    res = False
    pos = -1
    for n, comment in enumerate(comments):
        body = comment.body
        if "Comparison between" in body and "Time Period from" in body:
            res = True
            pos = n
    return res, pos


def compute_regression_table(
    rts,
    tf_github_org,
    tf_github_repo,
    tf_triggering_env,
    metric_name,
    comparison_branch,
    baseline_branch="master",
    baseline_tag=None,
    comparison_tag=None,
    baseline_deployment_name="oss-standalone",
    comparison_deployment_name="oss-standalone",
    print_improvements_only=False,
    print_regressions_only=False,
    skip_unstable=False,
    regressions_percent_lower_limit=5.0,
    simplify_table=False,
    test="",
    testname_regex=".*",
    verbose=False,
    last_n_baseline=-1,
    last_n_comparison=-1,
    metric_mode="higher-better",
    from_date=None,
    from_ts_ms=None,
    to_date=None,
    to_ts_ms=None,
    use_metric_context_path=None,
    running_platform=None,
    baseline_architecture=ARCH_X86,
    comparison_architecture=ARCH_X86,
    first_n_baseline=-1,
    first_n_comparison=-1,
):
    START_TIME_NOW_UTC, _, _ = get_start_time_vars()
    START_TIME_LAST_MONTH_UTC = START_TIME_NOW_UTC - datetime.timedelta(days=31)
    if from_date is None:
        from_date = START_TIME_LAST_MONTH_UTC
    if to_date is None:
        to_date = START_TIME_NOW_UTC
    if from_ts_ms is None:
        from_ts_ms = int(from_date.timestamp() * 1000)
    if to_ts_ms is None:
        to_ts_ms = int(to_date.timestamp() * 1000)
    from_human_str = humanize.naturaltime(
        dt.datetime.utcfromtimestamp(from_ts_ms / 1000)
    )
    to_human_str = humanize.naturaltime(dt.datetime.utcfromtimestamp(to_ts_ms / 1000))
    logging.info(
        "Using a time-delta from {} to {}".format(from_human_str, to_human_str)
    )
    baseline_str, by_str_baseline, comparison_str, by_str_comparison = get_by_strings(
        baseline_branch,
        comparison_branch,
        baseline_tag,
        comparison_tag,
    )
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
    if use_metric_context_path:
        test_filter = "test_name:metric_context_path"
        used_key = testcases_metric_context_path_setname
    tags_regex_string = re.compile(testname_regex)
    if test != "":
        test_names = test.split(",")
        logging.info("Using test name {}".format(test_names))
    else:
        test_names = get_test_names_from_db(
            rts, tags_regex_string, test_names, used_key
        )
    (
        detected_regressions,
        table,
        total_improvements,
        total_regressions,
        total_stable,
        total_unstable,
        total_comparison_points,
    ) = from_rts_to_regression_table(
        baseline_deployment_name,
        comparison_deployment_name,
        baseline_str,
        comparison_str,
        by_str_baseline,
        by_str_comparison,
        from_ts_ms,
        to_ts_ms,
        last_n_baseline,
        last_n_comparison,
        metric_mode,
        metric_name,
        print_improvements_only,
        print_regressions_only,
        skip_unstable,
        regressions_percent_lower_limit,
        rts,
        simplify_table,
        test_filter,
        test_names,
        tf_triggering_env,
        verbose,
        running_platform,
        baseline_architecture,
        comparison_architecture,
        first_n_baseline,
        first_n_comparison,
    )
    logging.info(
        "Printing differential analysis between {} and {}".format(
            baseline_str, comparison_str
        )
    )
    writer = MarkdownTableWriter(
        table_name="Comparison between {} and {}.\n\nTime Period from {}. (environment used: {})\n".format(
            baseline_str,
            comparison_str,
            from_human_str,
            baseline_deployment_name,
        ),
        headers=[
            "Test Case",
            "Baseline {} (median obs. +- std.dev)".format(baseline_str),
            "Comparison {} (median obs. +- std.dev)".format(comparison_str),
            "% change ({})".format(metric_mode),
            "Note",
        ],
        value_matrix=table,
    )
    table_output = ""

    from io import StringIO
    import sys

    old_stdout = sys.stdout
    sys.stdout = mystdout = StringIO()

    writer.dump(mystdout, False)

    sys.stdout = old_stdout

    table_output = mystdout.getvalue()

    return (
        detected_regressions,
        table_output,
        total_improvements,
        total_regressions,
        total_stable,
        total_unstable,
        total_comparison_points,
    )


def get_by_strings(
    baseline_branch,
    comparison_branch,
    baseline_tag,
    comparison_tag,
):
    baseline_covered = False
    comparison_covered = False
    by_str_baseline = ""
    by_str_comparison = ""
    baseline_str = ""
    comparison_str = ""
    if baseline_branch is not None:
        baseline_covered = True
        by_str_baseline = "branch"
        baseline_str = baseline_branch
    if comparison_branch is not None:
        comparison_covered = True
        by_str_comparison = "branch"
        comparison_str = comparison_branch

    if baseline_tag is not None:
        if comparison_covered:
            logging.error(
                "--baseline-branch and --baseline-tag are mutually exclusive. Pick one..."
            )
            exit(1)
        baseline_covered = True
        by_str_baseline = "version"
        baseline_str = baseline_tag

    if comparison_tag is not None:
        # check if we had already covered comparison
        if comparison_covered:
            logging.error(
                "--comparison-branch and --comparison-tag are mutually exclusive. Pick one..."
            )
            exit(1)
        comparison_covered = True
        by_str_comparison = "version"
        comparison_str = comparison_tag

    if baseline_covered is False:
        logging.error(
            "You need to provider either " + "( --baseline-branch or --baseline-tag ) "
        )
        exit(1)
    if comparison_covered is False:
        logging.error(
            "You need to provider either "
            + "( --comparison-branch or --comparison-tag ) "
        )
        exit(1)
    return baseline_str, by_str_baseline, comparison_str, by_str_comparison


def from_rts_to_regression_table(
    baseline_deployment_name,
    comparison_deployment_name,
    baseline_str,
    comparison_str,
    by_str_baseline,
    by_str_comparison,
    from_ts_ms,
    to_ts_ms,
    last_n_baseline,
    last_n_comparison,
    metric_mode,
    metric_name,
    print_improvements_only,
    print_regressions_only,
    skip_unstable,
    regressions_percent_lower_limit,
    rts,
    simplify_table,
    test_filter,
    test_names,
    tf_triggering_env,
    verbose,
    running_platform=None,
    baseline_architecture=ARCH_X86,
    comparison_architecture=ARCH_X86,
    first_n_baseline=-1,
    first_n_comparison=-1,
):
    print_all = print_regressions_only is False and print_improvements_only is False
    table = []
    detected_regressions = []
    total_improvements = 0
    total_stable = 0
    total_unstable = 0
    total_regressions = 0
    total_comparison_points = 0
    noise_waterline = 3
    progress = tqdm(unit="benchmark time-series", total=len(test_names))
    for test_name in test_names:
        multi_value_baseline = check_multi_value_filter(baseline_str)
        multi_value_comparison = check_multi_value_filter(comparison_str)

        filters_baseline = [
            "{}={}".format(by_str_baseline, baseline_str),
            "metric={}".format(metric_name),
            "{}={}".format(test_filter, test_name),
            "deployment_name={}".format(baseline_deployment_name),
            "triggering_env={}".format(tf_triggering_env),
        ]
        if running_platform is not None:
            filters_baseline.append("running_platform={}".format(running_platform))
        if baseline_architecture != ARCH_X86:
            filters_baseline.append(f"arch={baseline_architecture}")
        filters_comparison = [
            "{}={}".format(by_str_comparison, comparison_str),
            "metric={}".format(metric_name),
            "{}={}".format(test_filter, test_name),
            "deployment_name={}".format(comparison_deployment_name),
            "triggering_env={}".format(tf_triggering_env),
        ]
        if running_platform is not None:
            filters_comparison.append("running_platform={}".format(running_platform))
        if comparison_architecture != ARCH_X86:
            filters_comparison.append(f"arch={comparison_architecture}")
        baseline_timeseries = rts.ts().queryindex(filters_baseline)
        comparison_timeseries = rts.ts().queryindex(filters_comparison)

        # avoiding target time-series
        comparison_timeseries = [x for x in comparison_timeseries if "target" not in x]
        baseline_timeseries = [x for x in baseline_timeseries if "target" not in x]
        progress.update()
        if verbose:
            logging.info(
                "Baseline timeseries for {}: {}. test={}".format(
                    baseline_str, len(baseline_timeseries), test_name
                )
            )
            logging.info(
                "Comparison timeseries for {}: {}. test={}".format(
                    comparison_str, len(comparison_timeseries), test_name
                )
            )
        if len(baseline_timeseries) > 1 and multi_value_baseline is False:
            baseline_timeseries = get_only_Totals(baseline_timeseries)

        if len(baseline_timeseries) != 1 and multi_value_baseline is False:
            if verbose:
                logging.warning(
                    "Skipping this test given the value of timeseries !=1. Baseline timeseries {}".format(
                        len(baseline_timeseries)
                    )
                )
                if len(baseline_timeseries) > 1:
                    logging.warning(
                        "\t\tTime-series: {}".format(", ".join(baseline_timeseries))
                    )
            continue

        if len(comparison_timeseries) > 1 and multi_value_comparison is False:
            comparison_timeseries = get_only_Totals(comparison_timeseries)
        if len(comparison_timeseries) != 1 and multi_value_comparison is False:
            if verbose:
                logging.warning(
                    "Comparison timeseries {}".format(len(comparison_timeseries))
                )
            continue

        baseline_v = "N/A"
        comparison_v = "N/A"
        baseline_values = []
        baseline_datapoints = []
        comparison_values = []
        comparison_datapoints = []
        percentage_change = 0.0
        baseline_v_str = "N/A"
        comparison_v_str = "N/A"
        largest_variance = 0
        baseline_pct_change = "N/A"
        comparison_pct_change = "N/A"

        note = ""
        try:
            for ts_name_baseline in baseline_timeseries:
                datapoints_inner = rts.ts().revrange(
                    ts_name_baseline, from_ts_ms, to_ts_ms
                )
                baseline_datapoints.extend(datapoints_inner)
            (
                baseline_pct_change,
                baseline_v,
                largest_variance,
            ) = get_v_pct_change_and_largest_var(
                baseline_datapoints,
                baseline_pct_change,
                baseline_v,
                baseline_values,
                largest_variance,
                last_n_baseline,
                verbose,
                first_n_baseline,
            )
            for ts_name_comparison in comparison_timeseries:
                datapoints_inner = rts.ts().revrange(
                    ts_name_comparison, from_ts_ms, to_ts_ms
                )
                comparison_datapoints.extend(datapoints_inner)

            (
                comparison_pct_change,
                comparison_v,
                largest_variance,
            ) = get_v_pct_change_and_largest_var(
                comparison_datapoints,
                comparison_pct_change,
                comparison_v,
                comparison_values,
                largest_variance,
                last_n_comparison,
                verbose,
                first_n_comparison,
            )

            waterline = regressions_percent_lower_limit
            if regressions_percent_lower_limit < largest_variance:
                note = "waterline={:.1f}%.".format(largest_variance)
                waterline = largest_variance

        except redis.exceptions.ResponseError:
            pass
        except ZeroDivisionError as e:
            logging.error("Detected a ZeroDivisionError. {}".format(e.__str__()))
            pass
        unstable = False
        if baseline_v != "N/A" and comparison_v != "N/A":
            if comparison_pct_change > 10.0 or baseline_pct_change > 10.0:
                note = "UNSTABLE (very high variance)"
                unstable = True

            baseline_v_str = prepare_value_str(
                baseline_pct_change, baseline_v, baseline_values, simplify_table
            )
            comparison_v_str = prepare_value_str(
                comparison_pct_change, comparison_v, comparison_values, simplify_table
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
                if -waterline >= percentage_change:
                    detected_regression = True
                    total_regressions = total_regressions + 1
                    note = note + " REGRESSION"
                    detected_regressions.append(test_name)
                elif percentage_change < -noise_waterline:
                    if simplify_table is False:
                        note = note + " potential REGRESSION"
                else:
                    if simplify_table is False:
                        note = note + " No Change"

            if percentage_change > 0.0 and not unstable:
                if percentage_change > waterline:
                    detected_improvement = True
                    total_improvements = total_improvements + 1
                    note = note + " IMPROVEMENT"
                elif percentage_change > noise_waterline:
                    if simplify_table is False:
                        note = note + " potential IMPROVEMENT"
                else:
                    if simplify_table is False:
                        note = note + " No Change"

            if (
                detected_improvement is False
                and detected_regression is False
                and not unstable
            ):
                total_stable = total_stable + 1

            if unstable:
                total_unstable += 1

            should_add_line = False
            if print_regressions_only and detected_regression:
                should_add_line = True
            if print_improvements_only and detected_improvement:
                should_add_line = True
            if print_all:
                should_add_line = True
            if unstable and skip_unstable:
                should_add_line = False

            if should_add_line:
                total_comparison_points = total_comparison_points + 1
                add_line(
                    baseline_v_str,
                    comparison_v_str,
                    note,
                    percentage_change,
                    table,
                    test_name,
                )
    return (
        detected_regressions,
        table,
        total_improvements,
        total_regressions,
        total_stable,
        total_unstable,
        total_comparison_points,
    )


def get_only_Totals(baseline_timeseries):
    logging.warning("\t\tTime-series: {}".format(", ".join(baseline_timeseries)))
    logging.info("Checking if Totals will reduce timeseries.")
    new_base = []
    for ts_name in baseline_timeseries:
        if "Totals" in ts_name:
            new_base.append(ts_name)
    baseline_timeseries = new_base
    return baseline_timeseries


def check_multi_value_filter(baseline_str):
    multi_value_baseline = False
    if "(" in baseline_str and "," in baseline_str and ")" in baseline_str:
        multi_value_baseline = True
    return multi_value_baseline


def prepare_value_str(baseline_pct_change, baseline_v, baseline_values, simplify_table):
    if baseline_v < 1.0:
        baseline_v_str = " {:.2f}".format(baseline_v)
    elif baseline_v < 10.0:
        baseline_v_str = " {:.1f}".format(baseline_v)
    else:
        baseline_v_str = " {:.0f}".format(baseline_v)
    stamp_b = ""
    if baseline_pct_change > 10.0:
        stamp_b = "UNSTABLE "
    if len(baseline_values) > 1:
        baseline_v_str += " +- {:.1f}% {}".format(
            baseline_pct_change,
            stamp_b,
        )
    if simplify_table is False and len(baseline_values) > 1:
        baseline_v_str += "({} datapoints)".format(len(baseline_values))
    return baseline_v_str


def get_test_names_from_db(rts, tags_regex_string, test_names, used_key):
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
    return test_names


def add_line(
    baseline_v_str,
    comparison_v_str,
    note,
    percentage_change,
    table,
    test_name,
):
    percentage_change_str = "{:.1f}% ".format(percentage_change)
    table.append(
        [
            test_name,
            baseline_v_str,
            comparison_v_str,
            percentage_change_str,
            note.strip(),
        ]
    )


def get_v_pct_change_and_largest_var(
    comparison_datapoints,
    comparison_pct_change,
    comparison_v,
    comparison_values,
    largest_variance,
    last_n=-1,
    verbose=False,
    first_n=-1,
):
    comparison_nsamples = len(comparison_datapoints)
    if comparison_nsamples > 0:
        _, comparison_v = comparison_datapoints[0]

        # Apply first_n and last_n boundaries
        start_idx = 0 if first_n < 0 else max(0, min(first_n, comparison_nsamples))
        end_idx = (
            comparison_nsamples
            if last_n < 0
            else max(0, min(last_n, comparison_nsamples))
        )

        selected_data = comparison_datapoints[start_idx:end_idx]

        for tuple in selected_data:
            comparison_values.append(tuple[1])

        comparison_df = pd.DataFrame(comparison_values)
        comparison_median = float(comparison_df.median())
        comparison_v = comparison_median
        comparison_std = float(comparison_df.std())
        if verbose:
            logging.info(
                "comparison_datapoints: {} value: {}; std-dev: {}; median: {}".format(
                    comparison_datapoints,
                    comparison_v,
                    comparison_std,
                    comparison_median,
                )
            )
        comparison_pct_change = (comparison_std / comparison_median) * 100.0
        if comparison_pct_change > largest_variance:
            largest_variance = comparison_pct_change
    return comparison_pct_change, comparison_v, largest_variance
