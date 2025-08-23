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
import statistics
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
    uid = args.grafana_uid
    if tf_github_repo.lower() in grafana_dashboards_uids and uid is None:
        uid = grafana_dashboards_uids[tf_github_repo.lower()]
        logging.info(
            f"Using uid from grafana_dashboards_uids. {grafana_dashboards_uids}. uid={uid}"
        )
    else:
        logging.info(f"Using uid from args. uid={uid}")
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
        total_unstable_baseline,
        total_unstable_comparison,
        total_latency_confirmed_regressions,
        latency_confirmed_regression_details,
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
        grafana_link_base,
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
                f"- Detected a total of {total_stable} stable tests between versions.\n"
            )

        if total_unstable > 0:
            unstable_details = []
            if total_unstable_baseline > 0:
                unstable_details.append(f"{total_unstable_baseline} baseline")
            if total_unstable_comparison > 0:
                unstable_details.append(f"{total_unstable_comparison} comparison")

            unstable_breakdown = (
                " (" + ", ".join(unstable_details) + ")" if unstable_details else ""
            )
            comparison_summary += (
                "- Detected a total of {} highly unstable benchmarks{}.\n".format(
                    total_unstable, unstable_breakdown
                )
            )

            # Add latency confirmation summary if applicable
            if total_latency_confirmed_regressions > 0:
                comparison_summary += "- Latency analysis confirmed regressions in {} of the unstable tests:\n".format(
                    total_latency_confirmed_regressions
                )

                # Add detailed breakdown as bullet points with test links
                if latency_confirmed_regression_details:
                    for detail in latency_confirmed_regression_details:
                        test_name = detail["test_name"]
                        commands_info = []
                        for cmd_detail in detail["commands"]:
                            commands_info.append(
                                f"{cmd_detail['command']} +{cmd_detail['change_percent']:.1f}%"
                            )

                        if commands_info:
                            # Create test link if grafana_link_base is available
                            test_display_name = test_name
                            if grafana_link_base is not None:
                                grafana_test_link = f"{grafana_link_base}?orgId=1&var-test_case={test_name}"
                                if baseline_branch is not None:
                                    grafana_test_link += (
                                        f"&var-branch={baseline_branch}"
                                    )
                                if comparison_branch is not None:
                                    grafana_test_link += (
                                        f"&var-branch={comparison_branch}"
                                    )
                                grafana_test_link += "&from=now-30d&to=now"
                                test_display_name = (
                                    f"[{test_name}]({grafana_test_link})"
                                )

                            # Add confidence indicator if available
                            confidence_indicator = ""
                            if "high_confidence" in detail:
                                confidence_indicator = (
                                    " 🔴" if detail["high_confidence"] else " ⚠️"
                                )

                            comparison_summary += f"  - {test_display_name}: {', '.join(commands_info)}{confidence_indicator}\n"
        if total_improvements > 0:
            comparison_summary += f"- Detected a total of {total_improvements} improvements above the improvement water line.\n"
        if total_regressions > 0:
            comparison_summary += f"- Detected a total of {total_regressions} regressions bellow the regression water line {args.regressions_percent_lower_limit}%.\n"
        comparison_summary += "\n"

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
        total_unstable_baseline,
        total_unstable_comparison,
        total_latency_confirmed_regressions,
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
    grafana_link_base=None,
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
        total_unstable_baseline,
        total_unstable_comparison,
        total_latency_confirmed_regressions,
        latency_confirmed_regression_details,
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
        grafana_link_base,
        baseline_branch,
        baseline_tag,
        comparison_branch,
        comparison_tag,
        from_date,
        to_date,
    )
    logging.info(
        "Printing differential analysis between {} and {}".format(
            baseline_str, comparison_str
        )
    )

    # Split table into improvements, regressions, and no-changes
    improvements_table = []
    regressions_table = []
    no_changes_table = []

    for row in table:
        # Check if there's a meaningful change (not stable/unstable)
        note = row[4].lower() if len(row) > 4 else ""
        percentage_str = row[3] if len(row) > 3 else "0.0%"

        # Extract percentage value
        try:
            percentage_val = float(percentage_str.replace("%", "").strip())
        except:
            percentage_val = 0.0

        # Categorize based on change type
        if "improvement" in note and "potential" not in note:
            # Only actual improvements, not potential ones
            improvements_table.append(row)
        elif ("regression" in note and "potential" not in note) or "unstable" in note:
            # Only actual regressions, not potential ones, plus unstable tests
            regressions_table.append(row)
        elif "no change" in note or "potential" in note:
            # No changes and potential changes (below significance threshold)
            no_changes_table.append(row)
        elif abs(percentage_val) > 3.0:  # Significant changes based on percentage
            if (percentage_val > 0 and metric_mode == "higher-better") or (
                percentage_val < 0 and metric_mode == "lower-better"
            ):
                improvements_table.append(row)
            else:
                regressions_table.append(row)
        else:
            no_changes_table.append(row)

    # Sort tables by percentage change
    def get_percentage_value(row):
        """Extract percentage value from row for sorting"""
        try:
            percentage_str = row[3] if len(row) > 3 else "0.0%"
            return float(percentage_str.replace("%", "").strip())
        except:
            return 0.0

    # Sort improvements by percentage change (highest first)
    improvements_table.sort(key=get_percentage_value, reverse=True)

    # Sort regressions by percentage change (most negative first for higher-better, most positive first for lower-better)
    if metric_mode == "higher-better":
        # For higher-better metrics, most negative changes are worst regressions
        regressions_table.sort(key=get_percentage_value)
    else:
        # For lower-better metrics, most positive changes are worst regressions
        regressions_table.sort(key=get_percentage_value, reverse=True)

    # Create improvements table (visible)
    improvements_writer = MarkdownTableWriter(
        table_name="Performance Improvements - Comparison between {} and {}.\n\nTime Period from {}. (environment used: {})\n".format(
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
        value_matrix=improvements_table,
    )

    # Create regressions table (visible)
    regressions_writer = MarkdownTableWriter(
        table_name="Performance Regressions and Issues - Comparison between {} and {}.\n\nTime Period from {}. (environment used: {})\n".format(
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
        value_matrix=regressions_table,
    )

    # Create no-changes table (hidden in markdown)
    no_changes_writer = MarkdownTableWriter(
        table_name="Tests with No Significant Changes",
        headers=[
            "Test Case",
            "Baseline {} (median obs. +- std.dev)".format(baseline_str),
            "Comparison {} (median obs. +- std.dev)".format(comparison_str),
            "% change ({})".format(metric_mode),
            "Note",
        ],
        value_matrix=no_changes_table,
    )

    table_output = ""

    from io import StringIO
    import sys

    old_stdout = sys.stdout
    sys.stdout = mystdout = StringIO()

    # Output improvements table first (if any)
    if improvements_table:
        improvements_writer.dump(mystdout, False)
        mystdout.write("\n\n")

    # Output regressions table (if any)
    if regressions_table:
        regressions_writer.dump(mystdout, False)
        mystdout.write("\n\n")

    # Add hidden no-changes table
    if no_changes_table:
        mystdout.write(
            "<details>\n<summary>Tests with No Significant Changes ({} tests)</summary>\n\n".format(
                len(no_changes_table)
            )
        )
        no_changes_writer.dump(mystdout, False)
        mystdout.write("\n</details>\n")

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
        total_unstable_baseline,
        total_unstable_comparison,
        total_latency_confirmed_regressions,
        latency_confirmed_regression_details,
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
    grafana_link_base=None,
    baseline_branch=None,
    baseline_tag=None,
    comparison_branch=None,
    comparison_tag=None,
    from_date=None,
    to_date=None,
):
    print_all = print_regressions_only is False and print_improvements_only is False
    table = []
    detected_regressions = []
    total_improvements = 0
    total_stable = 0
    total_unstable = 0
    total_unstable_baseline = 0
    total_unstable_comparison = 0
    total_regressions = 0
    total_comparison_points = 0
    total_latency_confirmed_regressions = 0
    latency_confirmed_regression_details = []  # Track specific test details
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
        unstable_baseline = False
        unstable_comparison = False
        latency_confirms_regression = False

        if baseline_v != "N/A" and comparison_v != "N/A":
            if comparison_pct_change > 10.0 or baseline_pct_change > 10.0:
                unstable = True
                unstable_baseline = baseline_pct_change > 10.0
                unstable_comparison = comparison_pct_change > 10.0

                # Build detailed unstable note
                unstable_parts = []
                if unstable_baseline and unstable_comparison:
                    unstable_parts.append(
                        "UNSTABLE (baseline & comparison high variance)"
                    )
                elif unstable_baseline:
                    unstable_parts.append("UNSTABLE (baseline high variance)")
                elif unstable_comparison:
                    unstable_parts.append("UNSTABLE (comparison high variance)")

                note = unstable_parts[0]

                # Log detailed warning about unstable data detection
                logging.warning(
                    f"UNSTABLE DATA DETECTED for test '{test_name}': "
                    f"baseline variance={baseline_pct_change:.1f}%, "
                    f"comparison variance={comparison_pct_change:.1f}% "
                    f"(threshold=10.0%)"
                )

                # For throughput metrics (higher-better), check both server-side and client-side latency
                if metric_mode == "higher-better":
                    logging.info(
                        f"Performing 2nd-level latency validation for unstable throughput metric '{test_name}' "
                        f"(metric_mode={metric_mode})"
                    )

                    # Check server-side p50 latency
                    (
                        server_latency_note,
                        server_confirms_regression,
                        server_regression_details,
                    ) = check_latency_for_unstable_throughput(
                        rts,
                        test_name,
                        baseline_str,
                        comparison_str,
                        by_str_baseline,
                        by_str_comparison,
                        baseline_deployment_name,
                        comparison_deployment_name,
                        tf_triggering_env,
                        from_ts_ms,
                        to_ts_ms,
                        last_n_baseline,
                        last_n_comparison,
                        first_n_baseline,
                        first_n_comparison,
                        running_platform,
                        baseline_architecture,
                        comparison_architecture,
                        verbose,
                    )

                    # Check client-side latency metrics
                    (
                        client_latency_note,
                        client_confirms_regression,
                        client_regression_details,
                    ) = check_client_side_latency(
                        rts,
                        test_name,
                        baseline_str,
                        comparison_str,
                        by_str_baseline,
                        by_str_comparison,
                        baseline_deployment_name,
                        comparison_deployment_name,
                        tf_triggering_env,
                        from_ts_ms,
                        to_ts_ms,
                        last_n_baseline,
                        last_n_comparison,
                        first_n_baseline,
                        first_n_comparison,
                        running_platform,
                        baseline_architecture,
                        comparison_architecture,
                        verbose,
                    )

                    # Combine results from both server and client side
                    combined_latency_notes = []
                    if server_latency_note:
                        combined_latency_notes.append(f"server: {server_latency_note}")
                    if client_latency_note:
                        combined_latency_notes.append(f"client: {client_latency_note}")

                    # Only confirm regression if BOTH server and client side show evidence AND data is stable enough
                    # Check if either server or client data contains unstable indicators
                    server_has_unstable = (
                        server_latency_note and "UNSTABLE" in server_latency_note
                    )
                    client_has_unstable = (
                        client_latency_note and "UNSTABLE" in client_latency_note
                    )

                    # Don't confirm regression if either side has unstable data
                    if server_has_unstable or client_has_unstable:
                        both_confirm_regression = False
                        unstable_sides = []
                        if server_has_unstable:
                            unstable_sides.append("server")
                        if client_has_unstable:
                            unstable_sides.append("client")
                        blocked_note = f"regression blocked due to unstable {' and '.join(unstable_sides)} latency data"
                        note += f"; {blocked_note}"
                        logging.info(
                            f"Blocking regression confirmation for '{test_name}' due to unstable latency data"
                        )
                        if server_has_unstable:
                            logging.info("  Server-side latency data is unstable")
                        if client_has_unstable:
                            logging.info("  Client-side latency data is unstable")
                    else:
                        both_confirm_regression = (
                            server_confirms_regression and client_confirms_regression
                        )

                    if combined_latency_notes:
                        combined_note = "; ".join(combined_latency_notes)
                        note += f"; {combined_note}"
                        logging.info(
                            f"Combined latency check result for '{test_name}': {combined_note}"
                        )

                        if both_confirm_regression:
                            logging.info(
                                f"BOTH server and client latency analysis CONFIRM regression for '{test_name}'"
                            )

                            # Set the flag for counting confirmed regressions
                            latency_confirms_regression = True

                            # Combine regression details from both server and client
                            combined_regression_details = (
                                server_regression_details or client_regression_details
                            )
                            if combined_regression_details:
                                combined_regression_details["server_side"] = (
                                    server_confirms_regression
                                )
                                combined_regression_details["client_side"] = (
                                    client_confirms_regression
                                )

                                # 2nd level confirmation is sufficient - always add to confirmed regressions
                                logging.info(
                                    f"Adding '{test_name}' to confirmed regressions based on 2nd level validation"
                                )

                                # Perform 3rd-level analysis: variance + p99 check for additional confidence scoring
                                logging.info(
                                    f"Performing 3rd-level analysis (variance + p99) for confidence scoring on '{test_name}'"
                                )
                                (
                                    confidence_note,
                                    high_confidence,
                                ) = perform_variance_and_p99_analysis(
                                    rts,
                                    test_name,
                                    baseline_str,
                                    comparison_str,
                                    by_str_baseline,
                                    by_str_comparison,
                                    baseline_deployment_name,
                                    comparison_deployment_name,
                                    tf_triggering_env,
                                    from_ts_ms,
                                    to_ts_ms,
                                    last_n_baseline,
                                    last_n_comparison,
                                    first_n_baseline,
                                    first_n_comparison,
                                    running_platform,
                                    baseline_architecture,
                                    comparison_architecture,
                                    verbose,
                                )

                                if confidence_note:
                                    note += f"; {confidence_note}"
                                    logging.info(
                                        f"Confidence analysis for '{test_name}': {confidence_note}"
                                    )
                                    # Use 3rd level confidence if available
                                    combined_regression_details["high_confidence"] = (
                                        high_confidence
                                    )
                                else:
                                    # No 3rd level data available - default to moderate confidence since 2nd level confirmed
                                    logging.info(
                                        f"No 3rd level data available for '{test_name}' - using 2nd level confirmation"
                                    )
                                    combined_regression_details["high_confidence"] = (
                                        True  # 2nd level confirmation is reliable
                                    )

                                # Always add to confirmed regressions when 2nd level confirms
                                latency_confirmed_regression_details.append(
                                    combined_regression_details
                                )
                        elif server_confirms_regression or client_confirms_regression:
                            side_confirmed = (
                                "server" if server_confirms_regression else "client"
                            )
                            side_not_confirmed = (
                                "client" if server_confirms_regression else "server"
                            )
                            insufficient_evidence_note = f"only {side_confirmed} side confirms regression ({side_not_confirmed} side stable) - insufficient evidence"
                            note += f"; {insufficient_evidence_note}"
                            logging.info(
                                f"Only {side_confirmed} side confirms regression for '{test_name}' - insufficient evidence"
                            )
                        else:
                            no_regression_note = (
                                "neither server nor client side confirms regression"
                            )
                            note += f"; {no_regression_note}"
                            logging.info(
                                f"Neither server nor client side confirms regression for '{test_name}'"
                            )
                    else:
                        logging.info(
                            f"No latency data available for secondary check on '{test_name}'"
                        )

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
                if unstable_baseline:
                    total_unstable_baseline += 1
                if unstable_comparison:
                    total_unstable_comparison += 1
                if latency_confirms_regression:
                    total_latency_confirmed_regressions += 1

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
                    grafana_link_base,
                    baseline_branch,
                    baseline_tag,
                    comparison_branch,
                    comparison_tag,
                    from_date,
                    to_date,
                )
    return (
        detected_regressions,
        table,
        total_improvements,
        total_regressions,
        total_stable,
        total_unstable,
        total_comparison_points,
        total_unstable_baseline,
        total_unstable_comparison,
        total_latency_confirmed_regressions,
        latency_confirmed_regression_details,
    )


def check_client_side_latency(
    rts,
    test_name,
    baseline_str,
    comparison_str,
    by_str_baseline,
    by_str_comparison,
    baseline_deployment_name,
    comparison_deployment_name,
    tf_triggering_env,
    from_ts_ms,
    to_ts_ms,
    last_n_baseline,
    last_n_comparison,
    first_n_baseline,
    first_n_comparison,
    running_platform,
    baseline_architecture,
    comparison_architecture,
    verbose=False,
):
    """
    Check client-side latency metrics to provide additional validation for regression detection.

    Returns:
        tuple: (note_string, confirms_regression_bool, regression_details_dict)
    """
    logging.info(f"Starting client-side latency check for test: {test_name}")
    try:
        # Client-side latency metrics to check
        client_metrics = [
            "p50_latency_ms",
            "Latency",
            "OverallQuantiles.allCommands.q50",
            "Tests.INSERT.AverageLatency_us_",
            "Tests.READ.AverageLatency_us_",
            "Tests.SEARCH.AverageLatency_us_",
            "Tests.UPDATE.AverageLatency_us_",
        ]

        client_latency_notes = []
        significant_client_latency_increases = 0
        regression_details = {"test_name": test_name, "commands": []}

        for metric in client_metrics:
            # Build filters for client-side latency metric
            filters_baseline = [
                f"{by_str_baseline}={baseline_str}",
                f"metric={metric}",
                f"test_name={test_name}",
                f"deployment_name={baseline_deployment_name}",
                f"triggering_env={tf_triggering_env}",
            ]
            filters_comparison = [
                f"{by_str_comparison}={comparison_str}",
                f"metric={metric}",
                f"test_name={test_name}",
                f"deployment_name={comparison_deployment_name}",
                f"triggering_env={tf_triggering_env}",
            ]

            # Add optional filters
            if running_platform is not None:
                filters_baseline.append(f"running_platform={running_platform}")
                filters_comparison.append(f"running_platform={running_platform}")
            if baseline_architecture != ARCH_X86:
                filters_baseline.append(f"arch={baseline_architecture}")
            if comparison_architecture != ARCH_X86:
                filters_comparison.append(f"arch={comparison_architecture}")

            # Query for client-side latency time-series
            baseline_client_ts = rts.ts().queryindex(filters_baseline)
            comparison_client_ts = rts.ts().queryindex(filters_comparison)

            if len(baseline_client_ts) == 0 or len(comparison_client_ts) == 0:
                if verbose:
                    logging.info(
                        f"  No client-side data found for metric '{metric}' in {test_name}"
                    )
                continue

            logging.info(
                f"  Found client-side metric '{metric}': {len(baseline_client_ts)} baseline, {len(comparison_client_ts)} comparison time-series"
            )

            # Filter out target time-series
            baseline_client_ts = [ts for ts in baseline_client_ts if "target" not in ts]
            comparison_client_ts = [
                ts for ts in comparison_client_ts if "target" not in ts
            ]

            if len(baseline_client_ts) == 0 or len(comparison_client_ts) == 0:
                continue

            # Use the first available time-series for each side
            baseline_ts = baseline_client_ts[0]
            comparison_ts = comparison_client_ts[0]

            # Get client-side latency data
            baseline_client_data = rts.ts().revrange(baseline_ts, from_ts_ms, to_ts_ms)
            comparison_client_data = rts.ts().revrange(
                comparison_ts, from_ts_ms, to_ts_ms
            )

            if len(baseline_client_data) == 0 or len(comparison_client_data) == 0:
                if verbose:
                    logging.info(
                        f"  No data points for metric '{metric}': baseline={len(baseline_client_data)}, comparison={len(comparison_client_data)}"
                    )
                continue

            # Calculate client-side latency statistics
            baseline_client_values = []
            comparison_client_values = []

            (_, baseline_client_median, _) = get_v_pct_change_and_largest_var(
                baseline_client_data,
                0,
                0,
                baseline_client_values,
                0,
                last_n_baseline,
                verbose,
                first_n_baseline,
            )

            (_, comparison_client_median, _) = get_v_pct_change_and_largest_var(
                comparison_client_data,
                0,
                0,
                comparison_client_values,
                0,
                last_n_comparison,
                verbose,
                first_n_comparison,
            )

            if baseline_client_median == "N/A" or comparison_client_median == "N/A":
                if verbose:
                    logging.info(
                        f"  Could not calculate median for metric '{metric}': baseline={baseline_client_median}, comparison={comparison_client_median}"
                    )
                continue

            # Calculate variance (coefficient of variation) for both baseline and comparison
            baseline_client_mean = (
                statistics.mean(baseline_client_values) if baseline_client_values else 0
            )
            baseline_client_stdev = (
                statistics.stdev(baseline_client_values)
                if len(baseline_client_values) > 1
                else 0
            )
            baseline_client_cv = (
                (baseline_client_stdev / baseline_client_mean * 100)
                if baseline_client_mean > 0
                else float("inf")
            )

            comparison_client_mean = (
                statistics.mean(comparison_client_values)
                if comparison_client_values
                else 0
            )
            comparison_client_stdev = (
                statistics.stdev(comparison_client_values)
                if len(comparison_client_values) > 1
                else 0
            )
            comparison_client_cv = (
                (comparison_client_stdev / comparison_client_mean * 100)
                if comparison_client_mean > 0
                else float("inf")
            )

            # Calculate client-side latency change (for latency, higher is worse)
            client_latency_change = (
                float(comparison_client_median) / float(baseline_client_median) - 1
            ) * 100.0

            logging.info(
                f"  Client metric '{metric}': baseline={baseline_client_median:.2f} (CV={baseline_client_cv:.1f}%), comparison={comparison_client_median:.2f} (CV={comparison_client_cv:.1f}%), change={client_latency_change:.1f}%"
            )

            # Check if client latency data is too unstable to be reliable
            client_data_unstable = (
                baseline_client_cv > 50.0 or comparison_client_cv > 50.0
            )

            if client_data_unstable:
                # Mark as unstable client latency data
                unstable_reason = []
                if baseline_client_cv > 50.0:
                    unstable_reason.append(f"baseline CV={baseline_client_cv:.1f}%")
                if comparison_client_cv > 50.0:
                    unstable_reason.append(f"comparison CV={comparison_client_cv:.1f}%")

                client_latency_notes.append(
                    f"{metric} UNSTABLE ({', '.join(unstable_reason)} - data too noisy for reliable analysis)"
                )
                logging.warning(
                    f"  Client metric '{metric}': UNSTABLE latency data detected - {', '.join(unstable_reason)}"
                )
            elif (
                abs(client_latency_change) > 5.0
            ):  # Only report significant client latency changes for stable data
                direction = "increased" if client_latency_change > 0 else "decreased"

                # Adjust significance threshold based on baseline variance
                if baseline_client_cv < 30.0:
                    # Low variance - use standard threshold
                    significance_threshold = 10.0
                elif baseline_client_cv < 50.0:
                    # Moderate variance - require larger change
                    significance_threshold = 15.0
                else:
                    # High variance - require much larger change
                    significance_threshold = 25.0

                client_latency_notes.append(
                    f"{metric} {direction} {abs(client_latency_change):.1f}% (baseline CV={baseline_client_cv:.1f}%)"
                )
                logging.info(
                    f"  Client metric '{metric}': SIGNIFICANT latency change detected ({direction} {abs(client_latency_change):.1f}%, baseline CV={baseline_client_cv:.1f}%)"
                )

                # Track significant client latency increases (potential regression confirmation)
                if client_latency_change > significance_threshold:
                    significant_client_latency_increases += 1
                    regression_details["commands"].append(
                        {
                            "command": metric,
                            "change_percent": client_latency_change,
                            "direction": direction,
                            "baseline_cv": baseline_client_cv,
                            "comparison_cv": comparison_client_cv,
                        }
                    )
                    logging.info(
                        f"  Client metric '{metric}': CONFIRMS regression (change={client_latency_change:.1f}% > threshold={significance_threshold:.1f}%)"
                    )
                else:
                    logging.info(
                        f"  Client metric '{metric}': Change below significance threshold (change={client_latency_change:.1f}% <= threshold={significance_threshold:.1f}%)"
                    )
            elif verbose:
                client_latency_notes.append(
                    f"{metric} stable (CV={baseline_client_cv:.1f}%)"
                )
                logging.info(
                    f"  Client metric '{metric}': latency stable (change={client_latency_change:.1f}%, baseline CV={baseline_client_cv:.1f}%)"
                )

        # Determine if client-side latency confirms regression
        confirms_regression = significant_client_latency_increases > 0

        # Return combined client latency notes
        if client_latency_notes:
            result = "; ".join(client_latency_notes)
            logging.info(
                f"Client-side latency check completed for {test_name}: {result}"
            )
            return (
                result,
                confirms_regression,
                regression_details if confirms_regression else None,
            )
        else:
            result = "client latency stable" if len(client_metrics) > 0 else None
            logging.info(
                f"Client-side latency check completed for {test_name}: {result or 'no data'}"
            )
            return result, False, None

    except Exception as e:
        logging.error(f"Error checking client-side latency for {test_name}: {e}")
        return None, False, None


def perform_variance_and_p99_analysis(
    rts,
    test_name,
    baseline_str,
    comparison_str,
    by_str_baseline,
    by_str_comparison,
    baseline_deployment_name,
    comparison_deployment_name,
    tf_triggering_env,
    from_ts_ms,
    to_ts_ms,
    last_n_baseline,
    last_n_comparison,
    first_n_baseline,
    first_n_comparison,
    running_platform,
    baseline_architecture,
    comparison_architecture,
    verbose=False,
):
    """
    Perform 3rd-level analysis using variance and p99 metrics to assess confidence in regression detection.

    Returns:
        tuple: (confidence_note, high_confidence_bool)
    """
    try:
        logging.info(f"Starting variance and p99 analysis for {test_name}")

        # Build filters for p99 latency metric using both metric=p99 and metric-type=(latencystats)
        filters_baseline = [
            f"{by_str_baseline}={baseline_str}",
            "metric=p99",
            "metric-type=(latencystats)",
            f"test_name={test_name}",
            f"deployment_name={baseline_deployment_name}",
            f"triggering_env={tf_triggering_env}",
        ]
        filters_comparison = [
            f"{by_str_comparison}={comparison_str}",
            "metric=p99",
            "metric-type=(latencystats)",
            f"test_name={test_name}",
            f"deployment_name={comparison_deployment_name}",
            f"triggering_env={tf_triggering_env}",
        ]

        # Add optional filters
        if running_platform is not None:
            filters_baseline.append(f"running_platform={running_platform}")
            filters_comparison.append(f"running_platform={running_platform}")
        if baseline_architecture != ARCH_X86:
            filters_baseline.append(f"arch={baseline_architecture}")
        if comparison_architecture != ARCH_X86:
            filters_comparison.append(f"arch={comparison_architecture}")

        # Query for p99 latency time-series
        logging.info(f"Querying p99 latencystats time-series for {test_name}")
        baseline_p99_ts = rts.ts().queryindex(filters_baseline)
        comparison_p99_ts = rts.ts().queryindex(filters_comparison)

        logging.info(f"Found {len(baseline_p99_ts)} baseline p99 latency time-series")
        logging.info(
            f"Found {len(comparison_p99_ts)} comparison p99 latency time-series"
        )

        # Filter out target time-series and unwanted commands (reuse existing function)
        def should_exclude_timeseries(ts_name):
            """Check if time-series should be excluded based on command"""
            if "target" in ts_name:
                return True
            ts_name_lower = ts_name.lower()
            excluded_commands = ["config", "info", "ping", "cluster", "resetstat"]
            return any(cmd in ts_name_lower for cmd in excluded_commands)

        baseline_p99_ts = [
            ts for ts in baseline_p99_ts if not should_exclude_timeseries(ts)
        ]
        comparison_p99_ts = [
            ts for ts in comparison_p99_ts if not should_exclude_timeseries(ts)
        ]

        if len(baseline_p99_ts) == 0 or len(comparison_p99_ts) == 0:
            logging.warning(
                f"No p99 latency data found for {test_name} after filtering"
            )
            return None, False

        # Extract command names from time-series (reuse existing function)
        def extract_command_from_ts(ts_name):
            """Extract meaningful command name from time-series name"""
            # Look for latencystats_latency_percentiles_usec_<COMMAND>_p99 pattern
            match = re.search(
                r"latencystats_latency_percentiles_usec_([^_/]+)_p99", ts_name
            )
            if match:
                return match.group(1)
            # Look for command= pattern in the time-series name
            match = re.search(r"command=([^/]+)", ts_name)
            if match:
                return match.group(1)
            # If no specific pattern found, try to extract from the end of the path
            parts = ts_name.split("/")
            if len(parts) > 0:
                return parts[-1]
            return "unknown"

        # Group time-series by command
        baseline_by_command = {}
        comparison_by_command = {}

        for ts in baseline_p99_ts:
            cmd = extract_command_from_ts(ts)
            if cmd not in baseline_by_command:
                baseline_by_command[cmd] = []
            baseline_by_command[cmd].append(ts)

        for ts in comparison_p99_ts:
            cmd = extract_command_from_ts(ts)
            if cmd not in comparison_by_command:
                comparison_by_command[cmd] = []
            comparison_by_command[cmd].append(ts)

        # Find common commands between baseline and comparison
        common_commands = set(baseline_by_command.keys()) & set(
            comparison_by_command.keys()
        )

        if not common_commands:
            logging.warning(
                f"No common commands found for p99 variance analysis in {test_name}"
            )
            return None, False

        variance_notes = []
        p99_notes = []
        high_confidence_indicators = 0
        total_indicators = 0

        # Analyze variance and p99 for each command
        for command in sorted(common_commands):
            total_indicators += 1
            logging.info(f"Analyzing p99 variance for command: {command}")

            baseline_ts_list = baseline_by_command[command]
            comparison_ts_list = comparison_by_command[command]

            # If multiple time-series for the same command, try to get the best one
            if len(baseline_ts_list) > 1:
                baseline_ts_list = get_only_Totals(baseline_ts_list)
            if len(comparison_ts_list) > 1:
                comparison_ts_list = get_only_Totals(comparison_ts_list)

            if len(baseline_ts_list) != 1 or len(comparison_ts_list) != 1:
                logging.warning(
                    f"  Skipping {command}: baseline={len(baseline_ts_list)}, comparison={len(comparison_ts_list)} time-series"
                )
                continue

            # Get p99 latency data for this command
            baseline_p99_data = []
            comparison_p99_data = []

            for ts_name in baseline_ts_list:
                datapoints = rts.ts().revrange(ts_name, from_ts_ms, to_ts_ms)
                baseline_p99_data.extend(datapoints)

            for ts_name in comparison_ts_list:
                datapoints = rts.ts().revrange(ts_name, from_ts_ms, to_ts_ms)
                comparison_p99_data.extend(datapoints)

            if len(baseline_p99_data) < 3 or len(comparison_p99_data) < 3:
                logging.warning(
                    f"  Insufficient p99 data for {command}: baseline={len(baseline_p99_data)}, comparison={len(comparison_p99_data)} datapoints"
                )
                continue

            # Extract values for variance calculation
            baseline_values = [dp[1] for dp in baseline_p99_data]
            comparison_values = [dp[1] for dp in comparison_p99_data]

            # Calculate variance (coefficient of variation)
            baseline_mean = statistics.mean(baseline_values)
            baseline_stdev = (
                statistics.stdev(baseline_values) if len(baseline_values) > 1 else 0
            )
            baseline_cv = (
                (baseline_stdev / baseline_mean * 100)
                if baseline_mean > 0
                else float("inf")
            )

            comparison_mean = statistics.mean(comparison_values)
            comparison_stdev = (
                statistics.stdev(comparison_values) if len(comparison_values) > 1 else 0
            )
            comparison_cv = (
                (comparison_stdev / comparison_mean * 100)
                if comparison_mean > 0
                else float("inf")
            )

            # Calculate p99 change
            p99_change = (
                ((comparison_mean - baseline_mean) / baseline_mean * 100)
                if baseline_mean > 0
                else 0
            )

            # Assess confidence based on variance and p99 change
            if baseline_cv < 30:  # Low variance in baseline (< 30% CV)
                if abs(p99_change) > 15:  # Significant p99 change
                    high_confidence_indicators += 1
                    p99_notes.append(
                        f"{command} p99 {'+' if p99_change > 0 else ''}{p99_change:.1f}% (stable baseline)"
                    )
                else:
                    p99_notes.append(
                        f"{command} p99 {'+' if p99_change > 0 else ''}{p99_change:.1f}% (stable baseline, minor change)"
                    )
            elif baseline_cv < 50:  # Moderate variance
                if abs(p99_change) > 25:  # Need larger change for confidence
                    high_confidence_indicators += 1
                    p99_notes.append(
                        f"{command} p99 {'+' if p99_change > 0 else ''}{p99_change:.1f}% (moderate baseline variance)"
                    )
                else:
                    p99_notes.append(
                        f"{command} p99 {'+' if p99_change > 0 else ''}{p99_change:.1f}% (moderate baseline variance, uncertain)"
                    )
            else:  # High variance
                if abs(p99_change) > 40:  # Need very large change for confidence
                    high_confidence_indicators += 1
                    p99_notes.append(
                        f"{command} p99 {'+' if p99_change > 0 else ''}{p99_change:.1f}% (high baseline variance, large change)"
                    )
                else:
                    p99_notes.append(
                        f"{command} p99 {'+' if p99_change > 0 else ''}{p99_change:.1f}% (high baseline variance, low confidence)"
                    )

            variance_notes.append(f"{command} baseline CV={baseline_cv:.1f}%")

            if verbose:
                logging.info(
                    f"  Command {command}: baseline CV={baseline_cv:.1f}%, comparison CV={comparison_cv:.1f}%, p99 change={p99_change:.1f}%"
                )

        # Determine overall confidence
        confidence_ratio = (
            high_confidence_indicators / total_indicators if total_indicators > 0 else 0
        )
        high_confidence = (
            confidence_ratio >= 0.5
        )  # At least 50% of indicators show high confidence

        # Create confidence note
        confidence_parts = []
        if variance_notes:
            confidence_parts.extend(variance_notes)
        if p99_notes:
            confidence_parts.extend(p99_notes)

        confidence_note = "; ".join(confidence_parts) if confidence_parts else None

        if confidence_note:
            confidence_level = "HIGH" if high_confidence else "LOW"
            cv_explanation = "CV=coefficient of variation (data stability: <30% stable, 30-50% moderate, >50% unstable)"
            confidence_note = (
                f"confidence={confidence_level} ({confidence_note}; {cv_explanation})"
            )

        logging.info(
            f"Variance and p99 analysis completed for {test_name}: confidence={confidence_ratio:.2f}, high_confidence={high_confidence}"
        )
        return confidence_note, high_confidence

    except Exception as e:
        logging.error(f"Error in variance and p99 analysis for {test_name}: {e}")
        return None, False


def check_latency_for_unstable_throughput(
    rts,
    test_name,
    baseline_str,
    comparison_str,
    by_str_baseline,
    by_str_comparison,
    baseline_deployment_name,
    comparison_deployment_name,
    tf_triggering_env,
    from_ts_ms,
    to_ts_ms,
    last_n_baseline,
    last_n_comparison,
    first_n_baseline,
    first_n_comparison,
    running_platform,
    baseline_architecture,
    comparison_architecture,
    verbose,
):
    """
    Check latency (p50) for unstable throughput metrics to provide additional context.
    Returns a tuple: (note_string, confirms_regression_bool, regression_details_dict)
    """
    logging.info(f"Starting latency check for unstable throughput test: {test_name}")
    try:
        # Build filters for p50 latency metric using both metric=p50 and metric-type=(latencystats)
        filters_baseline = [
            f"{by_str_baseline}={baseline_str}",
            "metric=p50",
            "metric-type=(latencystats)",
            f"test_name={test_name}",
            f"deployment_name={baseline_deployment_name}",
            f"triggering_env={tf_triggering_env}",
        ]
        filters_comparison = [
            f"{by_str_comparison}={comparison_str}",
            "metric=p50",
            "metric-type=(latencystats)",
            f"test_name={test_name}",
            f"deployment_name={comparison_deployment_name}",
            f"triggering_env={tf_triggering_env}",
        ]

        # Add optional filters
        if running_platform is not None:
            filters_baseline.append(f"running_platform={running_platform}")
            filters_comparison.append(f"running_platform={running_platform}")
        if baseline_architecture != ARCH_X86:
            filters_baseline.append(f"arch={baseline_architecture}")
        if comparison_architecture != ARCH_X86:
            filters_comparison.append(f"arch={comparison_architecture}")

        # Query for p50 latency time-series
        logging.info(f"Querying p50 latencystats time-series for {test_name}")
        logging.info(f"Baseline filters: {filters_baseline}")
        logging.info(f"Comparison filters: {filters_comparison}")

        baseline_latency_ts = rts.ts().queryindex(filters_baseline)
        comparison_latency_ts = rts.ts().queryindex(filters_comparison)

        logging.info(
            f"Found {len(baseline_latency_ts)} baseline p50 latency time-series"
        )
        logging.info(
            f"Found {len(comparison_latency_ts)} comparison p50 latency time-series"
        )

        if verbose and baseline_latency_ts:
            logging.info(f"Baseline latency time-series: {baseline_latency_ts}")
        if verbose and comparison_latency_ts:
            logging.info(f"Comparison latency time-series: {comparison_latency_ts}")

        # Filter out target time-series and unwanted commands
        def should_exclude_timeseries(ts_name):
            """Check if time-series should be excluded based on command"""
            # Exclude target time-series
            if "target" in ts_name:
                return True

            # Convert to lowercase for case-insensitive matching
            ts_name_lower = ts_name.lower()

            # Exclude administrative commands (case-insensitive)
            excluded_commands = ["config", "info", "ping", "cluster", "resetstat"]
            return any(cmd in ts_name_lower for cmd in excluded_commands)

        baseline_latency_ts_before = len(baseline_latency_ts)
        comparison_latency_ts_before = len(comparison_latency_ts)

        # Apply filtering and log what gets excluded
        baseline_excluded = [
            ts for ts in baseline_latency_ts if should_exclude_timeseries(ts)
        ]
        comparison_excluded = [
            ts for ts in comparison_latency_ts if should_exclude_timeseries(ts)
        ]

        baseline_latency_ts = [
            ts for ts in baseline_latency_ts if not should_exclude_timeseries(ts)
        ]
        comparison_latency_ts = [
            ts for ts in comparison_latency_ts if not should_exclude_timeseries(ts)
        ]

        logging.info(
            f"After filtering: baseline {baseline_latency_ts_before} -> {len(baseline_latency_ts)}, "
            f"comparison {comparison_latency_ts_before} -> {len(comparison_latency_ts)}"
        )

        if baseline_excluded:
            logging.info(
                f"Excluded {len(baseline_excluded)} baseline administrative command time-series"
            )
            if verbose:
                for ts in baseline_excluded:
                    logging.info(f"  Excluded baseline: {ts}")
        if comparison_excluded:
            logging.info(
                f"Excluded {len(comparison_excluded)} comparison administrative command time-series"
            )
            if verbose:
                for ts in comparison_excluded:
                    logging.info(f"  Excluded comparison: {ts}")

        if len(baseline_latency_ts) == 0 or len(comparison_latency_ts) == 0:
            logging.warning(
                f"No p50 latency data found for {test_name} after filtering"
            )
            return None, False, None

        # Extract command names from time-series to match baseline and comparison
        def extract_command_from_ts(ts_name):
            """Extract meaningful command name from time-series name"""
            import re

            # Look for latencystats_latency_percentiles_usec_<COMMAND>_p50 pattern
            match = re.search(
                r"latencystats_latency_percentiles_usec_([^_/]+)_p50", ts_name
            )
            if match:
                return match.group(1)

            # Look for command= pattern in the time-series name
            match = re.search(r"command=([^/]+)", ts_name)
            if match:
                return match.group(1)

            # If no specific pattern found, try to extract from the end of the path
            # e.g., .../Ops/sec/GET -> GET
            parts = ts_name.split("/")
            if len(parts) > 0:
                return parts[-1]
            return "unknown"

        # Group time-series by command
        baseline_by_command = {}
        comparison_by_command = {}

        for ts in baseline_latency_ts:
            cmd = extract_command_from_ts(ts)
            if verbose:
                logging.info(f"Baseline time-series '{ts}' -> command '{cmd}'")
            if cmd not in baseline_by_command:
                baseline_by_command[cmd] = []
            baseline_by_command[cmd].append(ts)

        for ts in comparison_latency_ts:
            cmd = extract_command_from_ts(ts)
            if verbose:
                logging.info(f"Comparison time-series '{ts}' -> command '{cmd}'")
            if cmd not in comparison_by_command:
                comparison_by_command[cmd] = []
            comparison_by_command[cmd].append(ts)

        # Find common commands between baseline and comparison
        common_commands = set(baseline_by_command.keys()) & set(
            comparison_by_command.keys()
        )

        logging.info(f"Baseline commands found: {sorted(baseline_by_command.keys())}")
        logging.info(
            f"Comparison commands found: {sorted(comparison_by_command.keys())}"
        )
        logging.info(
            f"Common commands for latency comparison: {sorted(common_commands)}"
        )

        if not common_commands:
            logging.warning(
                f"No common commands found for latency comparison in {test_name}"
            )
            return None, False, None

        latency_notes = []
        significant_latency_increases = (
            0  # Track commands with significant latency increases
        )
        regression_details = {"test_name": test_name, "commands": []}

        # Compare latency for each command individually
        for command in sorted(common_commands):
            logging.info(f"Analyzing latency for command: {command}")
            baseline_ts_list = baseline_by_command[command]
            comparison_ts_list = comparison_by_command[command]

            logging.info(
                f"  Command {command}: {len(baseline_ts_list)} baseline, {len(comparison_ts_list)} comparison time-series"
            )

            # If multiple time-series for the same command, try to get the best one
            if len(baseline_ts_list) > 1:
                logging.info(
                    f"  Multiple baseline time-series for {command}, filtering..."
                )
                baseline_ts_list = get_only_Totals(baseline_ts_list)
            if len(comparison_ts_list) > 1:
                logging.info(
                    f"  Multiple comparison time-series for {command}, filtering..."
                )
                comparison_ts_list = get_only_Totals(comparison_ts_list)

            if len(baseline_ts_list) != 1 or len(comparison_ts_list) != 1:
                logging.warning(
                    f"  Skipping {command}: baseline={len(baseline_ts_list)}, comparison={len(comparison_ts_list)} time-series"
                )
                continue

            # Get latency data for this command
            baseline_latency_data = []
            comparison_latency_data = []

            for ts_name in baseline_ts_list:
                datapoints = rts.ts().revrange(ts_name, from_ts_ms, to_ts_ms)
                baseline_latency_data.extend(datapoints)

            for ts_name in comparison_ts_list:
                datapoints = rts.ts().revrange(ts_name, from_ts_ms, to_ts_ms)
                comparison_latency_data.extend(datapoints)

            if len(baseline_latency_data) == 0 or len(comparison_latency_data) == 0:
                logging.warning(
                    f"  No latency data for {command}: baseline={len(baseline_latency_data)}, comparison={len(comparison_latency_data)} datapoints"
                )
                continue

            logging.info(
                f"  Command {command}: {len(baseline_latency_data)} baseline, {len(comparison_latency_data)} comparison datapoints"
            )

            # Calculate latency statistics for this command
            baseline_latency_values = []
            comparison_latency_values = []

            (_, baseline_latency_median, _) = get_v_pct_change_and_largest_var(
                baseline_latency_data,
                0,
                0,
                baseline_latency_values,
                0,
                last_n_baseline,
                verbose,
                first_n_baseline,
            )

            (_, comparison_latency_median, _) = get_v_pct_change_and_largest_var(
                comparison_latency_data,
                0,
                0,
                comparison_latency_values,
                0,
                last_n_comparison,
                verbose,
                first_n_comparison,
            )

            if baseline_latency_median == "N/A" or comparison_latency_median == "N/A":
                logging.warning(
                    f"  Could not calculate median for {command}: baseline={baseline_latency_median}, comparison={comparison_latency_median}"
                )
                continue

            # Calculate variance (coefficient of variation) for both baseline and comparison
            baseline_latency_mean = (
                statistics.mean(baseline_latency_values)
                if baseline_latency_values
                else 0
            )
            baseline_latency_stdev = (
                statistics.stdev(baseline_latency_values)
                if len(baseline_latency_values) > 1
                else 0
            )
            baseline_latency_cv = (
                (baseline_latency_stdev / baseline_latency_mean * 100)
                if baseline_latency_mean > 0
                else float("inf")
            )

            comparison_latency_mean = (
                statistics.mean(comparison_latency_values)
                if comparison_latency_values
                else 0
            )
            comparison_latency_stdev = (
                statistics.stdev(comparison_latency_values)
                if len(comparison_latency_values) > 1
                else 0
            )
            comparison_latency_cv = (
                (comparison_latency_stdev / comparison_latency_mean * 100)
                if comparison_latency_mean > 0
                else float("inf")
            )

            # Calculate latency change (for latency, lower is better)
            latency_change = (
                float(comparison_latency_median) / float(baseline_latency_median) - 1
            ) * 100.0

            logging.info(
                f"  Command {command}: baseline p50={baseline_latency_median:.2f} (CV={baseline_latency_cv:.1f}%), comparison p50={comparison_latency_median:.2f} (CV={comparison_latency_cv:.1f}%), change={latency_change:.1f}%"
            )

            # Check if latency data is too unstable to be reliable
            latency_data_unstable = (
                baseline_latency_cv > 50.0 or comparison_latency_cv > 50.0
            )

            if latency_data_unstable:
                # Mark as unstable latency data
                unstable_reason = []
                if baseline_latency_cv > 50.0:
                    unstable_reason.append(f"baseline CV={baseline_latency_cv:.1f}%")
                if comparison_latency_cv > 50.0:
                    unstable_reason.append(
                        f"comparison CV={comparison_latency_cv:.1f}%"
                    )

                latency_notes.append(
                    f"{command} p50 UNSTABLE ({', '.join(unstable_reason)} - data too noisy for reliable analysis)"
                )
                logging.warning(
                    f"  Command {command}: UNSTABLE latency data detected - {', '.join(unstable_reason)}"
                )
            elif (
                abs(latency_change) > 5.0
            ):  # Only report significant latency changes for stable data
                direction = "increased" if latency_change > 0 else "decreased"

                # Adjust significance threshold based on baseline variance
                if baseline_latency_cv < 30.0:
                    # Low variance - use standard threshold
                    significance_threshold = 10.0
                elif baseline_latency_cv < 50.0:
                    # Moderate variance - require larger change
                    significance_threshold = 15.0
                else:
                    # High variance - require much larger change
                    significance_threshold = 25.0

                latency_notes.append(
                    f"{command} p50 {direction} {abs(latency_change):.1f}% (baseline CV={baseline_latency_cv:.1f}%)"
                )
                logging.info(
                    f"  Command {command}: SIGNIFICANT latency change detected ({direction} {abs(latency_change):.1f}%, baseline CV={baseline_latency_cv:.1f}%)"
                )

                # Track significant latency increases (potential regression confirmation)
                if latency_change > significance_threshold:
                    significant_latency_increases += 1
                    regression_details["commands"].append(
                        {
                            "command": command,
                            "change_percent": latency_change,
                            "direction": direction,
                            "baseline_cv": baseline_latency_cv,
                            "comparison_cv": comparison_latency_cv,
                        }
                    )
                    logging.info(
                        f"  Command {command}: CONFIRMS regression (change={latency_change:.1f}% > threshold={significance_threshold:.1f}%)"
                    )
                else:
                    logging.info(
                        f"  Command {command}: Change below significance threshold (change={latency_change:.1f}% <= threshold={significance_threshold:.1f}%)"
                    )
            elif verbose:
                latency_notes.append(
                    f"{command} p50 stable (CV={baseline_latency_cv:.1f}%)"
                )
                logging.info(
                    f"  Command {command}: latency stable (change={latency_change:.1f}%, baseline CV={baseline_latency_cv:.1f}%)"
                )

        # Determine if latency confirms regression
        confirms_regression = significant_latency_increases > 0

        # Return combined latency notes
        if latency_notes:
            result = "; ".join(latency_notes)
            logging.info(f"Latency check completed for {test_name}: {result}")
            return (
                result,
                confirms_regression,
                regression_details if confirms_regression else None,
            )
        else:
            result = "p50 latency stable" if common_commands else None
            logging.info(
                f"Latency check completed for {test_name}: {result or 'no data'}"
            )
            return result, False, None

    except Exception as e:
        logging.error(f"Error checking latency for {test_name}: {e}")
        return None, False, None


def get_only_Totals(baseline_timeseries):
    logging.warning("\t\tTime-series: {}".format(", ".join(baseline_timeseries)))
    logging.info("Checking if Totals will reduce timeseries.")
    new_base = []
    for ts_name in baseline_timeseries:
        if "Totals" in ts_name:
            new_base.append(ts_name)

    # If no "Totals" time-series found, try to pick the best alternative
    if len(new_base) == 0:
        logging.warning(
            "No 'Totals' time-series found, trying to pick best alternative."
        )
        # Prefer time-series without quotes in metric names
        unquoted_series = [ts for ts in baseline_timeseries if "'" not in ts]
        if unquoted_series:
            new_base = unquoted_series
        else:
            # Fall back to original list
            new_base = baseline_timeseries

    # If we still have multiple time-series after filtering for "Totals",
    # prefer the one without quotes in the metric name
    if len(new_base) > 1:
        logging.info("Multiple time-series found, preferring unquoted metric names.")
        unquoted_series = [ts for ts in new_base if "'" not in ts]
        if unquoted_series:
            new_base = unquoted_series

        # If we still have multiple, take the first one
        if len(new_base) > 1:
            logging.warning(
                "Still multiple time-series after filtering, taking the first one: {}".format(
                    new_base[0]
                )
            )
            new_base = [new_base[0]]

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
    grafana_link_base=None,
    baseline_branch=None,
    baseline_version=None,
    comparison_branch=None,
    comparison_version=None,
    from_date=None,
    to_date=None,
):
    grafana_link = None
    if grafana_link_base is not None:
        grafana_link = "{}?orgId=1".format(grafana_link_base)
        grafana_link += f"&var-test_case={test_name}"

        if baseline_branch is not None:
            grafana_link += f"&var-branch={baseline_branch}"
        if baseline_version is not None:
            grafana_link += f"&var-version={baseline_version}"
        if comparison_branch is not None:
            grafana_link += f"&var-branch={comparison_branch}"
        if comparison_version is not None:
            grafana_link += f"&var-version={comparison_version}"
        grafana_link += "&from=now-30d&to=now"

    # Create test name with optional Grafana link
    test_name_display = test_name
    if grafana_link is not None:
        test_name_display = f"[{test_name}]({grafana_link})"

    percentage_change_str = "{:.1f}% ".format(percentage_change)
    table.append(
        [
            test_name_display,
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
        comparison_median = float(comparison_df.median().iloc[0])
        comparison_v = comparison_median
        comparison_std = float(comparison_df.std().iloc[0])
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
