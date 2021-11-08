#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import csv
import json
import logging
import datetime

import redis
from redistimeseries.client import Client

from redisbench_admin.export.common.common import split_tags_string
from redisbench_admin.run.git import git_vars_crosscheck

from redisbench_admin.run.redistimeseries import timeseries_test_sucess_flow
from redisbench_admin.utils.benchmark_config import (
    get_defaults,
    parse_exporter_timemetric,
)
from redisbench_admin.utils.remote import get_ts_tags_and_name


def export_command_logic(args, project_name, project_version):
    logging.info(
        "Using: {project_name} {project_version}".format(
            project_name=project_name, project_version=project_version
        )
    )
    timeseries_dict = None
    benchmark_file = args.benchmark_result_file
    test_name = args.test_name
    deployment_version = args.deployment_version
    triggering_env = args.triggering_env
    deployment_name = args.deployment_name
    deployment_type = args.deployment_type
    results_format = args.results_format
    (_, github_branch, github_org, github_repo, _,) = git_vars_crosscheck(
        None, args.github_branch, args.github_org, args.github_repo, None
    )
    exporter_timemetric_path = None
    metrics = []
    exporter_spec_file = args.exporter_spec_file
    if github_branch is None and deployment_version is None:
        logging.error(
            "You need to specify at least one (or more) of --deployment-version --github_branch arguments"
        )
        exit(1)
    if results_format != "csv":
        if exporter_spec_file is None:
            logging.error(
                "--exporter-spec-file is required for all formats with exception of csv"
            )
            exit(1)
        else:
            (
                _,
                metrics,
                exporter_timemetric_path,
                _,
                _,
            ) = get_defaults(exporter_spec_file)

    extra_tags_dict = split_tags_string(args.extra_tags)
    logging.info("Using the following extra tags: {}".format(extra_tags_dict))

    results_dict = {}
    if results_format == "json":
        with open(benchmark_file, "r") as json_file:
            results_dict = json.load(json_file)
    if args.override_test_time:
        datapoints_timestamp = int(args.override_test_time.timestamp() * 1000.0)
        logging.info(
            "Overriding test time with the following date {}. Timestamp {}".format(
                args.override_test_time, datapoints_timestamp
            )
        )
    else:
        logging.info(
            "Trying to parse the time-metric from path {}".format(
                exporter_timemetric_path
            )
        )
        datapoints_timestamp = parse_exporter_timemetric(
            exporter_timemetric_path, results_dict
        )
        if datapoints_timestamp is None:
            datapoints_timestamp = int(
                datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000.0
            )
            logging.warning(
                "Error while trying to parse datapoints timestamp. Using current system timestamp Error: {}".format(
                    datapoints_timestamp
                )
            )
    break_by_dict = {}
    if deployment_version is not None:
        break_by_dict.update({"version": deployment_version})
    if github_branch is not None:
        break_by_dict.update({"branch": github_branch})
    if results_format == "csv":
        logging.info("Parsing CSV format from {}".format(benchmark_file))
        timeseries_dict = export_opereto_csv_to_timeseries_dict(
            benchmark_file,
            break_by_dict,
            datapoints_timestamp,
            deployment_name,
            deployment_type,
            extra_tags_dict,
            github_org,
            github_repo,
            triggering_env,
        )
        logging.info("Parsed a total of {} metrics".format(len(timeseries_dict.keys())))
    logging.info(
        "Checking connection to RedisTimeSeries to host: {}:{}".format(
            args.redistimeseries_host, args.redistimeseries_port
        )
    )
    rts = Client(
        host=args.redistimeseries_host,
        port=args.redistimeseries_port,
        password=args.redistimeseries_pass,
    )
    try:
        rts.redis.ping()
    except redis.exceptions.ConnectionError as e:
        logging.error(
            "Error while connecting to RedisTimeSeries data sink at: {}:{}. Error: {}".format(
                args.redistimeseries_host, args.redistimeseries_port, e.__str__()
            )
        )
        exit(1)

    benchmark_duration_seconds = None

    timeseries_test_sucess_flow(
        True,
        deployment_version,
        None,
        benchmark_duration_seconds,
        None,
        metrics,
        deployment_name,
        deployment_type,
        exporter_timemetric_path,
        results_dict,
        rts,
        datapoints_timestamp,
        test_name,
        github_branch,
        github_org,
        github_repo,
        triggering_env,
        extra_tags_dict,
        None,
        None,
        timeseries_dict,
    )


def export_opereto_csv_to_timeseries_dict(
    benchmark_file,
    break_by_dict,
    datapoints_timestamp,
    deployment_name,
    deployment_type,
    extra_tags_dict,
    tf_github_org,
    tf_github_repo,
    triggering_env,
):
    results_dict = {}
    with open(benchmark_file, "r") as csv_file:
        full_csv = list(csv.reader(csv_file, delimiter=",", quoting=csv.QUOTE_ALL))
        if len(full_csv) >= 2:
            header = full_csv[0]
            metrics = header[1:]
            for row in full_csv[1:]:
                assert len(row) == len(header)
                test_name = row[0]
                for metric_pos, metric_name in enumerate(metrics):
                    metric_v_pos = 1 + metric_pos
                    if metric_v_pos < len(row):
                        metric_value = row[metric_v_pos]

                        for break_by_key, break_by_value in break_by_dict.items():
                            break_by_str = "by.{}".format(break_by_key)
                            timeserie_tags, ts_name = get_ts_tags_and_name(
                                break_by_key,
                                break_by_str,
                                break_by_value,
                                None,
                                deployment_name,
                                deployment_type,
                                extra_tags_dict,
                                metric_name,
                                metric_name,
                                metric_name,
                                triggering_env,
                                test_name,
                                metric_name,
                                tf_github_org,
                                tf_github_repo,
                                triggering_env,
                                False,
                            )
                            results_dict[ts_name] = {
                                "labels": timeserie_tags.copy(),
                                "data": {datapoints_timestamp: metric_value},
                            }
    return results_dict
