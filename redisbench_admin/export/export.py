#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#


import json
import logging

import redis
from redistimeseries.client import Client

from redisbench_admin.export.common.common import split_tags_string
from redisbench_admin.run.git import git_vars_crosscheck

from redisbench_admin.run.redistimeseries import timeseries_test_sucess_flow
from redisbench_admin.utils.benchmark_config import (
    get_defaults,
    parse_exporter_timemetric,
)


def export_command_logic(args, project_name, project_version):
    logging.info(
        "Using: {project_name} {project_version}".format(
            project_name=project_name, project_version=project_version
        )
    )
    benchmark_file = args.benchmark_result_file
    results_format = args.results_format
    (_, tf_github_branch, tf_github_org, tf_github_repo, _,) = git_vars_crosscheck(
        None, args.github_branch, args.github_org, args.github_repo, None
    )
    results_dict = {}
    if results_format == "json":
        with open(benchmark_file, "r") as json_file:
            results_dict = json.load(json_file)
    extra_tags_array = split_tags_string(args.extra_tags)
    extra_tags_dict = {}
    for kv_pair in extra_tags_array:
        kv_tuple = kv_pair.split("=")
        if len(kv_tuple) < 2:
            pass
        key = kv_tuple[0]
        value = kv_tuple[1]
        extra_tags_dict[key] = value
    logging.info("Using the following extra tags: {}".format(extra_tags_dict))

    logging.info("Checking connection to RedisTimeSeries.")
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
    exporter_spec_file = args.exporter_spec_file
    (
        _,
        metrics,
        exporter_timemetric_path,
        _,
        _,
    ) = get_defaults(exporter_spec_file)

    datapoints_timestamp = parse_exporter_timemetric(
        exporter_timemetric_path, results_dict
    )

    timeseries_test_sucess_flow(
        True,
        args.deployment_version,
        None,
        benchmark_duration_seconds,
        None,
        metrics,
        args.deployment_name,
        args.deployment_type,
        exporter_timemetric_path,
        results_dict,
        rts,
        datapoints_timestamp,
        args.test_name,
        tf_github_branch,
        tf_github_org,
        tf_github_repo,
        args.triggering_env,
        extra_tags_dict,
        None,
        None,
    )
