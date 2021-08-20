#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import logging

import redis

from redisbench_admin.run.common import (
    merge_default_and_config_metrics,
    common_exporter_logic,
    get_start_time_vars,
)
from redisbench_admin.utils.remote import (
    get_project_ts_tags,
    get_overall_dashboard_keynames,
)
from redisbench_admin.utils.utils import get_ts_metric_name


def redistimeseries_results_logic(
    artifact_version,
    benchmark_config,
    default_metrics,
    deployment_type,
    exporter_timemetric_path,
    results_dict,
    rts,
    test_name,
    tf_github_branch,
    tf_github_org,
    tf_github_repo,
    tf_triggering_env,
    metadata_tags={},
    build_variant_name=None,
    running_platform=None,
):
    # check which metrics to extract
    exporter_timemetric_path, metrics = merge_default_and_config_metrics(
        benchmark_config, default_metrics, exporter_timemetric_path
    )
    (
        per_version_time_series_dict,
        per_branch_time_series_dict,
        testcase_metric_context_paths,
    ) = common_exporter_logic(
        deployment_type,
        exporter_timemetric_path,
        metrics,
        results_dict,
        rts,
        test_name,
        tf_github_branch,
        tf_github_org,
        tf_github_repo,
        tf_triggering_env,
        artifact_version,
        metadata_tags,
        build_variant_name,
        running_platform,
    )
    return (
        per_version_time_series_dict,
        per_branch_time_series_dict,
        testcase_metric_context_paths,
    )


def add_standardized_metric_bybranch(
    metric_name,
    metric_value,
    tf_github_branch,
    deployment_type,
    rts,
    start_time_ms,
    test_name,
    tf_github_org,
    tf_github_repo,
    tf_triggering_env,
    metadata_tags={},
    build_variant_name=None,
    running_platform=None,
):
    tsname_use_case_duration = get_ts_metric_name(
        "by.branch",
        tf_github_branch,
        tf_github_org,
        tf_github_repo,
        deployment_type,
        test_name,
        tf_triggering_env,
        metric_name,
        None,
        False,
        build_variant_name,
        running_platform,
    )
    labels = get_project_ts_tags(
        tf_github_org,
        tf_github_repo,
        deployment_type,
        tf_triggering_env,
        metadata_tags,
        build_variant_name,
        running_platform,
    )
    labels["branch"] = tf_github_branch
    labels["test_name"] = str(test_name)
    labels["metric"] = str(metric_name)
    logging.info(
        "Adding metric {}={} to time-serie named {}".format(
            metric_name, metric_value, tsname_use_case_duration
        )
    )
    try:
        logging.info(
            "Creating timeseries named {} with labels {}".format(
                tsname_use_case_duration, labels
            )
        )
        rts.create(tsname_use_case_duration, labels=labels)
    except redis.exceptions.ResponseError:
        logging.warning(
            "Timeseries named {} already exists".format(tsname_use_case_duration)
        )
        pass
    rts.add(
        tsname_use_case_duration,
        start_time_ms,
        metric_value,
        labels=labels,
    )


def add_standardized_metric_byversion(
    metric_name,
    metric_value,
    artifact_version,
    deployment_type,
    rts,
    start_time_ms,
    test_name,
    tf_github_org,
    tf_github_repo,
    tf_triggering_env,
    metadata_tags={},
    build_variant_name=None,
    running_platform=None,
):
    tsname_use_case_duration = get_ts_metric_name(
        "by.version",
        artifact_version,
        tf_github_org,
        tf_github_repo,
        deployment_type,
        test_name,
        tf_triggering_env,
        metric_name,
        None,
        False,
        build_variant_name,
        running_platform,
    )
    labels = get_project_ts_tags(
        tf_github_org,
        tf_github_repo,
        deployment_type,
        tf_triggering_env,
        metadata_tags,
        build_variant_name,
    )
    labels["version"] = artifact_version
    labels["test_name"] = str(test_name)
    labels["metric"] = str(metric_name)
    logging.info(
        "Adding metric {}={} to time-serie named {}".format(
            metric_name, metric_value, tsname_use_case_duration
        )
    )
    try:
        logging.info(
            "Creating timeseries named {} with labels {}".format(
                tsname_use_case_duration, labels
            )
        )
        rts.create(tsname_use_case_duration, labels=labels)
    except redis.exceptions.ResponseError:
        logging.warning(
            "Timeseries named {} already exists".format(tsname_use_case_duration)
        )
        pass
    rts.add(
        tsname_use_case_duration,
        start_time_ms,
        metric_value,
        labels=labels,
    )


def timeseries_test_sucess_flow(
    push_results_redistimeseries,
    artifact_version,
    benchmark_config,
    benchmark_duration_seconds,
    dataset_load_duration_seconds,
    default_metrics,
    deployment_type,
    exporter_timemetric_path,
    results_dict,
    rts,
    start_time_ms,
    test_name,
    tf_github_branch,
    tf_github_org,
    tf_github_repo,
    tf_triggering_env,
    metadata_tags={},
    build_variant_name=None,
    running_platform=None,
):
    if push_results_redistimeseries:
        logging.info("Pushing results to RedisTimeSeries.")
        _, _, testcase_metric_context_paths = redistimeseries_results_logic(
            artifact_version,
            benchmark_config,
            default_metrics,
            deployment_type,
            exporter_timemetric_path,
            results_dict,
            rts,
            test_name,
            tf_github_branch,
            tf_github_org,
            tf_github_repo,
            tf_triggering_env,
            metadata_tags,
            build_variant_name,
            running_platform,
        )
        (
            _,
            testcases_setname,
            _,
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
            build_variant_name,
            running_platform,
            test_name,
        )

        try:
            rts.redis.sadd(testcases_setname, test_name)
            if "arch" in metadata_tags:
                rts.redis.sadd(project_archs_setname, metadata_tags["arch"])
            if "os" in metadata_tags:
                rts.redis.sadd(project_oss_setname, metadata_tags["os"])
            if "compiler" in metadata_tags:
                rts.redis.sadd(project_compilers_setname, metadata_tags["compiler"])
            if tf_github_branch is not None and tf_github_branch != "":
                rts.redis.sadd(project_branches_setname, tf_github_branch)
            if artifact_version is not None and artifact_version != "":
                rts.redis.sadd(project_versions_setname, artifact_version)

            if running_platform is not None:
                rts.redis.sadd(running_platforms_setname, running_platform)
            if build_variant_name is not None:
                rts.redis.sadd(build_variant_setname, build_variant_name)
            for metric_context_path in testcase_metric_context_paths:
                rts.redis.sadd(
                    testcases_metric_context_path_setname, metric_context_path
                )
                rts.redis.sadd(
                    testcases_and_metric_context_path_setname,
                    "{}:{}".format(test_name, metric_context_path),
                )
            rts.incrby(
                tsname_project_total_success,
                1,
                timestamp=start_time_ms,
                labels=get_project_ts_tags(
                    tf_github_org,
                    tf_github_repo,
                    deployment_type,
                    tf_triggering_env,
                    metadata_tags,
                    build_variant_name,
                    running_platform,
                ),
            )
            if tf_github_branch is not None and tf_github_branch != "":
                add_standardized_metric_bybranch(
                    "benchmark_duration",
                    benchmark_duration_seconds,
                    str(tf_github_branch),
                    deployment_type,
                    rts,
                    start_time_ms,
                    test_name,
                    tf_github_org,
                    tf_github_repo,
                    tf_triggering_env,
                    metadata_tags,
                    build_variant_name,
                    running_platform,
                )
                add_standardized_metric_bybranch(
                    "dataset_load_duration",
                    dataset_load_duration_seconds,
                    str(tf_github_branch),
                    deployment_type,
                    rts,
                    start_time_ms,
                    test_name,
                    tf_github_org,
                    tf_github_repo,
                    tf_triggering_env,
                    metadata_tags,
                    build_variant_name,
                    running_platform,
                )
            if artifact_version is not None and artifact_version != "":
                add_standardized_metric_byversion(
                    "benchmark_duration",
                    benchmark_duration_seconds,
                    artifact_version,
                    deployment_type,
                    rts,
                    start_time_ms,
                    test_name,
                    tf_github_org,
                    tf_github_repo,
                    tf_triggering_env,
                    metadata_tags,
                    build_variant_name,
                    running_platform,
                )
                add_standardized_metric_byversion(
                    "dataset_load_duration",
                    dataset_load_duration_seconds,
                    artifact_version,
                    deployment_type,
                    rts,
                    start_time_ms,
                    test_name,
                    tf_github_org,
                    tf_github_repo,
                    tf_triggering_env,
                    metadata_tags,
                    build_variant_name,
                    running_platform,
                )
        except redis.exceptions.ResponseError as e:
            logging.warning(
                "Error while updating secondary data structures {}. ".format(
                    e.__str__()
                )
            )
            pass


def timeseries_test_failure_flow(
    args,
    deployment_type,
    rts,
    start_time_ms,
    tf_github_org,
    tf_github_repo,
    tf_triggering_env,
    tsname_project_total_failures,
):
    if args.push_results_redistimeseries:
        if start_time_ms is None:
            _, start_time_ms, _ = get_start_time_vars()
        try:
            rts.incrby(
                tsname_project_total_failures,
                1,
                timestamp=start_time_ms,
                labels=get_project_ts_tags(
                    tf_github_org,
                    tf_github_repo,
                    deployment_type,
                    tf_triggering_env,
                ),
            )
        except redis.exceptions.ResponseError as e:
            logging.warning(
                "Error while updating secondary data structures {}. ".format(
                    e.__str__()
                )
            )
            pass
