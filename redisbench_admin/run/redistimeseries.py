#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import logging

import redis

from redisbench_admin.profilers.profilers_schema import get_profilers_rts_key_prefix
from redisbench_admin.run.common import (
    merge_default_and_config_metrics,
    common_exporter_logic,
    get_start_time_vars,
)
from redisbench_admin.utils.remote import (
    get_project_ts_tags,
    get_overall_dashboard_keynames,
    exporter_create_ts,
    push_data_to_redistimeseries,
)
from redisbench_admin.utils.utils import get_ts_metric_name


def prepare_timeseries_dict(
    artifact_version,
    benchmark_config,
    default_metrics,
    deployment_name,
    deployment_type,
    exporter_timemetric_path,
    results_dict,
    test_name,
    tf_github_branch,
    tf_github_org,
    tf_github_repo,
    tf_triggering_env,
    metadata_tags={},
    build_variant_name=None,
    running_platform=None,
    datapoints_timestamp=None,
):
    time_series_dict = {}
    # check which metrics to extract
    exporter_timemetric_path, metrics = merge_default_and_config_metrics(
        benchmark_config, default_metrics, exporter_timemetric_path
    )
    (
        per_version_time_series_dict,
        per_branch_time_series_dict,
        testcase_metric_context_paths,
        version_target_tables,
        branch_target_tables,
    ) = common_exporter_logic(
        deployment_name,
        deployment_type,
        exporter_timemetric_path,
        metrics,
        results_dict,
        test_name,
        tf_github_branch,
        tf_github_org,
        tf_github_repo,
        tf_triggering_env,
        artifact_version,
        metadata_tags,
        build_variant_name,
        running_platform,
        datapoints_timestamp,
    )
    time_series_dict.update(per_version_time_series_dict)
    time_series_dict.update(per_branch_time_series_dict)
    return (
        time_series_dict,
        testcase_metric_context_paths,
        version_target_tables,
        branch_target_tables,
    )


def add_standardized_metric_bybranch(
    metric_name,
    metric_value,
    tf_github_branch,
    deployment_name,
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
    if metric_value is not None:
        tsname_use_case_duration = get_ts_metric_name(
            "by.branch",
            tf_github_branch,
            tf_github_org,
            tf_github_repo,
            deployment_name,
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
            deployment_name,
            deployment_type,
            tf_triggering_env,
            metadata_tags,
            build_variant_name,
            running_platform,
        )
        labels["branch"] = tf_github_branch
        labels["deployment_name+branch"] = "{} {}".format(
            deployment_name, tf_github_branch
        )
        labels["test_name"] = str(test_name)
        labels["metric"] = str(metric_name)
        logging.info(
            "Adding metric {}={} to time-serie named {}".format(
                metric_name, metric_value, tsname_use_case_duration
            )
        )
        ts = {"labels": labels}
        exporter_create_ts(rts, ts, tsname_use_case_duration)
        logging.error(labels)
        rts.ts().add(
            tsname_use_case_duration,
            start_time_ms,
            metric_value,
            labels=labels,
        )
    else:
        logging.warning(
            "Given that metric {}={} ( is None ) we will skip adding it to timeseries".format(
                metric_name, metric_value
            )
        )


def add_standardized_metric_byversion(
    metric_name,
    metric_value,
    artifact_version,
    deployment_name,
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
    if metric_value is not None:
        tsname_use_case_duration = get_ts_metric_name(
            "by.version",
            artifact_version,
            tf_github_org,
            tf_github_repo,
            deployment_name,
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
            deployment_name,
            deployment_type,
            tf_triggering_env,
            metadata_tags,
            build_variant_name,
        )
        labels["version"] = artifact_version
        labels["deployment_name+version"] = "{} {}".format(
            deployment_name, artifact_version
        )
        labels["test_name"] = str(test_name)
        labels["metric"] = str(metric_name)
        logging.info(
            "Adding metric {}={} to time-serie named {}".format(
                metric_name, metric_value, tsname_use_case_duration
            )
        )
        ts = {"labels": labels}
        exporter_create_ts(rts, ts, tsname_use_case_duration)
        rts.ts().add(
            tsname_use_case_duration,
            start_time_ms,
            metric_value,
            labels=labels,
        )
    else:
        logging.warning(
            "Given that metric {}={} ( is None ) we will skip adding it to timeseries".format(
                metric_name, metric_value
            )
        )


def timeseries_test_sucess_flow(
    push_results_redistimeseries,
    artifact_version,
    benchmark_config,
    benchmark_duration_seconds,
    dataset_load_duration_seconds,
    default_metrics,
    deployment_name,
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
    timeseries_dict=None,
):
    testcase_metric_context_paths = []
    version_target_tables = None
    branch_target_tables = None

    if timeseries_dict is None:
        (
            timeseries_dict,
            testcase_metric_context_paths,
            version_target_tables,
            branch_target_tables,
        ) = prepare_timeseries_dict(
            artifact_version,
            benchmark_config,
            default_metrics,
            deployment_name,
            deployment_type,
            exporter_timemetric_path,
            results_dict,
            test_name,
            tf_github_branch,
            tf_github_org,
            tf_github_repo,
            tf_triggering_env,
            metadata_tags,
            build_variant_name,
            running_platform,
            start_time_ms,
        )
    if push_results_redistimeseries:
        logging.info(
            "Pushing results to RedisTimeSeries. Have {} distinct data-points to insert.".format(
                len(timeseries_dict.keys())
            )
        )
        push_data_to_redistimeseries(rts, timeseries_dict)
        if version_target_tables is not None:
            logging.info(
                "There are a total of {} distinct target tables by version".format(
                    len(version_target_tables.keys())
                )
            )
            for (
                version_target_table_keyname,
                version_target_table_dict,
            ) in version_target_tables.items():
                logging.info(
                    "Setting target table by version on key {}".format(
                        version_target_table_keyname
                    )
                )
                if "contains-target" in version_target_table_dict:
                    del version_target_table_dict["contains-target"]
                rts.hset(
                    version_target_table_keyname, None, None, version_target_table_dict
                )
        if branch_target_tables is not None:
            logging.info(
                "There are a total of {} distinct target tables by branch".format(
                    len(branch_target_tables.keys())
                )
            )
            for (
                branch_target_table_keyname,
                branch_target_table_dict,
            ) in branch_target_tables.items():

                logging.info(
                    "Setting target table by branch on key {}".format(
                        branch_target_table_keyname
                    )
                )
                if "contains-target" in branch_target_table_dict:
                    del branch_target_table_dict["contains-target"]
                rts.hset(
                    branch_target_table_keyname, None, None, branch_target_table_dict
                )

        update_secondary_result_keys(
            artifact_version,
            benchmark_duration_seconds,
            build_variant_name,
            dataset_load_duration_seconds,
            deployment_name,
            deployment_type,
            metadata_tags,
            rts,
            running_platform,
            start_time_ms,
            test_name,
            testcase_metric_context_paths,
            tf_github_branch,
            tf_github_org,
            tf_github_repo,
            tf_triggering_env,
        )
    return version_target_tables, branch_target_tables


def update_secondary_result_keys(
    artifact_version,
    benchmark_duration_seconds,
    build_variant_name,
    dataset_load_duration_seconds,
    deployment_name,
    deployment_type,
    metadata_tags,
    rts,
    running_platform,
    start_time_ms,
    test_name,
    testcase_metric_context_paths,
    tf_github_branch,
    tf_github_org,
    tf_github_repo,
    tf_triggering_env,
):
    (
        _,
        testcases_setname,
        deployment_name_zsetname,
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
        rts.zadd(deployment_name_zsetname, {deployment_name: start_time_ms})
        if test_name is not None:
            deployment_name_zsetname_testnames = (
                deployment_name_zsetname
                + "{}:deployment_name={}".format(
                    deployment_name_zsetname, deployment_name
                )
            )
            rts.zadd(deployment_name_zsetname_testnames, {test_name: start_time_ms})
            rts.sadd(testcases_setname, test_name)
            testcases_zsetname = testcases_setname + ":zset"
            rts.zadd(testcases_zsetname, {test_name: start_time_ms})
            if "component" in metadata_tags:
                testcases_zsetname_component = "{}:zset:component:{}".format(
                    testcases_setname, metadata_tags["component"]
                )
                rts.zadd(testcases_zsetname_component, {test_name: start_time_ms})
        if "arch" in metadata_tags:
            rts.sadd(project_archs_setname, metadata_tags["arch"])
        if "os" in metadata_tags:
            rts.sadd(project_oss_setname, metadata_tags["os"])
        if "compiler" in metadata_tags:
            rts.sadd(project_compilers_setname, metadata_tags["compiler"])
        if tf_github_branch is not None and tf_github_branch != "":
            rts.sadd(project_branches_setname, tf_github_branch)
        if artifact_version is not None and artifact_version != "":
            rts.sadd(project_versions_setname, artifact_version)
        if running_platform is not None:
            rts.sadd(running_platforms_setname, running_platform)
        if build_variant_name is not None:
            rts.sadd(build_variant_setname, build_variant_name)
        if testcase_metric_context_paths is not None:
            for metric_context_path in testcase_metric_context_paths:
                if testcases_metric_context_path_setname != "":
                    rts.sadd(testcases_metric_context_path_setname, metric_context_path)
                    rts.sadd(
                        testcases_and_metric_context_path_setname,
                        "{}:{}".format(test_name, metric_context_path),
                    )
        rts.ts().incrby(
            tsname_project_total_success,
            1,
            timestamp=start_time_ms,
            labels=get_project_ts_tags(
                tf_github_org,
                tf_github_repo,
                deployment_name,
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
                deployment_name,
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
                deployment_name,
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
                deployment_name,
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
                deployment_name,
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
            "Error while updating secondary data structures {}. ".format(e.__str__())
        )
        pass


def timeseries_test_failure_flow(
    args,
    deployment_name,
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
            rts.ts().incrby(
                tsname_project_total_failures,
                1,
                timestamp=start_time_ms,
                labels=get_project_ts_tags(
                    tf_github_org,
                    tf_github_repo,
                    deployment_name,
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


def datasink_profile_tabular_data(
    github_branch,
    github_org_name,
    github_repo_name,
    github_sha,
    overall_tabular_data_map,
    rts,
    setup_type,
    start_time_ms,
    start_time_str,
    test_name,
    tf_triggering_env,
):
    zset_profiles_key_name = get_profilers_rts_key_prefix(
        tf_triggering_env,
        github_org_name,
        github_repo_name,
    )
    profile_test_suffix = "{start_time_str}:{test_name}/{setup_type}/{github_branch}/{github_hash}".format(
        start_time_str=start_time_str,
        test_name=test_name,
        setup_type=setup_type,
        github_branch=github_branch,
        github_hash=github_sha,
    )
    rts.zadd(
        zset_profiles_key_name,
        {profile_test_suffix: start_time_ms},
    )
    for (
        profile_tabular_type,
        tabular_data,
    ) in overall_tabular_data_map.items():
        tabular_suffix = "{}:{}".format(profile_tabular_type, profile_test_suffix)
        logging.info(
            "Pushing to data-sink tabular data from pprof ({}). Tabular suffix: {}".format(
                profile_tabular_type, tabular_suffix
            )
        )

        table_columns_text_key = "{}:columns:text".format(tabular_suffix)
        table_columns_type_key = "{}:columns:type".format(tabular_suffix)
        logging.info(
            "Pushing list key (named {}) the following column text: {}".format(
                table_columns_text_key, tabular_data["columns:text"]
            )
        )
        rts.rpush(table_columns_text_key, *tabular_data["columns:text"])
        logging.info(
            "Pushing list key (named {}) the following column types: {}".format(
                table_columns_type_key, tabular_data["columns:type"]
            )
        )
        rts.rpush(table_columns_type_key, *tabular_data["columns:type"])
        for row_name in tabular_data["columns:text"]:
            table_row_key = "{}:rows:{}".format(tabular_suffix, row_name)
            row_values = tabular_data["rows:{}".format(row_name)]
            rts.rpush(table_row_key, *row_values)
