from redisbench_admin.export.common.common import (
    get_or_none,
    get_kv_tags,
    prepare_tags,
    get_timeserie_name,
    add_datapoint,
    get_metric_detail,
)
from redisbench_admin.export.redis_benchmark.metrics_definition import (
    redis_benchmark_metrics_definition,
)


def warn_if_tag_none(tag_name, tag_value, tool, level="Warning"):
    if tag_value is None:
        print(
            '{}! The tag "{}" is None. Given that {} cannot infer'
            " it you should pass it via --extra-tags {}=<value>".format(
                level, tag_name, tool, tag_name
            )
        )


def get_tag_fromextra_tags_array(array, tag_name):
    result = None
    for inner_dict in array:
        inne_result = get_or_none(inner_dict, tag_name)
        if inne_result is not None:
            result = inne_result
    return result


def fill_tags_from_passed_array(extra_tags_array):
    git_sha = get_tag_fromextra_tags_array(extra_tags_array, "git_sha")
    if git_sha is None:
        git_sha = get_tag_fromextra_tags_array(extra_tags_array, "redis_git_sha1")
    warn_if_tag_none("git_sha", git_sha, "redis-benchmark")
    deployment_type = get_tag_fromextra_tags_array(extra_tags_array, "deployment_type")
    if deployment_type is None:
        deployment_type = get_tag_fromextra_tags_array(extra_tags_array, "redis_mode")
    warn_if_tag_none("deployment_type", deployment_type, "redis-benchmark")
    project = get_tag_fromextra_tags_array(extra_tags_array, "project")
    warn_if_tag_none("project", project, "redis-benchmark")
    project_version = get_tag_fromextra_tags_array(extra_tags_array, "project_version")
    if project_version is None:
        project_version = get_tag_fromextra_tags_array(
            extra_tags_array, "redis_version"
        )
    warn_if_tag_none("project_version", project_version, "redis-benchmark")
    return deployment_type, git_sha, project, project_version, "benchmark"


def redis_benchmark_export_logic(
    benchmark_result, extra_tags_array, results_type, time_series_dict
):
    ok = True
    start_time_ms = get_tag_fromextra_tags_array(extra_tags_array, "start_time_ms")
    if start_time_ms is None:
        start_time_ms = get_tag_fromextra_tags_array(
            extra_tags_array, "server_time_usec"
        )
    if start_time_ms is None:
        start_time_ms = get_tag_fromextra_tags_array(
            extra_tags_array, "extract_milli_time"
        )
    if start_time_ms is None:
        warn_if_tag_none("start_time_ms", start_time_ms, "redis-benchmark,", "Error")
        ok = False
        return ok, time_series_dict

    (
        deployment_type,
        git_sha,
        project,
        project_version,
        step,
    ) = fill_tags_from_passed_array(extra_tags_array)

    col0_row0 = benchmark_result["col_0"][0]
    # new format
    # "test","rps","avg_latency_ms","min_latency_ms","p50_latency_ms","p95_latency_ms","p99_latency_ms","max_latency_ms"
    if col0_row0 != "test":
        # old format
        # "test","rps"
        benchmark_result["col_0"] = ["test"] + benchmark_result["col_0"]
        benchmark_result["col_1"] = ["rps"] + benchmark_result["col_1"]
    metrics_in_csv = {}
    for col_name, col in benchmark_result.items():
        metrics_in_csv[col[0]] = col_name
    for test_pos, testcase_name in enumerate(benchmark_result["col_0"]):
        (
            common_broader_kv_tags,
            common_git_sha_kv_tags,
            common_version_kv_tags,
        ) = get_kv_tags(
            deployment_type,
            extra_tags_array,
            git_sha,
            project,
            project_version,
            results_type,
            step,
            prepare_tags(testcase_name),
        )

        if test_pos > 0:
            for metric_def in redis_benchmark_metrics_definition:
                metric_csv_col_name = metric_def["metric-csv-col"]
                metric_name = metric_csv_col_name
                if metric_csv_col_name in metrics_in_csv:
                    benchmark_result_col = metrics_in_csv[metric_csv_col_name]
                    metric_column = benchmark_result[benchmark_result_col]
                    metric_value = metric_column[test_pos]
                    broader_kv = common_broader_kv_tags.copy()
                    broader_kv.append({"metric-name": prepare_tags(metric_name)})
                    version_kv = common_version_kv_tags.copy()
                    version_kv.append({"metric-name": prepare_tags(metric_name)})
                    git_sha_kv = common_git_sha_kv_tags.copy()
                    git_sha_kv.append({"metric-name": prepare_tags(metric_name)})
                    git_sha_ts_name = get_timeserie_name(git_sha_kv)
                    (
                        metric_step,
                        metric_family,
                        _,
                        _,
                        metric_unit,
                        _,
                        _,
                        _,
                    ) = get_metric_detail(metric_def)

                    git_sha_tags_kv = git_sha_kv.copy()
                    git_sha_tags_kv.extend(
                        [
                            {"metric-step": metric_step},
                            {"metric-family": metric_family},
                            {"metric-unit": metric_unit},
                        ]
                    )
                    add_datapoint(
                        time_series_dict,
                        git_sha_ts_name,
                        start_time_ms,
                        metric_value,
                        git_sha_tags_kv,
                    )
    return ok, time_series_dict
