from redisbench_admin.export.common.common import split_key_metrics_by_step, prepare_tags, get_timeserie_name, \
    add_datapoint, get_or_None, get_kv_tags
from redisbench_admin.utils.results import get_key_results_and_values


def extract_benchmark_run_info_details(benchmark_result):
    benchmark_config = benchmark_result["run-info"]

    start_time_ms = benchmark_config["start-time-ms"]
    start_time_humanized = benchmark_config["start-time-humanized"]
    end_time_ms = benchmark_config["end-time-ms"]
    end_time_humanized = benchmark_config["end-time-humanized"]
    duration_ms = benchmark_config["duration-ms"]
    duration_humanized = benchmark_config["duration-humanized"]

    return start_time_ms, start_time_humanized, end_time_ms, end_time_humanized, duration_ms, duration_humanized


def extract_benchmark_config_details(benchmark_result):
    benchmark_config = benchmark_result["benchmark-config"]
    testcase_name = benchmark_config["name"]
    specifications_version = benchmark_config["specifications-version"]
    testcase_description = benchmark_config["description"]
    key_metrics_specs = benchmark_config["key-metrics"]
    return testcase_name, specifications_version, testcase_description, key_metrics_specs


def extract_key_configs(benchmark_result):
    benchmark_config = benchmark_result["key-configs"]
    deployment_type = benchmark_config["deployment-type"]
    deployment_shards = benchmark_config["deployment-shards"]
    project = "redisearch"
    if "project" in benchmark_config:
        project = benchmark_config["project"]
    project_version = None
    if "version" in benchmark_config:
        project_version = benchmark_config["version"]
    if "redisearch-version" in benchmark_config:
        project_version = benchmark_config["redisearch-version"]
    git_sha = benchmark_config["git_sha"]
    return deployment_type, deployment_shards, project, project_version, git_sha


def ftsb_export_logic(benchmark_result, extra_tags_array, filename, included_steps, results_type, time_series_dict,
                      use_result):
    key_result_steps = benchmark_result[results_type].keys()
    testcase_name, specifications_version, testcase_description, key_metrics_specs = extract_benchmark_config_details(
        benchmark_result)
    key_metrics_specs_per_step = split_key_metrics_by_step(key_metrics_specs)
    deployment_type, deployment_shards, project, project_version, git_sha = extract_key_configs(benchmark_result)
    start_time_ms, start_time_humanized, end_time_ms, end_time_humanized, duration_ms, duration_humanized = extract_benchmark_run_info_details(
        benchmark_result)
    for step in key_result_steps:
        common_broader_kv_tags, common_git_sha_kv_tags, common_version_kv_tags = get_kv_tags(deployment_type,
                                                                                             extra_tags_array, git_sha,
                                                                                             project, project_version,
                                                                                             results_type, step,
                                                                                             testcase_name)
        common_git_sha_kv_tags.extend(extra_tags_array)
        if step in included_steps:
            key_metrics_specs = key_metrics_specs_per_step[step]
            key_result_run_name, metrics = get_key_results_and_values(benchmark_result, step, use_result)
            for metric_name, metric_value in metrics.items():
                broader_kv = common_broader_kv_tags.copy()
                broader_kv.append({"metric-name": prepare_tags(metric_name)})
                version_kv = common_version_kv_tags.copy()
                version_kv.append({"metric-name": prepare_tags(metric_name)})
                git_sha_kv = common_git_sha_kv_tags.copy()
                git_sha_kv.append({"metric-name": prepare_tags(metric_name)})

                git_sha_ts_name = get_timeserie_name(git_sha_kv)

                key_metric_spec = key_metrics_specs[metric_name]
                metric_step, metric_family, _, _, metric_unit, _, _, _ = get_metric_detail(key_metric_spec)

                git_sha_tags_kv = git_sha_kv.copy()
                git_sha_tags_kv.extend(
                    [{"metric-step": metric_step}, {"metric-family": metric_family}, {"metric-unit": metric_unit}])
                add_datapoint(time_series_dict, git_sha_ts_name, start_time_ms, metric_value, git_sha_tags_kv)
    return time_series_dict


def get_metric_detail(key_metric_spec):
    metric_step = get_or_None(key_metric_spec, "step")
    metric_family = get_or_None(key_metric_spec, "metric-family")
    metric_json_path = get_or_None(key_metric_spec, "metric-json-path")
    metric_name = get_or_None(key_metric_spec, "metric-name")
    metric_unit = get_or_None(key_metric_spec, "unit")
    metric_type = get_or_None(key_metric_spec, "metric-type")
    metric_comparison = get_or_None(key_metric_spec, "comparison")
    metric_per_step_comparison_priority = get_or_None(key_metric_spec, "per-step-comparison-metric-priority")
    return metric_step, metric_family, metric_json_path, metric_name, metric_unit, metric_type, metric_comparison, metric_per_step_comparison_priority
