import os
import pandas as pd
import redis

from redisbench_admin.compare.compare import get_key_results_and_values
from redisbench_admin.utils.utils import retrieve_local_or_remote_input_json
import matplotlib.pyplot as plt
import seaborn as sns

sns.set(style="darkgrid")
from redistimeseries.client import Client


def get_timeserie_name(labels_kv_array):
    name = ""
    for label_kv in labels_kv_array:
        k = list(label_kv.keys())[0]
        v = list(label_kv.values())[0]
        k = prepare_tags(k)
        v = prepare_tags(v)
        if name != "":
            name += ":"
        name += "{k}={v}".format(k=k, v=v)
    return name


def prepare_tags(k):
    if type(k) != str:
        k = "{}".format(k)
    k = k.replace(" ", "_")
    k = k.lower()
    return k


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


def add_datapoint(time_series_dict, broader_ts_name, start_time_ms, metric_value, tags_array):
    if broader_ts_name not in time_series_dict:
        tags_dict = {}
        for tag_kv in tags_array:
            k = list(tag_kv.keys())[0]
            v = list(tag_kv.values())[0]
            tags_dict[k] = v
        time_series_dict[broader_ts_name] = {"index": [], "data": [], "tags-array": tags_array, "tags": tags_dict}
    time_series_dict[broader_ts_name]["index"].append(start_time_ms)
    time_series_dict[broader_ts_name]["data"].append(metric_value)


def split_tags_string(extra_tags):
    result = []
    extra_tags = extra_tags.split(",")
    if len(extra_tags) > 0:
        for extra_tag in extra_tags:
            kv = extra_tag.split("=")
            if len(kv) == 2:
                k = prepare_tags(kv[0])
                v = prepare_tags(kv[1])
                result.append({k: v})
    return result


def split_key_metrics_by_step(key_metrics_specs):
    key_metrics_by_step = {}
    for key_metric_spec in key_metrics_specs:
        step = None
        if "step" in key_metric_spec and "metric-name" in key_metric_spec:
            step = key_metric_spec["step"]
            metric_name = key_metric_spec["metric-name"]
            if step not in key_metrics_by_step:
                key_metrics_by_step[step] = {}
            key_metrics_by_step[step][metric_name] = key_metric_spec
    return key_metrics_by_step


def get_or_None(dict, property):
    result = None
    if property in dict:
        result = dict[property]
    return result


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


def export_command_logic(args):
    benchmark_files = args.benchmark_result_files
    local_path = os.path.abspath(args.local_dir)
    use_result = args.use_result
    included_steps = args.steps.split(",")

    extra_tags_array = split_tags_string(args.extra_tags)
    print(extra_tags_array)
    results_type = "key-results"
    time_series_dict = {}
    benchmark_results = retrieve_local_or_remote_input_json(benchmark_files, local_path, "--benchmark-result-files")
    for filename, benchmark_result in benchmark_results.items():
        print(filename)
        key_result_steps = benchmark_result[results_type].keys()
        testcase_name, specifications_version, testcase_description, key_metrics_specs = extract_benchmark_config_details(
            benchmark_result)
        key_metrics_specs_per_step = split_key_metrics_by_step(key_metrics_specs)
        deployment_type, deployment_shards, project, project_version, git_sha = extract_key_configs(benchmark_result)
        start_time_ms, start_time_humanized, end_time_ms, end_time_humanized, duration_ms, duration_humanized = extract_benchmark_run_info_details(
            benchmark_result)

        for step in key_result_steps:
            common_broader_kv_tags = [
                {"project": project}, {"use-case": testcase_name}, {"deployment-type": deployment_type},
                {"results-type": results_type}, {"step": step}]
            common_broader_kv_tags.extend(extra_tags_array)
            common_version_kv_tags = [
                {"project": project}, {"use-case": testcase_name}, {"deployment-type": deployment_type},
                {"results-type": results_type}, {"step": step}, {"version": project_version}]
            common_version_kv_tags.extend(extra_tags_array)
            common_git_sha_kv_tags = [
                {"project": project}, {"use-case": testcase_name}, {"deployment-type": deployment_type},
                {"results-type": results_type}, {"step": step}, {"version": project_version}, {"git_sha": git_sha}]
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

                    broader_ts_name = get_timeserie_name(broader_kv)
                    version_ts_name = get_timeserie_name(version_kv)
                    git_sha_ts_name = get_timeserie_name(git_sha_kv)

                    key_metric_spec = key_metrics_specs[metric_name]
                    metric_step, metric_family, _, _, metric_unit, _, _, _ = get_metric_detail(key_metric_spec)

                    # add_datapoint(time_series_dict,broader_ts_name,start_time_ms,metric_value,tags_kv)
                    # add_datapoint(time_series_dict, version_ts_name, start_time_ms, metric_value, tags_kv)

                    git_sha_tags_kv = git_sha_kv.copy()
                    git_sha_tags_kv.extend(
                        [{"metric-step": metric_step}, {"metric-family": metric_family}, {"metric-unit": metric_unit}])
                    add_datapoint(time_series_dict, git_sha_ts_name, start_time_ms, metric_value, git_sha_tags_kv)
    rts = Client(host=args.host,port=args.port,password=args.password)
    for timeseries_name, time_series in time_series_dict.items():
        try:
            rts.create(timeseries_name, labels=time_series['tags'])
        except redis.exceptions.ResponseError:
            # if ts already exists continue
            pass
        for pos, timestamp in enumerate(time_series['index']):
            value = time_series['data'][pos]
            try:
                rts.add(timeseries_name, timestamp, value)
            except redis.exceptions.ResponseError:
                # if ts already exists continue
                pass

