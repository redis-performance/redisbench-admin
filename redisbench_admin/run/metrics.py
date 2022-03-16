#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import logging
import datetime as dt

from jsonpath_ng import parse


def extract_results_table(
    metrics,
    results_dict,
):
    results_matrix = []
    cleaned_metrics = []
    already_present_metrics = []
    # insert first the dict metrics
    for jsonpath in metrics:
        if type(jsonpath) == dict:
            cleaned_metrics.append(jsonpath)
            metric_jsonpath = list(jsonpath.keys())[0]
            already_present_metrics.append(metric_jsonpath)
    for jsonpath in metrics:
        if type(jsonpath) == str:
            if jsonpath not in already_present_metrics:
                already_present_metrics.append(jsonpath)
                cleaned_metrics.append(jsonpath)

    for jsonpath in cleaned_metrics:
        test_case_targets_dict = {}
        metric_jsonpath = jsonpath
        find_res = None
        try:
            if type(jsonpath) == str:
                jsonpath_expr = parse(jsonpath)
            if type(jsonpath) == dict:
                metric_jsonpath = list(jsonpath.keys())[0]
                test_case_targets_dict = jsonpath[metric_jsonpath]
                jsonpath_expr = parse(metric_jsonpath)
            find_res = jsonpath_expr.find(results_dict)
        except Exception:
            pass
        finally:
            if find_res is not None:
                use_metric_context_path = False
                if len(find_res) > 1:
                    use_metric_context_path = True
                for metric in find_res:
                    metric_name = str(metric.path)
                    metric_value = float(metric.value)
                    metric_context_path = str(metric.context.path)
                    if metric_jsonpath[0] == "$":
                        metric_jsonpath = metric_jsonpath[1:]
                    if metric_jsonpath[0] == ".":
                        metric_jsonpath = metric_jsonpath[1:]

                    # retro-compatible naming
                    if use_metric_context_path is False:
                        metric_name = metric_jsonpath

                    metric_name = metric_name.replace("'", "")
                    metric_name = metric_name.replace('"', "")
                    metric_name = metric_name.replace("(", "")
                    metric_name = metric_name.replace(")", "")
                    metric_name = metric_name.replace(" ", "_")

                    results_matrix.append(
                        [
                            metric_jsonpath,
                            metric_context_path,
                            metric_name,
                            metric_value,
                            test_case_targets_dict,
                            use_metric_context_path,
                        ]
                    )

            else:
                logging.warning(
                    "Unable to find metric path {} in result dict".format(jsonpath)
                )
    return results_matrix


def collect_redis_metrics(
    redis_conns, sections=["memory", "cpu", "commandstats"], section_filter=None
):
    start_time = dt.datetime.utcnow()
    start_time_ms = int((start_time - dt.datetime(1970, 1, 1)).total_seconds() * 1000)
    res = []
    overall = {}
    multi_shard = False
    if len(redis_conns) > 1:
        multi_shard = True
    for conn_n, conn in enumerate(redis_conns):
        conn_res = {}
        for section in sections:
            info = conn.info(section)
            conn_res[section] = info
            if section not in overall:
                overall[section] = {}
            for k, v in info.items():
                collect = True
                if section_filter is not None:
                    if section in section_filter:
                        if k not in section_filter[section]:
                            collect = False
                if collect and type(v) is float or type(v) is int:
                    if k not in overall[section]:
                        overall[section][k] = 0
                    overall[section][k] += v
                if collect and type(v) is dict:
                    for inner_k, inner_v in v.items():
                        if type(inner_v) is float or type(inner_v) is int:
                            final_str_k = "{}_{}".format(k, inner_k)
                            if multi_shard:
                                final_str_k += "_shard_{}".format(conn_n + 1)
                            if final_str_k not in overall[section]:
                                overall[section][final_str_k] = inner_v

        res.append(conn_res)

    kv_overall = {}
    for sec, kv_detail in overall.items():
        for k, metric_value in kv_detail.items():
            metric_name = "{}_{}".format(sec, k)
            kv_overall[metric_name] = metric_value

    return start_time_ms, res, kv_overall


def from_info_to_overall_shard_cpu(benchmark_cpu_stats):
    import numpy as np

    total_avg_cpu_pct = 0.0
    res = {}
    for shard_n, cpu_stats_arr in benchmark_cpu_stats.items():
        avg_cpu_pct = None
        shards_cpu_arr = []
        # we need at least 2 elements to compute the cpu usage
        if len(cpu_stats_arr) >= 2:
            for start_pos in range(0, len(cpu_stats_arr) - 2):
                avg_cpu_pct = get_avg_cpu_pct(
                    avg_cpu_pct, cpu_stats_arr[start_pos], cpu_stats_arr[start_pos + 1]
                )
                if avg_cpu_pct is not None:
                    shards_cpu_arr.append(avg_cpu_pct)
            if len(shards_cpu_arr) > 0:
                avg_cpu_pct = np.percentile(shards_cpu_arr, 75)

        res[shard_n] = avg_cpu_pct
        if avg_cpu_pct is not None:
            total_avg_cpu_pct += avg_cpu_pct
    return total_avg_cpu_pct, res


def get_avg_cpu_pct(avg_cpu_pct, stats_start_pos, stats_end_pos):
    avg_cpu_pct = None
    if "server_time_usec" in stats_end_pos and "server_time_usec" in stats_start_pos:
        start_ts_micros = stats_start_pos["server_time_usec"]
        end_ts_micros = stats_end_pos["server_time_usec"]
        start_total_cpu = get_total_cpu(stats_start_pos)
        end_total_cpu = get_total_cpu(stats_end_pos)
        total_secs = (end_ts_micros - start_ts_micros) / 1000000
        total_cpu_usage = end_total_cpu - start_total_cpu
        avg_cpu_pct = 100.0 * (total_cpu_usage / total_secs)
    return avg_cpu_pct


def get_total_cpu(info_data):
    total_cpu = 0.0
    total_cpu = total_cpu + info_data["used_cpu_sys"]
    total_cpu = total_cpu + info_data["used_cpu_user"]
    return total_cpu


BENCHMARK_RUNNING_GLOBAL = False
BENCHMARK_CPU_STATS_GLOBAL = {}


def collect_cpu_data(redis_conns=[], delta_secs: float = 5.0, delay_start: float = 1.0):
    global BENCHMARK_CPU_STATS_GLOBAL
    global BENCHMARK_RUNNING_GLOBAL
    import time

    counter = 0
    time.sleep(delay_start)
    while BENCHMARK_RUNNING_GLOBAL:
        for shard_n, redis_conn in enumerate(redis_conns, 1):
            keyname = "{}".format(shard_n)
            if keyname not in BENCHMARK_CPU_STATS_GLOBAL:
                BENCHMARK_CPU_STATS_GLOBAL[keyname] = []
            BENCHMARK_CPU_STATS_GLOBAL[keyname].append(redis_conn.info())
        time.sleep(delta_secs)
        counter += 1
