#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import logging
import datetime as dt
import redis
from jsonpath_ng import parse

CPU_USAGE_SUMMARY_METRICS = (
    ("total_shards_used_cpu_pct", "Total shards CPU usage %", 75),
    ("total_shards_used_cpu_p50_pct", "Total shards CPU usage p50 %", 50),
    ("total_shards_used_cpu_p99_pct", "Total shards CPU usage p99 %", 99),
)
CPU_USAGE_SUMMARY_METRIC_NAMES = tuple(
    metric_name for metric_name, _, _ in CPU_USAGE_SUMMARY_METRICS
)


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
                    if metric.value is None:
                        logging.warning(
                            "Skipping metric {} (path: {}) because its value is None".format(
                                metric_jsonpath, metric_name
                            )
                        )
                        continue
                    try:
                        metric_value = float(metric.value)
                    except (TypeError, ValueError):
                        logging.warning(
                            "Skipping metric {} (path: {}) because its value {!r} is not numeric".format(
                                metric_jsonpath, metric_name, metric.value
                            )
                        )
                        continue
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
    redis_conns,
    sections=["memory", "cpu", "commandstats", "latencystats"],
    section_filter=None,
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
            try:
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
                    if collect and (type(v) is float or type(v) is int):
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
            except redis.exceptions.ConnectionError as e:
                logging.warning(
                    f"Unable to collect section {section} from redis. error: {e.__str__}"
                )

        res.append(conn_res)

    kv_overall = {}
    for sec, kv_detail in overall.items():
        for k, metric_value in kv_detail.items():
            metric_name = "{}_{}".format(sec, k)
            kv_overall[metric_name] = metric_value

    return start_time_ms, res, kv_overall


def from_info_to_overall_shard_cpu(benchmark_cpu_stats):
    """Summarize Redis process CPU usage over time and across shards.

    Consecutive INFO samples provide one CPU utilization interval. We first use
    p50, p75, and p99 over time for each shard, then sum the matching percentile
    across shards. The established total metric remains the sum of per-shard
    p75 values; p50 and p99 show the usual load and brief peaks without taking
    percentiles of percentiles across shards.

    Returns an empty summary when no shard has at least two samples. This keeps
    short runs from reporting a misleading zero.
    """
    import numpy as np

    res = {}
    shard_cpu_percentiles = []
    for shard_n, cpu_stats_arr in benchmark_cpu_stats.items():
        avg_cpu_pct = None
        shard_cpu_intervals = []
        # Two cumulative CPU samples are required to calculate one interval.
        if len(cpu_stats_arr) >= 2:
            for start_pos in range(0, len(cpu_stats_arr) - 1):
                avg_cpu_pct = get_avg_cpu_pct(
                    avg_cpu_pct, cpu_stats_arr[start_pos], cpu_stats_arr[start_pos + 1]
                )
                if avg_cpu_pct is not None:
                    shard_cpu_intervals.append(avg_cpu_pct)
            if len(shard_cpu_intervals) > 0:
                percentiles = {
                    percentile: np.percentile(shard_cpu_intervals, percentile)
                    for _, _, percentile in CPU_USAGE_SUMMARY_METRICS
                }
                shard_cpu_percentiles.append(percentiles)
                # Preserve the per-shard p75 map returned by this function.
                avg_cpu_pct = percentiles[75]

        res[shard_n] = avg_cpu_pct

    summary = {}
    if shard_cpu_percentiles:
        summary = {
            metric_name: np.sum(
                [values[percentile] for values in shard_cpu_percentiles]
            )
            for metric_name, _, percentile in CPU_USAGE_SUMMARY_METRICS
        }
    return summary, res


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


def collect_search_and_bigredis_metrics(redis_conns):
    """Collect the 4 atomic search/memory metrics from Redis INFO sections.

    Returns a flat dict with:
      - bigredis_used_ram
      - bigredis_used_disk
      - search_memory_search_used_memory_indexes
      - search_disk_search_disk_usage

    Any section that is unavailable (e.g. no bigredis module) is silently
    skipped; the corresponding keys will simply be absent from the result.
    """

    _, _, bigredis_kv = collect_redis_metrics(
        redis_conns,
        ["bigredis"],
        {"bigredis": ["used_ram", "used_disk"]},
    )
    _, _, search_mem_kv = collect_redis_metrics(
        redis_conns,
        ["search_memory"],
        {"search_memory": ["search_used_memory_indexes"]},
    )
    _, _, search_disk_kv = collect_redis_metrics(
        redis_conns,
        ["search_disk"],
        {"search_disk": ["search_disk_usage"]},
    )

    merged = {}
    merged.update(bigredis_kv)
    merged.update(search_mem_kv)
    merged.update(search_disk_kv)

    return merged


BENCHMARK_RUNNING_GLOBAL = False
BENCHMARK_CPU_STATS_GLOBAL = {}


def reset_cpu_stats():
    """Discard samples from previous benchmarks before starting a new run."""
    BENCHMARK_CPU_STATS_GLOBAL.clear()


def collect_cpu_data(redis_conns=[], delta_secs: float = 5.0, delay_start: float = 1.0):
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
