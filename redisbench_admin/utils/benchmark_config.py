from jsonpath_ng import parse


def parse_exporter_metrics_definition(
    benchmark_config: dict, configkey: str = "redistimeseries"
):
    metrics = []
    if configkey in benchmark_config:
        if "metrics" in benchmark_config[configkey]:
            for metric_name in benchmark_config[configkey]["metrics"]:
                metrics.append(metric_name)
    return metrics


def parse_exporter_timemetric_definition(
    benchmark_config: dict, configkey: str = "redistimeseries"
):
    metric_path = None
    if "timemetric" in benchmark_config[configkey]:
        metric_path = benchmark_config[configkey]["timemetric"]
    return metric_path


def parse_exporter_timemetric(metric_path: str, results_dict: dict):
    jsonpath_expr = parse(metric_path)
    datapoints_timestamp = int(jsonpath_expr.find(results_dict)[0].value)
    return datapoints_timestamp
