from jsonpath_ng import parse


def parseExporterMetricsDefinition(
        benchmark_config: dict, configkey: str = "redistimeseries"
):
    metrics = []
    if configkey in benchmark_config:
        if "metrics" in benchmark_config[configkey]:
            for metric_name in benchmark_config[configkey]["metrics"]:
                metrics.append(metric_name)
    return metrics


def parseExporterTimeMetricDefinition(
        benchmark_config: dict, configkey: str = "redistimeseries"
):
    metricPath = None
    if "timemetric" in benchmark_config[configkey]:
        metricPath = benchmark_config[configkey]["timemetric"]
    return metricPath


def parseExporterTimeMetric(metricPath: str, results_dict: dict):
    jsonpath_expr = parse(metricPath)
    datapoints_timestamp = int(jsonpath_expr.find(results_dict)[0].value)
    return datapoints_timestamp
