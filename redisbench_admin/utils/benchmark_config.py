from jsonpath_ng import parse


<<<<<<< HEAD
def parseExporterMetricsDefinition(
    benchmark_config: dict, configkey: str = "redistimeseries"
):
    metrics = []
    if configkey in benchmark_config:
        if "metrics" in benchmark_config[configkey]:
            for metric_name in benchmark_config[configkey]["metrics"]:
=======
def parseExporterMetricsDefinition(benchmark_config, configkey="redistimeseries"):
    metrics = []
    if configkey in benchmark_config:
        if "metrics" in benchmark_config[configkey]:
            for metric_name in benchmark_config[configkey][
                "metrics"
            ]:
>>>>>>> origin/master
                metrics.append(metric_name)
    return metrics


<<<<<<< HEAD
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
=======
def parseExporterTimeMetric(benchmark_config, results_dict, configkey="redistimeseries"):
    datapoints_timestamp = None
    if "timemetric" in benchmark_config[configkey]:
        tspath = benchmark_config[configkey]["timemetric"]
        jsonpath_expr = parse(tspath)
        datapoints_timestamp = int(
            jsonpath_expr.find(results_dict)[0].value
        )
>>>>>>> origin/master
    return datapoints_timestamp
