from jsonpath_ng import parse


def parseExporterMetricsDefinition(benchmark_config, configkey="redistimeseries"):
    metrics = []
    if configkey in benchmark_config:
        if "metrics" in benchmark_config[configkey]:
            for metric_name in benchmark_config[configkey][
                "metrics"
            ]:
                metrics.append(metric_name)
    return metrics


def parseExporterTimeMetric(benchmark_config, results_dict, configkey="redistimeseries"):
    datapoints_timestamp = None
    if "timemetric" in benchmark_config[configkey]:
        tspath = benchmark_config[configkey]["timemetric"]
        jsonpath_expr = parse(tspath)
        datapoints_timestamp = int(
            jsonpath_expr.find(results_dict)[0].value
        )
    return datapoints_timestamp
