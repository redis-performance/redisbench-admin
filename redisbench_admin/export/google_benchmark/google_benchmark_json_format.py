#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import logging


def metric_safe_name(row, replace_by="_"):
    import re

    metric_name = row.strip()
    metric_name = re.sub(r"\W+", replace_by, metric_name)
    return metric_name


def generate_summary_json_google_benchmark(input_json):
    result_json = {}
    test_names = []
    for benchmark in input_json["benchmarks"]:
        original_name = benchmark["name"]
        benchmark_name = original_name
        non_safe_count = len(original_name) - len(metric_safe_name(original_name, ""))
        if non_safe_count > 0:
            benchmark_name = metric_safe_name(original_name)
            while "_" == benchmark_name[len(benchmark_name) - 1]:
                benchmark_name = benchmark_name[: len(benchmark_name) - 1]
            logging.warning(
                "Given the benchmark name {} contains {} non alphanumeric characters, we're replacing it by the safe version {}".format(
                    original_name, "-", benchmark_name
                )
            )
        metrics = {}
        test_names.append(benchmark_name)
        for metric_name, metric_value_str in benchmark.items():
            metric_value = None
            try:
                metric_value = float(metric_value_str)
            except ValueError:
                pass
            if metric_value is not None:
                logging.info(
                    "Adding google.benchmark to benchmark {} metric named {}={}".format(
                        benchmark_name, metric_name, metric_value
                    )
                )
                metrics[metric_name] = metric_value

        result_json[benchmark_name] = metrics
        logging.warning(metrics)

    return result_json, test_names
