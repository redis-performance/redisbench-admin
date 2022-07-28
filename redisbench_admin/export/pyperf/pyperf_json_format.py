#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import logging
import numpy as np


def metric_safe_name(row, replace_by="_"):
    import re

    metric_name = row.strip()
    metric_name = re.sub(r"\W+", replace_by, metric_name)
    return metric_name


def generate_summary_json_pyperf(input_json):
    result_json = {}
    for benchmark in input_json["benchmarks"]:
        original_name = benchmark["metadata"]["name"]
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

        runs = benchmark["runs"]
        total_runs = len(runs)
        results = []
        for run in runs:
            if "values" in run:
                for value in run["values"]:
                    results.append(value)
        avg = np.average(results)
        std = np.std(results)
        logging.info(
            "Adding pyperf metric named {}: avg={} stddev={} total_runs={}".format(
                benchmark_name, avg, std, total_runs
            )
        )
        result_json[benchmark_name] = {"avg": avg, "std": std, "total_runs": total_runs}

    return result_json
