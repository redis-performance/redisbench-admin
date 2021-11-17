#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import logging

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
