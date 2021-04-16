import json


def get_key_results_and_values(results_json, step, use_result):
    selected_run = None
    metrics = {}
    if (
        "key-results" in results_json
        and use_result in results_json["key-results"][step]
    ):
        for name, value in results_json["key-results"][step][use_result][0].items():
            if name == "run-name":
                selected_run = value
            else:
                metrics[name] = value
    return selected_run, metrics


def from_results_dataframe_to_key_results_dict(results_dataframe, step, step_df_dict):
    key_results_dict = {
        "table": json.loads(results_dataframe.to_json(orient="records"))
    }
    best_result = results_dataframe.head(n=1)
    worst_result = results_dataframe.tail(n=1)
    first_sorting_col = step_df_dict[step]["sorting_metric_names"][0]
    first_sorting_median = results_dataframe[first_sorting_col].median()
    result_index = (
        results_dataframe[first_sorting_col].sub(first_sorting_median).abs().idxmin()
    )
    median_result = results_dataframe.loc[[result_index]]
    key_results_dict["best-result"] = json.loads(best_result.to_json(orient="records"))
    key_results_dict["median-result"] = json.loads(
        median_result.to_json(orient="records")
    )
    key_results_dict["worst-result"] = json.loads(
        worst_result.to_json(orient="records")
    )
    key_results_dict["reliability-analysis"] = {
        "var": json.loads(results_dataframe.var().to_json()),
        "stddev": json.loads(results_dataframe.std().to_json()),
    }
    return key_results_dict
