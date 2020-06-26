import json


def get_key_results_and_values(results_json, step, use_result):
    selected_run = None
    metrics = {}
    if "key-results" in results_json and use_result in results_json["key-results"][step]:
        for name, value in results_json["key-results"][step][use_result][0].items():
            if name == "run-name":
                selected_run = value
            else:
                metrics[name] = value
    return selected_run, metrics


def from_resultsDF_to_key_results_dict(resultsDataFrame, step, step_df_dict):
    key_results_dict = {}
    key_results_dict["table"] = json.loads(resultsDataFrame.to_json(orient='records'))
    best_result = resultsDataFrame.head(n=1)
    worst_result = resultsDataFrame.tail(n=1)
    first_sorting_col = step_df_dict[step]["sorting_metric_names"][0]
    first_sorting_median = resultsDataFrame[first_sorting_col].median()
    result_index = resultsDataFrame[first_sorting_col].sub(first_sorting_median).abs().idxmin()
    median_result = resultsDataFrame.loc[[result_index]]
    key_results_dict["best-result"] = json.loads(best_result.to_json(orient='records'))
    key_results_dict["median-result"] = json.loads(
        median_result.to_json(orient='records'))
    key_results_dict["worst-result"] = json.loads(worst_result.to_json(orient='records'))
    key_results_dict["reliability-analysis"] = {
        'var': json.loads(resultsDataFrame.var().to_json()),
        'stddev': json.loads(
            resultsDataFrame.std().to_json())}
    return key_results_dict
