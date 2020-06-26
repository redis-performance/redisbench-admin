import os
import sys

import pandas as pd

from redisbench_admin.utils.results import get_key_results_and_values
from redisbench_admin.utils.utils import retrieve_local_or_remote_input_json


def compare_command_logic(args):
    baseline_file = args.baseline_file
    comparison_file = args.comparison_file
    local_path = os.path.abspath(args.local_dir)
    use_result = args.use_result
    included_steps = args.steps.split(",")
    max_pct_change = args.fail_above_pct_change
    max_negative_pct_change = max_pct_change * -1.0
    enabled_fail = args.enable_fail_above

    baseline_json = retrieve_local_or_remote_input_json(baseline_file, local_path, "--baseline-file")
    if baseline_json is None:
        print('Error while retrieving {}! Exiting..'.format(baseline_file))
        sys.exit(1)

    comparison_json = retrieve_local_or_remote_input_json(comparison_file, local_path, "--comparison-file")
    if comparison_json is None:
        print('Error while retrieving {}! Exiting..'.format(comparison_file))
        sys.exit(1)

    ##### Comparison starts here #####
    baseline_key_results_steps = baseline_json["key-results"].keys()
    comparison_key_results_steps = comparison_json["key-results"].keys()
    baseline_df_config = generate_comparison_dataframe_configs(baseline_json["benchmark-config"],
                                                               baseline_key_results_steps)
    comparison_df_config = generate_comparison_dataframe_configs(comparison_json["benchmark-config"],
                                                                 comparison_key_results_steps)

    percentange_change_map = {}
    for step in baseline_key_results_steps:
        if step in included_steps:
            df_dict = {}
            percentange_change_map[step] = {}
            print("##############################")
            print("Comparing {} step".format(step))
            key_result_run_name, baseline_metrics = get_key_results_and_values(baseline_json, step, use_result)
            key_result_run_name, comparison_metrics = get_key_results_and_values(comparison_json, step, use_result)
            for baseline_metric_name, baseline_metric_value in baseline_metrics.items():
                comparison_metric_value = None
                if baseline_metric_name in comparison_metrics:
                    comparison_metric_value = comparison_metrics[baseline_metric_name]
                df_dict[baseline_metric_name] = [baseline_metric_value, comparison_metric_value]
            df = pd.DataFrame(df_dict, index=["baseline", "comparison"])
            print("Percentage of change for comparison on {}".format(step))
            df = df.append(df.pct_change().rename(index={'comparison': 'pct_change'}).loc['pct_change'] * 100.0)

            for metric_name, items in df.iteritems():

                lower_is_better = baseline_df_config[step]["sorting_metric_sorting_direction_map"][metric_name]

                multiplier = 1.0
                # if lower is better than negative changes are and performance improvement
                if lower_is_better:
                    multiplier = -1.0

                pct_change = items.get("pct_change") * multiplier
                df.at['pct_change', metric_name] = pct_change
                percentange_change_map[step][metric_name] = pct_change

            print(df)
            if enabled_fail:
                failing_metrics_serie = df.loc['pct_change'] <= max_negative_pct_change
                failing_metrics = df.loc['pct_change'][failing_metrics_serie]
                ammount_of_failing_metrics = len(failing_metrics)
                if ammount_of_failing_metrics > 0:
                    df_keys = df.keys()
                    print("There was a total of {} metrics that presented a regression above {} %".format(
                        ammount_of_failing_metrics, max_pct_change))
                    for pos, failed in enumerate(failing_metrics_serie):
                        if failed:
                            print("\tMetric '{}' failed. with an percentage of change of {:.2f} %".format(df_keys[pos],
                                                                                                          df.loc[
                                                                                                              'pct_change'][
                                                                                                              pos]))
                    sys.exit(1)
        else:
            print("Skipping step: {} due to command line argument --steps not containing it ({})".format(step, ",".join(
                included_steps)))


def generate_comparison_dataframe_configs(benchmark_config, steps):
    step_df_dict = {}
    for step in steps:
        step_df_dict[step] = {}
        step_df_dict[step]["df_dict"] = {"run-name": []}
        step_df_dict[step]["sorting_metric_names"] = []
        step_df_dict[step]["sorting_metric_sorting_direction"] = []
        step_df_dict[step]["sorting_metric_sorting_direction_map"] = {}
        step_df_dict[step]["metric_json_path"] = []
    for metric in benchmark_config["key-metrics"]:
        step = metric["step"]
        metric_name = metric["metric-name"]
        metric_json_path = metric["metric-json-path"]
        step_df_dict[step]["sorting_metric_names"].append(metric_name)
        step_df_dict[step]["metric_json_path"].append(metric_json_path)
        step_df_dict[step]["df_dict"][metric_name] = []
        step_df_dict[step]["sorting_metric_sorting_direction"].append(
            False if metric["comparison"] == "higher-better" else True)
        step_df_dict[step]["sorting_metric_sorting_direction_map"][metric_name] = False if metric[
                                                                                               "comparison"] == "higher-better" else True
    return step_df_dict
