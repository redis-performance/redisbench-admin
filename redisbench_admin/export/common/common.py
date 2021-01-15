def get_timeserie_name(labels_kv_array):
    name = ""
    for label_kv in labels_kv_array:
        k = list(label_kv.keys())[0]
        v = list(label_kv.values())[0]
        k = prepare_tags(k)
        v = prepare_tags(v)
        if name != "":
            name += ":"
        name += "{k}={v}".format(k=k, v=v)
    return name


def prepare_tags(k):
    if type(k) != str:
        k = "{}".format(k)
    k = k.replace(" ", "_")
    k = k.replace("(", "_")
    k = k.replace(")", "_")
    k = k.lower()
    return k


def add_datapoint(time_series_dict, broader_ts_name, start_time_ms, metric_value, tags_array):
    if broader_ts_name not in time_series_dict:
        tags_dict = {}
        for tag_kv in tags_array:
            k = list(tag_kv.keys())[0]
            v = list(tag_kv.values())[0]
            tags_dict[k] = v
        time_series_dict[broader_ts_name] = {"index": [], "data": [], "tags-array": tags_array, "tags": tags_dict}
    time_series_dict[broader_ts_name]["index"].append(start_time_ms)
    time_series_dict[broader_ts_name]["data"].append(metric_value)


def split_tags_string(extra_tags):
    result = []
    extra_tags = extra_tags.split(",")
    if len(extra_tags) > 0:
        for extra_tag in extra_tags:
            kv = extra_tag.split("=")
            if len(kv) == 2:
                k = prepare_tags(kv[0])
                v = prepare_tags(kv[1])
                result.append({k: v})
    return result


def split_key_metrics_by_step(key_metrics_specs):
    key_metrics_by_step = {}
    for key_metric_spec in key_metrics_specs:
        step = None
        if "step" in key_metric_spec and "metric-name" in key_metric_spec:
            step = key_metric_spec["step"]
            metric_name = key_metric_spec["metric-name"]
            if step not in key_metrics_by_step:
                key_metrics_by_step[step] = {}
            key_metrics_by_step[step][metric_name] = key_metric_spec
    return key_metrics_by_step


def get_or_None(dict, property):
    result = None
    if property in dict:
        result = dict[property]
    return result


def get_kv_tags(deployment_type, extra_tags_array, git_sha, project, project_version, results_type, step,
                testcase_name):
    common_broader_kv_tags = [
        {"project": project}, {"use-case": testcase_name}, {"deployment-type": deployment_type},
        {"results-type": results_type}, {"step": step}]
    common_broader_kv_tags.extend(extra_tags_array)
    common_version_kv_tags = [
        {"project": project}, {"use-case": testcase_name}, {"deployment-type": deployment_type},
        {"results-type": results_type}, {"step": step}, {"version": project_version}]
    common_version_kv_tags.extend(extra_tags_array)
    common_git_sha_kv_tags = [
        {"project": project}, {"use-case": testcase_name}, {"deployment-type": deployment_type},
        {"results-type": results_type}, {"step": step}, {"version": project_version}, {"git_sha": git_sha}]
    return common_broader_kv_tags, common_git_sha_kv_tags, common_version_kv_tags
