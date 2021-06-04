#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#


import json
import os

import redis
from redistimeseries.client import Client

from redisbench_admin.export.common.common import split_tags_string

from redisbench_admin.export.redis_benchmark.redis_benchmark_csv_format import (
    redis_benchmark_export_logic,
)
from redisbench_admin.utils.utils import retrieve_local_or_remote_input_json


def export_command_logic(args):
    benchmark_files = args.benchmark_result_files
    local_path = os.path.abspath(args.local_dir)
    results_format = args.results_format
    input_tags_json = args.input_tags_json
    extra_tags_array = split_tags_string(args.extra_tags)

    if input_tags_json != "":
        print("Reading extra tags from json file: {}".format(input_tags_json))
        with open(input_tags_json, "r") as input_tags_json_file:
            tags_dict = json.load(input_tags_json_file)
            print(tags_dict)
            for k, v in tags_dict.items():
                extra_tags_array.append({k: v})

    print("Using the following extra tags: {}".format(extra_tags_array))
    results_type = "key-results"
    time_series_dict = {}
    if results_format == "redis-benchmark":
        benchmark_results = retrieve_local_or_remote_input_json(
            benchmark_files, local_path, "--benchmark-result-files", "csv"
        )
        for _, benchmark_result in benchmark_results.items():
            ok, time_series_dict = redis_benchmark_export_logic(
                benchmark_result, extra_tags_array, results_type, time_series_dict
            )

    elif results_format == "memtier_benchmark":
        print("TBD...")
        return 1
    else:
        print("results format not supported. Exiting...")
        return 1

    rts = Client(host=args.host, port=args.port, password=args.password)

    for timeseries_name, time_series in time_series_dict.items():
        try:
            rts.create(timeseries_name, labels=time_series["tags"])
        except redis.exceptions.ResponseError:
            # if ts already exists continue
            pass
        for pos, timestamp in enumerate(time_series["index"]):
            value = time_series["data"][pos]
            try:
                rts.add(timeseries_name, timestamp, value)
            except redis.exceptions.ResponseError:
                # if ts already exists continue
                pass
