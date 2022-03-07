#  BSD 3-Clause License
#
#  Copyright (c) 2022., Redis Labs Modules
#  All rights reserved.
#
import os

import redis
import yaml

from redisbench_admin.run.ann.ann import prepare_ann_benchmark_command


def test_prepare_ann_benchmark_command():
    with open("./tests/test_data/ann-config.yml", "r") as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        command_arr, command_str = prepare_ann_benchmark_command(
            "localhost",
            "6379",
            False,
            benchmark_config["clientconfig"],
            "result.json",
            ".",
        )
        assert (
            " ".join(command_arr[2:])
            == '--algorithm redisearch-hnsw --dataset mnist-784-euclidean --run-group M-4 --count 1 --build-clients 1 --test-clients 0 --host localhost --port 6379 --json-output ./result.json'
        )


def test_run_ann_from_command():
    run_builder = True
    TST_BUILDER_X = os.getenv("TST_RUN", "0")
    if TST_BUILDER_X == "0":
        run_builder = False
    if run_builder:
        conn = redis.Redis(port=6379)
        conn.ping()
        conn.flushall()
